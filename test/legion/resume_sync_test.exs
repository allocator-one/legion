defmodule Legion.ResumeSyncTest do
  # Global Mimic interception is shared with native AgentServer children.
  use ExUnit.Case, async: false
  use Mimic

  alias Legion.Store.Payload

  @moduletag capture_log: true

  defmodule CheckpointAgent do
    @moduledoc "Continue the saved calculation."
    use Legion.Agent
  end

  defmodule Store do
    @behaviour Legion.Store

    def start_link,
      do: Agent.start_link(fn -> %{rows: %{}, writes: [], hook: nil} end, name: __MODULE__)

    def seed(payload),
      do: Agent.update(__MODULE__, &put_in(&1, [:rows, payload.agent_id], payload))

    def hook(hook), do: Agent.update(__MODULE__, &Map.put(&1, :hook, hook))
    def writes, do: Agent.get(__MODULE__, &Enum.reverse(&1.writes))

    @impl true
    def persistence_frequency, do: :step

    @impl true
    def list(_limit), do: []

    @impl true
    def get(id) do
      Agent.get(__MODULE__, fn state ->
        case Map.fetch(state.rows, id) do
          {:ok, payload} -> {:ok, payload}
          :error -> :error
        end
      end)
    end

    @impl true
    def save(payload) do
      hook = Agent.get(__MODULE__, & &1.hook)

      if hook == :fail_final and payload.status == :idle do
        :error
      else
        maybe_wait(hook, payload)

        # Deliberately retain only this checkpoint envelope. The startup write
        # must itself contain the restored conversation, not just metadata.
        Agent.update(__MODULE__, fn state ->
          %{
            state
            | rows: Map.put(state.rows, payload.agent_id, payload),
              writes: [payload | state.writes]
          }
        end)

        maybe_interrupt(hook, payload)
        :ok
      end
    end

    defp maybe_wait({:hold_final, owner, token}, %Payload{status: :idle}) do
      send(owner, {:final_save_waiting, self(), token})

      receive do
        {:allow_final_save, ^token} -> :ok
      after
        5_000 -> exit(:final_save_not_released)
      end
    end

    defp maybe_wait(_hook, _payload), do: :ok

    defp maybe_interrupt(:interrupt_startup, %Payload{started_at: started})
         when not is_nil(started),
         do: exit(:interrupted_after_startup_save)

    defp maybe_interrupt(:interrupt_completing, %Payload{
           conversation_state: %{executor_state: %{phase: :completing}},
           started_at: nil
         }),
         do: exit(:interrupted_after_completing_save)

    defp maybe_interrupt(_hook, _payload), do: :ok
  end

  setup :set_mimic_global

  setup do
    start_supervised!(%{id: Store, start: {Store, :start_link, []}})
    id = "sync-resume-#{System.unique_integer([:positive])}"
    %{id: id}
  end

  test "returns the saved Lua value on the last step without another task", %{id: id} do
    expected = %{"answer" => 42, "source" => "saved"}
    seed(id, 2, 0, [{"prepared", expected}])
    owner = self()

    stub(ReqLLM, :generate_object, fn _model, messages, _schema ->
      send(owner, {:provider_request, messages})
      response("eval_and_complete", "return prepared")
    end)

    assert {:ok, ^expected} = resume(id)
    assert_received {:provider_request, messages}
    assert Enum.count(messages, &(&1[:content] == "original task")) == 1
    refute_received {:provider_request, _}
    assert {:ok, %Payload{status: :idle}} = Store.get(id)
    assert :error = Legion.lookup(id)
  end

  test "an exhausted checkpoint returns the native cancellation without a request", %{id: id} do
    seed(id, 3)
    stub(ReqLLM, :generate_object, fn _, _, _ -> flunk("exhausted checkpoint dispatched") end)

    assert {:cancel, :reached_max_iterations} = resume(id)
    assert :error = Legion.lookup(id)
  end

  test "a consumed retry permits only the outstanding retry request", %{id: id} do
    seed(id, 2, 1)
    requests = :counters.new(1, [])

    stub(ReqLLM, :generate_object, fn _, _, _ ->
      :counters.add(requests, 1, 1)
      response("eval_and_continue", "error('still broken')")
    end)

    assert {:cancel, :reached_max_retries} = resume(id)
    assert :counters.get(requests, 1) == 1
    assert :error = Legion.lookup(id)
  end

  test "startup preserves the complete checkpoint through a second interruption", %{id: id} do
    expected = %{"answer" => 42}
    checkpoint = seed(id, 2, 1, [{"prepared", expected}])
    Store.hook(:interrupt_startup)
    requests = :counters.new(1, [])

    stub(ReqLLM, :generate_object, fn _, _, _ ->
      :counters.add(requests, 1, 1)
      response("eval_and_complete", "return prepared")
    end)

    assert {:error, _} = resume(id)
    assert {:ok, restored} = Store.get(id)
    assert restored.conversation_state == checkpoint.conversation_state
    assert restored.status == :running
    assert :counters.get(requests, 1) == 0
    assert :error = Legion.lookup(id)

    Store.hook(nil)
    assert {:ok, ^expected} = resume(id)
    assert :counters.get(requests, 1) == 1
  end

  test "a completing checkpoint returns its raw result after interruption without replay", %{
    id: id
  } do
    expected = %{"answer" => 42, "nested" => %{"value" => "exact"}}
    seed(id, 2, 0, [{"prepared", expected}])
    Store.hook(:interrupt_completing)
    requests = :counters.new(1, [])

    stub(ReqLLM, :generate_object, fn _, _, _ ->
      :counters.add(requests, 1, 1)
      response("eval_and_complete", "return prepared")
    end)

    assert {:error, _} = resume(id)
    assert {:ok, %Payload{conversation_state: %{executor_state: state}}} = Store.get(id)
    assert state == %{phase: :completing, iteration: 2, retries: 0, result: expected}
    assert :error = Legion.lookup(id)

    Store.hook(nil)
    assert {:ok, ^expected} = resume(id)
    assert :counters.get(requests, 1) == 1
    assert :error = Legion.lookup(id)
  end

  test "an explicitly persisted nil result is a valid completed value", %{id: id} do
    checkpoint = seed(id, 2)
    state = %{phase: :completing, iteration: 2, retries: 0, result: nil}
    Store.seed(put_in(checkpoint.conversation_state.executor_state, state))
    stub(ReqLLM, :generate_object, fn _, _, _ -> flunk("completion dispatched") end)

    assert {:ok, nil} = resume(id)
  end

  test "legacy completing state without a raw result fails before writes or requests", %{id: id} do
    checkpoint = seed(id, 2)
    state = %{phase: :completing, iteration: 2, retries: 0}
    Store.seed(put_in(checkpoint.conversation_state.executor_state, state))
    stub(ReqLLM, :generate_object, fn _, _, _ -> flunk("legacy completion dispatched") end)

    assert {:error, :invalid_resume_checkpoint} = resume(id)
    assert Store.writes() == []
    assert :error = Legion.lookup(id)
  end

  test "missing or malformed checkpoints never become fresh turns", %{id: id} do
    stub(ReqLLM, :generate_object, fn _, _, _ -> flunk("invalid checkpoint dispatched") end)
    assert {:error, :invalid_resume_checkpoint} = resume(id)
    checkpoint = seed(id, 1)

    invalid = [
      %{checkpoint | status: :idle},
      %{checkpoint | agent_module: __MODULE__},
      %{checkpoint | parent_agent_id: "some-other-parent"},
      %{checkpoint | conversation_state: nil},
      put_in(checkpoint.conversation_state.messages, []),
      put_in(checkpoint.conversation_state.executor_state, :nonexistent),
      put_in(checkpoint.conversation_state.executor_state.iteration, -1),
      put_in(checkpoint.conversation_state.executor_state.retries, -1),
      put_in(checkpoint.conversation_state.executor_state.phase, :unknown)
    ]

    for payload <- invalid do
      Store.seed(payload)
      assert {:error, :invalid_resume_checkpoint} = resume(id)
      assert Store.writes() == []
      assert :error = Legion.lookup(id)
    end
  end

  test "explicit store and nonempty identity are required" do
    for opts <- [[], [store: Store], [agent_id: "id"], [store: Store, agent_id: ""]] do
      assert {:error, :invalid_resume_options} =
               apply(Legion, :resume_sync, [CheckpointAgent, opts])
    end

    assert Store.writes() == []
  end

  test "an existing live identity is refused without sending it a task", %{id: id} do
    {:ok, pid} = Legion.start_link(CheckpointAgent, store: Store, agent_id: id)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    seed(id, 2)
    before = Legion.get_messages(pid)
    writes = Store.writes()
    stub(ReqLLM, :generate_object, fn _, _, _ -> flunk("duplicate identity dispatched") end)

    assert {:error, :already_running} = resume(id)
    assert Legion.get_messages(pid) == before
    assert Store.writes() == writes
    assert Process.alive?(pid)
  end

  test "success is returned only after the final Store acknowledgement", %{id: id} do
    seed(id, 2)
    token = make_ref()
    Store.hook({:hold_final, self(), token})
    stub(ReqLLM, :generate_object, fn _, _, _ -> response("return", "", "finished") end)
    task = Task.async(fn -> resume(id) end)

    assert_receive {:final_save_waiting, server, ^token}, 1_000
    assert Task.yield(task, 0) == nil
    send(server, {:allow_final_save, token})
    assert {:ok, "finished"} = Task.await(task)
    assert :error = Legion.lookup(id)
  end

  test "a failed final save returns an error and leaves no live agent", %{id: id} do
    seed(id, 2)
    Store.hook(:fail_final)
    stub(ReqLLM, :generate_object, fn _, _, _ -> response("return", "", "uncommitted") end)

    assert {:error, _} = resume(id)
    refute match?({:ok, %Payload{status: :idle}}, Store.get(id))
    assert :error = Legion.lookup(id)
  end

  test "an executor exit returns an error and removes the temporary process", %{id: id} do
    seed(id, 2)
    stub(ReqLLM, :generate_object, fn _, _, _ -> exit(:simulated_native_failure) end)

    assert {:error, _} = resume(id)
    assert :error = Legion.lookup(id)
  end

  test "caller death terminates blocked recovered work", %{id: id} do
    seed(id, 2)
    owner = self()

    stub(ReqLLM, :generate_object, fn _, _, _ ->
      send(owner, {:provider_waiting, self()})

      receive do
        :never_sent -> response("return", "", "unexpected")
      end
    end)

    caller = spawn(fn -> resume(id) end)
    assert_receive {:provider_waiting, server}, 1_000
    monitor = Process.monitor(server)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^server, _}, 1_000
    assert :error = Legion.lookup(id)
  end

  defp seed(id, iteration, retries \\ 0, bindings \\ []) do
    payload = %Payload{
      agent_id: id,
      agent_module: nil,
      status: nil,
      usage: [],
      conversation_state: %{
        messages: [
          Legion.Executor.message(:user, "original task"),
          Legion.Executor.message(:eval_result, "saved progress")
        ],
        bindings: bindings,
        executor_state: %{phase: :awaiting_llm, iteration: iteration, retries: retries}
      }
    }

    Store.seed(payload)
    payload
  end

  defp resume(id) do
    apply(Legion, :resume_sync, [
      CheckpointAgent,
      [store: Store, agent_id: id, max_iterations: 3, max_retries: 1]
    ])
  end

  defp response(action, code, result \\ "") do
    {:ok,
     %ReqLLM.Response{
       id: "synthetic-resume-response",
       model: "test",
       context: nil,
       object: %{"action" => action, "code" => code, "result" => result},
       usage: %{input_tokens: 1, output_tokens: 1}
     }}
  end
end
