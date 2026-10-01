defmodule Legion.Store.Payload do
  @moduledoc """
  Data supplied to and returned from a `Legion.Store`.

  Payloads are partial updates: `agent_id` is required, while a `nil` value for
  every other field means the store must preserve its existing value. A
  `conversation_state` holds the persisted messages, bindings, and executor
  checkpoint. Its `:executor_state` value is `:nonexistent` for an ordinary snapshot
  or a map when step persistence captures an interrupted turn.

  An `:awaiting_llm` checkpoint contains `:phase`, `:iteration`, and `:retries`.
  A `:completing` checkpoint also requires `:result`, containing the raw code
  evaluation result so recovery can return it without executing the code again.
  An explicitly stored `nil` result is valid and differs from a missing key;
  the partial-update convention for top-level fields does not apply to this
  nested result. Legacy completing checkpoints without `:result` are invalid
  for synchronous recovery with `Legion.resume_sync/2`.

  See `Legion.Store` for the full store contract.
  """

  @enforce_keys [:agent_id]
  defstruct [
    :agent_id,
    :agent_module,
    :parent_agent_id,
    :status,
    :started_at,
    :conversation_state,
    :usage,
    :ratelimit_metadata
  ]

  @type status :: :idle | :running

  @type executor_state ::
          %{
            phase: :awaiting_llm,
            iteration: non_neg_integer(),
            retries: non_neg_integer()
          }
          | %{
              phase: :completing,
              iteration: non_neg_integer(),
              retries: non_neg_integer(),
              result: term()
            }

  @type state :: %{
          messages: [map()],
          bindings: term(),
          executor_state: executor_state() | :nonexistent
        }

  @type t :: %__MODULE__{
          agent_id: Legion.Store.agent_id(),
          agent_module: module() | nil,
          parent_agent_id: Legion.Store.agent_id() | nil,
          status: status() | nil,
          started_at: NaiveDateTime.t() | nil,
          conversation_state: state() | nil,
          usage: [map()] | nil,
          ratelimit_metadata: map() | nil
        }
end
