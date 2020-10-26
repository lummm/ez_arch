defmodule Ez.Requests do
  @moduledoc """
  This holds state for requests.
  """
  use GenServer

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def put_req(req_id, pid) do
    GenServer.cast(__MODULE__, {:put_req, req_id, pid})
  end

  def pop_req(req_id) do
    GenServer.call(__MODULE__, {:pop_req, req_id})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:put_req, req_id, pid}, state) do
    {:noreply, put_in(state, [req_id], %{
            pid: pid}
      )}
  end

  @impl true
  def handle_call({:pop_req, req_id}, _from, state) do
    info = Map.get(state, req_id)
    {:reply, info, Map.delete(state, req_id)}
  end

end
