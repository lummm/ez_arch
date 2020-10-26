defmodule Ez.ReqRegistry do
  @moduledoc """
  This holds state for requests.
  """
  use GenServer

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def put(req_id, pid) do
    GenServer.cast(__MODULE__, {:put, req_id, pid})
  end
  def clear(req_id) do
    GenServer.cast(__MODULE__, {:clear, req_id})
  end
  def get(req_id) do
    GenServer.call(__MODULE__, {:get, req_id})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:put, req_id, pid}, state) do
    {:noreply, Map.put(state, req_id, %{pid: pid})}
  end
  def handle_cast({:clear, req_id}, state) do
    {:noreply, Map.delete(state, req_id)}
  end

  @impl true
  def handle_call({:get, req_id}, _from, state) do
    {:reply, state[req_id], state}
  end

end
