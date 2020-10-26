defmodule Ez.Requests do
  @moduledoc """
  This holds state for requests.
  """
  use GenServer

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def match_pid_req(pid, req_id) do
    GenServer.cast(__MODULE__, {:match_pid_req, pid, req_id})
  end

  def get_pid_for_req(req_id) do
    GenServer.call(__MODULE__, {:get_pid, req_id})
  end

  # Server callbacks
  @impl true
  def init(_args) do
    {:ok, %{
        requests: %{},
     }}
  end

  @impl true
  def handle_cast({:match_pid_req, pid, req_id}, state) do
    {:noreply, put_in(state, [:requests, req_id], pid)}
  end

  @impl true
  def handle_call({:get_pid, req_id}, _from, state) do
    {:reply, get_in(state, [:requests, req_id]), state}
  end

end
