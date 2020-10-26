defmodule Ez.Env do
  use GenServer

  # Client API
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # vars
  @doc """
  An EZ request will park for (backpressure_ratio * X) milliseconds when
  sent to a worker with X active jobs, assuming the number of jobs is above
  the backpressure_threshold.
  """
  def backpressure_ratio, do: GenServer.call(__MODULE__,
        {:get, :backpressure_ratio})

  @doc """
  Threshold number of jobs at which to apply backpressure ratio.
  """
  def backpressure_threshold, do: GenServer.call(__MODULE__,
        {:get, :backpressure_threshold})

  @doc """
  Port at which EZ requests are made over ZMQ.
  """
  def ez_port, do: GenServer.call(__MODULE__, {:get, :ez_port})

  @doc """
  MAxixmum time in milliseconds we will let a request timeout reach.
  After this point the request is considered failed.
  """
  def max_req_timeout, do: GenServer.call(__MODULE__,
        {:get, :max_req_timeout})

  @doc """
  Milliseconds after which we assume a worker died.
  """
  def worker_lifetime, do: GenServer.call(__MODULE__,
        {:get, :worker_lifetime})

  @doc """
  Port workers listen for work / heartbeat on over ZMQ.
  """
  def worker_port, do: GenServer.call(__MODULE__, {:get, :worker_port})

  # Server callbacks
  @impl true
  def init(_args) do
    {:ok, load()}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state, key, nil), state}
  end

  @impl true
  def handle_cast({:reload}, _state) do
    {:noreply, load()}
  end

  # private
  defp load do
    state = %{
      backpressure_ratio: load_type(Float, "BACKPRESSURE_RATIO", "10.0"),
      backpressure_threshold: load_type(Integer,
        "BACKPRESSURE_THRESHOLD", "5"),
      ez_port: load_type(Integer, "EZ_PORT"),
      min_req_timeout: load_type(Integer, "MIN_REQ_TIMEOUT", "500"),
      max_req_timeout: load_type(Integer, "MAX_REQ_TIMEOUT", "5000"),
      worker_lifetime: load_type(Integer, "WORKER_LIFETIME_MS", "2500"),
      worker_port: load_type(Integer, "WORKER_PORT"),
    }
    IO.puts("loaded env #{Kernel.inspect(state)}")
    state
  end

  defp load_type(type, key, default \\ nil) do
    as_str = if default, do: load(key, default), else: load(key)
    {as_int, ""} = type.parse(as_str)
    as_int
  end

  defp load(key) do
    System.fetch_env!(key)
  end
  defp load(key, default) do
    case System.fetch_env(key) do
      {:ok, val} -> val
      :error -> default
    end
  end

end
