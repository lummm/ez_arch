defmodule Ez.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    children = [
      worker(Ez.Env, []),
      worker(Ez.ReqRegistry, []),
      supervisor(Ez.WorkerInterfaceSup, []),
      worker(Ez.Workers, []),
      supervisor(Ez.ZmqInterfaceSup, []),
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

end
