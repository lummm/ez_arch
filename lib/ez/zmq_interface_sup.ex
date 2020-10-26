defmodule Ez.ZmqInterfaceSup do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    children = [
      worker(Ez.ZmqInterface, []),
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

end
