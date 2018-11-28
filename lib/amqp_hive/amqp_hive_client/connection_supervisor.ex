defmodule AmqpHiveClient.ConnectionSupervisor do
  use DynamicSupervisor

  require Logger

  def start_link(conn) do
    DynamicSupervisor.start_link(__MODULE__, conn, name: __MODULE__)
  end

  def child_spec(conn) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [conn]}
    }
  end

  def start_connection(conn) do
    DynamicSupervisor.start_child(
      __MODULE__,
      AmqpHiveClient.Connection.child_spec(conn)
    )
  end

  def stop_connection(_connection_name, name) when is_atom(name) do
    case Process.whereis(name) do
      nil ->
        {:error, "No process"}

      pid ->
        DynamicSupervisor.terminate_child(
          __MODULE__,
          pid
        )
    end
  end

  def stop_connection(_name, pid) do
    DynamicSupervisor.terminate_child(
      __MODULE__,
      pid
    )
  end

  def init(_conn) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
