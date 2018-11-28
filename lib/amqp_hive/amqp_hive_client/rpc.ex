defmodule AmqpHiveClient.Rpc do
  require Logger
  def start_link() do
    Task.start_link(fn -> loop(%{}) end)
  end

  defp loop(map) do
    receive do
      {:received_response, response} ->
        {caller, ref} = Map.get(map, "caller")
        send caller, {ref, response}
        response
      {:send, {caller, ref, [_name, callback]}} ->
        callback.()
        loop(Map.put(map, "caller", {caller, ref}))
      other ->
        Logger.debug(fn -> "Received other: #{inspect(other)}" end)
    end
  end

  def task(registered_name, pid, func, args) do
    ref = :erlang.make_ref()
    task = %Task{owner: self(), pid: pid, ref: ref}
    Swarm.send(registered_name, {func, {self(), ref, args}})
    task
  end

  def register_task(name, args) do
    {:ok, pid} = AmqpHiveClient.Rpc.start_link()
    AmqpHive.RpcRegistry.monitor(pid, {name, args})
    {:ok, pid}
  end

  def publish(conn_name, route, payload, options \\ []) do
    uuid = UUID.uuid4()
    correlation_id = "task-#{uuid}"
    options = options ++ [correlation_id: correlation_id]
    args = [correlation_id, fn -> 
      AmqpHiveClient.Producer.publish(conn_name, route, payload, options)
    end]
    {:ok, pid} = AmqpHive.RpcRegistry.register_task(correlation_id, args)
    AmqpHiveClient.Rpc.task(correlation_id, pid, :send, args)
  end
end