defmodule AmqpHive.Application do
    use Application
  
    require Logger
    
    def start(_type, _args) do
      Logger.info(fn -> "Starting AmqpHive Application" end)

      topologies = Application.get_env(:libcluster, :topologies, [])
      connections = Application.get_env(:amqp_hive, :connections, [])

      children = [
        {Cluster.Supervisor, [topologies, [name: AmqpHive.ClusterSupervisor]]},
        AmqpHive.ConsumerRegistry.child_spec(),
        AmqpHive.RpcRegistry.child_spec(),
        AmqpHiveClient.QueueHandler.child_spec(),
        AmqpHiveClient.ConnectionSupervisor.child_spec(connections),
        AmqpHiveClient.ConnectionManager.child_spec(connections)
        # AmqpHive.LoadTest.child_spec()
      ]

      Supervisor.start_link(children, strategy: :one_for_one, name: AmqpHive.Supervisor)
    end
end
