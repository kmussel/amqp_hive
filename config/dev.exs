use Mix.Config

config :logger, 
  level: :debug

config :libcluster,
  debug: true,
  topologies: [
    amqp_hive: [
      strategy: Cluster.Strategy.Epmd,
      config: [
        hosts: ~w(a@127.0.0.1 b@127.0.0.1 c@127.0.0.1)a
      ]
    ]
  ]

config :amqp_hive,
  connections: [
    %{
      name: "dev_connection",
      connection: %{
        host: "localhost",
        virtual_host: "deployments",
        port: 5672,
        username: "guest",
        password: "guest"
      },
      consumers: [
        %{
          name: "handle_deployments",
          queue: "deployments",
          options: [
            durable: true
          ]
        },
        %{
          name: "handle_deployments2",
          queue: "deployments",
          options: [
            durable: true
          ]
        }
      ]
    }
  ]