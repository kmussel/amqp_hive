use Mix.Config

config :libcluster,
  debug: true,
  topologies: [
    amqp_hive: [
      strategy: Cluster.Strategy.Epmd,
      config: [
        hosts: ~w(a@127.0.0.1 b@127.0.0.1)a
      ]
    ]
  ]

config :amqp_hive,
  connections: [
    %{
      name: "prod_connection",
      connection: %{
        host: "${AMQP_HOST}",
        virtual_host: "",
        port: "${AMQP_PORT}",
        username: "${AMQP_USERNAME}",
        password: "${AMQP_PASSWORD}"
      },
      consumers: [
        %{
          name: "queue_worker",
          queue: "workers",
          options: [
            durable: true
          ]
        }
      ]
    }
  ]