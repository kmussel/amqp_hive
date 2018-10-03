defmodule AmqpHive.MixProject do
  use Mix.Project

  @version File.read!("VERSION") |> String.trim()

  def project do
    [
      app: :amqp_hive,
      version: @version,
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [
        :poison,
        :httpoison,
        :libcluster,
        :logger,        
        :amqp,        
        :swarm
      ],
      mod: {AmqpHive.Application, []}
      
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:secure_random, "~> 0.5"},
      {:libcluster, "~> 3.0.3"},
      {:swarm, "~> 3.0"},
      {:poison, "~> 3.1.0"},
      {:httpoison, "~> 1.0"},
      {:amqp, "~> 1.0"},
      {:elixir_uuid, "~> 1.2.0"}
    ]
  end
end
