defmodule AmqpHiveClient.Handlers.Registry do  
  use AMQP
  require Logger

  def handle_startup() do
    Logger.info("Handle Startup")
    # resubscribe_queues()
  end


  def resubscribe_queues() do
    Logger.info("Resubscribe to queues")
    connections = Application.get_env(:amqp_hive, :connections, [])
    Enum.each(connections, fn conn ->
      conn_name = Map.get(conn, :name, "")
      connection = Map.get(conn, :connection, %{})
      
      consumers = Map.get(conn, :consumers, [])
      local_queue_names = 
        Enum.reduce(consumers, [], fn consumer, acc ->
          acc ++ [Map.get(consumer, :queue)]
        end)

      local_queue_names = local_queue_names ++ ["argus"]
      Logger.info("Local queue names = #{inspect(local_queue_names)}")
      queues = get_queues(connection)
      Enum.each(queues, fn queue ->
        queue_name = Map.get(queue, "name")
        if !is_nil(queue_name) do
          case Enum.member?(local_queue_names, queue_name) do
            true -> 
              # Logger.info("Queue #{queue_name} is a member")
              nil
            false ->
              name = "#{conn_name}-#{queue_name}"

              consumer = %{
                name: name,
                queue: queue_name,
              }
              AmqpHive.ConsumerRegistry.start_consumer_via_swarm(name, consumer, conn_name, "Initial Start")
          end
        end
      end)
    end)
  end

  def get_queues(%{host: host, virtual_host: vhost, username: username, password: password } = _conn) do
    headers = [
      {:"Content-Type", "application/json"},
      {:Authorization, "Basic #{basic_auth_cred(username, password)}"}
    ]

    with {:ok, %HTTPoison.Response{body: body}} <-
      HTTPoison.get(
        "#{rabbitmq_api_queue_url(host, vhost)}",
        headers,
        recv_timeout: 60_000
      ) do
      Poison.decode!(body)
    else
      error -> Logger.info("Error #{inspect(error)}")
      []
    end
  end

  def get_queues(_), do: []


  def rabbitmq_api_queue_url(host, virtual_host) do
    "https://#{host}/api/queues/#{virtual_host}"
  end

  def basic_auth_cred(username, password) do
    Base.encode64(username <> ":" <> password)
  end
end
