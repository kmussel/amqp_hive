defmodule AmqpHiveClient.Connection do
  use GenServer
  use AMQP
  require Logger

  def start_link(name, conn) do
    GenServer.start_link(__MODULE__, {name, conn}, name: name)
  end

  def child_spec(conn) do
    name = get_name(conn)

    %{
      id: name,
      start: {__MODULE__, :start_link, [name, conn]},
      restart: :temporary
    }
  end

  def init({name, conn}) do
    Logger.info(fn -> "Connection conn name = #{inspect(name)}" end)

    # Establish connection and start default consumers
    case establish_new_connection(conn) do
      {:ok, state} ->
        children = [
          AmqpHiveClient.ConsumerSupervisor.child_spec(name),
          AmqpHiveClient.ProducerSupervisor.child_spec(name)
        ]

        res = Supervisor.start_link(children, strategy: :one_for_one)        
        Logger.info(fn -> "Connection Started = #{inspect(res)}" end)
        {:ok, state}

      {:error, reason} ->
        {:error, "Could Not start connection #{inspect(reason)}"}
    end
  end

  def get_name(conn) do
    case Map.get(conn, :name) do
      nil -> __MODULE__
      name -> :"#{name}"
    end
  end

  defp establish_new_connection(conn) do
    connection = Map.get(conn, :connection, %{})
    host = Map.get(connection, :host, "")
    virtual_host = Map.get(connection, :virtual_host, "")    
    port = Map.get(connection, :port, 5672)
    username = Map.get(connection, :username, "")
    password = Map.get(connection, :password, "")

    # def open(options) when is_list(options) do
    #   options = options
    #   |> normalize_ssl_options
  
    #   amqp_params =
    #     amqp_params_network(username:           Keyword.get(options, :username,           "guest"),
    #                         password:           Keyword.get(options, :password,           "guest"),
    #                         virtual_host:       Keyword.get(options, :virtual_host,       "/"),
    #                         host:               Keyword.get(options, :host,               'localhost') |> to_charlist,
    #                         port:               Keyword.get(options, :port,               :undefined),
    #                         channel_max:        Keyword.get(options, :channel_max,        0),
    #                         frame_max:          Keyword.get(options, :frame_max,          0),
    #                         heartbeat:          Keyword.get(options, :heartbeat,          10),
    #                         connection_timeout: Keyword.get(options, :connection_timeout, 60000),
    #                         ssl_options:        Keyword.get(options, :ssl_options,        :none),
    #                         client_properties:  Keyword.get(options, :client_properties,  []),
    #                         socket_options:     Keyword.get(options, :socket_options,     []),
    #                         auth_mechanisms:    Keyword.get(options, :auth_mechanisms,    [&:amqp_auth_mechanisms.plain/3, &:amqp_auth_mechanisms.amqplain/3]))
  
    #   do_open(amqp_params)
    # end

    conn_options = [host: "#{host}", 
                virtual_host: virtual_host, 
                port: port, 
                username: username, 
                password: password,
                heartbeat: 20,
                heartbeat_interval: 20,
                channel_max: 10000
                # client_properties: [
                #   connection_attempts: 20
                #   retry_delay: 10
                # ]
              ]

    # connection_url =
    #   "amqp://#{username}:#{password}@#{host}:#{port}/#{virtual_host}?heartbeat_interval=20&retry_delay=10&connection_attempts=20"
    # connect_params = Enum.map(connection, fn {key, value} -> {:"#{key}", value} end)

    # Logger.info(fn -> "AMQP connect params: #{inspect(conn_options)}" end)

    # case AMQP.Connection.open(connection_url) do
    case AMQP.Connection.open(conn_options) do
      {:ok, amqpconn} ->
        Process.link(amqpconn.pid)
        {:ok, {conn, amqpconn, %{}}}

      {:error, {reason, message}} ->
        Logger.error(fn ->
          "Connection Failed for #{inspect(reason)} with message: #{inspect(message)}"
        end)
        {:error, message}

      {:error, reason} ->
        Logger.error(fn -> "Connection Failed for #{inspect(reason)}" end)
        {:error, reason}
    end
  rescue
    # Requeue unless it's a redelivered message.
    # This means we will retry consuming a message once in case of exception
    # before we give up and have it moved to the error queue
    #
    # You might also want to catch :exit signal in production code.
    # Make sure you call ack, nack or reject otherwise comsumer will stop
    # receiving messages.
    exception ->
      # :ok = Basic.reject channel, meta.delivery_tag, requeue: not redelivered
      Logger.error(fn -> "Error converting #{inspect(exception)} to json" end)
      {:error, "Error establishing connection"}
  end

  def request_channel(parent, consumer) when is_atom(parent) do
    # GenServer.call(parent, {:consumer_remove, consumer})
    GenServer.cast(parent, {:chan_request, consumer})
  end

  def request_channel(parent, consumer) do
    GenServer.cast(:"#{parent}", {:chan_request, consumer})
  end

  def remove_consumer(parent, consumer) do
    GenServer.call(:"#{parent}", {:consumer_remove, consumer})
  end

  def handle_call(
        {:consumer_remove, consumer},
        _from,
        {properties, conn, channel_mappings} = _state
      ) do
    # Logger.debug(fn -> "HANDLE CALL: remove consumer #{inspect(channel_mappings)}" end)
    channel = Map.get(channel_mappings, consumer)
    close_channel(channel)
    new_mapping = Map.delete(channel_mappings, consumer)
    {:reply, :ok, {properties, conn, new_mapping}}
  end

  def handle_cast({:chan_request, consumer}, {properties, conn, channel_mappings} = _state) do
    # Logger.debug(fn -> "HANDLE CAST: channel request #{inspect(state)}" end)
    new_mapping = store_channel_mapping(conn, consumer, channel_mappings)

    case Map.get(new_mapping, consumer) do
      nil ->
        try_create_channel(consumer)

      channel ->           
        AmqpHiveClient.Consumer.channel_available(consumer, channel)
    end

    {:noreply, {properties, conn, new_mapping}}
  end

  def handle_cast(_other, state) do
    {:noreply, state}
  end

  def handle_info({:create_channel, consumer}, {_properties, _conn, _channel_mappings} = state) do
    GenServer.cast(self(), {:chan_request, consumer})
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, {properties, conn, channel_mappings} = _state) do
    # Logger.debug("[CONNECTION] DOWN #{inspect(reason)}")
    channel = Map.get(channel_mappings, pid)
    close_channel(channel)
    new_mapping = Map.delete(channel_mappings, pid)
    {:noreply, {properties, conn, new_mapping}}
  end

  def handle_info(_reason, state) do
    {:noreply, state}
  end

  defp try_create_channel(consumer) do
    # In 3 seconds
    Process.send_after(self(), {:create_channel, consumer}, 3 * 1000)
  end

  defp store_channel_mapping(conn, consumer, channel_mappings) do
    Map.put_new_lazy(channel_mappings, consumer, fn -> create_channel(conn, consumer) end)
  end

  defp close_channel(channel) do
    if Process.alive?(channel.pid) do
      Channel.close(channel)
    end
  rescue
    _exception ->
      Logger.info(fn -> "Could not close channel" end)
  end

  defp create_channel(conn, consumer) do
    case Channel.open(conn) do
      {:ok, chan} ->  
        Process.monitor(consumer)       
        chan
      other -> 
        Logger.info("CREATe CHANNEL FAILED with = #{inspect(other)}")
        nil
    end
  end
end
