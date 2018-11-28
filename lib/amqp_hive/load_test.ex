defmodule AmqpHive.LoadTest do
    use GenServer
    require Logger
  
    @start_delay Application.get_env(:hive, :start_delay, 10_000)

    @name __MODULE__
    @num_queues 10 #_000
  
    def start_link do
      GenServer.start_link(__MODULE__, [], name: __MODULE__)
    end
  
    def child_spec() do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, []}
        }
    end

    def init(_) do
      Process.send_after(self(), :start, @start_delay)
      {:ok, {[], 0}}
    end

    def kill_queues() do
      send(Process.whereis(@name), :kill_queues)
    end
    def handle_info(:start, {_queues, _} = _state) do
      allqueues = 
        for i <- 0..@num_queues, i > 0 do
            uuid = UUID.uuid4()        
            AmqpHiveClient.Producer.publish("dev_connection", "rpc.create_deployment", %{"deployment_id" => uuid, "msg" => "hello there"}, [exchange: "amq.topic"])
            uuid
        end

      Process.send_after(self(), :send_message, 100_000)
      {:noreply, {allqueues, 0}}
    end
  
    def handle_info(:send_message, {queues, _} = state) do
        for queue <- queues do
            AmqpHiveClient.Producer.publish("dev_connection", "#{queue}", %{"action" => "log", "msg" => "hello there"})
        end
        Process.send_after(self(), :kill_queues, 100_000)
        {:noreply, state}
    end

    def handle_info(:kill_queues, {queues, _} = state) do
        for queue <- queues do
            AmqpHiveClient.Producer.publish("dev_connection", "#{queue}", %{"action" => "finished", "msg" => "hello there"})
        end      
      {:noreply, state}
    end

    def handle_info(:message_cnt, {_queue, cnt} = state) do
        Logger.info("message cnt = #{cnt}")
        {:noreply, state}
    end

    def handle_cast(:message_received, {queues, cnt} = _state) do
        {:noreply, {queues, cnt + 1}}
    end

    # def handle_info(:remove_all, state) do
        # send(AmqpHive.LoadTest, :kill_queues)
        # send(AmqpHive.LoadTest, :message_cnt)
        # send(AmqpHive.LoadTest, :send_message)
    #     Swarm.members(:amqp_hive_consumers)
    #     Swarm.publish(:amqp_hive_consumers, :finished)
    # AmqpHiveClient.Producer.publish("dev_connection", "00018bc8-4caa-4666-802d-5f6f31f2ac6b", %{"action" => "finished", "msg" => "hello there"})
  end