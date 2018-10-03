defmodule AmqpHiveClient.QueueHandler do
    use GenServer
    use AMQP
    require Logger
  
    def start_link() do
      GenServer.start_link(__MODULE__, [], name: __MODULE__)
    end
  
    def child_spec() do
      %{
        id: __MODULE__,
        start: {__MODULE__, :start_link, []}
      }
    end
  
    def init() do
      {:ok, {}}
    end
  
    def handle_cast({:delete_queue, chan, queue}, state) do
    #   Logger.info(fn -> "Deleteing Queue #{queue}" end)
      AMQP.Queue.delete(chan, queue, [])
      {:noreply, state}
    end
end