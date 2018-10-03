# AmqpHive

A high availability amqp connection/consumer manager using Swarm.  

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `amqp_hive` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:amqp_hive, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/amqp_hive](https://hexdocs.pm/amqp_hive).



There are two parts to this.  
1.  Handling local AMQP connections and consumers without registering them with the other nodes in the cluster.     This is to have these start up on each new instance we bring up.  
2.  Register the dynamic consumer processes that we want to be started on any available node in the cluster.  


###Part One:
We want to do things the elixir way and allow for the processes to die but we also want to keep an entire application from crashing if a connection fails.  To do this we use Dynamic Supervisors with child processes that have temporary restart policies.  This will keep elixir from trying to restart the process.  We want to make sure that the child processes are restarted therefore we will keep a separate registry for the local connections and consumers. We'll call this local registry our ConnectionManager (CM). The CM will be on a time interval to constantly try and launch the local connections and consumers.  
Each of the local connection and consumer processes will have a distinct name which will be added to the state of the CM.  If one of the processes goes down it will remove them from the state and on the next loop of the CM it will start those processes again which adds them back to the state. 


###Part Two:
We use Swarm to load balance consumer processes across available nodes.  
We register our consumer with Swarm using a unique name, the module and function name to call when starting the process and the arguments to pass to that function.  
`Swarm.register_name(name, AmqpHiveClient.ConnectionManager, :register_consumer, [name, consumer, connection_name])`
The arguments passed to the function are a unique name, the consumer options such as queue and queue durability and the connection name to attach the consumer to.  
```
{
  "my_conn-consumer_queue1", 
  %{name: "my_conn-consumer_queue1", queue: "consumer_queue1", options: [durable: true]}, 
  "my_conn"
}
```

The Connection Manager then makes sure that connection has started and adds the consumer process.  That process is then monitored by our Swarm ConsumerRegistry. If it goes down for anything other than normal it will be started back up.  


##Config:

The connection manager state will have the connection config

```
config :amqp_hive,
  connections: [
    %{
      name: "dev_connection",
      connection: %{
        host: "localhost",
        virtual_host: "deployments",
        port: "5672",
        username: "guest",
        password: "guest"
      },
      consumers: [
        %{
          name: "consumer1",
          queue: "deployments",
          options: [
            durable: true
          ]
        }
      ]
    }
  ]
```

To test out dynamically adding queues and consumers:
1. Install Rabbitmq [http://www.rabbitmq.com/download.html]
2. Setup Virtual Hosts, Queues and Bindings. 
    A. Create a virtual_host named "deployments" or update the dev.exs file with whatever name you choose
    B. Create a queue called "deployments" or update the dev.exs file consumer `queue`
    C. Create a binding from amq.topic exhange to the queue you created in B.  
        Use the routing key `rpc.create_deployment`
        If you change the routing key you'll need to update the `handle_route` function in the consumer.ex file
        and the handle_info :start function in load_test.ex
3.  Inside the AmqpHive.Application children, uncomment the AmqpHive.LoadTest.child_spec() 
        (dont forget the comma)
4.  Depending on the size of your rabbitmq instance you might want to modify the @num_queues in AmqpHive.LoadTest.  Currently set at 10_000

5. Open up 3 instances of a terminal and run this application in each one.  (Or however many you want)
    iex --name a@127.0.0.1 -S mix
    iex --name b@127.0.0.1 -S mix
    iex --name c@127.0.0.1 -S mix

