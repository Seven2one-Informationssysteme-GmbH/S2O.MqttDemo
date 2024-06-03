namespace S2O.MqttDemo;

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Define the MQTT client settings
        var server = "localhost";
        var port = 1883;
        var clientId = "S2O.MqttDemo";
        var reconnectDelay = TimeSpan.FromSeconds(60);
        var autoReconnectDelay = TimeSpan.FromSeconds(5);

        // Define the topics to subscribe to
        var topics = new List<string>
        { 
            "karlsruhe/a/#",
            "karlsruhe/b/#"
        };

        // Create a new MQTT client
        using var client = new MqttClient(server, port, clientId, reconnectDelay, autoReconnectDelay, topics);

        // Connect to the MQTT broker and subscribe to the topics to receive messages
        await client.Start();

        // Main loop
        while (1 == 1)
        {
            Log.Output("Listening to incoming messages.");
            Log.Output("Enter 's' to send some messages or 'q' to quit.");

            var input = Console.ReadKey();
            Log.Info(string.Empty);

            if ('q'.Equals(input.KeyChar))
            {
                // Quit the application
                break;
            }
            else if ('s'.Equals(input.KeyChar))
            {
                // Send some messages
                var messages = new List<Message>
                {
                    new Message("other/topic", "Hello from S2O.MqttDemo 1"),
                    new Message("other/topic", "Hello from S2O.MqttDemo 2"),
                    new Message("other/topic", "Hello from S2O.MqttDemo 3"),
                    new Message("karlsruhe/a/1", "Hello from S2O.MqttDemo 4"),
                    new Message("karlsruhe/a/2", "Hello from S2O.MqttDemo 5")
                };

                await client.Send(messages);
            }
            else
            {
                Log.Warning("Unknown command");
            }
        }
    }
}
