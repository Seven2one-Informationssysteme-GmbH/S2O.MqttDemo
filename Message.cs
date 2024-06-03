namespace S2O.MqttDemo;

public struct Message
{
    public Message(string topic, string payload)
    {
        Topic = topic;
        Payload = payload;
    }

    public string Topic { get; init; }
    public string Payload { get; init; }
}
