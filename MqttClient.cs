namespace S2O.MqttDemo;

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MQTTnet.Protocol;
using System;
using System.Text;

public class MqttClient : IDisposable
{
    private const MqttQualityOfServiceLevel DefaultMqttQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce;
    private static readonly Encoding DefaultMessageEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly string _server;
    private readonly int _port;
    private readonly string _clientId;
    private readonly TimeSpan _reconnectDelay;
    private readonly TimeSpan _autoReconnectDelay;
    private readonly IReadOnlyList<string> _topics;
    private readonly CancellationTokenSource _disposeCancellationTokenSource;

    private IManagedMqttClient? _client;
    private ManagedMqttClientOptions? _clientOptions;
    private Task? _reconnectTask;
    private bool isDisposed;

    public MqttClient(
        string server,
        int port,
        string clientId,
        TimeSpan reconnectDelay,
        TimeSpan autoReconnectDelay,
        IReadOnlyList<string> topics)
    {
        _server = server;
        _port = port;
        _clientId = clientId;
        _reconnectDelay = reconnectDelay;
        _autoReconnectDelay = autoReconnectDelay;
        _topics = topics;

        _disposeCancellationTokenSource = new CancellationTokenSource();
    }

    public async Task Start()
    {
        if (_client is not null)
        {
            throw new InvalidOperationException("MQTT client is already started");
        }

        Log.Info($"MQTT client '{_clientId}' starting");

        _clientOptions = CreateManagedMqttClientOptions(_server, _port, _clientId, _autoReconnectDelay);
        var topicFilters = CreateTopicFilters(_clientId, _topics);

        _client = new MqttFactory().CreateManagedMqttClient();

        _client.ApplicationMessageReceivedAsync += OnApplicationMessageReceived;
        _client.ConnectedAsync += OnConnected;
        _client.ConnectingFailedAsync += OnConnectingFailed;
        _client.DisconnectedAsync += OnDisconnected;
        _client.SynchronizingSubscriptionsFailedAsync += OnSynchronizingSubscriptionsFailed;

        await _client.SubscribeAsync(topicFilters); // Do subscibe BEFORE StartAsync() to receive stored messages immediately!
        await _client.StartAsync(_clientOptions);

        _reconnectTask = Reconnect();

        Log.Info($"MQTT client '{_clientId}' started");
    }

    public async Task Send(IReadOnlyList<Message> messages)
    {
        if (_client is null)
        {
            throw new InvalidOperationException("MQTT client is not started");
        }

        Log.Info($"MQTT client '{_clientId}' is sending {messages.Count} messages");
        var exceptions = new List<Exception>();

        _client!.ApplicationMessageProcessedAsync += OnApplicationMessageProcessed;
        try
        {
            foreach (var message in messages)
            {
                var qualityOfServiceLevel = DefaultMqttQualityOfServiceLevel;
                Log.Info($"MQTT client '{_clientId}' is sending message with topic '{message.Topic}', quality of service level '{qualityOfServiceLevel}', payload '{message.Payload}'");

                var payload = DefaultMessageEncoding.GetBytes(message.Payload);
                var applicationMessage = new MqttApplicationMessageBuilder()
                    .WithTopic(message.Topic)
                    .WithPayload(payload)
                    .WithQualityOfServiceLevel(qualityOfServiceLevel)
                    .Build();
                await _client!.EnqueueAsync(applicationMessage);
            }

            await WaitUntilMessagesSent();

            if (exceptions.Count > 0)
            {
                throw new AggregateException(
                    $"MQTT client '{_clientId}' encountered errors when sending messages",
                    exceptions);
            }
        }
        finally
        {
            _client!.ApplicationMessageProcessedAsync -= OnApplicationMessageProcessed;
            Log.Info($"MQTT client '{_clientId}' sent {messages.Count} messages");
        }

        Task OnApplicationMessageProcessed(ApplicationMessageProcessedEventArgs eventArgs)
        {
            if (eventArgs.Exception == null)
            {
                Log.Info($"MQTT client '{_clientId}' sent message with topic '{eventArgs.ApplicationMessage?.ApplicationMessage?.Topic}' and QoS '{eventArgs.ApplicationMessage?.ApplicationMessage?.QualityOfServiceLevel}'");
            }
            else
            {
                Log.Error(eventArgs.Exception, $"MQTT client '{_clientId}' encountered an error when sending message with topic '{eventArgs.ApplicationMessage?.ApplicationMessage?.Topic}'");
                exceptions.Add(eventArgs.Exception);
            }

            return Task.CompletedTask;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected void Dispose(bool disposing)
    {
        if (!isDisposed)
        {
            if (disposing)
            {
                _disposeCancellationTokenSource?.Cancel();
                _reconnectTask?.Wait();

                try
                {
                    _client?.StopAsync().Wait(TimeSpan.FromSeconds(10));
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"MQTT client '{_clientId}' error when stopping");
                }

                _client?.Dispose();
                _client = null;
            }

            isDisposed = true;
        }
    }

    private async Task WaitUntilMessagesSent()
    {
        Log.Info($"MQTT client '{_clientId}' is waiting for messages to be sent");

        while (_client!.PendingApplicationMessagesCount > 0)
        {
            _disposeCancellationTokenSource.Token.ThrowIfCancellationRequested();
            await Task.Delay(TimeSpan.FromMilliseconds(1), _disposeCancellationTokenSource.Token);
        }
    }

    // Reconnect client every few minutes to ensure receiving ALL stored messages from the broker
    private async Task Reconnect()
    {
        Log.Info($"MQTT client '{_clientId}' reconnect task started");
        while (!_disposeCancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                await Delay();

                if (!_disposeCancellationTokenSource.IsCancellationRequested
                    && _client is not null 
                    && _clientOptions is not null)
                {
                    Log.Info($"MQTT client '{_clientId}' reconnecting");
                    await _client.StopAsync();
                    await _client.StartAsync(_clientOptions);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"MQTT client '{_clientId}' error when reconnecting");
            }
        }

        Log.Info($"MQTT client '{_clientId}' reconnect task stopped");
    }

    private async Task Delay()
    {
        try
        {
            await Task.Delay(_reconnectDelay, _disposeCancellationTokenSource.Token);
        }
        catch (TaskCanceledException)
        {
            // Do nothing
        }
    }

    private async Task OnApplicationMessageReceived(MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        var topic = eventArgs.ApplicationMessage?.Topic;
        var qualityOfServiceLevel = eventArgs.ApplicationMessage?.QualityOfServiceLevel;
        var payload = DefaultMessageEncoding.GetString(eventArgs.ApplicationMessage?.PayloadSegment ?? Array.Empty<byte>());

        Log.Info($"MQTT client '{_clientId}' received message with topic '{topic}', QoS '{qualityOfServiceLevel}', payload '{payload}'");

        try
        {
            // We acknowledge manually after our message processing ran successfully.
            // So we disable AutoAcknowledge.
            eventArgs.AutoAcknowledge = false;

            // *********************************
            // YOUR MESSAGE PROCESSING CODE HERE
            // *********************************

            await AcknowledgeMessage(eventArgs); // Acknowledge message if processing was successful
            //await NegativeAcknowledge(eventArgs); // Negative acknowledge message if an error happened during processing
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"MQTT client '{_clientId}' error when processing received message");

            Log.Info($"MQTT client '{_clientId}' sending NACK");
            eventArgs.AutoAcknowledge = true;
            eventArgs.ProcessingFailed = true;
            eventArgs.ReasonCode = MqttApplicationMessageReceivedReasonCode.UnspecifiedError;
        }
    }

    private async Task AcknowledgeMessage(MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        try
        {
            Log.Info($"MQTT client '{_clientId}' sending ACK for message");
            eventArgs.ProcessingFailed = false; // Processing succeeded

            await eventArgs.AcknowledgeAsync(_disposeCancellationTokenSource.Token);
        }
        catch (Exception ex)
        {
            // Just log any errors as the messages will still be resent
            // by the message broker if no acknowledge occurs.
            Log.Error(ex, $"MQTT client '{_clientId}' error when sending ACK for message");
        }
    }

    private async Task NegativeAcknowledge(MqttApplicationMessageReceivedEventArgs eventArgs)
    {
        try
        {
            Log.Info($"MQTT client '{_clientId}' sending NACK for message");
            eventArgs.ProcessingFailed = true; // Processing failed!
            eventArgs.ReasonCode = MqttApplicationMessageReceivedReasonCode.ImplementationSpecificError;

            await eventArgs.AcknowledgeAsync(_disposeCancellationTokenSource.Token);
        }
        catch (Exception ex)
        {
            // Just log any errors as the messages will still be resent
            // by the message broker if no acknowledge occurs.
            Log.Error(ex, $"MQTT client '{_clientId}' error when sending NACK for message");
        }
    }

    private Task OnConnected(MqttClientConnectedEventArgs eventArgs)
    {
        Log.Info($"MQTT client '{_clientId}' connected to '{_server}:{_port}'");
        if (eventArgs.ConnectResult.IsSessionPresent)
        {
            Log.Info($"MQTT client '{_clientId}' session is present.");
        }
        else
        {
            Log.Warning($"MQTT client '{_clientId}' session not present. This is OK if a new client id is used for the first time. Otherwise there is the risk of loosing messages.");
        }

        return Task.CompletedTask;
    }

    private Task OnDisconnected(MqttClientDisconnectedEventArgs eventArgs)
    {
        if (eventArgs.Reason == MqttClientDisconnectReason.NormalDisconnection)
        {
            Log.Info($"MQTT client '{_clientId}' disconnected from '{_server}:{_port}'");
        }
        else
        {
            Log.Warning(
                eventArgs.Exception,
                $"MQTT client '{_clientId}' disconnected from '{_server}:{_port}' with reason '{eventArgs.Reason}'");
        }

        return Task.CompletedTask;
    }

    private Task OnSynchronizingSubscriptionsFailed(ManagedProcessFailedEventArgs eventArgs)
    {
        Log.Error(
            eventArgs.Exception,
            $"MQTT client '{_clientId}' synchronizing subscriptions failed against '{_server}:{_port}'");
        return Task.CompletedTask;
    }

    private Task OnConnectingFailed(ConnectingFailedEventArgs eventArgs)
    {
        Log.Warning(
            eventArgs.Exception,
            $"MQTT client '{_clientId}' connection to '{_server}:{_port}' failed with result code '{eventArgs.ConnectResult?.ResultCode}'");
        return Task.CompletedTask;
    }

    private static ManagedMqttClientOptions CreateManagedMqttClientOptions(
        string server,
        int port,
        string clientId,
        TimeSpan autoReconnectDelay)
    {
        Log.Info($"MQTT client '{clientId}' will connect to '{server}:{port}'");

        // Use this for authentication with certificates
        //var caCertificate = new X509Certificate2(certificateAuthorityCertificatePath);
        //var clientCertificate = new X509Certificate2(clientCertificatePath, clientCertificatePassword);

        var clientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(server, port)
            .WithClientId(clientId)
            .WithProtocolVersion(MqttProtocolVersion.V311) // We are using protocol V3.11 because we didn't receive stored messages from the broker with V5
            .WithCleanSession(false) // Use persistent sessions
            // Use this for authentication with certificates
            //.WithTlsOptions(options =>
            //{
            //    options.WithClientCertificates(new List<X509Certificate2> { caCertificate, clientCertificate });
            //    options.UseTls(true);
            //    options.WithSslProtocols(SslProtocols.Tls13);
            //    options.WithAllowUntrustedCertificates(true); // For self-signed certificates
            //    options.WithCertificateValidationHandler(e => true); // For self-signed certificates
            //})
            .Build();

        return new ManagedMqttClientOptionsBuilder()
                .WithAutoReconnectDelay(autoReconnectDelay)
                .WithMaxPendingMessages(1000)
                .WithClientOptions(clientOptions)
                .Build();
    }

    private ICollection<MqttTopicFilter> CreateTopicFilters(string clientId, IEnumerable<string> topics)
    {
        var topicFilters = new List<MqttTopicFilter>();
        foreach (var topic in topics)
        {
            var topicFilter = new MqttTopicFilterBuilder()
                .WithTopic(topic)
                .WithQualityOfServiceLevel(DefaultMqttQualityOfServiceLevel)
                .Build();

            if (topicFilter != null)
            {
                Log.Info($"MQTT client '{clientId}' will subscribe to topic '{topicFilter.Topic}' with QoS '{topicFilter.QualityOfServiceLevel}'");
                topicFilters.Add(topicFilter);
            }
        }

        return topicFilters;
    }
}

