using System;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Monitor.Ingestion;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace FunctionEventhubDispatcher
{
    public class EventHubDispatcher
    {
        private readonly ILogger<EventHubDispatcher> _logger;

        public EventHubDispatcher(ILogger<EventHubDispatcher> logger)
        {
            _logger = logger;
        }

        [Function(nameof(EventHubDispatcher))]
        public async Task Run([EventHubTrigger("powerplatformhub", Connection = "eventHub")] EventData[] events)
        {
            // Initialize variables
            var endpoint = new Uri("AzureMonitorUri");
            var ruleId = "dcr-ruleId";
            var streamName = "Custom-StreamName";

            // Create credential and client
            var credential = new DefaultAzureCredential();
            LogsIngestionClient client = new(endpoint, credential);

            DateTimeOffset currentTime = DateTimeOffset.UtcNow;

            foreach (EventData @event in events)
            {
                _logger.LogInformation("Event Body: {body}", @event.Body);

                _logger.LogInformation("Event Content-Type: {contentType}", @event.ContentType);
                try
                {
                    var jsonString = @event.EventBody.ToString();
                    var trace = JsonSerializer.Deserialize<EventTrace>(jsonString);
                    BinaryData data = BinaryData.FromObjectAsJson(
                    new[] {
                        new
                        {
                            Element = trace.Element,
                            SessionId = trace.SessionId,
                            UserId = trace.UserId,
                            UserName = trace.UserName,
                            UserEmail = trace.UserEmail,
                            UserTimestamp = trace.UserTimestamp,
                            ClientIP = trace.ClientIP,
                            Trace = trace.Trace,
                            
                        }
                    });

                    var response = await client.UploadAsync(ruleId, streamName, RequestContent.Create(data)).ConfigureAwait(false);
                    if (response.IsError)
                    {
                        throw new Exception(response.ToString());
                    }

                    Console.WriteLine("Log upload completed using content upload");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Upload failed with Exception: " + ex.Message);
                }
            }
        }
    } 

    public class EventTrace
    {
        public string Element { get; set; }
        public string SessionId { get; set; }
        public string UserId { get; set; }
        public string UserName { get; set; }
        public string UserEmail { get; set; }
        public string UserTimestamp { get; set; }
        public string ClientIP { get; set; }
        public string Trace { get; set; }
    }
}
