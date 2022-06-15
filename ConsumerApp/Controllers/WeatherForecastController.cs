using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Threading;
using Newtonsoft.Json;

namespace ConsumerApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IEnumerable<WeatherForecast> Get()
        {
            var config = new ConsumerConfig
            {
                GroupId = "weather-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe("Weather-topic");
            CancellationTokenSource token = new();
            try
            {
                while(true)
                {
                    var response = consumer.Consume(token.Token);
                    if(response.Message != null)
                    {
                        var weather = JsonConvert.DeserializeObject<WeatherForecast>(response.Message.Value);
                        Console.WriteLine(weather);
                    }
                }
                return null;
            }
            catch(Exception ex)
            {
                return null;
            }
        }
    }
}
