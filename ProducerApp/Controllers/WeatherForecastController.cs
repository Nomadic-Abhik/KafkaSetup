using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace ProducerApp.Controllers
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
        public async Task<IEnumerable<WeatherForecast>> Get()
        {
            WeatherForecast model = new WeatherForecast
            {
                Date = DateTime.Now,
                TemperatureC = 32,
                Summary = "This is today Weather"
            };
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                while (model!= null)
                {
                    var response = await producer.ProduceAsync("Weather-topic",
                        new Message<Null, string> { Value = JsonConvert.SerializeObject(model)});
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
