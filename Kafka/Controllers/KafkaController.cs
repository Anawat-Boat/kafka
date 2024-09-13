using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Controllers
{
    [Route("api/[controller]/producer")]
    [ApiController]
    public class KafkaController : ControllerBase
    {
        private readonly ProducerConfig _config;
        int a;

        public KafkaController()
        {
            _config = new ProducerConfig { BootstrapServers = "10.112.86.164:9092,10.112.86.165:9092,10.112.86.166:9092" };

        }

        [HttpPost("send")]
        public async Task<IActionResult> SendMessage([FromBody] string message)
        {
            using (var producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                try
                {
                    a = a + 1;
                    var result = await producer.ProduceAsync("re-calculate", new Message<Null, string> { Value = message + a });
                    return Ok($"Message sent to {result.TopicPartitionOffset}");
                }
                catch (ProduceException<Null, string> ex)
                {
                    return StatusCode(500, $"Error producing message: {ex.Error.Reason}");
                }
            }
        }
    }
}