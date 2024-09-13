using Kafka.BackgroundServices;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<KafkaConsumerService>();
builder.Services.AddControllers();


var app = builder.Build();
app.MapGet("/", () => "Hello World!");
app.MapControllers();

app.Run();
