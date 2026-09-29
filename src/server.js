const config = require('./config');
const { createApp } = require('./app');
const { OrderConsumer } = require('./consumer');

async function bootstrap() {
  const { app, producer } = createApp();

  // Start background Kafka consumer
  const consumer = new OrderConsumer();
  await consumer.start().catch(err => {
    console.error('[Bootstrap] Failed to start consumer:', err);
  });

  const server = app.listen(config.port, () => {
    console.log('\n======================================================');
    console.log(` 🚀 Kafka Event Pipeline API Server running on port ${config.port}`);
    console.log(`    Mode:         ${config.kafka.executionMode.toUpperCase()}`);
    console.log(`    Brokers:      ${config.kafka.brokers.join(',')}`);
    console.log(`    Topic:        ${config.kafka.topics.orders}`);
    console.log(`    Health check: http://localhost:${config.port}/health`);
    console.log(`    Orders API:   http://localhost:${config.port}/api/orders`);
    console.log(`    Metrics API:  http://localhost:${config.port}/api/metrics`);
    console.log('======================================================\n');
  });

  const gracefulShutdown = async () => {
    console.log('\n[Server] Initiating graceful shutdown...');
    server.close(async () => {
      await producer.disconnect();
      await consumer.stop();
      console.log('[Server] Shutdown complete.');
      process.exit(0);
    });
  };

  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
}

if (require.main === module) {
  bootstrap().catch(err => {
    console.error('[Bootstrap Fatal Error]', err);
    process.exit(1);
  });
}

module.exports = { bootstrap };
