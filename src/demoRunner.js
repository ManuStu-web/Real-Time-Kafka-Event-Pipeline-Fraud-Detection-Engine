const config = require('./config');
const { OrderProducer } = require('./producer');
const { OrderConsumer } = require('./consumer');
const { storage } = require('./storage');

async function runLiveDemo(count = 12) {
  console.log('\n' + '='.repeat(70));
  console.log(' >>> KAFKA DISTRIBUTED EVENT PIPELINE: LIVE CLASSROOM DEMO <<< ');
  console.log('='.repeat(70));
  console.log(` Tech Stack:     Node.js + Express + Apache Kafka + KafkaJS`);
  console.log(` Topic:          ${config.kafka.topics.orders}`);
  console.log(` Consumer Group: ${config.kafka.consumerGroup}`);
  console.log(` Events to Send: ${count}`);
  console.log(` Anomaly Rate:   30% (orders exceeding $1,000 fraud threshold)`);
  console.log('='.repeat(70) + '\n');

  storage.reset();

  const consumer = new OrderConsumer('mock');
  await consumer.start();

  const producer = new OrderProducer('mock');
  await producer.connect();

  console.log('[Demo] Producer and Consumer connected. Beginning event stream...\n');

  // Stream orders
  await producer.runBatch(count, 350, 0.35);

  // Allow short buffer for final consumer processing
  await new Promise(res => setTimeout(res, 500));

  const metrics = storage.getMetrics();
  const recentOrders = storage.getAllOrders(8);

  console.log('\n' + '='.repeat(70));
  console.log(' [*] PIPELINE REAL-TIME ANALYTICS DASHBOARD');
  console.log('='.repeat(70));
  console.log(` Total Orders Processed : ${metrics.totalProcessed}`);
  console.log(` Total Transaction Vol  : $${metrics.totalVolumeUsd.toLocaleString('en-US', { minimumFractionDigits: 2 })}`);
  console.log(` Average Order Amount   : $${metrics.averageOrderValue.toFixed(2)}`);
  console.log(` Verified Normal Orders : ${metrics.verifiedOrdersCount}`);
  console.log(` Flagged Suspicious     : ${metrics.flaggedOrdersCount}`);
  console.log(` Review Required        : ${metrics.reviewOrdersCount}`);
  console.log(` Dead Letter Queue (DLQ): ${metrics.dlqCount}`);
  console.log('-'.repeat(70));
  console.log(' Sample Processed Orders in Persistent Store:');
  console.log(` ${'ORDER ID'.padEnd(12)} ${'USER'.padEnd(10)} ${'AMOUNT'.padEnd(10)} ${'STATUS'.padEnd(20)} FLAGS`);
  console.log('-'.repeat(70));

  for (const o of recentOrders) {
    const flagsStr = o.flags && o.flags.length > 0 ? o.flags.join(', ') : 'None';
    console.log(
      ` ${o.orderId.substring(0, 8)}...   ${o.userId.padEnd(10)} $${o.totalAmount.toFixed(2).padEnd(9)} ${o.status.padEnd(20)} ${flagsStr}`
    );
  }
  console.log('='.repeat(70) + '\n');

  await producer.disconnect();
  await consumer.stop();
  process.exit(0);
}

if (require.main === module) {
  runLiveDemo().catch(err => {
    console.error('Demo error:', err);
    process.exit(1);
  });
}

module.exports = { runLiveDemo };
