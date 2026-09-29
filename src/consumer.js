const config = require('./config');
const { getKafkaClients } = require('./kafkaClient');
const { EventProcessor } = require('./processor');
const { storage } = require('./storage');

class OrderConsumer {
  constructor(mode = config.kafka.executionMode, groupId = config.kafka.consumerGroup) {
    this.mode = mode;
    this.groupId = groupId;
    this.client = getKafkaClients(mode);
    this.consumer = this.client.createConsumer(this.groupId);
    this.processor = new EventProcessor();
    this.ordersTopic = config.kafka.topics.orders;
    this.dlqTopic = config.kafka.topics.dlq;
    this.isRunning = false;
    this.processedCount = 0;
  }

  async start() {
    if (this.isRunning) return;

    console.log(`[Consumer] Connecting to Kafka (Group: "${this.groupId}", Topic: "${this.ordersTopic}")...`);
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.ordersTopic, fromBeginning: true });

    this.isRunning = true;
    console.log('[Consumer] Started and listening for order events.');

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleMessage({ topic, partition, message });
      }
    });
  }

  async handleMessage({ topic, partition, message }) {
    const rawString = message.value ? message.value.toString() : '';
    const offset = message.offset !== undefined ? message.offset : 0;
    let parsedPayload;

    try {
      parsedPayload = JSON.parse(rawString);
    } catch (err) {
      // Malformed JSON -> Route immediately to Dead Letter Queue
      const dlqEntry = storage.saveDLQRecord({
        rawPayload: rawString,
        errorMessage: `JSON parse error: ${err.message}`,
        topic,
        partition,
        offset
      });
      console.warn(`[Consumer] [DLQ] Captured unparseable message at partition ${partition}, offset ${offset}`);
      return { status: 'DLQ_SAVED', dlqEntry };
    }

    // Process order through business rules and risk evaluation
    const result = this.processor.processOrder(parsedPayload);

    if (!result.success) {
      // Validation failure -> Route to Dead Letter Queue
      const dlqEntry = storage.saveDLQRecord({
        rawPayload: JSON.stringify(parsedPayload),
        errorMessage: result.error,
        topic,
        partition,
        offset
      });
      console.warn(`[Consumer] [DLQ] Validation failed for order: ${result.error}`);
      return { status: 'DLQ_SAVED', dlqEntry };
    }

    // Idempotent write to persistent/storage store
    const saved = storage.saveProcessedOrder(result.processedOrder);
    this.processedCount++;

    const statusTag = saved.status.startsWith('FLAGGED') ? '[ALERT]' : '[OK]';
    console.log(
      `[Consumer] ${statusTag} [${saved.status}] Order: ${saved.orderId.substring(0, 8)}... ` +
      `| User: ${saved.userId} | $${saved.totalAmount.toFixed(2)} ` +
      `| Risk: ${saved.riskScore.toFixed(2)} | Part: ${partition} Off: ${offset}`
    );

    return { status: 'PROCESSED', order: saved };
  }

  async stop() {
    if (this.isRunning) {
      await this.consumer.disconnect();
      this.isRunning = false;
      console.log('[Consumer] Disconnected gracefully.');
    }
  }
}

// Standalone execution entry point
if (require.main === module) {
  const modeArg = process.argv.find(a => a.startsWith('--mode='));
  const mode = modeArg ? modeArg.split('=')[1] : config.kafka.executionMode;

  const consumer = new OrderConsumer(mode);
  consumer.start().catch(err => {
    console.error('[Consumer Error]', err);
    process.exit(1);
  });

  const shutdown = async () => {
    console.log('\n[Consumer] Shutting down...');
    await consumer.stop();
    const metrics = storage.getMetrics();
    console.log('=== Final Metrics ===', metrics);
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = {
  OrderConsumer
};
