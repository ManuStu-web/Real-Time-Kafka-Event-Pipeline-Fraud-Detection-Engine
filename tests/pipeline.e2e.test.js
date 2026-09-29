const { OrderProducer } = require('../src/producer');
const { OrderConsumer } = require('../src/consumer');
const { storage } = require('../src/storage');

describe('End-to-End Event Stream Pipeline Integration Test', () => {
  let producer;
  let consumer;

  beforeAll(async () => {
    storage.reset();
    consumer = new OrderConsumer('mock', 'e2e-test-group');
    await consumer.start();

    producer = new OrderProducer('mock');
    await producer.connect();
  });

  afterAll(async () => {
    await producer.disconnect();
    await consumer.stop();
  });

  test('TC-18: End-to-end stream: batch of orders produced, consumed, and persisted with correct metrics', async () => {
    const BATCH_SIZE = 5;

    // Produce batch
    const produced = await producer.runBatch(BATCH_SIZE, 10, 0.4);
    expect(produced.length).toBe(BATCH_SIZE);

    // Wait a brief tick for async event processing
    await new Promise(resolve => setTimeout(resolve, 300));

    const metrics = storage.getMetrics();
    expect(metrics.totalProcessed).toBe(BATCH_SIZE);
    expect(metrics.totalVolumeUsd).toBeGreaterThan(0);
    expect(metrics.verifiedOrdersCount + metrics.flaggedOrdersCount).toBe(BATCH_SIZE);

    // Verify all orders exist in storage
    for (const order of produced) {
      const stored = storage.getOrderById(order.orderId);
      expect(stored).toBeDefined();
      expect(stored.userId).toBe(order.userId);
      expect(stored.status).toBeDefined();
    }
  });
});
