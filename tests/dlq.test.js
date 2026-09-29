const { OrderConsumer } = require('../src/consumer');
const { storage } = require('../src/storage');

describe('Dead Letter Queue (DLQ) & Resilience Test Suite', () => {
  let consumer;

  beforeEach(() => {
    storage.reset();
    consumer = new OrderConsumer('mock', 'test-dlq-group');
  });

  test('TC-12: Unparseable non-JSON raw message is saved to DLQ without crashing', async () => {
    const malformedMessage = {
      value: Buffer.from('{ corrupt-non-json-message-data :::'),
      offset: 12
    };

    const result = await consumer.handleMessage({
      topic: 'order_events',
      partition: 1,
      message: malformedMessage
    });

    expect(result.status).toBe('DLQ_SAVED');
    expect(result.dlqEntry).toBeDefined();
    expect(result.dlqEntry.errorMessage).toMatch(/json/i);

    const metrics = storage.getMetrics();
    expect(metrics.dlqCount).toBe(1);
    expect(metrics.totalProcessed).toBe(0);
  });

  test('TC-13: Semantically invalid order payload is saved to DLQ', async () => {
    const invalidMessage = {
      value: Buffer.from(JSON.stringify({
        productName: 'Missing UserId and Price',
        category: 'Test'
      })),
      offset: 14
    };

    const result = await consumer.handleMessage({
      topic: 'order_events',
      partition: 0,
      message: invalidMessage
    });

    expect(result.status).toBe('DLQ_SAVED');
    expect(result.dlqEntry.errorMessage).toMatch(/userId/i);

    const dlqRecords = storage.getDLQRecords();
    expect(dlqRecords.length).toBe(1);
  });
});
