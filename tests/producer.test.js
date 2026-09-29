const { OrderProducer, generateMockOrder } = require('../src/producer');

describe('Kafka Producer & Event Generator Test Suite', () => {
  let producer;

  beforeAll(async () => {
    producer = new OrderProducer('mock');
    await producer.connect();
  });

  afterAll(async () => {
    await producer.disconnect();
  });

  test('TC-9: generateMockOrder creates structured, valid event object', () => {
    const order = generateMockOrder(false);
    expect(order.orderId).toBeDefined();
    expect(order.userId).toMatch(/^user_\d+$/);
    expect(order.totalAmount).toBeGreaterThan(0);
    expect(order.quantity).toBeGreaterThanOrEqual(1);
    expect(order.currency).toBe('USD');
  });

  test('TC-10: generateMockOrder with forceHighValue produces order >= $1000', () => {
    const order = generateMockOrder(true);
    expect(order.totalAmount).toBeGreaterThanOrEqual(1000.0);
  });

  test('TC-11: publishOrder dispatches message using userId as partition key', async () => {
    const sample = generateMockOrder(false);
    const result = await producer.publishOrder(sample);

    expect(result.orderId).toBe(sample.orderId);
    expect(result.userId).toBe(sample.userId);
    expect(result.topic).toBe('order_events');
    expect(result.result).toBeDefined();
  });
});
