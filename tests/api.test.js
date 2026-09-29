const request = require('supertest');
const { createApp } = require('../src/app');
const { OrderProducer } = require('../src/producer');
const { storage } = require('../src/storage');

describe('Express REST API Test Suite', () => {
  let app;
  let producer;

  beforeAll(async () => {
    storage.reset();
    producer = new OrderProducer('mock');
    await producer.connect();
    const serverInstance = createApp(producer);
    app = serverInstance.app;
  });

  afterAll(async () => {
    await producer.disconnect();
  });

  test('TC-14: GET /health returns 200 OK and service status', async () => {
    const res = await request(app).get('/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('UP');
    expect(res.body.service).toMatch(/Kafka Event Pipeline/i);
  });

  test('TC-15: POST /api/orders accepts valid order and publishes to Kafka (202 Accepted)', async () => {
    const payload = {
      userId: 'user_200',
      productName: 'Ergonomic Desk Mat',
      category: 'Office',
      quantity: 1,
      pricePerUnit: 35.0,
      totalAmount: 35.0
    };

    const res = await request(app)
      .post('/api/orders')
      .send(payload);

    expect(res.status).toBe(202);
    expect(res.body.message).toMatch(/published/i);
    expect(res.body.userId).toBe('user_200');
  });

  test('TC-16: POST /api/orders rejects missing userId with 400 Bad Request', async () => {
    const invalid = {
      productName: 'Desk Mat',
      pricePerUnit: 35.0
    };

    const res = await request(app)
      .post('/api/orders')
      .send(invalid);

    expect(res.status).toBe(400);
    expect(res.body.error).toBeDefined();
  });

  test('TC-17: GET /api/metrics returns real-time pipeline metrics', async () => {
    const res = await request(app).get('/api/metrics');
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty('totalProcessed');
    expect(res.body).toHaveProperty('totalVolumeUsd');
    expect(res.body).toHaveProperty('verifiedOrdersCount');
    expect(res.body).toHaveProperty('flaggedOrdersCount');
  });
});
