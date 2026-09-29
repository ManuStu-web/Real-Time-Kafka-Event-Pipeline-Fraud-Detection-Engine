const express = require('express');
const cors = require('cors');
const config = require('./config');
const { OrderProducer, generateMockOrder } = require('./producer');
const { storage } = require('./storage');

function createApp(producerInstance = null) {
  const app = express();
  app.use(cors());
  app.use(express.json());

  const producer = producerInstance || new OrderProducer();

  // Connect producer on first startup
  producer.connect().catch(err => {
    console.warn('[Express] Warning connecting producer:', err.message);
  });

  // Health Check
  app.get('/health', (req, res) => {
    res.json({
      status: 'UP',
      service: 'Kafka Event Pipeline API',
      timestamp: new Date().toISOString(),
      uptimeSeconds: process.uptime(),
      executionMode: config.kafka.executionMode,
      broker: config.kafka.brokers.join(',')
    });
  });

  // Ingest single order via HTTP and publish to Kafka
  app.post('/api/orders', async (req, res) => {
    try {
      const orderPayload = req.body;
      if (!orderPayload || typeof orderPayload !== 'object') {
        return res.status(400).json({ error: 'Request body must be a valid JSON object' });
      }

      // If missing orderId or timestamp, assign them
      if (!orderPayload.userId) {
        return res.status(400).json({ error: 'Field "userId" is required' });
      }

      const published = await producer.publishOrder(orderPayload);
      return res.status(202).json({
        message: 'Order published to Kafka topic',
        topic: config.kafka.topics.orders,
        orderId: published.orderId,
        userId: published.userId
      });
    } catch (err) {
      console.error('[API Error /api/orders]', err);
      return res.status(500).json({ error: 'Failed to publish order event', details: err.message });
    }
  });

  // Trigger batch generation of synthetic orders
  app.post('/api/orders/generate', async (req, res) => {
    try {
      const count = parseInt(req.body.count, 10) || 5;
      const delayMs = req.body.delayMs !== undefined ? parseInt(req.body.delayMs, 10) : 100;
      const anomalyRate = req.body.anomalyRate !== undefined ? parseFloat(req.body.anomalyRate) : 0.2;

      // Run batch asynchronously so request responds immediately
      producer.runBatch(count, delayMs, anomalyRate).catch(err => {
        console.error('[Generate Error]', err);
      });

      return res.status(202).json({
        message: `Triggered generation of ${count} order events`,
        topic: config.kafka.topics.orders,
        status: 'PROCESSING'
      });
    } catch (err) {
      return res.status(500).json({ error: 'Failed to trigger batch generation', details: err.message });
    }
  });

  // Query processed orders
  app.get('/api/orders', (req, res) => {
    const limit = parseInt(req.query.limit, 10) || 50;
    const orders = storage.getAllOrders(limit);
    res.json({
      count: orders.length,
      orders
    });
  });

  // Query specific order
  app.get('/api/orders/:id', (req, res) => {
    const order = storage.getOrderById(req.params.id);
    if (!order) {
      return res.status(404).json({ error: `Order with ID "${req.params.id}" not found` });
    }
    return res.json(order);
  });

  // Query pipeline metrics
  app.get('/api/metrics', (req, res) => {
    const metrics = storage.getMetrics();
    res.json(metrics);
  });

  // Query Dead Letter Queue
  app.get('/api/dlq', (req, res) => {
    const limit = parseInt(req.query.limit, 10) || 50;
    const dlq = storage.getDLQRecords(limit);
    res.json({
      count: dlq.length,
      records: dlq
    });
  });

  return { app, producer };
}

module.exports = {
  createApp
};
