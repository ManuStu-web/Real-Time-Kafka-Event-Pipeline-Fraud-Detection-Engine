const { v4: uuidv4 } = require('uuid');
const config = require('./config');
const { getKafkaClients } = require('./kafkaClient');

const SAMPLE_USERS = Array.from({ length: 10 }, (_, i) => `user_${100 + i}`);
const SAMPLE_PRODUCTS = [
  { name: 'Wireless Noise-Canceling Headphones', category: 'Electronics', basePrice: 199.99 },
  { name: 'Mechanical Gaming Keyboard', category: 'Electronics', basePrice: 89.99 },
  { name: 'Organic Arabica Coffee Beans', category: 'Grocery', basePrice: 18.50 },
  { name: 'Ergonomic Office Chair', category: 'Office', basePrice: 280.00 },
  { name: 'Designer Leather Handbag', category: 'Luxury', basePrice: 1450.00 },
  { name: '4K Ultra-HD Smart TV 65"', category: 'Electronics', basePrice: 750.00 },
  { name: 'High-End Swiss Chronograph Watch', category: 'Luxury', basePrice: 3200.00 },
  { name: 'Stainless Steel Water Bottle', category: 'Home', basePrice: 24.99 },
  { name: 'Distributed Systems with Kafka Handbook', category: 'Books', basePrice: 45.00 },
  { name: 'Flagship Smartphone 5G', category: 'Electronics', basePrice: 1199.00 }
];

const SAMPLE_IPS = [
  '192.168.1.45', '10.0.0.12', '172.16.0.88',
  '203.0.113.195', '198.51.100.4', '198.51.100.99'
];

function generateMockOrder(forceHighValue = false) {
  const user = SAMPLE_USERS[Math.floor(Math.random() * SAMPLE_USERS.length)];
  const prod = SAMPLE_PRODUCTS[Math.floor(Math.random() * SAMPLE_PRODUCTS.length)];
  const quantity = Math.floor(Math.random() * 3) + 1;

  let pricePerUnit;
  if (forceHighValue) {
    pricePerUnit = Math.round((Math.random() * 3000 + 1200) * 100) / 100;
  } else {
    // Normal small price fluctuation (+-10%)
    const factor = 0.9 + Math.random() * 0.2;
    pricePerUnit = Math.round(prod.basePrice * factor * 100) / 100;
  }

  const totalAmount = Math.round(pricePerUnit * quantity * 100) / 100;

  return {
    orderId: uuidv4(),
    userId: user,
    productName: prod.name,
    category: prod.category,
    quantity,
    pricePerUnit,
    totalAmount,
    currency: 'USD',
    ipAddress: SAMPLE_IPS[Math.floor(Math.random() * SAMPLE_IPS.length)],
    timestamp: new Date().toISOString()
  };
}

class OrderProducer {
  constructor(mode = config.kafka.executionMode) {
    this.mode = mode;
    this.client = getKafkaClients(mode);
    this.producer = this.client.createProducer();
    this.topic = config.kafka.topics.orders;
    this.isConnected = false;
  }

  async connect() {
    if (!this.isConnected) {
      console.log(`[Producer] Connecting to Kafka broker (${this.mode} mode)...`);
      await this.producer.connect();
      this.isConnected = true;
      console.log('[Producer] Connected successfully.');
    }
  }

  async disconnect() {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('[Producer] Disconnected.');
    }
  }

  async publishOrder(orderPayload) {
    if (!this.isConnected) {
      await this.connect();
    }

    const key = String(orderPayload.userId);
    const value = JSON.stringify(orderPayload);

    // Key-based message partition routing ensures per-user FIFO order
    const result = await this.producer.send({
      topic: this.topic,
      messages: [
        {
          key,
          value,
          headers: {
            source: 'order-api',
            producedAt: new Date().toISOString()
          }
        }
      ]
    });

    return {
      orderId: orderPayload.orderId,
      userId: key,
      totalAmount: orderPayload.totalAmount,
      topic: this.topic,
      result
    };
  }

  async runBatch(count = 10, delayMs = config.businessRules.producerDelayMs, anomalyRate = 0.3) {
    await this.connect();
    console.log(`[Producer] Publishing stream of ${count} orders to "${this.topic}"...`);
    const published = [];

    for (let i = 1; i <= count; i++) {
      const isAnomaly = Math.random() < anomalyRate;
      const order = generateMockOrder(isAnomaly);

      await this.publishOrder(order);
      published.push(order);

      const tag = order.totalAmount >= config.businessRules.fraudThresholdAmount ? ' [HIGH-VALUE ALERT]' : '';
      console.log(
        `[Producer] [${i}/${count}] Order ${order.orderId.substring(0, 8)}... ` +
        `| User: ${order.userId.padEnd(9)} | $${order.totalAmount.toFixed(2).padStart(8)} ` +
        `| ${order.productName.substring(0, 25).padEnd(25)}${tag}`
      );

      if (i < count && delayMs > 0) {
        await new Promise(res => setTimeout(res, delayMs));
      }
    }

    return published;
  }
}

// Standalone execution entry point
if (require.main === module) {
  const args = process.argv.slice(2);
  const countArg = args.find(a => a.startsWith('--count='));
  const count = countArg ? parseInt(countArg.split('=')[1], 10) : 10;

  const modeArg = args.find(a => a.startsWith('--mode='));
  const mode = modeArg ? modeArg.split('=')[1] : config.kafka.executionMode;

  const producer = new OrderProducer(mode);
  producer.runBatch(count)
    .then(async () => {
      await producer.disconnect();
      process.exit(0);
    })
    .catch(err => {
      console.error('[Producer Error]', err);
      process.exit(1);
    });
}

module.exports = {
  OrderProducer,
  generateMockOrder
};
