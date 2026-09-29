const path = require('path');
require('dotenv').config();

const config = {
  env: process.env.NODE_ENV || 'development',
  port: parseInt(process.env.PORT, 10) || 3000,

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'order-pipeline-service',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    topics: {
      orders: process.env.KAFKA_TOPIC_ORDERS || 'order_events',
      dlq: process.env.KAFKA_TOPIC_DLQ || 'order_dlq'
    },
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || 'order-processing-group',
    executionMode: (process.env.EXECUTION_MODE || 'mock').toLowerCase() // 'kafka' or 'mock'
  },

  businessRules: {
    fraudThresholdAmount: parseFloat(process.env.FRAUD_THRESHOLD_AMOUNT) || 1000.0,
    producerDelayMs: parseInt(process.env.PRODUCER_DELAY_MS, 10) || 400
  },

  storage: {
    dataDir: path.resolve(__dirname, '../data')
  }
};

module.exports = config;
