const path = require('path');
require('dotenv').config();

const config = {
  env: process.env.NODE_ENV || 'development',
  port: parseInt(process.env.PORT, 10) || 3000,

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'order-pipeline-service',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map(s => s.trim()),
    topics: {
      orders: process.env.KAFKA_TOPIC_ORDERS || 'order_events',
      dlq: process.env.KAFKA_TOPIC_DLQ || 'order_dlq'
    },
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || 'order-processing-group',
    executionMode: (process.env.EXECUTION_MODE || 'mock').toLowerCase(), // 'kafka' or 'mock'
    ssl: process.env.KAFKA_SSL === 'true' || Boolean(process.env.KAFKA_SASL_USERNAME),
    sasl: process.env.KAFKA_SASL_USERNAME ? {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256', // 'plain' or 'scram-sha-256'
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD
    } : null
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
