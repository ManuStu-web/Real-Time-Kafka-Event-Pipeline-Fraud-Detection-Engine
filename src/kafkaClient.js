const { Kafka } = require('kafkajs');
const config = require('./config');

// In-Memory Kafka simulation for local testing, CI/CD, and environments without Docker
class MockKafkaCluster {
  constructor() {
    this.topics = new Map(); // topic -> Array<{ partition, offset, key, value, timestamp }>
    this.subscribers = new Map(); // topic -> Array<handler>
  }

  reset() {
    this.topics.clear();
    this.subscribers.clear();
  }

  async send(topic, messages) {
    if (!this.topics.has(topic)) {
      this.topics.set(topic, []);
    }
    const topicQueue = this.topics.get(topic);
    const sentRecords = [];

    for (const msg of messages) {
      // Deterministic partition by hashing key if present
      const partition = msg.key ? Math.abs(this._hashString(msg.key.toString())) % 3 : 0;
      const offset = topicQueue.length;
      const record = {
        topic,
        partition,
        offset,
        key: Buffer.isBuffer(msg.key) ? msg.key : Buffer.from(String(msg.key || '')),
        value: Buffer.isBuffer(msg.value) ? msg.value : Buffer.from(String(msg.value)),
        timestamp: String(Date.now())
      };
      topicQueue.push(record);
      sentRecords.push(record);

      // Notify active consumers asynchronously
      const handlers = this.subscribers.get(topic) || [];
      for (const handler of handlers) {
        setImmediate(() => {
          handler({
            topic,
            partition,
            message: record
          }).catch(err => {
            console.error('[MockConsumer Error]', err);
          });
        });
      }
    }
    return sentRecords;
  }

  subscribe(topic, handler) {
    if (!this.subscribers.has(topic)) {
      this.subscribers.set(topic, []);
    }
    this.subscribers.get(topic).push(handler);
  }

  _hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0;
    }
    return hash;
  }
}

const mockCluster = new MockKafkaCluster();

class MockProducer {
  constructor() {
    this.isConnected = false;
  }
  async connect() {
    this.isConnected = true;
  }
  async send({ topic, messages }) {
    return mockCluster.send(topic, messages);
  }
  async disconnect() {
    this.isConnected = false;
  }
}

class MockConsumer {
  constructor(groupId) {
    this.groupId = groupId;
    this.subscribedTopics = [];
    this.isRunning = false;
  }
  async connect() {
    this.isRunning = true;
  }
  async subscribe({ topic, fromBeginning }) {
    this.subscribedTopics.push(topic);
  }
  async run({ eachMessage }) {
    for (const topic of this.subscribedTopics) {
      mockCluster.subscribe(topic, eachMessage);
    }
  }
  async disconnect() {
    this.isRunning = false;
  }
}

function getKafkaClients(mode = config.kafka.executionMode) {
  if (mode === 'kafka') {
    const kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      retry: {
        initialRetryTime: 300,
        retries: 5
      }
    });

    return {
      type: 'real',
      createProducer: () => kafka.producer(),
      createConsumer: (groupId = config.kafka.consumerGroup) => kafka.consumer({ groupId }),
      kafka
    };
  } else {
    return {
      type: 'mock',
      createProducer: () => new MockProducer(),
      createConsumer: (groupId = config.kafka.consumerGroup) => new MockConsumer(groupId),
      mockCluster
    };
  }
}

module.exports = {
  getKafkaClients,
  mockCluster
};
