const fs = require('fs');
const path = require('path');
const config = require('./config');

class StorageManager {
  constructor() {
    this.orders = new Map(); // orderId -> Order object (in-memory fast index)
    this.dlqRecords = [];
    this.dataDir = config.storage.dataDir;
    this.auditFilePath = path.join(this.dataDir, 'processed_orders.jsonl');

    this._ensureDir();
  }

  _ensureDir() {
    try {
      if (!fs.existsSync(this.dataDir)) {
        fs.mkdirSync(this.dataDir, { recursive: true });
      }
    } catch (err) {
      // In-memory fallback if fs is constrained
    }
  }

  saveProcessedOrder(order) {
    // Idempotent insertion by orderId (prevents duplicate processing in at-least-once delivery)
    this.orders.set(order.orderId, { ...order });

    // Append to audit log file asynchronously
    try {
      fs.appendFileSync(this.auditFilePath, JSON.stringify(order) + '\n', 'utf-8');
    } catch (err) {
      // Ignore fs write errors in ephemeral environments
    }

    return order;
  }

  saveDLQRecord(dlqEntry) {
    const entry = {
      id: this.dlqRecords.length + 1,
      ...dlqEntry,
      failedAt: dlqEntry.failedAt || new Date().toISOString()
    };
    this.dlqRecords.push(entry);
    return entry;
  }

  getOrderById(orderId) {
    return this.orders.get(orderId) || null;
  }

  getAllOrders(limit = 100) {
    const list = Array.from(this.orders.values());
    return list.slice(-limit).reverse();
  }

  getDLQRecords(limit = 50) {
    return this.dlqRecords.slice(-limit).reverse();
  }

  getMetrics() {
    const ordersList = Array.from(this.orders.values());
    const totalProcessed = ordersList.length;
    const totalVolumeUsd = ordersList.reduce((sum, o) => sum + (Number(o.totalAmount) || 0), 0);
    const flaggedOrdersCount = ordersList.filter(o => o.status && o.status.startsWith('FLAGGED')).length;
    const verifiedOrdersCount = ordersList.filter(o => o.status === 'VERIFIED').length;
    const reviewOrdersCount = ordersList.filter(o => o.status === 'REQUIRES_REVIEW').length;
    const dlqCount = this.dlqRecords.length;
    const averageOrderValue = totalProcessed > 0 ? totalVolumeUsd / totalProcessed : 0;

    return {
      totalProcessed,
      totalVolumeUsd: Math.round(totalVolumeUsd * 100) / 100,
      averageOrderValue: Math.round(averageOrderValue * 100) / 100,
      verifiedOrdersCount,
      flaggedOrdersCount,
      reviewOrdersCount,
      dlqCount
    };
  }

  reset() {
    this.orders.clear();
    this.dlqRecords = [];
  }
}

// Singleton storage instance
const storage = new StorageManager();

module.exports = {
  StorageManager,
  storage
};
