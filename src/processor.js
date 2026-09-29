const { v4: uuidv4 } = require('uuid');
const config = require('./config');

class EventProcessor {
  constructor(fraudThreshold = config.businessRules.fraudThresholdAmount) {
    this.fraudThreshold = fraudThreshold;
  }

  validateOrder(payload) {
    if (!payload || typeof payload !== 'object') {
      return { isValid: false, error: 'Payload must be a non-null object' };
    }

    const { userId, productName, category, quantity, pricePerUnit, totalAmount } = payload;

    if (!userId || typeof userId !== 'string' || userId.trim() === '') {
      return { isValid: false, error: 'Field "userId" is required and must be a non-empty string' };
    }

    if (!productName || typeof productName !== 'string' || productName.trim() === '') {
      return { isValid: false, error: 'Field "productName" is required and must be a non-empty string' };
    }

    if (!category || typeof category !== 'string' || category.trim() === '') {
      return { isValid: false, error: 'Field "category" is required and must be a non-empty string' };
    }

    const parsedQty = Number(quantity);
    if (isNaN(parsedQty) || parsedQty <= 0 || !Number.isInteger(parsedQty)) {
      return { isValid: false, error: 'Field "quantity" must be a positive integer greater than 0' };
    }

    const parsedPrice = Number(pricePerUnit);
    if (isNaN(parsedPrice) || parsedPrice <= 0) {
      return { isValid: false, error: 'Field "pricePerUnit" must be a positive number greater than 0' };
    }

    const parsedTotal = totalAmount !== undefined ? Number(totalAmount) : parsedQty * parsedPrice;
    if (isNaN(parsedTotal) || parsedTotal <= 0) {
      return { isValid: false, error: 'Field "totalAmount" must be a positive number greater than 0' };
    }

    const sanitized = {
      orderId: payload.orderId || uuidv4(),
      userId: userId.trim(),
      productName: productName.trim(),
      category: category.trim(),
      quantity: parsedQty,
      pricePerUnit: Math.round(parsedPrice * 100) / 100,
      totalAmount: Math.round(parsedTotal * 100) / 100,
      currency: payload.currency || 'USD',
      ipAddress: payload.ipAddress || '127.0.0.1',
      timestamp: payload.timestamp || new Date().toISOString()
    };

    return { isValid: true, sanitized };
  }

  processOrder(rawPayload) {
    const validation = this.validateOrder(rawPayload);
    if (!validation.isValid) {
      return {
        success: false,
        error: validation.error,
        rawPayload
      };
    }

    const order = validation.sanitized;
    const flags = [];
    let riskScore = 0.0;

    // Rule 1: High Transaction Value
    if (order.totalAmount >= this.fraudThreshold) {
      flags.push(`AMOUNT_EXCEEDS_THRESHOLD_$${this.fraudThreshold}`);
      riskScore += 0.65;
    }

    // Rule 2: Luxury Category Spike
    if (order.category.toLowerCase() === 'luxury') {
      flags.push('HIGH_RISK_CATEGORY_LUXURY');
      riskScore += 0.20;
    }

    // Rule 3: Bulk Quantity Purchase
    if (order.quantity >= 3) {
      flags.push('BULK_QUANTITY_PURCHASE');
      riskScore += 0.15;
    }

    riskScore = Math.min(Math.round(riskScore * 100) / 100, 1.0);

    let status = 'VERIFIED';
    if (riskScore >= 0.60) {
      status = 'FLAGGED_HIGH_VALUE';
    } else if (riskScore >= 0.30) {
      status = 'REQUIRES_REVIEW';
    }

    const processedOrder = {
      ...order,
      processedAt: new Date().toISOString(),
      status,
      riskScore,
      flags
    };

    return {
      success: true,
      processedOrder
    };
  }
}

module.exports = {
  EventProcessor
};
