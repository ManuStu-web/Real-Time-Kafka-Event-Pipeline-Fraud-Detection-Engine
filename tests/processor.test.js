const { EventProcessor } = require('../src/processor');

describe('Event Processor & Fraud Detection Test Suite', () => {
  let processor;

  beforeEach(() => {
    processor = new EventProcessor(1000.0);
  });

  test('TC-5: Low-value normal orders receive VERIFIED status and low risk score', () => {
    const raw = {
      userId: 'user_101',
      productName: 'Coffee Beans',
      category: 'Grocery',
      quantity: 1,
      pricePerUnit: 20.0,
      totalAmount: 20.0
    };

    const res = processor.processOrder(raw);
    expect(res.success).toBe(true);
    expect(res.processedOrder.status).toBe('VERIFIED');
    expect(res.processedOrder.riskScore).toBeLessThan(0.30);
    expect(res.processedOrder.flags.length).toBe(0);
  });

  test('TC-6: High-value transaction exceeding threshold receives FLAGGED_HIGH_VALUE status', () => {
    const raw = {
      userId: 'user_102',
      productName: 'Professional Drone',
      category: 'Electronics',
      quantity: 1,
      pricePerUnit: 1499.0,
      totalAmount: 1499.0
    };

    const res = processor.processOrder(raw);
    expect(res.success).toBe(true);
    expect(res.processedOrder.status).toBe('FLAGGED_HIGH_VALUE');
    expect(res.processedOrder.riskScore).toBeGreaterThanOrEqual(0.60);
    expect(res.processedOrder.flags).toContain('AMOUNT_EXCEEDS_THRESHOLD_$1000');
  });

  test('TC-7: Luxury category and bulk purchase elevate risk score compoundingly', () => {
    const raw = {
      userId: 'user_105',
      productName: 'Luxury Chronograph',
      category: 'Luxury',
      quantity: 3,
      pricePerUnit: 1200.0,
      totalAmount: 3600.0
    };

    const res = processor.processOrder(raw);
    expect(res.success).toBe(true);
    expect(res.processedOrder.status).toBe('FLAGGED_HIGH_VALUE');
    expect(res.processedOrder.flags).toContain('HIGH_RISK_CATEGORY_LUXURY');
    expect(res.processedOrder.flags).toContain('BULK_QUANTITY_PURCHASE');
    expect(res.processedOrder.flags).toContain('AMOUNT_EXCEEDS_THRESHOLD_$1000');
    expect(res.processedOrder.riskScore).toBe(1.0); // Capped at 1.0
  });

  test('TC-8: Invalid order fails processing gracefully with descriptive error', () => {
    const invalid = {
      userId: 'user_109',
      quantity: 'invalid-number'
    };

    const res = processor.processOrder(invalid);
    expect(res.success).toBe(false);
    expect(res.error).toBeDefined();
  });
});
