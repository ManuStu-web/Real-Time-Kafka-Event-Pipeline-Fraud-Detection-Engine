const { EventProcessor } = require('../src/processor');

describe('Order Validation Test Suite', () => {
  let processor;

  beforeEach(() => {
    processor = new EventProcessor(1000.0);
  });

  test('TC-1: Valid order payload passes validation with sanitized fields', () => {
    const raw = {
      userId: 'user_101',
      productName: 'Mechanical Keyboard',
      category: 'Electronics',
      quantity: 2,
      pricePerUnit: 89.99,
      totalAmount: 179.98
    };

    const result = processor.validateOrder(raw);
    expect(result.isValid).toBe(true);
    expect(result.sanitized).toBeDefined();
    expect(result.sanitized.userId).toBe('user_101');
    expect(result.sanitized.totalAmount).toBe(179.98);
    expect(result.sanitized.orderId).toBeDefined();
    expect(result.sanitized.currency).toBe('USD');
  });

  test('TC-2: Missing or empty userId is rejected', () => {
    const raw = {
      userId: '   ',
      productName: 'Desk Lamp',
      category: 'Home',
      quantity: 1,
      pricePerUnit: 25.0
    };

    const result = processor.validateOrder(raw);
    expect(result.isValid).toBe(false);
    expect(result.error).toMatch(/userId/i);
  });

  test('TC-3: Non-positive quantity or price is rejected', () => {
    const invalidQty = {
      userId: 'user_102',
      productName: 'Coffee Mug',
      category: 'Kitchen',
      quantity: -1,
      pricePerUnit: 15.0
    };
    expect(processor.validateOrder(invalidQty).isValid).toBe(false);

    const invalidPrice = {
      userId: 'user_102',
      productName: 'Coffee Mug',
      category: 'Kitchen',
      quantity: 1,
      pricePerUnit: 0
    };
    expect(processor.validateOrder(invalidPrice).isValid).toBe(false);
  });

  test('TC-4: Auto-computes totalAmount when not explicitly provided', () => {
    const raw = {
      userId: 'user_103',
      productName: 'Wireless Mouse',
      category: 'Electronics',
      quantity: 3,
      pricePerUnit: 25.0
    };

    const result = processor.validateOrder(raw);
    expect(result.isValid).toBe(true);
    expect(result.sanitized.totalAmount).toBe(75.0);
  });
});
