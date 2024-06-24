class LineItem:
  def __init__(self, sku, price, quantity):
    self.sku = sku
    self.price = price
    self.quantity = quantity

  def asdict(self):
      return {'sku': self.sku, 'price': self.price, 'qty': self.quantity}