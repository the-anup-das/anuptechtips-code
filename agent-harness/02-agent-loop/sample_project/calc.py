def total(prices, discount=0.0):
    """Return the sum of prices with a discount applied.

    discount is a fraction between 0 and 1, so 0.1 means 10% off.
    """
    subtotal = sum(prices)
    return subtotal - discount
