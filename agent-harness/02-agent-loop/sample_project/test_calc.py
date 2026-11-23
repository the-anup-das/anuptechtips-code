import unittest

from calc import total


class TestTotal(unittest.TestCase):
    def test_no_discount(self):
        self.assertEqual(total([10, 20]), 30)

    def test_ten_percent_off(self):
        self.assertAlmostEqual(total([10, 20], discount=0.1), 27.0)


if __name__ == "__main__":
    unittest.main()
