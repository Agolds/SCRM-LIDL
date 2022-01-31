import unittest
from program.program import Program


class TestAmountOpen(unittest.TestCase):
    def testNumberRows(self):
        """
        Check whether Dataframe contains 10 rows
        """
        df = Program.getTopTenOpened()
        count = df.count()
        self.assertEqual(count, 10, "Number of rows must be 10")


if __name__ == '__main__':
    unittest.main()
