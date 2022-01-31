import unittest
from program.program import Program


class TestAmountOpen(unittest.TestCase):
    def testRowType(self):
        """
        Check whether column type is number
        """
        df = Program.getOneMinuteWindow()
        types = df.select("Opened", "Closed").dtypes

        self.assertIsInstance(types[0][1], int, "Column data type should be int")
        self.assertIsInstance(types[1][1], int, "Column data type should be int")


if __name__ == '__main__':
    unittest.main()
