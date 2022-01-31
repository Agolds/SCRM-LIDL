import unittest
from program.program import Program


class TestAvg(unittest.TestCase):
    def testAvgResultType(self):
        """
        Check result type and number validations
        """
        num = Program.getAvgNumber()
        self.assertIsInstance(num, int, "Number of actions should be a whole number")

    def testAvgResultNumber(self):
        num = Program.getAvgNumber()
        self.assertGreaterEqual(num, 0, "Average has to be greater than 0")


if __name__ == '__main__':
    unittest.main()
