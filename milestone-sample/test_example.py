import unittest

import example

'''
This test checks the example module (example.py) written in the 
first exercise to get familiar with the milestone testing.
'''


class ExampleTests:

    def test_example(self):
        self._check("Hello World", example.hello_world())


class TestExample(unittest.TestCase, ExampleTests):

    def _check(self, expected, current):
        self.assertEqual(expected, current)

if __name__ == '__main__':
    unittest.main()
