import unittest
from transistor import get_last_element
from transistor.config import load_transducer


class TestUtilityFunctions(unittest.TestCase):

    def test_load_transducer(self):
        transducer = load_transducer('get_last_element')
        self.assertIs(transducer, get_last_element)
