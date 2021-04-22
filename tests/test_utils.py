import pandas as pd
from unittest import TestCase
from crypto_data_fetcher.utils import normalize_to_unix

class TestUtils(TestCase):
    def test_normalize_to_unix(self):
        self.assertEqual(normalize_to_unix(1), 1)
        self.assertEqual(normalize_to_unix(pd.to_datetime(1, unit='s', utc=True)), 1)

