import unittest
import numpy as np
from datetime import datetime, timezone
from era5_replay import parse_utc, hourly_increment

class ReplayTests(unittest.TestCase):
    def test_hour01_first_interval(self):
        self.assertAlmostEqual(hourly_increment(np.array([0.]), np.array([.0002]), 0)[0], .0002)
    def test_hour12_diff(self):
        self.assertAlmostEqual(hourly_increment(np.array([.001]), np.array([.0017]), 11)[0], .0007)
    def test_midnight_cross_day_diff(self):
        self.assertAlmostEqual(hourly_increment(np.array([.0028]), np.array([.003]), 23)[0], .0002)
    def test_large_negative_fails(self):
        with self.assertRaises(ValueError):
            hourly_increment(np.array([.01]), np.array([.001]), 15, tiny_tolerance=1e-5)
    def test_utc(self):
        self.assertEqual(parse_utc('2018-04-01T00:00'), datetime(2018,4,1,tzinfo=timezone.utc))
if __name__=='__main__':
    unittest.main()
