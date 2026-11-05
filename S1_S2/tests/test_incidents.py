import unittest
from datetime import datetime, timezone
from telegram_incidents import Hour, combine_hours, describe

class TestIncidents(unittest.TestCase):
    def test_consecutive_and_duplicate(self):
        def hour(s, temp):
            return Hour('3011', 'kukurydza_ziemniaki',
                        datetime.fromisoformat(s).replace(tzinfo=timezone.utc), temp, 0)
        data = [hour('2018-04-01T01:00', -1), hour('2018-04-01T02:00', -2),
                hour('2018-04-01T02:00', -1.5), hour('2018-04-01T05:00', -1)]
        result = combine_hours(data, min_hours=2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].hours, 2)
        self.assertEqual(result[0].minimum, -2)
        self.assertEqual(result[0].start.hour, 1)
        self.assertIn('SYMULACJA 2018', describe(result[0]))
    def test_split_by_crop_region(self):
        t = datetime(2018,4,1,1,tzinfo=timezone.utc)
        values = [Hour('3011','a',t,-2,0), Hour('3011','b',t,-5,-4)]
        self.assertEqual(len(combine_hours(values, min_hours=1)), 2)
    def test_deterministic_id(self):
        t = datetime(2018,4,1,1,tzinfo=timezone.utc)
        h = Hour('3011','a',t,-2,0)
        self.assertEqual(combine_hours([h],1)[0].event_id, combine_hours([h],1)[0].event_id)

if __name__ == '__main__':
    unittest.main()
