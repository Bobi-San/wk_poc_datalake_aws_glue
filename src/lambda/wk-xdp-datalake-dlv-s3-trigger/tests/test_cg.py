import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock


def get_current_datetime():
    return datetime.now(timezone.utc)


class DateTimeTest(unittest.TestCase):
    def test_get_current_datetime(self):
        mock_datetime = MagicMock()
        mock_datetime.now.return_value = datetime(2023, 5, 19, 10, 30, 0)

        with unittest.mock.patch('datetime.datetime', mock_datetime):
            result = get_current_datetime()

        self.assertEqual(result, datetime(2023, 5, 19, 10, 30, 0))
