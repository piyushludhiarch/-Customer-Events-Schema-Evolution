import unittest

from app import categorize_event


class CategorizeEventTests(unittest.TestCase):
    def test_business_categories(self):
        cases = {
            "LOGIN": "SESSION",
            "LOGOUT": "SESSION",
            "SEARCH": "DISCOVERY",
            "VIEW": "DISCOVERY",
            "PURCHASE": "TRANSACTION",
            "PAYMENT": "TRANSACTION",
            "SOMETHING_NEW": "OTHER",
        }
        for event_type, expected in cases.items():
            with self.subTest(event_type=event_type):
                self.assertEqual(categorize_event(event_type), expected)

    def test_category_is_case_insensitive_and_handles_missing_type(self):
        self.assertEqual(categorize_event(" purchase "), "TRANSACTION")
        self.assertEqual(categorize_event(None), "OTHER")


if __name__ == "__main__":
    unittest.main()
