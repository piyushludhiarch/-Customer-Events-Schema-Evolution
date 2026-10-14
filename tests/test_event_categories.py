import unittest  # Provides Python's built-in unit-testing tools.

from app import categorize_event  # Imports the function this test file checks.


class CategorizeEventTests(unittest.TestCase):  # Groups related event-category tests.
    def test_business_categories(self):  # Checks the main event-to-category mappings.
        cases = {  # Pairs sample event names with the expected business categories.
            "LOGIN": "SESSION",  # Login is session activity.
            "LOGOUT": "SESSION",  # Logout is session activity.
            "SEARCH": "DISCOVERY",  # Search is content discovery.
            "VIEW": "DISCOVERY",  # Viewing content is discovery.
            "PURCHASE": "TRANSACTION",  # A purchase is a transaction.
            "PAYMENT": "TRANSACTION",  # A payment is a transaction.
            "SOMETHING_NEW": "OTHER",  # Unknown event names use the fallback category.
        }  # Finishes the event-to-category examples.
        for event_type, expected in cases.items():  # Tests every event name in the examples.
            with self.subTest(event_type=event_type):  # Shows the specific event if a test fails.
                self.assertEqual(categorize_event(event_type), expected)  # Compares the actual and expected categories.

    def test_category_is_case_insensitive_and_handles_missing_type(self):  # Checks lower-case and missing event names.
        self.assertEqual(categorize_event(" purchase "), "TRANSACTION")  # Confirms spaces and letter case are ignored.
        self.assertEqual(categorize_event(None), "OTHER")  # Confirms a missing event name uses the fallback category.

    def test_category_handles_non_string_inputs(self):  # Checks surprising values from malformed data.
        self.assertEqual(categorize_event(123), "OTHER")  # Confirms numeric input falls back safely.


if __name__ == "__main__":  # Checks whether this test file was launched directly.
    unittest.main()  # Runs all the tests in this file.
  