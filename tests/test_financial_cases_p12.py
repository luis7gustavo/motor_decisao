"""Negative checks for the P12 fixture validator."""

import copy
import json
import unittest

from scripts.validate_financial_cases_p12 import FIXTURE, validate


class FinancialCasesP12Tests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_current_fixture(self):
        self.assertEqual(validate(self.payload), (3, 2))

    def test_wrong_portfolio_peak_is_rejected(self):
        changed = copy.deepcopy(self.payload)
        changed["portfolio"]["expected"]["base_capital_need"] = "460.00"
        with self.assertRaisesRegex(ValueError, "portfolio.base_capital_need"):
            validate(changed)

    def test_unknown_price_cannot_be_zero_profit(self):
        changed = copy.deepcopy(self.payload)
        changed["cases"][2]["expected"]["profit"] = "0.00"
        with self.assertRaisesRegex(ValueError, "null outputs"):
            validate(changed)

    def test_shared_cost_cannot_be_paid_twice(self):
        changed = copy.deepcopy(self.payload)
        changed["cases"][0]["events"].append(copy.deepcopy(changed["cases"][0]["events"][2]))
        with self.assertRaisesRegex(ValueError, "cost event missing or repeated"):
            validate(changed)


if __name__ == "__main__":
    unittest.main()
