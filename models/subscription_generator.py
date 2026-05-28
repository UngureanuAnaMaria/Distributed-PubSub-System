import random
from typing import List, Dict
from models.subscription import Subscription
from models.publication import PRESET_COMPANIES, PRESET_DATES

NUMERIC_OPERATORS: List[str] = ["==", "<", ">", "<=", ">="]


# posibila imbunatatire -> in config sa dau denumirea campului pt == pondere, poate chiar o lista
class SubscriptionGenerator:
    @staticmethod
    def generate_set(total_count: int, company_equality_weight: float,
                     weights: Dict[str, float]) -> List[Subscription]:

        company_presence = ([True] * int(total_count * weights["company"]) +
                            [False] * (total_count - int(total_count * weights["company"])))
        value_presence = ([True] * int(total_count * weights["value"]) +
                          [False] * (total_count - int(total_count * weights["value"])))
        drop_presence = ([True] * int(total_count * weights["drop"]) +
                         [False] * (total_count - int(total_count * weights["drop"])))
        variation_presence = ([True] * int(total_count * weights["variation"]) +
                              [False] * (total_count - int(total_count * weights["variation"])))
        date_presence = ([True] * int(total_count * weights["date"]) +
                         [False] * (total_count - int(total_count * weights["date"])))

        company_present_count = int(total_count * weights["company"])
        equality_count = int(company_present_count * company_equality_weight)
        company_operators = ["=="] * equality_count + [random.choice(["!=", "<", ">"])
                                                       for _ in range(company_present_count - equality_count)]

        random.shuffle(company_presence)
        random.shuffle(value_presence)
        random.shuffle(drop_presence)
        random.shuffle(variation_presence)
        random.shuffle(date_presence)
        random.shuffle(company_operators)

        generated_subscriptions: List[Subscription] = []
        company_idx = 0

        for i in range(total_count):
            is_empty = not (company_presence[i] or value_presence[i] or drop_presence[i] or variation_presence[i] or
                            date_presence[i])

            # Trading Alg -> previne crearea de subscriptii goale
            if is_empty:
                for j in range(i + 1, total_count):
                    fields_at_j = [
                        ("company", company_presence[j]),
                        ("value", value_presence[j]),
                        ("variation", variation_presence[j]),
                        ("drop", drop_presence[j]),
                        ("date", date_presence[j])
                    ]

                    true_fields_at_j = [field_name for field_name, is_true in fields_at_j if is_true]

                    if len(true_fields_at_j) >= 2:
                        stolen_field = random.choice(true_fields_at_j)

                        if stolen_field == "company":
                            company_presence[j] = False
                        elif stolen_field == "value":
                            value_presence[j] = False
                        elif stolen_field == "drop":
                            drop_presence[j] = False
                        elif stolen_field == "variation":
                            variation_presence[j] = False
                        elif stolen_field == "date":
                            date_presence[j] = False

                        if stolen_field == "company":
                            company_presence[i] = True
                        elif stolen_field == "value":
                            value_presence[i] = True
                        elif stolen_field == "drop":
                            drop_presence[i] = True
                        elif stolen_field == "variation":
                            variation_presence[i] = True
                        elif stolen_field == "date":
                            date_presence[i] = True

                        break

            instance_filters = []

            if company_presence[i]:
                op = company_operators[company_idx]
                company_idx += 1
                val = random.choice(PRESET_COMPANIES)
                instance_filters.append(("company", op, val))

            if value_presence[i]:
                op = random.choice(NUMERIC_OPERATORS)
                val = round(random.uniform(10.0, 500.0), 2)
                instance_filters.append(("value", op, val))

            if drop_presence[i]:
                op = random.choice(NUMERIC_OPERATORS)
                val = round(random.uniform(0.0, 50.0), 2)
                instance_filters.append(("drop", op, val))

            if variation_presence[i]:
                op = random.choice(NUMERIC_OPERATORS)
                val = round(random.uniform(0.0, 1.0), 4)
                instance_filters.append(("variation", op, val))

            if date_presence[i]:
                op = random.choice(NUMERIC_OPERATORS)
                val = random.choice(PRESET_DATES)
                instance_filters.append(("date", op, val))

            generated_subscriptions.append(Subscription(subscriber_id=f"Sub_{i + 1}",
                                                        filters=instance_filters))

        return generated_subscriptions
