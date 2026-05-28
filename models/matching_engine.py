import logging
from datetime import datetime
from models.publication import Publication
from models.subscription import Subscription

logger = logging.getLogger("MATCHING ENGINE")


class MatchingEngine:
    @staticmethod
    def is_match(publication: Publication, subscription: Subscription) -> bool:
        # Match Universal
        if not subscription.filters:
            return True

        for field_name, op, sub_val in subscription.filters:
            if field_name not in publication.fields:
                return False

            pub_val = publication.fields[field_name]

            if field_name == "date":
                try:
                    v1 = datetime.strptime(pub_val, "%d.%m.%Y").date()
                    v2 = datetime.strptime(sub_val, "%d.%m.%Y").date()
                except Exception as e:
                    logger.error(f"Error parsing date data: {e}")
                    return False
            else:
                v1 = pub_val
                v2 = sub_val

            if op == "==":
                if not (v1 == v2): return False
            elif op == "!=":
                if not (v1 != v2): return False
            elif op == "<":
                if not (v1 < v2): return False
            elif op == ">":
                if not (v1 > v2): return False
            elif op == "<=":
                if not (v1 <= v2): return False
            elif op == ">=":
                if not (v1 >= v2): return False

        return True
