from dataclasses import dataclass, field
from typing import List, Dict, Any, Tuple


@dataclass
class Subscription:
    subscriber_id: str
    filters: List[Tuple[str, str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"subscriber_id": self.subscriber_id, "filters": self.filters}
