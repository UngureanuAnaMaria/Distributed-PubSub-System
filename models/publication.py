import random
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any

PRESET_COMPANIES: List[str] = ["Google", "Apple", "Microsoft", "Amazon", "Tesla"]
PRESET_DATES: List[str] = ["2.02.2022", "15.06.2023", "10.11.2024", "27.05.2026"]

@dataclass # decorator -> automat: __init__(field = {}, timestamp = current time), __repr__(toString), __eq__
class Publication:
    fields: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.fields:
            self.fields = {
                "company": random.choice(PRESET_COMPANIES),
                "value": round(random.uniform(10.0, 500.0),2),
                "drop": round(random.uniform(0.0, 50.0), 2),
                "variation": round(random.uniform(0.0, 1.0), 4),
                "date": random.choice(PRESET_DATES)
            }

    def to_dict(self) -> Dict[str, Any]:
        return {"fields": self.fields, "timestamp": self.timestamp}