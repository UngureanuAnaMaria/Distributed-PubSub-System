import hashlib
from datetime import datetime

# Secrete cunoscute DOAR de Publisher și Subscriber, NICIODATĂ de Broker
SECRET_SALT = "UAIC_SECURE_PUB_SUB_2026"
SECRET_MULT = 17.5
SECRET_OFFSET = 1042.0

class CryptoUtils:
    @staticmethod
    def encrypt_string(val: str) -> str:
        """Criptează texte folosind SHA-256 (Deterministic Hashing)."""
        return hashlib.sha256((val + SECRET_SALT).encode()).hexdigest()

    @staticmethod
    def encrypt_number(val: float) -> float:
        """Criptează numere păstrând ordinea (Order-Preserving Encryption)."""
        return float(val) * SECRET_MULT + SECRET_OFFSET

    @staticmethod
    def encrypt_date(date_str: str) -> str:
        """Transformă data în timestamp criptat și o formatează ca string fix pentru comparare OPE."""
        timestamp = datetime.strptime(date_str, "%d.%m.%Y").timestamp()
        val = float(timestamp) * SECRET_MULT + SECRET_OFFSET
        return f"{val:020.4f}" # Padding garantat pt evaluari corecte de < sau > pe string-uri