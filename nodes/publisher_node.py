import logging
import json
import sys
import random
import asyncio
import time
import os

from models.crypto_utils import CryptoUtils

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.load_config import load_config
from models.publication import Publication
from models import publication_pb2

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("PUBLISHER NODE")

config = load_config()
BROKER_PORTS = config["broker_ports"]
PUBLISHER_INTERVAL = config["publisher_interval"]
SERIALIZATION_MODE = config.get("serialization", "json").lower()

# --- EVALUATION CONFIG ---
EVALUATION_TIME_LIMIT = 180.0  # 3 minute


async def start_publisher(publisher_id: str, stats: dict) -> None:
    logger_node = logging.getLogger(f"Publisher-{publisher_id}")
    logger_node.info(f"Node {publisher_id} started for a {EVALUATION_TIME_LIMIT}s test using {SERIALIZATION_MODE.upper()} serialization...")

    start_time = time.time()

    while time.time() - start_time < EVALUATION_TIME_LIMIT:
        chosen_port = random.choice(BROKER_PORTS)
        try:
            # pregateste conezxiunea
            connect_task = asyncio.open_connection('localhost', chosen_port)
            # executa conexiunea cu timeout daca esueaza atunci exception TimeoutError
            _, writer = await asyncio.wait_for(connect_task, timeout=3.0)

            publication = Publication()

            encrypted_fields = {
                "company": CryptoUtils.encrypt_string(publication.fields["company"]),
                "value": CryptoUtils.encrypt_number(publication.fields["value"]),
                "drop": CryptoUtils.encrypt_number(publication.fields["drop"]),
                "variation": CryptoUtils.encrypt_number(publication.fields["variation"]),
                "date": CryptoUtils.encrypt_date(publication.fields["date"])
            }
            publication.fields = encrypted_fields

            if SERIALIZATION_MODE == "protobuf":
                # --- RUTARE PROTOBUF (Binar) ---
                pb_msg = publication_pb2.PublicationMessage()
                pb_msg.company = publication.fields["company"]
                pb_msg.value = publication.fields["value"]
                pb_msg.drop = publication.fields["drop"]
                pb_msg.variation = publication.fields["variation"]
                pb_msg.date = publication.fields["date"]
                pb_msg.timestamp = publication.timestamp

                binary_data = pb_msg.SerializeToString()
                length_prefix = len(binary_data).to_bytes(4, 'big')

                writer.write(length_prefix + binary_data)
            else:
                # --- RUTARE JSON (Text) ---
                packet = json.dumps({
                    "type": "PUBLISH",
                    "fields": publication.fields,
                    "timestamp": publication.timestamp
                }) + "\n"

                writer.write(packet.encode('utf-8'))

            await writer.drain()
            writer.close()
            await writer.wait_closed()
            
            stats['total_emitted'] += 1
            logger_node.info(f"[EMITTED #{stats['total_emitted']}] Sent to Broker {chosen_port} (via {SERIALIZATION_MODE.upper()})")

        except Exception as e:
            logger_node.warning(f"Failed to connect/send to Broker [{chosen_port}]: {e}")

        await asyncio.sleep(PUBLISHER_INTERVAL)
        
    logger_node.info(f"Node {publisher_id} has finished the evaluation run.")


if __name__ == "__main__":
    global_stats = {'total_emitted': 0}

    async def main() -> None:
        logger.info(f"Launching publishers for a {EVALUATION_TIME_LIMIT} seconds test...")
        await asyncio.gather(
            start_publisher("1", global_stats),
            start_publisher("2", global_stats)
        )
        
        print("\n" + "="*50)
        print("--- PUBLISHER EVALUATION FINISHED ---")
        print(f"Total Publications Emitted in 3 minutes: {global_stats['total_emitted']}")
        print("="*50 + "\n")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPublisher stream stopped manually.")