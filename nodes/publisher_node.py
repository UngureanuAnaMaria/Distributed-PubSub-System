import logging
import json
import sys
import random
import asyncio
import time
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.load_config import load_config
from models.publication import Publication

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("PUBLISHER NODE")

config = load_config()
BROKER_PORTS = config["broker_ports"]
PUBLISHER_INTERVAL = config["publisher_interval"]

# --- EVALUATION CONFIG ---
EVALUATION_TIME_LIMIT = 180.0  # 3 minute


async def start_publisher(publisher_id: str, stats: dict) -> None:
    logger_node = logging.getLogger(f"Publisher-{publisher_id}")
    logger_node.info(f"Node {publisher_id} started for a {EVALUATION_TIME_LIMIT}s test...")

    start_time = time.time()

    while time.time() - start_time < EVALUATION_TIME_LIMIT:
        chosen_port = random.choice(BROKER_PORTS)
        try:
            connect_task = asyncio.open_connection('localhost', chosen_port)
            _, writer = await asyncio.wait_for(connect_task, timeout=3.0)

            publication = Publication()
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
            logger_node.info(f"[EMITTED #{stats['total_emitted']}] Sent to Broker {chosen_port}")

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