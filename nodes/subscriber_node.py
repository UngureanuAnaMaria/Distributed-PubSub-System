import asyncio
import logging
import random
import sys
import json
import time
import os
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.load_config import load_config
from models.subscription import Subscription
from models.subscription_generator import SubscriptionGenerator

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("SUBSCRIBER NODE")

config = load_config()
BROKER_PORTS = config["broker_ports"]
SUBSCRIPTION_WEIGHTS = config["subscription_weights"]
DEFAULT_EQUALITY_WEIGHT = config["default_equality_weight"]

# --- EVALUATION CONFIG ---
EVALUATION_TIME_LIMIT = 180.0  # 3 minute
TOTAL_SUBSCRIPTIONS = 10000


async def start_distributed_subscriber(subscriber_id: str, subscriptions: List[Subscription], stats: dict) -> None:
    logger_node = logging.getLogger(f"Subscriber-{subscriber_id}")
    logger_node.info(f"Node {subscriber_id} routing {len(subscriptions)} filters...")

    connections = {}
    for port in BROKER_PORTS:
        try:
            reader, writer = await asyncio.open_connection('localhost', port)
            connections[port] = (reader, writer)
        except Exception as e:
            logger_node.error(f"Failed to connect to Broker [{port}]: {e}")

    # Routing
    for index, subscription in enumerate(subscriptions):
        target_port = BROKER_PORTS[index % len(BROKER_PORTS)]
        if target_port in connections:
            _, writer = connections[target_port]
            packet = json.dumps({
                "type": "SUBSCRIBE",
                "subscriber_id": subscriber_id,
                "filters": subscription.filters
            }) + "\n"
            writer.write(packet.encode('utf-8'))
            await writer.drain()

    logger_node.info(f"Node {subscriber_id} ready. Listening for matches for {EVALUATION_TIME_LIMIT}s...")
    start_time = time.time()

    async def listen_to_broker(port: int, reader: asyncio.StreamReader):
        while time.time() - start_time < EVALUATION_TIME_LIMIT:
            try:
                # FOLOSIM readline() IN LOC DE read(4096)
                data = await asyncio.wait_for(reader.readline(), timeout=1.0)
                if not data:
                    break
                
                packet = data.decode('utf-8').strip()
                if not packet:
                    continue
                    
                message = json.loads(packet)
                if message.get("type") == "NOTIFICATION":
                    latency = time.time() - message["publication"]["timestamp"]
                    stats['latencies'].append(latency)
                    stats['matches'] += 1
                        
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger_node.error(f"Listening error on [{port}]: {e}")
                break
    listen_tasks = [listen_to_broker(port, reader) for port, (reader, _) in connections.items()]
    await asyncio.gather(*listen_tasks)


if __name__ == "__main__":
    async def main() -> None:
        logger.info(f"Generating {TOTAL_SUBSCRIPTIONS} balanced subscriptions...")
        generated_subscriptions = SubscriptionGenerator.generate_set(
            TOTAL_SUBSCRIPTIONS,
            company_equality_weight=DEFAULT_EQUALITY_WEIGHT,
            weights=SUBSCRIPTION_WEIGHTS
        )

        global_stats = {'matches': 0, 'latencies': []}
        chunk_size = TOTAL_SUBSCRIPTIONS // 3

        logger.info("Launching evaluation. Please start the Publishers now!")
        await asyncio.gather(
            start_distributed_subscriber("0", generated_subscriptions[0:chunk_size], global_stats),
            start_distributed_subscriber("1", generated_subscriptions[chunk_size:2*chunk_size], global_stats),
            start_distributed_subscriber("2", generated_subscriptions[2*chunk_size:], global_stats)
        )

        avg_latency = sum(global_stats['latencies']) / len(global_stats['latencies']) if global_stats['latencies'] else 0
        
        print("\n" + "="*60)
        print("============= EVALUATION REPORT (3 MINUTES) =============")
        print(f"Total Subscriptions Loaded:  {TOTAL_SUBSCRIPTIONS}")
        print(f"Equality Weight Configured:  {DEFAULT_EQUALITY_WEIGHT * 100}%")
        print(f"Total Matches Received:      {global_stats['matches']}")
        print(f"Average Delivery Latency:    {avg_latency:.6f} seconds")
        print("=========================================================\n")


    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSubscriber simulation closed by user.")