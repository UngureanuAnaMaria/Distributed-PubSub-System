import asyncio
import logging
import random
import sys
import json
import time
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


async def start_subscriber(subscriber_id: str, subscription: Subscription) -> None:
    logger_node = logging.getLogger(f"Subscriber-{subscriber_id}")
    logger_node.info(f"Node {subscriber_id} started and is registering subscriptions...")

    chosen_port = random.choice(BROKER_PORTS)
    logger_node.info(f"Connecting randomly to Broker on port [{chosen_port}]...")

    try:
        reader, writer = await asyncio.open_connection('localhost', chosen_port)

        packet = json.dumps({
            "type": "SUBSCRIBE",
            "subscriber_id": subscriber_id,
            "filters": subscription.filters
        }) + "\n"

        writer.write(packet.encode('utf-8'))
        await writer.drain()
        logger_node.info(f"Successfully registered. Stored Filters: {subscription.filters}")

        while True:
            data = await reader.read(4096)
            if not data:
                logger_node.error("Connection with Broker was lost.")
                break

            packets = data.decode('utf-8').strip().split('\n')
            for packet in packets:
                if not packet:
                    continue

                message = json.loads(packet)

                if message.get("type") == "NOTIFICATION":
                    latency = time.time() - message["publication"]["timestamp"]
                    logger_node.info(
                        f"======> [MATCH RECEIVED] "
                        f"Company: {message['publication']['fields']['company']} | "
                        f"Value: {message['publication']['fields']['value']} | "
                        f"Drop: {message['publication']['fields']['drop']} | "
                        f"Variation: {message['publication']['fields']['variation']} | "
                        f"Date: {message['publication']['fields']['date']} | "
                        f"Latency: {latency:.6f}s"
                    )

    except Exception as e:
        logger_node.error(f"Connection failed or dropped with Broker [{chosen_port}]: {e}")
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    async def main() -> None:
        logger.info("Generating balanced subscription data from generator...")
        generated_subscriptions = SubscriptionGenerator.generate_set(
            3,
            company_equality_weight=DEFAULT_EQUALITY_WEIGHT,
            weights=SUBSCRIPTION_WEIGHTS
        )

        logger.info("Launching subscribers concurrently into the network...")
        await asyncio.gather(
            *(start_subscriber(f"{i}", generated_subscriptions[i]) for i in range(3))
        )


    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSubscriber simulation closed by user.")
