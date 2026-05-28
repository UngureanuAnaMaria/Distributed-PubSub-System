import logging
import json
import sys
import random
import asyncio
from models.load_config import load_config
from models.publication import Publication

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])  # handler -> implicit este stderr, nu stdout
logger = logging.getLogger("PUBLISHER NODE")

config = load_config()
BROKER_PORTS = config["broker_ports"]
PUBLISHER_INTERVAL = config["publisher_interval"]


async def start_publisher(publisher_id: str) -> None:
    logger = logging.getLogger(f"Publisher-{publisher_id}")
    logger.info(f"Node {publisher_id} started and is generating publication stream...")

    while True:
        chosen_port = random.choice(BROKER_PORTS)
        try:
            connect_task = asyncio.open_connection('localhost', chosen_port)
            _, writer = await asyncio.wait_for(connect_task, timeout=3.0)  # 3s limit to connect

            publication = Publication()
            packet = json.dumps({
                "type": "PUBLISH",
                "fields": publication.fields,
                "timestamp": publication.timestamp
            }) + "\n"

            # bytes - only format accepted by the network
            writer.write(packet.encode('utf-8'))
            await writer.drain()  # send data and wait for buffer to become empty

            writer.close()
            await writer.wait_closed()
            logger.info(f"Publication sent successfully to Broker {chosen_port}| "
                        f"Payload: {json.dumps(publication.fields)}")

        except ConnectionRefusedError:
            # brokerul este oprit
            logger.error(f"Broker [{chosen_port}] is completely offline (Connection Refused). Retrying...")

        except asyncio.TimeoutError:
            # brokerul e pornit, dar e blocat/suprasolicitat si nu raspunde in 3 secunde
            logger.warning(f"Broker [{chosen_port}] connection timed out. Server might be overloaded. Retrying...")

        except (ConnectionResetError, OSError) as network_error:
            # pica rețeaua sau apar erori de sistem de operare
            logger.error(f"Network transport error with Broker [{chosen_port}]: {network_error}. Retrying...")

        except Exception as unknown_error:
            # Plasa de siguranta pentru orice alta eroare neprevazuta (ex: eroare de parsare JSON)
            logger.critical(f"Unexpected critical error: {unknown_error}")

        await asyncio.sleep(PUBLISHER_INTERVAL)


if __name__ == "__main__":
    async def main() -> None:
        logger.info("Launching publishers concurrently into the network...")
        # Paralelizare -> lansate simultan, alternând utilizarea procesorului în momentele de await
        await asyncio.gather(
            start_publisher("1"),
            start_publisher("2")
        )


    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPublisher stream stopped by user.")
