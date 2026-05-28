import asyncio
import logging
import sys
import json
from typing import List, Dict

from models.matching_engine import MatchingEngine
from models.publication import Publication
from models.subscription import Subscription
from models.load_config import load_config

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])

config = load_config()
BROKER_PORTS = config["broker_ports"]


class BrokerNode:
    def __init__(self, broker_id: str, port: int) -> None:
        self.broker_id = broker_id
        self.port = port
        self.logger = logging.getLogger(f"Broker-{broker_id}")

        # baza locala din RAM a brokerului, writer -> conex
        self.subscriptions: Dict[asyncio.StreamWriter, List[Subscription]] = {}

    async def start(self) -> None:
        # de fiecare data cand un client se conecteaza porneste handle_client()
        server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
        self.logger.info(f"Broker node {self.broker_id} is ONLINE and listening on port {self.port}...")
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info('peername')  # extract client IP & port
        self.logger.info(f"New connection established from: {peer_name}")

        while True:
            try:
                data = await reader.read(4096)  # chunk size = 4KB
                if not data:
                    self.logger.error("Connection with Client was lost.")
                    break  # Client disconnected

                packets = data.decode('utf-8').strip().split('\n')
                for packet in packets:
                    if not packet:
                        continue

                    message = json.loads(packet)

                    if message.get("type") == "SUBSCRIBE":
                        subscription = Subscription(message["subscriber_id"], message["filters"])
                        if writer not in self.subscriptions:
                            self.subscriptions[writer] = []
                        self.subscriptions[writer].append(subscription)
                        self.logger.info(f"Subscription STORED for subscriber {message['subscriber_id']}")
                    elif message.get("type") == "PUBLISH":
                        publication = Publication(message["fields"], message["timestamp"])
                        self.logger.info(f"Publication received. Payload: {json.dumps(publication.fields)}")

                        for client_writer, subscriptions in list(self.subscriptions.items()):
                            for subscription in subscriptions:
                                if MatchingEngine.is_match(publication, subscription):
                                    try:
                                        notification = json.dumps({
                                            "type": "NOTIFICATION",
                                            "publication": publication.to_dict()
                                        }) + "\n"
                                        client_writer.write(notification.encode('utf-8'))
                                        await client_writer.drain()
                                        self.logger.info(
                                            f"-> [MATCH] Notification pushed successfully to {subscription.subscriber_id}")
                                    except Exception as e:
                                        self.logger.error(f"Failed to send notification to client: {e}")
                                        if client_writer in self.subscriptions:
                                            del self.subscriptions[client_writer]

            except Exception as e:
                self.logger.error(f"Error handling traffic for {peer_name}: {e}")
                break

        if writer in self.subscriptions:
            del self.subscriptions[writer]
        writer.close()
        await writer.wait_closed()
        self.logger.info(f"Connection closed for: {peer_name}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m nodes.broker_node <broker_id> <port>")
        sys.exit(1)

    broker_name = sys.argv[1]
    broker_port = int(sys.argv[2])

    if broker_port not in BROKER_PORTS:
        print(f"Error: Port {broker_port} is not defined in config.json")
        sys.exit(1)

    try:
        asyncio.run(BrokerNode(broker_name, broker_port).start())
    except KeyboardInterrupt:
        print(f"\nBroker {broker_name} stopped.")
