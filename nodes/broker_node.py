import asyncio
import logging
import sys
import json
from typing import List, Dict

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

        # Baza locala din RAM a brokerului: writer -> lista de subscriptii
        self.subscriptions: Dict[asyncio.StreamWriter, List[Subscription]] = {}

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
        self.logger.info(f"Broker node {self.broker_id} is ONLINE and listening on port {self.port}...")
        async with server:
            await server.serve_forever()

    async def forward_to_peers(self, fields: dict, timestamp: float) -> None:
        """Redirecționează publicația primită de la publisher către ceilalți brokeri din rețea."""
        forward_message = {
            "type": "PUBLISH",
            "fields": fields,
            "timestamp": timestamp,
            "forwarded": True
        }
        packet = json.dumps(forward_message) + "\n"

        for peer_port in BROKER_PORTS:
            if peer_port == self.port:
                continue
            try:
                # Conectare dinamică, trimitere pachet și închidere conexiune (similara cu publisher-ul)
                _, writer = await asyncio.open_connection('localhost', peer_port)
                writer.write(packet.encode('utf-8'))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.logger.info(f"-> [FORWARD] Publication routed to peer broker on port [{peer_port}]")
            except Exception as e:
                self.logger.warning(f"Could not forward to peer broker on port [{peer_port}]: {e}")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info('peername')
        self.logger.info(f"New connection established from: {peer_name}")

        while True:
            try:
                # CITIM LINIE CU LINIE IN LOC DE BUCATI DE 4096 BYTES
                data = await reader.readline()
                if not data:
                    self.logger.info(f"Connection with client {peer_name} was closed.")
                    break

                packet = data.decode('utf-8').strip()
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
                        is_forwarded = message.get("forwarded", False)
                        
                        log_prefix = "[FORWARDED] " if is_forwarded else ""
                        self.logger.info(f"{log_prefix}Publication received. Payload: {json.dumps(publication.fields)}")

                        # 1. Matching local și notificare pentru subscriberii conectați direct la acest broker
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

                        # 2. Dacă publicația vine direct de la un Publisher (nu este deja forward-ată), o trimitem peer-ilor
                        if not is_forwarded:
                            # Folosim asyncio.create_task pentru a nu bloca fluxul principal al clientului curent
                            asyncio.create_task(self.forward_to_peers(publication.fields, publication.timestamp))

            except Exception as e:
                self.logger.error(f"Error handling traffic for {peer_name}: {e}")
                break

        if writer in self.subscriptions:
            del self.subscriptions[writer]
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
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