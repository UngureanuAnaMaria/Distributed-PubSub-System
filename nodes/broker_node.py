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
from models import publication_pb2

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])

config = load_config()
BROKER_PORTS = config["broker_ports"]
SERIALIZATION_MODE = config.get("serialization", "json").lower()


class BrokerNode:
    def __init__(self, broker_id: str, port: int) -> None:
        self.broker_id = broker_id
        self.port = port
        self.logger = logging.getLogger(f"Broker-{broker_id}")

        self.subscriptions: Dict[asyncio.StreamWriter, List[Subscription]] = {}

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
        self.logger.info(f"Broker node {self.broker_id} is ONLINE and listening on port {self.port}...")
        async with server:
            await server.serve_forever()

    async def forward_to_peers(self, fields: dict, timestamp: float) -> None:
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
                _, writer = await asyncio.open_connection('localhost', peer_port)
                writer.write(packet.encode('utf-8'))
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def process_matching(self, publication: Publication, is_forwarded: bool) -> None:
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
                    except Exception as e:
                        if client_writer in self.subscriptions:
                            del self.subscriptions[client_writer]

        if not is_forwarded:
            asyncio.create_task(self.forward_to_peers(publication.fields, publication.timestamp))

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info('peername')

        while True:
            try:
                # Citim fix primul byte pentru a detecta formatul (Framing)
                first_byte_chunk = await reader.read(1)
                if not first_byte_chunk:
                    break

                if first_byte_chunk == b'\n':
                    continue

                # Dacă primul byte este '{' (cod ASCII 123), este JSON
                if first_byte_chunk[0] == 123:
                    rest_of_line = await reader.readline()
                    packet = (first_byte_chunk + rest_of_line).decode('utf-8').strip()
                    if not packet:
                        continue
                        
                    message = json.loads(packet)
                    
                    if message.get("type") == "SUBSCRIBE":
                        subscription = Subscription(message["subscriber_id"], message["filters"])
                        if writer not in self.subscriptions:
                            self.subscriptions[writer] = []
                        self.subscriptions[writer].append(subscription)
                        
                    elif message.get("type") == "PUBLISH":
                        publication = Publication(message["fields"], message["timestamp"])
                        is_forwarded = message.get("forwarded", False)
                        log_prefix = "[FORWARDED] " if is_forwarded else ""
                        self.logger.info(f"{log_prefix}Publication received (JSON). Payload: {json.dumps(publication.fields)}")
                        await self.process_matching(publication, is_forwarded=is_forwarded)

                else:
                    # Dacă nu este '{', atunci este un pachet binar (Protobuf)
                    # Citim restul de 3 bytes din prefixul de lungime (am citit deja 1 byte sus)
                    rest_of_length = await reader.readexactly(3)
                    msg_length = int.from_bytes(first_byte_chunk + rest_of_length, 'big')

                    # Citim fix atâția bytes câți ne-a indicat prefixul
                    binary_data = await reader.readexactly(msg_length)

                    pb_msg = publication_pb2.PublicationMessage()
                    pb_msg.ParseFromString(binary_data)

                    fields = {
                        "company": pb_msg.company,
                        "value": round(pb_msg.value, 2),
                        "drop": round(pb_msg.drop, 2),
                        "variation": round(pb_msg.variation, 4),
                        "date": pb_msg.date
                    }
                    publication = Publication(fields=fields, timestamp=pb_msg.timestamp)
                    
                    self.logger.info(f"Publication received (PROTOBUF). Payload: {json.dumps(publication.fields)}")
                    await self.process_matching(publication, is_forwarded=False)

            except asyncio.IncompleteReadError:
                break
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