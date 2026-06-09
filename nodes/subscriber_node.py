import asyncio
import logging
import random
import sys
import json
import time
import os
from typing import List

from models.crypto_utils import CryptoUtils

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
EVALUATION_TIME_LIMIT = 30.0  
TOTAL_SUBSCRIPTIONS = 10000


async def resilient_worker(subscriber_id: str, subscriptions: List[Subscription], initial_port: int, stats: dict, start_time: float) -> None:
    """Un task care menține o listă de filtre activă în rețea. Dacă brokerul cade, se mută pe altul."""
    logger_node = logging.getLogger(f"Sub-{subscriber_id}-Worker")
    current_port = initial_port

    while time.time() - start_time < EVALUATION_TIME_LIMIT:
        try:
            reader, writer = await asyncio.open_connection('localhost', current_port)
            
            # 1. Ne (re)abonăm. Trimitem filtrele acestui worker către brokerul curent.
            """for sub in subscriptions:
                packet = json.dumps({
                    "type": "SUBSCRIBE",
                    "subscriber_id": subscriber_id,
                    "filters": sub.filters
                }) + "\n"
                writer.write(packet.encode('utf-8'))
            await writer.drain()
            logger_node.info(f"Assigned {len(subscriptions)} filters to Broker [{current_port}]")
"""

            for sub in subscriptions:
                encrypted_filters = []
                for field_name, op, val in sub.filters:
                    if field_name == "company":
                        enc_val = CryptoUtils.encrypt_string(val)
                    elif field_name == "date":
                        enc_val = CryptoUtils.encrypt_date(val)
                    else:
                        enc_val = CryptoUtils.encrypt_number(val)
                    encrypted_filters.append([field_name, op, enc_val])

                # Împachetăm cu filtrele criptate, nu cu cele originale
                packet = json.dumps({
                    "type": "SUBSCRIBE",
                    "subscriber_id": subscriber_id,
                    "filters": encrypted_filters
                }) + "\n"
                writer.write(packet.encode('utf-8'))
                
            await writer.drain()
            logger_node.info(f"Assigned {len(subscriptions)} ENCRYPTED filters to Broker [{current_port}]")


            # 2. Ascultăm notificări
            while time.time() - start_time < EVALUATION_TIME_LIMIT:
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=1.0)
                    if not data:
                        # Brokerul a închis conexiunea brusc (CRASH!)
                        logger_node.warning(f"Connection lost with Broker [{current_port}]! Broker might be DOWN.")
                        break # Ieșim din bucla de ascultare pentru a declanșa reconectarea
                    
                    packet = data.decode('utf-8').strip() # transforma din bytes in string
                    if not packet:
                        continue
                        
                    message = json.loads(packet)
                    if message.get("type") == "NOTIFICATION": # Am primit o notoficare valida 
                        latency = time.time() - message["publication"]["timestamp"] # Latenta
                        stats['latencies'].append(latency)
                        stats['matches'] += 1
                            
                except asyncio.TimeoutError:
                    continue 

        except Exception as e:
            logger_node.error(f"Cannot connect to Broker [{current_port}].")

        # 3. LOGICA DE FALLBACK: Dacă am ajuns aici, conexiunea a picat. 
        # Alegem alt port care NU este portul curent picat și reîncercăm!
        available_ports = [p for p in BROKER_PORTS if p != current_port]
        current_port = random.choice(available_ports)
        logger_node.info(f"Relocating filters to alternative Broker [{current_port}]...")
        
        await asyncio.sleep(2.0) # Așteptăm puțin înainte de a bombarda rețeaua cu o nouă conexiune


async def start_distributed_subscriber(subscriber_id: str, subscriptions: List[Subscription], stats: dict, start_time: float) -> None:
    # Împărțim filtrele acestui subscriber în mod egal pe numărul de brokeri disponibili
    chunks = [subscriptions[i::len(BROKER_PORTS)] for i in range(len(BROKER_PORTS))]
    
    workers = []
    for idx, chunk in enumerate(chunks):
        if chunk:
            target_port = BROKER_PORTS[idx]
            # Lansăm câte un worker independent pentru fiecare bucată de filtre
            workers.append(resilient_worker(subscriber_id, chunk, target_port, stats, start_time))
            
    await asyncio.gather(*workers)


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
        start_time = time.time()

        logger.info("Launching evaluation. Please start the Publishers now!")
        await asyncio.gather(
            start_distributed_subscriber("0", generated_subscriptions[0:chunk_size], global_stats, start_time),
            start_distributed_subscriber("1", generated_subscriptions[chunk_size:2*chunk_size], global_stats, start_time),
            start_distributed_subscriber("2", generated_subscriptions[2*chunk_size:], global_stats, start_time)
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