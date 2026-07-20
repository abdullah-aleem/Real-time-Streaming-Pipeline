import time 
from event_generater import EventGenerater
import json 
import random


def event_generator():
    return {
        "user_id": random.randint(1, 100),
        "event_type": random.choice(["click", "view", "purchase"]),
        "timestamp": int(time.time())
    }
def main():
    producer= EventGenerater(bootstrap_servers="localhost:9092",topic="user_events")
    try:
        while True:
            event = event_generator()
            print(f"Sending event: {event}")
            producer.send_event(event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()
if __name__ == "__main__":
    main()