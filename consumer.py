import json

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
TEMPERATURE_THRESHOLD = 35.0
TARGET_BUILDING = "A"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

total_temperature = 0.0
event_count = 0

print(f"Listening to topic: {TOPIC}")
print(f"Filtering events from Building {TARGET_BUILDING}")

try:
    for message in consumer:
        event = message.value

        if event["campus_building"] != TARGET_BUILDING:
            continue

        temperature = event["temperature_c"]

        event_count += 1
        total_temperature += temperature
        average_temperature = total_temperature / event_count

        print(
            f"{event['timestamp']} | "
            f"{event['sensor_id']} | "
            f"Building {event['campus_building']} | "
            f"T={temperature}°C | "
            f"RH={event['humidity_pct']}%"
        )

        print(f"Running average: {average_temperature:.2f}°C")

        if temperature >= TEMPERATURE_THRESHOLD:
            print("ALERT: High temperature detected!")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
