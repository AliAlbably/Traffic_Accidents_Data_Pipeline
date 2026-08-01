import json
import time
import pandas as pd
from kafka import KafkaProducer

print("📡 Connecting to High-Speed Kafka Producer...")

# Direct connection to Kafka via internal Docker network
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=0,                  # Maximum throughput speed
    linger_ms=10,            # Batching to reduce network overhead
    batch_size=16384 * 4     # Increased batch size for high-speed streaming
)

topic_name = 'traffic_events'
csv_path = '/opt/spark/work-dir/data/US_Accidents_March23.csv'

selected_cols = [
    'Severity', 'Start_Lat', 'Start_Lng',
    'Temperature(F)', 'Humidity(%)', 'Pressure(in)',
    'Visibility(mi)', 'Wind_Speed(mph)'
]

try:
    print(f"🚀 Streaming events into topic '{topic_name}'...")
    count = 0
    start_time = time.time()
    
    # Reading CSV in chunks to optimize memory usage
    for chunk in pd.read_csv(csv_path, usecols=selected_cols, chunksize=5000):
        chunk = chunk.dropna()
        for _, row in chunk.iterrows():
            event_data = {
                'Severity': int(row['Severity']),
                'Start_Lat': float(row['Start_Lat']),
                'Start_Lng': float(row['Start_Lng']),
                'Temperature(F)': float(row['Temperature(F)']),
                'Humidity(%)': float(row['Humidity(%)']),
                'Pressure(in)': float(row['Pressure(in)']),
                'Visibility(mi)': float(row['Visibility(mi)']),
                'Wind_Speed(mph)': float(row['Wind_Speed(mph)'])
            }
            producer.send(topic_name, value=event_data)
            count += 1
            
            # Print performance metrics every 10,000 events
            if count % 10000 == 0:
                elapsed = time.time() - start_time
                speed = int(count / elapsed) if elapsed > 0 else 0
                print(f"⚡ Successfully sent {count:,} events! (Speed: {speed} events/sec)")

except KeyboardInterrupt:
    print("\n🛑 Producer stopped manually.")
finally:
    # Ensure all buffered records are sent before closing
    producer.flush()
    producer.close()
    print("✅ Pipeline closed safely.")