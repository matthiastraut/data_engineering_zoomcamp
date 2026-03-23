import csv
import json
import time

from kafka import KafkaProducer

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    start_time = time.time()
    csv_file = 'green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for i, row in enumerate(reader):
            if i == 0:
                print(row)
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-trips"
            producer.send('green-trips', value=row)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()
    end_time = time.time()
    print(f'Duration: {end_time-start_time}')

if __name__ == "__main__":
    main()