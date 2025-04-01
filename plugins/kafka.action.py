import json
from confluent_kafka import Producer
import os

def register_arguments(parser):
    parser.add_argument("--kafka-endpoint", type=str, required=True, help="Kafka broker address")

def register_action(file_path, file_content, file_metadata, machine_metadata):
    from argparse import Namespace
    import sys

    # Prevent processing of .kafka files
    if file_path.endswith(".kafka"):
        print(f"Skipping processing for Kafka confirmation file: {file_path}", file=sys.stdout)
        return

    args = Namespace(kafka_endpoint="localhost:9092")  # Default value, overridden by CLI
    topic = "file-events"

    producer = Producer({'bootstrap.servers': args.kafka_endpoint})
    message = json.dumps({
        "type": "log",
        "file_path": file_path,
        "file_content": file_content,
        "file_metadata": {
            "size": file_metadata.st_size,
            "modified": file_metadata.st_mtime
        },
        "machine_metadata": machine_metadata
    })

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print(f"Message delivery failed for file {file_path}: {err}", file=sys.stderr)
        else:
            kafka_message_id_file = file_path + ".kafka"
            try:
                with open(kafka_message_id_file, "w") as f:
                    f.write(f"Topic: {msg.topic()}\n")
                    f.write(f"Partition: {msg.partition()}\n")
                    f.write(f"Offset: {msg.offset()}\n")
                print(f"Sent file {file_path} to Kafka topic {topic}. Message ID saved to {kafka_message_id_file}", file=sys.stdout)
            except Exception as e:
                print(f"Error saving Kafka message ID for file {file_path}: {e}", file=sys.stderr)

    producer.produce(topic, key=file_path, value=message, callback=delivery_report)
    producer.flush()

if __name__ == '__main__':
    # Example usage (for testing)
    import argparse
    import stat
    import platform
    import time

    parser = argparse.ArgumentParser(description="Simulate sending file events to Kafka.")
    register_arguments(parser)
    parser.add_argument("file_to_send", type=str, help="Path to the file to simulate sending.")
    parsed_args = parser.parse_args()

    file_path_to_send = parsed_args.file_to_send

    if not os.path.exists(file_path_to_send):
        print(f"Error: File not found: {file_path_to_send}")
        sys.exit(1)

    # Simulate creating a .kafka file (for testing the skip logic)
    if file_path_to_send.endswith(".txt"):
        kafka_confirmation_file = file_path_to_send + ".kafka"
        with open(kafka_confirmation_file, "w") as f:
            f.write("This is a test kafka confirmation file.")
        print(f"Simulated creation of: {kafka_confirmation_file}")

    try:
        with open(file_path_to_send, "r") as f:
            file_content_to_send = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

    try:
        file_metadata_to_send = os.stat(file_path_to_send)
    except Exception as e:
        print(f"Error getting file metadata: {e}")
        sys.exit(1)

    machine_metadata_to_send = {
        "hostname": platform.node(),
        "os": platform.system(),
        # Add other relevant machine info here
    }

    register_action(file_path_to_send, file_content_to_send, file_metadata_to_send, machine_metadata_to_send)