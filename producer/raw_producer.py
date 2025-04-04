import json
from logger import log
from cantools.database.namedsignalvalue import NamedSignalValue
from .base_producer import KafkaProducer

from utils.message_payload import MessagePayload
import logging

class KafkaCanDataProducer(KafkaProducer):
    """
    Kafka producer for standard data messages, handling serialization and topic-specific production.
    """
    def __init__(self, brokers, topic_path):
        super().__init__(brokers)
        
        with open(topic_path, 'r') as file:
            self.topics = json.load(file)

    def send_data(self, payload: MessagePayload):
        """
        Process and send data to specific Kafka topics based on the data content.
        """
        payload.kafka_producer_error = []
        serialized_raw_data = self.convert_to_serializable(payload.can_decoded_data)
                
        for producer_data in serialized_raw_data:
            try:
                key = self.create_key_for_raw_data(
                    producer_data.get('vin', None),
                    producer_data.get('event_time', None),
                    producer_data.get('raw_can_id', None)
                )
                        
                super().send_data(
                    key=key,
                    data=producer_data,
                    topic="raw-decompressed-can"
                )
                payload.success_counts += 1
            except Exception as e:
                log("[Data Producer]: Failed to send the raw processed data to Kafka.", level=logging.ERROR)
                payload.kafka_producer_error.append((producer_data, "raw-decompressed-can"))
                continue
        
        super().flush()
        log(f"[Data Producer]: Message batch for VIN {payload.vin} is processed.", level=logging.INFO)
    
    def convert_to_serializable(self, data_item):
        """
        Convert data including NamedSignalValue to serializable formats.
        """
        if isinstance(data_item, dict):
            return {key: self.convert_to_serializable(val) for key, val in data_item.items()}
        elif isinstance(data_item, list):
            return [self.convert_to_serializable(elem) for elem in data_item]
        elif isinstance(data_item, NamedSignalValue):
            return data_item.value if hasattr(data_item, 'value') else str(data_item)
        return data_item
    
    def create_key_for_raw_data(self, vin, event_time, raw_can_id):
        """
        Create a key by combining VIN and event time.
        """
        return f"{vin}_{event_time}_{raw_can_id}"