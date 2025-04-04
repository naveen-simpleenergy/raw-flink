from stages import CanDecodingStage
from utils  import KafkaConfig, MessagePayload, setup_flink_environment
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pathlib import Path
from stages import Decompressor
from stages import ValidationStage
from producer import KafkaSender
from dotenv import load_dotenv
load_dotenv()
import os

BASE_DIR = Path(__file__).parent
DBC_FILE_PATH = str(BASE_DIR / "dbc_files/SimpleOneGen1_V2_2.dbc")
JSON_FILE = str(BASE_DIR / "signalTopic.json")

def main():
    env = setup_flink_environment()
    kafka_source = KafkaConfig.create_kafka_source()
    kafka_output_config = KafkaConfig.get_kafka_producer_config()
    kafka_sender = KafkaSender(kafka_output_config, JSON_FILE)
    
    decompressor = Decompressor()
    validator = ValidationStage()
    can_decoding_stage = CanDecodingStage(DBC_FILE_PATH)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(5000))
    data_stream = env.from_source(source=kafka_source, watermark_strategy=watermark_strategy, source_name="Kafka Source")

    processed_stream = (data_stream
                        .map(MessagePayload, output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(decompressor.execute, output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(validator.execute, output_type=Types.PICKLED_BYTE_ARRAY())  
                        .map(can_decoding_stage.execute, output_type=Types.PICKLED_BYTE_ARRAY())
                        .map(kafka_sender, output_type=Types.STRING()))
    


    processed_stream.print() 

    env.execute("raw_Parser")

if __name__ == "__main__":

    main()
