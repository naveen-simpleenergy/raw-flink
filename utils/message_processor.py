import json
import logging
from logger import log
from utils import DataProcessor
from interface import Stage
from typing import Tuple, Any

class MessageProcessor(Stage):
    """
    Processes binary messages to extract and validate JSON data.

    This class focuses on decoding, decompressing, and validating binary message content.
    """

    def execute(self, binary_message: bytes) -> Tuple[dict, str]:
        """
        Decodes and processes a binary message.

        Args:
            binary_message (bytes): The binary message to be processed.

        Returns:
            Tuple[dict, str]: A tuple containing the processed JSON data and username.

        Raises:
            ValueError: If the message processing fails at any step.
        """
        try:
            message_json = json.loads(binary_message)
            if 'payload' not in message_json or 'username' not in message_json:
                log("Payload or username missing in the message.", level=logging.ERROR, data=message_json)
                raise ValueError("Payload or username missing in the message.")
            payload_decompressed = DataProcessor.decompress_base64(message_json['payload'])
            json_data = DataProcessor.parse_and_validate_json(payload_decompressed)
            return json_data, message_json['username']
        except json.JSONDecodeError:
            log("JSON decoding failed for binary message.", level=logging.WARNING, data=binary_message)
            raise ValueError("JSON decoding failed for binary message.")
        except ValueError as ve:
            log("ValueError during message processing.", level=logging.DEBUG, data={'binary_message': binary_message, 'error': str(ve)})
            raise ve
        except Exception as e:
            log("Unhandled exception in message processing.", level=logging.ERROR, exception=e, data=binary_message)
            raise ValueError(f"Unhandled exception: {e}")