import json
import logging
from logger import log
from interface import Stage

from utils.message_payload import MessagePayload

class ValidationStage(Stage):
    """
    A processing stage in a message processing pipeline that handles the parsing and validation of decompressed messages. 
    It ensures the integrity and structure of the data according to predefined schemas.
    """

    def execute(self, payload: MessagePayload):
        """
        Parses and validates the decompressed message contained in the payload.

        Args:
            payload (MessagePayload): The payload object containing the decompressed message to be processed.

        Raises:
            ValueError: If the message processing fails due to missing or malformed data.
        """
        try:
            payload.parsed = self.parse_json(payload.decompressed)
        except ValueError as ve:
            log("[ValidationStage]: ValueError during message processing.", level=logging.DEBUG, data=payload, error=ve)
            raise ve
        except Exception as e:
            log("[ValidationStage]: Unhandled exception in message processing.", level=logging.ERROR, exception=e, data=payload)
            raise ValueError(f"Unhandled exception: {e}")
        
    def parse_json(self, data: bytes) -> list:
        """
        Parses the JSON content from bytes and validates its structure according to a predefined schema.

        Args:
            data (bytes): The decompressed data to be parsed into JSON format.

        Returns:
            List[Dict[str, Any]]: A list of validated JSON objects.

        Raises:
            ValueError: If JSON parsing fails or if the data does not conform to the expected structure.
        """
        try:
            json_data = json.loads(data.decode('utf-8'))
            return [self.validate_json(item) for item in json_data]
        except json.JSONDecodeError as e:
            log("[ValidationStage]: Failed to parse and validate JSON data", level=logging.WARNING, exception=e, data=data)
            raise ValueError(f"JSON parsing failed: {e}")
        except ValueError as ve:
            log("[ValidationStage]: ValueError during JSON parsing.", level=logging.DEBUG, data={'data': data, 'error': str(ve)})
            raise
        except Exception as e:
            log("[ValidationStage]: Unhandled exception during message parsing and validation.", level=logging.ERROR, exception=e, data=data)
            raise ValueError(f"Unhandled exception during message parsing and validation: {e}")

    def validate_json(self, json_item: dict) -> dict:
        """
        Validates a single JSON item against the expected schema, checking for required keys and data integrity.

        Args:
            json_item (Dict[str, Any]): A single JSON object to validate.

        Returns:
            Dict[str, Any]: The validated JSON object.

        Raises:
            ValueError: If the JSON object is missing required keys or data does not meet specifications.
        """
        required_keys = ['can_raw', 'can_id', 'sequence', 'stream', 'timestamp']
        missing_keys = [key for key in required_keys if key not in json_item]
        if missing_keys:
            log(f"[ValidationStage]: JSON item is missing required keys.", level=logging.WARNING, data={'missing_keys': missing_keys, 'json_item': json_item})
            raise ValueError(f"Missing keys: {', '.join(missing_keys)}")
        
        if len(json_item['can_raw']) != 16:
            log("[ValidationStage]: Raw can data is not of length 16.", level=logging.WARNING, data=json_item)
            raise ValueError("Raw can data is not of length 16.")

        return {key: json_item[key] for key in required_keys if key in json_item}