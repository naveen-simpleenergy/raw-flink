import logging
import zlib
import base64
from logger import log
from interface import Stage
from utils.message_payload import MessagePayload

class Decompressor(Stage):
    """
    Processes binary messages by decompressing base64-encoded data and managing errors associated with decompression.

    Methods:
        execute: Processes the payload by decompressing its data and handling errors.
        decompress_base64: Decompresses base64-encoded data.
    """
    def execute(self, payload: MessagePayload):
        """
        Processes a given MessagePayload by decompressing its contained data.

        Args:
            payload (MessagePayload): The message payload object containing compressed data.

        Raises:
            ValueError: If essential fields are missing in the payload or if decompression fails.
        """
        try:
            if not payload.compressed or not payload.vin:
                log("[DecompressStage]: Payload or username missing in the message.", level=logging.ERROR, data=payload)
                raise ValueError("Payload or username missing in the message.")
            
            payload.decompressed = self.decompress_base64(payload.compressed)
        except ValueError as ve:
            log("[DecompressStage]: ValueError during message processing.", level=logging.DEBUG, data=payload)
            raise ve
        except Exception as e:
            log("[DecompressStage]: Unhandled exception in message processing.", level=logging.ERROR, exception=e, data=payload)
            raise ValueError(f"Unhandled exception: {e}")
        
    def decompress_base64(self, compressed_data_base64: str) -> bytes:
        """
        Decompresses a base64 encoded string.

        Args:
            compressed_data_base64 (str): The base64 encoded compressed data.

        Returns:
            bytes: The decompressed data bytes.

        Raises:
            ValueError: If decompression fails due to incorrect encoding or corruption.
        """
        try:
            compressed_data = base64.b64decode(compressed_data_base64)
            decompressed_data = zlib.decompress(compressed_data, zlib.MAX_WBITS)
            return decompressed_data
        except (zlib.error, base64.binascii.Error) as e:
            log("[DecompressStage]: Decompression failed for base64 data.", level=logging.ERROR, exception=e, data={'compressed_data_base64': compressed_data_base64})
            raise ValueError(f"Decompression failure: {e}")