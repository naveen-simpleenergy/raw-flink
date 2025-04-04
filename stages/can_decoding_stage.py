import cantools
from logger import log
from interface import Stage
from typing import Dict
from utils.message_payload import MessagePayload

import logging

class CanDecodingStage(Stage):
    """
    CanDecodingStage is a stage in a processing pipeline that decodes CAN messages
    using a specified DBC file.
    """
    def __init__(self, dbc:cantools):
        """
        Initialize CanDecodingStage with a path to a DBC file.

        Args:
            dbc (cantools): A DBC file loaded using the cantools library.
        """
        self.db = dbc
    
    def execute(self, payload: MessagePayload):
        """
        Decode a list of CAN messages.

        Args:
            messages (Tuple[List[Dict], str]): A tuple containing a list of raw messages and VIN.

        Returns:
            Tuple[List[Tuple[Any, int, Dict]], str, List[Dict]]: A tuple containing a list of decoded messages
            along with their CAN ID and raw CAN data, the VIN, and a list of any decoding errors.

        Raises:
            ValueError: If the raw message structure is invalid.
        """
        successful_decodings = []
        errors = []
        dbc_key_error = []
        
        for raw_message in payload.parsed:
            try:
                can_id_hex = self.extract_can_id(raw_message['can_id'])
                if not self.is_can_id_known(can_id_hex):
                    raise KeyError(f"CAN ID {raw_message['can_id']} not found in DBC file.")
                
                raw_can_data_for_producer = self.construct_raw_can_data(raw_message, payload.vin)
                successful_decodings.append(raw_can_data_for_producer)
            except KeyError as ke:
                dbc_key_error.append(raw_message)
                log(f"[CanDecodingStage]: CAN ID not found error.", level=logging.WARNING, exception=ke, data=raw_message)
            except Exception as e:
                errors.append(raw_message)
                log(f"[CanDecodingStage]: Failed to decode message.", level=logging.CRITICAL, exception=e, data=raw_message)
                
        payload.can_decoded_data = successful_decodings
        payload.can_decoding_errors = {
            'dbc': dbc_key_error,
            'others': errors,
        }

    def construct_raw_can_data(self, raw_message: Dict, vin) -> Dict:
        """
        Construct a dictionary representing raw CAN data from a raw message.
        """
        return {
            "vin": vin,
            "raw_can_id": hex(int(raw_message['can_id'], 16)),
            "event_time": int(raw_message['timestamp'], 16),
            **{f"byte{i+1}": int(raw_message['can_raw'][i*2:(i+1)*2], 16) for i in range(len(raw_message['can_raw'])//2)}
        }
    
    def extract_can_id(self, can_id_hex: str) -> int:
        """
        Extract a 29-bit CAN ID from a hexadecimal CAN ID.
        """
        return hex(int(str(can_id_hex), 16))  # int(can_id_hex, 16) & 0x1FFFFFFF
    
    def is_can_id_known(self, can_id_hex: int) -> bool:
        """
        Check if a CAN ID exists in the DBC file.
        """
        try:
            can_id_29bit = int(str(can_id_hex), 16) & 0x1FFFFFFF
            self.db.get_message_by_frame_id(can_id_29bit)
            return True
        except KeyError:
            return False
