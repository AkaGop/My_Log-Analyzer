# log_parser.py

import re
from io import StringIO
from config import CEID_MAP, RPTID_MAP

def _parse_s6f11_report(full_text: str) -> dict:
    """
    Final, robust, and simple parser for S6F11.
    It tokenizes the message and maps values by their documented order.
    """
    data = {}
    
    # --- START OF HIGHLIGHTED FIX ---

    # Step 1: Tokenize the entire data block into a flat list of primitive values.
    # This captures both strings in quotes and numbers.
    values = re.findall(r"<(?:A|U\d)\s\[\d+\]\s(?:'([^']*)'|(\d+))>", full_text)
    flat_values = [s if s else i for s, i in values]

    # An S6F11 must have at least DATAID, CEID, and RPTID.
    if len(flat_values) < 3:
        return {}

    # Step 2: Map values directly based on the stable SECS-II S6F11 structure.
    try:
        ceid = int(flat_values[1])  # The 2nd token is always CEID
        rptid = int(flat_values[2]) # The 3rd token is always RPTID
    except (ValueError, IndexError):
        return {} # Handle malformed data

    # Step 3: Populate the dictionary if the IDs are known.
    if ceid in CEID_MAP:
        data['CEID'] = ceid
        if "Alarm" in CEID_MAP.get(ceid, ''):
            data['AlarmID'] = ceid
    
    if rptid in RPTID_MAP:
        data['RPTID'] = rptid
        # The actual data payload starts from the 4th token onwards.
        data_payload = flat_values[3:]
        
        # Map the payload to the schema from our config.
        field_names = RPTID_MAP.get(rptid, [])
        for i, name in enumerate(field_names):
            if i < len(data_payload):
                data[name] = data_payload[i]
            
    # --- END OF HIGHLIGHTED FIX ---
            
    return data

def _parse_s2f49_command(full_text: str) -> dict:
    """Parses S2F49 Remote Commands (This function is correct)."""
    data = {}
    rcmd_match = re.search(r"<\s*A\s*\[\d+\]\s*'([A-Z_]{5,})'", full_text)
    if rcmd_match: data['RCMD'] = rcmd_match.group(1)
    lotid_match = re.search(r"'LOTID'\s*>\s*<A\[\d+\]\s*'([^']*)'", full_text, re.IGNORECASE)
    if lotid_match: data['LotID'] = lotid_match.group(1)
    panels_match = re.search(r"'LOTPANELS'\s*>\s*<L\s\[(\d+)\]", full_text, re.IGNORECASE)
    if panels_match: data['PanelCount'] = int(panels_match.group(1))
    return data

def parse_log_file(uploaded_file):
    events = []
    if not uploaded_file: return events
    try: stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
    except UnicodeDecodeError: stringio = StringIO(uploaded_file.getvalue().decode("latin-1", errors='ignore'))
    
    lines = [line for line in stringio.readlines() if line.strip()]
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        header_match = re.match(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+),\[([^\]]+)\],(.*)", line)
        if not header_match: i += 1; continue
        timestamp, log_type, message_part = header_match.groups()
        msg_match = re.search(r"MessageName=(\w+)|Message=.*?:\'(\w+)\'", message_part)
        msg_name = (msg_match.group(1) or msg_match.group(2)) if msg_match else "N/A"
        event = {"timestamp": timestamp, "log_type": log_type, "msg_name": msg_name}
        if ("Core:Send" in log_type or "Core:Receive" in log_type) and i + 1 < len(lines) and lines[i+1].strip().startswith('<'):
            j = i + 1
            data_block_lines = []
            while j < len(lines) and lines[j].strip() != '.':
                data_block_lines.append(lines[j]); j += 1
            i = j
            if data_block_lines:
                full_data_block_text = "".join(data_block_lines)
                details = {}
                if msg_name == 'S6F11': details = _parse_s6f11_report(full_data_block_text)
                elif msg_name == 'S2F49': details = _parse_s2f49_command(full_data_block_text)
                if details: event['details'] = details
        if 'details' in event:
            events.append(event)
        i += 1
    return events
