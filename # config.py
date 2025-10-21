# config.py

"""
Single source of truth for all static configuration data, knowledge bases, and report definitions.
"""

SECS_MAP = {
    "S1F1": "Are You There Request", "S1F2": "Are You There Data",
    "S1F3": "Selected Equipment Status Request", "S1F4": "Selected Equipment Status Data",
    "S2F49": "Enhanced Remote Command", "S2F50": "Enhanced Remote Command Acknowledge",
    "S6F11": "Event Report Send", "S6F12": "Event Report Acknowledge",
}

CEID_MAP = {
    11: "GemEquipmentOFFLINE", 12: "GemControlStateLOCAL", 13: "GemControlStateREMOTE",
    14: "GemMsgRecognition", 16: "GemPPChangeEvent", 30: "GemProcessStateChange",
    101: "AlarmClear", 102: "AlarmSet", 18: "AlarmSet", 113: "AlarmSet", 114: "AlarmSet",
    120: "IDRead", 121: "UnloadedFromMag/LoadedToTool", 127: "LoadedToTool",
    131: "LoadToToolCompleted", 132: "UnloadFromToolCompleted", 136: "MappingCompleted",
    141: "PortStatusChange", 151: "MagazineDocked", 180: "RequestMagazineDock",
    181: "MagazineDocked", 182: "MagazineUndocked", 183: "RequestOperatorIdCheck",
    184: "RequestOperatorLogin", 185: "RequestMappingCheck",
}

RPTID_MAP = {
    152: ['Timestamp', 'OperatorID'],
    150: ['Timestamp', 'MagazineID'],
    151: ['Timestamp', 'PortID', 'MagazineID', 'OperatorID'],
    141: ['Timestamp', 'PortID', 'PortStatus'],
    120: ['Timestamp', 'LotID', 'PanelID', 'Orientation', 'ResultCode', 'SlotID'],
    121: ['Timestamp', 'LotID', 'PanelID', 'SlotID'],
    122: ['Timestamp', 'LotID', 'SourcePortID', 'DestPortID', 'PanelList'],
    11:  ['Timestamp', 'ControlState'],
    101: ['Timestamp', 'AlarmID'],
}
