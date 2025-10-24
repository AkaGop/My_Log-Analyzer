# analyzer.py
from datetime import datetime
import pandas as pd
from collections import Counter
from config import ALARM_MAP, CRITICAL_ALARM_IDS

def find_precursor_patterns(df: pd.DataFrame, window_size: int = 5) -> pd.DataFrame:
    if 'details.AlarmID' not in df.columns: return pd.DataFrame()
    df['AlarmID_numeric'] = pd.to_numeric(df['details.AlarmID'], errors='coerce')
    critical_indices = df[df['AlarmID_numeric'].isin(CRITICAL_ALARM_IDS)].index.tolist()
    precursor_sequences = []
    for idx in critical_indices:
        start = max(0, idx - window_size)
        window_df = df.iloc[start:idx]
        warnings = window_df[(window_df['AlarmID_numeric'].notna()) & (~window_df['AlarmID_numeric'].isin(CRITICAL_ALARM_IDS))]
        if not warnings.empty:
            sequence = tuple(warnings['EventName'].tolist())
            failed_alarm_id = int(df.loc[idx, 'AlarmID_numeric'])
            failed_alarm_name = ALARM_MAP.get(failed_alarm_id, f"Critical Alarm {failed_alarm_id}")
            precursor_sequences.append({"Pattern": " -> ".join(sequence), "Leads_To_Failure": failed_alarm_name})
    if not precursor_sequences: return pd.DataFrame()
    pattern_counts = Counter((seq['Pattern'], seq['Leads_To_Failure']) for seq in precursor_sequences)
    result = [{"Precursor Pattern": p, "Leads to Failure": f, "Occurrences": c} for (p, f), c in pattern_counts.items()]
    return pd.DataFrame(result).sort_values(by="Occurrences", ascending=False)

def perform_eda(df: pd.DataFrame) -> dict:
    eda_results = {}
    if 'EventName' in df.columns: eda_results['event_counts'] = df['EventName'].value_counts()
    else: eda_results['event_counts'] = pd.Series(dtype='int64')
    if 'details.AlarmID' in df.columns:
        alarm_events = df[df['details.AlarmID'].notna()].copy()
        if not alarm_events.empty:
            alarm_ids = pd.to_numeric(alarm_events['details.AlarmID'], errors='coerce').dropna()
            eda_results['alarm_counts'] = alarm_ids.value_counts()
            eda_results['alarm_table'] = alarm_events[['timestamp', 'EventName', 'details.AlarmID']]
        else: eda_results['alarm_counts'] = pd.Series(dtype='int64'); eda_results['alarm_table'] = pd.DataFrame()
    else: eda_results['alarm_counts'] = pd.Series(dtype='int64'); eda_results['alarm_table'] = pd.DataFrame()
    return eda_results

def analyze_data(events: list) -> dict:
    summary = {
        "operators": set(), "magazines": set(), "lot_id": "N/A", "panel_count": 0,
        "job_start_time": "N/A", "job_end_time": "N/A", "total_duration_sec": 0.0,
        "avg_cycle_time_sec": 0.0, "job_status": "No Job Found", "control_state_changes": []
    }
    if not events: return summary
    start_event = next((e for e in events if e.get('details', {}).get('RCMD') == 'LOADSTART'), None)
    if start_event:
        summary['lot_id'] = start_event['details'].get('LotID', 'N/A')
        if not summary['lot_id'] or summary['lot_id'] == 'N/A':
            summary['lot_id'] = next((e['details'].get('LotID') for e in events if e.get('details', {}).get('LotID')), 'N/A')
        try: summary['panel_count'] = int(start_event['details'].get('PanelCount', 0))
        except: summary['panel_count'] = 0
        summary['job_start_time'] = start_event['timestamp']
        summary['job_status'] = "Started but did not complete"
        start_index = events.index(start_event)
        end_event = next((e for e in events[start_index:] if e.get('details', {}).get('CEID') in [131, 132]), None)
        if end_event:
            summary['job_status'] = "Completed"
            try:
                t_start = datetime.strptime(start_event['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
                t_end = datetime.strptime(end_event['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
                duration = (t_end - t_start).total_seconds()
                if duration >= 0:
                    summary['total_duration_sec'] = round(duration, 2)
                    if summary['panel_count'] > 0:
                        summary['avg_cycle_time_sec'] = round(duration / summary['panel_count'], 2)
            except: summary['job_status'] = "Time Calculation Error"
    else:
        if any(e.get('details', {}).get('CEID') in [120, 127] for e in events):
            summary['lot_id'] = "Dummy/Test Panels"
    for event in events:
        details = event.get('details', {})
        if details.get('OperatorID'): summary['operators'].add(details['OperatorID'])
        if details.get('MagazineID'): summary['magazines'].add(details['MagazineID'])
        ceid = details.get('CEID')
        if ceid == 12: summary['control_state_changes'].append({"Timestamp": event['timestamp'], "State": "LOCAL"})
        elif ceid == 13: summary['control_state_changes'].append({"Timestamp": event['timestamp'], "State": "REMOTE"})
    return summary
