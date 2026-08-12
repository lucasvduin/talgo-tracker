import json
import csv
import os
from datetime import datetime, timezone, timedelta
from websocket import create_connection

CSV_FILENAME = "hamburg_cph_trains.csv"
WS_URL = "wss://api.mittog.dk/api/ws/departure/PA/dinstation/"
JSON_DIR = "jsons"

def get_logged_trains(filename):
    """Reads the CSV and returns a dictionary of rows keyed by (Train ID, Scheduled Date)."""
    logged_trains = {}
    
    if not os.path.exists(filename):
        return logged_trains
        
    with open(filename, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            logged_trains[(row.get("Train ID"), row.get("Scheduled Date"))] = row
                
    return logged_trains

def classify_train(unit_types):
    """Classifies the train type based on UnitType values."""
    ut_set = set(unit_types)
    
    if any(ut in ut_set for ut in ["MFU", "ER"]): return "IC3"
    elif any(ut in ut_set for ut in ["BPD", "APT", "BPT", "BPH"]): return "Talgo"
    elif any(ut in ut_set for ut in ["AFMPZ", "AMPZ", "BRMPZ", "BBMPZ", "BMPZ", "BDMPZ"]): return "Railjet"
    elif any(ut in ut_set for ut in ["BV", "BPX", "AV", "BVS", "BPB"]): return "German IC Coaches"
    elif ut_set == {"EB"}: return "Vectron-hauled"
    else: return "Unknown"

def main():
    # 1. Fetch Existing Data into a Dictionary
    logged_trains = get_logged_trains(CSV_FILENAME)
    
    # Use explicit timezone (CEST is UTC+2)
    tz_cest = timezone(timedelta(hours=2))
    current_time = datetime.now(tz_cest)
    today_prefix = current_time.strftime("%Y-%m-%d %H:%M:%S")

    # 2. Connect to WebSocket and retrieve payload
    print(f"Connecting to {WS_URL}...")
    try:
        ws = create_connection(WS_URL)
        result = ws.recv()
        ws.close()
    except Exception as e:
        print(f"Failed to connect or fetch data from WebSocket: {e}")
        return

    data = json.loads(result)
    
    # Save raw JSON to a file (for GitHub Artifacts)
    os.makedirs(JSON_DIR, exist_ok=True)
    json_filename = os.path.join(JSON_DIR, f"mittog_data_{current_time.strftime('%Y%m%d_%H%M%S')}.json")
    with open(json_filename, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Saved raw JSON payload to {json_filename}")

    trains = data.get("data", {}).get("Trains", [])

    # 3. Parse and Filter Trains
    for train in trains:
        product = train.get("Product", "")
        pub_id = train.get("PublicTrainId", "")
        
        # EXCLUDE rule
        if product in ["HM", "IC", "TRAINBUS"]:
            continue
            
        schedule_time_dep = train.get("ScheduleTimeDeparture", "")
        split_time = schedule_time_dep.split(" ")
        
        if len(split_time) == 2:
            sched_date, sched_time = split_time[0], split_time[1]
        else:
            sched_date, sched_time = "Unknown", "Unknown"

        # Parse Routes array
        routes = train.get("Routes", [])
        origins, destinations, unit_types, door_numbers = [], [], [], []
        
        for r in routes:
            orig = r.get("OriginStationId", "").replace("&", "")
            dest = r.get("DestinationStationId", "").replace("&", "")
            if orig and orig not in origins: origins.append(orig)
            if dest and dest not in destinations: destinations.append(dest)

            ut = r.get("UnitType")
            if ut: unit_types.append(ut)
                
            for d in r.get("Doors", []):
                num = d.get("Number")
                if num and num.isdigit():
                    door_numbers.append(int(num))

        origin_val = origins[0] if origins else "Unknown"
        dest_val = destinations[0] if destinations else "Unknown"
        
        # INCLUDE rule check
        valid_stations = {"HMB", "AP", "KH"}
        has_valid_station = bool(set(origins + destinations).intersection(valid_stations))
        contains_target_id = "39" in pub_id or "117" in pub_id
        
        if not (product == "EX" or has_valid_station or contains_target_id):
            continue

        # 4. Extract Specific Data
        changes_to = train.get("ChangesTo", [])
        is_cancelled_dep = train.get("IsCancelledDeparture", False)
        
        if changes_to:
            status = f"Cancelled (Replaced by {', '.join(changes_to)})"
        elif is_cancelled_dep:
            status = "Cancelled"
        else:
            status = "Scheduled"

        train_type = classify_train(unit_types)
        
        if door_numbers:
            carriage_info = f"{len(door_numbers)} cars (Nos. {min(door_numbers)}-{max(door_numbers)})"
        else:
            carriage_info = "0 cars"
            
        raw_units = " + ".join(unit_types)
        
        # Construct the updated row
        new_row = {
            "Timestamp": today_prefix,
            "Scheduled Date": sched_date,
            "Scheduled Time": sched_time,
            "Train ID": pub_id,
            "Status": status,
            "Origin": origin_val,
            "Destination": dest_val,
            "Train Type Classification": train_type,
            "Carriage Info": carriage_info,
            "Raw Units": raw_units
        }

        # Dict assignment overwrites stale data with fresh data
        logged_trains[(pub_id, sched_date)] = new_row

    # 5. Write the dictionary back to CSV
    if not logged_trains:
        print("No matching trains to log to CSV at this time.")
        return

    fieldnames = [
        "Timestamp", "Scheduled Date", "Scheduled Time", 
        "Train ID", "Status", "Origin", "Destination", 
        "Train Type Classification", "Carriage Info", "Raw Units"
    ]
    
    with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(logged_trains.values())
        
    print(f"Successfully updated CSV with {len(logged_trains)} total tracked train(s).")

if __name__ == "__main__":
    main()
    
