import json
import csv
import os

CSV_FILENAME = "hamburg_cph_trains.csv"
JSON_DIR = "jsons"

def classify_train(unit_types):
    ut_set = set(unit_types)
    if any(ut in ut_set for ut in ["MFU", "ER"]): return "IC3"
    elif any(ut in ut_set for ut in ["BPD", "APT", "BPT", "BPH"]): return "Talgo"
    elif any(ut in ut_set for ut in ["AFMPZ", "AMPZ", "BRMPZ", "BBMPZ", "BMPZ", "BDMPZ"]): return "Railjet"
    elif any(ut in ut_set for ut in ["BV", "BPX", "AV", "BVS", "BPB"]): return "German IC Coaches"
    elif ut_set == {"EB"}: return "Vectron-hauled"
    else: return "Unknown"

def fix_csv():
    if not os.path.exists(JSON_DIR):
        print(f"No {JSON_DIR} directory found. Cannot rebuild history.")
        return
        
    # Get all json files and sort them alphabetically (chronological by filename formatting)
    json_files = sorted([f for f in os.listdir(JSON_DIR) if f.endswith(".json")])
    
    if not json_files:
        print("No JSON files found in directory.")
        return

    logged_trains = {}
    print(f"Rebuilding CSV from {len(json_files)} historical JSON files...")

    for filename in json_files:
        filepath = os.path.join(JSON_DIR, filename)
        
        # Extract pseudo-timestamp from filename (e.g., mittog_data_20240520_140000.json)
        try:
            time_part = filename.replace("mittog_data_", "").replace(".json", "")
            formatted_time = f"{time_part[:4]}-{time_part[4:6]}-{time_part[6:8]} {time_part[9:11]}:{time_part[11:13]}:{time_part[13:15]}"
        except:
            formatted_time = "Historical File"

        with open(filepath, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Skipping {filename}, invalid JSON.")
                continue

        trains = data.get("data", {}).get("Trains", [])

        for train in trains:
            product = train.get("Product", "")
            pub_id = train.get("PublicTrainId", "")
            
            if product in ["HM", "IC", "TRAINBUS"]:
                continue
                
            schedule_time_dep = train.get("ScheduleTimeDeparture", "")
            split_time = schedule_time_dep.split(" ")
            
            if len(split_time) == 2:
                sched_date, sched_time = split_time[0], split_time[1]
            else:
                sched_date, sched_time = "Unknown", "Unknown"

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
            
            valid_stations = {"HMB", "AP", "KH"}
            has_valid_station = bool(set(origins + destinations).intersection(valid_stations))
            contains_target_id = "39" in pub_id or "117" in pub_id
            
            if not (product == "EX" or has_valid_station or contains_target_id):
                continue

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
            
            new_row = {
                "Timestamp": formatted_time,
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

            # Overwrites older records with the most recent status found in the historical JSONs
            logged_trains[(pub_id, sched_date)] = new_row

    # Write rebuilt data back to CSV
    fieldnames = ["Timestamp", "Scheduled Date", "Scheduled Time", "Train ID", "Status", "Origin", "Destination", "Train Type Classification", "Carriage Info", "Raw Units"]
    
    with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(logged_trains.values())
        
    print(f"Successfully rebuilt historical CSV with {len(logged_trains)} perfectly deduplicated entries!")

if __name__ == "__main__":
    fix_csv()
    
