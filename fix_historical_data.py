import csv
import os

CSV_FILENAME = "hamburg_cph_trains.csv"
TEMP_FILENAME = "hamburg_cph_trains_temp.csv"

def classify_train(unit_types):
    ut_set = set(unit_types)
    if any(ut in ut_set for ut in ["MFU", "ER"]):
        return "IC3"
    elif any(ut in ut_set for ut in ["BPD", "APT", "BPT", "BPH"]):
        return "Talgo"
    elif any(ut in ut_set for ut in ["AFMPZ", "AMPZ", "BRMPZ", "BBMPZ", "BMPZ", "BDMPZ"]):
        return "Railjet"
    elif any(ut in ut_set for ut in ["BV", "BPX", "AV", "BVS", "BPB"]):
        return "German IC Coaches"
    elif ut_set == {"EB"}:
        return "Vectron-hauled"
    else:
        return "Unknown"

def fix_csv():
    if not os.path.exists(CSV_FILENAME):
        print("CSV not found.")
        return

    updated_rows = []
    headers = []

    # Read the existing data
    with open(CSV_FILENAME, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        headers = reader.fieldnames
        for row in reader:
            # If it's currently Unknown, try to reclassify based on Raw Units
            if row["Train Type Classification"] == "Unknown" and row["Raw Units"]:
                unit_types = [u.strip() for u in row["Raw Units"].split("+")]
                row["Train Type Classification"] = classify_train(unit_types)
            updated_rows.append(row)

    # Write the updated data back to the file
    with open(CSV_FILENAME, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=";")
        writer.writeheader()
        writer.writerows(updated_rows)

    print("Successfully updated historical CSV entries!")

if __name__ == "__main__":
    fix_csv()
