import pandas as pd
import pymongo
import json
from datetime import datetime
import os
import time

def parse_date2(date_str):
    """Converts date string in YYYY-MM-DD format to a Python datetime object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception as e:
        print(f"Date parsing error: {e} for {date_str}")
        return None
    
def parse_date(date_str):
    """Converts date string in DD/MM/YYYY format to a Python datetime object."""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except Exception as e:
        print(f"Date parsing error: {e} for {date_str}")
        return None

    # # Group by 'patient_id' to create nested documents.
    # patients = []
    # for patient_id, group in df.groupby("patient_id"):
    #     # We assume all rows share the same patient-level data.
    #     first_record = group.iloc[0]
    #     patient_doc = {
    #         "patient_id": patient_id,
    #         "name": {
    #             "first": first_record["first"],
    #             "last": first_record["last"]
    #         },
    #         "date_of_birth": parse_date(str(first_record["date_of_birth"])),
    #         "gender": first_record["gender"],
    #         "medical_records": []
    #     }
        
    #     # For each record, build the nested medical record document.
    #     for _, row in group.iterrows():
    #         record = {
    #             "record_id": row["record_id"],
    #             "diagnosis": row["diagnosis"],
    #             "treatment": row["treatment"],
    #             "prescriptions": [{
    #                 "drug_name": row["drug_name"],
    #                 "dosage": row["dosage"],
    #                 "frequency": row["frequency"]
    #             }],
    #             "doctor": {
    #                 "id": row["doctor_id"],
    #                 "name": row["doctor_name"],
    #                 "specialization": row["specialization"]
    #             },
    #             "visit_date": parse_date(str(row["visit_date"]))
    #         }
    #         patient_doc["medical_records"].append(record)
        
    #     patients.append(patient_doc)
    
    # # Insert the patient documents into MongoDB.
    # if patients:
    #     result = patients_collection.insert_many(patients)
    #     print(f"Inserted document ids: {result.inserted_ids}")
    # else:
    #     print("No data to insert.")

    # # --- Sample Query 1: Find patients with a specific diagnosis (e.g., 'Diabetes') ---
    # print("\nPatients with diagnosis Diabetes:")
    # diabetes_cursor = patients_collection.find({"medical_records.diagnosis": "Diabetes"})
    # for doc in diabetes_cursor:
    #     print(doc)
    
    # # --- Sample Query 2: Aggregation to count visits per doctor ---
    # print("\nCount of visits per doctor:")
    # pipeline = [
    #     {"$unwind": "$medical_records"},
    #     {"$group": {"_id": "$medical_records.doctor.name", "total_visits": {"$sum": 1}}}
    # ]
    # agg_result = list(patients_collection.aggregate(pipeline))
    # for result in agg_result:
    #     print(result)

def connect_to_mongo():
    """Retries connection to MongoDB until successful."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://medical_mongo:27017/")
    retries = 5  # Number of retries
    delay = 5  # Seconds to wait between retries

    for attempt in range(retries):
        try:
            client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.server_info()  # Force connection test
            print("Connected to MongoDB successfully!")
            return client
        except pymongo.errors.ServerSelectionTimeoutError:
            print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
            time.sleep(delay)

    print("Failed to connect to MongoDB. Exiting.")
    exit(1)

def import_patients(db):
    """Reads CSV and inserts patient records into MongoDB."""
    file_path = "/app/data/healthcare_dataset-20250506.csv"
    try:
        df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")
        patient_records = []

        for _, row in df.iterrows():
            patient_doc = {
                "name": row["Name"],
                "age": int(row["Age"]),
                "gender": row["Gender"],
                "blood_type": row["Blood Type"],
                "medical_condition": row["Medical Condition"],
                "date_of_admission": parse_date(row["Date of Admission"]),
                "doctor": row["Doctor"],
                "hospital": row["Hospital"],
                "insurance_provider": row["Insurance Provider"],
                "billing_amount": float(row["Billing Amount"]),
                "room_number": row["Room Number"],
                "admission_type": row["Admission Type"],
                "discharge_date": parse_date(row["Discharge Date"]),
                "medication": row["Medication"],
                "test_results": row["Test Results"]
            }
            patient_records.append(patient_doc)

        db["patients2"].insert_many(patient_records)
        print(f"Inserted {len(patient_records)} records into `patients2` collection.")
    except Exception as e:
        print(f"Error importing patients: {e}")

def import_restaurants(db):
    """Reads JSON and inserts restaurant records into MongoDB."""
    file_path = "/app/data/restaurants.json"
    try:
        with open(file_path, "r", encoding="utf-8") as json_file:
            restaurant_records = json.load(json_file)

        db["restaurants"].insert_many(restaurant_records)
        print(f"Inserted {len(restaurant_records)} records into `restaurants` collection.")
    except Exception as e:
        print(f"Error importing restaurants: {e}")

def main():
    """Main function to manage data imports."""
    client = connect_to_mongo()
    db = client["medical_db"]

    import_patients(db)
    import_restaurants(db)

if __name__ == "__main__":
    main()
