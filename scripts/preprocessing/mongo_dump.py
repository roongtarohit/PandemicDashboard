"""
Dump data from VAST 2010 MC2 Data files to MongoDB

Prerequisites:
=========================
  * VAST 2010 MC2 Data files
  * Symptom Mappings

Mongo Document Format:
=========================
    {
        "patient_id": 23686,
        "age": 45,
        "gender": "F",
        "symptom_text": "ABD PAIN,VOMITING",
        "symptoms": [
            "Abdominal Pain",
            "Vomitting"
        ],
        "admit_date": "4-16-2009",
        "death": True,
        "death_date": "4-20-2009",
        "city": "Aleppo"
    }

Example Usage:
=========================
python mongo_dump.py \ 
    --vast_path "/Users/nowke/Projects/data-viz/VAST2010-Mini-2-Data Files-v2-04-03" \
    --symptom_map "/Users/nowke/Projects/data-viz/pandemic-dashboard/scripts/symptom_mappings/mapping.json" \
    --mongo_conn_str "mongodb://localhost:27017"
"""
import argparse
import arrow
import csv
import json
import pymongo


hospital_record_files = [
    {"city": "Aleppo", "admit": "Aleppo.csv", "deaths": "Aleppo-deaths.csv"},
    {"city": "Colombia", "admit": "Colombia.csv", "deaths": "Colombia-deaths.csv"},
    {"city": "Iran", "admit": "Iran.csv", "deaths": "Iran-deaths.csv"},
    {"city": "Karachi", "admit": "Karachi.csv", "deaths": "Karachi-deaths.csv"},
    {"city": "Lebanon", "admit": "Lebanon.csv", "deaths": "Lebanon-deaths.csv"},
    {"city": "Nairobi", "admit": "Nairobi.csv", "deaths": "Nairobi-deaths.csv"},
    {"city": "Saudi", "admit": "Saudi Arabia.csv",
        "deaths": "Saudi Arabia-deaths.csv"},
    {"city": "Thailand", "admit": "Thailand.csv", "deaths": "Thailand-deaths.csv"},
    {"city": "Turkey", "admit": "Turkey.csv", "deaths": "Turkey-deaths.csv"},
    {"city": "Venezuela", "admit": "Venezuela.csv",
        "deaths": "Venezuela-deaths.csv"},
    {"city": "Yemen", "admit": "Yemen.csv", "deaths": "Yemen-deaths.csv"}
]


def get_symptom_mapping(symptom_map_file):
    with open(symptom_map_file, 'r') as f:
        mapping = json.load(f)
        return mapping


def get_death_mapping(vast_path, death_record_file):
    mapping = {}
    with open(f'{vast_path}/{death_record_file}', 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            mapping[row['ID']] = {
                'death': row['DATE_OF_DEATH']
            }

    return mapping


def save_city_data(mongo_conn_str, vast_path, record_file, death_map, symptom_mapping):
    mongo_client = pymongo.MongoClient(mongo_conn_str)
    db = mongo_client.pandemic_db
    collection = db.patient_records

    with open(f'{vast_path}/{record_file["admit"]}', 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        record_count = 0
        batch_documents = []

        for row in csv_reader:
            symptoms = symptom_mapping[row["SYNDROME"]]
            if symptoms:
                is_death = row["PATIENT_ID"] in death_map
                if is_death:
                    death_date = arrow.get(
                        death_map[row["PATIENT_ID"]]["death"],
                        'M-D-YYYY'
                    ).datetime
                else:
                    death_date = None
                document = {
                    "patient_id": int(row["PATIENT_ID"]),
                    "age": int(row["AGE"]),
                    "gender": row["GENDER"],
                    "symptom_text": row["SYNDROME"],
                    "symptoms": symptom_mapping[row["SYNDROME"]],
                    "admit_date": arrow.get(row["DATE"], 'M-D-YYYY').datetime,
                    "death": is_death,
                    "death_date": death_date,
                    "city": record_file["city"]
                }
                batch_documents.append(document)
            record_count += 1

            if record_count % 10000 == 0:
                # Perform bulk insert and reset `batch_documents`
                print(f"Record {record_count}")
                collection.insert_many(batch_documents)
                batch_documents = []


def mongo_dump(mongo_conn_str, vast_path, symptom_map_file):
    symptom_mapping = get_symptom_mapping(symptom_map_file)
    for record_file in hospital_record_files:
        print(f"Processing city - {record_file['city']}")
        print("==============================")

        death_map = get_death_mapping(vast_path, record_file["deaths"])
        save_city_data(mongo_conn_str, vast_path, record_file,
                       death_map, symptom_mapping)

        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mongo Dumper')
    parser.add_argument('--vast_path', type=str, required=True)
    parser.add_argument('--symptom_map', type=str, required=True)
    parser.add_argument('--mongo_conn_str', type=str, required=True)

    args = parser.parse_args()
    mongo_dump(args.mongo_conn_str, args.vast_path, args.symptom_map)
