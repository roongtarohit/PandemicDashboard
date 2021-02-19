"""
Gets data required for symptoms - symptoms data by age group, date, city

Start Date - datetime.datetime(2009, 4, 16, 0, 0)
End Date - datetime.datetime(2009, 6, 29, 0, 0)

Output (Sorted asc by `date`)
===================
[
    {
        "date": "6-12-2010",
        "cities": {
            "Aleppo": {
              "0": { "Fever": 15, "Abdominal Pain": 30, ... },
              "21": { "Fever": 15, "Abdominal Pain": 30, ... },
              "41": { "Fever": 15, "Abdominal Pain": 30, ... },
            },
            ...
        }
    },
    {
        ...
    }
]
"""
import argparse
import arrow
import datetime
import json
import pymongo

cities = ["Aleppo", "Colombia", "Iran", "Karachi", "Lebanon",
          "Nairobi", "Saudi", "Thailand", "Turkey", "Venezuela", "Yemen"]
age_groups = [
    {"start": 0, "end": 20},
    {"start": 21, "end": 40},
    {"start": 41, "end": 60},
    {"start": 61, "end": 100},
]
syndromes = [
    "Abdominal Pain",
    "Back Pain",
    "Bleeding Nose",
    "Diarrhoea",
    "Fever",
    "Headache",
    "Rash",
    "Vision Problems",
    "Vomitting"
]


def get_collection(mongo_conn_str):
    mongo_client = pymongo.MongoClient(mongo_conn_str)
    db = mongo_client.pandemic_db
    collection = db.patient_records
    return collection


def save_symptoms_data(result):
    with open("../data/symptoms.json", "w") as f:
        json.dump(result, f)


def get_symptoms_data(mongo_conn_str):
    collection = get_collection(mongo_conn_str)
    start = datetime.datetime(2009, 4, 16)
    end = datetime.datetime(2009, 6, 29)

    result = []

    for date in arrow.Arrow.range('day', start, end):
        entry = {"date": str(date.date()), "cities": {}}
        for city in cities:
            doc = {}
            for age_group in age_groups:
                syndrome_data = {}
                for syndrome in syndromes:
                    documents = collection.count_documents({
                        "admit_date": date.datetime,
                        "city": city,
                        "age": {
                            "$gte": age_group["start"],
                            "$lte": age_group["end"]
                        },
                        "symptoms": syndrome
                    })
                    syndrome_data[syndrome] = documents
                
                doc[age_group["start"]] = syndrome_data

            entry["cities"][city] = doc
            print(date.date(), city)
        result.append(entry)

    save_symptoms_data(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Symptoms data')
    parser.add_argument('--mongo_conn_str', type=str, required=True)

    args = parser.parse_args()
    get_symptoms_data(args.mongo_conn_str)
