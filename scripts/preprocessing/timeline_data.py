"""
Gets data required for timeline - cases by date and city

Start Date - datetime.datetime(2009, 4, 16, 0, 0)
End Date - datetime.datetime(2009, 6, 29, 0, 0)

Output (Sorted asc by `date`)
===================
[
    {
        "date": "6-12-2010",
        "cities": {
            "Aleppo": 1200,
            "Karachi": 2400,
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


def get_collection(mongo_conn_str):
    mongo_client = pymongo.MongoClient(mongo_conn_str)
    db = mongo_client.pandemic_db
    collection = db.patient_records
    return collection


def save_timeline_data(result):
    with open("../data/timeline.json", "w") as f:
        json.dump(result, f)


def get_timeline_data(mongo_conn_str):
    collection = get_collection(mongo_conn_str)
    start = datetime.datetime(2009, 4, 16)
    end = datetime.datetime(2009, 6, 29)

    result = []

    for date in arrow.Arrow.range('day', start, end):
        entry = {"date": str(date.date()), "cities": {}}
        for city in cities:
            cases_count = collection.count_documents({
                "admit_date": date.datetime,
                "city": city
            })
            deaths_count = collection.count_documents({
                "admit_date": date.datetime,
                "city": city,
                "death": True
            })
            entry["cities"][city] = {
                "cases": cases_count,
                "deaths": deaths_count
            }
            print(date.date(), city)
        result.append(entry)

    save_timeline_data(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Timeline data')
    parser.add_argument('--mongo_conn_str', type=str, required=True)

    args = parser.parse_args()
    get_timeline_data(args.mongo_conn_str)
