import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def read_and_upload(filename, db):
    metadata = pd.read_csv(filename)

    for index, row in metadata.iterrows():
            doc_ref = db.collection(u'station_metadata').document(row['ID'])

            doc_ref.set({
                u'Name': row['Name'],
                u'ID': row['ID'],
                u'State': row['State'],
                u'Network': row['Network'],
                u'County': row['County'],
                u'Elevation': row['Elevation_ft'],
                u'Latitude': row['Latitude'],
                u'Longitude': row['Longitude'],
            })


if __name__ == '__main__':
    firebase_admin.initialize_app()
    db = firestore.client()

    read_and_upload('./data/usda_all_stations_metadata.csv', db)

    print('Success!')