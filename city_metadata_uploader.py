import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def read_and_upload(filename, db):
    metadata = pd.read_csv(filename)

    for index, row in metadata.iterrows():
            doc_ref = db.collection(u'city_metadata').document(row['Name'])

            doc_ref.set({
                u'Name': row['Name'],
                u'Latitude': row['Latitude'],
                u'Longitude': row['Longitude'],
                u'County': row['County']
            })


if __name__ == '__main__':
    cred = credentials.Certificate(//FILL IN WITH FIREBASE CREDENTIALS)
    firebase_admin.initialize_app(cred)
   
    db = firestore.client()

    read_and_upload('./data/utah_all_cities.csv', db)

    print('Success!')