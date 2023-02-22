import pandas as pd
import requests
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

ALL_PERCENT_MEDIAN_1DAY_YESTERDAY = 'https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport/daily/start_of_period/state=%22ut%22%20AND%20county=%22Beaver%22,%22Box%20Elder%22,%22Cache%22,%22Carbon%22,%22Daggett%22,%22Davis%22,%22Duchesne%22,%22Emery%22,%22Garfield%22,%22Grand%22,%22Iron%22,%22Juab%22,%22Kane%22,%22Millard%22,%22Morgan%22,%22Piute%22,%22Rich%22,%22Salt%20Lake%22,%22San%20Juan%22,%22Sanpete%22,%22Sevier%22,%22Summit%22,%22Tooele%22,%22Uintah%22,%22Utah%22,%22Wasatch%22,%22Washington%22,%22Wayne%22,%22Weber%22%20AND%20network=%22BOR%22%20AND%20element=%22RESC%22%20AND%20outServiceDate=%222100-01-01%22%7Cname/-1,-1/stationId,name,RESC::value,RESC::average_1991,latitude,longitude?fitToScreen=false'

def header_line_count(URL):
    page_text = requests.get(URL).text

    skip = 0

    for line in page_text:
        if line.startswith("#"):
            skip = skip + 1

    return skip

def generate_csv(URL):
    skip_first = header_line_count(URL)
    raw_df = pd.read_csv(URL, skiprows=skip_first)
    shape = raw_df.shape

    melted_df = pd.melt(raw_df,
              id_vars=raw_df.columns[[0]],
              value_vars=raw_df.columns[1:shape[1]-1], 
              var_name= 'Data', 
              value_name='Value')

    return melted_df

def upload_melted(melted_df, db):    
    Average = ""
    Today = ""
    Name = ""
    Latitude = ""
    Longitude = ""
    for index, row in melted_df.iterrows():
        ID = row['Data'][row['Data'].find('(')+1:row['Data'].find(')')]
        Date = row['Date']

        docs = db.collection(u'station_metadata').where(u'ID', u'==', ID).stream()
        
        for doc in docs:
            dict = doc.to_dict()
            Name = dict['Name']
            Latitude = dict['Latitude']
            Longitude = dict['Longitude']
            break

        if('Average' in row['Data']):
            Average = row['Value']
        else:
            Today = row['Value']

        if(Average != "" and Today != ""):
            doc_ref = db.collection(u'reservoir_levels').document(Date + ' ' + Name)

            doc_ref.set({
                u'ID': ID,
                u'Latitude': Latitude,
                u'Longitude': Longitude,
                u'Name': Name,
                u'Level': Today,
                u'Date': datetime.strptime(Date, '%Y-%m-%d')
            })
            Average = ""
            Today = ""

if __name__ == '__main__':
    cred = credentials.Certificate(//FILL IN WITH FIREBASE CREDENTIALS)
    firebase_admin.initialize_app(cred)

    db = firestore.client()

    melted = generate_csv(ALL_PERCENT_MEDIAN_1DAY_YESTERDAY)
    upload_melted(melted, db)

    print('Success!')



