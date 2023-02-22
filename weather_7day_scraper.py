import pandas as pd
import requests
import datetime
from datetime import date
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import calendar

WEATHER_API_LINK1 = 'https://api.open-meteo.com/v1/forecast?latitude='
WEATHER_API_LINK2 = '&longitude='
WEATHER_API_LINK3 = '&daily=weathercode,precipitation_sum&temperature_unit=fahrenheit&windspeed_unit=mph&precipitation_unit=inch&timezone=America%2FDenver'

rain = ['51', '53', '55', '61', '63', '65', '80', '81', '82', '95', '96', '99']
snow = ['56', '57', '66', '67', '71', '73', '75', '77', '85', '86']

type_dict = {0: '-', 1: '-', 2: '-', 3: '-', 4: '-', 5: '-', 6: '-'}

count_dict = {1: [0, 0, 0, 0, 0, 0, 0]}

final_count_dict = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}

def collect_and_upload(db):    
    cities = db.collection(u'city_metadata')

    for city in cities.stream():
        Name = city.get('Name')
        Latitude = city.get('Latitude')
        Longitude = city.get('Longitude')
        County = city.get('County')

        api_link_complete = WEATHER_API_LINK1 + str(Latitude) + WEATHER_API_LINK2 + str(Longitude) + WEATHER_API_LINK3
        api_data = requests.get(api_link_complete).json()

        time = api_data.get('daily').get('time')
        weathercode = api_data.get('daily').get('weathercode')
        precipitation_sum = api_data.get('daily').get('precipitation_sum')
        
        day = 0
        for code in weathercode:
            if(str(code) in rain):
                day_weather = type_dict[day]

                if(day_weather == '-'):
                    type_dict[day] = 'Rain'
                if(day_weather == 'Snow'):
                    type_dict[day] == 'Rain & Snow'
                
                count_dict[County][day] += 1
            if(str(code) in snow):
                
                day_weather = type_dict[day]

                if(day_weather == '-'):
                    type_dict[day] = 'Snow'
                if(day_weather == 'Rain'):
                    type_dict[day] == 'Rain & Snow'

                count_dict[County][day] += 1
                
            day += 1

        doc_ref = db.collection(u'weather_forecasts').document(str(date.today()) + ' ' + Name)

        doc_ref.set({
            u'Name': Name,
            u'Latitude': Latitude,
            u'Longitude': Longitude,
            u'time': time,
            u'weathercode': weathercode,
            u'precipitation_sum': precipitation_sum
        })
        
        
    for c in count_dict:
        day = 0
        for count in count_dict[c]:
            if(count > 0):
                final_count_dict[day] += 1
            
            day += 1

    today = date.today()
    for x in range(0, 7):
        curr_date = today + datetime.timedelta(days=x)

        doc_ref = db.collection(u'combined_weather').document(str(x))

        doc_ref.set({
            u'DayNum': x,
            u'Day': calendar.day_name[curr_date.weekday()],
            u'Type': str(type_dict[x]),
            u'Regions': str(final_count_dict[x])
        })
        
        
def delete_collection(coll_ref, batch_size):
    docs = coll_ref.list_documents(page_size=batch_size)
    deleted = 0

    for doc in docs:
        print(f'Deleting doc {doc.id} => {doc.get().to_dict()}')
        doc.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)



if __name__ == '__main__':
    cred = credentials.Certificate(//FILL IN WITH FIREBASE CREDENTIALS)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    
    for x in range(2, 32):
        count_dict[x] = [0, 0, 0, 0, 0, 0, 0]

    collect_and_upload(db)


    print('Success!')