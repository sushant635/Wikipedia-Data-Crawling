import json 
import pandas as pd 
import requests
from bs4 import BeautifulSoup
from geopy import Nominatim
from datetime import datetime

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    try:
        response = requests.get(url,timeout=10)
        response.raise_for_status()
        # print(response.text)
        return response.text
    except requests.RequestException as e:
        print(e)

def get_wikipedia_data(html):
    soup = BeautifulSoup(html,'html.parser')
    table = soup.find_all("table",{'class':"wikitable"})[1]
    # print(table)
    table_rows = table.find_all('tr')
    return table_rows


url = 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'



def clean_text(text):
    text = str(text).strip()
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    print(text)
    return text

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    row = get_wikipedia_data(html)

    data = []
    for i in range(1,len(row)):
        # print(row[i])
        tds = row[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)
    # data_df = pd.DataFrame(data)
    # data_df.to_csv('data/testing.csv',index=False)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "Ok"

def get_lat_long(country,city):
    geolocator = Nominatim(user_agent='dataLearning')
    location = geolocator.geocode(f'{city}, {country}')


    if location:
        print(location.latitude,location.longitude)
        return location.latitude,location.longitude

    return None




def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows',task_ids='extract_data_from_wikipedia')

    data = json.loads(data)
    print(f"Data pulled from XCom: {data}")
    stadium_df = pd.DataFrame(data)
    print(stadium_df)
    stadium_df['location'] = stadium_df.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadium_df['images'] = stadium_df['images'].apply(lambda x: x if x not in ['NO_IMAGE','',None] else NO_IMAGE)
    stadium_df['capacity'] = stadium_df['capacity'].astype(int)

    duplicates = stadium_df[stadium_df.duplicated(['location'])]

    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'],x['city']),axis=1)
    stadium_df.update(duplicates)

    kwargs['ti'].xcom_push(key='rows', value=stadium_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows',task_ids='transform_wikipedia_data')
    
    data = json.loads(data)
    data = pd.DataFrame(data)
    print(data)
    file_name = ('stadium_cleaned_football_'+ str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    data.to_csv('abfs://coniter@datlake.dfs.core.windows.net/data/'+file_name,
                storage_options={
                    'account_key':''
                },index=False)










