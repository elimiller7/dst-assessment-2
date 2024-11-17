import requests
import random
import pandas as pd
import csv

file_name = '~/University/bristol/year_4/DataScienceToolbox/Assessment2/unpushed_work/common-forenames-by-country.csv'
forename_data = pd.read_csv(file_name)
forename_data = forename_data['Localized Name']


def album_pref(username):

    api_key = ''
    url = f"http://ws.audioscrobbler.com/2.0/?method=user.getTopAlbums&user={username}&api_key={api_key}&format=json"

    response = requests.get(url)
    data = response.json()

    output = []

    if "topalbums" in data:
        albums = data["topalbums"]["album"]
        for album in albums:
            output.append([album['name'], album['artist']['name'], album['playcount']])
    
    return output

def username_data_gen(n):
    data = []
    for i in range(n):
        username = forename_data[i]
        if album_pref(username) != []:
            data.append(album_pref(username))
    
    return data

data = [['album_name', 'artist', 'playcount']]

data.append(username_data_gen(1000))

with open('output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)



