import requests
import random

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


def username_generator():

    letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    n = random.randint(1, 10)
    username = []
    for j in range(n):
        m = random.randint(1, 25)
        username.append(letters[m])
    username = ''.join(username)
    return username


def username_data_gen(n):
    data = []
    for i in range(n):
        username = username_generator()
        if album_pref(username) != []:
            data.append(album_pref(username))
    
    return data

d = username_data_gen(100)
print(len(d)) 