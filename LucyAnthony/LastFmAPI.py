import requests

api_key = '6275d1d7adc70d522d2bdd63eb5c8f6e'
username = 'lucyanthony243'
url = f"http://ws.audioscrobbler.com/2.0/?method=user.getTopAlbums&user={username}&api_key={api_key}&format=json"

response = requests.get(url)
data = response.json()

if "topalbums" in data:
    albums = data["topalbums"]["album"]
    for album in albums:
        print(f"Album: {album['name']}, Artist: {album['artist']['name']}, Playcount: {album['playcount']}")