import pandas as pd
import numpy as np

data = pd.read_csv('last_fm_data/user_artists.dat', sep='\t')
data = data[:1000]

users = data['userID'].drop_duplicates().tolist()
artists = data['artistID'].drop_duplicates().tolist()
users = users[:1000]
artists = artists[:1000]

def get_weight_from_ids(user, artist):
    for i in range(len(data)):
        if data['userID'][i] == user and data['artistID'][i] == artist:
            return data['weight'][i]


n = len(users)
m = len(artists)

matrix = np.zeros((n, m))

for i in range(n):
    user_id = users[i]
    for j in range(m):
        artist_id = artists[j]
        matrix[i, j] = get_weight_from_ids(user_id, artist_id)

matrix = np.nan_to_num(matrix, nan=0)

print(matrix)