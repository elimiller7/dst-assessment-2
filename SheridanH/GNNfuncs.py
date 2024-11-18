import pandas as pd

def get_friends(userID : str, df : pd.DataFrame):
    """Returns a list containing the friends of userID in df. df needs columns 'userID', 'friendID' (usually user_friends)."""
    friends = df.loc[df['userID'] == userID]
    return [x for x in friends['friendID']]

def get_tags(userID : str, df: pd.DataFrame):
    """Returns a list containing the tags that userID has assigned to artists in df. df needs columns 'userID', 'tagID' (usually user_taggedartists)."""
    tags = df.loc[df['userID'] == userID]
    return [x for x in tags['tagID']]

def get_artists(userID : str, df : pd.DataFrame):
    """Returns a list containing the artists that userID has listened to in df. df needs columns 'userID', 'artistID' (usually user_artists)."""
    artists = df.loc[df['userID'] == userID]
    return [x for x in artists['artistID']]

def get_df_name(df : pd.DataFrame, globals : dict): # From https://stackoverflow.com/questions/31727333/get-the-name-of-a-pandas-dataframe
    """Returns the name of the input dataframe as a string. Solely used for nice formatting. Requires input of globals() dict."""
    name =[x for x in globals if globals[x] is df][0]
    return name