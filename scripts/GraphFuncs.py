import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

def get_friends(userID : int, df : pd.DataFrame):
    """Returns a list containing the friends of userID in df. df needs columns 'userID', 'friendID' (usually user_friends)."""
    friends = df.loc[df['userID'] == userID]
    return [x for x in friends['friendID']]

def get_tags(userID : int, df: pd.DataFrame):
    """Returns a list containing the tags that userID has assigned to artists in df. df needs columns 'userID', 'tagID' (usually user_taggedartists)."""
    tags = df.loc[df['userID'] == userID]
    return [x for x in tags['tagID']]

def get_artists(userID : int, df : pd.DataFrame):
    """Returns a list containing the artists that userID has listened to in df. df needs columns 'userID', 'artistID' (usually user_artists)."""
    artists = df.loc[df['userID'] == userID]
    return [x for x in artists['artistID']]

def get_df_name(df : pd.DataFrame, globals : dict): # From https://stackoverflow.com/questions/31727333/get-the-name-of-a-pandas-dataframe
    """Returns the name of the input dataframe as a string. Solely used for nice formatting. Requires input of globals() dict."""
    name =[x for x in globals if globals[x] is df][0]
    return name

def plot_spring_graph(vertices_pd : pd.DataFrame, edges_pd : pd.DataFrame, fig_size = (12,8)):
    """Plots a graph using the spring_layout using two input DataFrames, one with the vertices, and one with the edges.
    vertices_pd, needs columns 'id' and 'partition', edges_pd needs columns 'src', 'dst', and 'relationship'."""
    G = nx.Graph()

    # Add nodes with the partition attribute
    for _, row in vertices_pd.iterrows():
        G.add_node(row['id'], partition=row['partition'])

    # Add edges
    for _, row in edges_pd.iterrows():
        G.add_edge(row['src'], row['dst'], relationship=row['relationship'])
    # Create the spring layout
    pos = nx.spring_layout(G,k=0.5,seed=1533)

    # Define the color map based on partitions
    color_map = ['lightblue' if G.nodes[node]['partition'] == 0 else '#FF6961' for node in G.nodes()]
    # Draw the graph
    plt.figure(figsize=fig_size)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color=color_map, font_weight='bold', font_size=10)
    plt.title('Graphical Representation of a Spring Layout Graph')
    plt.show()

def plot_multipartite_graph(vertices_pd : pd.DataFrame, edges_pd : pd.DataFrame, fig_size = (12,8)):
    """Plots a graph using the multipartite_layout two input DataFrames, one with the vertices, and one with the edges.
    vertices_pd, needs columns 'id' and 'partition', edges_pd needs columns 'src', 'dst', and 'relationship'."""
    G = nx.Graph()

    # Add nodes with the partition attribute
    for _, row in vertices_pd.iterrows():
        G.add_node(row['id'], partition=row['partition'])

    # Add edges
    for _, row in edges_pd.iterrows():
        G.add_edge(row['src'], row['dst'], relationship=row['relationship'])
    # Create the spring layout
    pos = nx.multipartite_layout(G, subset_key='partition')

    # Define the color map based on partitions
    color_map = ['lightblue' if G.nodes[node]['partition'] == 0 else '#FF6961' for node in G.nodes()]
    # Draw the graph
    plt.figure(figsize=fig_size)
    nx.draw(G, pos, with_labels=True, node_size=1500, node_color=color_map, font_weight='bold', font_size=10)
    plt.title('Graphical Representation of a Multipartite Graph')
    plt.show()
