import pandas as pd
import torch
from torch.nn import Linear, Parameter
from torch_geometric.nn import MessagePassing
from torch_geometric.utils import add_self_loops, degree

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

# From https://pytorch-geometric.readthedocs.io/en/latest/tutorial/create_gnn.html
class GCNConv(MessagePassing):
    def __init__(self, in_channels, out_channels):
        super().__init__(aggr='add')  # "Add" aggregation (Step 5).
        self.lin = Linear(in_channels, out_channels, bias=False)
        self.bias = Parameter(torch.empty(out_channels))

        self.reset_parameters()

    def reset_parameters(self):
        self.lin.reset_parameters()
        self.bias.data.zero_()

    def forward(self, x, edge_index):
        # x has shape [N, in_channels]
        # edge_index has shape [2, E]

        # Step 1: Add self-loops to the adjacency matrix.
        edge_index, _ = add_self_loops(edge_index, num_nodes=x.size(0))

        # Step 2: Linearly transform node feature matrix.
        x = self.lin(x)

        # Step 3: Compute normalization.
        row, col = edge_index
        deg = degree(col, x.size(0), dtype=x.dtype)
        deg_inv_sqrt = deg.pow(-0.5)
        deg_inv_sqrt[deg_inv_sqrt == float('inf')] = 0
        norm = deg_inv_sqrt[row] * deg_inv_sqrt[col]

        # Step 4-5: Start propagating messages.
        out = self.propagate(edge_index, x=x, norm=norm)

        # Step 6: Apply a final bias vector.
        out = out + self.bias

        return out

    def message(self, x_j, norm):
        # x_j has shape [E, out_channels]

        # Step 4: Normalize node features.
        return norm.view(-1, 1) * x_j