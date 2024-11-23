import world
import dataloader
import model
import utils
from pprint import pprint
from parse import parse_args
args = parse_args()

if world.dataset in ['gowalla', 'yelp2018', 'amazon-book']:
    dataset = dataloader.Loader(path="../data/"+world.dataset)
elif world.dataset == 'lastfm':
    dataset = dataloader.LastFM()
# SH8 - registered LastFM2 dataset
elif world.dataset == 'lastfm2':
    dataset = dataloader.LastFM2()

if args.verbose == 'True': # SH8 - Changed to not print vonfig if not verbose
    print('===========config================')
    pprint(world.config)
    print("cores for test:", world.CORES)
    print("comment:", world.comment)
    print("tensorboard:", world.tensorboard)
    print("LOAD:", world.LOAD)
    print("Weight path:", world.PATH)
    print("Test Topks:", world.topks)
    print("using bpr loss")
    print('===========end===================')

MODELS = {
    'mf': model.PureMF,
    'lgn': model.LightGCN
}