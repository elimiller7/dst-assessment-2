import time
start2 = time.time() # SH8 - added timing capabilities for whole model

#SH8 - grabbed arguments from parse.py for --verbose
from parse import parse_args
args = parse_args()
print(f'Verbose = {args.verbose}')

import world
import utils
from world import cprint
import torch
import numpy as np
from tensorboardX import SummaryWriter
import Procedure
from os.path import join
# ==============================
utils.set_seed(world.seed)
print(">>SEED:", world.seed)
# ==============================
import register
from register import dataset
import dataloader

# SH8 - Forced reload of scripts since Kernel does not automatically detect import changes. This allows us to run the model multiple times without restarting Kernel.
import parse
import importlib
for module in [world,utils,Procedure,register,parse]:
    importlib.reload(module)

Recmodel = register.MODELS[world.model_name](world.config, dataset)
Recmodel = Recmodel.to(world.device)
bpr = utils.BPRLoss(Recmodel, world.config)

weight_file = utils.getFileName()
print(f"load and save to {weight_file}")
if world.LOAD:
    try:
        Recmodel.load_state_dict(torch.load(weight_file,map_location=torch.device('cpu')))
        world.cprint(f"loaded model weights from {weight_file}")
    except FileNotFoundError:
        print(f"{weight_file} not exists, start from beginning")
Neg_k = 1

# init tensorboard
if world.tensorboard:
    w : SummaryWriter = SummaryWriter(
                                    join(world.BOARD_PATH, time.strftime("%m-%d-%Hh%Mm%Ss-") + "-" + world.comment)
                                    )
else:
    w = None
    world.cprint("not enable tensorflowboard")

# SH8 - separate timer for training
start3 = time.time()

try:
    for epoch in range(world.TRAIN_epochs+1): # SH8 - minor alteration to make sure results are saved for final epoch (added +1)
        start = time.time()

        # SH8 - changed to print less often if not verbose
        if epoch %10 == 0 and epoch %100 != 0:
            if args.verbose == 'True':
                cprint("[TEST]")
            Procedure.Test(dataset, Recmodel, epoch, w, world.config['multicore'])
        elif epoch %100 == 0:
            cprint("[TEST]")
            Procedure.Test(dataset, Recmodel, epoch, w, world.config['multicore'])

        output_information = Procedure.BPR_train_original(dataset, Recmodel, bpr, epoch, neg_k=Neg_k,w=w)

        # SH8 - make sure final epoch isn't printed since otherwise we get e.g. epoch 11/10
        if epoch+1 != world.TRAIN_epochs+1:
            print(f'\rEPOCH[{epoch+1}/{world.TRAIN_epochs}] {output_information}', end = '') # SH8 - made it overwrite last print rather than print new line each time
        torch.save(Recmodel.state_dict(), weight_file)
finally:
    if world.tensorboard:
        w.close()

# SH8 - added timing capabilities
end2 = time.time()
print(f'Total time taken: {end2-start2}')
print(f'Training time: {end2-start3}')