#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j



source $HOME/venv/c4/bin/activate

python3.11 upload.py
