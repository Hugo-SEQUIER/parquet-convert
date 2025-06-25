#!/bin/bash

# Installation avec flags de contournement pour Python 3.13
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
export CFLAGS="-w"

# Essayer d'installer pandas avec des wheels pré-compilées uniquement
pip install --only-binary=all pandas

# Si ça échoue, essayer avec une version plus récente de Cython
if [ $? -ne 0 ]; then
    pip install --upgrade cython
    pip install pandas --no-cache-dir
fi

# Installer le reste des dépendances
pip install -r requirements.txt 