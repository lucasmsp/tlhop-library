#!/bin/bash

date
timestamp=$(date +%s)
python3 setup.py sdist
VERSION=$(python3 -c 'exec(open("tlhop/__init__.py").read()); print(__version__)')
pip3 install --upgrade --no-deps --force-reinstall dist/tlhop-library-"$VERSION".tar.gz

cp dist/tlhop-library-"$VERSION".tar.gz dist/tlhop-library-"$VERSION"-"$timestamp".tar.gz
date
echo "New version of tlhop-library-"$VERSION"-"$timestamp".tar.gz"
