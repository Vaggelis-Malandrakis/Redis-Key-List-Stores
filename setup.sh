#!/usr/bin/env bash

sudo apt-get install python3.6
sudo apt install python3-pip
python3 -m venv venv
source venv/bin/activate
pip3 install redis
pip3 install xlrd
pip3 install mysql-connector-python
