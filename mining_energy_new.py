# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 23:11:05 2019

@author: cmcmilla
"""

import pandas as pd
import numpy as np
import zipfile
import requests


# Direct download of census zip file for fuels (energy and purchases)
# Establishment counts, too
# BEA API for quantity index by state and general mining industry
# Change in energy from quantity index (2012 == 100)

with zipfile.ZipFile(census_url) as miningzip:
    with miningzip.open() as census_data:
        
'https://www2.census.gov/econ2012/EC/sector21/EC1221SM1.zip
