# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 15:14:53 2019

@author: cmcmilla
"""
import json
import requests
import numpy as np

def fipfind(data_directory, f, missingfips):
    """
    Match missing FIPS with facility coordinates using FCC API.
    """

    z2f = json.load(open(data_directory + '/zip2fips.json'))

    if np.isnan(missingfips.loc[f, 'LATITUDE']) == False:

        lat = missingfips.loc[f, 'LATITUDE']

        lon = missingfips.loc[f, 'LONGITUDE']

        payload = {
            'format': 'json', 'latitude': lat, 'longitude': lon,
            'showall': 'True', 'censusYear': 2010
            }

        r = requests.get('http://geo.fcc.gov/api/census/block/find?', 
            params=payload
            )

        if r.json()['County']['FIPS'] == None:
            
            fipfound = 0
        
        else:
            
            fipfound = r.json()['County']['FIPS']

        return fipfound

    if ((missingfips.loc[f, 'ZIP'] > 1000) 

        & (np.isnan(missingfips.loc[ f, 'COUNTY_FIPS'])==True) 

        & (str(missingfips.loc[f, 'ZIP']) in z2f)):

        fipfound = int(z2f[str(missingfips.loc[f,'ZIP'])])

        return fipfound

    else:

        fipfound = 0

    return fipfound