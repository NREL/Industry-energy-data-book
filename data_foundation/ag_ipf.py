# -*- coding: utf-8 -*-
"""
Created on Wed Mar 1 2017

@author: Colin

Based on mecs_ipf from Ookie.
"""

import pandas as pd
import numpy as np
import itertools as it

colDict = {'regions': ['Atlantic', 'Midwest', 'Plains', 'South', 'West'],
           'energy': ['Diesel', 'Gasoline', 'LPG', 'Other', 'Lubricants']
            }

#takes values in the dictionary above and creates a list of all combinations
def combine(columns):

    labels = [colDict[x] for x in columns]
    labels = list(it.product(*labels))
    output = []
    for i,label in enumerate(labels):
        output.append('_'.join(label))
    return output

#create column headings that have combinations of regions and energy carriers
headings = combine(['regions','energy'])

path = 'C:/Users/cmcmilla/desktop/Industrial Energy Model/'

ag_file = "Ag_Model_Inpupts.xlsx"

county_df = pd.read_excel(ag_file, sheetname = 'Census_County')

energy_df = pd.read_excel(ag_file, sheetname = 'Surveys_Fuel_adj')

# filename = 'NAICS-energy_carrier_MECS3.2_dummies_noTotals.csv'
# naics_df = pd.read_csv(path+filename)

# filename = 'Employment_Size-energy_carrier_MECS3.3_noTotals.csv'
# emply_df = pd.read_csv(path+filename)

# filename = 'Value_of_Shipments-energy_carrier_MECS3.3.csv'
# value_df = pd.read_csv(path+filename)

#two-dimensional iterative proportional fitting algorithm
def ipf2D_calc(seed, col, row):

    #col matrix should have dimensions of (m,1)
    #row matrix should have dimensions of (1,n)
    #seed matrix should have dimensions of (m,n)
    col_dim = col.shape[0]
    row_dim = row.shape[1]

    for n in range(3000): #set maximumn number of iterations
        error = 0.0
        #faster 'pythonic(?)' version
        sub = seed.sum(axis=1,keepdims=True)
        sub = col / sub
        sub[np.isnan(sub)] = 0.0
        sub = sub.flatten()
        sub = np.repeat(sub[:,np.newaxis],row_dim,axis=1)
        seed = seed*sub
        diff = (seed.sum(axis=1,keepdims=True)-col)
        diff = diff*diff
        error += diff.sum()

        sub = seed.sum(axis=0,keepdims=True)
        sub = row / sub
        sub[np.isnan(sub)] = 0.0
        sub = sub.flatten()
        sub = np.repeat(sub[:,np.newaxis],col_dim,axis=1)
        sub = sub.transpose()
        seed = seed*sub
        diff = (seed.sum(axis=0,keepdims=True)-row)
        diff = diff*diff
        diff = diff.sum()
        error = np.sqrt(error)
        if error < 1e-15: break
    if error > 1e-13: print("Max Iterations ", error) #report error if max iterations reached
    return seed

#Define seeds by initial fuel estimates.
seed_shop = pd.read_excel(ag_file, sheetname = 'IPF_seed')
seed_shop_dict = {}
for r in ColDict['regions']:
    seed_shop_dict[r] =\
        seed_shop[seed_shop.region == r][('county_fips'):('Lubricants')]


first = True
for heading in headings:
    print(heading)
    col = naics_df[heading].as_matrix(columns=None)
    row = emply_df[heading].as_matrix(columns=None)

    col = np.array([col])
    row = np.array([row])
    col = np.transpose(col)

    #lacking microdata sample, set the inital guess as all ones, except
    #instances where CBP fac count = 0 & MECS energy != 0
    seed = np.array(seed_shop_dict[heading.split("_")[0]])
    #seed = np.ones((col.shape[0],row.shape[1]))

    col = col.astype(float)
    row = row.astype(float)

    if first: naics_emply = ipf2D_calc(seed, col, row)
    else: naics_emply = np.hstack((naics_emply, ipf2D_calc(seed, col, row)))
    first = False

headings = np.array(combine(['regions','energy','employment']))

naics_emply = np.vstack((headings, naics_emply))

#Need to add column of MECS_NAICS_dummies
filename = 'naics_employment.csv'
np.savetxt(path + filename, naics_emply, fmt='%s', delimiter=",")
