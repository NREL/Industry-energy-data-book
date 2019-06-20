# -*- coding: utf-8 -*-
"""
Created on Wed Mar 1 2017

@author: Colin

Based on mecs_ipf from Ookie.

IPF for producing state-level 3-digit NAICS code cost of materials,
parts, supplies, electricity, and fuels ($1,000) by employment size
"""

import pandas as pd
import numpy as np
import itertools as it

path = 'C:/Users/cmcmilla/desktop/Industrial Energy Model/'

ag_file = "Cons_State_2012Census_InputCalc.xlsx"

state_3DN_df = pd.read_excel(ag_file, sheetname = 'cons_3DNAICS_IPF')

state_3DN_df["State_3DNAICS"] = \
    state_3DN_df['Geographic area name']+"_" +\
        state_3DN_df['2012 NAICS code'].apply(lambda x: str(x))

state_2DNemp_df = pd.read_excel(ag_file, sheetname = 'cons_2DNAICS_emp_IPF')
state_2DNemp_df.fillna(0, inplace = True)

colDict = {}

colDict['states_3D'] = state_3DN_df.State_3DNAICS.values

colDict['state'] = state_3DN_df['Geographic area name'].drop_duplicates().value
s
colDict['empsize'] = \
    state_2DNemp_df[
        'Employment size of establishments'
        ].drop_duplicates()[1:].apply(lambda x: str(x)).values


#takes values in the dictionary above and creates a list of all combinations
def combine(columns):

    labels = [colDict[x] for x in columns]
    labels = list(it.product(*labels))
    output = []
    for i,label in enumerate(labels):
        output.append('_'.join(label))
    return output

#create column headings that have combinations of regions and energy carriers
#headings = combine(['regions','energy'])
#Define headings as employment size categories
headings = colDict['state']

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



first = True
state3D_emply = pd.DataFrame()
for heading in headings:
    #print(heading)
    col = state_3DN_df[state_3DN_df['Geographic area name'] == heading].iloc[:, 10].as_matrix(columns=None)
    row = state_2DNemp_df[state_2DNemp_df['Geographic area name'] == heading].iloc[1:,9].as_matrix(columns=None)

    col = np.array([col])
    row = np.array([row])
    col = np.transpose(col)

    #set seed to account for states that lack firms of a given size
    seed = np.vstack((row, row, row))

    col = col.astype(float)
    row = row.astype(float)

    state3D_emply = state3D_emply.append(pd.DataFrame(ipf2D_calc(seed, col, row)))

    # if first: state3D_emply = pd.DataFrame(ipf2D_calc(seed, col, row))
    # else: state3D_emply = state3D_emply.append(pd.DataFrame(ipf2D_calc(seed, col, row)))

    # if first: state3D_emply = ipf2D_calc(seed, col, row)
    # else: state3D_emply = np.hstack((state3D_emply, ipf2D_calc(seed, col, row)))
    first = False

#headings_final = np.array(combine(['states_3D','empsize']))

#state3D_emply = np.vstack((headings_final, state3D_emply))

state3D_emply.index = colDict['states_3D']

state3D_emply.columns = colDict['empsize']

state3D_emply.to_csv('cons_emp_IPF_results.csv')
