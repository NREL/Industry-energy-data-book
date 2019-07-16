# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 09:54:27 2019

@author: cmcmilla
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import numpy as np



def summarize_ghgrp_energy(data):
    
    file_dir = '../calculation_data/'

    fuelxwalk_file = 'MECS_FT_IPF.csv'
    
    fuelxwalk = pd.read_csv(
                os.path.join(file_dir, fuelxwalk_file)
                )[["EPA_FUEL_TYPE", "MECS_FT"]]
    
    data['MECS_FT'] = pd.merge(data, fuelxwalk, left_on='FUEL_TYPE',
                               right_on='EPA_FUEL_TYPE', how='left')
    
    #Export xls of energy data grouped by year for creating figures in excel
    with pd.ExcelWriter(
            'Y:/6A20/Public/IEDB/ghgrp_summary_IEDB.xlsx'
            ) as writer:
        
        data.groupby(
                ['REPORTING_YEAR', 'PRIMARY_NAICS_CODE']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_naics')
        
        data.groupby(
                ['REPORTING_YEAR', 'UNIT_TYPE']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_unit')
        
        data.groupby(
                ['REPORTING_YEAR', 'COUNTY_FIPS']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_county')
        
        data.groupby(
                ['REPORTING_YEAR', 'STATE_NAME']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_state')
        
        data.groupby(
                ['REPORTING_YEAR', 'MECS_FT']
                ).MMBtu_TOTAL.sum().reset_index().pivot(
                        'REPORTING_YEAR', 'MECS_FT'
                        ).to_excel(writer, sheet_name='by_fuel')
    
    
    # Summary plots using Seaborn
    sns.set(context='talk', style='whitegrid', palette='Set2')
        
    # Box and whiskers by year and facility, showing distribution over
    #time
    year_plant = data.groupby(
            ['REPORTING_YEAR', 'FACILITY_ID'], as_index=False
            ).MMBtu_TOTAL.sum()
    
    
    
    # drop values == 0
    year_plant = pd.DataFrame(year_plant[year_plant.MMBtu_TOTAL >0])
    
    fac_counts = year_plant.groupby('REPORTING_YEAR',
                                    as_index=False).FACILITY_ID.count()
    
    
    fig, ax = plt.subplots(figsize=(10,8))
    
    sns.stripplot(x='REPORTING_YEAR', y='MMBtu_TOTAL',
                  data=year_plant[year_plant.REPORTING_YEAR.isin([2010, 2017])],
                  dodge=True, jitter=True,
                  alpha=.25, zorder=1)


    sns.pointplot(x='REPORTING_YEAR', y='MMBtu_TOTAL',
                  data=year_plant[year_plant.REPORTING_YEAR.isin([2010, 2017])],
                  dodge=False, join=False, palette="dark", estimator=np.median,
                  markers="s", scale=1, ci=None)
    
#    [ax.text(p[0], p[1], p[1], color='black') for p in zip(
#            ax.get_xticks(), np.around(year_plant[year_plant.REPORTING_YEAR.isin(
#                    [2010, 2017]
#                    )].groupby('REPORTING_YEAR').MMBtu_TOTAL.median().values,0))]

    ax.set_yscale('log')
    
    ax.yaxis.grid(True)
    
    ax.set_ylabel('MMBtu (log)')
    
    ax.set_xlabel('Year')

    fig.savefig('Y:/6A20/Public/IEDB/large_fac_summary.png', dpi=100,
                bbox_inches='tight')
    
    #Fuel mix plot over time
    year_fuel = data.groupby(['REPORTING_YEAR','FUEL_TYPE', 'MECS_FT'],
                             as_index=False).MMBtu_TOTAL.sum()
    
    year_fuel.fillna('Other', inplace=True)
    
    
    mecs_fraction = year_fuel.groupby(
            ['REPORTING_YEAR', 'MECS_FT']
            ).MMBtu_TOTAL.sum().divide(
                    year_fuel.groupby('REPORTING_YEAR').MMBtu_TOTAL.sum()
                    )
    
    mecs_fraction.name = 'Fraction'
    
    year_fuel = year_fuel.set_index(['REPORTING_YEAR', 'MECS_FT']).join(
            mecs_fraction
            ).reset_index()
    
    # Fuel type plot
    fig, ax = plt.subplots(figsize=(10,8))
    
    sns.lineplot(x='REPORTING_YEAR', y='Fraction', hue='MECS_FT',
               data=year_fuel.drop_duplicates())
    
    ax.set_xlabel('Year')
    
    yvals = ax.get_yticks()
    
    ax.set_yticklabels(['{:,.0%}'.format(x) for x in yvals])
    
    ax.set_ylabel('Annual Fuel Mix')
    
    ax.legend(title='Fuel Type', bbox_to_anchor=(1.04,1),
              labels=['Coal', 'Coke and Breeze', 'Diesel', 'LPG-NGL', 
                      'Natural Gas', 'Other', 'Residual Fuel Oil'], ncol=1,
                      frameon=False)
    
    fig.savefig('Y:/6A20/Public/IEDB/large_fac_fuel.png', dpi=100,
                bbox_inches='tight')
    
    fig.clear()
    
    
    # Take top 5 "other fuels" in each year; aggregate remaining as
    # "everything else"
    other_grpd = year_fuel[year_fuel.MECS_FT == 'Other'].groupby(
            'REPORTING_YEAR'
            )

    year_fuel_other = pd.DataFrame()
    
    for g in other_grpd.groups:
        
        top_fuels = other_grpd.get_group(g)[
                        ['FUEL_TYPE', 'MMBtu_TOTAL']
                        ].sort_values(
                    by='MMBtu_TOTAL', ascending=False
                    )[0:5]
        
        top_fuels['REPORTING_YEAR'] = g
        
        all_other = pd.DataFrame([[g,
                        other_grpd.get_group(g)[
                                ['FUEL_TYPE', 'MMBtu_TOTAL']
                                ].sort_values(
                            by='MMBtu_TOTAL', ascending=False
                            )[5:].MMBtu_TOTAL.sum()]],
                        columns=['REPORTING_YEAR', 'MMBtu_TOTAL'])
        
        all_other['FUEL_TYPE'] = 'Everything Else'
        
        year_fuel_other = pd.concat([year_fuel_other, top_fuels, all_other],
                                    axis=0, ignore_index=True, sort=False)

    year_fuel_other.replace('Wood and Wood Residuals (dry basis)',
                            'Wood and Wood Residuals', inplace=True)
    
    year_fuel_other = year_fuel_other.groupby(['REPORTING_YEAR', 'FUEL_TYPE'],
                                              as_index=False).MMBtu_TOTAL.sum()

    year_fuel_other['Fraction']= year_fuel_other.set_index(
            'REPORTING_YEAR'
            ).MMBtu_TOTAL.divide(
                year_fuel.set_index('REPORTING_YEAR').MMBtu_TOTAL.sum(level=0)
                ).reset_index()['MMBtu_TOTAL']
    
    
    # "Other fuels" plot
    fig, ax = plt.subplots(figsize=(10,8))
    
    sns.lineplot(x='REPORTING_YEAR', y='Fraction', hue='FUEL_TYPE',
                 data=year_fuel_other, markers=False)
    
    ax.set_xlabel('Year')
    
    yvals = ax.get_yticks()
    
    ax.set_yticklabels(['{:,.0%}'.format(x) for x in yvals])
    
    ax.set_ylabel('Annual Fuel Mix')
    
    ax.legend(title='Fuel Type', bbox_to_anchor=(1.04,1), ncol=1,
              frameon=False)
    
    fig.savefig('Y:/6A20/Public/IEDB/large_fac_fuel_other.png', dpi=100,
                bbox_inches='tight')
    
    # Table of largest emitting facilities
    top_x = ghgrp_energy.groupby(
            ['REPORTING_YEAR', 'FACILITY_ID', 'FACILITY_NAME',
             'PRIMARY_NAICS_CODE', 'STATE'], as_index=False
            ).MMBtu_TOTAL.sum().sort_values(ascending=False).xs(2017)[0:10]
    
    top_x.columns = ['GHGRP Facility ID', 'Facility Name', 'NAICS Code',
                     'State' , 'TBtu']
    
    top_x['TBtu'] = top_x.TBtu.divide(10**6)
        