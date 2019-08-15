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
import re

#%%
def summarize_ghgrp_energy(data):
    
    file_dir = '../calculation_data/'

    fuelxwalk_file = 'MECS_FT_IPF.csv'
    
    fuelxwalk = pd.read_csv(
                os.path.join(file_dir, fuelxwalk_file)
                )[["EPA_FUEL_TYPE", "MECS_FT"]]
    
    data = pd.merge(data, fuelxwalk, left_on='FUEL_TYPE',
                    right_on='EPA_FUEL_TYPE', how='left')
    
    data = data[data.REPORTING_YEAR.between(2010, 2016)]

    def define_equipment(data):
        """

        """
        unitname_eq_dict = {'furnace': ['furnace', 'furn'],
                            'dryer': ['dryer', 'dehydrator', 'drying'],
                            'kiln': ['kiln'],
                            'process_heater': ['heater', 'htr', 'heating', 
                                               'reheat'],
                             'oven': ['oven', 'stove'], 'calciner': ['calciner'],
                             'cupola': ['cupola'], 'boiler': ['boiler'], 
                             'building_heating': ['building heat', 'space heater',
                                                  'comfort heater', 'hot water',
                                                  'water heater', 'bld heat',
                                                  'office', 'hvac', 'space heat'],
                             'turbine': ['turbine'],
                             'engine': ['engine', 'rice', 'ice', 'ic', 'mill',
                                        'screen', 'crush'], 'pump':['pump'],
                             'compressor': ['compressor'],
                             'generator': ['generator'], 'other': ['crane'],
                             'oxidizer': ['RTO','oxidizer', 'RCO', 'TODF',
                                          'thermox'],
                            'incinerator': ['incinerator']}

        unittype_eq_dict = {'furnace': ['F', 'Chemical Recovery Furnace', 'CF',
                                        'Chemical Recovery Combustion Unit',
                                        'Direct Reduction Furnace'],
                            'boiler': ['OB', 'S', 'PCWW', 'BFB', 'PCWD',
                                       'PCT', 'CFB', 'PCO', 'OFB', 'PFB'],
                            'kiln': ['Pulp Mill Lime Kiln', 'Lime Kiln', 'K'],
                            'calciner': ['C'], 
                            'process_heater':['PRH', 'CatH', 'NGLH', 'HMH', 
                                              'FeFL'],
                            'engine': ['RICE', 'Electricity Generator'],
                            'turbine': ['CCCT', 'SCCT'],
                            'sulfur_recovery': ['Sulfur Recovery Plant'],
                            'flare': ['Flare', 'FLR'],
                            'oven': ['O', 'COB', 'IFCE'], 'dryer':['PD'],
                            'hydrogen_production': ['HPPU'],
                            'building_heating': ['CH', 'HWH'], 
                            'oxidizer': ['TODF', 'RTO', 'RCO'],
                            'incinerator': ['ICI', 'MWC', 'II']}

        def equip_dict_to_df(eq_dict):
            """
            Convert unit type/unit name dictionaries to dataframes.
            """
            eq_df = pd.DataFrame.from_dict(
                    eq_dict, orient='index'
                    ).reset_index()

            eq_df = pd.melt(
                    eq_df, id_vars='index', value_name='unit'
                    ).rename(columns={'index': 'equipment'}).drop(
                            'variable', axis=1
                            )

            eq_df = eq_df.dropna().set_index('unit')

            return eq_df

        def equip_unit_type(unit_type, unittype_eq_df):
            """
            Match GHGRP unit type to end use specified in unittype_eu_dict.
            """

            equip = re.match('(\w+) \(', unit_type)

            if equip != None:

                equip = re.match('(\w+)', equip.group())[0]

                if equip in unittype_eq_df.index:

                    equip = unittype_eq_df.loc[equip, 'equipment']

                else:

                    equip = np.nan

            else:

                if unit_type in unittype_eq_df.index:

                    equip = unittype_eq_df.loc[unit_type, 'equipment']

            return equip

        def equip_unit_name(unit_name, unitname_eq_df):
            """
            Find keywords in GHGRP unit name descriptions and match them
            to appropriate end uses based on unitname_eu_dict.
            """

            for i in unitname_eq_df.index:

                equip = re.search(i, unit_name.lower())

                if equip == None:

                    continue

                else:

                    equip = unitname_eq_df.loc[i, 'equipment']

                    return equip

            equip = np.nan

            return equip

        unittype_eq_df = equip_dict_to_df(unittype_eq_dict)

        unitname_eq_df = equip_dict_to_df(unitname_eq_dict)
        
        unit_types = data.UNIT_TYPE.dropna().unique()

        type_match = list()

        for utype in unit_types:

            equipment = equip_unit_type(utype, unittype_eq_df)

            type_match.append([utype, equipment])

        type_match = pd.DataFrame(type_match,
                                  columns=['UNIT_TYPE', 'equipment'])
        
        # Fix None values
        type_match.iloc[
                type_match[type_match.equipment.isin([None])].index, 1
                ] = np.nan

        data = pd.merge(data, type_match, on='UNIT_TYPE', how='left')

        # Next, match end use by unit name for facilites that report OCS for
        # unit type.
        eq_ocs = data[
                (data.UNIT_TYPE == 'OCS (Other combustion source)') |
                (data.UNIT_TYPE.isnull())
                ][['UNIT_TYPE', 'UNIT_NAME']]

        eq_ocs['equipment'] = eq_ocs.UNIT_NAME.apply(
                lambda x: equip_unit_name(x, unitname_eq_df)
                )

        data.equipment.update(eq_ocs.equipment)

        data.drop(data.columns.difference(
                set(['COUNTY_FIPS','MECS_Region', 'MMBtu_TOTAL', 'MECS_FT',
                     'PRIMARY_NAICS_CODE', 'MECS_NAICS','equipment',
                     'FUEL_TYPE', 'STATE_NAME', 'REPORTING_YEAR',
                     'FACILITY_ID'])
                ), axis=1, inplace=True)

        # Set equipment == 'other' for remaining 
        data.equipment.fillna('not reported', inplace=True)

        return data

    data = define_equipment(data)
    
    #Export xls of energy data grouped by year for creating figures in excel
    with pd.ExcelWriter(
            'Y:/6A20/Public/IEDB/ghgrp_summary_IEDB.xlsx'
            ) as writer:
        
        data.groupby(
                ['REPORTING_YEAR', 'PRIMARY_NAICS_CODE']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_naics')
        
        data.groupby(
                ['REPORTING_YEAR', 'equipment']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_equipment')
        
        data.groupby(
                ['REPORTING_YEAR', 'equipment']
                ).MMBtu_TOTAL.sum().divide(
                        data.groupby(['REPORTING_YEAR']).MMBtu_TOTAL.sum()
                        ).to_excel(writer, sheet_name='by_equipment_fraction')
        
        data.groupby(
                ['REPORTING_YEAR', 'COUNTY_FIPS']
                ).MMBtu_TOTAL.sum().to_excel(writer, sheet_name='by_county')
        
        pd.concat(
                [data.groupby(['REPORTING_YEAR', 'STATE_NAME']).MMBtu_TOTAL.sum(),
                 data.groupby(
                         ['REPORTING_YEAR', 'STATE_NAME']
                         ).FACILITY_ID.unique().apply(lambda x: np.size(x))], 
                 axis=1).to_excel(writer, sheet_name='by_state')
        
        data.groupby(
                ['REPORTING_YEAR', 'MECS_FT']
                ).MMBtu_TOTAL.sum().reset_index().pivot(
                        'REPORTING_YEAR', 'MECS_FT'
                        ).to_excel(writer, sheet_name='by_fuel')

        data.groupby(
                ['REPORTING_YEAR', 'FACILITY_ID', 'PRIMARY_NAICS_CODE']
                ).MMBtu_TOTAL.sum().reset_index().pivot_table(
                        values=['MMBtu_TOTAL'],
                        index=['FACILITY_ID', 'PRIMARY_NAICS_CODE'],
                        columns=['REPORTING_YEAR']
                        ).to_excel(writer, sheet_name='by_facility')

    # Summary plots using Seaborn
    sns.set(context='talk', style='whitegrid', palette='Set2')

    # Box and whiskers by year and facility, showing distribution over
    #time
    year_plant = data.groupby(
            ['REPORTING_YEAR', 'FACILITY_ID'], as_index=False
            ).MMBtu_TOTAL.sum()

    # drop values == 0
    year_plant = pd.DataFrame(year_plant[year_plant.MMBtu_TOTAL >0])
    
    fig, ax = plt.subplots(figsize=(10,8))
    
    sns.stripplot(x='REPORTING_YEAR', y='MMBtu_TOTAL',
                  data=year_plant[year_plant.REPORTING_YEAR.isin([2010, 2016])],
                  dodge=True, jitter=True, palette=sns.color_palette("Paired"),
                  alpha=.30, zorder=1)

    sns.stripplot(
            x=[2010, 2016],
            y=year_plant[
                    year_plant.REPORTING_YEAR.isin([2010, 2016])
                    ].groupby('REPORTING_YEAR').MMBtu_TOTAL.median(),
            dodge=False, jitter=False, palette=sns.color_palette("Paired"), 
            marker="D", size=12, linewidth=2, alpha=1,
            edgecolor='black')
#    
    
#    [ax.text(p[0], p[1], p[1], color='black') for p in zip(
#            ax.get_xticks(), np.around(year_plant[year_plant.REPORTING_YEAR.isin(
#                    [2010, 2016]
#                    )].groupby('REPORTING_YEAR').MMBtu_TOTAL.median().values,0))]

    ax.set_yscale('log')
    
    ax.yaxis.grid(True)
    
    ax.set_ylabel('MMBtu (log)')
    
    ax.set_xlabel('Year')

    fig.savefig('Y:/6A20/Public/IEDB/large_fac_summary.pdf', dpi=100,
                bbox_inches='tight')
    
    print(
        np.around(
            year_plant[year_plant.REPORTING_YEAR.isin([2010, 2016])].groupby(
                    'REPORTING_YEAR'
                    ).MMBtu_TOTAL.median().values,0
            )
        )
    
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
    
    fig.savefig('Y:/6A20/Public/IEDB/large_fac_fuel.pdf', dpi=100,
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
    
    fig.savefig('Y:/6A20/Public/IEDB/large_fac_fuel_other.pdf', dpi=100,
                bbox_inches='tight')
#%%
#    # Table of largest emitting facilities
#    top_x = ghgrp_energy.groupby(
#            ['REPORTING_YEAR', 'FACILITY_ID', 'FACILITY_NAME',
#             'PRIMARY_NAICS_CODE', 'STATE'], as_index=False
#            ).MMBtu_TOTAL.sum().sort_values(ascending=False).xs(2016)[0:10]
#    
#    top_x.columns = ['GHGRP Facility ID', 'Facility Name', 'NAICS Code',
#                     'State' , 'TBtu']
#    
#    top_x['TBtu'] = top_x.TBtu.divide(10**6)
        