# -*- coding: utf-8 -*-
"""
Created on Mon Feb 25 15:19:42 2019

@author: cmcmilla
"""
import pandas as pd
import heat_rate_uncertainty_IEDB as hr_uncert
import os
# %%
class tier_energy:
    """
    Class for methods that estimate combustion energy use from emissions data
    reported to the EPA's GHGRP.
    """

    def __init__(self, years=None, std_efs=None, calc_uncert=False):

        # EPA standard emission factors by fuel type
        if std_efs is None:

            self.std_efs = pd.DataFrame(
                    hr_uncert.FuelUncertainty().fuel_efs)

            #make sure no duplicate fuel types
            self.std_efs = self.std_efs.drop_duplicates(subset='fuel_type')

            self.std_efs.set_index('fuel_type', inplace=True)

            self.std_efs.index.names = ['FUEL_TYPE']

        else:

            self.std_efs = std_efs


        self.std_efs.rename(
                columns={'co2_kgco2_per_mmbtu': 'CO2_kgCO2_per_mmBtu'},
                inplace=True)

        self.data_columns = ['FACILITY_ID', 'REPORTING_YEAR', 'FACILITY_NAME',
                             'UNIT_NAME', 'UNIT_TYPE', 'FUEL_TYPE',
                             'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND',
                             'CO2e_TOTAL']

        self.years = years

        self.calc_uncert = calc_uncert

        def tier_table_wa(tier_table):
            """
            Format and calculate weighted average for data reported in
            tier 2 and tier 3 data tables.
            Tables are 't2_hhv' and 't3'
            """

            filedir = os.path.join('../', 'calculation_data/', 'ghgrp_data/')

            #Check first if data have been downloaded already
            dl_tables = {'t2_hhv': ['t2_hhv'],
                         't2_boiler': ['t2_boiler'],
                         't3': ['t3_solid', 't3_gas', 't3_liquid']}

            tier_data = pd.DataFrame()

            for file in dl_tables[tier_table]:

                for y in self.years:

                    # Reporting began in 2014, therefore no data for 2010-2013
                    if y < 2014:

                        continue

                    file_y = file + '_' + str(y) + '.csv'

                    if file_y in os.listdir(filedir):

                        df = pd.read_csv(filedir+file_y)

                        df.columns = \
                            [x.lower() for x in df.columns]

                        tier_data = tier_data.append(df, ignore_index=True,
                                                     sort=True)

                    else:

                        print('No data file for', file_y, '\n',
                              'Downloading from EPA API')

                        tier_data = \
                            hr_uncert.FuelUncertainty(
                                years=y
                                ).dl_tier(tier_table)

                        tier_data.to_csv(filedir+file_y)

                        if '.' in tier_data.columns[0]:

                            tier_data.columns = \
                                [x.split('.')[1].lower() for x in tier_data.columns]

                        else:

                            tier_data.columns = \
                                [x.lower() for x in tier_data.columns]

            for c in ['reporting_year', 'facility_id']:

                tier_data.dropna(subset=[c], axis=0, inplace=True)

                tier_data.loc[:, c] = tier_data[c].astype(int)

            if 'unnamed: 0' in tier_data.columns:

                tier_data.drop('unnamed: 0', axis=1, inplace=True)

            def calc_wa(data, weighting_column, weighted_column):
                """
                Method for calculating annual weighted average of monthly
                reported values for high heat, molecular weight,
                and carbon content.
                """

#                for c in [weighting_column, weighted_column]:
#
#                    data.dropna(subset=[c], axis=0, inplace=True)
#
#                    data[c] = data[c].astype(float)

                if 'molecular_weight' in data.columns:

                    data.molecular_weight = data.molecular_weight.astype(float)

                # Correct HHV values that appear to be off by an order of
                # magnitude for natural gas
                if weighted_column == 'high_heat_value':

                    hhv_correct = data[
                        (data.fuel_type=='Natural Gas (Weighted U.S. Average)') &
                        (data[weighted_column].between(0, 0.00019,
                                                       inclusive=False))
                        ][weighted_column]*10

                    if hhv_correct.empty != True:

                        tier_data[weighted_column].update(hhv_correct)

                data['energy_mmbtu'] = \
                    data[weighting_column].dropna().multiply(
                            data[weighted_column].dropna()
                            )

                if weighting_column == 'mass_of_steam':

                    data_annual = pd.DataFrame(data.groupby(
                        ['facility_id', 'reporting_year', 'fuel_type',
                         'unit_name']
                        )['energy_mmbtu'].sum())

                else:

                    data_annual = pd.DataFrame(data.groupby(
                            ['facility_id', 'reporting_year', 'fuel_type',
                             'unit_name']
                            )['energy_mmbtu', 'fuel_combusted'].sum())

                # Some months have more than one entry. Take the mean of
                # these values
                tier_wa = data[data[weighting_column]>0].groupby(
                        ['facility_id', 'reporting_year','fuel_type',
                         'unit_name', 'month']
                        )[weighting_column, weighted_column].mean()

                tier_wa = tier_wa[weighted_column].multiply(
                        tier_wa[weighting_column]
                        ).sum(level=(0,1,2,3)).divide(
                            tier_wa[weighting_column].sum(level=(0,1,2,3))
                            )

                data_annual[weighted_column+'_wa'] = tier_wa

                data_annual.index.names = \
                    [x.upper() for x in data_annual.index.names]

                return data_annual

            if tier_table == 't2_hhv':

                tier_data_annual = calc_wa(tier_data, 'fuel_combusted',
                                           'high_heat_value')

                if self.calc_uncert:

                    hr_uncert.FuelUncertainty.tier_bootstrap(
                            tier_data, 'high_heat_value', 'hhv'
                            )

            if tier_table == 't2_boiler':

                tier_data_annual = calc_wa(tier_data, 'mass_of_steam',
                                           'boiler_ratio_b')

                if self.calc_uncert:

                    hr_uncert.FuelUncertainty.tier_bootstrap(
                            tier_data, 'boiler_ratio_b', 'br'
                            )

            if tier_table == 't3':

                tier_data_annual = pd.concat(
                        [calc_wa(
                                tier_data, 'fuel_combusted', 'carbon_content'
                                 ),
                         calc_wa(
                                 tier_data,'fuel_combusted','molecular_weight'
                                )['molecular_weight_wa']], axis=1
                        )

                if self.calc_uncert:

                    hr_uncert.FuelUncertainty.tier_bootstrap(
                            tier_data, 'carbon_content', 'cc'
                            )

                    hr_uncert.FuelUncertainty.tier_bootstrap(
                            tier_data, 'molecular_weight', 'mw'
                            )

            return tier_data_annual

        self.t2hhv_data_annual = tier_table_wa('t2_hhv')

        self.t2boiler_data_annual = tier_table_wa('t2_boiler')

        self.t3_data_annual = tier_table_wa('t3')

#%%
    def filter_data(self, subpart_c_df, tier_column):
        """
        Filter relevant emissions data from subpart C dataframe based
        on specified tier column.
        """

        ghg_data = pd.DataFrame(
                subpart_c_df.dropna(subset=[tier_column], axis=0)
                )

        data_columns = [x for x in self.data_columns]

        data_columns.append(tier_column)

        if tier_column == 'T4CH4COMBUSTIONEMISSIONS':

            data_columns.append('ANNUAL_HEAT_INPUT')

        ghg_data = pd.DataFrame(ghg_data[data_columns])

        ghg_data = pd.DataFrame(
                ghg_data[ghg_data.REPORTING_YEAR.isin(self.years)]
                )

        return ghg_data

#    def tier2_hhv_check(self, tier2_ghg_data):
#        """
#        Compare emissions calculated from reported fuel use and hhv data with
#        reported emissions.
#        """
#
#        # Check indices
#        tier2_index_names = ['FACILITY_ID','REPORTING_YEAR', 'FUEL_TYPE',
#                             'UNIT_NAME']
#
#        for df in [self.t2hhv_data_annual, tier2_ghg_data]:
#
#            if df != tier2_index_names:
#
#                df.reset_index(inplace=True)
#
#                df.set_index(tier2_index_names, inplace=True)
#
#            else:
#
#                continue
#
#        energy_check = pd.merge(self.t2hhv_data_annual, tier2_ghg_data,
#                                left_index=True, right_index=True,
#                                how='inner')
#
#        energy_check = pd.merge(energy_check.reset_index(),
#                                self.std_efs.reset_index(),
#                                on='FUEL_TYPE', how='left')
#
#        energy_check.set_index(tier2_index_names, inplace=True)
#
#        energy_check['mmtco2_calc'] = \
#            energy_check.energy_mmbtu.multiply(
#                    energy_check.CO2_kgCO2_per_mmBtu
#                    ).divide(1000)
#
#        energy_check['mmtco2_diff'] = \
#            energy_check[
#                ['mmtco2_calc', 'TIER2_CO2_COMBUSTION_EMISSIONS']
#                ].pct_change(axis=1)
#
#        energy_check.loc[:, 'mmtco2_diff'] = energy_check.mmtco2_diff.abs()
#
#        energy_check.sort_values('mmtco2_diff', ascending=False, inplace=True)
#
#        energy_check.to_csv('tier2_energy_check.csv')
#
#        if 'tier2_energy_check.csv' in os.listdir():
#
#            print('Energy check results saved')
#
#        else:
#
#            print('Error. Energy check results not saved')

    def tier1_calc(self, subpart_c_df):
        """
        Estimate energy use for facilities reporting emissions using the
        Tier 1 methodology.
        """

        tier_column = 'TIER1_CO2_COMBUSTION_EMISSIONS'

        ghg_data = self.filter_data(subpart_c_df, tier_column)

        energy = pd.DataFrame()

        for ftc in ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']:

            df = pd.merge(ghg_data,
                          pd.DataFrame(
                                  self.std_efs.loc[:, 'CO2_kgCO2_per_mmBtu']
                                  ), left_on=ftc, right_index=True,
                          how='inner')

            df['energy_mmbtu'] = df[tier_column].multiply(1000).divide(
                    df['CO2_kgCO2_per_mmBtu']
                    )

            energy = energy.append(df, sort=True)

        energy.drop(['CO2_kgCO2_per_mmBtu'], axis=1, inplace=True)

        return energy

    def tier2_calc(self, subpart_c_df):
        """
        Calculate energy use for facilities reporting emissions using the
        Tier 2 methodology. There are facilities that report Tier 2 emissions
        but do not report associated fuel hhv values for every combustion
        unit. Where possible, energy values in these instances are estimated
        using custom emission factors calculated from reported CO2 emissions
        and reported hhv values by fuel type and by facility. EPA standard
        emission factors are used to estimate energy values for remaining
        facilities.
        """

        tier_column = 'TIER2_CO2_COMBUSTION_EMISSIONS'

        ghg_data = self.filter_data(subpart_c_df, tier_column)

        energy = pd.DataFrame()

        t2_data_combined = pd.concat(
                [self.t2boiler_data_annual, self.t2hhv_data_annual],
                ignore_index=False, sort=True
                )

        fuel_type_cats = ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']

        for ft in fuel_type_cats:

            df = pd.DataFrame(ghg_data.dropna(subset=[ft], axis=0))

            if df.empty == True:

                continue

            else:

                if ft != 'FUEL_TYPE':

                    df.drop(set(fuel_type_cats).difference({ft}), axis=1,
                            inplace=True)

                    df.rename(columns={ft: 'FUEL_TYPE'}, inplace=True)

                df.set_index(['FACILITY_ID', 'REPORTING_YEAR', 'FUEL_TYPE',
                              'UNIT_NAME'], inplace=True)

                # Some facilities reporting Tier 2 emissions may be missing
                # from the tier 2 hhv table.
                # Appy standard emission factors for
                # these facilities to estimate energy use.
                df = pd.merge(df, t2_data_combined.dropna(
                        subset=['energy_mmbtu']
                        )[['energy_mmbtu']], left_index=True,
                        right_index=True, how='left')

                df.reset_index(inplace=True)

                df = pd.merge(df, self.std_efs.reset_index(),
                              on='FUEL_TYPE', how='left')

                df_no_mmbtu = pd.DataFrame(df[df.energy_mmbtu.isnull()])

                # Calculate emission factors by facility, fuel, and year,
                # and apply for facilities missing data in hhv table.
                # Tier 2 data weren't reported prior to 2014. Use
                # a simple average of the emission factors for 2014 - 2017
                # for remaining facilities.
                custom_efs = {}

                custom_efs['by_fac'] = df[df.energy_mmbtu.notnull()].groupby(
                    ['FACILITY_ID', 'REPORTING_YEAR', 'FUEL_TYPE']
                    )[tier_column].sum().divide(
                        df[df.energy_mmbtu.notnull()].groupby(
                            ['FACILITY_ID', 'REPORTING_YEAR','FUEL_TYPE']
                            ).energy_mmbtu.sum()
                        ).multiply(1000)

                custom_efs['all'] = \
                    df[(df.energy_mmbtu.notnull())].groupby(
                    ['FUEL_TYPE']
                    )[tier_column].sum().divide(
                        df[df.energy_mmbtu.notnull()].groupby(
                            ['FUEL_TYPE']
                            ).energy_mmbtu.sum()
                        ).multiply(1000)

                # Update EPA default emission factors with custom emission
                # factors calculated from Tier 2 data.
                for k in ['all', 'by_fac']:

                    custom_efs[k].name = 'CO2_kgCO2_per_mmBtu'

                    df_no_mmbtu.set_index(custom_efs[k].index.names,
                                          inplace=True)

                    df_no_mmbtu.CO2_kgCO2_per_mmBtu.update(custom_efs[k])

                    df_no_mmbtu.reset_index(inplace=True)

                energy_update = df_no_mmbtu[tier_column].divide(
                        df_no_mmbtu.CO2_kgCO2_per_mmBtu
                        )*1000

                energy_update.name = 'energy_mmbtu'

                df_no_mmbtu.energy_mmbtu.update(energy_update)

                df.dropna(subset=['energy_mmbtu'], axis=0, inplace=True)

                df = df.append(df_no_mmbtu, sort=True)

                df.drop(['CO2_kgCO2_per_mmBtu', 'CH4_gCH4_per_mmBtu'],
                        axis=1, inplace=True)

            energy = energy.append(df, ignore_index=True, sort=True)

        return energy

    def tier3_calc(self, subpart_c_df):

        """
        Emissions reported using the Tier 3 methodology rely on measurements
        of fuel combusted (in units of mass or volume) and carbon content.
        HHV information reported by Tier 2 facilities is used first to estimate
        energy values. EPA standard emission factors are then used to
        estimate energy use of unmatched fuels.

        """

        tier_column = 'TIER3_CO2_COMBUSTION_EMISSIONS'

        ghg_data = self.filter_data(subpart_c_df, tier_column)

        energy = pd.DataFrame()

        # Calculated annual hhv (mass or volumne per mmbtu) by fuel.
        # Note that reporting for these measurements began in 2014.
        hhv_average = self.t2hhv_data_annual.reset_index().groupby(
                    ['FUEL_TYPE']
                    ).high_heat_value_wa.mean()

        # Calculate energy value of combusted fuels
        t3_mmbtu = pd.DataFrame(self.t3_data_annual.fuel_combusted.values,
                                index=self.t3_data_annual.index,
                                columns=['fuel_combusted'])

        t3_mmbtu['energy_mmbtu'] = \
            t3_mmbtu.fuel_combusted.multiply(
                    hhv_average, level='FUEL_TYPE', fill_value=0
                    )

        t3_mmbtu.reset_index(inplace=True)

        for dataframe in [ghg_data, t3_mmbtu]:

            for col in ['FUEL_TYPE', 'UNIT_NAME']:

                dataframe[col] = dataframe[col].astype('str')

        t3_mmbtu.set_index(['FACILITY_ID', 'REPORTING_YEAR','UNIT_NAME'],
                            inplace=True)

        ghg_data.set_index(['FACILITY_ID', 'REPORTING_YEAR','UNIT_NAME'],
                            inplace=True)

        ghg_data = ghg_data.join(t3_mmbtu[['energy_mmbtu']])

        ghg_data.reset_index(inplace=True)

        energy = energy.append(
                ghg_data[ghg_data.energy_mmbtu.notnull()].copy(deep=True),
                ignore_index=True)

        # Match fuel types for remaining data
        fuel_type_cats = ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']

        for ft in fuel_type_cats:

            df_by_ef = pd.DataFrame(
                    ghg_data[ghg_data.energy_mmbtu.isnull()].dropna(
                            subset=[ft], axis=0
                            )
                    )

            if df_by_ef.empty == True:

                continue

            else:

#                # Calculate emissions first by hhv_average[by_fac], then by
#                # hhv_average[by_fuel], then by std emission factors
#                df.set_index(['FACILITY_ID', 'REPORTING_YEAR','UNIT_NAME'],
#                              inplace=True)
#
#
#
#                df_by_ef = pd.DataFrame(df[~df.index.isin(df_by_hhv.index)])



                df_by_ef = df_by_ef.set_index(ft).join(
                        self.std_efs['CO2_kgCO2_per_mmBtu']
                        )

                df_by_ef['energy_mmbtu'] = df_by_ef[tier_column].divide(
                        df_by_ef['CO2_kgCO2_per_mmBtu']
                        )*1000

                df_by_ef.reset_index(inplace=True)

                df_by_ef.rename(columns={'index': ft}, inplace=True)

                # Need to account for facilities that report volume and HHV
                # of blast furnace gas
                if ft == 'FUEL_TYPE':

                    fuel_qty = df_by_ef.dropna(subset=['TIER3_EQ_C5_FUEL_QTY'])

                    fuel_qty.energy_mmbtu = \
                        fuel_qty.TIER3_EQ_C5_FUEL_QTY.multiply(
                            fuel_qty.TIER3_EQ_C8_HHV_GAS
                            )

                    df_by_ef.energy_mmbtu.update(fuel_qty.energy_mmbtu)

                energy = energy.append(
                    df_by_ef[['FACILITY_ID', 'REPORTING_YEAR', ft,'UNIT_NAME',
                              tier_column, 'UNIT_TYPE', 'energy_mmbtu']],
                              ignore_index=True, sort=True
                    )

        energy.sort_index(inplace=True)

        return energy

    def tier4_calc(self, subpart_c_df):
        """
        Annual heat input and fuel quantity are not consistently reported
        by facilities using the Tier 4 approach. As a result, energy values
        that are not reported under the column ANNUAL_HEAT_INPUT are estimated
        using reported CH4 emissions and standard CH4 emissions factors.
        This is effectively the same approach and code used for Tier 1
        reported emissions
        """

        #CH4 emissions in metric tons
        tier_column = 'T4CH4COMBUSTIONEMISSIONS'

        ghg_data = self.filter_data(subpart_c_df, tier_column)

        energy = pd.DataFrame()

        reported_energy = pd.DataFrame(
                ghg_data[ghg_data.ANNUAL_HEAT_INPUT.notnull()]
                )

        reported_energy.rename(columns={'ANNUAL_HEAT_INPUT': 'energy_mmbtu'},
                               inplace=True)

        for ftc in ['FUEL_TYPE', 'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND']:

            df = pd.merge(
                    ghg_data[~ghg_data.index.isin(reported_energy.index)],
                          pd.DataFrame(
                                  self.std_efs.loc[:, 'CH4_gCH4_per_mmBtu']
                                  ), left_on=ftc, right_index=True,
                          how='inner'
                    )

            # multiply by 10**6 because emission factor in grams and not
            # kilograms
            df['energy_mmbtu'] = df[tier_column].multiply(10**6).divide(
                    df['CH4_gCH4_per_mmBtu']
                    )

            energy = energy.append(df, sort=True)

        energy.drop(['CH4_gCH4_per_mmBtu'], axis=1, inplace=True)

        energy = energy.append(reported_energy, sort=True)

        return energy

    def calc_all_tiers(self, subpart_c_df):
        """
        Assemble all the calculations and their results into a single
        dataframe.
        """

        energy = pd.DataFrame()

#        tier1_results = self.tier1_calc(subpart_c_df)
#
#        tier2_results = self.tier2_calc(subpart_c_df)
#
#        tier3_results = self.tier3_calc(subpart_c_df)
#
#        tier4_results = self.tier4_calc(subpart_c_df)
#
#        def final_formatting(df):
#
#            df.reset_index(inplace=True)
#
#            fts = ['FUEL_TYPE', 'FUEL_TYPE_BLEND', 'FUEL_TYPE_OTHER']
#
#            fts = list(set.intersection(set(fts), df.columns))
#
#            df.loc[:, 'final_ft'] = df[fts].apply(
#                    lambda x: pd.Series(x).sort_values()[0], axis=1
#                    )
#
#            df.dropna(axis=1, how='all', inplace=True)
#
#            df = df.set_index('final_ft').join(self.std_efs)
#
#            return df
#        print('tier1')
#        tier1_results = final_formatting(tier1_results)
#        print('tier2')
#        tier2_results = final_formatting(tier2_results)
#        print('tier3')
#        tier3_results = final_formatting(tier3_results)
#        print('tier4')
#        tier4_results = final_formatting(tier4_results)
#        tier1_results.dropna(axis=1, how='all').to_csv('check_tier12_c.csv')
#        tier2_results.dropna(axis=1, how='all').to_csv('check_tier22_c.csv')
#        tier3_results.dropna(axis=1, how='all').to_csv('check_tier3_c.csv')
#        tier4_results.dropna(axis=1, how='all').to_csv('check_tier42_c.csv')

        energy = energy.append(self.tier1_calc(subpart_c_df),
                               ignore_index=True, sort=True)

        energy = energy.append(self.tier2_calc(subpart_c_df),
                               ignore_index=True, sort=True)

        energy = energy.append(self.tier3_calc(subpart_c_df),
                               ignore_index=True, sort=True)

        energy = energy.append(self.tier4_calc(subpart_c_df),
                               ignore_index=True, sort=True)

        energy.dropna(axis=1, how='all', inplace=True)

        energy.rename(columns={'energy_mmbtu': 'MMBtu_TOTAL'}, inplace=True)

        return energy
