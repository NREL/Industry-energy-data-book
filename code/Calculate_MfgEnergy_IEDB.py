import pandas as pd
import numpy as np
import itertools as itools
import os
import io
import zipfile
import re
import requests
import dask.dataframe as dd


class Manufacturing_energy:
    """
    Calcualtes energy for a single year, combining energy values for GHGRP
    facilities calculated by GHGRP_config_IPH.py, with energy values calculated
    below for remaining manufacturing facilities.

    """

    #Set analysis year and required file paths
    def __init__(self, year, energy_ghgrp):

        self.year = year

        if self.year > 2012:

            self.naics_column = 'PRIMARY_NAICS_CODE_12'

        else:

            self.naics_column = 'PRIMARY_NAICS_CODE'

        self.file_dir = '../calculation_data/'

        self.fuelxwalk_file = 'MECS_FT_IPF.csv'

        self.naics_2012_file = 'mecs_naics_2012.csv'

        self.naics_old_file = 'mecs_naics.csv'

        self.ipf_results_file = 'mecs_'+str(self.year)+\
            '_ipf_results_naics_employment.csv'

        self.mecs_naics = pd.DataFrame()

        if self.year >=2014:

            self.end_use_file = 'table5_2_2014_formatted.csv'

        else:

            self.end_use_file = 'table5_2_2010_formatted.csv'

        self.eia_epa_ids = pd.read_csv(
                os.path.join(self.file_dir, 'eia_epa_xwalk.csv'),
                encoding='latin_1')

        for file in [self.naics_2012_file, self.naics_old_file]:

            mdf = pd.read_csv(os.path.join(self.file_dir, file))

            if file == 'mecs_naics.csv':

                mdf['vintage'] = 2010

            else:

                mdf['vintage'] = 2012

            self.mecs_naics = self.mecs_naics.append(mdf, ignore_index=True)

        self.fuelxwalkDict = dict(pd.read_csv(
                os.path.join(self.file_dir, self.fuelxwalk_file)
                )[["EPA_FUEL_TYPE", "MECS_FT"]].values)

        self.empsize_dict = {'Under 50': 'n1_49', '50-99': 'n50_99',
                             '100-249': 'n100_249','250-499': 'n250_499',
                             '500-999': 'n500_999','1000 and Over': 'n1000'}

        self.energy_ghgrp_y = pd.DataFrame(
                energy_ghgrp[energy_ghgrp.REPORTING_YEAR == self.year]
                )

    def update_naics(self, ghgrp_matching):
        """
        Import list of NAICS codes used in MECS. Need to account for CBP data
        after 2011 use 2012 NAICS, while MECS and GHGRP use 2007 NAICS.
        """

        def MatchMECS_NAICS(DF, naics_column):
            """
            Method for matching 6-digit NAICS codes with adjusted
            MECS NAICS.
            """
            DF[naics_column].fillna(0, inplace = True)

            DF.loc[:, naics_column] = DF[naics_column].astype('int')

            DF_index = DF[DF[naics_column].between(310000, 400000,
                          inclusive=False)]

            #split ghgrp data into pre- and post-2012 to account for the
            # change in NAICS base year for CBP data.

            nctest = DF.loc[DF_index.index, [naics_column, 'REPORTING_YEAR']]

            for n in ['N6', 'N5', 'N4', 'N3']:

                n_level = int(n[1])

                nctest[n] = nctest[naics_column].apply(
                        lambda x: int(str(x)[0:n_level]))

            #Match GHGRP NAICS to highest-level MECS NAICS. Will match to
            #"dummy" "-09" NAICS where available. This is messy, but
            #functional.
            if self.year < 2012:

                ncmatch = pd.concat(
                    [pd.merge(nctest,
                              self.mecs_naics[self.mecs_naics.vintage == 2010],
                              left_on=column, right_on='MECS_NAICS',
                              how= 'left')['MECS_NAICS']
                        for column in ['N6', 'N5', 'N4', 'N3']], axis=1
                    )

            else:

                ncmatch = pd.concat(
                    [pd.merge(nctest,
                              self.mecs_naics[self.mecs_naics.vintage == 2012],
                              left_on=column, right_on='MECS_NAICS',
                              how='left')['MECS_NAICS']
                        for column in ['N6', 'N5', 'N4', 'N3']], axis =1
                    )

            ncmatch.columns = ['N6', 'N5', 'N4', 'N3']

            ncmatch.index = nctest.index

            ncmatch['NAICS_MATCH'] = 0

            for n in range(3, 7):

                column = 'N'+str(n)

                ncmatch.NAICS_MATCH.update(ncmatch[column].dropna())

            #Update GHGRP dataframe with matched MECS NAICS.
            DF['MECS_NAICS'] = 0

            DF['MECS_NAICS'].update(ncmatch.NAICS_MATCH)

            return DF


        # Map EPA fuel types to MECS fuel types. Note this doens't cover all
        # custom fuel types in GHGRP.
        self.energy_ghgrp_y['MECS_FT'] = np.nan

        for f in ['FUEL_TYPE_OTHER','FUEL_TYPE_BLEND', 'FUEL_TYPE']:

            self.energy_ghgrp_y['MECS_FT'].update(
                    self.energy_ghgrp_y[f].map(self.fuelxwalkDict)
                    )

        #Match GHGRP-reported 6-digit NAICS code with MECS NAICS
        
        if self.naics_column == 'PRIMARY_NAICS_CODE_12':
            
            self.energy_ghgrp_y = \
                pd.merge(self.energy_ghgrp_y,
                         ghgrp_matching[['FACILITY_ID',
                                         self.naics_column]],
                         on='FACILITY_ID', how='left')

        self.energy_ghgrp_y = MatchMECS_NAICS(
                self.energy_ghgrp_y, 'PRIMARY_NAICS_CODE'
                )

        # Filter out facilities that use PRIMARY_NAICS_CODE == 486210 and
        # NAICS_USED == 0
        self.energy_ghgrp_y = self.energy_ghgrp_y[
                (self.energy_ghgrp_y[self.naics_column] != 486210)
                ].copy(deep=True)


    def GHGRP_Totals_byMECS(self):
        """
        From calculated GHGRP energy data, create sums by MECS Region,
        MECS NAICS and MECS fuel type for a given MECS year.
        """

        ghgrp_mecs = pd.DataFrame(
            self.energy_ghgrp_y[self.energy_ghgrp_y.MECS_NAICS != 0][
                        ['MECS_Region', 'MECS_NAICS', 'MECS_FT','MMBtu_TOTAL']
                        ]
            )

        ghgrp_mecs.dropna(inplace = True)

        ghgrp_mecs['MECS_R_FT'] = ghgrp_mecs['MECS_Region'] + '_' + \
            ghgrp_mecs['MECS_FT']

        r_f = []

        for r in ['Midwest', 'Northeast', 'South', 'West']:

            r_f.append([r + '_' + c + '_Total' for c in ghgrp_mecs[
                    ghgrp_mecs.MECS_Region == r
                    ].MECS_FT.dropna().unique()])

        for n in range(len(r_f)):
            r_f[n].append(r_f[n][1].split("_")[0] + "_Total_Total")

        if self.year < 2012:

            ghgrp_mecstotals = pd.DataFrame(
                index=self.mecs_naics[
                    self.mecs_naics.vintage == 2010
                    ].MECS_NAICS_dummies, columns=np.array(r_f).flatten()
                )

        else:

            ghgrp_mecstotals = pd.DataFrame(
                index=self.mecs_naics[
                    self.mecs_naics.vintage == 2012
                    ].MECS_NAICS_dummies, columns=np.array(r_f).flatten()
                )

        for name, group in ghgrp_mecs.groupby(['MECS_R_FT', 'MECS_NAICS'])[
            'MMBtu_TOTAL']:
                ghgrp_mecstotals.loc[int(name[1]), name[0] + '_Total'] = \
                    group.sum()

        for name, group in ghgrp_mecs.groupby(['MECS_Region', 'MECS_NAICS'])[
            'MMBtu_TOTAL']:
                ghgrp_mecstotals.loc[
                    int(name[1]), name[0] + '_Total_Total'] = group.sum()

        ghgrp_mecstotals.fillna(0, inplace=True)

        # Convert from MMBtu to TBTu
        ghgrp_mecstotals = ghgrp_mecstotals/10**6

        return ghgrp_mecstotals


    def GHGRP_electricity_calc(self):
        """
        Retrieve EIA 923 data and return a dataframe of net electricity for
            industrial cogen and noncogen reporters.
        """

        base_url = \
            'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_'

        try:

            r = requests.get(base_url+str(self.year)+'.zip')

        except requests.exceptions.RequestException as e:

            print(e)

        with zipfile.ZipFile(io.BytesIO(r.content)) as data_zip:

            file_list = data_zip.namelist()
            
            skip_rows_dis = 4
            
            code_column = 'Sector Code'
            
            if self.year == 2010:
                
                dis_sheet = 'Schedule 6'
                
                skip_rows_dis = 8
                
                code_column = 'Sector_Code'
                
            elif self.year == 2011:
                
                dis_sheet = 'Source_and_disposition, 2011'
            
            else:
                
                dis_sheet = 'Source_and_disposition'

            for f in file_list:

                if re.search('Disposition', f):

                    with data_zip.open(f) as data_file:

                        eia923_dis = pd.read_excel(
                                data_file, skiprows=skip_rows_dis, 
                                sheet_name=dis_sheet
                                )

                        eia923_dis.columns = \
                            [x.replace('\n', ' ') for x in eia923_dis.columns]
                            
                        eia923_dis.columns = \
                            [x.replace('  ', ' ') for x in eia923_dis.columns]
                            
                        eia923_dis.columns = \
                            [x.title() for x in eia923_dis.columns]

                        eia923_dis = pd.DataFrame(
                                eia923_dis[eia923_dis[code_column].isin(
                                        [6, 7]
                                        )]
                                )

                    data_file.close()

                if re.search('2_3_4_5', f):

                    with data_zip.open(f) as data_file:
                        
                        skip_rows_gen = 5
                        
                        if self.year == 2010:
                            
                            skip_rows_gen = 7

                        eia923_gen = pd.read_excel(
                                data_file, skiprows=skip_rows_gen,
                                sheet_name='Page 1 Generation and Fuel Data'
                                )

                        eia923_gen.columns = \
                            [x.replace('\n', ' ') for x in eia923_gen.columns]
                            
                        eia923_gen.columns = \
                            [x.replace('  ', ' ') for x in eia923_gen.columns]
                            
                        eia923_gen.columns = \
                            [x.title() for x in eia923_gen.columns]

                        eia923_gen = pd.DataFrame(
                            eia923_gen[
                                (eia923_gen['Eia Sector Number'].isin([6, 7]))&
                                (eia923_gen['Aer Fuel Type Code'].isin(
                                        ['SUN', 'GEO', 'HPS', 'HYC', 'WND']
                                        ))]
                                )

                        eia923_gen = eia923_gen.groupby('Plant Name')[
                                'Net Generation (Megawatthours)'
                                ].sum()

                        eia923_gen.name = 'RE_gen'

                        data_file.close()

                else:

                    continue

        eia923 = eia923_dis.set_index('Plant Name').join(eia923_gen)

        eia923.replace('.', 0, inplace=True)

        eia923.fillna(0, inplace=True)

        for col in ['Other Incoming Electricity', 'Incoming Electricity',
                    'RE_gen', 'Retail Sales', 'Sales for Resale',
                    'Sales For Resale','Tolling Agreements']:
            
            if col in eia923.columns:

                eia923[col] = eia923[col].astype(int)
                
            else:
                
                continue
            
        if self.year == 2010: 

            eia923['Net_electricity'] = eia923[
                    ['Other Incoming Electricity', 'RE_gen']
                    ].sum(axis=1).subtract(
                        eia923[
                            ['Retail Sales', 'Sales For Resale']
                            ].sum(axis=1)
                        )
               
        else:
            
            eia923['Net_electricity'] = eia923[
                    ['Incoming Electricity', 'RE_gen']
                    ].sum(axis=1).subtract(
                        eia923[['Retail Sales', 'Sales For Resale',
                                'Tolling Agreements']].sum(axis=1)
                        )

        # Convert from MWh to MMBtu
        eia923['Net_electricity'].update(eia923.Net_electricity.multiply(
                3.41214163)
                )

        ghgrp_electricity = eia923.groupby(
                'Plant Code', as_index=False
                ).Net_electricity.sum()

        ghgrp_electricity = pd.merge(ghgrp_electricity, self.eia_epa_ids[
                ['FACILITY_ID', 'EIA_PLANT_ID']
                ].drop_duplicates(), left_on='Plant Code',
            right_on='EIA_PLANT_ID', how='inner')
        
        elect_fac_ids = ghgrp_electricity.FACILITY_ID.values

        ghgrp_electricity['MECS_FT'] = 'Net_electricity'

        ghgrp_electricity.rename(columns={'Net_electricity': 'MMBtu_TOTAL'},
                                 inplace=True)

        ghgrp_electricity = ghgrp_electricity.set_index('FACILITY_ID').join(
            self.energy_ghgrp_y.set_index('FACILITY_ID')[
                ~self.energy_ghgrp_y.set_index('FACILITY_ID').index.duplicated()
                ][['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                   'MECS_NAICS', 'fipstate']], how='inner')

        ghgrp_electricity['data_source'] = 'eia'

        ghgrp_electricity.rename(columns={'PRIMARY_NAICS_CODE':'naics'},
                                       inplace=True)

        ghgrp_electricity['COUNTY_FIPS'] = \
            ghgrp_electricity.COUNTY_FIPS.astype(int)

        ghgrp_electricity = dd.from_pandas(
            ghgrp_electricity.set_index('fipstate'),
            npartitions=len(ghgrp_electricity.fipstate.unique())
            )

        return ghgrp_electricity, elect_fac_ids

    def calc_intensities(self, cbp_matching):
        """
        Calculate MECS intensities (energy per establishment) based on 2010 or
        2014 CBP establishment counts and IPF results.
        Note that datasets don't match perfectly-- i.e., results of 'NaN'
        indicate that IPF calculated an energy value for a MECSs region, NAICS,
        and facility count that corresponds to a zero CBP facility count;
        results of 'inf' indicate a nonzero CBP facility count for a
        MECS region, NAICS, and facility count with an IPF-caculated energy
        value of zero.
        """

        #Format results from IPF of MECS energy data by region, fuel type,
        #and employment size.

        ipf_results_formatted = pd.read_csv(
                self.file_dir+self.ipf_results_file, index_col=0
                )

        mecs_intensities = pd.melt(
                ipf_results_formatted,
                id_vars=['MECS_Region', 'Emp_Size', 'MECS_FT'],
                var_name=['MECS_NAICS_dummies'], value_name='energy'
                )

        mecs_intensities['MECS_NAICS_dummies'] =\
            mecs_intensities.MECS_NAICS_dummies.astype('int')

        mecs_intensities.set_index(
                ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size'],
                inplace=True
                )

        cbp_grpd = cbp_matching.groupby(
                ['MECS_Region', 'MECS_NAICS_dummies'], as_index=False
                ).sum()

        cbp_grpd = pd.melt(
                cbp_grpd, id_vars=['MECS_Region', 'MECS_NAICS_dummies'],
                value_vars=[x for x in self.empsize_dict.values()],
                var_name=['Emp_Size'], value_name='est_count'
                )

        cbp_grpd.set_index(
                ['MECS_Region', 'MECS_NAICS_dummies', 'Emp_Size'],
                inplace=True
                )

        mecs_intensities = mecs_intensities.join(cbp_grpd)

        mecs_intensities['intensity'] =\
            mecs_intensities.energy.divide(mecs_intensities.est_count,
                                           fill_value=0)

        mecs_intensities.drop(['energy', 'est_count'], axis=1, inplace=True)

        mecs_intensities.reset_index(inplace=True)

        #Fill NaN values for intensities with 0.
        mecs_intensities.fillna(0, inplace=True)

        mecs_intensities.replace(np.inf, 0, inplace=True)

        return mecs_intensities


    def combfuel_calc(self, cbp_corrected, mecs_intensities):

        """
        Calculate county-level manufacturing energy use based on CBP facility
        counts, calculated MECS intensities, and calculated facility energy use
        for GHGRP facilites.
        Net electricity undergoes an additional adjustment.
        
        County sums are not disaggregated by employment size class.

        Returns a Dask DataFrame
        """

        energy_nonghgrp = pd.melt(
                cbp_corrected, id_vars=['fips_matching', 'MECS_NAICS_dummies',
                                        'MECS_Region', 'fipstate', 'fipscty',
                                        'naics', 'COUNTY_FIPS', 'MECS_NAICS'],
                value_vars=[x for x in self.empsize_dict.values()],
                var_name=['Emp_Size'], value_name='est_count'
                )
        
        # Get rid of non-manufacturing naics
        energy_nonghgrp = pd.DataFrame(
                energy_nonghgrp[energy_nonghgrp.MECS_NAICS !=0]
                )

        # Need to set mecs_intensities index to include MECS_FT? Then reindex
        # energy_nonghgrp?

        energy_nonghgrp = dd.merge(
            energy_nonghgrp.set_index(
                ['MECS_NAICS_dummies']
                ), mecs_intensities[
                    mecs_intensities.MECS_FT != 'Net_electricity'
                    ].set_index(
                        ['MECS_NAICS_dummies']
                        ), on=['MECS_NAICS_dummies', 'MECS_Region', 'Emp_Size'],
                         how='inner'
            )

        energy_nonghgrp.reset_index(inplace=True)

        energy_nonghgrp['MMBtu_TOTAL'] = energy_nonghgrp.est_count.multiply(
                energy_nonghgrp.intensity, fill_value=0
                )*10**6

        energy_nonghgrp.drop(['fips_matching'], axis=1, inplace=True)

        energy_nonghgrp['COUNTY_FIPS'] = \
            energy_nonghgrp.COUNTY_FIPS.astype(int)

        energy_nonghgrp = energy_nonghgrp.groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS',
                 'MECS_FT', 'fipstate', 'fipscty'], as_index=False
                )[['MMBtu_TOTAL', 'est_count']].sum()

        energy_nonghgrp['data_source'] = 'mecs_ipf'

        energy_nonghgrp = pd.merge(
            energy_nonghgrp, self.energy_ghgrp_y[['COUNTY_FIPS', 'STATE']],
            on='COUNTY_FIPS', how='left'
            )

        energy_ghgrp_y = self.energy_ghgrp_y.groupby(
                ['MECS_Region', 'STATE', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                 'MECS_NAICS','MECS_FT'], as_index=False
                ).MMBtu_TOTAL.sum()

        energy_ghgrp_y['data_source'] = 'ghgrp'

        energy_ghgrp_y.rename(columns={'PRIMARY_NAICS_CODE':'naics'},
                                       inplace=True)

        energy_ghgrp_y['COUNTY_FIPS'] = energy_ghgrp_y.COUNTY_FIPS.astype(int)

        fips_dict = energy_nonghgrp[
                ['COUNTY_FIPS', 'fipstate', 'fipscty']
                ].drop_duplicates().set_index('COUNTY_FIPS').to_dict('index')

        def match_countyfips(county_fips, fips_dict, fips_cat):

            if county_fips in fips_dict.keys():

                return fips_dict[county_fips][fips_cat]

            else:

                fips_len = len(str(county_fips))

                missing_dict = {
                        'fipstate': int(str(county_fips)[0:fips_len-3]),
                        'fipscty': int(str(county_fips)[fips_len-3:])
                        }

                return missing_dict[fips_cat]


        energy_ghgrp_y['fipscty'] = energy_ghgrp_y.COUNTY_FIPS.apply(
                lambda x: match_countyfips(x, fips_dict, 'fipscty')
                )

        energy_ghgrp_y['fipstate'] = energy_ghgrp_y.COUNTY_FIPS.apply(
                lambda x: match_countyfips(x, fips_dict, 'fipstate')
                )

        county_combustion_energy_dd = dd.from_pandas(
            energy_nonghgrp.append(
                energy_ghgrp_y, ignore_index=True
                ).set_index('fipstate'),
            npartitions=len(energy_nonghgrp.fipstate.unique())
            )

        county_combustion_energy_dd['naics'] =\
                county_combustion_energy_dd.naics.astype('int').compute()

        return county_combustion_energy_dd

    def electricity_calc(self, cbp_corrected_923, mecs_intensities):

        """
        Calculate county-level manufacturing net electricity use based on CBP
        facility counts, calculated MECS intensities, and net electricity
        use calcualted from EIA 923 data.

        Returns a Dask DataFrame
        """

        elect_nonghgrp = pd.melt(
                cbp_corrected_923,
                id_vars=['fips_matching', 'MECS_NAICS_dummies',
                         'MECS_Region', 'fipstate', 'fipscty', 'naics',
                         'COUNTY_FIPS', 'MECS_NAICS'],
                value_vars=[x for x in self.empsize_dict.values()],
                var_name=['Emp_Size'], value_name='est_count'
                )

        # Need to set mecs_intensities index to include MECS_FT? Then reindex
        # elect_nonghgrp?

        elect_nonghgrp = dd.merge(
            elect_nonghgrp.set_index(
                ['MECS_NAICS_dummies']
                ), mecs_intensities[
                    mecs_intensities.MECS_FT == 'Net_electricity'
                    ].set_index(
                        ['MECS_NAICS_dummies']
                        ), on=['MECS_NAICS_dummies', 'MECS_Region',
                               'Emp_Size'], how='inner'
            )

        elect_nonghgrp.reset_index(inplace=True)

        elect_nonghgrp['MMBtu_TOTAL'] = elect_nonghgrp.est_count.multiply(
                elect_nonghgrp.intensity, fill_value=0
                )*10**6

        elect_nonghgrp.drop(['fips_matching'], axis=1, inplace=True)

        elect_nonghgrp['COUNTY_FIPS'] = \
            elect_nonghgrp.COUNTY_FIPS.astype(int)

        elect_nonghgrp = elect_nonghgrp.groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'naics', 'MECS_NAICS',
                 'MECS_FT', 'fipstate', 'fipscty'], as_index=False
                )[['MMBtu_TOTAL', 'est_count']].sum()

        elect_nonghgrp['data_source'] = 'mecs_ipf'

#        fips_dict = elect_nonghgrp[
#                ['COUNTY_FIPS', 'fipstate', 'fipscty']
#                ].drop_duplicates().set_index('COUNTY_FIPS').to_dict('index')
#
#        def match_countyfips(county_fips, fips_dict, fips_cat):
#
#            if county_fips in fips_dict.keys():
#
#                return fips_dict[county_fips][fips_cat]
#
#            else:
#
#                fips_len = len(str(county_fips))
#
#                missing_dict = {
#                        'fipstate': int(str(county_fips)[0:fips_len-3]),
#                        'fipscty': int(str(county_fips)[fips_len-3:])
#                        }
#
#                return missing_dict[fips_cat]
#
#
#        energy_eia923['fipscty'] = energy_eia923.COUNTY_FIPS.apply(
#                lambda x: match_countyfips(x, fips_dict, 'fipscty')
#                )
#
#        energy_eia923['fipstate'] = energy_eia923.COUNTY_FIPS.apply(
#                lambda x: match_countyfips(x, fips_dict, 'fipstate')
#                )

        county_elect_dd = dd.from_pandas(
                elect_nonghgrp.set_index('fipstate'),
                npartitions=len(elect_nonghgrp.fipstate.unique())
                )

        county_elect_dd['naics'] =\
                county_elect_dd.naics.astype('int').compute()

        return county_elect_dd


    def calc_enduse(self, eu_fraction_dict, county_energy_dd, temps=False):
        """

        Returns Dask DataFrame
        """
        unitname_eu_dict = {
                'Process Heating': ['furnace', 'kiln', 'dryer', 'heater',
                                    'oven','calciner', 'stove', 'htr', 'furn',
                                    'cupola'],
                'Conventional Boiler Use': ['boiler'],
                'CHP and/or Cogeneration Process': ['turbine'],
                'Facility HVAC': ['building heat', 'space heater'],
                'Machine Drive': ['engine','compressor', 'pump', 'rice'],
                'Conventional Electricity Generation': ['generator'],
                'Other Nonprocess Use': ['hot water', 'crane', 'water heater',
                                     'comfort heater', 'RTO', 'TODF',
                                     'oxidizer', 'RCO']
                }

        unittype_eu_dict = {
                'Process Heating': ['F', 'PD', 'K', 'PRH', 'O', 'NGLH', 'CF',
                                 'HMH', 'C', 'HPPU', 'CatH', 'COB', 'FeFL',
                                 'Chemical Recovery Furnace', 'IFCE',
                                 'Pulp Mill Lime Kiln', 'Lime Kiln',
                                 'Chemical Recovery Combustion Unit',
                                 'Direct Reduction Furnace',
                                 'Sulfur Recovery Plant'],
                'Conventional Boiler Use': ['OB', 'S', 'PCWW', 'BFB', 'PCWD',
                                        'PCT', 'CFB', 'PCO', 'OFB', 'PFB'],
                'CHP and/or Cogeneration Process': ['CCCT', 'SCCT'],
                'Facility HVAC': ['CH'],
                'Other Nonprocess Use': ['HWH', 'TODF', 'ICI', 'FLR', 'RTO',
                                         'II', 'MWC', 'Flare', 'RCO' ],
                'Conventional Electricity Generation': ['RICE',
                                                        'Electricity Generator']
                }

        def eu_dict_to_df(eu_dict):
            """
            Convert unit type/unit name dictionaries to dataframes.
            """
            eu_df = pd.DataFrame.from_dict(
                    eu_dict, orient='index'
                    ).reset_index()

            eu_df = pd.melt(
                    eu_df, id_vars='index', value_name='unit'
                    ).rename(columns={'index': 'end_use'}).drop(
                            'variable', axis=1
                            )

            eu_df = eu_df.dropna().set_index('unit')

            return eu_df

        def eu_unit_type(unit_type, unittype_eu_df):
            """
            Match GHGRP unit type to end use specified in unittype_eu_dict.
            """

            enduse = re.match('(\w+) \(', unit_type)

            if enduse != None:

                enduse = re.match('(\w+)', enduse.group())[0]

                if enduse in unittype_eu_df.index:

                    enduse = unittype_eu_df.loc[enduse, 'end_use']

                else:

                    enduse = np.nan

            else:

                if unit_type in unittype_eu_df.index:

                    enduse = unittype_eu_df.loc[unit_type, 'end_use']

            return enduse

        def eu_unit_name(unit_name, unitname_eu_df):
            """
            Find keywords in GHGRP unit name descriptions and match them
            to appropriate end uses based on unitname_eu_dict.
            """

            for i in unitname_eu_df.index:

                enduse = re.search(i, unit_name.lower())

                if enduse == None:

                    continue

                else:

                    enduse = unitname_eu_df.loc[i, 'end_use']

                    return enduse

            enduse = np.nan

            return enduse


        unittype_eu_df = eu_dict_to_df(unittype_eu_dict)

        unitname_eu_df = eu_dict_to_df(unitname_eu_dict)

        # Base ghgrp energy end use disaggregation on reported unit type and
        # unit name.
        eu_ghgrp = self.energy_eia923.copy(deep=True)

        # First match end uses to provided unit types. Most unit types are
        # specified as OCS (other combustion source).
        unit_types = eu_ghgrp.UNIT_TYPE.dropna().unique()

        type_match = list()

        for utype in unit_types:

            enduse = eu_unit_type(utype, unittype_eu_df)

            type_match.append([utype, enduse])

        type_match = pd.DataFrame(type_match,
                                  columns=['UNIT_TYPE', 'end_use'])

        eu_ghgrp = pd.merge(eu_ghgrp, type_match, on='UNIT_TYPE', how='left')

        # Next, match end use by unit name for facilites that report OCS for
        # unit type.
        eu_ocs = eu_ghgrp[
                (eu_ghgrp.UNIT_TYPE == 'OCS (Other combustion source)') |
                (eu_ghgrp.UNIT_TYPE.isnull())
                ][['UNIT_TYPE', 'UNIT_NAME']]

        eu_ocs['end_use'] = eu_ocs.UNIT_NAME.apply(
                lambda x: eu_unit_name(x, unitname_eu_df)
                )

        eu_ghgrp.end_use.update(eu_ocs.end_use)

        eu_ghgrp.drop(eu_ghgrp.columns.difference(
                set(['COUNTY_FIPS','MECS_Region', 'MMBtu_TOTAL', 'MECS_FT',
                     'PRIMARY_NAICS_CODE', 'MECS_NAICS','end_use',
                     'FACILITY_ID'])
                ), axis=1, inplace=True)

        # sum energy of unit types and unit names matched to an end use
        eu_ghgrp_matched = eu_ghgrp[eu_ghgrp.end_use.notnull()].pivot_table(
                values='MMBtu_TOTAL', columns='end_use',
                index=['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                       'MECS_NAICS', 'MECS_FT'], aggfunc='sum', fill_value=0
                )

        eu_ghgrp_matched = eu_ghgrp_matched.join(
                eu_ghgrp.pivot_table(values='FACILITY_ID',
                index=['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE',
                       'MECS_NAICS', 'MECS_FT'], aggfunc='count')
                )

        # Calculate the remaining GHGRP facilities energy use
        # with MECS data.
        eu_ghgrp_notmatched = \
            eu_ghgrp[(eu_ghgrp.end_use.isnull()) &
                     (eu_ghgrp.MECS_FT.notnull())].copy(deep=True)

        enduses = eu_fraction_dict['GHGRP'].columns.values

        eu_ghgrp_notmatched = pd.merge(
                eu_ghgrp_notmatched.set_index(['MECS_NAICS', 'MECS_FT']),
                eu_fraction_dict['GHGRP'], left_index=True,
                right_index=True, how='left'
                )

        for eu in enduses:

            eu_ghgrp_notmatched[eu] = \
                eu_ghgrp_notmatched.MMBtu_TOTAL.multiply(
                        eu_ghgrp_notmatched[eu], fill_value=0
                        )

        agg_cols = [x for x in itools.product(enduses, ['sum'])]

        agg_cols.append(('FACILITY_ID', 'count'))

        eu_ghgrp_notmatched = eu_ghgrp_notmatched.reset_index().groupby(
                ['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE', 'MECS_NAICS',
                 'MECS_FT',], as_index=False).agg(dict(agg_cols))

        eu_ghgrp_notmatched.set_index('MECS_NAICS', inplace=True)

        eu_ghgrp_matched.reset_index(
                ['MECS_Region', 'COUNTY_FIPS', 'PRIMARY_NAICS_CODE','MECS_FT'],
                inplace=True
                )

        for df in [eu_ghgrp_matched, eu_ghgrp_notmatched]:

            df.rename(columns={'PRIMARY_NAICS_CODE':'naics',
                               'FACILITY_ID': 'est_count'}, inplace=True)


            df['data_source'] = 'ghgrp'


        # Calculate end use of energy estimated from MECS data with MECS end
        # use.
        enduses = eu_fraction_dict['nonGHGRP'].columns.values

        eu_energy_dd = dd.merge(
                county_energy_dd[county_energy_dd.data_source=='mecs_ipf'],
                eu_fraction_dict['nonGHGRP'].reset_index('MECS_FT'),
                on=['MECS_NAICS', 'MECS_FT'], how='left'
                )

        for eu in enduses:

            eu_energy_dd[eu] = \
                eu_energy_dd.MMBtu_TOTAL.mul(eu_energy_dd[eu],
                                             fill_value=0)

        # This throws FutureWanring related to sorting for pandas concat,
        # but currently there's no option to address this in dd.concat
        eu_energy_dd = dd.concat(
                [df for df in [eu_energy_dd, eu_ghgrp_matched,
                               eu_ghgrp_notmatched]], axis='index',
                join='outer', interleave_partitions=True)


        eu_energy_dd_final = dd.melt(
                eu_energy_dd.reset_index(), value_vars=enduses.tolist(),
                id_vars=['MECS_NAICS', 'COUNTY_FIPS', 'Emp_Size', 'MECS_FT',
                         'MECS_Region', 'data_source', 'est_count', 'fipscty',
                         'fipstate', 'naics'], var_name='End_use',
                value_name='MMBtu'
                )

        # clean up by removing MMBtu values == 0..
        eu_energy_dd_final = \
            eu_energy_dd_final[eu_energy_dd_final.MMBtu !=0]

        eu_energy_dd_final = eu_energy_dd_final.set_index('MECS_NAICS')
#                                                          compute=True)

        return eu_energy_dd_final



    ##
    # #Results analysis
    # with pd.ExcelWriter('2010_comparisons.xlsx') as writer:
    #   CountyEnergy.groupby('MECS_Region').sum().to_excel(
    #       writer, sheet_name = 'By Region'
    #       )
    #   CountyEnergy_wGHGRP.groupby('MECS_Region').sum().to_excel(
    #       writer, sheet_name = 'By Region wGHGRP'
    #       )
    #   CountyEnergy.groupby('MECS_NAICS').sum().to_excel(
    #       writer, sheet_name = 'By NAICS'
    #       )
    #   CountyEnergy_wGHGRP.groupby('MECS_NAICS').sum().to_excel(
    #       writer, sheet_name = 'By NAICS wGHGRP'
    #       )
    #   CountyEnergy.groupby(('MECS_Region', 'MECS_NAICS')).sum().to_excel(
    #       writer, sheet_name = 'By Region & NAICS'
    #       )
    #   CountyEnergy_wGHGRP.groupby(('MECS_Region', 'MECS_NAICS')).sum().to_excel(
    #       writer, sheet_name = 'By Region & NAICS wGHGRP'
    #       )
    #
