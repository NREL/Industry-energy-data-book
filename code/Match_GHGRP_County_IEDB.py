import pandas as pd
import numpy as np
import itertools as itools
import os

#%%
class County_matching:
    """
    Class containing methods to further format Census Business Patterns (CBP)
    establishment count data by NAICS and employment size. Then corrects
    establishment count data using EPA GHGRP data.
    """

    def __init__(self, year):

        self.year = year

        self.data_dir = 'calculation_data'

        self.mecs2010_naics_file = '/mecs_naics.csv'

        self.mecs2014_naics_file = '/mecs_naics_2012.csv'

        self.fips_file = '/US_FIPS_Codes.csv'

        if self.year > 2011:

            mecs_naics_file = self.mecs2014_naics_file

        else:

            mecs_naics_file = self.mecs2010_naics_file

        self.mecs_naics = pd.read_csv(
                os.path.join('../', self.data_dir + mecs_naics_file)
                )

        self.MECS_regions = pd.read_csv(
            os.path.join('../', self.data_dir + self.fips_file),
            index_col=['COUNTY_FIPS']
            )

    def format_ghgrp(self, energy_ghgrp, cbp_matching):
        """
        Import GHGRP reporting data and calculated energy. Create count of GHGRP
        reporters by zip and NAICS.Excludes facilities where calculated
        energy == 0. This avoids removing facilities from the CBP count that
        don't have a GHGRP-calculated energy value.
        """

        ghgrp_matching = pd.DataFrame(
            energy_ghgrp[(energy_ghgrp.MMBtu_TOTAL !=0) &
                         (energy_ghgrp.REPORTING_YEAR == self.year)],
            columns=['FACILITY_ID', 'FACILITY_NAME',
                     'COUNTY', 'COUNTY_FIPS', 'LATITUDE','LONGITUDE', 'STATE',
                     'ZIP', 'PRIMARY_NAICS_CODE','SECONDARY_NAICS_CODE']
            )

        ghgrp_matching.reset_index(inplace=True, drop=True)

        #Apply pimary NAICS code as secondary NAICS code where facility has
        #reported none.
        snaics_na = ghgrp_matching[
                ghgrp_matching.SECONDARY_NAICS_CODE.isnull()
                ].index

        ghgrp_matching.loc[snaics_na, 'SECONDARY_NAICS_CODE'] = \
            ghgrp_matching.loc[snaics_na, 'PRIMARY_NAICS_CODE']

        ghgrp_matching.COUNTY_FIPS.fillna(0, inplace=True)

        for c in ['SECONDARY_NAICS_CODE', 'PRIMARY_NAICS_CODE', 'COUNTY_FIPS']:

            ghgrp_matching[c] = ghgrp_matching[c].astype('int')

        ghgrp_matching['INDUSTRY'] = ghgrp_matching.PRIMARY_NAICS_CODE.apply(
            lambda x: (int(str(x)[0:2]) in [11, 21, 23, 31, 32, 33])
            ) | (ghgrp_matching.SECONDARY_NAICS_CODE.apply(
                    lambda x: (int(str(x)[0:2]) in [11, 21, 23, 31, 32, 33])
                    ))

        ghgrp_matching = pd.DataFrame(
                ghgrp_matching[ghgrp_matching.INDUSTRY == True]
                )

        ghgrp_matching.drop_duplicates(['FACILITY_ID'], inplace=True)

        #Update NAICS (2007) to 2012 NAICS if year > 2011
        if self.year > 2011:

            naics07_12 = pd.read_csv(
                os.path.join('../', self.data_dir+'/2007_to_2012_NAICS.csv')
                )

            for c in ['2007 NAICS Code', '2012 NAICS Code']:

                naics07_12[c] = naics07_12[c].astype('int32')

            naics07_12.drop_duplicates(['2007 NAICS Code'], keep='last',
                                       inplace=True)

            naics07_12.set_index('2007 NAICS Code', inplace=True)

            ghgrp_matching = \
                ghgrp_matching.set_index('PRIMARY_NAICS_CODE').join(
                        naics07_12['2012 NAICS Code']
                        )

            ghgrp_matching.rename(
                    columns={'2012 NAICS Code': 'PRIMARY_NAICS_CODE_12'},
                    inplace=True
                    )

            ghgrp_matching.index.name = 'PRIMARY_NAICS_CODE'

            ghgrp_matching.reset_index(inplace=True)

            ghgrp_matching = \
                ghgrp_matching.set_index('SECONDARY_NAICS_CODE').join(
                        naics07_12['2012 NAICS Code']
                        )

            ghgrp_matching.rename(
                    columns={'2012 NAICS Code': 'SECONDARY_NAICS_CODE_12'},
                    inplace=True
                    )

            ghgrp_matching.index.name = 'SECONDARY_NAICS_CODE'

            ghgrp_matching.reset_index(inplace=True)
#            naics07_12 = pd.DataFrame(
#                naics07_12[naics07_12['2007 NAICS Code'].isin(
#                    [x for x in zip(*ghgrp_matching[
#                        ['PRIMARY_NAICS_CODE','SECONDARY_NAICS_CODE']
#                        ].values)][0]
#                            )]
#                    )

            ghgrp_matching.set_index('FACILITY_ID', inplace=True)

            ghgrp_matching.loc[1003006, 'PRIMARY_NAICS_CODE'] = 424710

            ghgrp_matching.loc[1004861, 'PRIMARY_NAICS_CODE'] = 325193

            ghgrp_matching.loc[1005954, 'PRIMARY_NAICS_CODE'] = 311211

            ghgrp_matching.loc[1004098, 'PRIMARY_NAICS_CODE'] = 322121

            ghgrp_matching.loc[1005445, 'PRIMARY_NAICS_CODE'] = 331524

            ghgrp_matching.reset_index(inplace=True)

            for c in ['PRIMARY_NAICS_CODE', 'SECONDARY_NAICS_CODE']:

                naics_column = c + '_12'

#                ghgrp_matching[naics_column] = ghgrp_matching[c].map(
#                        naics07_12['2012 NAICS Code'].to_dict()
#                        )

                ghgrp_matching[naics_column].fillna(0, inplace=True)

                ghgrp_matching[naics_column] = \
                    ghgrp_matching[naics_column].astype(int)

#                ghgrp_matching['FIP_' + c[0] + 'N_12'] = [
#                    i for i in zip(ghgrp_matching['COUNTY_FIPS'],
#                                   ghgrp_matching[naics_column])
#                    ]
#
        else:

            ghgrp_matching.set_index('FACILITY_ID', inplace=True)

            ghgrp_matching.loc[1003006, 'PRIMARY_NAICS_CODE'] = 424710

            ghgrp_matching.loc[1004861, 'PRIMARY_NAICS_CODE'] = 325193

            ghgrp_matching.loc[1005954, 'PRIMARY_NAICS_CODE'] = 311211

            ghgrp_matching.loc[1004098, 'PRIMARY_NAICS_CODE'] = 322121

            ghgrp_matching.loc[1005445, 'PRIMARY_NAICS_CODE'] = 331524

            ghgrp_matching.reset_index(inplace=True)
#
#            for c in ['PRIMARY_NAICS_CODE', 'SECONDARY_NAICS_CODE']:
#
#                ghgrp_matching['FIP_' + c[0] +'N'] = \
#                    [i for i in zip(ghgrp_matching.loc[:,'COUNTY_FIPS'],
#                                    ghgrp_matching.loc[:, c])]

    #Find updated GHGRP NAICS that aren't in the CBP.
    #Results show that 111419 Other Food Crops Grown Under Cover is not listed
    #in the CBP. There are only two GHGRP facilities that report under this
    #NAICS. It is not clear what the alternative NAICS should be based on the
    #NAICS values reported for the counties the GHGRP facilities are located in.

        if self.year > 2011:

            ghgrp_matching['pn12_in_cbp'] = pd.Series(
                    [i for i in zip(ghgrp_matching['COUNTY_FIPS'],
                                    ghgrp_matching['PRIMARY_NAICS_CODE_12'])]
                    ).isin(cbp_matching.fips_n)

            ghgrp_matching['sn12_in_cbp'] = pd.Series(
                    [i for i in zip(ghgrp_matching['COUNTY_FIPS'],
                                    ghgrp_matching['SECONDARY_NAICS_CODE_12'])]
                    ).isin(cbp_matching.fips_n)

#            ghgrp_matching['pn12_in_cbp']  = \
#                ghgrp_matching.PRIMARY_NAICS_CODE_12.isin(cbp_matching.naics)
#
#            ghgrp_matching['sn12_in_cbp']  = \
#                ghgrp_matching.SECONDARY_NAICS_CODE_12.isin(cbp_matching.naics)

            ghgrp_missing = pd.DataFrame(
                ghgrp_matching[(ghgrp_matching.pn12_in_cbp == False) &
                               (ghgrp_matching.sn12_in_cbp == False)][[
                                       'PRIMARY_NAICS_CODE_12',
                                       'SECONDARY_NAICS_CODE_12',
                                       'COUNTY','COUNTY_FIPS','FACILITY_NAME',
                                       'FACILITY_ID'
                                        ]]
                )

        else:

            ghgrp_matching['pn_in_cbp']  = \
                ghgrp_matching.PRIMARY_NAICS_CODE.isin(cbp_matching.naics)

            ghgrp_matching['sn_in_cbp']  = \
                ghgrp_matching.SECONDARY_NAICS_CODE.isin(cbp_matching.naics)

            ghgrp_missing = pd.DataFrame(
                ghgrp_matching[(ghgrp_matching.pn_in_cbp == False) &
                               (ghgrp_matching.sn_in_cbp == False)][[
                                       'PRIMARY_NAICS_CODE',
                                       'SECONDARY_NAICS_CODE', 'COUNTY_FIPS',
                                       'COUNTY', 'FACILITY_NAME','FACILITY_ID'
                                       ]]
                )

        ghgrp_manual = pd.merge(
                cbp_matching[['COUNTY_FIPS', 'naics']],
                                ghgrp_missing, on='COUNTY_FIPS',
                                how='inner'
                )

        # Export facilities for manual NAICS mapping with CBP
        ghgrp_manual['naics_4n'] = ghgrp_manual.naics.apply(
                lambda x: int(str(x)[0:4])
                )

        if self.year > 2011:

            ghgrp_manual['pn_4n'] = ghgrp_manual.PRIMARY_NAICS_CODE_12.apply(
                lambda x: int(str(x)[0:4])
                )

            ghgrp_manual['sn_4n'] = ghgrp_manual.SECONDARY_NAICS_CODE_12.apply(
                lambda x: int(str(x)[0:4])
                )

        else:

            ghgrp_manual['pn_4n'] = ghgrp_manual.PRIMARY_NAICS_CODE.apply(
                lambda x: int(str(x)[0:4])
                )

            ghgrp_manual['sn_4n'] = ghgrp_manual.SECONDARY_NAICS_CODE.apply(
                lambda x: int(str(x)[0:4])
                )

        ghgrp_manual = ghgrp_manual.where(
                (ghgrp_manual.naics_4n == ghgrp_manual.pn_4n) |
                (ghgrp_manual.naics_4n == ghgrp_manual.sn_4n)
                ).dropna()

        ghgrp_manual.to_csv(os.path.join(
                '../', self.data_dir + \
                '/facilities_for_manual_cbp-ghgrp_matching.csv'
                ))

        #Import csv that has manual matching for unmatched facilities
        ghgrp_manual_matched = pd.read_csv(os.path.join(
                '../', self.data_dir+'/manual_fac_matching_corrected.csv'
                ))

        ghgrp_manual_matched.set_index('FACILITY_ID', inplace=True)


        #Select NAICS that corresponds to CBP data.
        if self.year > 2011:

            ghgrp_matching['NAICS_MATCHED'] = np.nan

            ghgrp_matching['NAICS_MATCHED'].update(
                    ghgrp_matching.where(
                            ghgrp_matching.pn12_in_cbp == True
                            ).dropna(
                                    subset=['pn12_in_cbp']
                                    ).PRIMARY_NAICS_CODE_12.astype(int)
                    )

            ghgrp_matching['NAICS_MATCHED'].update(
                    ghgrp_matching.where(
                            (ghgrp_matching.pn12_in_cbp == False) &
                            (ghgrp_matching.sn12_in_cbp == True)).dropna(
                                    subset=['sn12_in_cbp']
                                    ).SECONDARY_NAICS_CODE_12.astype(int)
                    )

            ghgrp_matching.set_index('FACILITY_ID', inplace=True)

            ghgrp_matching['NAICS_MATCHED'].update(ghgrp_manual_matched.naics)

            ghgrp_matching.reset_index(inplace=True)

            ghgrp_matching['FIPS_NAICS_MATCHED'] = [
                    i for i in zip(ghgrp_matching['COUNTY_FIPS'],
                                   ghgrp_matching['NAICS_MATCHED'])
                    ]

#            ghgrp_matching.loc[
#                ghgrp_matching[ghgrp_matching.pn12_in_cbp == True].index,
#                'NAICS_USED'
#                ] = ghgrp_matching[ghgrp_matching.pn12_in_cbp == True][
#                        'PRIMARY_NAICS_CODE_12'
#                        ]

#            ghgrp_matching.loc[ghgrp_matching[
#                    (ghgrp_matching.pn12_in_cbp == False) &
#                    (ghgrp_matching.sn12_in_cbp == True)
#                    ].index, 'NAICS_USED'] = ghgrp_matching[
#                        (ghgrp_matching.pn12_in_cbp == False) &
#                        (ghgrp_matching.sn12_in_cbp == True)][
#                            'SECONDARY_NAICS_CODE_12'
#                            ]

        else:

            ghgrp_matching['NAICS_MATCHED'] = np.nan

            ghgrp_matching['NAICS_MATCHED'].update(
                    ghgrp_matching.where(
                            ghgrp_matching.pn_in_cbp == True
                            ).dropna(
                                    subset=['pn_in_cbp']
                                    ).PRIMARY_NAICS_CODE.astype(int)
                    )

            ghgrp_matching['NAICS_MATCHED'].update(
                    ghgrp_matching.where(
                            (ghgrp_matching.pn_in_cbp == False) &
                            (ghgrp_matching.sn_in_cbp == True)).dropna(
                                    subset=['sn_in_cbp']
                                    ).SECONDARY_NAICS_CODE.astype(int)
                    )

            ghgrp_matching.set_index('FACILITY_ID', inplace=True)

            ghgrp_matching['NAICS_MATCHED'].update(ghgrp_manual_matched.naics)

            ghgrp_matching.reset_index(inplace=True)

            ghgrp_matching['FIPS_NAICS_MATCHED'] = \
                [i for i in zip(ghgrp_matching.loc[:,'COUNTY_FIPS'],
                                ghgrp_matching.loc[:, 'NAICS_MATCHED'])]

        # Check if any remaining facilities are unmatched.
        ghgrp_missing = pd.DataFrame(
                ghgrp_matching[(~ghgrp_matching.FIPS_NAICS_MATCHED.isin(
                        cbp_matching.fips_n
                        )) & (ghgrp_matching.NAICS_MATCHED.between(
                                300000, 400000
                                ))]
                ).drop_duplicates(subset='FIPS_NAICS_MATCHED')

#        if len(ghgrp_missing) > 3:
#
#            print("%d unmatched GHGRP facilities remain" % len(
#                    ghgrp_missing
#                    ))
#
#            return


#            ghgrp_matching.loc[
#                ghgrp_matching[ghgrp_matching.pn_in_cbp == True].index,
#                    'NAICS_USED'
#                ] = ghgrp_matching[ghgrp_matching.pn_in_cbp == True][
#                    'PRIMARY_NAICS_CODE'
#                    ]
#
#            ghgrp_matching.loc[ghgrp_matching[(
#                ghgrp_matching.pn_in_cbp == False
#                    ) & (ghgrp_matching.sn_in_cbp == True)].index, 'NAICS_USED'
#                ] = ghgrp_matching[(
#                    ghgrp_matching.pn_in_cbp == False
#                ) & (ghgrp_matching.sn_in_cbp == True)]['SECONDARY_NAICS_CODE']

        return ghgrp_matching

    def ghgrp_counts(self, cbp_matching, ghgrp_matching):
        """
        Method for adding GHGRP facility counts to formatted CBP data.
        Identify which NAICS codes in the Census data are covered in MECS
        Begin by importing MECS data. Note that CBP data after 2011 use 2012
        NAICS. MECS data are based on 2007 NAICS. Most significant difference is
        aggregation of Alkalies and Chlorine Manufacturing,
        Carbon Black Manufacturing, and
        All other Basic Inorganic Chemical Manufacturing into a single
        NAICS code.
        """

        cbp_matching_counts = cbp_matching.copy(deep=True)

        cbp_matching_counts['in_ghgrp'] = \
            cbp_matching_counts.fips_matching.isin(ghgrp_matching.COUNTY_FIPS)

        def facility_counts(df, c):
            """
            Counts how many GHGRP-reporting facilities by NAICS are located
            in a county
            """
            counts = pd.DataFrame(
                    df.groupby(c, as_index=False)['COUNTY_FIPS'].count()
                    )

            counts.columns = [c, 'ghgrp_fac']

            return counts

        #Create dictionaries of ghgrp facility counts based on NAICS updated
        #to 2012 values.

        ghgrpcounts = facility_counts(ghgrp_matching,'FIPS_NAICS_MATCHED')

        cbp_matching_counts = pd.merge(
                    cbp_matching_counts, ghgrpcounts, left_on='fips_n',
                    right_on='FIPS_NAICS_MATCHED', how='left')

#        if self.year > 2011:
#
#            ghgrpcounts_FIPPN_dict = facility_counts(ghgrp_matching,
#                'FIP_PN_12')['FAC_COUNT'].to_dict()
#
#            ghgrpcounts_FIPSN_dict = facility_counts(ghgrp_matching,
#                'FIP_SN_12')['FAC_COUNT'].to_dict()
#
#        else:
#
#            ghgrpcounts_FIPPN_dict = facility_counts(ghgrp_matching,
#                'FIP_PN')['FAC_COUNT'].to_dict()
#
#            ghgrpcounts_FIPSN_dict = facility_counts(ghgrp_matching,
#                'FIP_SN')['FAC_COUNT'].to_dict()

        #Map GHGRP facilities count to Census data
#        cbp_matching['ghgrp_pn'] = [fn in ghgrpcounts_FIPPN_dict.keys()
#            for fn in cbp_matching.fips_n.tolist()
#            ]
#
#        cbp_matching['ghgrp_sn'] = [fn in ghgrpcounts_FIPSN_dict.keys()
#            for fn in cbp_matching.fips_n.tolist()
#            ]
#
#        cbp_matching['ghgrp_fac'] = 0

        cbp_matching_counts['est_small'] = cbp_matching_counts.loc[
            :, ('n1_4'): ('n20_49')].sum(axis = 1)

        cbp_matching_counts['est_large'] = cbp_matching_counts.loc[
            :, ('n50_99'): ('n1000')].sum(axis = 1)

#        cbp_matching['n_in_mecs'] = \
#            cbp_matching.naics.isin(self.mecs_naics.MECS_NAICS)

        def MatchMECS_NAICS(DF, naics_column):
            """
            Method for matching 6-digit NAICS codes with adjusted MECS NAICS.
            """

            DF[naics_column].fillna(0, inplace=True)

            if DF[naics_column].dtype != int:

                DF[naics_column] = DF[naics_column].astype(int)

            DF_index = DF[DF[naics_column]>0].index

            nctest = [
                DF.loc[DF_index, naics_column].apply(lambda x: int(str(x)[
                    0:len(str(x))- i
                    ])) for i in range(0,4)
                ]

            nctest = pd.concat(nctest, axis = 1)

            nctest.columns = ['N6', 'N5', 'N4', 'N3']

            #Pare down to manufacturing NAICS only (311 - 339)
            nctest = pd.DataFrame(nctest[nctest.N3.between(311, 339)])

            #Match GHGRP NAICS to highest-level MECS NAICS. Will match to
            #"dummy-09" NAICS where available. This is messy, but functional.
            ncmatch = pd.concat(
                [pd.merge(nctest, self.mecs_naics, left_on=column,
                          right_on=self.mecs_naics.MECS_NAICS,
                          how='left').iloc[:, -1]
                            for column in nctest.columns], axis =1
                )

            ncmatch.columns = nctest.columns

            ncmatch.index = nctest.index

            ncmatch['NAICS_MATCH'] = np.nan

            for n in range(3, 7):

                column = 'N'+str(n)

                ncmatch.NAICS_MATCH.update(ncmatch[column].dropna())

#            ncmatch['NAICS_MATCH'] = ncmatch.apply(
#                lambda x: int(list(x.dropna())[0]), axis = 1
#                )

            #Update GHGRP dataframe with matched MECS NAICS.
            DF['MECS_NAICS'] = 0

            DF.MECS_NAICS.update(ncmatch.NAICS_MATCH)

            return DF

        #Match Census NAICS to NAICS available in MECS. Note that MECS does not
        #include agriculture, mining, and construction industries and does not
        #include6-digit detail for all manufacturing NAICS (31 - 33)
        cbp_matching_counts = MatchMECS_NAICS(cbp_matching_counts, 'naics')

        cbp_matching_counts  = pd.merge(
                cbp_matching_counts,
                self.mecs_naics[['MECS_NAICS', 'MECS_NAICS_dummies']],
                on='MECS_NAICS', how='left'
                )

        cbp_matching_counts.set_index('fipstate', inplace=True)

        print(cbp_matching_counts.columns)

        cbp_matching_counts = cbp_matching_counts.join(
                self.MECS_regions.drop_duplicates(
                        ['FIPS State']
                        ).set_index('FIPS State')['MECS_Region']
                )

        cbp_matching_counts.reset_index(inplace=True, drop=False)

        cbp_matching_counts.rename(
            columns={'index': 'fipstate'}, inplace=True
            )

        # Change cement facilities from in_ghgrp == True to in_ghgrp == False
        # to account for the data availablility issues in Envirofacts for
        # cement facilities reporting to GHGRP.
        cement_facs = cbp_matching_counts[
            cbp_matching_counts.naics==327310
            ].copy(deep=False)

        cement_facs.loc[:, 'in_ghgrp'] = False

        cement_facs.loc[:, 'ghgrp_fac'] = 0

        cbp_matching_counts.update(cement_facs)

        return cbp_matching_counts


#    def flag_counties(self, cbp_matching, ghgrp_matching):
#        """
#        Identifies counties where the GHGRP or CBP NAICS designation is
#        potentially incorrect.
#        Outputs to working drive csv ("flagged_county_list") of flagged
#        counties.
#        """
#        count_flagged_cbp = pd.DataFrame(
#            cbp_matching[cbp_matching.in_ghgrp == True], copy = True
#            )
#
#        count_flagged_cbp['N2'] = count_flagged_cbp.naics.apply(
#            lambda x: int(str(x)[0:2]))
#
#        count_flagged_cbp = count_flagged_cbp[
#            count_flagged_cbp.N2 != 23
#            ]
#
#        count_flagged_cbp.drop('N2', axis = 1, inplace = True)
#
#        count_flagged_cbp = count_flagged_cbp.groupby(
#            ['fips_matching', 'naics']
#            )['ghgrp_fac'].sum()
#
#        if self.year > 2011:
#
#            ghgrp_count = ghgrp_matching.groupby([
#                'COUNTY_FIPS', 'PRIMARY_NAICS_CODE_12'])['FACILITY_ID'].count()
#
#        else:
#            ghgrp_count = ghgrp_matching.groupby([
#                'COUNTY_FIPS', 'PRIMARY_NAICS_CODE'])['FACILITY_ID'].count()
#
#        fac_count_compare = pd.concat(
#            [count_flagged_cbp, ghgrp_count], axis = 1
#            )
#
#        flagged_list = pd.DataFrame(
#            fac_count_compare[fac_count_compare.ghgrp_fac.isnull() == True], \
#                copy = True
#            )
#
#        flagged_list.to_csv(
#            os.path.join('../', self.data_dir + '/flagged_county_list.csv')
#            )
#
#        return flagged_list

    # ghgrp_for_matching['flagged'] = [i in fac_count_compare[(
    #   fac_count_compare.ghgrp_fac != fac_count_compare.FACILITY_ID
    #   )].index for i in ghgrp_for_matching.COUNTY_FIPS]

    # #Export flagged facilities for manual NAICS correction
    # ghgrp_for_matching[ghgrp_for_matching.flagged == True].iloc[:,0:5].to_csv(
    #   'ghgrp_facilities_county-flagged.csv'
    #   )

    @staticmethod
    def correct_cbp(cbp_matching_counts):
        """
        Method for correcting CBP facility counts based on GHGRP facilities.
        """

        def fac_correct(df, index):
            """
            Removes the largest facilities in a given county with matching GHGRP
            facilities.
            """

            large = ['n50_99', 'n100_249', 'n250_499', 'n500_999', 'n1000']

            small = ['n1_4', 'n5_9', 'n10_19', 'n20_49']

            ghgrp_fac_count = df.loc[index, 'ghgrp_fac']

#            fac_count_large = df.loc[index, 'est_large']
#
#            fac_count_small = df.loc[index, 'est_small']

            fac_count_total = df.loc[index, 'est']

            # The iteration by dataframe index starting at line 547 is
            # slow and an alternative approach should be tested.
#            cbp_ghgrp = pd.DataFrame(
#                    cbp_matching[cbp_matching.in_ghgrp == True]
#                    )
#
#            cbp_ghgrp['ghgrp_lessthan_cbp'] = \
#                cbp_ghgrp.ghgrp_fac <= cbp_ghgrp.est
#
#            cbp_ghgrp['final_fac_count'] = 0
#
#            cbp_ghgrp['final_fac_count'].update(
#                    cbp_ghgrp[cbp_ghgrp.ghgrp_lessthan_cbp == True].ghgrp_fac
#                    )
#
#            cbp_ghgrp['final_fac_count'].update(
#                    cbp_ghgrp[cbp_ghgrp.ghgrp_lessthan_cbp == False].est
#                    )

            if ghgrp_fac_count <= fac_count_total:

                n = ghgrp_fac_count

            else:

                n = fac_count_total

            while n > 0:

                maxsize = [c for c in itools.compress(small + large, df.loc[
                        index, ('n1_4'):('n1000')
                    ].values)][-1]

                df.loc[index, maxsize] = df.loc[index, maxsize] - 1

                n = n - 1

            df.loc[index, 'est_large_corrected'] = df.loc[
                index, ('n50_99'):('n1000')
                ].sum()

            df.loc[index, 'est_small_corrected'] = df.loc[
                index, ('n1_4'):('n20_49')
                ].sum()


        #Apply method for removing GHGRP facilities from the counts of the
        #largest CBP facilities. 'cbp_matching' contains the original
        #CBP facility counts.
        cbp_ghgrp = \
            cbp_matching_counts[cbp_matching_counts.in_ghgrp == True].copy(
                    deep=True
                    )

#        cbp_ghgrp = pd.DataFrame(
#            cbp_matching[cbp_matching.in_ghgrp == True]
#            )

        for i in cbp_ghgrp[cbp_ghgrp.ghgrp_fac > 0].index:

            cbp_ghgrp.update(fac_correct(cbp_ghgrp, i))

        cbp_corrected = cbp_matching_counts.copy(deep=True)

        cbp_corrected['est_small_corrected'] = cbp_corrected.est_small

        cbp_corrected['est_large_corrected'] = cbp_corrected.est_large

        cbp_corrected.update(cbp_ghgrp)

#        # Change dtypes
#        for c in cbp_corrected.columns.difference(
#                ['empflag', 'emp_nf', 'qp1_nf', 'qp1', 'ap_nf', 'ap',
#                  'COUNTY_FIPS', 'region', 'industry', 'fips_n', 'in_ghgrp',
#                  'ghgrp_pn', 'ghgrp_sn', 'n_in_mecs', 'MECS_Region']
#                ):
#
#            cbp_corrected[c] = cbp_corrected[c].astype(int)

        return cbp_corrected


        #The following dataframe provides the original facility counts for
        #the matching counties provided in cbp_corrected.
        #Aggregate original and corrected CBP manufacturing facility counts
        #by MECS region.
#         cbp_original_byMECS = cbp_matching[
#             cbp_matching.MECS_NAICS != 0
#             ].groupby(['MECS_Region', 'MECS_NAICS'])
#
#         cbp_corrected_byMECS = cbp_corrected[
#             cbp_corrected.MECS_NAICS != 0
#             ].groupby(['MECS_Region', 'MECS_NAICS'])
