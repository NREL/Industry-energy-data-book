
# %%
import pandas as pd
import numpy as np
import os
import itertools as itools

class IPF_seed:
    """
    Create and format iterative proportional fitting (IPF) seed.
    """
    def __init__(self, year=2014):

        if year >= 2014:

            self.year = 2014

            file_extension = '.xlsx'

            skipfooter = 12

        if year < 2014:

            self.year = 2010

            file_extension = '.xls'

            skipfooter = 46

        self.filepath = os.path.join('../', 'calculation_data/')

        self.url_3_2 =\
            'https://www.eia.gov/consumption/manufacturing/data/'+\
            str(self.year) + '/xls/table3_2' + file_extension

        self.url_3_3 = \
            'https://www.eia.gov/consumption/manufacturing/data/'+\
            str(self.year) + '/xls/table3_3' + file_extension

        self.naics_group_dict = {
            2007: {311:[31131, 3112, 3114, 3115, 3116],
                   3112: [311221], 321: [321113, 3212, 3219], 312: [],
                   3212: [321219], 322: [322110, 322121, 322122, 322130],
                   324: [324110, 324121, 324199],
                   325: [325110, 325120, 325181, 325182, 325188, 325192,
                         325193, 325199, 325211, 325212, 325222, 325311,
                         325312, 325992], 3254: [325412],
                   327: [327121, 327211, 327212, 327213, 327215, 327310,
                         327410, 327420, 327993], 331: [],
                   3313: [331314, 331315, 331316], 3314: [331419],
                   3315: [331511, 331521, 331524], 334: [334413],
                   336: [336111, 336112, 3364], 3364: [336411]},
            2012: {311:[31131, 3112, 3114, 3115, 3116],
                   3112: [311221], 321: [321113, 3212, 3219], 312: [],
                   3212: [321219], 322: [322110, 322121, 322122, 322130],
                   324: [324110, 324121, 324122, 324199],
                   325: [325110, 325120, 325180, 325193, 325194, 325199,
                         325211, 325212, 325220, 325311, 325312, 325992, 3254],
                   3254: [325412],
                   327: [327120, 327211, 327212, 327213, 327215, 327310,
                         327410, 327420, 327993],331: [],
                   3313: [331314, 331315, 331318], 3314: [331410],
                   3315: [331511, 331523, 331524], 334: [334413],
                   336: [336111, 336112, 3364], 3364: [336411]}
            }

        def create_unformatted_tables():

            table3_2 = pd.read_excel(
                self.url_3_2, sheet='Table 3.2', index_col=None, skiprows=10,
                skipfooter=skipfooter
                )

            table3_3 = pd.read_excel(
                self.url_3_3, sheet='Table 3.3', index_col=None, skiprows=10,
                skipfooter=skipfooter
                )

            for df in [table3_2, table3_3]:

                df.dropna(axis=0, how='all', inplace=True)

                df.iloc[:, 0].fillna(method='ffill', inplace=True)

                df.reset_index(inplace=True, drop=True)

            #table3_2 = table3_2.fillna(axis=1, method='bfill')

            table3_3 = table3_3.fillna(axis=1, method='bfill')

            table3_2.columns = ['NAICS', 'NAICS_desc', 'Total',
                                'Net_electricity', 'Residual_fuel_oil',
                                'Diesel', 'Natural_gas', 'LPG_NGL',	'Coal',
                                'Coke_and_breeze', 'Other']

            table3_3.columns = ['Characteristic', 'Total',
                                'Net_electricity', 'Residual_fuel_oil',
                                'Diesel', 'Natural_gas', 'LPG_NGL', 'Coal',
                                'Coke_and_breeze', 'Other']

            # Replace nonreported values below 0.5 with value = 0.1. Some of
            # these are adjusted later in manual formatting.
            table3_2.replace(
                to_replace={'*': 0.1, '--': 0, 'Q': np.nan,'W': np.nan},
                value=None, inplace=True
                )

            table3_3.replace(
                to_replace={'*': 0.1, '--': 0, 'Q': np.nan, 'W': np.nan},
                value=None, inplace=True
                )

            for n in [0, 1]:

                table3_2.iloc[:, n] = \
                    table3_2.iloc[:, n].apply(lambda x: str(x).strip())

                if n == 0:

                    table3_3.iloc[:, n] = \
                        table3_3.iloc[:, n].apply(lambda x: str(x).strip())

            def get_regions(df, df_column):
                """
                Returns dataframe with region values.
                """

                df['Region'] = np.nan

                df['Region'].update(
                    df.loc[
                        df[df_column].apply(lambda x: type(x)) ==str, 'Total'
                        ].apply(lambda x: x.split(' Census Region')[0])
                        )

                df.Region.fillna(method='ffill', inplace=True)

                df.replace({'Total United States': 'United States'},
                           inplace=True)

                return df

            table3_2 = get_regions(table3_2, 'Total')

            table3_2.dropna(thresh=5, axis=0, inplace=True)

            table3_2['nans'] = \
                table3_2.apply(lambda x: x.isnull(), axis=0).sum(axis=1)

            for i in table3_2[table3_2.nans ==1].index:

                if table3_2.loc[i, 'Total'] == np.nan:

                    continue

                else:

                    na_fill = table3_2.loc[i, 'Total'] - \
                        table3_2.loc[i, ('Net_electricity'): ('Other')].sum()

                    table3_2.loc[i, :] = table3_2.loc[i,:].fillna(na_fill)

            table3_2.drop('nans', inplace=True, axis=1)

            # Create regions column for Table 3.3
            table3_3 = get_regions(table3_3, 'Total')

            table3_3['Data_cat'] = table3_3[table3_3.Characteristic.isin(
                    ['Value of Shipments and Receipts', 'Employment Size']
                    )].Characteristic

            table3_3.replace(
                {'Employment Size': 'Employment_size',
                 'Value of Shipments and Receipts': 'Value_of_shipments'},
                 inplace=True
                 )

            table3_3.Data_cat.fillna(method='ffill', inplace=True)

            table3_3.dropna(thresh=4, inplace=True)

            # Export for manual final formatting and filling in data.
            writer = pd.ExcelWriter(
                    self.filepath+'Tables3_'+str(self.year)+'_unformatted.xlsx'
                    )

            table3_2.to_excel(writer, index=False, sheet_name='Table3.2',
                              na_rep='NaN')

            table3_3.to_excel(writer, index=False, sheet_name='Table3.3',
                              na_rep='NaN')

            writer.save()

        # First check if "MECS2014_unformatted.xlsx" exists in filepath.
        # If not, proceed with creating it.
        # Need to make sure that automated formatting above does not
        # result in negative values for fuels other than net electricity.
        if 'Tables3_'+str(self.year)+'_formatted.xlsx' not in os.listdir(
                self.filepath
                ):

            create_unformatted_tables()

            print('Unformatted file created.','\n',
                  'Perform manual formatting for MECS and re-run')

            return

        # Re-import formatted sheets
        self.table3_2 = pd.read_excel(
                self.filepath + "Tables3_"+str(self.year)+"_formatted.xlsx",
                sheet_name='Table3.2'
                )

        self.table3_3 = pd.read_excel(
                self.filepath + "Tables3_"+str(self.year)+"_formatted.xlsx",
                sheet_name='Table3.3')

        self.table3_2.dropna(axis=0, thresh=4, inplace=True)

        self.table3_2 = self.table3_2[(self.table3_2.NAICS_desc != 'Total') &
                            (self.table3_2.Region != 'United States')]

        self.table3_3.dropna(axis=0, thresh=3, inplace=True)

        self.table3_3 = \
            self.table3_3[(self.table3_3.Characteristic != 'Total') &
                          (self.table3_3.Region != 'United States')]

        self.table3_3.rename(columns={'Characteristic': 'employment_class',
                                      'HGL': 'LPG_NGL'},
                             inplace=True)

        for df in [self.table3_2, self.table3_3]:

            df.rename(columns={'Region': 'region', 'NAICS': 'naics'},
                      inplace=True)

            df.drop(['Total'], axis=1, inplace=True)

        self.table3_3.fillna(0, inplace=True)


        # For IPF, need to remove NAICS group totals and create dummy NAICS
        # in Table 3.2 to capture energy use of NAICS codes not covered by
        # MECS.
        if self.year < 2012:

            naics_year = 2007

        else:

            naics_year = 2012

        self.table3_2.set_index('region', inplace=True)

        dummy_all = pd.DataFrame()

        for ng in self.naics_group_dict[naics_year].keys():

            # Skip 312, which is covered by survey results for 3121, and 3122
            if ng == 312:

                self.table3_2 = self.table3_2[self.table3_2.naics != ng]

                continue

            dummy_naics = int(str(ng) + (5-len(str(ng)))*str(0) + str(9))

            dummy_value = \
                self.table3_2[self.table3_2.naics == ng].loc[
                    :, ('Net_electricity'):('Other')].subtract(
                        self.table3_2[self.table3_2.naics.isin(
                                self.naics_group_dict[naics_year][ng]
                                )].loc[
                                    :, ('Net_electricity'):('Other')
                                    ].sum(level=0)
                        )

            dummy_value['naics'] = dummy_naics

            # Replace negative values with zero.
            dummy_value = dummy_value.where(dummy_value>0).fillna(0)

            dummy_value.fillna(0, inplace=True)

            dummy_all = dummy_all.append(dummy_value, sort=True)

            #Delete category total
            self.table3_2 = self.table3_2[self.table3_2.naics != ng]

        self.table3_2 = pd.DataFrame(self.table3_2).append(
                dummy_all, sort=True
                )

    def create_seed(self, cbp_matching):
        """
        Create IPF seed based on reformatted and adjusted MECS Table 3.2 and
        Table 3.3. Seed is adjusted based on CBP data and MECS fuel use
        from Table 3.2.
        """

        regions =  self.table3_3.region.unique()

        fuels = self.table3_2.columns.difference(['NAICS_desc', 'naics'])

        emply = self.table3_3[
                self.table3_3.Data_cat == 'Employment_size'
                ].employment_class.unique()

        seed_index = \
            ['_'.join(x) for x in itools.product(regions, fuels, emply)]

        seed_df = pd.DataFrame(1, columns=self.table3_2.naics.unique(),
                               index=seed_index)

        seed_df.reset_index(inplace=True)

        def ft_split(s):
            """
            Handles splitting off fuel types with more than one word.
            """
            split = s.split('_')

            ft = split[1]

            for n in range(2, len(split)-1):

                ft = ft + '_' + split[n]

            return ft

        seed_df.loc[:, 'region'] = seed_df.iloc[:, 0].apply(
            lambda x: x.split('_')[0]
            )

        seed_df.loc[:, 'Fuel_type'] = seed_df.iloc[:, 0].apply(
            lambda x: ft_split(x)
            )

        seed_df.loc[:, 'EMPSZES'] = seed_df.iloc[:, 0].apply(
            lambda x: x.split('_')[-1]
            )

        #Change seed values to zero based on CBP employment size count by
        #industry and region.
        seed_df.set_index(['region', 'EMPSZES'], inplace=True)

        # Reformat CBP data
        cbp_pivot = cbp_matching.copy(deep=True)

        cbp_pivot.rename(columns={"n50_99": "50-99", "n100_249": "100-249",
                                  "n250_499": "250-499", "n500_999": "500-999",
                                  "n1000": "1000 and Over"},
                         inplace=True)

        cbp_pivot = cbp_pivot.melt(
                id_vars=['region', 'MECS_NAICS_dummies'],
                value_vars=['Under 50', '50-99', '100-249',
                            '250-499', '500-999', '1000 and Over'],
                var_name='EMPSZES'
                )

        cbp_pivot = pd.pivot_table(cbp_pivot, index=['region', 'EMPSZES'],
                                   columns=['MECS_NAICS_dummies'], values=['value'],
                                   aggfunc='sum')

        cbp_pivot.columns = cbp_pivot.columns.droplevel()

        shared_cols = []

        for c in cbp_pivot.columns:

            if c in seed_df.columns:

                shared_cols.append(c)

        cbp_mask = cbp_pivot[shared_cols].reindex(seed_df.index).fillna(0)

        seed_df_cbp = seed_df.copy(deep=True)

        seed_df_cbp.update(
                seed_df_cbp[shared_cols].where(cbp_mask != 0, False)
                )

        #Change seed values to zero based on MECS fuel use by industry and
        #region.
        table3_2_mask = \
            self.table3_2.drop('NAICS_desc', axis=1).reset_index().melt(
                id_vars=['region', 'naics'], var_name=['Fuel_type']
                )

        table3_2_mask = pd.pivot_table(
                table3_2_mask, index=['region', 'Fuel_type'],
                columns='naics', values='value', aggfunc='sum'
                )

#        table3_2_mask.columns = table3_2_mask.columns.droplevel()

        seed_df_cbp.reset_index(drop=False, inplace=True)

        seed_df_cbp.set_index(['region','Fuel_type'], inplace=True)

        seed_df_cbp_t32 = seed_df_cbp.copy(deep=True)

        table3_2_mask = table3_2_mask.reindex(
                seed_df_cbp_t32.index, copy=False
                )

        seed_df_cbp_t32.update(
                seed_df_cbp_t32[shared_cols].multiply(table3_2_mask)
                )

#        seed_df_cbp_t32.update(
#                seed_df_cbp_t32[shared_cols].where(table3_2_mask != 0, 0)
#                )

        seed_df.reset_index(inplace=True)

        seed_df.set_index(['region', 'Fuel_type'], inplace=True)

        seed_df_cbp_t32.update(seed_df_cbp_t32[shared_cols].multiply(
                seed_df[shared_cols]
                ))

        seed_df_cbp_t32.reset_index(drop=False, inplace=True)

        seed_df_cbp_t32.update(
                seed_df_cbp_t32[shared_cols].where(
                        seed_df_cbp_t32[shared_cols] == 0, 1
                        )
                )

        seed_df_cbp_t32.drop(['EMPSZES', 'Fuel_type'], axis=1, inplace=True)

        seed_df_cbp_t32[shared_cols] = \
            seed_df_cbp_t32[shared_cols].astype(int)

        return seed_df_cbp_t32
