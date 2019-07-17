import requests
import pandas as pd
import io
import os
import zipfile
import re
#%%
class CBP:

    def __init__(self, year):
        
        self.data_dir = 'data_foundation/data for calculations/'
        
        self.year = year

        def naics_table(year):
            """
            Get the relevant NAICS values for a CBP dataset.
            Check if NAICS codes have been downloaded to disk. Download
            from Census API if not.
            """
            
            base_html = 'https://api.census.gov/data/'
            
            if year < 2012:
                
                naics_file = 'naics_2007.csv'
                
                html = \
                    base_html + '2011/cbp?get=NAICS2007,NAICS2007_TTL&for=us'
                    
            if year >= 2012:
                
                naics_file = 'naics_2012.csv'
                
                html = 'https://api.census.gov/data/2012/cbp/variables.json'
                    
            if naics_file in os.listdir(os.path.join('../', self.data_dir)):
                
                naics_df = pd.read_csv(
                        os.path.join('../', self.data_dir+naics_file)
                        )
    
            else:
                
                r = requests.get(html)
                
                if year >=2012:
                    
                    naics_df = pd.DataFrame.from_dict(
                        r.json()['variables']['NAICS2012']['values']['item'],
                        orient='index'
                        )
                    
                    naics_df = naics_df[2:]
                    
                    naics_df.reset_index(inplace=True)
                    
                    naics_df.columns=['naics', 'desc']
                
                else:
                    
                    naics_df = pd.DataFrame(r.json()[2:],
                                            columns=['naics', 'desc', 'us'])
                    
                    naics_df.drop(['us'], axis=1, inplace=True)
                
                naics_df = pd.DataFrame(
                        naics_df[(naics_df.naics != '31-33') & 
                                 (naics_df.naics != '44-45') & 
                                 (naics_df.naics != '48-49')])
        
                naics_df['n_naics'] = naics_df.naics.apply(
                        lambda x: len(x)
                        )
                
                naics_df['naics'] = naics_df.naics.astype('int')
                
                naics_df = pd.DataFrame(
                        naics_df[naics_df.naics.between(1, 400000)]
                        )
                
                naics_df.to_csv(
                    os.path.join('../', self.data_dir + naics_file)
                    )
                    
            return naics_df

        self.naics_df = naics_table(self.year)
        
        self.naics_cbp = {}

        cbp_file = 'cbp' + str(self.year)[2:] + 'co'

        cbp_csv_url = \
            'https://www2.census.gov/programs-surveys/cbp/' + \
            'datasets/' + str(self.year) + '/' + cbp_file + '.zip'

        # first check if file exists
        if cbp_file + '.txt' not in os.listdir(
                os.path.join('../', self.data_dir)
                ):

            zip_cbp =  zipfile.ZipFile(
                io.BytesIO(
                    requests.get(cbp_csv_url).content
                    )
                )

            zip_cbp.extractall(os.path.join('../', self.data_dir))

        cbp = pd.read_csv(
                os.path.join('../', self.data_dir + '/' + cbp_file + '.txt')
                )

        # NAICS codes are a strings that include - and / characters
        def fix_naics(naics):
            """
            Regex to retain only digits in naics strings. Returns
            integer.
            """
            if re.match("\d+", naics) is None:
                naics = 0

            else:
                naics = int(re.findall("\d+", naics)[0])

            return naics

        cbp.naics = cbp.naics.apply(fix_naics)

        # Create concatentated FIPS field to match GHGRP COUNTY_FIPS
        def state_fips_str(x):

            if len(str(x)) == 1:

                fips = '0' + str(x)

            else:
                fips = str(x)

            return fips

        def county_fips_str(x):

            int_len = len(str(x))

            mult = 3 - int_len

            fips = '0' * mult + str(x)

            return fips

        cbp['COUNTY_FIPS'] = \
            cbp.fipstate.apply(state_fips_str) + \
                cbp.fipscty.apply(county_fips_str)
                
        cbp['COUNTY_FIPS'] = cbp.COUNTY_FIPS.astype(int)

        census_regions = pd.read_csv(
                os.path.join('../', self.data_dir + '/US_FIPS_Codes.csv'),
                index_col=['COUNTY_FIPS'])

        cbp['region'] = cbp.fipstate.map(
            dict(census_regions[
                ['FIPS State', 'MECS_Region']
                ].values)
            )

        cbp.reset_index(drop=True, inplace=True)

        # Create employment size categories that match MECS
        cbp.loc[:, 'Under 50'] = cbp[
            ['n1_4', 'n5_9', 'n10_19', 'n20_49']
            ].sum(axis=1)

        self.cbp = cbp
        
        # Remaining lines of code further format cbp data into cbp_matching
        # for comparison against GHGRP facilities.
        self.cbp['naics_n'] = self.cbp.naics.apply(lambda x: len(str(x)))

        self.cbp['industry'] = \
            self.cbp.loc[self.cbp[self.cbp.naics != 0].index, 'naics'].apply(
                    lambda x: int(str(x)[0:2]) in [11, 21, 23, 31, 32, 33]
                    )

        self.cbp_matching = pd.DataFrame(
                self.cbp[(self.cbp.industry == True) &
                         (self.cbp.naics_n == 6)])

        self.cbp_matching['fips_matching'] = self.cbp_matching['COUNTY_FIPS']
#            self.cbp_matching.fipstate.astype(str) + \
#                self.cbp_matching.fipscty.astype(str)

        self.cbp_matching['fips_matching'] = \
            self.cbp_matching.fips_matching.astype(int)

        #Correct instances where CBP NAICS are wrong
        #Hancock County, WV has a large electroplaing and rolling facility
        #that shouldn't be classified as 331110/331111 
        if self.year >= 2012:

            self.cbp_matching.drop(
                self.cbp_matching[(self.cbp_matching.fips_matching == 54029) &
                             (self.cbp_matching.naics == 331110)].index,
                inplace=True
                )

        else:

            self.cbp_matching.drop(
                self.cbp_matching[(self.cbp_matching.fips_matching == 54029) &
                             (self.cbp_matching.naics == 331111)].index,
                inplace=True
                )

        #Create n1-49 column to match MECS reporting.
        self.cbp_matching['n1_49'] = self.cbp_matching['Under 50']

        self.cbp_matching['fips_n'] = [
            i for i in zip(self.cbp_matching.loc[:, 'fips_matching'], \
                self.cbp_matching.loc[:,'naics'])
            ]

        #Remove state-wide "999" county FIPS
        self.cbp_matching = pd.DataFrame(
            self.cbp_matching[self.cbp_matching.fipscty != 999]
            )
        
        self.cbp_matching.reset_index(drop=True, inplace=True)


# It'd be nice to use the Census API, but querying all counties for a given
# state results in an error about exceeding cell limit.
# This method loops through all states and their counties, but it takes
# too long to complete.
#API_auth_path = "U:/API_auth.json"
#
#with open(API_auth_path, 'r') as f:
#        auth_file = json.load(f)
#
#api_key = auth_file['census_API']

#naics2012_soup = BeautifulSoup(requests.get(
#    'https://api.census.gov/data/2014/cbp/variables/NAICS2012/values.html'
#    ).content, "lxml")
# def get_cbp(year, api_key):
#     year= str(2014)
#
#     fips = pd.read_excel(
#         'https://www2.census.gov/programs-surveys/popest/geographies/' +
#         year + '/all-geocodes-v' + year + '.xls', header=None
#         )
#
#     fips.dropna(inplace=True)
#
#     fips = pd.DataFrame(fips.values, columns=fips.iloc[0, :]).iloc[1:, :]
#
#     # Methods for converting FIPS values back to text
#     # Can't get dtype or converter options in pd.read_excel to preserve
#     # the original spreadsheet values in the resulting DataFrame.
#     def state_fips_str(x):
#         if len(str(x)) == 1:
#             fips = '0' + str(x)
#
#         else:
#             fips = str(x)
#
#         return fips
#
#     def county_fips_str(x):
#
#         int_len = len(str(x))
#
#         mult = 3 - int_len
#
#         fips = '0' * mult + str(x)
#
#         return fips
#
#     fips.loc[:, 'State Code (FIPS)'] = fips['State Code (FIPS)'].apply(
#         lambda x: state_fips_str(x)
#         )
#
#     fips.loc[:, 'County Code (FIPS)'] = fips['County Code (FIPS)'].apply(
#         lambda x: county_fips_str(x)
#         )
#
#     fips.loc[:, 'state_county'] = fips['State Code (FIPS)'].add(
#         fips['County Code (FIPS)']
#         )
#
#     fips.drop_duplicates(subset=['state_county'], inplace=True)
#
#     fips.to_csv('C:\\Users\\cmcmilla\\desktop\\test.csv')
#
#     fips_dict = {}
#
#     for i in fips['State Code (FIPS)'].unique():
#
#         if type(
#             fips.set_index('State Code (FIPS)')['County Code (FIPS)'].loc[i]
#             ) == np.str:
#
#             pass
#
#         else:
#
#             fips_dict[i] = [x for x in fips.set_index('State Code (FIPS)')[
#                 'County Code (FIPS)'
#                 ].loc[i].values]
#
#     cbp_list = []
#
#     for k, v in fips_dict.items():
#
#         for l in v:
#
#             if l == '000':
#
#                 continue
#
#             else:
#                 cbp_api = \
#                     'https://api.census.gov/data/' + year + '/cbp?get=' + \
#                     'GEO_ID,EMPSZES,NAICS2012,LFO_TTL,GEO_TTL' + \
#                     '&for=county:' + l + '&in=state:' + k + '&key=' + api_key
#
#                 cbp_list.append(requests.get(cbp_api).json())
#
#             print(cbp_api)
# def import_seed(file_path):
#     """
#     Imports and formats seed for manufacturing IPF from specified path.
#     """
#
#     def ft_split(s):
#         """
#         Handles splitting off fuel types with more than one word.
#         """
#         split = s.split('_')
#
#         ft = split[1]
#
#         for n in range(2, len(split)-1):
#
#             ft = ft + '_' + split[n]
#
#         return ft
#
#     seed_df = pd.read_csv(file_path, index_col=None)
#
#     seed_df = seed_df.replace({0:1})
#
#     seed_cols = [seed_df.columns[0]]
#
#     for c in seed_df.columns[1:]:
#         seed_cols.append(int(c))
#
#     seed_df.columns = seed_cols
#
#     seed_df.loc[:, 'region'] = seed_df.iloc[:, 0].apply(
#         lambda x: x.split('_')[0]
#         )
#
#     seed_df.loc[:, 'Fuel_type'] = seed_df.iloc[:, 0].apply(
#         lambda x: ft_split(x)
#         )
#
#     seed_df.loc[:, 'EMPSZES'] = seed_df.iloc[:, 0].apply(
#         lambda x: x.split('_')[-1]
#         )
#
#     return seed_df
#
# # %%
# def seed_correct_cbp(seed_df, cbp):
#     """
#     Changes seed values to zero based on CBP empolyment size count by
#     industry and region.
#     """
#
#     seed_df.set_index(['region', 'EMPSZES'], inplace=True)
#
#     # Reformat CBP data
#     cbp_pivot = cbp.copy(deep=True)
#
#     cbp_pivot.rename(columns={"n50_99": "50-99", "n100_249": "100-249",
#                               "n250_499": "250-499", "n500_999": "500-999"},
#                      inplace=True)
#
#     cbp_pivot = cbp_pivot.melt(id_vars=['region', 'naics'],
#                                value_vars=['Under 50', '50-99', '100-249',
#                                            '250-499', '500-999', 'Over 1000'],
#                                 var_name='EMPSZES')
#
#     cbp_pivot = pd.pivot_table(cbp_pivot, index=['region', 'EMPSZES'],
#                                columns=['naics'], values=['value'],
#                                aggfunc='sum')
#
#     cbp_pivot.columns = cbp_pivot.columns.droplevel()
#
#     shared_cols = []
#
#     for c in cbp_pivot.columns:
#         if c in seed_df.columns:
#             shared_cols.append(c)
#
#     cbp_mask = cbp_pivot[shared_cols].reindex(seed_df.index).fillna(0)
#
#     seed_df.update(seed_df[shared_cols].where(cbp_mask != 0, 0))
#
#     return seed_df
#
# seed = seed_correct_cbp(seed, cbp)
# # %%
# def seed_correct_MECS(seed_df, table3_2):
#     """
#     Changes seed values to zero based on MECS fuel use by industry and
#     region.
#     """
#
#     seed_df.reset_index(inplace=True, drop=False)
#
#     melt_cols = [0]
#
#     for n in range(3, len(table3_2.columns)):
#         melt_cols.append(n)
#
#     table3_2_mask = pd.pivot_table(
#         table3_2.iloc[:, melt_cols].melt(
#             id_vars=['region', 'naics'], var_name=['Fuel_type']
#             ),
#         index=['region', 'Fuel_type'], columns='naics'
#         )
#
#     table3_2_mask.columns = table3_2_mask.columns.droplevel()
#
#     seed_df.reset_index(drop=False, inplace=True)
#
#     seed_df.set_index(['region','Fuel_type'], inplace=True)
#
#     table3_2_mask = table3_2_mask.reindex(seed_df.index)
#
#     shared_cols = []
#
#     for c in table3_2_mask.columns:
#         if c in seed_df.columns:
#             shared_cols.append(c)
#
#     table3_2_mask = table3_2_mask[shared_cols].reindex(
#         seed_df.index
#         ).fillna(0)
#
#     seed_df.update(seed_df[shared_cols].where(table3_2_mask != 0, 0))
#
#     seed_df.reset_index(drop=True, inplace=True)
#
#     seed_df.drop(['EMPSZES'], axis=1, inplace=True)
#
#     return seed_df
#
# # %%

