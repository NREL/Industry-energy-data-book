import pandas as pd
import requests
import urllib
import json

"""
Estimate 2017 county-level fuel use in the agricultural sector based on 2017 
USDA Census results. Only diesel, gasoline, LP gas, and 'other' (assumed to be 
residual fuel oil) are included.
"""




# 1.EXPENSES ##################################################################
# state_tot: state total fuel expenses ($) ####################################
"""
Automatically collect state-level total fuel expenses data by NAICS
code from USDA NASS 2017 Census results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'CENSUS',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'STATE',
          'short_desc': 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $',
          'domain_desc': 'NAICS CLASSIFICATION'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
state_tot = pd.DataFrame(datajson['data'], 
                         columns=['state_name',
                                  'state_alpha',
                                  'domaincat_desc',
                                  'Value'])
pd.set_option('display.max_columns', None)
#print(state_tot.head(20))


####### Split the column of NAICS codes:
state_tot[['a','b']] = state_tot.domaincat_desc.str.split("(", expand=True)
state_tot[['NAICS','c']] = state_tot.b.str.split(")", expand=True)
state_tot = state_tot.drop(['domaincat_desc','a','b','c'], axis=1)
#print(state_tot.head(20))

    
####### Remove invalid values & Rename columns & Set index & Sort
invalid = '                 (D)'
state_tot = state_tot.replace(invalid, state_tot.replace([invalid], '0'))
state_tot.rename(columns = {'state_name':'state', 
                            'state_alpha':'state_abbv', 
                            'Value':'state_fuel_expense'}, 
                            inplace=True)
state_tot = state_tot.sort_index(ascending=True)
state_tot = state_tot[['state','state_abbv','NAICS','state_fuel_expense']]
#print(state_tot.head(50))
    
    
####### Remove commas in numbers
state_tot['state_fuel_expense'] = state_tot['state_fuel_expense'].apply(
                                  lambda x: x.replace(',', "")).astype(int)
#print(state_tot.head(50))






# region_diesel: region diesel expenses ($) ###################################
"""
Automatically collect region-level disel expense data from USDA NASS 2017 
Survey results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, DIESEL - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_diesel = pd.DataFrame(datajson['data'], 
                             columns=['region_desc',
                                      'short_desc',
                                      'Value'])
pd.set_option('display.max_columns', None)
#print(region_diesel)


####### Rename columns
region_diesel.rename(columns = {'region_desc':'region', 
                                'short_desc':'fuel_type', 
                                'Value':'fuel_expenses_$'}, 
                                inplace=True)


####### Reformat the column of region
region_diesel[['region','a']] = region_diesel.region.str.split(
                                ",", expand=True)
region_diesel = region_diesel.drop('a', axis=1)
region_diesel.set_index('region', inplace=True)
#print(region_diesel)


####### Reformat the column of fuel type
region_diesel['fuel_type'] = region_diesel['fuel_type'].replace(
                            'FUELS, DIESEL - EXPENSE, MEASURED IN $', 'DIESEL')
region_diesel = region_diesel.sort_index(ascending=True)
#print(region_diesel)
    
    
####### Remove commas in numbers
region_diesel['fuel_expenses_$'] = region_diesel['fuel_expenses_$'].apply(
                                 lambda x: x.replace(',', "")).astype('int64')
#print(region_diesel)






# region_gasoline: region gasoline expenses ($) ###############################
"""
Automatically collect region-level gasoline expense data from USDA NASS 2017 
Survey results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, GASOLINE - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_gasoline = pd.DataFrame(datajson['data'], 
                               columns=['region_desc',
                                        'short_desc',
                                        'Value'])
pd.set_option('display.max_columns', None)
#print(region_gasoline)


####### Rename columns
region_gasoline.rename(columns = {'region_desc':'region', 
                                  'short_desc':'fuel_type', 
                                  'Value':'fuel_expenses_$'}, 
                                  inplace=True)


####### Reformat the column of region
region_gasoline[['region','a']] = region_gasoline.region.str.split(
                                  ",", expand=True)
region_gasoline = region_gasoline.drop('a', axis=1)
region_gasoline.set_index('region', inplace=True)
#print(region_gasoline)


####### Reformat the column of fuel type
region_gasoline['fuel_type'] = region_gasoline['fuel_type'].replace(
                        'FUELS, GASOLINE - EXPENSE, MEASURED IN $', 'GASOLINE')
region_gasoline = region_gasoline.sort_index(ascending=True)
#print(region_gasoline)
    
    
####### Remove commas in numbers
region_gasoline['fuel_expenses_$'] = region_gasoline['fuel_expenses_$'].apply(
                                  lambda x: x.replace(',', "")).astype('int64')
#print(region_gasoline)






# region_lpg: region lp gas expenses ($) ######################################
"""
Automatically collect region-level LP gas expense data from USDA NASS 2017 
Survey results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, LP GAS - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_lpg = pd.DataFrame(datajson['data'], 
                          columns=['region_desc',
                                   'short_desc',
                                   'Value'])
pd.set_option('display.max_columns', None)
#print(region_lpg)


####### Rename columns
region_lpg.rename(columns = {'region_desc':'region', 
                             'short_desc':'fuel_type', 
                             'Value':'fuel_expenses_$'}, 
                             inplace=True)


####### Reformat the column of region
region_lpg[['region','a']] = region_lpg.region.str.split(",", expand=True)
region_lpg = region_lpg.drop('a', axis=1)
region_lpg.set_index('region', inplace=True)
#print(region_lpg)


####### Reformat the column of fuel type
region_lpg['fuel_type'] = region_lpg['fuel_type'].replace(
                          'FUELS, LP GAS - EXPENSE, MEASURED IN $', 'LPG')
region_lpg = region_lpg.sort_index(ascending=True)
#print(region_lpg)
    
    
####### Remove commas in numbers
region_lpg['fuel_expenses_$'] = region_lpg['fuel_expenses_$'].apply(
                                lambda x: x.replace(',', "")).astype('int64')
#print(region_lpg)






# region_other: other region fuel expenses ($) ################################
"""
Automatically collect other region-level fuel expense data from USDA NASS 2017 
Survey results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS, OTHER - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_other = pd.DataFrame(datajson['data'], 
                            columns=['region_desc',
                                     'short_desc',
                                     'Value'])
pd.set_option('display.max_columns', None)
#print(region_other)


####### Rename columns
region_other.rename(columns = {'region_desc':'region', 
                               'short_desc':'fuel_type', 
                               'Value':'fuel_expenses_$'}, 
                               inplace=True)


####### Reformat the column of region
region_other[['region','a']] = region_other.region.str.split(",", expand=True)
region_other = region_other.drop('a', axis=1)
region_other.set_index('region', inplace=True)
#print(region_other)


####### Reformat the column of fuel type
region_other['fuel_type'] = region_other['fuel_type'].replace(
                            'FUELS, OTHER - EXPENSE, MEASURED IN $', 'OTHER')
region_other = region_other.sort_index(ascending=True)
#print(region_other)
    
    
####### Remove commas in numbers
region_other['fuel_expenses_$'] = region_other['fuel_expenses_$'].apply(
                                  lambda x: x.replace(',', "")).astype('int64')
#print(region_other)






# frac: fuel type fraction of total fuel expenses #############################
"""
Calculate each fuel type's fraction of total fuel expenses based on
region_diesel, region_gasoline, region_lp, and region_other.
"""

frac = pd.merge(region_diesel, region_gasoline, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = pd.merge(frac, region_lpg, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = pd.merge(frac, region_other, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = frac.set_index('fuel_type', append=True)

frac['fuel_type_frac'] = frac['fuel_expenses_$'].divide(
                         frac['fuel_expenses_$'].sum(level='region'))

frac = frac.drop(['fuel_expenses_$'], axis=1)

frac = frac.reset_index()
#print(frac)






# expense： state expenses by fuel type and NAICS ($) ##########################
"""
Combine state_tot and frac
Step 1： In the dataframe 'state_tot', add each state's region.
Step 2: Merge 'frac' and 'state_tot'. Name the merged df as 'expense'.
Step 3: Create a new column 'fuel_expense_$', which equals 
        'state_fuel_expense_$' * 'fuel_type_frac'. 
Final columns: state, state_abbv, region, NAICS, fuel_type, fuel_expenses_$
"""

####### Step 1
region_file = pd.read_csv('ag_source_region.csv')

region = region_file[['state', 'state_abbv', 'region']]

state_tot = pd.merge(state_tot, region, on=[
            'state', 'state_abbv'], how='outer')

state_tot = state_tot[[
            'state','state_abbv','region','NAICS','state_fuel_expense']]


####### Step 2
expense = pd.merge(state_tot, frac, on='region')


####### Step 3
expense['fuel_expense_dollar'] = \
              expense.state_fuel_expense * expense.fuel_type_frac

expense = expense.drop(['state_fuel_expense', 'fuel_type_frac'], axis=1)
#print(expense.head(30))










# 2.PRICES#####################################################################
# price_diesel: state level diesel price ($/gal) ##############################
"""
Automatically collect 2017 monthly diesel retail price data by region from EIA
and calculate state-level data.
"""

price_diesel_region = pd.read_excel(
        'https://www.eia.gov/petroleum/gasdiesel/xls/psw18vwall.xls',
        sheet_name='Data 2', header=2)

price_diesel_region.columns = ['Date',
                               'US',
                               'EAST COAST',
                               'NEW ENGLAND',
                               'CENTRAL ATLANTIC',
                               'LOWER ATLANTIC',
                               'MIDWEST',
                               'GULF COAST',
                               'ROCKY MOUNTAIN',
                               'WEST COAST',
                               'CALIFORNIA',
                               'WEST COAST EXCEPT CALIFORNIA']

price_diesel_region = price_diesel_region.drop([
                      'US', 'EAST COAST', 'WEST COAST'], axis=1)


####### Only keep 2017 data 
price_diesel_region = price_diesel_region.loc[
                      price_diesel_region['Date'].dt.year==2017]
price_diesel_region.reset_index(level=0, inplace=True)
price_diesel_region = price_diesel_region.drop(['index','Date'], axis=1)
#print(price_diesel_region)


####### Transpose the dataframe
price_diesel_region = price_diesel_region.transpose()
price_diesel_region.index.name = 'diesel_padd'
#print(price_diesel_region)


####### Calculate 2017 average diesel price
price_diesel_region['price_dollar_per_gal'] = price_diesel_region.mean(axis=1)
price_diesel_region = price_diesel_region[['price_dollar_per_gal']]
#print(price_diesel_region)


####### Mark the fuel type
price_diesel_region['fuel_type'] = 'DIESEL'
price_diesel_region = price_diesel_region[[
                      'fuel_type', 'price_dollar_per_gal']]
price_diesel_region.reset_index(level=0, inplace=True)
#print(price_diesel_region)


####### Add state column
region_file = pd.read_csv('ag_source_region.csv')
region = region_file[['state','diesel_padd']]

price_diesel = pd.merge(
               price_diesel_region, region, on='diesel_padd', how='outer')

price_diesel.set_index('state', inplace=True)
price_diesel= price_diesel.drop('diesel_padd', axis=1)
price_diesel = price_diesel.sort_index()
#print(price_diesel)






# price_gasoline: region level gasoline price ($/gal) #########################
"""
Automatically collect 2017 monthly gasoline retail price data by region from 
EIA and calculate state-level data.
"""

price_gasol_region = pd.read_excel(
        'https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls',
        sheet_name='Data 3', header=2)

price_gasol_region.columns = ['Date',
                              'US',
                              'EAST COAST',
                              'NEW ENGLAND',
                              'CENTRAL ATLANTIC',
                              'LOWER ATLANTIC',
                              'MIDWEST',
                              'GULF COAST',
                              'ROCKY MOUNTAIN',
                              'WEST COAST',
                              'CALIFORNIA',
                              'COLORADO',
                              'FLORIDA',
                              'MASSACHUSETTS',
                              'MINNESOTA',
                              'NEW YORK',
                              'OHIO',
                              'TEXAS',
                              'WASHINGTON',
                              'BOSTON','CHICAGO','CLEVELAND','DENVER',
                              'HOUSTON','LOS ANGELES','MIAMI','NEW YORK CITY',
                              'SAN FRANCISCO','SEATTLE']

price_gasol_region = price_gasol_region.drop(['US', 'EAST COAST', 'BOSTON',
             'CHICAGO','CLEVELAND','DENVER','HOUSTON','LOS ANGELES','MIAMI',
             'NEW YORK CITY','SAN FRANCISCO','SEATTLE'], axis=1)


####### Only keep 2017 data 
price_gasol_region = price_gasol_region.loc[
                     price_gasol_region['Date'].dt.year==2017]
price_gasol_region.reset_index(level=0, inplace=True)
price_gasol_region = price_gasol_region.drop(['index','Date'], axis=1)
#print(price_gasol_region)


####### Transpose the dataframe
price_gasol_region = price_gasol_region.transpose()
price_gasol_region.index.name = 'gasoline_padd'
#print(price_gasol_region)


####### Calculate 2017 average gasoline price
price_gasol_region['price_dollar_per_gal'] = price_gasol_region.mean(axis=1)
price_gasol_region = price_gasol_region[['price_dollar_per_gal']]
#print(price_gasol_region)


####### Mark the fuel type
price_gasol_region['fuel_type'] = 'GASOLINE'
price_gasol_region = price_gasol_region[[
                     'fuel_type', 'price_dollar_per_gal']]
price_gasol_region.reset_index(level=0, inplace=True)
#print(price_gasol_region)


####### Add state column
region_file = pd.read_csv('ag_source_region.csv')
region = region_file[['state', 'gasoline_padd']]

price_gasoline = pd.merge(
                 price_gasol_region, region, on='gasoline_padd', how='outer')

price_gasoline.set_index('state', inplace=True)
price_gasoline= price_gasoline.drop('gasoline_padd', axis=1)
price_gasoline = price_gasoline.sort_index()
#print(price_gasoline)






# price_lpg: region level lp gas price ($/gal) ################################
"""
Automatically collect 2017 monthly wholesale propane price data by region from
EIA and calculate state-level data.
"""

price_lpg_region = pd.read_excel(
     'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPLLPA_PWR_DPGAL_W.xls',
     sheet_name='Data 1', header=2)

price_lpg_region.columns = ['Date', 'US', 'EAST COAST', 'CENTRAL ATLANTIC',
                            'MARYLAND', 'NEW JERSEY', 'NEW YORK',
                            'PENNSYLVANIA', 'LOWER ATLANTIC', 'GEORGIA',
                            'NORTH CAROLINA', 'VIRGINIA', 'MIDWEST',
                            'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS',
                            'KENTUCKY', 'MICHIGAN', 'MINNESOTA', 'MISSOURI',
                            'NEBRASKA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA',
                            'SOUTH DAKOTA', 'WISCONSIN', 'GULF COAST',
                            'ALABAMA', 'ARKANSAS', 'MISSISSIPPI', 'TEXAS',
                            'ROCKY MOUNTAIN', 'COLORADO']

####### Only keep 2017 data
price_lpg_region = price_lpg_region.loc[price_lpg_region['Date'].dt.year==2017]
price_lpg_region.reset_index(level=0, inplace=True)
price_lpg_region = price_lpg_region.drop(['index','Date'], axis=1)
#print(price_lpg_region)


####### Transpose the dataframe
price_lpg_region = price_lpg_region.transpose()
price_lpg_region.index.name = 'lpg_padd'
#print(price_lpg_region)


####### Calculate 2017 average wholesale propane price
price_lpg_region['price_dollar_per_gal'] = price_lpg_region.mean(axis=1)
price_lpg_region = price_lpg_region[['price_dollar_per_gal']]
#print(price_lpg_region)


####### Mark the fuel type
price_lpg_region['fuel_type'] = 'LPG'
price_lpg_region = price_lpg_region[['fuel_type', 'price_dollar_per_gal']]
price_lpg_region.reset_index(level=0, inplace=True)
#print(price_lpg_region)


####### Add state column
region_file = pd.read_csv('ag_source_region.csv')
region = region_file[['state', 'lpg_padd']]

price_lpg = pd.merge(price_lpg_region, region, on='lpg_padd', how='outer')

price_lpg.set_index('state', inplace=True)
price_lpg= price_lpg.drop('lpg_padd', axis=1)
price_lpg = price_lpg.sort_index()
#print(price_lpg)






# price_other: region level heating oil price ($/gal) #########################
"""
Automatically collect 2017 monthly wholesale propane price data by region from
EIA and calculate state-level data.
"""

price_other_region = pd.read_excel(
     'https://www.eia.gov/dnav/pet/xls/PET_PRI_WFR_A_EPD2F_PWR_DPGAL_W.xls',
     sheet_name='Data 1', header=2)

price_other_region.columns = ['Date', 'US', 'EAST COAST', 'NEW ENGLAND', 
                              'CONNECTICUT', 'MAINE', 'MASSACHUSETTS', 
                              'NEW HAMPSHIRE', 'RHODE ISLAND', 'VERMONT', 
                              'CENTRAL ATLANTIC', 'DELAWARE', 'MARYLAND', 
                              'NEW JERSEY', 'NEW YORK', 'PENNSYLVANIA', 
                              'LOWER ATLANTIC', 'NORTH CAROLINA', 'VIRGINIA', 
                              'MIDWEST', 'ILLINOIS', 'INDIANA', 'IOWA', 
                              'KANSAS', 'KENTUCKY', 'MICHIGAN', 'MINNESOTA', 
                              'MISSOURI', 'NEBRASKA', 'NORTH DAKOTA', 'OHIO', 
                              'SOUTH DAKOTA', 'WISCONSIN']

price_other_region = price_other_region.drop(
                     ['EAST COAST', 'NEW ENGLAND'], axis=1)


####### Only keep 2017 data
price_other_region = price_other_region.loc[
                     price_other_region['Date'].dt.year==2017]

price_other_region.reset_index(level=0, inplace=True)
price_other_region = price_other_region.drop(['index','Date'], axis=1)
#print(price_other_region)


####### Transpose the dataframe
price_other_region = price_other_region.transpose()
price_other_region.index.name = 'other_padd'
#print(price_other_region)


####### Calculate 2017 average wholesale heating oil price
price_other_region['price_dollar_per_gal'] = price_other_region.mean(axis=1)
price_other_region = price_other_region[['price_dollar_per_gal']]
#print(price_other_region)


####### Mark the fuel type
price_other_region['fuel_type'] = 'OTHER'
price_other_region = price_other_region[['fuel_type', 'price_dollar_per_gal']]
price_other_region.reset_index(level=0, inplace=True)
#print(price_other_region)


####### Add state column
region_file = pd.read_csv('ag_source_region.csv')
region = region_file[['state', 'other_padd']]

price_other = pd.merge(
              price_other_region, region, on='other_padd', how='outer')

price_other.set_index('state', inplace=True)
price_other= price_other.drop('other_padd', axis=1)
price_other = price_other.sort_index()
#print(price_other)






# price: state prices by fuel type ($/mmbtu) ##################################
"""
Concatenate price_diesel, price_gasoline, price_lpg, and price_other.
Convert $/gallon to $/barrel then to $/mmbtu
"""

price = pd.concat([price_diesel, price_gasoline, price_lpg, price_other])

price['price_dollar_per_barrel'] = price['price_dollar_per_gal'] * 42

heat_content = pd.DataFrame(
               {'fuel_type': ['DIESEL', 'GASOLINE', 'LPG', 'OTHER'],
                'mmbtu_per_barrel':[5.772, 5.053, 3.836, 6.287]})              # https://www.eia.gov/totalenergy/data/monthly/pdf/sec13.pdf

price = price.reset_index().merge(heat_content, how='outer').set_index('state')

price['price_dollar_per_mmbtu'] = \
                   price['price_dollar_per_barrel'] / price['mmbtu_per_barrel']

price = price.drop([
                  'price_dollar_per_gal', 'price_dollar_per_barrel', 
                  'mmbtu_per_barrel'], axis=1)
#print(price)










# 3.FUEL CONSUMPTION ##########################################################
# fuel_state: state fuel use (mmbtu) ##########################################
"""
Merge expense and price. Divide expense by price to calculate state-level fuel
consumption by NAICS. 
"""

fuel_state = pd.merge(expense, price, on=['state','fuel_type'], how='outer')

fuel_state['fuel_state_mmbtu'] = \
        fuel_state['fuel_expense_dollar']/fuel_state['price_dollar_per_mmbtu']
        
#print(fuel_state.head(50))






# fc: county farm counts by NAICS #############################################
"""
Automatically collect county-level farm counts data by NAICS from USDA NASS 
2017 Census results and calculate each county's state fraction. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'CENSUS',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'FARMS & LAND & ASSETS',
          'year': 2017,
          'agg_level_desc': 'COUNTY',
          'short_desc': 'FARM OPERATIONS - NUMBER OF OPERATIONS',
          'domain_desc': 'NAICS CLASSIFICATION'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
fc = pd.DataFrame(datajson['data'], 
                  columns=['state_name','state_alpha','county_name',
                           'domaincat_desc','Value'])


####### Split the column of NAICS codes:
fc[['a','b']] = fc.domaincat_desc.str.split("(", expand=True)
fc[['NAICS','c']] = fc.b.str.split(")", expand=True)
fc = fc.drop(['domaincat_desc','a','b','c'], axis=1)
#print(fc.head(20))

    
####### Remove invalid values & Rename columns & Set index & Sort
invalid = '                 (D)'
fc = fc.replace(invalid, fc.replace([invalid], '0'))
fc.rename(columns = {'state_name':'state', 
                     'state_alpha':'state_abbv', 
                     'Value':'farm_counts',
                     'county_name':'county'}, 
                     inplace=True)
fc.set_index('state', inplace=True)
fc = fc.sort_index(ascending=True)
#print(fc.head(20))


####### Remove observations for NAICS 1119, which double counts observations 
####### for 11192 and "11193 & 11194 & 11199".
fc = fc[fc.NAICS != '1119']
#print(fc.head(20))


####### Remove commas in numbers
fc['farm_counts'] = fc['farm_counts'].apply(lambda x: x.replace(
        ',', "")).astype(int)


####### Calculate the fraction of county-level establishments by NAICS
fc['fc_statefraction'] = fc['farm_counts'].divide(
                         fc['farm_counts'].sum(level='state'))
#print(fc.head(20))





 
# fuel_county: county fuel use (mmbtu) ########################################
"""
Calculations based on fuel_state (state-level fuel use by NAICS) and fc 
(county farm counts by NAICS)
"""

fuel_county = fc.reset_index().merge(fuel_state).set_index('state')

fuel_county['fuel_county_mmbtu'] = \
                    fuel_county.fc_statefraction * fuel_county.fuel_state_mmbtu
                    
fuel_county = fuel_county[['state_abbv', 'county', 'NAICS', 'fuel_type',
                           'fuel_state_mmbtu', 'fuel_county_mmbtu']]
                    
fuel_county.to_csv('ag_output_fuel_use_by_county_mmbtu.csv')
#print(fuel_county.head(50))
