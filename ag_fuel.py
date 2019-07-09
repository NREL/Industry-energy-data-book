import pandas as pd
import requests
import urllib
import json

"""
Estimate 2017 county-level fuel use in the agricultural sector based on 2017 
USDA Census results. Only diesel, gasoline, LP gas, and 'other' (assumed to be 
residual fuel oil) are included.
"""





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
                            'Value':'fuel_expenses_$'}, 
                            inplace=True)
state_tot = state_tot.sort_index(ascending=True)
state_tot = state_tot[['state', 'state_abbv', 'NAICS', 'fuel_expenses_$']]
#print(state_tot.head(50))
    
    
####### Remove commas in numbers
state_tot['fuel_expenses_$'] = state_tot['fuel_expenses_$'].apply(
                               lambda x: x.replace(',', "")).astype(int)
#print(state_tot.head(50))






# region_tot: region total fuel expenses ($) ##################################
"""
Automatically collect data (the fuel type fraction of total fuel expenses by
region) from USDA NASS 2017 Survey results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'SURVEY',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'REGION : MULTI-STATE',
          'commodity_desc': 'FUELS',
          'short_desc': 'FUELS - EXPENSE, MEASURED IN $'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
region_tot = pd.DataFrame(datajson['data'], 
                          columns=['region_desc',
                                   'short_desc',
                                   'Value'])
pd.set_option('display.max_columns', None)
#print(region_tot)


####### Rename columns
region_tot.rename(columns = {'region_desc':'region', 
                             'short_desc':'fuel_type', 
                             'Value':'fuel_expenses_$'}, 
                             inplace=True)


####### Reformat the column of region
region_tot = region_tot[~region_tot.region.str.contains('OTHER')]
region_tot[['region','a']] = region_tot.region.str.split(",", expand=True)
region_tot = region_tot.drop('a', axis=1)
region_tot.set_index('region', inplace=True)
#print(region_tot)


####### Reformat the column of fuel type
region_tot['fuel_type'] = region_tot['fuel_type'].replace(
                          'FUELS - EXPENSE, MEASURED IN $', 'TOTAL')
region_tot = region_tot.sort_index(ascending=True)
#print(region_tot)
    
    
####### Remove commas in numbers
region_tot['fuel_expenses_$'] = region_tot['fuel_expenses_$'].apply(
                                lambda x: x.replace(',', "")).astype('int64')
#print(region_tot)






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






# region_lp: region lp gas expenses ($) #######################################
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
region_lp = pd.DataFrame(datajson['data'], 
                         columns=['region_desc',
                                  'short_desc',
                                  'Value'])
pd.set_option('display.max_columns', None)
#print(region_lp)


####### Rename columns
region_lp.rename(columns = {'region_desc':'region', 
                            'short_desc':'fuel_type', 
                            'Value':'fuel_expenses_$'}, 
                            inplace=True)


####### Reformat the column of region
region_lp[['region','a']] = region_lp.region.str.split(",", expand=True)
region_lp = region_lp.drop('a', axis=1)
region_lp.set_index('region', inplace=True)
#print(region_lp)


####### Reformat the column of fuel type
region_lp['fuel_type'] = region_lp['fuel_type'].replace(
                         'FUELS, LP GAS - EXPENSE, MEASURED IN $', 'LP GAS')
region_lp = region_lp.sort_index(ascending=True)
#print(region_lp)
    
    
####### Remove commas in numbers
region_lp['fuel_expenses_$'] = region_lp['fuel_expenses_$'].apply(
                               lambda x: x.replace(',', "")).astype('int64')
#print(region_lp)






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

frac = pd.merge(frac, region_lp, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac = pd.merge(frac, region_other, on=[
        'region', 'fuel_type','fuel_expenses_$'], how='outer')

frac['fuel_type_frac'] = frac['fuel_expenses_$'].divide(frac['fuel_expenses_$'].sum(level='region'))                 #ValueError: cannot reindex from a duplicate axis

print(frac)
