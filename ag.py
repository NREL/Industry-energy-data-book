import pandas as pd

import requests
import urllib
import json

"""
Estimate 2014 county-level data based on 2012 USDA Census results.
Only diesel, gasoline, LP gas, and 'other' (assumed to be residual
fuel oil) are included.
"""






# aebn: agricultural expenses by NAICS #######################################
"""
Automatically collect state-level agricultural expense data by NAICS
code from USDA NASS 2017 Census results. 
"""

base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
          'source_desc': 'CENSUS',
          'sector_desc': 'ECONOMICS',
          'group_desc': 'EXPENSES',
          'year': 2017,
          'agg_level_desc': 'STATE',
          'short_desc': 'AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $',
          'domain_desc': 'NAICS CLASSIFICATION'}

r = requests.get(base_url, params=params)
#print(r.content)
url = r.url
#print(url)
    
response = urllib.request.urlopen(url)
data = response.read()
datajson = json.loads(data)
aebn = pd.DataFrame(datajson['data'], 
                    columns=['state_name','state_alpha','state_ansi',
                             'domaincat_desc','Value'])


####### Split the column of NAICS codes:
aebn[['a','b']] = aebn.domaincat_desc.str.split("(", expand=True)
aebn[['NAICS','c']] = aebn.b.str.split(")", expand=True)
aebn = aebn.drop(['domaincat_desc','a','b','c'], axis=1)
#print(aebn.head(20))

    
####### Remove invalid values & Rename columns & Set index & Sort
invalid = '                 (D)'
aebn = aebn.replace(invalid, aebn.replace([invalid], '0'))
aebn.rename(columns = {'state_name':'state', 
                       'state_alpha':'state_abbv', 
                       'Value':'ag_expense_$'}, 
                       inplace=True)
aebn.set_index('state', inplace=True)
aebn = aebn.sort_index(ascending=True)
#print(aebn.head(50))
    
    
####### Remove commas in numbers
aebn['ag_expense_$'] = aebn['ag_expense_$'].apply(lambda x: x.replace(
        ',', "")).astype(int)

    
####### Find fraction by state
aebn['ag_expense_state_pct'] = aebn['ag_expense_$'].divide(
                               aebn['ag_expense_$'].sum(level='state'))
#aebn.to_csv('ag_expenses_by_naics_000dollars.csv')
#print(aebn.head(50))
    
    
    
    
    
    
# ep: electricity price ##################################################
"""
Automatically collect 2017 annual electricity sales (MWh) and revenues 
(000 $) data by state and calculate electricity prices ($/kWh) in each 
state. 
"""
    
####### Collect electricity revenue data (000 $) 
rev = pd.read_excel(
        'http://www.eia.gov/electricity/data/state/revenue_annual.xlsx',
        header=1, nrows=51)
    
rev = rev[['State','Industrial']]
rev.rename(columns = {'State':'state_abbv',
                      'Industrial':'rev_000$'}, inplace = True)
#print(rev)
    
    
####### Collect electricity sales data (MWh) 
sal = pd.read_excel(
        'http://www.eia.gov/electricity/data/state/sales_annual.xlsx',
         header=1, nrows=51)

sal = sal[['State','Industrial']]
sal.rename(columns = {'State':'state_abbv',
                      'Industrial':'sal_mwh'}, inplace = True)
#print(sal)
    
    
####### Calculate electricity price ($/kWh)
merge = pd.merge(sal, rev, on='state_abbv')
merge['ep_kwh'] = merge['rev_000$']/merge['sal_mwh']
ep = merge[['state_abbv','ep_kwh']]
#ep.to_csv('ag_electricity_price_dollarsperkwh.csv')
#print(ep)






# ee: electricity expenses ###################################################
"""
Collect 2017 farm sector electricity expenses by state from USDA ERS
https://data.ers.usda.gov/reports.aspx?ID=17842#
P474eafd3e12544e19338a00227af3001_2_252iT0R0x17
"""

ee_source = pd.read_excel('ag_SOURCE_electricity_expenses.xlsx',
                          sheet_name=18, 
                          header=5,
                          usecols="B:C"
                          )                                                       #API?
ee = ee_source.drop(ee_source.index[[0,1,2]])
ee.columns = ['state','ee_000_dollars']
ee['state'] = ee['state'].str.upper()
#ee.to_csv('ag_electricity_expenses_000dollars.csv)
#print(ee.head(10))






# elec: electricity use ######################################################
"""
Calculate 2017 agricultural sector electricity use (MMBtu) by NAICS code in 
each state based on aebn (the share of a subsector's ag expenses in a state's 
total ag expenses), ee (each state's electricity expenses, 000$), and ep
(each state's electricity price, $/kWh).
"""

elec = aebn.reset_index().merge(ep).merge(ee).set_index('state')

elec['elec_mmbtu']=elec.ag_expense_state_pct * elec.ee_000_dollars*1000 / elec.ep_kwh *0.00341214

elec = elec[['state_abbv', 'NAICS', 'elec_mmbtu']]

#elec.to_csv('ag_electricity_use_mmbtu.csv')
#print(elec.head(50))






# fc: farm counts ############################################################

"""
Automatically collect county-level farm counts data by NAICS from USDA NASS 
2017 Census results. 
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
fc.to_csv('ag_farm_counts.csv')
print(fc.head(20))


####### Remove observations for NAICS 1119, which double counts observations 
####### for 11192 and "11193 & 11194 & 11199".
fc = pd.read_csv('ag_farm_counts.csv')                                                  # Only for test
fc = fc[fc.NAICS != '1119']
#print(fc.head(20))


####### Remove commas in numbers
fc['farm_counts'] = fc['farm_counts'].apply(lambda x: x.replace(
        ',', "")).astype(int)


####### Calculate the fraction of county-level establishments by NAICS
fc_state = pd.pivot_table(fc, values = ['farm_counts'], index = ['state'],
                    columns = ['NAICS'], aggfunc = np.sum
                    )

for s in fc_state.index:
    
    for n in fc_state.columns.levels[1]:
        
        i = fc[(fc.index == s) & (fc['NAICS'] == n)].index
        
        if i.size == 0:
            pass
        
        else:
            fc.loc[i,'statefraction'] = fc[fc.state == s].NAICS / fc_state.loc[
                    s, ('NAICS', n)]
            
print(fc.head(10))



        
       
