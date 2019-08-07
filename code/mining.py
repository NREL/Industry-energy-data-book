import pandas as pd
import requests
import urllib
import json
import get_cbp


"""
Estimate county-level fuel consumption in the mining sector 
"""




# 1.NATIONAL FUEL CONSUMPTION #################################################
# (1.1) fuel_cost: 2012 Costs (000$) by NAICS & Fuel ##########################
"""
fuel_cost_source: 2012 Economic Census data from ‘Mining: Subject Series: 
Materials Summary: Selected Supplies, Minerals Received for Preparation, 
Purchased Machinery, and Fuels Consumed by Type of Industry: 2012.’
Variables: https://api.census.gov/data/2012/ecnmatfuel/variables.html.
Columns: NAICS, fuel_id, fuel_name, fuel_cost

fuel_cost: National mining-sector fuel costs by fuel type. Columns: NAICS, 
fuel_id, fuel_name (only crude, coal, diesel, residual, natural gas, and 
gasoline), fuel_cost_k_usd.
"""

####### Collect data from Census

def get_fuel_cost(naics):
    
    base_url = 'https://api.census.gov/data/2012/ecnmatfuel'
    
    params = {
    'get':
    'NAICS2012,MATFUEL,MATFUEL_TTL,MATFUELCOST,MATFUELCOST_F,MATFUELQTY,UNITS_TTL,M_FI',
    'for':'us',
    'NAICS2012':naics,
    'key':'489f08f390013bc6d41ee377e86ea8c1b0dd5267'}
    
    r = requests.get(base_url, params=params)
    #print(r.content)
    url = r.url
    #print(url)
    
    response = urllib.request.urlopen(url)
    data = response.read()
    datajson = json.loads(data)
    get_fuel_cost = pd.DataFrame(datajson) 
    
    get_fuel_cost.columns = get_fuel_cost.iloc[0]
    get_fuel_cost = get_fuel_cost[1:]

    get_fuel_cost.rename(columns = {'NAICS2012':'NAICS',
                                    'MATFUEL':'fuel_id', 
                                    'MATFUEL_TTL':'fuel_name', 
                                    'MATFUELCOST':'fuel_cost_k_usd',
                                    'MATFUELCOST_F':'fuel_cost_missing',
                                    'MATFUELQTY':'fuel_qty',
                                    'UNITS_TTL':'fuel_qty_unit',
                                    'M_FI':'fuel_flag'},
                                    inplace=True)
    
    get_fuel_cost = get_fuel_cost[['NAICS','fuel_id','fuel_name',
                                   'fuel_cost_k_usd','fuel_cost_missing',
                                   'fuel_qty','fuel_qty_unit','fuel_flag']]
    
    get_fuel_cost = get_fuel_cost.loc[:,~get_fuel_cost.columns.duplicated()]
    
    # Remove material costs, keep only fuel costs:
    get_fuel_cost = get_fuel_cost[get_fuel_cost.fuel_flag == 'F']
    get_fuel_cost = get_fuel_cost.drop('fuel_flag', axis=1)
    
    return get_fuel_cost


mining_naics = [211111,211112,212111,212112,212113,212210,212221,212222,212231,
                212234,212291,212299,212311,212312,212313,212319,212321,212322,
                212324,212325,212391,212392,212393,212399,213111,213112,213113,
                213114,213115]

fuel_cost_source = pd.DataFrame()

for n in mining_naics:
    fuel_cost_source = pd.concat([fuel_cost_source, get_fuel_cost(n)])

fuel_cost_source.set_index('NAICS', inplace=True)                              # Only for test
fuel_cost_source.to_csv('mining_fuel_cost_source.csv')                         # Only for test



####### Fill out missing data in column fuel_cost_k_usd
fuel_cost_source = pd.read_csv('mining_fuel_cost_source.csv')                  # Only for test

fuel_cost_source.set_index('NAICS', inplace=True)
fuel_cost_source = fuel_cost_source[fuel_cost_source.fuel_id != 21111003]      # Ignore the costs of natural gas produced and used in the same plant

fuel_cost = pd.DataFrame()

for n in mining_naics:
    
    single_sector = fuel_cost_source.loc[n, : ]
    single_sector = single_sector[single_sector.fuel_cost_missing != 'X']
    
    tot = single_sector.iloc[0]['fuel_cost_k_usd']
    cost_sum = single_sector.iloc[1:]['fuel_cost_k_usd'].sum()
    missing_value_sum = tot - cost_sum
    
    missing_value_counts = single_sector[single_sector['fuel_cost_k_usd']==0][
                           'fuel_cost_k_usd'].count()
    
    missing_value = missing_value_sum / missing_value_counts
    
    single_sector['fuel_cost_k_usd'] =single_sector['fuel_cost_k_usd'].replace(
                                                              0, missing_value)
    
    single_sector = single_sector[['fuel_id','fuel_name','fuel_cost_k_usd']]
    
    fuel_cost = pd.concat([fuel_cost, single_sector])

fuel_cost = fuel_cost.reset_index()



####### Rename fuels
fuel_name_dict = {'fuel_id':[2,960018,974000,21111015,21211003,32411015,
                             32411017,32411019],
                  'fuel':['TOTAL','MISC','UNDISTRIBUTED','GAS','COAL',
                               'GASOLINE','DIESEL','RESIDUAL FUEL OIL']}
                  
fuel_name = pd.DataFrame(fuel_name_dict)

fuel_cost = fuel_cost.drop('fuel_name', axis=1)
fuel_cost = pd.merge(fuel_cost, fuel_name, on='fuel_id', how='outer')

fuel_cost.set_index(['NAICS','fuel_id'], inplace=True)
fuel_cost = fuel_cost.sort_index().reset_index()
fuel_cost = fuel_cost[['NAICS','fuel_id','fuel','fuel_cost_k_usd']]
print(fuel_cost)






# (1.2) fuel_price


























