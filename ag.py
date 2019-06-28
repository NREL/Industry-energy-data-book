import pandas as pd

import numpy as np

class Ag(object):
    
    @staticmethod
    def state_expense_by_naics():
        """
        Automatically collect state-level agricultural expense data by NAICS
        code from USDA NASS 2017 Census results. 
        """
        
        import requests
        import urllib
        import json
        
        base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'
        
        params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
                  'source_desc': 'CENSUS',
                  'sector_desc': 'ECONOMICS',
                  'group_desc': 'EXPENSES',
                  'year': 2017,
                  'agg_level_desc': 'STATE',
                  'short_desc': 'AG SERVICES, OTHER - EXPENSE, MEASURED IN $',
                  'domain_desc': 'NAICS CLASSIFICATION'}
 
        r = requests.get(base_url, params=params)
        #print(r.content)
        url = r.url
        #print(url)
        
        response = urllib.request.urlopen(url)
        data = response.read()
        datajson = json.loads(data)
        sebn = pd.DataFrame(datajson['data'], 
                          columns=['state_name','state_alpha','state_ansi',
                                   'domaincat_desc','Value'])
        
        ####### Split the column of NAICS codes:
        sebn[['a','b']] = sebn.domaincat_desc.str.split("(", expand=True)
        sebn[['NAICS','c']] = sebn.b.str.split(")", expand=True)
        sebn = sebn.drop(['domaincat_desc','a','b','c'], axis=1)
        #print(sebn.head(20))
        
        ####### Remove invalid values & Rename columns & Set index & Sort
        invalid = '                 (D)'
        sebn = sebn.replace(invalid, sebn.replace([invalid], [None]))
        sebn.rename(columns = {'state_name':'state', 
                               'state_alpha':'state_abbv', 
                               'Value':'ag_expense_$'}, 
                    inplace=True)
        sebn.set_index('state', inplace=True)
        sebn = sebn.sort_index(ascending=True)
        print(sebn.head(50))
        #sebn.to_csv('C:\iedb\code\sebn.csv')
        
        return sebn
