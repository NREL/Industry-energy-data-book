
import requests
import pandas as pd
import json

class Ag:

    def get_ag_census_data(year, data):
        """
        Automatically collect state-level total fuel expenses data by NAICS
        code from USDA NASS 2017 Census results.

        year is 2012 or 2017

        fuel_type is fuels or electricity
        """

        if data == 'fuels':

            data_desc = 'FUELS, INCL LUBRICANTS - EXPENSE, MEASURED IN $'

            group_desc = 'EXPENSES'

        if data == 'elctricity':

            data_desc = 'AG SERVICES, UTILITIES - EXPENSE, MEASURED IN $'

            group_desc = 'EXPENSES'

        if data == 'farm_counts':



        base_url = 'http://quickstats.nass.usda.gov/api/api_GET/'

        params = {'key': '0E2FCC55-CF7E-3C9F-B173-99196B47DFC8',
                  'source_desc': 'CENSUS', 'sector_desc': 'ECONOMICS',
                  'group_desc': 'EXPENSES', 'year': year,
                  'agg_level_desc': 'STATE', 'short_desc': data_desc,
                  'domain_desc': 'NAICS CLASSIFICATION'}

        r = requests.get(base_url, params=params)
        #print(r.content)
        url = r.url
        #print(url)

        response = urllib.request.urlopen(url)

        data = response.read()

        datajson = json.loads(data)

        state_tot = pd.DataFrame(
            datajson['data'], columns=['state_name', 'state_alpha',
                                       'domaincat_desc','Value']
            )

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
                                    'state_alpha':'state_abbr',
                                    'Value':'ag_expense_$'},
                         inplace=True)

        state_tot.set_index('state', inplace=True)

        state_tot = state_tot.sort_index(ascending=True)

        ####### Remove commas in numbers
        state_tot['ag_expense_$'] = state_tot['ag_expense_$'].apply(
            lambda x: x.replace(',', "")
            ).astype(int)

        ####### Find fraction by state
        state_tot['ag_expense_state_pct'] = state_tot['ag_expense_$'].divide(
                                       state_tot['ag_expense_$'].sum(level='state')
                                       )

        return state_tot
