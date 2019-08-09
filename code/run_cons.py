import pandas as pd
import requests
import urllib
import json
import get_cbp
import cons

def Construction(calculation_years=range(2010, 2017)):
    # import 2012 Economic Census data.
    
    census_data = pd.concat(
                [cons.census(naics) for naics in [23, 236, 237, 238]],
                ignore_index=True
                )
    # Fill in missing values
    DE = cons.fill_in_missing_data('DE')
    DC = cons.fill_in_missing_data('DC')
    WV = cons.fill_in_missing_data('WV')

    census_data = census_data[(census_data.state_abbr != 'DE') &
                              (census_data.state_abbr != 'DC') &
                              (census_data.state_abbr != 'WV')]

    census_data = pd.concat([census_data, DE, DC, WV])

    census_data.set_index('state', inplace=True)

    census_data = census_data.sort_index().reset_index()

    census_data = census_data.apply(pd.to_numeric, errors='ignore')

    # Calculate state-level diesel consumption
    diesel_state = cons.calc_diesel_state(census_data)

    # Calculate state-level natural gas consumption
    ng_state = cons.calc_ng_state(census_data)

    
