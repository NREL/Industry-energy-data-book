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
    
    
    # Calculate state-level energy use (all in MMBtu)
    # Diesel use
    diesel_state = cons.calc_diesel_state(census_data)

    # Natural gas use
    ng_state = cons.calc_ng_state(census_data)
    
    # Electricity use
    elect_state = cons.calc_elec_state(census_data)
    
    # Liquid petroleum gas use
    lpg_state = cons.calc_lpg_state(census_data)
    
    energy_state = pd.concat(
            [diesel_state, ng_state, elect_state, lpg_state], axis=0,
            ignore_index=True
            )
    
    # Calculate GDP multiplier
    multiplier = cons.calc_bea_multiplier()
    
    # Calculate county fraction of state construction establishments by 
    # NAICS code.
    county_frac = cons.calc_county_fraction(cbp_2012)
    
    # Format state energy data.
    
    # Calculate county energy 
    cons_energy = cons.calc_county_energy(
       energy_state, county_frac, multiplier,
       calculation_years=range(2010, 2017)
       )
    

    
