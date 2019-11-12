

import pandas as pd
import dask.dataframe as dd
#import run_mfg
# import mfg calcs
# import cons, ag, mining calcs
# import combine_sectors


# For now just read in dask dataframes of calculated data. Code for
# agriculture, construction, and mining needs to be refactored into
# classes and methods that can then be called here.
# This is the version for the databook.
mfg_file = 'mfg_county_energy_20190809.parquet.gzip'

# # This is the updated version
# mfg_file = 'mfg_county_energy_20191111_2207.gzip'

ag_file = 'ag_county_energy_20190813_1604.parquet.gzip'

cons_file = 'cons_county_energy_20190813_1555.parquet.gzip'

mining_file = 'mining_county_energy_20190814_0006.parquet.gzip'

county_energy = dd.concat(
        [dd.read_parquet('../results/'+f, engine='pyarrow') for f in \
             [mfg_file, ag_file, cons_file, mining_file]],
        interleave_partitions=True
        ).compute()

def check_naics(naics):
    """
    Map 6-digit NAICS to description of 2-digit NAICS.
    """

    n2_dict = {11: 'Agriculture', 21: 'Mining', 23: 'Construction',
               31: 'Manufacturing', 32: 'Manufacturing',
               33: 'Manufacturing', 92: 'Manufacturing', 48: 'Manufacturing'}

    if type(naics) == str:

        n_out = 'Agriculture'

    else:

        n2 = int(str(naics)[0:2])

        n_out = n2_dict[n2]

    return n_out

county_energy['ind_sector'] = county_energy.NAICS.apply(
        lambda x: check_naics(x)
        )

# Some 2017 data hangning around in mining (and others?)
county_energy = county_energy[county_energy.year != 2017]

# Clean up columns
county_energy = county_energy[county_energy.index.notnull()]

county_energy.state.update(county_energy.state.dropna().drop_duplciates())

county_energy.drop(['MECS_NAICS', 'MECS_Region', 'STATE', 'data_source',
                    'est_count', 'fipscty', 'state_abbr'], axis=1, inplace=True)

county_energy.reset_index(inplace=True)

county_energy.columns = [x.upper() for x in county_energy.columns]

county_energy.set_index('FIPSTATE', inplace=True)

# Export for data catalog
county_energy.to_csv(
    '../results/county_energy_estimates_IEDB.gzip', compression='gzip',
    index=True, header=True
    )

# Sum for county totals by fuel
# county_energy.groupby(
#         ['year', 'COUNTY_FIPS', 'MECS_FT'], as_index=False
#         ).MMBtu_TOTAL.sum().to_csv('../results/county_summary_fuels.csv')
#
# # Sum for county totals by sector
# county_energy.groupby(
#         ['year', 'COUNTY_FIPS', 'ind_sector'], as_index=False
#         ).MMBtu_TOTAL.sum().to_csv('../results/county_summary_sector.csv')


## Set calulation years
#years = range(2010, 2018)

## Run energy calculations for each industrial subsector
#mfg_energy = run_mfg.Manufacturing(calculation_years=years)
#
#cons_energy = run_cons.Construction(calculation_years=years)
#
#mining_energy = run_mining.Mining(calculation_years=years)
#
#ag_energy = run_ag.Agriculture(calculation_years=years)
#
## Combine and format combine sectors
#county_energy = combine_sectors(mfg=mfg_energy, cons=cons_energy,
#                                mining=mining_energy, ag=ag_energy)


#def county_formatting():
#
#    # Match fuel types to MECS fuel types
#    df.replace({'COAL': 'Coal', 'NATURAL GAS': 'Natural_gas',
#                'CRUDE OIL': 'Other', 'Gasoline': 'Other', 'MISC': 'Other',
#                'RESIDUAL FUEL OIL': 'Residual_fuel_oil',
#                'ELECTRICITY': 'Net_electricity',
#                'DIESEL': 'Distillate_fuel_oil', 'LP GAS': 'LPG_HGL'},
#                inplace=True)
#
#    fuel_county.rename(columns={'NAICS': 'naics'}, inplace=True)
