# -*- coding: utf-8 -*-
"""
Created on Tues July 16 2019

@author: cmcmilla
"""

import pandas as pd
import Calculate_MfgEnergy_IEDB
import mecs_ipf_IEDB as ipf
import get_cbp
import MECS_IPF_seed_format_IEDB as ipf_seed
import Match_GHGRP_County_IEDB as county_matching
import mecs_table5_2_formatting
import datetime as dt
import dask.dataframe as dd
import os
import sys

energy_ghgrp = pd.read_parquet(
        'c:/Users/cmcmilla/Solar-for-Industry-Process-Heat/'+\
        'Results analysis/ghgrp_energy_20190614-1056.parquet',engine='pyarrow'
        )

calculation_years = range(2010, 2018)
#%%

iedb_data = pd.DataFrame()

for y in calculation_years:

    cbp = get_cbp.CBP(y)

    tcm = county_matching.County_matching(y)

    ghgrp_matching = tcm.format_ghgrp(energy_ghgrp, cbp.cbp_matching)

    # Instantiate class for a single year
    tcmfg = Calculate_MfgEnergy_IEDB.Manufacturing_energy(y, energy_ghgrp)

    # update NAICS codes for energy_ghgrp based on ghgrp_matching
    tcmfg.update_naics(ghgrp_matching)

    #EIA electricity data
    ghgrp_electricity = tcmfg.GHGRP_electricity_calc()

    # GHGRP matching for EIA electricity data
    ghgrp_matching_923 = ghgrp_matching[ghgrp_matching.FACILITY_ID.isin(
            ghgrp_electricity.reset_index().FACILITY_ID
            )]

    cbp_matching_923 = tcm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching_923)

    cbp_corrected_923 = tcm.correct_cbp(cbp_matching_923)

    # Separate process for combustion fuels
    cbp.cbp_matching = tcm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)

    cbp_corrected = tcm.correct_cbp(cbp.cbp_matching)

    ghgrp_mecstotals = tcmfg.GHGRP_Totals_byMECS()

    seed_methods = ipf_seed.IPF_seed(year=y)

    seed_df = seed_methods.create_seed(cbp.cbp_matching)

    ipf_methods = ipf.IPF(y, table3_2=seed_methods.table3_2,
                       table3_3=seed_methods.table3_3)

    # Run IPF. Saves resulting energy values as csv
    ipf_methods.mecs_ipf(seed_df)

    mecs_intensities = tcmfg.calc_intensities(cbp.cbp_matching)
    #%%
    # Calculates non-ghgrp combustion energy use and combines with
    # ghgrp energy use. Distinguishes between data sources with 'data_source'
    # column.
    # This is a dask dataframe.
    mfg_comb_energy = tcmfg.combfuel_calc(cbp_corrected, mecs_intensities)

    #estimate non-ghgrp electricity use
    mfg_elect_energy = tcmfg.electricity_calc(cbp_corrected_923,
                                              mecs_intensities)


    enduse_methods = mecs_table5_2_formatting.table5_2(y)

    enduse_fraction = enduse_methods.calculate_eu_share()

    # Enduse breakdown without temperatures.
    # This returns a dask dataframe.
    mfg_energy_enduse = tcmfg.calc_enduse(enduse_fraction, mfg_energy,
                                          temps=False)

    # Save as parquet
    os.mkdir('../results/mfg_eu_'+dt.datetime.now().strftime('%Y%m%d_%H%M'))

    dd.to_parquet(
            mfg_energy_enduse,
            '../results/mfg_eu_'+dt.datetime.now().strftime('%Y%m%d_%H%M'),
            write_index=True, engine='pyarrow', compression='gzip'
            )

    # Enduse breakdown with temperatures; Returns only process heating end uses
    # with defined temperatures.
    # This returns a Pandas dataframe
    mfg_energy_enduse_temps = tcmfg.calc_enduse(enduse_fraction, mfg_energy,
                                                temps=True)

    # Save as parquet
    mfg_energy_enduse_temps.to_parquet(
            '../results/mfg_eu_temps_'+dt.datetime.now().strftime('%Y%m%d_%H%M')+\
            '.parquet.gzip', engine='pyarrow', compression='gzip'
            )
