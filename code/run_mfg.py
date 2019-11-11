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
import dask.dataframe as dd
import os


def Manufacturing(calculation_years=range(2010, 2017)): #CBP data only through 2016; 2017 scheduled for Nov 2019

    energy_ghgrp = pd.read_parquet(
            '../results/ghgrp_energy_20191108-1416.parquet',
            engine='pyarrow'
            )

    for y in calculation_years:

        print(y)

        print("getting cbp")
        cbp = get_cbp.CBP(y)

        cm = county_matching.County_matching(y)

        print("matching ghgrp")
        ghgrp_matching = cm.format_ghgrp(energy_ghgrp, cbp.cbp_matching)

        # Instantiate class for a single year
        cmfg = Calculate_MfgEnergy_IEDB.Manufacturing_energy(y, energy_ghgrp)

        # update NAICS codes for energy_ghgrp based on ghgrp_matching
        print("updating naics")
        cmfg.update_naics(ghgrp_matching)

        # Separate process for combustion fuels
        print("counting ghgrps")
        cbp_matching_counts = cm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)
        # cbp.cbp_matching = cm.ghgrp_counts(cbp.cbp_matching, ghgrp_matching)

        print("correcting naics")
        # cbp_corrected = cm.correct_cbp(cbp.cbp_matching)
        cbp_corrected = cm.correct_cbp(cbp_matching_counts)

        print("cbp_matching cols, ", cbp.cbp_matching.columns)

        #Export cbp_corrected for 2012 for mining
        if y == 2012:

            cbp_corrected[(cbp_corrected.naics > 210000) & \
                          (cbp_corrected.naics < 220000)][
                                 ['fipstate', 'fipscty', 'naics', 'est',
                                  'COUNTY_FIPS']
                                 ].to_csv(
                '../calculation_data/cbp_corrected_mining.csv'
                )

        # Run IPF only for MECS years, 2010 and 2014
        if (y == 2010) | (y == 2014):

            seed_methods = ipf_seed.IPF_seed(year=y)

            seed_df = seed_methods.create_seed(cbp_matching_counts)

            ipf_methods = ipf.IPF(y, table3_2=seed_methods.table3_2,
                               table3_3=seed_methods.table3_3)

            # Run IPF. Saves resulting energy values as csv
            ipf_methods.mecs_ipf(seed_df)

            mecs_intensities = cmfg.calc_intensities(cbp_matching_counts)

            mecs_intensities.to_pickle('mecs_intensities.pkl')

        else:

            mecs_intensities = pd.read_pickle('mecs_intensities.pkl')

        # Calculates non-ghgrp combustion energy use and combines with
        # ghgrp energy use. Distinguishes between data sources with 'data_source'
        # column.
        # This is a dask dataframe partitioned by STATE
        print("calculating combustion")
        mfg_comb_energy = cmfg.combfuel_calc(cbp_corrected, mecs_intensities)

        mfg_comb_energy['year'] = y

        #EIA electricity data; dask dataframe, partitioned by STATE
        print("calculating electricity")
        ghgrp_electricity, elect_fac_ids = cmfg.GHGRP_electricity_calc()

        # GHGRP matching for EIA electricity data
        ghgrp_matching_923 = pd.DataFrame(
            ghgrp_matching[ghgrp_matching.FACILITY_ID.isin(
                elect_fac_ids
                )]
            )

        print('cbp_matching: ', cbp.cbp_matching.columns)

        cbp_matching_923 = cm.ghgrp_counts(cbp.cbp_matching,
                                           ghgrp_matching_923)

        cbp_corrected_923 = cm.correct_cbp(cbp_matching_923)

        #estimate non-ghgrp electricity use. Dask dataframe partitioned by STATE
        mfg_elect_energy = cmfg.electricity_calc(cbp_corrected_923,
                                                  mecs_intensities)

        mfg_elect_energy['year'] = y

        if y == calculation_years[0]:

            mfg_energy = dd.multi.concat(
                    [mfg_comb_energy, mfg_elect_energy, ghgrp_electricity],
                    axis=0, join='outer', interleave_partitions=True
                     )

#            mfg_energy = mfg_energy.append(ghgrp_electricity,
#                                           interleave_partions=True)

        else:

            mfg_energy = dd.multi.concat(
                    [mfg_energy, mfg_comb_energy, mfg_elect_energy,
                     ghgrp_electricity], axis=0, join='outer',
                     interleave_partitions=True
                     )
#
#            mfg_energy = mfg_energy.append(mfg_elect_energy,
#                                           interleave_partitions=True)
#
#            mfg_energy = mfg_energy.append(ghgrp_electricity,
#                                           interleave_partitions=True)
#
#    mfg_energy = mfg_energy.calculate()[0]

    return mfg_energy
