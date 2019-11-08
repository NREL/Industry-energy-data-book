# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 14:26:54 2019

@author: cmcmilla
"""

import datetime as dt
from calc_GHGRP_energy_IEDB import GHGRP
from calc_GHGRP_AA_IEDB import subpartAA



ghgrp = GHGRP((2010, 2017), calc_uncertainty=False)

ghgrp_data = {}

for k in ghgrp.table_dict.keys():

    ghgrp_data[k] = ghgrp.import_data(k)

energy_subC = ghgrp.calc_energy_subC(ghgrp_data['subpartC'],
                                     ghgrp_data['subpartV_fac'])

energy_subD = ghgrp.calc_energy_subD(ghgrp_data['subpartD'],
                                     ghgrp_data['subpartV_fac'])

energy_subAA = subpartAA(aa_ff=ghgrp_data['subpartAA_ff'],
                         aa_sl=ghgrp_data['subpartAA_liq'],
                         std_efs=ghgrp.std_efs).energy_calc()

energy_ghgrp = ghgrp.energy_merge(energy_subC, energy_subD, energy_subAA,
                                  ghgrp_data['subpartV_fac'])

time = dt.datetime.today().strftime("%Y%m%d-%H%M")

# Save results
energy_ghgrp.to_parquet(
        "../Results analysis/ghgrp_energy_" + time + ".parquet",
        engine='pyarrow', compression='gzip'
        )
