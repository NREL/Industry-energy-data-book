# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 09:50:36 2019

@author: cmcmilla
"""

import pandas as pd
import numpy as np
import requests
import json

class LMDI:
    """
    Implements log mean divisia index (LMDI) decomposition approaches
    for energy intensity at the national or state level.
    See https://doi.org/10.1016/j.enpol.2003.10.010 for more information
    on LMDI.
    
    Years as a range.
    """
    
    def __init__(self, years, base_year=2010, state=False):
        
        self.years = [x for x in years]
        
        self.base_year = base_year
        
        self.state = state
        
        base_url = 'https://apps.bea.gov/api/data'
        
        with open('c:/Users/cmcmilla/bea_api.json') as jfile:
            
            api_key = json.load(jfile)
            
        api_key = api_key['bea']
        
        industries = [11, '111CA', '113FF', 21, 211, 212, 213, 23, '311FT',
                      '313TT', '315AL', 321, 322, 323, 324,
                      325, 326, 327, 331, 332, 333, 334, 335, '3361MV',
                      '3364OT', 337, 339]
        
        if self.state == False:
            
            # Get real gross output data from Bureau of Economic Analysis
            # Values in Billions of chained (2012) dollars
            activity_params = {'UserID': api_key, 'method': 'GetData',
                               'ResultFormat': 'JSON',
                               'datasetname': 'GDPbyIndustry', 'Frequency':'A',
                               'TableID': '208','Industry': industries,
                               'Year': years}
            
            self.energy_data = 

        else:
            
            activity_params = {'UserID': api_key, 'method': 'GetData',
                               'ResultFormat': 'JSON',
                               'datasetname': 'Regional', 'Frequency':'A',
                               'TableID': '208','Industry': industries,
                               'Year': years}
        
            self.energy_data = 
            
        # Get real gross output data from Bureau of Economic Analysis
        # Values in Billions of chained (2012) dollars
        self.activity_data = pd.DataFrame.from_records(
                requests.get(
                        base_url, params=activity_params
                        ).json()['BEAAPI']['Results']['Data'],
                columns=['DataValue', 'IndustrYDescription', 'Industry',
                         'Year']
                )
        
        
    def multiplicative(self, t):
        
        self.activity_data.set_index(['Industry', 'Year'], inplace=True)
        
        self.energy_data.set_index(['Industry', 'Year'], inplace=True)
        
        def L(a, b):
            """
            """
            output = (a-b)/(np.log(a)-np.log(b)) 
        
            return output
        
        
        e_t = self.energy_data.xs(t, level='Year')
    
        e_base = self.energy_data.xs(self.base_year, level='Year')
        
        d_act = np.exp(
                sum(
                    L(e_t, e_base).multiply(
                        L(e_t.sum(), e_base.sum())
                        )
                    ).multiply(
                        np.log(
                            self.activity_data.xs(t, level='Year').sum()
                            ).multiply(
                                    self.activity_data.xs(self.base_year,
                                                          level='Year').sum()
                                    )
                        )
                )

        d_str = np.exp(
                sum(
                    L(e_t, e_base).multiply(
                        L(e_t.sum(), e_base.sum())
                        ).multiply(
                            np.log(
                                self.activity_data.xs(t, level='Year')
                                ).multiply(
                                        self.activity_data.xs(self.base_year,
                                                              level='Year'))
                        )
                        )
                )

        d_int = np.exp(
                sum(
                    L(e_t, e_base).multiply(
                        L(e_t.sum(), e_base.sum())
                        ).multiply(
                            np.log(e_t.divide(
                                self.activity_data.xs(t, level='Year')
                                )).multiply(e_base.divide(
                                        self.activity_data.xs(self.base_year,
                                                              level='Year')
                                        ))
                            )
                )
            )
               
        ida_dict = {}
        
        ida_dict['activity'] = d_act
        
        ida_dict['structure'] = d_str
        
        ida_dict['intensity'] = d_int
        
        ida_dict['total'] = d_act*d_str*d_int
         
        return ida_dict
        

    def additive(self):
        """
        
        """
        self.activity_data.set_index(['Industry', 'Year'], inplace=True)
        
        self.energy_data.set_index(['Industry', 'Year'], inplace=True)        
        
        e_t = self.energy_data.xs(t, level='Year')
    
        e_base = self.energy_data.xs(self.base_year, level='Year')
        
        d_act = sum(
                e_t.subtract(e_base.multiply(np.log(e_t))).subtract(
                        np.log(e_base).multiply(np.log(
                                self.activity_data.xs(t, level='Year').sum()
                                )*self.activity_data.xs(self.base_year,
                                                        level='Year').sum()
                                )
                        )
                )


        d_str = sum(
                e_t.subtract(e_base.multiply(np.log(e_t))).subtract(
                        np.log(e_base).multiply(np.log(
                                self.activity_data.xs(t,level='Year')
                                ).multiply(
                                    self.activity_data.xs(self.base_year,
                                                              level='Year')
                                )
                        )
                )

        d_int = sum(
                e_t.subtract(e_base.multiply(np.log(e_t))).subtract(
                        np.log(e_base).multiply(np.log(e_t.divide(
                                self.activity_data.xs(t,level='Year')
                                )).multiply(e_base.divide(
                                    self.activity_data.xs(self.base_year,
                                                              level='Year')
                                ))
                        )
                )
        
        ida_dict = {}
        
        ida_dict['activity'] = d_act
        
        ida_dict['structure'] = d_str
        
        ida_dict['intensity'] = d_int
        
        ida_dict['total'] = d_act+d_str+d_int
         
        return ida_dict
        
    def calculate(form):
        
        