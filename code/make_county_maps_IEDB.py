
# conda env geo_env
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import requests
import zipfile
from io import BytesIO
from mpl_toolkits.axes_grid1 import make_axes_locatable
import re

class mapping:

    def __init__(self):

        cshp_file = \
            'Y:/6A20/Public/IEDB/cb_2014_us_county_20m/cb_2014_us_county_20m.shp'

        sshp_file = \
            'Y:/6A20/Public/IEDB/cb_2014_us_state_5m/cb_2014_us_state_5m.shp'

        self.cshp = gpd.read_file(cshp_file)

        self.sshp = gpd.read_file(sshp_file)

        #Convert to Mercator
        self.cshp = self.cshp.to_crs(epsg=3395)
        self.sshp = self.sshp.to_crs(epsg=3395)

        cb_url = 'http://colorbrewer2.org/export/colorbrewer.json'

        cb_r = requests.get(cb_url)

        self.colors = cb_r.json()

        #  to convert to HEX
        for c in self.colors.keys():

            for n in self.colors[c].keys():

                rgb_list = self.colors[c][n]

                if type(rgb_list) == str:

                    continue

                hex_list = []

                for v in rgb_list:

                    hex = tuple(
                        int(x) for x in re.search(
                            '([^a-z,(,](\w+,\w+,\w+)|(\w,\w+,\w+))', v
                            ).group().split(',')
                        )

                    hex_list.append(hex)

                self.colors[c][n] = hex_list

    def make_county_choropleth(data, palette, filename, class_scheme,
                               scheme_kwds):
        """

        """
        def format_county_fips(cf):

            cf = str(cf)

            if len(cf)<=4:

                cf = '0'+cf

            return cf

        data['COUNTY_FIPS'] = data.COUNTY_FIPS.apply(
            lambda x: format_county_fips(x)
            )

        # match on geo_id
        map_data = self.cshp.set_index('GEOID').join(
            data.set_index('COUNTY_FIPS').MMBtu
            )

        map_data.dropna(subset=['MMBtu'], inplace=True)

        # set the range for the choropleth
        vmin, vmax = map_data.MMBtu.min(), map_data.MMBtu.max()

        # create figure and axes for Matplotlib
        fig, ax = plt.subplots(1, figsize=(10, 10))

        # divider = make_axes_locatable(ax)
        #
        # cax = divider.append_axes("right", size="5%", pad=0.1)

        map_data2.plot(column='MMBtu', cmap='Blues', linewidth=0.8, ax=ax,
                       edgecolor='0.8', scheme=class_scheme,
                       scheme_kwds=scheme_kwds, legend=True,
                       legend_kwds={'title': 'Energy Use (MMBtu)'})

        ax.axis('off')

        # # Create colorbar for legened
        # sm = plt.cm.ScalarMappable(
        #     cmap='Blues', norm=plt.Normalize(vmin=vmin, vmax=vmax)
        #     )
        #
        # sm._A = []
        #
        # cbar = fig.colorbar(sm)
        #
        # cbar.ax.set_ylabel('Energy Use (MMBtu)')

        fig.savefig(filename+'_.svg', dpi=500, bbox_inches='tight')
        # Create colorbar as a legend
        #sm = plt.cm.ScalarMappable(
            #cmap=’Blues’, norm=plt.Normalize(vmin=vmin, vmax=vmax)
            #)
        # empty array for the data range
        #sm._A = []
        # add the colorbar to the figure
        #cbar = fig.colorbar(sm)

        #fig.savefig(“map_export.svg”, dpi=400)
