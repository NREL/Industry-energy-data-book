
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

        fuel_file = \
            'Y:/6A20/Public/IEDB/Code/output/county_summary_fuels.csv'

        sector_file = \
            'Y:/6A20/Public/IEDB/Code/output/' +\
            'county_summary_sector_largest_sector.xlsx'

        #import energy results
        self.energy = pd.read_csv(fuel_file, index_col=0)

        self.sector = pd.read_excel(sector_file, sheet_name='for_map')

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

    def make_county_choropleth(self, data_df, palette, filename, class_scheme,
                               scheme_kwds):
        """

        Class_scheme and scheme_kwds is scheme provided by mapclassify
        (e.g. ‘box_plot’, ‘equal_interval’, ‘fisher_jenks’, etc.). See
        https://pysal.readthedocs.io/en/1.5/library/esda/mapclassify.html for
        more details on both schemes and their keywords.
        """
        def format_county_fips(cf):

            cf = str(int(cf))

            if len(cf)<=4:

                cf = '0'+cf

            return cf

        data = data_df.copy(deep=True)

        data['COUNTY_FIPS'] = data.COUNTY_FIPS.apply(
            lambda x: format_county_fips(x)
            )

        # match on geo_id
        map_data = self.cshp.set_index('GEOID').join(
            data.set_index('COUNTY_FIPS').MMBtu_TOTAL
            )

        map_data.dropna(subset=['MMBtu_TOTAL'], inplace=True)

        # set the range for the choropleth
        vmin, vmax = map_data.MMBtu_TOTAL.min(), map_data.MMBtu_TOTAL.max()

        # create figure and axes for Matplotlib
        fig, ax = plt.subplots(1, figsize=(10, 10))

        # divider = make_axes_locatable(ax)
        #
        # cax = divider.append_axes("right", size="5%", pad=0.1)

        if scheme_kwds == None:

            map_data.plot(column='MMBtu_TOTAL', cmap=palette, linewidth=0.8,
                          ax=ax, edgecolor='0.8', scheme=class_scheme,
                          legend=True,
                          legend_kwds={'title': 'Energy Use (MMBtu)'})
        else:

            map_data.plot(column='MMBtu_TOTAL', cmap=palette, linewidth=0.8,
                          ax=ax, edgecolor='0.8', scheme=class_scheme,
                          classification_kwds=scheme_kwds, legend=True,
                          legend_kwds={'title': 'Energy Use (MMBtu)'})

        ax.axis('off')

        # Create colorbar for legened
        # sm = plt.cm.ScalarMappable(
        #     cmap=palette, norm=plt.Normalize(vmin=vmin, vmax=vmax)
        #     )
        #
        # sm._A = []
        #
        # cbar = fig.colorbar(sm)
        #
        # cbar.ax.set_ylabel('Energy Use (MMBtu)')

        fig.savefig(filename+'.svg', dpi=500, bbox_inches='tight')

        plt.close()

    def make_sector_map(self, data):
        """
        Method for creating county map indicating largest energy-using
        subsector.
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

        ## Need to specify colors or will geopandas automatcially assign?

# mapping = mapping()
#
# # Make % change map (2010 - 2016)
# pct_ch = mapping.energy.groupby(
#     ['COUNTY_FIPS', 'year'], as_index=False
#     ).MMBtu_TOTAL.sum()
#
# pct_ch = pct_ch[pct_ch.year.isin([2010, 2016])]
#
# pct_ch = pct_ch.groupby('COUNTY_FIPS').apply(
#     lambda x: x.pct_change()
#     ).dropna()
#
# pct_ch.drop(['COUNTY_FIPS', 'year'], axis=1, inplace=True)
#
# pct_ch.reset_index('COUNTY_FIPS', inplace=True)
#
# mapping.make_county_choropleth(pct_ch, palette='GnBu',
#                                filename='county_pct_change',
#                                class_scheme='fisherjenks', scheme_kwds=None)
#
# Make total energy map
mapping.make_county_choropleth(
    mapping.energy[mapping.energy.year==2016].groupby(
        'COUNTY_FIPS', as_index=False).MMBtu_TOTAL.sum(), palette='Blues',
    filename='county_total_2016',class_scheme='fisherjenks',scheme_kwds={'k':7}
    )

# Make fuel maps
fuel_maps = {'Coal':'Greys', 'Natural_gas':'bone_r',
             'Net_electricity': 'plasma', 'Other': 'YlOrRd'}

for fuel, palette in fuel_maps.items():

    map_data = mapping.energy[
        (mapping.energy.MECS_FT==fuel) &
        (mapping.energy.year==2016)
        ]

    mapping.make_county_choropleth(
        map_data, palette=palette, filename='county_'+fuel+'_2016',
        class_scheme='fisherjenks', scheme_kwds={'k':7}
        )

# Colors Accent, Accent_r, Blues, Blues_r, BrBG, BrBG_r, BuGn, BuGn_r, BuPu,
#BuPu_r, CMRmap, CMRmap_r, Dark2, Dark2_r, GnBu, GnBu_r, Greens, Greens_r,
#Greys, Greys_r, OrRd, OrRd_r, Oranges, Oranges_r, PRGn, PRGn_r, Paired,
#Paired_r, Pastel1, Pastel1_r, Pastel2, Pastel2_r, PiYG, PiYG_r, PuBu, PuBuGn,
#PuBuGn_r, PuBu_r, PuOr, PuOr_r, PuRd, PuRd_r, Purples, Purples_r, RdBu,
#RdBu_r, RdGy, RdGy_r, RdPu, RdPu_r, RdYlBu, RdYlBu_r, RdYlGn, RdYlGn_r, Reds,
#Reds_r, Set1, Set1_r, Set2, Set2_r, Set3, Set3_r, Spectral, Spectral_r, Wistia,
#Wistia_r, YlGn, YlGnBu, YlGnBu_r, YlGn_r, YlOrBr, YlOrBr_r, YlOrRd, YlOrRd_r,
#afmhot, afmhot_r, autumn, autumn_r, binary, binary_r, bone, bone_r, brg, brg_r,
#bwr, bwr_r, cividis, cividis_r, cool, cool_r, coolwarm, coolwarm_r, copper,
#copper_r, cubehelix, cubehelix_r, flag, flag_r, gist_earth, gist_earth_r,
#gist_gray, gist_gray_r, gist_heat, gist_heat_r, gist_ncar, gist_ncar_r,
#gist_rainbow, gist_rainbow_r, gist_stern, gist_stern_r, gist_yarg, gist_yarg_r,
#gnuplot, gnuplot2, gnuplot2_r, gnuplot_r, gray, gray_r, hot, hot_r, hsv, hsv_r,
#icefire, icefire_r, inferno, inferno_r, jet, jet_r, magma, magma_r, mako,
#mako_r, nipy_spectral, nipy_spectral_r, ocean, ocean_r, pink, pink_r, plasma,
#plasma_r, prism, prism_r, rainbow, rainbow_r, rocket, rocket_r, seismic,
#seismic_r, spring, spring_r, summer, summer_r, tab10, tab10_r, tab20, tab20_r,
#tab20b, tab20b_r, tab20c, tab20c_r, terrain, terrain_r, twilight, twilight_r,
#twilight_shifted, twilight_shifted_r, viridis, viridis_r, vlag, vlag_r, winter,
#winter_r
