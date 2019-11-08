# Industry Energy Data Book (IEDB)
The 2018 Industry Energy Data Book summarizes the status of, and
identifies the key trends in energy use and its underlying economic drivers
across the four industrial subsectors: agriculture, construction, manufacturing,
and mining. In addition to aggregating and visualizing industrial data from
across multiple sectors, the IEDB also provides annual estimates of
combustion energy use for large energy-using facilities and county-level
industrial energy use. The landing page for the IEDB is here: (tbd)

This repository contains the source code for estimating combustion energy
for large energy users and for estimating industrial energy use at the county
level. Both data sets are available for download from the
(NREL Data Catalog)[https://data.nrel.gov/].

These estimates are a small part of the effort to improve the resolution and
timeliness of publicly-available industrial energy data for the United States.
The source code provided here is meant to be improved over time with the help
of the developer and energy analyst communities.   

## Combustion Energy Estimates of Large Energy Users
Annual combustion energy is estimated from 2010 - 2017 for all industrial
facilities reporting combustion emissions under the (U.S. EPA's Greenhouse Gas
Reporting Program (GHGRP))[https://www.epa.gov/ghgreporting]. The basic approach
is to back out combustion energy use from reported GHG emissions. The calculation
methodology is an evolution of the approach first outlined by
(McMillan et al. (2016))[https://doi.org/10.2172/1335587] and
subsequently refined by (McMillan and Ruth (2018))[https://doi.org/10.1016/j.apenergy.2019.01.077].
Note that because the GHGRP does not track estimates from purchased electricity,
electricity use is not estimated in this data set.

The most notable improvement in the methodology is using calculation
tier-specific information (e.g., facility-reported higher heating values [HHV]
and emission factors) instead of relying solely on EPA default emission factors.
Methods have also been introduced to estimate the uncertainty of calculations,
as discussed in McMillan and Ruth (2019).

## County-Level Estimates of Industrial Energy
Annual energy use for the industrial sector (agriculture, construction, mining,
and manufacturing) for 2010 - 2016 at the county level. The combustion energy
estimates of large energy users provide the foundation of the county-level
estimates. Energy use of industrial facilities that do not report to the GHGRP
are estimated using data sources from the Energy Information Administration,
Census Bureau, and the U.S. Department of Agriculture. The methodology was first
described by (McMillan and Narwade (2018))[https://doi.org/10.2172/1484348],
with the associated source code provided here: https//github.com/NREL/Industry-Energy-Tool/.
