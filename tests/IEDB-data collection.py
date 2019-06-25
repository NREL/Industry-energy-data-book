import urllib
import json
from pandas.io.json import json_normalize

#api_key = 8c7aa740538295da92172263382e2a16
#Total consumption : natural gas : United States : all industrial (total) : annual

url = 'http://api.eia.gov/series/?api_key=8c7aa740538295da92172263382e2a16&series_id=ELEC.CONS_TOT.NG-US-97.A'
response = urllib.request.urlopen(url)
info = response.read()
data = json.loads(info)

# The TypeError that was thrown said something about 'list indices must be
# list integers or slices, not str'. It looks like data['series'] is
# returning a list:
print(type(data['series']))

# There's only one entry in the list:
print(len(data['series']))

# Lists don't have a 'value' attributes, so you can't use JSON
print(json_normalize(data['series'][0]['data']))

# So, just return the list:
print(data['series'][0]['data'])

# Or, make make it into a Pandas DataFrame:
import pandas as pd

df = pd.DataFrame(data['series'][0]['data'], columns=['year', 'energy'])
