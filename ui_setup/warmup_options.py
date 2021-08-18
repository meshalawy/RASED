#%%

countries = geopandas.read_file('/export/scratch/mmusleh/osm/osm_change_analysis/UIA_World_Countries_Boundaries_with_ISO3/World_Countries__Generalized_.shp')
countries = countries[['COUNTRYAFF', 'ISO3', 'geometry']].dissolve(by='COUNTRYAFF')
us_states_gdf = geopandas.GeoDataFrame.from_file('us-states.json').dissolve(by='name')

country_objects = {}
country_objects['All'] = {
    'name':'All',
    'dataframe_filter': None,
    'postgis_filter': None,
    'bounds':((-90, -180), (90, 180))} 

for name,geometry in countries.geometry.iteritems():
    west,south,east,north = geometry.bounds
    bounds=((south, west), (north, east))
    postgis_filter = f'SRID=4326;{geometry.to_wkt()}'
    country_objects[name] = {
        'name':name,
        'dataframe_filter':name,
        'postgis_filter': postgis_filter,
        'bounds':((south, west), (north, east))
    } 
    


state_objects = {}
state_objects['All'] = {
    'name':'All',
    'dataframe_filter': None,
    'postgis_filter': country_objects['United States']['postgis_filter'],
    'bounds': country_objects['United States']['bounds']
} 

for name,geometry in us_states_gdf.geometry.iteritems():
    west,south,east,north = geometry.bounds
    bounds=((south, west), (north, east))
    postgis_filter = f'SRID=4326;{geometry.to_wkt()}'
    state_objects[name] = {
        'name':name,
        'dataframe_filter':name,
        'postgis_filter': postgis_filter,
        'bounds':((south, west), (north, east))
    } 


location_group_options = {
    'All': {'name': 'All',  'countries':tuple(countries.index)},
    'US': {'name': 'US', 'countries':('United States',)},
    'South America': {'name': 'South America', 'countries': tuple(pd.read_csv('countries/south_america.csv').Country) },
    'Europe': {'name': 'Europe', 'countries': tuple(pd.read_csv('countries/europe.csv').Country)},
    'Africa': {'name': 'Africa', 'countries': tuple(pd.read_csv('countries/africa.csv').Country)},
    'Middle East': {'name': 'Middle East', 'countries': tuple(pd.read_csv('countries/middle_east.csv').Country)},
    'GCC': {'name': 'GCC', 'countries': tuple(pd.read_csv('countries/gcc.csv').Country)},
    'Asia': {'name': 'Asia', 'countries': tuple(pd.read_csv('countries/asia.csv').Country)}
}


import pickle
pickle.dump((countries, us_states_gdf, country_objects, state_objects, location_group_options), open('warmup_options.pkl', 'wb'))
# %%
