#%%
import pandas as pd
import numpy as np
import glob
from tqdm.contrib.concurrent import process_map

import geopandas
from shapely.geometry import Point
from sqlalchemy import create_engine



def find_highway_type(data):
    keys = data['tags_keys']
    values = data['tags_values']

    if 'highway' in keys:
        return values[keys.index('highway')]
    elif 'restriction' in keys:
        return 'restriction:' + values[keys.index('restriction')]
    elif 'junction' in keys:
        return 'junction:' + values[keys.index('junction')]


def do_aggregation(df):
    df = df.pivot_table(index=['day','road_type'],columns=['country','state', 'element','operation'],values='id',aggfunc='count')
    return df.replace(0,np.nan).dropna(axis=1,how="all")


def do_aggregation_db(df):
    return pd.get_dummies(df, columns=['element', 'operation']).groupby(by=['changeset','road_type']).agg({
        'day': lambda x: x.iloc[0],
        'country': lambda x: x.iloc[0],
        'state': lambda x: x.iloc[0],
        'element_node': any,
        'element_relation': any,
        'element_way': any,
        'operation_create': any,
        'operation_modify': any,
        'lat':lambda x: x.iloc[0],
        'lon':lambda x: x.iloc[0],
    }).reset_index(level=[0,1])

def save_to_db(df):
    gdf = geopandas.GeoDataFrame(df.drop(['lat','lon'], axis=1), geometry=  df[['lon','lat']].apply(lambda p: Point(*(p.values)), axis=1)).set_crs(4326)    
    engine = create_engine("postgresql://dmlab:postgisisfun@cs-spatial-314:5432/osm_changes")  
    gdf.to_postgis("changeset_ids", engine, if_exists='append')  
    return

def read_pkl_day_file(f):
    df = pd.read_pickle(f, compression='gzip')
    day = f[-19:-9]
    df['day'] = day
    df['road_type'] = df.apply(find_highway_type, axis = 1)
    
    aggregated_df = do_aggregation(df)
    aggregated_df.to_pickle(f'data/changes_aggregated/{day}.pkl.gzip', compression='gzip')
    db_aggregated_df = do_aggregation_db(df)
    save_to_db(db_aggregated_df)
    
    return aggregated_df

def aggregate(day):
    day = f'osm_map_changes_data/{day}.pkl.gzip'
    df = read_pkl_day_file(day)
    return df

# %%