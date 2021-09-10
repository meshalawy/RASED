# %%
#
# Author: Mashaal Musleh (musle005@umn.edu)
# Created on Sat Jun 12 2021
#
#



import requests 
import re
from datetime import date, datetime, timedelta

import geopandas
from geopandas.tools import sjoin
from shapely.geometry import Point

import xml.etree.ElementTree as ET
import gzip
import pandas as pd 
import glob, os

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pathlib import Path
from tqdm.contrib.concurrent import thread_map
from concurrent.futures import ThreadPoolExecutor





class OSM_Chagneset_Analysis:
    FORMAT="%Y-%m-%d"
    #date_str: YYYY-MM-DD
    
    def __init__(self, date_str):
        self.date_str = date_str
        self.diff_folder = f'diff_{date_str}'
        self.changesets_folder = f'changesets_{date_str}'

        # setting up downlnoad agent for retry
        self.session = requests.Session()

        retries = Retry(total=50,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 502, 503, 504 ])

        self.session.mount('http://', HTTPAdapter(max_retries=retries))

    
    
    # approximation to find changeset files and diff files for the given day
    # margin day before and day after. 
    def get_changeset_range(self):
        date_obj = datetime.strptime(self.date_str, self.FORMAT)
        start_date_str = (date_obj + timedelta(days=-1)).strftime(self.FORMAT)
        end_date_str = (date_obj + timedelta(days=1)).strftime(self.FORMAT)
        changeset_files = []
        response = self.session.get('https://planet.openstreetmap.org/replication/changesets/').text
        lines = re.findall(r'(.*alt="\[DIR\]".*)', response)
        for line in lines[-1::-1]:
            last_modified = re.findall(r'a>\s*(.{10})', line)[0]
            if last_modified >= start_date_str:
                level1_dir = re.findall(r'href="(.*?)">',line)[0][:-1]
                response2 = self.session.get('https://planet.openstreetmap.org/replication/changesets/' + level1_dir).text
                lines2 = re.findall(r'(.*alt="\[DIR\]".*)', response2)
                for line2 in lines2:
                    last_modified = re.findall(r'a>\s*(.{10})', line2)[0]
                    level2_dir = re.findall(r'href="(.*?)">',line2)[0][:-1]
                    if last_modified >= start_date_str and last_modified <= end_date_str:
                        changeset_files.append(level1_dir + level2_dir)

        
        if not changeset_files:
           raise RuntimeError('No OSM diff files for this date yet.')


        return range(int(min(changeset_files)+"000"), int(max(changeset_files)+"999")+1)
        

    
    def get_diff_range(self):
        # based on the logic of OSM dumps, one file per day, since 2012-09-12, increamenting file name by one.
        date_obj = datetime.strptime(self.date_str, self.FORMAT)
        diff_file = (date_obj - datetime.strptime("2012-09-11", self.FORMAT)).days
        return range(diff_file, diff_file+1)


    

    def download(self, args):
        url, file = args
        # try:
        while not Path(file).exists() or Path(file).stat().st_size == 0:
            open(file, 'wb+').write(self.session.get(url).content)
        # except:
            # return args
        
    

    def download_diff_files(self):
        self.clear_downloaded_data(diff=True, create_dirs=True)
        diff = self.get_diff_range()
        urls = []
        for i in diff:
            path = f'{i:011,}'.replace(',', '/')
            url = f'https://planet.openstreetmap.org/replication/day/{path}.osc.gz'
            file = f'{self.diff_folder}/{i}.osc.gz'
            urls.append((url, file))

        with ThreadPoolExecutor(4) as pool:
            pool.map(self.download, urls)

    
    def download_changeset_files(self):
        self.clear_downloaded_data(changesets=True, create_dirs=True)
        changesets = self.get_changeset_range()
        urls = []
        for i in changesets:
            path = f'{i:011,}'.replace(',', '/')
            url = f'https://planet.openstreetmap.org/replication/changesets/{path}.osm.gz'
            file = f'{self.changesets_folder}/{i}.osm.gz'
            urls.append((url, file))
        
        with ThreadPoolExecutor(4) as pool:
            pool.map(self.download, urls)

        


    def process_diff_files(self):

        def process_single_diff_file(f):

            def iter_diffs(xml):
                operations = ['modify', 'delete', 'create']
                for op in operations:
                    for diffs in xml.iter(op):
                        for element in diffs:        
                            dict = element.attrib.copy()
                            dict['operation'] = op
                            dict['element'] = element.tag
                            # for t in element.iter('tag'):
                            #     if t.attrib['k'] in ['highway', 'restriction', 'junction']:
                            #         dict['tag:' + t.attrib['k']] = t.attrib['v']

                            tags = {t.attrib['k'] : t.attrib['v'] for t in element.iter('tag')}
                            # tags, values = zip(*[(t.attrib['k'],t.attrib['v']) for t in element.iter('tag')])
                            if {'highway', 'restriction', 'junction'}.intersection(tags.keys()):
                                dict['tags_keys'] = list(tags.keys())
                                dict['tags_values'] = list(tags.values())

                                yield dict

            xml = ET.parse(gzip.open(f,'rt')).getroot()
            df = pd.DataFrame(list(iter_diffs(xml)))

            numeric_fields = ['id','version','uid','changeset','lat','lon']
            categorical_fields = ['element', 'operation'] + [col for col in df.columns if col.startswith('tag:')]
            for field in numeric_fields:
                df[field] = pd.to_numeric(df[field])

            for field in categorical_fields:
                df[field] = df[field].astype('category')
            
            return df

        files = glob.glob(f"{self.diff_folder}/*.osc.gz")
        dfs = []

        dfs = thread_map(process_single_diff_file, files, max_workers=6)
        diff_df = pd.concat(dfs,ignore_index=True)
        return diff_df



    def process_changesets_files(self):

        def process_single_changesets_file(f):
            
            def iter_changesets(xml):
                for cs in xml.iter('changeset'):
                    yield cs.attrib.copy()

            try:
                xml = ET.parse(gzip.open(f,'rt')).getroot()
                df = pd.DataFrame(list(iter_changesets(xml)))
                if (len(df)):
                    numeric_fields = ['id','min_lat','max_lat','min_lon','max_lon']
                    for field in numeric_fields:
                        df[field] = pd.to_numeric(df[field])
                return df
            except Exception as e:
                print(e)
                return None


        files = glob.glob(f"{self.changesets_folder}/*.osm.gz")
        dfs = []
        dfs = thread_map(process_single_changesets_file, files, max_workers=8)
        changesets_df = pd.concat(dfs,ignore_index=True).drop_duplicates(subset='id').dropna(subset=['min_lon','min_lat','max_lon','max_lat'], how='any')
        return changesets_df



    # find location for data with no location present(way+relation) by approximately taking the centroid of the MBR available in changeset metadadata:
    def assign_locations(self, diff_df, changesets_df):
        with_location = diff_df[~pd.isna(diff_df.lat)]
        no_location = diff_df[pd.isna(diff_df.lat)]
        # no_location['copy_index'] = no_location.index
        no_location = pd.merge(left=no_location, left_on='changeset', right=changesets_df[['id','min_lat','max_lat','min_lon','max_lon']], right_on='id', how='inner')
        no_location['lat'] = no_location[['min_lat','max_lat']].mean(axis=1)
        no_location['lon'] = no_location[['min_lon','max_lon']].mean(axis=1)
        no_location.drop(['id_y','min_lat','max_lat','min_lon','max_lon'], axis=1, inplace=True)
        no_location.rename(columns={'id_x':'id'}, inplace=True)

        # data = pd.merge(left=diff_df, left_on='changeset', right=changesets_df, right_on='id', how='inner')
        data_df = pd.concat([with_location, no_location], ignore_index=True)
        for field in ['lat','lon']:
            data_df[field] = pd.to_numeric(data_df[field])

        return data_df


    # Constructing GeoDataFrame
    # GeoDataFrame is faster to query by bounding box.
    def create_geodataframe(self, data_df):
        geom = data_df[['lon','lat']].apply(lambda p: Point(*(p.values)), axis=1)
        data_gdf = geopandas.GeoDataFrame(data_df, geometry=geom).set_crs(4326)
        data_gdf.sindex
        return data_gdf

    
    def assign_countries(self, data_gdf):
        countries = geopandas.read_file('misc/UIA_World_Countries_Boundaries_with_ISO3/World_Countries__Generalized_.shp')
        countries.drop([col for col in countries.columns if col not in ['COUNTRYAFF','geometry']], axis = 1, inplace=True)
        countries.sindex

        joined = sjoin(data_gdf, countries, how='left').drop('index_right', axis=1)
        data_gdf['country'] = joined['COUNTRYAFF']

        states = geopandas.read_file('misc/us-states.json')
        states = geopandas.GeoDataFrame(states.name, geometry=states.geometry)
        states.sindex

        us_only = geopandas.GeoDataFrame(data_gdf[data_gdf.country=='United States'], crs=4326)
        joined = sjoin(us_only, states, how='left').drop('index_right', axis=1)
        data_gdf['state'] = data_gdf['country']
        data_gdf.loc[joined.index, 'state'] = joined['name']

        data_gdf['state'] = data_gdf['state'].astype(object)
        data_gdf.loc[pd.isna(data_gdf['state']), 'state'] = data_gdf.loc[pd.isna(data_gdf['state']), 'country'].astype(object)
        data_gdf['state'] = data_gdf['state'].astype('category')
        data_gdf['country'] = data_gdf['country'].astype('category')

        return data_gdf


    def clear_downloaded_data(self, diff=False, changesets=False, create_dirs=False):
        if diff:
            Path(self.diff_folder).mkdir(exist_ok=True)
            with ThreadPoolExecutor() as pool:
                pool.map(os.remove, glob.glob(f'{self.diff_folder}/*'))
            
            if not create_dirs:   
                Path(self.diff_folder).rmdir()
        
        if changesets:
            Path(self.changesets_folder).mkdir(exist_ok=True)
            with ThreadPoolExecutor() as pool:
                pool.map(os.remove, glob.glob(f'{self.changesets_folder}/*'))
            
            if not create_dirs:   
                Path(self.changesets_folder).rmdir()

                

