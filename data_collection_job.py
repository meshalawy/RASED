# %%
from datetime import date, datetime, timedelta
from functools import partial
from multiprocessing import Manager
from pathlib import Path

from crawler import OSM_Chagneset_Analysis
from aggregator import aggregate
from tqdm.contrib.concurrent import process_map
import pandas as pd
import json 
import glob




def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# find missing days since last crawled day
status = json.load(open('status.json'))
last_day = status['last_day']
last_day = datetime.strptime(last_day, "%Y-%m-%d").date()
days =  list(daterange(last_day + timedelta(1), date.today() + timedelta(-1)))
days = [day.strftime("%Y-%m-%d") for day in days]


# adding any days left from the previous run in case of any unhandled exception
left_over =  [f[-10:] for f in glob.glob("diff_*")] + [f[-10:] for f in glob.glob("diff_*")]
days = sorted(list(set(days + left_over)))




if __name__ == "__main__":
    def execute_crawling_cleaning_aggregation_workflow(download_lock, all_df_lock, day):
        analayzer = OSM_Chagneset_Analysis(day)
        
        ######### crawling 

        # only limited number of processed downloading at a time
        download_lock.acquire()
        analayzer.download_diff_files() 
        analayzer.download_changeset_files()
        download_lock.release()

        ######### cleaning & preperation
        diff_df = analayzer.process_diff_files()
        changesets_df = analayzer.process_changesets_files()
        data = analayzer.assign_locations(diff_df, changesets_df)
        data = analayzer.create_geodataframe(data)
        data = analayzer.assign_countries(data)

        Path('osm_map_changes_data').mkdir(exist_ok=True)
        data.drop('geometry', axis=1).to_pickle(f'osm_map_changes_data/{day}.pkl.gzip', compression='gzip')


        ############ aggregation
        df = aggregate(day)


        ##### writing results back 

        # make sure no one else is writing the data
        all_df_lock.acquire()
        all = pd.read_pickle('data/changes_aggregated/all.pkl.gzip', compression='gzip')
        all = pd.concat([all, df]).sort_index(level='day')
        all.to_pickle('data/changes_aggregated/all.pkl.gzip', compression='gzip')

        # updat the status of the last availabe day, only if downloaded day is graater than existing days.
        status = json.load(open('status.json'))
        last_day = status['last_day']
        if day > last_day:
            status['last_day'] = day
            json.dump(status, open('status.json', 'w'))
        all_df_lock.release()

        analayzer.clear_downloaded_data(diff=True, changesets=True)


    print(datetime.now())
    print('crawling: ', days)

    m = Manager()
    download_lock = m.Semaphore(4)
    all_df_lock = m.Lock()
    func = partial(execute_crawling_cleaning_aggregation_workflow, download_lock, all_df_lock)
    process_map(func,days,max_workers=20)
