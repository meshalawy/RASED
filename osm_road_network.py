#%%

from attr import dataclass
from ipywidgets.widgets.widget_string import Label
import panel as pn
import param
import pandas as pd
import geopandas

import pickle

from itertools import chain

from datetime import datetime, date, time, timedelta
from functools import partial

from ipyleaflet import Map, Marker, MarkerCluster, basemaps, basemap_to_tiles, SplitMapControl
from ipywidgets import HTML
from shapely import geometry

from shapely.geometry import box
from sqlalchemy import create_engine


import plotly.graph_objects as go

import glob
from tqdm.contrib.concurrent import process_map


class TypeCategorySelector(param.Parameterized):
    selected_types = param.List(precedence=-1)

    

    categories = pd.read_csv('ui_setup/categories.csv').groupby('category').agg({
        'type': list,
        'order': min
    }).sort_values(by='order')['type'].apply(list).to_dict()
    categories['Other'] = []

    
    category_params = [f"category_{i}" for i in range(len(categories))]

    # first one are selected by default
    # Not the best practice, but a quick to do the job and 
    # define variable number of params. still slightly safe 
    # because No external code is executed
    for i,p in enumerate(category_params):
        exec(f"{p} = param.Boolean(default={i<1})")

    selected_types = list(chain(*list(categories.values())[:1]))

    
    def set_all_possible_types(self, types):
        current = set(list(chain(*list(self.categories.values())[:])))
        self.categories['Other'] = set(types).difference(current)


    @param.depends(*category_params, watch=True)
    def update_list(self):
        selection = []        
        for i,param_name in enumerate(self.category_params):
            if self.__getattribute__(param_name):
                selection += list(self.categories.values())[i]
        
        if selection:
            self.selected_types = selection
        else:
            self.selected_types = list(chain(*list(self.categories.values())[:]))

        

    def view(self):
        return pn.Column(
            pn.Param(self.param, 
                widgets={
                    param_name: {'widget_type': pn.widgets.Toggle, 'name': category }
                             for param_name, category in zip(self.category_params, self.categories.keys())}
            )
        )

        


class Dashboard(param.Parameterized):
    
    

    start_date  = param.Date(default= date.today() - timedelta (days=8))
    end_date    = param.Date(default= date.today() - timedelta (days=1))

    # start_date  = param.Date(default= date(2019,1,1))
    # end_date    = param.Date(default= date(2021,6,30))


    categories  = TypeCategorySelector(name='Category Selection')
    as_percentage   = param.Boolean(default=False)

    # player timestamp
    player = pn.widgets.Player(start=0, end=8, value= 0,loop_policy= 'once',show_loop_controls= False,interval= 1000, width=500, sizing_mode='fixed')

    



    # pre processed options to avoid doing it on every request. Check the file "warmup_options.py" 
    countries, us_states_gdf, country_objects, state_objects, location_group_options = pickle.load(open('ui_setup/warmup_options.pkl', 'rb'))
        
    location_group = param.ObjectSelector(default=location_group_options['All'], objects=location_group_options)
    country     = param.ObjectSelector( objects=country_objects)

    elements= {
        'Way': 'way',
        'Relation': 'relation',
        'Node':'node'
    }
    operations= {
        'Create': 'create',
        'Modify': 'modify'
    }
    elements    = param.ListSelector(default=['way','relation','node'], objects=elements)
    operations  = param.ListSelector(default=['create', 'modify'], objects=operations)

    query_button = param.Action(lambda x: x.param.trigger('query_button'), label='Query Data')



    data = param.DataFrame(precedence=-1, default=pd.DataFrame(index=pd.MultiIndex(levels=[[],[]],codes=[[],[]],names=['day', 'type'])))
    query = param.DataFrame(precedence=-1, default=pd.DataFrame(columns=['Total']))
    tabulator = pn.widgets.Tabulator(pd.DataFrame(), selectable='checkbox', pagination='local', sizing_mode='stretch_width', 
        widths={
            'Type':200,
            'Total':150,
            'Ways Created': 140,
            'Ways Modified': 140,
            'Relations Created': 140,
            'Relations Modified': 140,
            'Nodes Created': 140,
            'Nodes Modified': 140,
        }
    )


    params_column = pn.Column()
    @param.depends('query_button', watch=True)
    def load_data(self):
        df = None
        with pn.param.set_values(self.params_column, loading=True):
            df = pd.read_pickle('data/changes_aggregated/all.pkl.gzip', compression='gzip')
            # TODO temporary fix make it type instead of road_type. Should be updated in the original data
            df.index.names = ['day', 'Type']
            
            idx = pd.IndexSlice
            s = self.start_date.strftime("%Y-%m-%d")
            e = self.end_date.strftime("%Y-%m-%d")
            df = df.loc[
                idx[s:e, self.categories.selected_types],
                idx[: , : , self.elements , self.operations]
            ]
        
            
        self.data = df.copy()


    
    @param.depends('data', 'location_group', watch=True)
    def get_query_results(self):

        idx = pd.IndexSlice
        
        query = self.data.loc[
            idx[:],
            idx[
                list(self.location_group['countries']) , 
            ]
        ]
        self.query = query
    

    def __init__(self, *args, **kwargs):  
        self.total_per_country = pd.read_pickle('data/total_per_country.pkl.gzip', compression='gzip')
    
        self.categories.set_all_possible_types(self.data.index.get_level_values(level=1))
        super().__init__(*args, **kwargs)


    leaflet_map = Map(center=(0, 0), zoom=2, scroll_wheel_zoom=True, layout={'height':'650px'} )
    markers_group = MarkerCluster()
    leaflet_map.add_layer(markers_group)    
    



    
    
    def params_view(self):
        self.params_column = pn.Column(
            pn.Param(self.param['start_date'], widgets={
                    'start_date': {
                        'widget_type': pn.widgets.DatePicker,
                        'start': date(2019,1,1),
                        'end': date(2021,6,28),
                    }
                }),
            pn.Param(self.param['end_date'], widgets={
                    'end_date': {
                        'widget_type': pn.widgets.DatePicker,
                        'start': date(2019,1,1),
                        'end': date(2021,6,28),
                    }
            }),
            pn.pane.Markdown("Data is currently available from 2019-01-01 to 2021-06-28", style={'color':'red'}),
            self.categories.view,
            pn.layout.VSpacer(),
            pn.widgets.StaticText(name='Elements', value=''),
            pn.Param(self.param['elements'], widgets={
                    'elements': pn.widgets.CheckButtonGroup
            }),
            pn.widgets.StaticText(name='Operations', value=''),
            pn.Param(self.param['operations'], widgets={
                    'operations': pn.widgets.CheckButtonGroup
            }),
            pn.Param(self.param['query_button'], widgets={
                    'query_button': {'widget_type': pn.widgets.Button, 'button_type': 'primary' }
            }),
            
        )
        return self.params_column

    def map_control_view(self):
        return pn.Row(
            pn.Param(
                self.param['location_group'], 
                widgets={'location_group':  pn.widgets.RadioButtonGroup},
                align = ('center', 'center')),
            pn.Row(
                self.player,
                self.player_info_view, 
                width=650,
                sizing_mode='fixed'
            )
        )
            

    
    @pn.depends('start_date', 'end_date', watch=True)
    def player_control_view(self):
        days = (self.end_date - self.start_date).days
        days = 0 if days < 0 else days
        self.player.end = days + 1
        

    @pn.depends('player.value')
    def player_info_view(self):
        text = ''
        if self.player.value :
            text = (self.start_date + timedelta(days=self.player.value -1)).strftime("%d-%b-%Y")
        else:
            text = 'All days'

        return pn.pane.Markdown("**Showing:**\n\n" + text, width=120, sizing_mode='fixed')

            



    @param.depends('location_group')
    def country_state_filter_view(self):
        print('updating drop down')
        
        import time

        name = ""
        if self.location_group['name'] == 'US' :    
            self.param.country.names = self.state_objects
            name = 'State' 
        else:
            new_list = {}
            west,south,east,north =  self.countries.loc[list(self.location_group['countries'])].total_bounds
            

            new_bounds = ((south, west), (north, east))
            t = time.process_time()
            new_list['All'] = {
                'country_name':'All',
                'dataframe_filter': None,
                'postgis_filter': self.location_group['postgis_filter'],
                'bounds':new_bounds
            }
            print('elapsed_time', time.process_time() - t)
            new_list.update({
                c: self.country_objects[c] for c in self.location_group['countries']
            })
                 
            self.param.country.names = new_list
            name = 'Country'

        
        self.param.country.objects = list(self.param.country.names.values())
        self.country = self.param.country.names['All']
        print(' done updating drop down')
        return pn.Param(
            self.param['country'],
            widgets={
                "country": {'name':name}
            }
        )



    @param.depends('query', 'tabulator.selection', 'as_percentage', 'player.value')
    def choropleth_map(self):
        print('choro')

        idx = pd.IndexSlice

        if self.player.value:
            day = self.start_date + timedelta(days = self.player.value -1) 
            day = day.strftime("%Y-%m-%d")
            player_day_filter = idx[day:day]
        else:
            player_day_filter = slice(None)
            
        if self.tabulator.selection:
            road_type_filter = self.tabulator.value.index[self.tabulator.selection].tolist()
            query = self.query.loc[(player_day_filter,road_type_filter),:]
        else:
            query = self.query.loc[player_day_filter]


        if self.as_percentage:
            query = query.groupby(level=[0,1,2],axis=1).sum().groupby(level=1).sum()
            query = query.loc[:, query.columns.intersection(self.total_per_country)]
            tpc = self.total_per_country.loc[query.index,query.columns].fillna(0)
            # query = (query/tpc.values * 100).fillna(0)

        geo_scope = None
        locationmode = None
        
        if self.location_group['name'] == 'US':
            query = query.groupby(level=1, axis=1).sum().sum()
            if self.as_percentage:
                tpc = tpc.groupby(level=1, axis=1).sum().sum()
                query = (query/tpc.values * 100).fillna(0)
            query.name = 'Total Updates'
            query = pd.merge(left=query, right=self.us_states_gdf, left_index=True, right_on='name')
            query['location_id'] = query['id']
            locationmode = 'USA-states'
            geo_scope = 'usa'
        else:
            query = query.groupby(level=0, axis=1).sum().sum()
            if self.as_percentage:
                tpc = tpc.groupby(level=0, axis=1).sum().sum()
                query = (query/tpc.values * 100).fillna(0)
            query.name = 'Total Updates'
            query = pd.merge(left=query, right=self.countries, left_index=True, right_on='COUNTRYAFF')
            query['location_id'] = query['ISO3']
            
        
                
        query['location_name'] = query.index + (' %' if self.as_percentage else '')
        fig = go.Figure(data=go.Choropleth(
            locations = query['location_id'],
            z = query['Total Updates'].round(2),
            text = query['location_name'],
            # hovertemplate = 'Price: %{z:$.2f}<extra></extra>',
            colorscale = 'Blues',
            autocolorscale=False,
            reversescale=False,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            colorbar_tickprefix = '',
            colorbar_title = 'Number of<br>Updates',
            locationmode = locationmode,
        ))

        fig.update_layout(
            geo=dict(
                showframe=False,
                showcoastlines=False,
                # fitbounds='locatoins'
                # projection_type='equirectangular'
                
            ),
            margin=dict(l=0, r=0, t=10, b=0),
            # height=600
        )
        if (geo_scope):
            fig.update_layout(
            geo_scope=geo_scope
        )
        else:
            fig.update_geos(fitbounds="locations")

        return fig
        


    @param.depends('country', 'query', 'as_percentage', 'player.value', watch=True)
    def update_tabular(self):
        # if not any(pd.DataFrame().columns):
        #     return

        print('update tabular')
        idx = pd.IndexSlice
        import numpy as np


        # road_type_filter = self.tabulator.value.index[self.tabulator.selection].tolist()

        if self.country['dataframe_filter']:
            if self.location_group['name'] == 'US':
                country_filter = (slice(None),[self.country['dataframe_filter']])                      
            else:
                country_filter = ([self.country['dataframe_filter']])
        else: 
            country_filter = (slice(None))


        if self.player.value:
            day = self.start_date + timedelta(days = self.player.value -1) 
            day = day.strftime("%Y-%m-%d")
            player_day_filter = idx[day:day]
        else:
            player_day_filter = slice(None)
        
        
        try:
            query = self.query.loc[player_day_filter,country_filter]
        except:
            
            query = pd.DataFrame()

        if len(query) and len(query.columns):
            query = query[query.any(axis=1)].groupby(level=1).sum().groupby(level=[2,3], axis=1).sum()
            query['Total'] = query.sum(axis=1)
            query.sort_values(by='Total', ascending=False, inplace=True)
            
            if self.as_percentage:
                tpc  = self.total_per_country.loc[
                    idx[query.index],
                    idx[
                        list(self.location_group['countries']),
                        :,
                        self.elements 
                    ]
                ].loc[
                    :,
                    country_filter

                ].fillna(0).groupby(level=[2], axis=1).sum().loc[query.index]
                tpc['Total'] = tpc.sum(axis=1)
                for o in self.operations:
                    query.loc[:,idx[self.elements,o]] = query.loc[:,idx[self.elements,o]] / tpc.loc[:,idx[self.elements]].values

                query.loc[:,idx['Total']] = query.loc[:,idx['Total']] / tpc.loc[:,idx['Total']].values
                query = query * 100

            
            query = query[['Total'] + self.elements].replace([np.inf, -np.inf], np.nan).fillna(0).round(2)
            query.rename({'node':'Nodes', 'way':'Ways','relation': 'Relations', 'create':'Created', 'modify':'Modified'}, axis=1, inplace=True)
            query.columns = query.columns.to_flat_index().str.join(' ')

            if self.as_percentage:
                query.rename(lambda x: x + ' %', axis=1, inplace=True)

            self.tabulator._update_data(query)
            # self.tabulator.selection = [query.index.get_loc(t) for t in road_type_filter]
        else:

            self.tabulator._update_data(pd.DataFrame(index=pd.Series(['#NA'], name='Total')))

        print('done update tabular')


    
        

    @param.depends('country', watch=True)
    def update_map_bounds(self):
        self.leaflet_map.fit_bounds(self.country['bounds'])

    
    def sample_map_view(self):
        button = pn.widgets.Button(name='Load a sample updates', button_type='primary')

        def query(event, button): 
            with pn.param.set_values(button, loading=True):
                if not self.elements or not self.operations:
                    pass 

                # note the order of x,y is different between leaflet, deckgl and shapely.
                (miny, minx),(maxy, maxx) = self.leaflet_map.bounds
                bb = f"SRID=4326;{box(minx,miny,maxx,maxy)}"
                

                country_bb = self.country['postgis_filter'] or f"SRID=4326;{box(-180,-90,180,90).to_wkb()}"

                    

                engine = create_engine("postgresql://dmlab:postgisisfun@cs-spatial-314:5432/osm_changes")  

                road_type_filter = ""
                selected_types = self.categories.selected_types + ['']
                if self.tabulator.selection:
                    selected_types = self.tabulator.value.index[self.tabulator.selection].tolist() + ['']
                
                if selected_types:
                    road_type_filter = f"AND road_type IN {tuple(selected_types)}"
                
                element_filter = ' OR '.join([f'element_{e}' for e in self.elements])
                operation_filter = ' OR '.join([f'operation_{e}' for e in self.operations])

                # country_filter = ""
                # if self.country['postgis_filter']:
                #     country_filter = f"AND country IN {self.country['postgis_filter']}"

                s = self.start_date.strftime("%Y-%m-%d")
                e = self.end_date.strftime("%Y-%m-%d")
                sql = f"""SELECT * FROM changeset_ids
                        WHERE day BETWEEN '{s}' AND '{e}' 
                        AND ({element_filter})
                        AND ({operation_filter})
                        
                        {road_type_filter} 
                        AND ST_INTERSECTS(geometry, '{bb}') 
                        AND ST_INTERSECTS(geometry, '{country_bb}') 
                        LIMIT 100"""
                
                
                changes = geopandas.read_postgis(sql, engine,geom_col='geometry').drop_duplicates(subset = ["changeset"])
                self.markers_group.markers = tuple(
                    Marker(location=p, 
                            draggable=False, 
                            popup=HTML(
                                value=f'<b>Changeset ID:</b> #<a href="https://overpass-api.de/achavi/?changeset={id}" target="_blank">{id}</a>')) for id, p in zip(changes.changeset, zip(changes.geometry.y,changes.geometry.x))
                )

        query_func = partial(query, button=button)
        button.on_click(query_func)
        return pn.Column(self.leaflet_map,button)

        
    def view(self):
        import numpy as np 

        pn.config.sizing_mode = 'stretch_width'

        bootstrap = pn.template.BootstrapTemplate(title='Road Network Updates on OSM',  header_background='black')
        bootstrap.sidebar.append(self.params_view())

        bootstrap.main.append(
            pn.Column(
                pn.Card(
                    pn.Column(
                        self.choropleth_map, 
                        self.map_control_view
                    ),title='Map View'
                ),
                pn.Row(
                    pn.Card(
                        pn.Column(
                            pn.Row(
                                self.country_state_filter_view,
                                # pn.layout.HSpacer(),
                                # pn.layout.HSpacer(),
                                pn.Column(
                                    pn.Param(self.param.as_percentage, widgets={
                                        'as_percentage': {
                                            'widget_type': pn.widgets.RadioButtonGroup,
                                            'options': {'Absolute Numbers':False, 'Percentage': True} 
                                        }
                                    }, margin=[22, 0, 0, 0])
                                )
                            ),
                            self.tabulator
                        ), title='Detailed Numbers'
                    ),
                    pn.Card(self.sample_map_view(), title='Sample', height=750)
                )
            )
        )

        
        return bootstrap




# pn.extension()


dashboard = Dashboard(name="OSM Changes On Road Network")
# panel = pn.Pane(dashboard.view)
panel = dashboard.view()



panel.servable()