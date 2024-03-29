#%%

from bokeh.models.formatters import NumeralTickFormatter
from bokeh.models import ColumnDataSource, Label
from bokeh.palettes import GnBu9,  BuPu9, BrBG9, Category10, Category20, Turbo256
from bokeh.plotting import figure
from panel.widgets import select
import plotly.graph_objects as go


import panel as pn
import param
from param.parameterized import depends
from bootstrap_with_analytics import BootstrapTemplate

import pandas as pd
from pandas import IndexSlice as idx
import numpy as np
import geopandas
from sqlalchemy import create_engine

from itertools import cycle, chain
from functools import partial

import json
import pickle

from datetime import date, datetime, timedelta


from ipyleaflet import Map, Marker, MarkerCluster
from ipywidgets import HTML
from metadata_view.metadata_view import MetadataView

#%%
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
                    param_name: {'widget_type': pn.widgets.Checkbox, 'name': category }
                             for param_name, category in zip(self.category_params, self.categories.keys())}
            )
        )

        


class Dashboard(param.Parameterized):
    
    # start_date  = param.Date(default= date.today() - timedelta (days=8))
    # end_date    = param.Date(default= date.today() - timedelta (days=1))

    start_date  = param.Date(default= date(2021,6,1))
    end_date    = param.Date(default= date(2021,6,28))


    categories  = TypeCategorySelector(name='Category Selection')
    as_percentage   = param.Boolean(default=False)
    selected_road_types = param.List(default=[])
    selected_countries = param.List(default=[])

    # pre processed options to avoid doing it on every request. Check the file "warmup_options.py" 
    countries, us_states_gdf, location_group_options = pickle.load(open('ui_setup/warmup_options.pkl', 'rb'))
        
    location_group = param.ObjectSelector(default=location_group_options['World'], objects=location_group_options)

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
    data2 = param.DataFrame(precedence=-1, default=pd.DataFrame(index=pd.MultiIndex(levels=[[],[]],codes=[[],[]],names=['day', 'type'])))
    query = param.DataFrame(precedence=-1, default=pd.DataFrame(columns=['Total']))
    query2 = param.DataFrame(precedence=-1, default=pd.DataFrame(columns=['Total']))


    params_column = pn.Column()
    unreflected_changes = pn.pane.Markdown(style = {'color':'red'})

    @param.depends('start_date','end_date', 'elements', 'operations', 'categories.selected_types', watch=True)
    def show_warning_unreflected_changes(self):
        self.unreflected_changes.object = 'A query parameter or more has changed. Make sure to click on "Query Data" to reflect the changes'

    @param.depends('query_button', watch=True)
    def load_data(self):
        df = None
        with pn.param.set_values(self.params_column, loading=True):
            if len(self.data2):
                df = self.data2.copy()
            else:
                df = pd.read_pickle('data/changes_aggregated/all.pkl.gzip', compression='gzip')
                # TODO temporary fix make it type instead of road_type. Should be updated in the original data
                df.index.names = ['day', 'Type']
                self.data2 = df.copy()
            
            s = self.start_date.strftime("%Y-%m-%d")
            e = self.end_date.strftime("%Y-%m-%d")
            df = df.loc[
                idx[s:e, self.categories.selected_types],
                idx[: , : , self.elements , self.operations]
            ]

            tpc = pd.read_pickle('data/total_per_country.pkl.gzip', compression='gzip').loc[
                idx[self.categories.selected_types],
                idx[: , : , self.elements]
            ]
        
        # reset the player in case it was used
        days = (self.end_date - self.start_date).days
        days = 0 if days < 0 else days
        self.player.end = days + 1
        self.player.value = 0

        # update tpc before updating data, because data is a param that will trigger 
        # "update_query_results" which reads self.total_per_country
        self.total_per_country = tpc.copy()
        self.data = df.copy()
        
        self.unreflected_changes.object = ''

    
    ####################################### Misc
    def get_empty_dataframe(self):
        return pd.DataFrame(index=pd.Series(['#NA'], name='Total'))

    def reselect_itmes_in_table(self, items, table):
        table.selection = [table.value.index.get_loc(t) for t in items if t in table.value.index]

    def get_location_group_string(self):
        return 'All ' + ('countries' if self.location_group['name'] == 'All' else f' in {self.location_group["name"]}')

    def get_only_20_note(self):
        options = {
            'x_units' : 'screen',
            'y_units' : 'screen',
            'background_fill_color' : 'white',
            'background_fill_alpha' : 1.0,
            'text_font_size' : "10px",
            'render_mode' : 'css',
        }
        return [
            Label(x_offset=200, y_offset=30, text='Only top 20 entries (absolute count) are shown here.', **options ),
            Label(x_offset=200, y_offset=15, text='Switch to table view for the full list.', **options )
        ]
    
    def is_location_group_US(self):
        return self.location_group['name'] == 'US'


    ########################################################################
    @param.depends('data', 'location_group', watch=True)
    def update_query_results(self):
        group_level = 1 if self.is_location_group_US() else 0

        
        query = self.data.loc[
            :,
            (list(self.location_group['countries']),)
        ].groupby(level=[group_level,2,3], axis = 1).sum()

        # temporary fix to remove a state called "United States" which was a falling back procedure
        # when we couldn't identify which state it falls. However, when calculating percentages per state, 
        # this cause an issue now because there is no such state. Should be fixed from original source.
        # in data preperation.
        if self.is_location_group_US():
            query.drop('United States', axis = 1, inplace=True)


        self.query_tpc = self.total_per_country.loc[
            :,
            (list(self.location_group['countries']),)
        ].fillna(0).groupby(level=[group_level,2], axis = 1).sum()
        
        # wait not to update views until the last param (query2) is updated
        # this is to avoid multiple rendering, i.e. rendering road_type_views after updating the country selection.
        self.pause_updates = True


        # clear selected countries and road_types and keep only ones that are existing in the new data
        self.selected_countries = sorted([c for c in self.selected_countries if c in query.columns.get_level_values(0)])
        # self.selected_road_types = sorted([c for c in self.selected_road_types if c in query.index.get_level_values(1)])
        # self.road_type_table.selection = [c for c in self.selected_road_types if c in query.index.get_level_values(1)])
        # table.value.index.get_loc(t) for t in items if t in table.value.index
        for c in self.selected_road_types:
            if c not in query.index.get_level_values(1):
                self.road_type_table.selection = []
                break
        
        # resume updates
        self.pause_updates = False
        self.query2 = query

    

    def __init__(self, *args, **kwargs):  
        self.categories.set_all_possible_types(self.data.index.get_level_values(level=1))

        # We use this to pause updates while updating multiple parameters, to avoid multiple rendering, 
        # until the last parameter is updated.
        self.pause_updates = False

        # check first and last days available to the system from the status file
        self.status = json.load(open('status.json'))
        self.last_day = datetime.strptime(self.status['last_day'], "%Y-%m-%d").date()
        self.first_day = datetime.strptime(self.status['first_day'], "%Y-%m-%d").date()

        self.start_date = self.last_day + timedelta(-30)
        self.end_date = self.last_day
        
        # initializing widgets and related elements:

        # 1- initializing items related to Road Type View:
        self.road_type_table = pn.widgets.DataFrame(pd.DataFrame(), autosize_mode = 'fit_columns', height=300, disabled=True)
        self.road_type_datasource = ColumnDataSource()
        self.road_type_tabs = pn.Tabs()
        
        ## linking the selectons of both the chart and the table
        def road_type_datasource_selection_change(attr, old, new):
            self.road_type_table.selection = new

        self.road_type_datasource.selected.on_change('indices', road_type_datasource_selection_change)
        self.road_type_table.link(self.road_type_datasource.selected, selection='indices')

        ## linking the widget to the selected_road_types parameter
        def callback(*events):
            for event in events:
                if event.name == 'selection':
                    self.selected_road_types = sorted(self.road_type_table.value.index[event.new].tolist())

        self.road_type_table.param.watch(callback, ['selection'], onlychanged=True)

        # commong between road_type and country plots
        self.plotting_keys = {
            'Ways Created' : GnBu9[0] ,
            'Ways Modified' : GnBu9[2] ,
            'Relations Created' : BuPu9[0] ,
            'Relations Modified' : BuPu9[2] ,
            'Nodes Created' : BrBG9[0],
            'Nodes Modified' : BrBG9[2]
        }
        ############################################

        # 2- initializing items related to Country View:
        self.country_table = pn.widgets.DataFrame(pd.DataFrame(), autosize_mode = 'fit_columns', height=300, disabled=True)
        self.country_datasource = ColumnDataSource()
        self.country_tabs = pn.Tabs()
        
        ## linking the selectons of both the chart and the table
        def country_datasource_selection_change(attr, old, new):
            self.country_table.selection = new

        self.country_datasource.selected.on_change('indices', country_datasource_selection_change)
        self.country_table.link(self.country_datasource.selected, selection='indices')

        ## linking the widget to the selected_countries parameter
        def callback(*events):
            for event in events:
                if event.name == 'selection':
                    self.selected_countries = sorted(self.country_table.value.index[event.new].tolist())

        self.country_table.param.watch(callback, ['selection'], onlychanged=True)
        #######################################################
        
        # 3- initializing items related to the Choropleth View:
        self.player = pn.widgets.Player(start=0, end=8, value= 0,loop_policy= 'once',show_loop_controls= False,interval= 500, width=400, sizing_mode='fixed')
        self.choropleth_chart = pn.pane.Plotly(height=270, config={'displayModeBar': False})
        #######################################################

        # 4- initializing items related to the Time Series View:
        self.time_series_tabs = pn.Tabs()
        #######################################################

        # 5- initializing items related to the Sample View:
        self.sample_map = Map(center=(0, 0), zoom=2, scroll_wheel_zoom=True, layout={'height':'400px'} )
        self.sample_markers = MarkerCluster()        
        self.sample_map.add_layer(self.sample_markers) 
        self.countries_bounds = json.load(open('ui_setup/countries_bounds.json', 'r'))   
        self.us_states_bounds = json.load(open('ui_setup/us_states_bounds.json', 'r'))   

        # 6- initializing items related to the Metadat View:
        self.metadata_view = MetadataView()




        pn.state.onload(lambda: self.param.trigger('query_button'))

        super().__init__(*args, **kwargs)

    
    def params_view(self):
        self.params_column = pn.Column(
            pn.Param(self.param['start_date'], widgets={
                    'start_date': {
                        'widget_type': pn.widgets.DatePicker,
                        'start': self.first_day,
                        'end': self.last_day,
                    }
                }),
            pn.Param(self.param['end_date'], widgets={
                    'end_date': {
                        'widget_type': pn.widgets.DatePicker,
                        'start': self.first_day,
                        'end': self.last_day,
                    }
            }),
            pn.pane.Markdown(f"Data is currently available from {self.status['first_day']} to {self.status['last_day']}. Last 30 days are selected by default", style={'color':'gray'}),
            self.categories.view,
            pn.widgets.StaticText(name='Elements', value=''),
            pn.Param(self.param['elements'], widgets={
                    'elements': pn.widgets.CheckBoxGroup
            }),
            pn.widgets.StaticText(name='Operations', value=''),
            pn.Param(self.param['operations'], widgets={
                    'operations': pn.widgets.CheckBoxGroup
            }),
            pn.Param(self.param['query_button'], widgets={
                    'query_button': {'widget_type': pn.widgets.Button, 'button_type': 'primary' }
            }),
            self.unreflected_changes
        )
        return self.params_column

    
    
    #######################################
    #######################################
    ########## Choropleth View ############
    #######################################
    #######################################

    @param.depends( 'query2', 'selected_road_types', 'as_percentage', 'player.value', watch=True)
    def choropleth_watcher(self):
        if self.pause_updates:
            return 

        print('choro', self.selected_road_types)


        if self.player.value:
            day = self.start_date + timedelta(days = self.player.value -1) 
            day = day.strftime("%Y-%m-%d")
            player_day_filter = idx[day:day]
        else:
            player_day_filter = slice(None)


        road_type_filter = self.selected_road_types or slice(None)
        query = self.query2.loc[(player_day_filter, road_type_filter),:]
        query = query.groupby(level=0, axis=1).sum().sum()
        query.name = 'Total Updates'


        if self.as_percentage:
            tpc = self.query_tpc.loc[(road_type_filter,)].groupby(level=0, axis=1).sum().sum()
            intersectin = query.index.intersection(tpc.index)
            query = query.loc[intersectin]
            tpc = tpc.loc[intersectin]
            query = (query/tpc.values * 100).fillna(0).round(2)


        geo_scope = None
        locationmode = None
        if self.is_location_group_US():
            query = pd.merge(left=query, right=self.us_states_gdf, left_index=True, right_on='name')
            query['location_id'] = query['id']
            locationmode = 'USA-states'
            geo_scope = 'usa'
        else:
            query = pd.merge(left=query, right=self.countries, left_index=True, right_on='COUNTRYAFF')
            query['location_id'] = query['ISO3']
            
        
                
        query['location_name'] = query.index + (' %' if self.as_percentage else '')
        fig = go.Figure(data=go.Choropleth(
                locations = query['location_id'],
                z = query['Total Updates'],
                text = query['location_name'],
                colorscale = 'Blues',
                autocolorscale=False,
                reversescale=False,
                marker_line_color='darkgray',
                marker_line_width=0.5,
                colorbar_tickprefix = '',
                colorbar_title = 'Number of<br>Updates',
                locationmode = locationmode,
            )
        )

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

        self.choropleth_chart.object = fig



    @pn.depends('player.value')
    def player_info_view(self):
        text = ''
        if self.player.value :
            text = (self.start_date + timedelta(days=self.player.value -1)).strftime("%d-%b-%Y")
            style =  {"color": "red"}
        else:
            text = 'All days'
            style = {}

        return pn.pane.Markdown("**Showing:**\n\n" + text, width=120, sizing_mode='fixed', style = style)


    @depends('selected_road_types')
    def choropleth_notes(self):
        showing_note = ""
        if self.selected_road_types:
            showing_note = ', '.join(self.selected_road_types)
        else:
            showing_note = 'All'

        showing_note_color = 'red' if self.selected_road_types else 'black'

        return pn.pane.HTML(f"""
            <span><b>Data For: </b></span>
            <span style='color:{showing_note_color}'>{showing_note} road/feature type(s).</span>
            &nbsp &nbsp
        """, height=20)

    ##########################################################################################################################

    
    
    
    #######################################
    #######################################
    ########## Road Type View #############
    #######################################
    #######################################
    
    @param.depends('query2', 'selected_countries','as_percentage')
    def road_type_view(self):
        if self.pause_updates:
            return self.road_type_tabs

        print('road_type_view', self.selected_countries)
        
        ## 1- prepare the data:
        table_data = self.get_empty_dataframe()
        if len(self.query2) and len(self.query2.columns):
            country_filter = self.selected_countries or slice(None)
            query = self.query2.loc[:,(country_filter,)]
            query =query[query.any(axis=1)].groupby(level=1).sum().groupby(level=[1,2], axis=1).sum()
            query['All'] = query.sum(axis=1)
            query.sort_values(by='All', ascending=False, inplace=True)
            
            if self.as_percentage:
                tpc  = self.query_tpc.loc[(query.index),(country_filter,)].groupby(level=[1], axis=1).sum()
                tpc['All'] = tpc.sum(axis=1)

                for c in query.columns:
                    query[c] = query[c] / tpc[c[0]].values


            
            query = query.loc[:,(['All'] + self.param.elements.objects,)].replace([np.inf, -np.inf], np.nan).fillna(0).round(4)
            query.rename({'node':'Nodes', 'way':'Ways','relation': 'Relations', 'create':'Created', 'modify':'Modified'}, axis=1, inplace=True)
            query.columns = query.columns.to_flat_index().str.join(' ')

            table_data = query.copy()

        
        ## 2- create chart:
        chart_data = table_data.iloc[:20]
        keys_colors = dict(filter(lambda entry: entry[0] in chart_data.columns, self.plotting_keys.items()))
        keys = list(keys_colors.keys())
        colors = list(keys_colors.values())

        road_types = chart_data.index.values.tolist()
        data = dict({'road_types' : road_types}, **{k: chart_data[k].values.tolist() for k in keys})


        p = figure(y_range=list(reversed(road_types)),  title="Total updates by road/feature type",
                toolbar_location=None, tools='tap')
        self.road_type_datasource.data = data
        p.hbar_stack(keys, y='road_types', height=0.6, color=colors, source=self.road_type_datasource,
                    legend_label=keys)


        p.y_range.range_padding = 0.1
        p.ygrid.grid_line_color = None
        p.legend.location = "center_right"
        p.axis.minor_tick_line_color = None
        format ='0.00%' if self.as_percentage else '0.0a'
        p.xaxis.formatter = NumeralTickFormatter(format=format)
        p.outline_line_color = None

        if len(table_data) > 20:
            for l in self.get_only_20_note():
                p.add_layout(l)
        
        chart = pn.pane.Bokeh(p, height=300)

        ## 3- update table view:
        if self.as_percentage:
            table_data.rename(lambda x: x + ' %', axis=1, inplace=True)
            table_data = table_data * 100

        self.road_type_table._update_data(table_data)
        
        # previoysly selected items are preserved in selected_road_types.  Reselect them again if exist.
        self.reselect_itmes_in_table(self.selected_road_types, self.road_type_table)


        ## 4- return view:
        self.road_type_tabs = pn.Tabs(
            ('Chart', chart), 
            ('Table', self.road_type_table),
            active=self.road_type_tabs.active,
            dynamic = True
        )
        return self.road_type_tabs


    @depends('selected_countries', 'selected_road_types')
    def road_types_notes(self):
        showing_note = ', '.join(self.selected_countries) or self.get_location_group_string()
        showing_note_color = 'red' if self.selected_countries else 'black'

        selection_note = ', '.join(self.selected_road_types) or 'None'
        selection_note_color = 'red' if self.selected_road_types else 'black'

        return pn.pane.HTML(f"""
            <span><b>Data For: </b></span>
            <span style='color:{showing_note_color}'>{showing_note}.</span>
            &nbsp &nbsp
            <span><b>Selected: </b></span>
            <span style='color:{selection_note_color}'>{selection_note}.</span>
            <br>
        """, height=20)



    ##########################################################################################################################



    #######################################
    #######################################
    ############ Country View #############
    #######################################
    #######################################

    @param.depends('query2', 'selected_road_types','as_percentage')
    def country_view(self):
        if self.pause_updates:
            return self.country_tabs


        print('country_view', self.selected_road_types)

        ## 1- prepare the data:
        table_data = self.get_empty_dataframe()
        if len(self.query2) and len(self.query2.columns):
            selected_roads = self.selected_road_types
            road_type_filter = idx[selected_roads] if selected_roads else idx[:]
            query = self.query2.loc[idx[:,road_type_filter],:].sum().unstack(0).T
            query['All'] = query.sum(axis=1)
            query.sort_values(by='All', ascending=False, inplace=True)


            if self.as_percentage:
                tpc  = self.query_tpc.loc[road_type_filter].sum().unstack(0).T.loc[query.index]
                tpc['All'] = tpc.sum(axis=1)

                for c in query.columns:
                    query[c] = query[c] / tpc[c[0]].values

            
            query = query[['All'] + self.elements].replace([np.inf, -np.inf], np.nan).fillna(0).round(4)
            query.rename({'node':'Nodes', 'way':'Ways','relation': 'Relations', 'create':'Created', 'modify':'Modified'}, axis=1, inplace=True)
            query.columns = query.columns.to_flat_index().str.join(' ')

            table_data = query.copy()



        ## 2- create chart:
        chart_data = table_data.iloc[:20]
        keys_colors = dict(filter(lambda entry: entry[0] in chart_data.columns, self.plotting_keys.items()))
        keys = list(keys_colors.keys())
        colors = list(keys_colors.values())

        countries = chart_data.index.values.tolist()
        data = dict({'countries' : countries}, **{k: chart_data[k].values.tolist() for k in keys})


        p = figure(y_range=list(reversed(countries)),  title="Total updates by country",
                toolbar_location=None, tools='tap')
        self.country_datasource.data = data
        p.hbar_stack(keys, y='countries', height=0.6, color=colors, source=self.country_datasource,
                    legend_label=keys)


        p.y_range.range_padding = 0.1
        p.ygrid.grid_line_color = None
        p.legend.location = "center_right"
        p.axis.minor_tick_line_color = None
        format ='0.00%' if self.as_percentage else '0.0a'
        p.xaxis.formatter = NumeralTickFormatter(format=format)
        p.outline_line_color = None
        
        if len(table_data) > 20:
            for l in self.get_only_20_note():
                p.add_layout(l)
        
        chart = pn.pane.Bokeh(p, height=300)
        


        ## 3- update table view:
        if self.as_percentage:
            table_data.rename(lambda x: x + ' %', axis=1, inplace=True)
            table_data = table_data * 100

        self.country_table._update_data(table_data)

        # previoysly selected items are preserved in selected_countries.  Reselect them again if exist.
        self.reselect_itmes_in_table(self.selected_countries, self.country_table)

        
        ## 4- return view:
        self.country_tabs = pn.Tabs(
            ('Table', self.country_table),
            ('Chart', chart), 
            active=self.country_tabs.active,
            dynamic = True
        )
        return self.country_tabs


    @depends('selected_countries', 'selected_road_types')
    def country_notes(self):
        showing_note = ', '.join(self.selected_road_types) or 'All'
        showing_note_color = 'red' if self.selected_road_types else 'black'

        selection_note = ', '.join(self.selected_countries) or 'None'
        selection_note_color = 'red' if self.selected_countries else 'black'

        return pn.pane.HTML(f"""
            <span><b>Data For: </b></span>
            <span style='color:{showing_note_color}'>{showing_note} road/feature type(s).</span>
            &nbsp &nbsp
            <span><b>Selected: </b></span>
            <span style='color:{selection_note_color}'>{selection_note}.</span>
            <br>
            <i>Table:</i> click+ctrl(cmd) to select/deselect entries.
            <i>Chart:</i> click+shift for multiple selection; click on empty area (or ESC) to clear. 
        """, height=50)
    ##########################################################################################################################


            

    #######################################
    #######################################
    ########## Time Series View ###########
    #######################################
    #######################################
    @param.depends('query2', 'selected_road_types', 'selected_countries', 'as_percentage')
    def time_series_view(self):
        if self.pause_updates:
            return self.time_series_tabs

        print('time_series_view', self.selected_road_types, self.selected_countries)


        p = figure (title="Updates over time", x_axis_type="datetime", toolbar_location="right", tools= 'hover, wheel_zoom, pan, reset', active_scroll='wheel_zoom')
        format ='0.00%' if self.as_percentage else '0.0a'
        p.yaxis.formatter = NumeralTickFormatter(format=format)
        p.outline_line_color = None

        p.hover.tooltips = [
            ( 'date',     '@date{%F}'             ), 
            ( 'volume',   f'@volume{{{format}}}'  ),
        ]
        p.hover.formatters = { "@date": "datetime"}

        query = self.query2.copy()
        if len(query):
            road_type_filter = self.selected_road_types or slice(None)
            countries_filter = self.selected_countries or slice(None)
            query = query.loc[(slice(None),road_type_filter),(countries_filter,)]
            query = query.groupby(level=0).sum().groupby(level=0, axis=1).sum()
            query = query.set_index(pd.to_datetime(query.index))
            name_for_total = self.get_location_group_string()
            if not self.selected_countries:
                query = pd.DataFrame({name_for_total:query.sum(axis=1)}, index=query.index)

            if self.as_percentage:
                tpc = self.query_tpc.loc[(road_type_filter),(countries_filter,)].sum().groupby(level=0).sum()
                if not self.selected_countries:
                    tpc = pd.Series({name_for_total:tpc.sum()})
                
                for c in query.columns:
                    query[c] = query[c] / tpc[c]
            
            if len(query) > 10: 
                query = query.rolling(7).mean().dropna()
                p.title.text += " (7-days moving average)"

            if len(query.columns) <= 10 :
                colors = Category10[10]
            elif len(query.columns) <= 20 :
                colors = Category20[20]
            else:
                colors = cycle(Turbo256)

            for country,color in zip (query,colors):
                ds = ColumnDataSource({
                    'date' : query.index,
                    'volume' : query[country]
                })
                p.line(x='date' ,y='volume', line_width=2, legend_label=country, color=color, name=country, source = ds)


        self.time_series_tabs = pn.Tabs(
            ('Chart', pn.pane.Bokeh(p, height=310)), 
            active=self.time_series_tabs.active,
            dynamic = True
        )
        return self.time_series_tabs


    @depends('selected_road_types')
    def time_series_notes(self):
        showing_note = ', '.join(self.selected_road_types) or 'All'
        showing_note_color = 'red' if self.selected_road_types else 'black'

        return pn.pane.HTML(f"""
            <span><b>Data For: </b></span>
            <span style='color:{showing_note_color}'>{showing_note} road/feature type(s).</span>
            &nbsp &nbsp
        """, height=20)
            
    ##########################################################################################################################


    #######################################
    #######################################
    ########## Metadata View ###########
    #######################################
    #######################################
    @pn.depends('query2', 'location_group', 'selected_road_types', 'selected_countries', watch=True)
    def update_metadata_view(self):
        if self.pause_updates:
            return 

        print('metadata_view')

        is_united_states_selected =  (
            self.is_location_group_US() or
            'United States' in self.selected_countries or
             (self.location_group['name'] == 'All' and not self.selected_countries)
        )
        
        # selected_countries param will hold states instead of countries in case location_group was the U.S.
        selected_states = [] if not self.is_location_group_US() else self.selected_countries

        self.metadata_view.selected_states = selected_states
        self.metadata_view.selected_road_types = self.selected_road_types
        self.metadata_view.is_united_states_selected = is_united_states_selected        



    ##########################################################################################################################
    
    
    #######################################
    #######################################
    ########## Sample View ###########
    #######################################
    #######################################

    def sample_view(self):
        sample_load_button = pn.widgets.Button(name='Load a sample updates', button_type='primary')

        def query(event, button): 
            with pn.param.set_values(button, loading=True):
                if not self.elements or not self.operations:
                    pass 

                # note the order of x,y is different between leaflet, deckgl and shapely.
                # (miny, minx),(maxy, maxx) = self.leaflet_map.bounds
                # bb = f"SRID=4326;{box(minx,miny,maxx,maxy)}"
                # country_bb = self.country['postgis_filter'] or f"SRID=4326;{box(-180,-90,180,90).to_wkb()}"


                engine = create_engine("postgresql://dmlab:postgisisfun@cs-spatial-314:5432/osm_changes")  

                road_type_filter = ""
                selected_types = self.categories.selected_types + ['']
                if self.selected_road_types:
                    selected_types = self.selected_road_types + ['']
                
                if selected_types:
                    road_type_filter = f"AND road_type IN {tuple(selected_types)}"
                
                element_filter = ' OR '.join([f'element_{e}' for e in self.elements])
                operation_filter = ' OR '.join([f'operation_{e}' for e in self.operations])

                country_filter = ""
                if self.is_location_group_US():
                    country_filter = f"AND country = 'United States'"
                    if self.selected_countries:
                        country_filter += f" AND state IN {tuple(self.selected_countries + [''])}"
                elif self.selected_countries:
                    country_filter += f"AND (" +  ' OR '.join([f"country='{c}'" for c in self.selected_countries]) + ")"
                elif self.location_group['name'] != 'All':
                    country_filter = f"AND (" +  ' OR '.join([f"country='{c}'" for c in self.location_group['countries']]) + ")"

                # AND ST_INTERSECTS(geometry, '{bb}') 
                # AND ST_INTERSECTS(geometry, '{country_bb}') 

                s = self.start_date.strftime("%Y-%m-%d")
                e = self.end_date.strftime("%Y-%m-%d")
                
                sql = f"""SELECT * FROM changeset_ids
                        WHERE day BETWEEN '{s}' AND '{e}' 
                        AND ({element_filter})
                        AND ({operation_filter})
                        
                        {road_type_filter} 
                        {country_filter}

                        LIMIT 100"""

                
                changes = geopandas.read_postgis(sql, engine,geom_col='geometry').drop_duplicates(subset = ["changeset"])
                self.sample_markers.markers = tuple(
                    Marker(location=p, 
                            draggable=False, 
                            popup=HTML(
                                value=f'<b>Changeset ID:</b> #<a href="https://overpass-api.de/achavi/?changeset={id}" target="_blank">{id}</a>')) for id, p in zip(changes.changeset, zip(changes.geometry.y,changes.geometry.x))
                )

        query_func = partial(query, button=sample_load_button)
        sample_load_button.on_click(query_func)
        return pn.Column(sample_load_button, self.sample_map)


    @param.depends('selected_countries', watch=True)
    def update_map_bounds(self):
        if self.pause_updates:
            return
        
        if len(self.selected_countries) != 1:
            return

        if self.is_location_group_US():
            bounds = self.us_states_bounds[self.selected_countries[0]]
        else:
            bounds = self.countries_bounds[self.selected_countries[0]]

        leaflet_bounds = (
            (bounds['south'], bounds['west']),
            (bounds['north'], bounds['east'])
        )
        self.sample_map.fit_bounds(leaflet_bounds)  


    def view(self):
         

        pn.config.sizing_mode = 'stretch_width'
        css = '''
        .slick-cell.selected {
            background-color: #43a2ca !important;
            color: white !important;
        }
        body {
            overflow : hidden;
        }
        #main {
            overflow-x : hidden;
        }
        '''
        pn.config.raw_css.append(css)

        bootstrap = BootstrapTemplate(title='RASED: A Dashboard for Monitoring Road Network Updates in OSM',  header_background='black', sidebar_width=220)
        bootstrap.sidebar.append(
            pn.Column(
                self.params_view(),
            )
        )

        bootstrap.main.append(
            pn.Column(
                pn.Row(
                    pn.Param(
                        self.param.as_percentage,
                        widgets={
                            'as_percentage': {
                                'widget_type': pn.widgets.RadioButtonGroup,
                                'options': {'Absolute Numbers':False, 'Percentage': True} 
                            }
                        }, width=300, sizing_mode='fixed'
                    ),
                    pn.Param(
                        self.param['location_group'], 
                        widgets={'location_group':  pn.widgets.RadioButtonGroup},
                        align = ('center', 'center')
                    ),
                ),
                pn.Row(
                    pn.Card(
                        pn.Column(
                            self.country_notes,
                            self.country_view
                        ),
                        title='Countries View'
                    ),
                    pn.Card(
                        pn.Column(
                            self.choropleth_notes,
                            self.choropleth_chart,
                            pn.Row(
                                self.player,
                                self.player_info_view, 
                                align='center'
                            )
                        ),
                        title = 'Choropleth View'
                    )
                ),
                pn.Row(
                    pn.Card(
                        pn.Column(
                            self.road_types_notes,
                            self.road_type_view
                        ),
                        title='Road/Feature Types View'
                    ),
                    pn.Card(
                        pn.Column(
                            self.time_series_notes,
                            self.time_series_view
                        ),
                        title='Time Series View'
                    )
                ),
                pn.Row(
                    pn.Row(
                        pn.Card(
                            self.sample_view(),
                            title='Sample View',
                            height=500
                        ),
                        pn.Card(
                            self.metadata_view.view(),
                            title='Metadata View',
                            height=500
                        )

                    )
                )
            )
        )

        return bootstrap


dashboard = Dashboard(name="RASED: A Dashboard for Monitoring Road Network Updates in OSM")
# panel = pn.Pane(dashboard.view)
panel = dashboard.view()

panel.servable()