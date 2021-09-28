#%%
import plotly.graph_objects as go

import panel as pn
import param

import pandas as pd
from pandas import IndexSlice as idx

#%%
class MetadataView(param.Parameterized):

    data = pd.read_pickle('data/metadata_counts.pkl')
    metadata_groups_df = pd.read_csv('ui_setup/osm_metadata_groups.csv').set_index(['category', 'metadata'])

    metadata_groups = dict()
    for  category_name in [
            'basic', 'lanes', 'parking', 'turns', 'speed limits', 'other limits',
            'winter', 'tolls', 'bridges', 'hazards and goods transportation', 
            'access', 'names', 'address', 'destination', 'bus', 'bicycle', 'pedestrian', 
            'basic (detailed)', 'other']:
        
        metadata_groups[category_name.title()] = metadata_groups_df.loc[category_name].index.tolist() 
    
    metadata_groups['All'] = list(metadata_groups_df.index.get_level_values('metadata'))

    metadata_group                 =   param.ObjectSelector(objects=metadata_groups, default=metadata_groups['Basic'])
    search                         =   param.String(default="")
    selected_states                =   param.List(default=[])
    selected_road_types            =   param.List(default=[])
    is_united_states_selected      =   param.Boolean(default=False)
    aggregated_data                =   param.DataFrame(default=pd.DataFrame())

    
    metadata_table                 =   pn.widgets.DataFrame(pd.DataFrame(), autosize_mode = 'fit_columns', height=300, disabled=True, sizing_mode = 'stretch_width')
    metadata_availability_note     =   "Metadata information are currently available for United States only, and represent a snapshot of the current status not the history."
    total_roads = 0
    





    ####################################### Misc
    def get_empty_dataframe(self):
        return pd.DataFrame(index=pd.Series(['#NA'], name='Total'))

    ####################################### Misc

    @pn.depends('selected_states', 'selected_road_types', 'is_united_states_selected', watch=True)
    def aggregate_table(self):
        table = self.get_empty_dataframe()
        
        if self.is_united_states_selected:
            selected_states = self.selected_states
            selected_road_types = self.selected_road_types
            state_filter = idx[selected_states] if self.selected_states else idx[:]
            road_types_filter = idx[selected_road_types] if self.selected_road_types else idx[:]
            data = self.data.loc[:, idx[state_filter, road_types_filter]]
            total = data.loc['all'].sum()
            self.total_roads = total 
            available = data.sum(axis=1).drop('all', axis=0)
            missing = total - available
            missing_percentage =  ((total - available) / total).round(4) * 100
            table = pd.DataFrame({'Available': available, 'Missing':missing, 'Missing %':missing_percentage}).rename_axis('Metadata').sort_values(by=['Missing', 'Metadata'], ascending=[False, True])

        table.index.rename('Metadata', inplace=True)
        self.aggregated_data = table


    def metadata_notes(self):
        showing_note_1 = (
            'None' if not self.is_united_states_selected 
            else 'All U.S. states' if not self.selected_states
            else ', '.join (self.selected_states)
        )
        showing_note_color_1 = 'red' if self.selected_states else 'black'

        
        
        showing_note_2 = 'All' if not self.selected_road_types else ', '.join(self.selected_road_types)
        showing_note_color_2 = 'red' if self.selected_road_types else 'black'

        full_showing_note = f"""
        <span><b>Data For: </b></span>
        <span style='color:{showing_note_color_1}'>{showing_note_1}</span>"""
        
        if showing_note_1 != 'None':
            full_showing_note += f"""; and <span style='color:{showing_note_color_2}'>{showing_note_2} road/feature type(s).</span>
            &nbsp &nbsp <span><b>Roads: </b></span>{self.total_roads:,.0f}
            """
        else :
            full_showing_note += "."


        return pn.pane.HTML( f"""
                {self.metadata_availability_note}
                <br>
                {full_showing_note}
            """, height=50)


    def filtered_table(self):
        filtered = self.aggregated_data.loc[self.aggregated_data.index.intersection(self.metadata_group)].copy()
        if self.search:
            filtered = filtered[filtered.index.str.contains(self.search)]

        return pn.widgets.DataFrame(filtered, autosize_mode = 'fit_columns', height=300, disabled=True, sizing_mode = 'stretch_width')


    def view(self):
        
        return pn.Column(
            self.metadata_notes,
            pn.Row(
                pn.Param(self.param['metadata_group']),
                pn.Param(self.param['search'], widgets={
                    'search': {
                        'widget_type' : pn.widgets.TextInput,
                        'placeholder': 'Type and hit enter to search for metadata ...' 
                    }
                })
            ),

            self.filtered_table
        )