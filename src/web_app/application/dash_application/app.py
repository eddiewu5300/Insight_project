import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.plotly as py
import plotly.graph_objs as go
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from dash.dependencies import Output, Input, State
import psycopg2
from cassandra.cluster import Cluster
import dash_table
import glob
from pathlib import Path



def connect_cassandra_cluster():
    cluster = Cluster(['10.0.0.13','10.0.0.7','10.0.0.5'])
    session = cluster.connect('project')
    return session
    

def get_data(input_text, table, session):
    table = table + "_review"
    if len(input_text) <= 0:
        command = "SELECT customer_id, star_rating, review_body FROM {} ORDER BY review_id ASC LIMIT 20;".format(
            table)
    else:
        command = "SELECT customer_id, star_rating, review_body FROM {}  WHERE product_title LIKE '%{}%' ORDER BY review_id ASC LIMIT 30;".format(
            table, input_text)
    try:
        data = session.execute(command)
    except:
        print("Error while connecting to Cassandra")
        data = [[]]
    return data
    


def get_fake(ls_id, table, session):
    fake_list = []
    for id in ls_id:
        try:
            row = session.execute(
                "SELECT * from {} where user_id='{}';".format(table, id))[0]
            fake = row[2]
            fake_list.append((fake))
        except:
            fake = False
            fake_list.append((fake))
    return fake_list



def generate_table(input_text, table, session):
    data = get_data(input_text, table, session)
    df = pd.DataFrame([ij for ij in i] for i in data)
    df.rename(columns={0: "User ID",
                       1: "Star", 2: "Review"}, inplace=True)
    ls_id = df['User ID']
    fake_list = get_fake(ls_id, session)
    fake_list = [html.Div(children='Warning', style={
                          'color': 'red'}) if x == True else '' for x in fake_list]
    df.insert(0, 'Fake', fake_list)
    df['Review'] = df['Review'].map(lambda x: x.replace('<br />',''))
    df['Review'] = df['Review'].map(lambda x: x.replace('&#34;',''))
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(len(df))]
    )


def Add_Dash(server):
    """Create a Dash app."""
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    dash_app = dash.Dash(__name__, external_stylesheets=external_stylesheets,
                         routes_pathname_prefix='/dashapp/')

    # Override the underlying HTML template
    #dash_app.index_string = html_layout

    # Create Dash Layout comprised of Data Tables
    dash_app.layout = html.Div([
        html.H1(children='''
        FakeOUT!!!!
    '''),
        html.Div(className='inputSection', children=[
            dcc.Input(id='input-productname', type='text', value=''),
            dcc.Dropdown(
                id='my-dropdown',
                options=[
                    {'label': 'Apparel', 'value': 'apparel'},
                    {'label': 'Outdoors', 'value': 'outdoors'},
                    {'label': 'Office', 'value': 'office_products'},
                    {'label': 'Music', 'value': 'music'},
                    {'label': 'Garden', 'value': 'lawn_and_garden'},
                    {'label': 'Home', 'value': 'home'},
                    {'label': 'Health', 'value': 'health__personal_care'},
                    {'label': 'Furniture', 'value': 'furniture'},
                    {'label': 'Electronics', 'value': 'electronics'},
                    {'label': 'Books', 'value': 'books'},
                    {'label': 'Beauty', 'value': 'beauty'},
                    {'label': 'Kitchen', 'value': 'kitchen'},
                    {'label': 'Automotive', 'value': 'automotive'}
                ],
                value='Apparel')
        ]),
        html.Div(id='output-query')
    ])
    init_callbacks(dash_app)
    return dash_app.server


def init_callbacks(dash_app):
    @dash_app.callback(Output('output-query', 'children'),
                  [Input('input-productname', 'value'),
                   Input('my-dropdown', 'value')])
    def update_output(input1, input2):
        session = connect_cassandra_cluster()
        return generate_table(input1.lower(), input2, session)
