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
from config.config import *


def fetchData(command):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="123",
                                      host="10.0.0.5",
                                      port="5432",
                                      database="project")
        cursor = connection.cursor()

        # Print PostgreSQL version
        cursor.execute(command)
        record = cursor.fetchall()
        print("query done" + "\n")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    return record

# Note: dash's category has the tag "imUrl" instead of "category"


def get_data(input_text, table):
    if len(input_text) <= 0:
        command = "SELECT customer_id, star_rating, review_body FROM {} ORDER BY review_id ASC LIMIT 20;".format(
            table)
    else:
        command = "SELECT customer_id, star_rating, review_body FROM {}  WHERE product_title LIKE '%{}%' ORDER BY review_id ASC  LIMIT 30;".format(
            table, input_text)
    return fetchData(command)


def get_fake(ls_id):
    fake_list = []
    cluster = Cluster(['10.0.0.13'])
    session = cluster.connect('project')
    for id in ls_id:
        try:
            row = session.execute(
                "SELECT * from apparel where user_id='{}';".format(id))[0]
            fake = row[2]
            fake_list.append((fake))
        except:
            fake = False
            fake_list.append((fake))
    return fake_list


# # Create a table based on the dataframe I created
def generate_table(input_text, table):
    data = get_data(input_text, table)
    df = pd.DataFrame([ij for ij in i] for i in data)
    df.rename(columns={0: "User ID",
                       1: "Star", 2: "Review"}, inplace=True)
    ls_id = df['User ID']
    fake_list = get_fake(ls_id)
    fake_list = [html.Div(children='Warning', style={
                          'color': 'red'}) if x == True else '' for x in fake_list]
    df['Fake'] = fake_list
    df['Review'] = df['Review'].map(lambda x: x.replace('<br />', ''))
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(len(df))]
    )


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div([
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


@app.callback(Output('output-query', 'children'),
              [Input('input-productname', 'value'),
               Input('my-dropdown', 'value')])
def update_output(input1, input2):
    return generate_table(input1.lower(), input2)


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)
