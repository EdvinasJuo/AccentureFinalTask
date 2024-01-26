from dash import dcc, html, dash_table, Dash
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
from dash.exceptions import PreventUpdate
from pymongo import MongoClient
import datetime
from dash_mantine_components import Container, Title, Grid, Col
import snowflake.connector
from snowflake_acc import *
from flask import Flask


app = Flask(__name__)

# Snowflake connection setup
showflake_con = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role='ACCOUNTADMIN'
)

cursor = showflake_con.cursor()

cursor.execute("""
    SELECT
  DATE_TRUNC('MONTH', EG.DATE) AS MONTH,
  EG.ISO3166_1 AS ISO,
  EG.COUNTRY_REGION,
  CD.LATITUDE,
  CD.LONGITUDE,
  SUM(EG.CASES) AS Cases,
  SUM(EG.DEATHS) AS Deaths
FROM
  COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL EG
JOIN
  COUNTRIES_DATA.PUBLIC.COUNTRIES CD ON EG.ISO3166_1 = CD.ISO2
GROUP BY
  MONTH,
  ISO,
  COUNTRY_REGION,
  LATITUDE,
  LONGITUDE
ORDER BY
  ISO,
  COUNTRY_REGION,
  MONTH;
""")

data = cursor.fetch_pandas_all()
df = pd.DataFrame(data)

cursor.close()
showflake_con.close()

dash_app = Dash(__name__, server=app, url_base_pathname='/dash/')

dash_app.layout = Container([
    Title('COVID-19 Data Visualization', color="blue", size="h1"),

    dcc.Location(id='url', refresh=False),

    dcc.Textarea(
        id='query_input',
        placeholder='Enter your SQL query here...',
        style={'width': '80%', 'height': 100},
        value='''SELECT COUNTRY_REGION FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION ORDER BY COUNTRY_REGION;''',
    ),

    html.Button('Run Query', id='run_query_button', n_clicks=0),

    html.Div(id='query_output'),

    dcc.Dropdown(
        id='country_dropdown',
        options=[
            {'label': country, 'value': country} for country in df['COUNTRY_REGION'].unique()
        ],
        value=df['COUNTRY_REGION'].unique()[0],
        multi=False
    ),

    Grid([
        Col([
            dcc.Graph(id='deaths_chart'),
        ], span=6),

        Col([
            dcc.Graph(id='cases_chart'),
        ], span=6),
    ]),

    dash_table.DataTable(
        id='summary_table',
        columns=[
            {'name': 'Month', 'id': 'MONTH'},
            {'name': 'ISO', 'id': 'ISO'},
            {'name': 'Country', 'id': 'COUNTRY_REGION'},
            {'name': 'Latitude', 'id': 'LATITUDE'},
            {'name': 'Longitude', 'id': 'LONGITUDE'},
            {'name': 'Total Cases', 'id': 'CASES'},
            {'name': 'Total Deaths', 'id': 'DEATHS'},
        ],
        page_size=12,
        style_table={
            'overflowX': 'auto',
            'backgroundColor': '#ffffff',
        },
        style_cell={
            'backgroundColor': '#ffffff',
            'color': '#000000',
        },
        style_header={
            'backgroundColor': '#f2f2f2',
            'fontWeight': 'bold',
        },
    ),

    dcc.Input(
        id='comment_input',
        type='text',
        placeholder='Add a comment to MongoDB for country that you have selected...',
        style={'width': '50%'}
    ),

    html.Button('Submit Comment', id='submit_button', n_clicks=0),

    html.Div(id='comments_output', style={'margin-top': '10px'}),
], style={'maxWidth': '1200px', 'margin': 'auto'})


def execute_custom_query(query):
    showflake_con = snowflake.connector.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role='ACCOUNTADMIN'
    )

    cursor = showflake_con.cursor()

    try:
        cursor.execute(query)
        data = cursor.fetch_pandas_all()
        result_df = pd.DataFrame(data)
        return result_df.to_dict('records')

    except Exception as e:
        return f"Error executing query: {str(e)}"

    finally:
        cursor.close()
        showflake_con.close()


@dash_app.callback(
    Output('query_output', 'children'),
    [Input('run_query_button', 'n_clicks')],
    [State('query_input', 'value')]
)
def run_custom_query(n_clicks, query):
    if n_clicks == 0:
        raise PreventUpdate

    result = execute_custom_query(query)

    if isinstance(result, str):  # If there's an error
        return html.Div(f"Error executing query: {result}")

    # Displying the results in a dash_table.DataTable
    table = dash_table.DataTable(
        id='custom_query_table',
        columns=[{'name': col, 'id': col} for col in result[0].keys()],
        data=result,
        style_table={
            'overflowX': 'auto',
            'backgroundColor': '#ffffff',
        },
        style_cell={
            'backgroundColor': '#ffffff',
            'color': '#000000',
        },
        style_header={
            'backgroundColor': '#f2f2f2',
            'fontWeight': 'bold',
        },
        page_size=5,
    )

    return table

@dash_app.callback(
    [Output('deaths_chart', 'figure'),
     Output('cases_chart', 'figure'),
     Output('summary_table', 'data'),
     Output('comments_output', 'children')],
    [Input('country_dropdown', 'value'),
     Input('submit_button', 'n_clicks')],
    [State('comment_input', 'value')]
)
def update_charts_and_insert_comment(selected_country, n_clicks, comment):
    # Filtering DataFrame based on selected country
    filtered_df = df[df['COUNTRY_REGION'] == selected_country]

    # Sorting DataFrame by month
    filtered_df = filtered_df.sort_values(by='MONTH')

    # Creating bar chart for Deaths using Plotly Express
    fig_deaths = px.bar(
        filtered_df,
        x='MONTH',
        y='DEATHS',
        color='DEATHS',
        barmode='group',
        labels={'DEATHS': 'Number of Deaths'},
        title=f'COVID-19 Deaths for {selected_country}',
        text_auto=True
    )

    # Creating bar chart for Cases using Plotly Express
    fig_cases = px.bar(
        filtered_df,
        x='MONTH',
        y='CASES',
        color='CASES',
        barmode='group',
        labels={'CASES': 'Number of Cases'},
        title=f'COVID-19 Cases for {selected_country}',
        text_auto=True
    )

    # Creating summarized information for the selected country
    summary_table_data = filtered_df.groupby(['MONTH', 'ISO', 'COUNTRY_REGION', 'LATITUDE', 'LONGITUDE']).agg({
        'CASES': 'sum',
        'DEATHS': 'sum'
    }).reset_index().to_dict('records')

    # Display comments
    comments_output = []
    if n_clicks > 0 and comment:
        filtered_df['MONTH'] = pd.to_datetime(filtered_df['MONTH'])
        mongo_client = MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["accenture"]
        mongo_collection = mongo_db["supplementary_data"]

        document = {
            "data_point_id": f"{selected_country}_data_added",
            "country_data": filtered_df.to_dict('records'),
            "comments": [{
                "user_id": "system",
                "comment": comment,
                "time_stamp": datetime.datetime.utcnow().isoformat()
            }]
        }

        mongo_collection.insert_one(document)

        # Append the new comment to the output
        comments_output.append(html.P(f"Comment Added to MongoDB: {comment}"))

    return fig_deaths, fig_cases, summary_table_data, comments_output


# Run the App
if __name__ == '__main__':
    dash_app.run_server(debug=True)
