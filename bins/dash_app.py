"""Dash application

Author: Enrique Olivares <enrique.olivares@wizeline.com>

Description: Application used as a dashboard example of chart data.
"""
from datetime import datetime

import dash_bootstrap_components as dbc
import pandas
import plotly.express as px
import sqlalchemy
from dash import Dash, dcc, html
from pandas import DataFrame
from plotly.graph_objects import Figure


def load_data():
    data = pandas.DataFrame()
    try:
        conn = sqlalchemy.create_engine(
            "postgresql://postgres:post@localhost:5432/ml_samples"
        )
        data = pandas.read_sql_table("monthly_charts_data", con=conn)
    except Exception:
        data = pandas.DataFrame()
    else:
        data["date"] = data.month.apply(
            lambda x: datetime.strptime(x, "%b %Y")
        )
    finally:
        return data


def all_time_top_revenued_artists(data: DataFrame, top: int = 10) -> Figure:
    indexes = (
        data[["artist", "indicativerevenue"]].groupby(["artist"]).idxmax()
    )
    data = data.loc[indexes.indicativerevenue]
    data = data.sort_values("indicativerevenue", ascending=False)
    data = data.head(top)
    fig = px.bar(
        data,
        x="artist",
        y="indicativerevenue",
        title="All Time Highest Revenued Artists",
        labels={"artist": "Artist", "indicativerevenue": "Revenue"},
    )

    return fig


def all_time_top_revenued_songs(data: DataFrame, top: int = 10) -> Figure:
    indexes = data[["song", "indicativerevenue"]].groupby(["song"]).idxmax()
    data = data.loc[indexes.indicativerevenue]
    data = data.sort_values("indicativerevenue", ascending=False)
    data = data.head(top)
    data["title"] = data.song + " - " + data.artist
    fig = px.bar(
        data,
        x="title",
        y="indicativerevenue",
        title="All Time Highest Revenued Songs",
        labels={"title": "Song Title", "indicativerevenue": "Revenue"},
    )

    return fig


def latest_chart_table(data: DataFrame) -> DataFrame:
    latest_date = data.date.max()
    data = data.query(f"date == '{latest_date}'")
    data = data.drop(columns="date")

    return data


def generate_html_table(data: DataFrame, max_rows: int = 10):
    header = html.Thead(html.Tr([html.Th(col) for col in data.columns]))
    body = html.Tbody(
        [
            html.Tr([html.Td(data.iloc[i][col]) for col in data.columns])
            for i in range(min(len(data), max_rows))
        ]
    )
    table = dbc.Table(
        [
            header,
            body,
        ],
        hover=True,
        bordered=True,
        responsive=True,
    )

    return table


def generate_all_time_plots_row(data: DataFrame, top: int = 10):
    row = dbc.Row(
        [
            dbc.Col(
                dcc.Graph(
                    id="highest-revenued-artists",
                    figure=all_time_top_revenued_artists(data, top),
                ),
            ),
            dbc.Col(
                dcc.Graph(
                    id="highest-revenued-songs",
                    figure=all_time_top_revenued_songs(data, top),
                ),
            ),
        ],
        align="center",
    )

    return row


def generate_latest_charts_row(data: DataFrame, max_rows: int = 10):
    row = dbc.Row(
        [
            generate_html_table(data, max_rows),
        ],
        align="center",
    )

    return row


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
df = load_data()
if df.size > 0:
    latest = latest_chart_table(df)
    content = [
        html.H2(children="All-Time Charts"),
        generate_all_time_plots_row(df, 5),
        html.Hr(),
        html.H2(children="Latest Chart"),
        generate_latest_charts_row(latest),
    ]
else:
    content = [dbc.Alert("ERROR: Data not found.", color="danger")]

app.layout = dbc.Container(
    children=[
        html.H1(children="Music History"),
        html.Hr(),
        *content,
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
