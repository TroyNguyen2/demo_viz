from plotly import express as px
from polars import DataFrame

def map_plot(df_Aeon:DataFrame):
    fig = px.scatter_geo(df_Aeon, 
                        lat= 'Lat',
                        lon= 'Lon',
                        color="Branch", # which column to use to set the color of markers
                        hover_name = "Branch",
                        hover_data={"total_spend":':,.0f',
                                    "age":True,
                                    "Main_transport":True,
                                    "Arrive_time":True,
                                    "Lat":False,
                                    "Lon":False,
                                    "Branch":False}                               
                        )
    fig.update_geos(
        visible=False, resolution=50,
        showcountries=True)
    fig.update_layout( title = 'Population distribution',
                    showlegend=True,
                    margin={"r":10,"t":0,"l":0,"b":10})

    return fig
