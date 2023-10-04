from plotly import express as px
from polars import DataFrame
import polars as pl

def household_person_age_3d(df_Aeon:DataFrame) -> px.scatter:
    fig = px.scatter_3d(df_Aeon, x='personal_monthly_income',
                         y='household_monthly_income',
                         z='Branch',
                         color='age',
                         height=900,
                         title="MONTHLY PURCHASE INCOME BY CUSTOMER",)
    fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
    return fig

def personal_income(df_Aeon:DataFrame):
    fig = px.histogram(df_Aeon, x="Branch", y="personal_monthly_income",
             color='age', barmode='group',histfunc="avg",
            text_auto='.2s',hover_name="age",title="Mean Personal income _ Age",
             height=400)
    return fig, df_Aeon.group_by(pl.col(["Branch","age"])).agg(pl.avg("personal_monthly_income").alias("mean_personal_monthly_income")).sort(["Branch","age"])

def household_income(df_Aeon:DataFrame):
    fig = px.histogram(df_Aeon, x="Branch", y="household_monthly_income",
            color='age', barmode='group',histfunc="avg",
            text_auto='.2s',hover_name="age",title="Mean Household income _ Age",
            height=400)
    return fig, df_Aeon.group_by(pl.col(["Branch","age"])).agg(pl.avg("household_monthly_income").alias("mean_household_monthly_income")).sort(["Branch","age"])
