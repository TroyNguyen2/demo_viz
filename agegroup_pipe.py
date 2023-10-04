import plotly.express as px
import plotly.graph_objects as go
import polars as pl

# df = pl.read_parquet(r"E:\test_dash\_Aeon\data_clean.parquet")
def plot_age(df_Aeon:pl.DataFrame):
        df_Surveyee = df_Aeon.group_by(pl.col(["Branch"])).agg(pl.count().alias("Surveyee"))
        # processing 2 group data
        df_Aeon = df_Aeon.group_by(pl.col(["Branch","age"])).agg(pl.sum("total_spend"),
                                                                pl.count("age").cast(pl.Int64, strict=False).alias("age_count"),
                                                                pl.mean("personal_monthly_income").cast(pl.Int64, strict=False).alias("mean_personal_income"),
                                                                pl.mean("household_monthly_income").cast(pl.Int64, strict=False).alias("mean_household_income"),
                                                                pl.median("personal_monthly_income").alias("median_personal_income")
                                                                ).sort("Branch").sort("age")
        # processing 3 cal age percentage and return
        df_Aeon   = df_Aeon.join(df_Surveyee,on = "Branch",how="left")
        df_Aeon   = df_Aeon.with_columns((pl.col("age_count")/pl.col("Surveyee")*100).round(2).alias("age_percentage"))
        df_Aeon   = df_Aeon.select(sorted(df_Aeon.columns))
        ## 1st plot
        fig = px.bar(df_Aeon, x="age_percentage", y="Branch", color='age', orientation='h',
                        text= 'age_percentage',
                        hover_data={"age_count":":,.0f",
                                "Surveyee":":,.0f",
                                "mean_personal_income":':,.0f',
                                "median_personal_income":":,.0f"},
                        labels={"age": "Age Group",
                                "total_spend": "Total Purchase",
                                "median_personal_income":"Median Personal Income (VND)",
                                "mean_personal_income":"Avg Personal Income (VND)",
                                "age_count":"Age Count (people)",
                                "age_percentage":"Age Ratio"+"%",
                                },
                        height=400,
                        title='Age group 2023')
        return fig, df_Aeon

def daily_monthly_purchase_age(df_Aeon:pl.DataFrame):
     df_Aeon =  df_Aeon.group_by(pl.col(["Branch","age"])).agg(pl.sum("day_spend"),
                                                               pl.sum("total_spend")).sort(["Branch","age"])
     fig = px.bar(df_Aeon, x="age", y={"day_spend":':,.0f'},
                color='Branch', barmode='group', text_auto=True,
                labels={
                        "age": "Age group",
                        "value": "Daily Purchase"
                        },
                height=400)
     fig_2 = px.bar(df_Aeon, x="age", y={"total_spend":':,.0f'},
                color='Branch', barmode='group', text_auto=True,
                labels={
                        "age": "Age group",
                        "value": "Monthly Purchase"
                        },
                height=400)
     return fig,fig_2, df_Aeon

def pie_purchase(df_Aeon:pl.DataFrame):
       df_Aeon = df_Aeon.group_by(pl.col(["Branch"])).agg(pl.sum("total_spend").alias("monthly_purchase"))
       df_Aeon = df_Aeon.with_columns(pl.sum("monthly_purchase").alias("capabilities"))
       df_Aeon = df_Aeon.with_columns((pl.col("monthly_purchase")/pl.col("capabilities")*100).round(2).alias("percentage_of_capabilities")).sort("Branch")
       fig = go.Figure(go.Pie(
              name = "Capabilities",
              values = df_Aeon['monthly_purchase'],
              textfont=dict(color='#000000'),
              labels = df_Aeon['Branch'],
              textposition='inside', textinfo='label+percent',
              customdata=df_Aeon['percentage_of_capabilities'],
              marker_colors=px.colors.qualitative.Plotly,
              hovertemplate = "Branch %{label}: <br>Monthly Purchase: %{value:,} VND </br>Percentage of Cap. : %{customdata} %"))
       fig.update_layout(title="Capabilities",showlegend=False, uniformtext_minsize=14)
       fig.add_annotation(text="Demo Mall",font=dict(size=18),showarrow=False)
       fig.update_traces(hole=.4, hoverinfo='label+percent')
       return fig
