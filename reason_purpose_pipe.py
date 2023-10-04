from plotly import express as px
from polars import DataFrame
import polars as pl

def reason_visit(df_Aeon:DataFrame):
     df_surveyee = df_Aeon.group_by(pl.col("Branch")).agg(pl.count().alias("Surveyee"))
     # Initialize df_reason
     df_reason = (df_Aeon.with_row_count('id')
                    .with_columns(pl.col("Reason_vist").str.split(",").alias("Split_Reason"))
                    .explode("Split_Reason"))
     # processing 2 group data
     group_visit = df_reason.group_by(pl.col(["Branch","age","Split_Reason"])).agg(
                    pl.count("Split_Reason").cast(pl.Int32, strict=False).alias("Reason_count"),
                    ).sort("Branch").sort("age")
     # Separate Other
     group_visit =group_visit.with_columns(pl.when(pl.col("Split_Reason").str.starts_with("Other"))
                                        .then(pl.lit("Other"))
                                        .otherwise(pl.col("Split_Reason")).alias("Split_Reason_2"))
     group_visit = group_visit.group_by(pl.col(["Branch","age","Split_Reason_2"])).agg(
                                             pl.sum("Reason_count"))
     
     group_visit   = group_visit.join(df_surveyee,on = "Branch",how="left")
     group_visit   = group_visit.with_columns((pl.col("Reason_count")*100 /pl.col("Surveyee")).alias("Reason_percentage")).sort(["Branch","age"])

     fig = px.histogram(group_visit, y='Reason_percentage', x='Split_Reason_2', text_auto='.2s',
               color='Branch',
               barmode='group',
               height=700,
               histfunc='sum',
               title="Reasons of Visit",
               hover_data= ["Reason_count"],
               labels={  "Reason_count": "Visiters",
                         "Split_Reason_2": "Reasons of Visiter",
                         }).update_layout( yaxis_title="% Percentage of Reason")
                    
     fig.update_traces(textfont_size=12, textangle=0, textposition="auto", cliponaxis=False)

     return fig, group_visit.group_by(pl.col(["Branch","Split_Reason_2"])).agg(pl.sum("Reason_percentage")).sort(["Branch"])

def purpose_visit(df_Aeon:DataFrame):
     df_surveyee = df_Aeon.group_by(pl.col("Branch")).agg(pl.count().alias("Surveyee"))
     # Initialize df_purpose
     df_purpose = (df_Aeon.with_row_count('id')
                    .with_columns(pl.col("Purpose_visit").str.split(",").alias("Split_Purpose"))
                    .explode("Split_Purpose"))
     # processing 2 group data
     group_purpose = df_purpose.group_by(pl.col(["Branch","age","Split_Purpose"])).agg(
                    pl.count("Split_Purpose").cast(pl.Int32, strict=False).alias("Purpose_count"),
                    )
     # Separate Other
     group_purpose = group_purpose.with_columns(pl.when(pl.col("Split_Purpose").str.starts_with("Other"))
                                        .then(pl.lit("Other"))
                                        .otherwise(pl.col("Split_Purpose")).alias("Split_Purpose_2"))
     group_purpose = group_purpose.group_by(pl.col(["Branch","age","Split_Purpose_2"])).agg(
                                            pl.sum("Purpose_count")).sort(["Branch","age"])
     group_purpose = group_purpose.join(df_surveyee,on = "Branch",how="left")
     group_purpose = group_purpose.with_columns((pl.col("Purpose_count")*100 /pl.col("Surveyee")).alias("Purpose_percentage"))
     # plot fig
     fig = px.histogram(group_purpose, y='Purpose_percentage', x='Split_Purpose_2', text_auto='.2s',
               color='Branch',
               barmode='group',
               height=700,
               histfunc='sum',
               title="Purposes of Visit ",
               hover_data= ["Purpose_count"],
               labels={
               "Purpose_count": "visiters",
               "Split_Purpose_2": "Purposes of Visiters",
               }).update_layout( yaxis_title="% Percentage of Purpose")
                    
     fig.update_traces(textfont_size=12, textangle=0, textposition="auto", cliponaxis=False)

     return fig,  group_purpose