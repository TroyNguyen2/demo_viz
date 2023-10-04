from plotly import express as px
from polars import DataFrame
import polars as pl

def monthly_household_income_percent(df_Aeon:DataFrame):
    income_group = [17000000,30000000,50000000]
    dict_sort ={"< 17mil. VND":"0",
                  "17 - 30mil. VND":"1",
                  "30 - 50mil. VND":"2",
                  ">50mil.++ VND":"3"}

    mapper_sort=DataFrame({
    "household_monthly_income": list(dict_sort.keys()),
    "values": list(dict_sort.values())
    })
    # Map
    income_household = df_Aeon.with_columns(pl.when(pl.col("household_monthly_income") <= income_group[0]).then(pl.lit(f"< {int(income_group[0]/1000000)}mil. VND"))
                            .when((income_group[0] <pl.col("household_monthly_income")) & (pl.col("household_monthly_income") <= income_group[1])).then(pl.lit(f"17 - {int(income_group[1]/1000000)}mil. VND"))
                            .when((income_group[1] <pl.col("household_monthly_income")) & (pl.col("household_monthly_income")  <= income_group[2])).then(pl.lit(f"30 - {int(income_group[2]/1000000)}mil. VND"))
                            .when((income_group[2] <pl.col("household_monthly_income"))).then(pl.lit(f">{int(income_group[2]/1000000)}mil.++ VND")).alias("household_monthly_income"))
    # Get the count branch to join
    dim_group_branch = income_household.group_by(pl.col("Branch")).agg(pl.count()) 

    # Processing 
    income_household = income_household.select(pl.col(["Branch","age","household_monthly_income","total_spend"]))
    income_household = income_household.group_by(pl.col(["Branch","household_monthly_income"])).agg((pl.count()).alias("household_income_count"))
    income_household = income_household.join(dim_group_branch,on ="Branch",how="left")
    income_household = income_household.with_columns((pl.col("household_income_count")/pl.col("count")*100).round(2).alias("percent_household_income"))
    income_household = income_household.join(mapper_sort,on ="household_monthly_income",how="left").sort(["Branch","values"])
                                    
    fig = px.bar(income_household, x="percent_household_income", y="Branch", color='household_monthly_income', orientation='h',
                text= 'percent_household_income',text_auto=True,
                hover_data={
                        "percent_household_income":":,.2f",
                        "household_income_count":":,.0f"},
                labels={"percent_household_income":"% Household Income Group"},
                height=400,
                title='MONTHLY HOUSEHOLD INCOME %')
    
    return fig,income_household.drop("values")

def monthly_personal_income_percent(df_Aeon:DataFrame):
    income_group = [10000000,20000000,40000000]
    dict_sort ={"<10mil. VND":"0",
                "10 - 20mil. VND":"1",
                "20 - 40mil. VND":"2",
                ">40mil.++ VND":"3" }
    
    mapper_sort=DataFrame({
    "personal_monthly_income": list(dict_sort.keys()),
    "values": list(dict_sort.values())
    })
    # Map
    income_personal = df_Aeon.with_columns(pl.when(pl.col("personal_monthly_income") <= income_group[0]).then(pl.lit(f"< {int(income_group[0]/1000000)}mil. VND"))
                            .when((income_group[0] <pl.col("personal_monthly_income")) & (pl.col("personal_monthly_income") <= income_group[1])).then(pl.lit(f"10 - {int(income_group[1]/1000000)}mil. VND"))
                            .when((income_group[1] <pl.col("personal_monthly_income")) & (pl.col("personal_monthly_income")  <= income_group[2])).then(pl.lit(f"20 - {int(income_group[2]/1000000)}mil. VND"))
                            .when((income_group[2] <pl.col("personal_monthly_income"))).then(pl.lit(f">{int(income_group[2]/1000000)}mil.++ VND")).alias("personal_monthly_income"))
    # Get the count branch to join
    dim_group_branch = income_personal.group_by(pl.col("Branch")).agg(pl.count())
    
    # Processing
    income_personal = income_personal.select(pl.col(["Branch","age","personal_monthly_income","total_spend"]))
    income_personal = income_personal.group_by(pl.col(["Branch","personal_monthly_income"])).agg((pl.count()).alias("personal_income_count"))
    income_personal = income_personal.join(dim_group_branch,on ="Branch",how="left")
    income_personal = income_personal.with_columns((pl.col("personal_income_count")/pl.col("count")*100).round(2).alias("percent_personal_income"))
    income_personal = income_personal.join(mapper_sort,on ="personal_monthly_income",how="left").sort(["Branch","values"])
                                    
    fig = px.bar(income_personal, x="percent_personal_income", y="Branch", color='personal_monthly_income', orientation='h',
                text= 'percent_personal_income',text_auto=True,
                hover_data={
                        "percent_personal_income":":,.2f",
                        "personal_income_count":":,.0f"},
                labels={"percent_personal_income":"% Personal Income Group"
                        },
                height=400,
                title='MONTHLY PERSONAL INCOME %')
    
    return fig,income_personal.drop("values") 


def purchase_personal_income(df_Aeon:DataFrame):
    income_group = [10000000,20000000,40000000]
    dict_sort ={"<10mil. VND":"0",
                "10 - 20mil. VND":"1",
                "20 - 40mil. VND":"2",
                ">40mil.++ VND":"3" }
    
    mapper_sort=DataFrame({
    "personal_monthly_income": list(dict_sort.keys()),
    "values": list(dict_sort.values())
    })
    
    income_personal = df_Aeon.with_columns(pl.when(pl.col("personal_monthly_income") <= income_group[0]).then(pl.lit(f"< {int(income_group[0]/1000000)}mil. VND"))
                            .when((income_group[0] <pl.col("personal_monthly_income")) & (pl.col("personal_monthly_income") <= income_group[1])).then(pl.lit(f"10 - {int(income_group[1]/1000000)}mil. VND"))
                            .when((income_group[1] <pl.col("personal_monthly_income")) & (pl.col("personal_monthly_income")  <= income_group[2])).then(pl.lit(f"20 - {int(income_group[2]/1000000)}mil. VND"))
                            .when((income_group[2] <pl.col("personal_monthly_income"))).then(pl.lit(f">{int(income_group[2]/1000000)}mil.++ VND")).alias("personal_monthly_income"))
    income_personal = income_personal.group_by(pl.col(["age","personal_monthly_income"])).agg(pl.sum("total_spend"))
    income_personal = income_personal.join(mapper_sort,on ="personal_monthly_income",how="left").sort(["age","values"])
    
    fig = px.bar(income_personal, x="personal_monthly_income", y={"total_spend":':,.0f'},
             color='age', barmode='stack',text_auto=True, title="Total Monthly Purchase by Personal Income Group",
             labels={'age':"Age group",
                     "personal_monthly_income":"Personal Income Group",
                     "value":"Total Monthly Purchase"
                     },
             height=400)
    return fig,income_personal.drop("values")


