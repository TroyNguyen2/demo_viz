# libs 
import polars as pl  
import streamlit as st  
from PIL import Image

# pipelines
from agegroup_pipe import plot_age,daily_monthly_purchase_age,pie_purchase
from reason_purpose_pipe import reason_visit,purpose_visit
from map_pipe import map_plot
from mean_household_personal_pipe import household_income,household_person_age_3d,personal_income
from household_personal_pipe import monthly_household_income_percent,monthly_personal_income_percent,purchase_personal_income

img = Image.open("Tung.png")
st.set_page_config(page_title="Demo", page_icon=Image.open("dashboard.png"), layout="wide")
st.sidebar.image(img,caption="Author: Nguyen Thanh Tung  \n \n \n  Please contact me: 093 114 0296  \n \n \n    Main role: Data Engineer / Analyst")

# ---- READ EXCEL ----
@st.cache_resource  
def get_data_from_excel():
    df = pl.scan_parquet("data_clean_demo.parquet")
    return df
def convert_df(df:pl.DataFrame):
    return df.to_pandas().to_csv(index="False").encode("utf-8-sig")
def generate_download_button(csv_data,filename,file_label):
    st.download_button(label=f"Download {file_label} as CSV",
                       data=csv_data,
                       file_name=f"{filename}.csv")
def click_button():
    st.session_state.button = not st.session_state.button
df = get_data_from_excel()
df_unique = df.select(["Branch","age","Gender","Date_Name","Buy_statement"]).collect()
# ---- SIDEBAR ----
st.sidebar.header("Please Filter Here:")
branch = st.sidebar.multiselect(
    "Select the Branch name:",
    options= df_unique.select("Branch").unique(maintain_order=True)["Branch"].to_pandas(),
    default= df_unique.select("Branch").unique(maintain_order=True)["Branch"].to_pandas()
)
age = st.sidebar.multiselect(
    "Select the Age group:",
    options= df_unique.select("age").unique(maintain_order=True)["age"].to_pandas(),
    default= df_unique.select("age").unique(maintain_order=True)["age"].to_pandas(),
)
gender = st.sidebar.multiselect(
    "Select the Gender:",
    options=  df_unique.select("Gender").unique(maintain_order=True)["Gender"].to_pandas(),
    default=  df_unique.select("Gender").unique(maintain_order=True)["Gender"].to_pandas()
)
day_type = st.sidebar.multiselect(
    "Select the in Weekday/Weekend",
    options=  df_unique.select("Date_Name").unique(maintain_order=True)["Date_Name"].to_pandas(),
    default=  df_unique.select("Date_Name").unique(maintain_order=True)["Date_Name"].to_pandas()
)
buy_statement = st.sidebar.multiselect(
    "Select the Buy statement:",
    options=  df_unique.select("Buy_statement").unique(maintain_order=True)["Buy_statement"].to_pandas(),
    default=  df_unique.select("Buy_statement").unique(maintain_order=True)["Buy_statement"].to_pandas()
)
mask = (pl.col("Branch").is_in(branch) & pl.col("Gender").is_in(gender) & pl.col("age").is_in(age) & pl.col("Date_Name").is_in(day_type) & pl.col("Buy_statement").is_in(buy_statement))
df_selection = df.filter(mask).collect()

# Check if the dataframe is empty:
if df_selection.is_empty():
    st.warning("No data available based on the current filter settings!")
    st.stop() # This will halt the app from further execution.

# ---- MAINPAGE ----
st.title("	:coffee: Demo Analytics Report")
st.markdown("##")

total_spending           = int(df_selection["total_spend"].sum())
average_rating           = round(df_selection["Rating"].mean(), 1) 
star_rating              = ":star:" * int(round(average_rating, 0))
mean_comsumption_value   = int(df_selection["total_spend"].mean())
median_comsumption_value = int(df_selection["total_spend"].median())

left_column, middle_column, right_column = st.columns(3)

with left_column:
    st.subheader("Total Purchase:")
    st.subheader("{:,} VND".format(total_spending))
    st.plotly_chart(pie_purchase(df_Aeon=df_selection))
with middle_column:
    st.subheader("Average Rating: {} / 10".format(str(average_rating)) )
    st.subheader(star_rating)
with right_column:
    st.subheader("Mean Spending per month:")
    st.subheader("{:,} VND ".format(mean_comsumption_value))
    st.subheader("Willing to Buy per month:")
    st.subheader("{:,} VND ".format(median_comsumption_value))
st.markdown("""---""")

#### Plot 1 - 2 - df
fig_0, df_age = plot_age(df_selection)
fig_1, fig_2 ,df_daily_monthly_purchase_age = daily_monthly_purchase_age(df_selection)

left_column ,right_column = st.columns(2)
left_column.plotly_chart(fig_0,theme="streamlit", use_container_width=True)
csv_age = convert_df(df_age)
generate_download_button(csv_age,"age","age")

with right_column:
    if 'button' not in st.session_state:
        st.session_state.button = False
    st.button('View: Daily - Monthly Purchase', on_click=click_button)
    if st.session_state.button:
        # The message and nested widget will remain on the page
        st.plotly_chart(fig_2,theme="streamlit", use_container_width=True)
    else:
        st.plotly_chart(fig_1,theme="streamlit", use_container_width=True)
    csv_daily_monthly_purchase_age = convert_df(df_daily_monthly_purchase_age)
    generate_download_button(csv_daily_monthly_purchase_age,"daily_monthly_purchase_age","daily_monthly_purchase_age")
st.markdown("""---""")


#### Plot 4 - 5 - df
fig_4 ,df_reason_visit = reason_visit(df_selection)
fig_5 ,df_purpose_visit = purpose_visit(df_selection)

left_column ,right_column = st.columns(2)
with left_column:
    st.plotly_chart(fig_4, theme="streamlit", use_container_width=True)
    csv_reason_visit = convert_df(df_reason_visit)
    generate_download_button(csv_reason_visit,"reason_group","reason_group")
with right_column:
    st.plotly_chart(fig_5,theme="streamlit", use_container_width=True)
    csv_purpose_visit = convert_df(df_purpose_visit)
    generate_download_button(csv_purpose_visit,"purpose_group","purpose_group")
st.markdown("""---""")

# #### Plot6 - df

fig_7  = household_person_age_3d(df_selection) #,household_income_2(df_selection)
st.plotly_chart(fig_7, theme="streamlit",use_container_width=True)
# right_column.plotly_chart(fig_13,theme="streamlit", use_container_width=True)
st.markdown("""---""")

#### Plot 8 - 9 - df
fig_8, df_person_income_avg   = personal_income(df_selection)
fig_9, df_household_income_avg=household_income(df_selection)
left_column ,right_column = st.columns(2)
with left_column:
    st.plotly_chart(fig_8, theme="streamlit",use_container_width=True)
    csv_person_income_avg = convert_df(df_person_income_avg)
    generate_download_button(csv_person_income_avg,"person_income_avg","person_income_avg")
with right_column:
    st.plotly_chart(fig_9, theme="streamlit",use_container_width=True)
    csv_household_income_avg = convert_df(df_household_income_avg)
    generate_download_button(csv_household_income_avg,"household_income_avg","household_income_avg")
st.markdown("""---""")

fig_10 ,df_purchase_personal_income = purchase_personal_income(df_selection)
st.plotly_chart(fig_10, theme="streamlit", use_container_width=True)
csv_purchase_personal_income = convert_df(df_purchase_personal_income)
generate_download_button(csv_purchase_personal_income,"Total Monthly Purchase by Personal Income Group","Total Monthly Purchase by Personal Income Group")
st.markdown("""---""")

left_column ,right_column = st.columns(2)
fig_11,df_spend_branch_income = monthly_household_income_percent(df_selection)
fig_12,df_spend_branch_income = monthly_personal_income_percent(df_selection)

with right_column:
    st.plotly_chart(fig_11, theme="streamlit",use_container_width=True)
    csv_household_income = convert_df(df_spend_branch_income)
    generate_download_button(csv_household_income,"monthly_household_income","monthly_household_income")
with left_column:
    st.plotly_chart(fig_12, theme="streamlit",use_container_width=True)
    csv_personal_income = convert_df(df_spend_branch_income)
    generate_download_button(csv_personal_income,"monthly_personal_income","monthly_personal_income")
st.markdown("""---""")

### Map 13
fig_13= map_plot(df_selection)
st.plotly_chart(fig_13, theme="streamlit",use_container_width=True)
st.markdown("""---""")

# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
