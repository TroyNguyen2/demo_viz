import polars as pl
from polars import DataFrame

df_Aeon = pl.read_excel(r"E:\test_dash\_Aeon\Aeon_Vietnam_Data.xlsx")

def clean_processing(df_Aeon:str) -> DataFrame:
    drop_cols=['Unnamed: 0',	'Response ID','Response started', 'Response completed','Collector Name','Input Gate Code',
               'Startdate_Correct', 'Enddate_Correct', 'Startdate_correct_hour', 'Enddate_Correct_hour', 'Day','Time taken (min)',
               'Input Surveyor Code','Input Surveyor Code','Address','Unique_ID']
    rename_dict={"Table Names":"Branch","Please select your gender.":"Gender",
                 "How many people (including yourself) came together to this mall today? (Select one)":"People_come_with",
                 "What are your main means of transport? (Select one)":"Main_transport",
                 "How long did it take to arrive from your house to this mall? (Minute)":"Arrive_time",
                 "How many cars and motorcyles do you own at home? (Unit)    (Private vehicle)  (Car(s))":"Cars",
                 "How many cars and motorcyles do you own at home? (Unit)    (Private vehicle)  (Motorcycle(s))":"Motorcycles",
                 "How long did you stay in this mall today? (Minute)":"Time_stay",
                 "Did you buy anything at this mall?":"Buy_statement",
                 "What is your personal monthly income?":"personal_monthly_income",
                 "What is your household monthly income?":"household_monthly_income",
                 "How many times in a month do you come to this mall?":"Time_visit",
                 "Which brands do you want to be open in this mall?  (1)":"brand_wanted_1",
                 "Which brands do you want to be open in this mall?  (2)":"brand_wanted_2",
                 "Which brands do you want to be open in this mall?  (3)":"brand_wanted_3",
                 "Which brands do you want to be open in this mall?  (4)":"brand_wanted_4",
                 "Which brands do you want to be open in this mall?  (5)":"brand_wanted_5",
                 "ASK FOR ALL / SHOWCARD  (How much is your OVERALL LIKING (from all elements, feelings, experiences) for this AEONMALL?)":"Rating",
                 "Please tell us the reasons for you coming to this AEON Mall today? (Select multiple)":"Reason_vist",
                 "<Mall   area (exclude AEON)>":"Purpose_visit"}
    
    # Segment age values into 4 agegroups
    mapper_age = {"12～15 years old (middle school)"  :"12~24",
                  "15～19 years old"                  :"12~24",
                  "20～24 years old"                  :"12~24",
                  "25～29 years old"                  :"25~39",
                  "30～34 years old"                  :"25~39",
                  "35～39 years old"                  :"25~39",
                  "40～44 years old"                  :"40~59",
                  "45～49 years old"                  :"40~59",
                  "50～54 years old"                  :"40~59",
                  "55～59 years old"                  :"40~59", # ELSE 60+
                  "60～64 years old"                  :"60+",
                  "65～69 years old"                  :"60+",
                  "70～79 years old"                  :"60+",
                  "80 years old and above"            :"60+",}
    mapper_rating ={"I like this AEONMALL very much": "10" ,
                    "Neutral, neither like nor dislike":"5" ,
                    "I don’t like this AEONMALL at all": "0",}

    dim_spend=['How much did you spend on the AEON sales floor today? (nghìn VND)  (AEON sales floor)  (Food)',
                'How much did you spend on the AEON sales floor today? (nghìn VND)  (AEON sales floor)  (Cosmetics・Pharmaceuticals・Daily consuma',
                  'How much did you spend on the AEON sales floor today? (nghìn VND)  (AEON sales floor)  (Clothing (Clothing, Traditional clothin',
                    'How much did you spend on the AEON sales floor today? (nghìn VND)  (AEON sales floor)  (Housing・Daily Goods (Bedding, interior,',
                      'How much did you spend on the AEON sales floor today? (nghìn VND)  (AEON sales floor)  (Delivery/Takeaway food (side dishes, su', 
                      'How much did you spend at the AEON MALL Specialty Stores today? (nghìn VND)  (AEON MALL Specialty Stores)  (Food)',
                        'How much did you spend at the AEON MALL Specialty Stores today? (nghìn VND)  (AEON MALL Specialty Stores)  (Cosmetics・Pharmaceu',
                          'How much did you spend at the AEON MALL Specialty Stores today? (nghìn VND)  (AEON MALL Specialty Stores)  (Clothing (Clothing,',
                            'How much did you spend at the AEON MALL Specialty Stores today? (nghìn VND)  (AEON MALL Specialty Stores)  (Housing・Daily Goods', 
                            'How much did you spend on the Service related sales floor   today? (nghìn VND)  (Service related sales floor)  (Foodcourt)',
                              'How much did you spend on the Service related sales floor   today? (nghìn VND)  (Service related sales floor)  (Restaurant)',
                                'How much did you spend on the Service related sales floor   today? (nghìn VND)  (Service related sales floor)  (Amusement)', 
                                'How much did you spend on the Service related sales floor   today? (nghìn VND)  (Service related sales floor)  (Cinema)',
                                  'How much did you spend on the Service related sales floor   today? (nghìn VND)  (Service related sales floor)  (Other Services)']
    
    df_Aeon  = df_Aeon.drop(drop_cols)
    df_Aeon  = df_Aeon.rename(rename_dict)

    dim_day   ="Time_visit"    
    
    """ Repalce the age with the mapper_age dict
        Replace null with 0 
        Get float number in personal_monthly_income then clean null
        calculate monthly purchase by let dim_spend * dim_day """ 
    
    # 1st processing
    df_Aeon     = df_Aeon.with_columns(pl.col("What is your age?").map_dict(mapper_age, default=pl.col("What is your age?")).alias("age"),
                                       pl.col("Rating").map_dict(mapper_rating, default=pl.col("Rating")),
                                       pl.col("personal_monthly_income").str.extract_all(r"[-+]?(?:\d*\.*\d+)").cast(pl.List(pl.Utf8),strict=False).list.join(", ").fill_null(strategy="zero"),
                                       pl.col("household_monthly_income").str.extract_all(r"[-+]?(?:\d*\.*\d+)").cast(pl.List(pl.Utf8),strict=False).list.join(", ").fill_null(strategy="min"),
                                       pl.col(dim_spend).fill_null(strategy="zero"), 
                                       pl.col("Buy_statement").str.replace(r"(?i), I bought something|, I didn't buy anything", ""),
                                       pl.col("People_come_with").str.extract_all(r"\d+").cast(pl.List(pl.Utf8),strict=False).list.join(", ").fill_null(strategy="zero"),
                                       pl.col("How many passengers (including yourself) rode with you? (Select one)").str.extract_all(r"\d+").cast(pl.List(pl.Utf8),strict=False).list.join(", ").fill_null(strategy="min"),
                                       )
                              
    # 2nd processing
    df_Aeon     = df_Aeon.with_columns(
                                       (pl.sum_horizontal(dim_spend)*pl.col(dim_day)*700).alias("total_spend"),
                                       (pl.col("personal_monthly_income").cast(pl.Float32,strict=False)*32000),
                                       (pl.col("household_monthly_income").cast(pl.Float32,strict=False)*85000),
                                       pl.col("People_come_with").cast(pl.Float32,strict=False),
                                       pl.col("How many passengers (including yourself) rode with you? (Select one)").cast(pl.Float32,strict=False),
                                       pl.col("Rating").cast(pl.Int8,strict=False),
                                       )                                                                 
    df_Aeon     = df_Aeon.with_columns((pl.col("personal_monthly_income")).fill_null(strategy="zero"),
                                       (pl.sum_horizontal(dim_spend)*700).alias("day_spend")).sort("Branch")
    
    return df_Aeon.drop(dim_spend).write_parquet("demo\data_clean_demo.parquet")
clean_processing(df_Aeon)
