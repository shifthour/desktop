# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC PROCESSING : The job Extract Group data from UWS group table.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-17\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          IntegrateDev2                         Kalyan neelam            2018-06-11

# MAGIC Extract Group data from UWS group table.
# MAGIC Group data extract for UWS feed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Tax_Year = get_widget_value('Tax_Year', '')
RunID = get_widget_value('RunID', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
UWSOwner = get_widget_value('UWSOwner', '')
IDSOwner = get_widget_value('IDSOwner', '')
AsOfDtm = get_widget_value('AsOfDtm', '')
uws_secret_name = get_widget_value('uws_secret_name', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"SELECT GRP_ID, TAX_YR, SRC_SYS_CD, CRT_USER_ID, GRP_NM, GRP_CNTCT_FIRST_NM, GRP_CNTCT_MIDINIT, GRP_CNTCT_LAST_NM, GRP_ADDR_LN_1, GRP_ADDR_LN_2, GRP_ADDR_LN_3, GRP_ADDR_CITY_NM, GRP_ADDR_ST_CD, GRP_ADDR_ZIP_CD_5, GRP_ADDR_ZIP_CD_4 FROM {UWSOwner}.GRP_MA_DOR WHERE TAX_YR = '{Tax_Year}'"
df_UWS_Group_Data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT GRP_ID, TAX_YR, SRC_SYS_CD, AS_OF_DTM FROM {IDSOwner}.GRP_MA_DOR WHERE TAX_YR = '{Tax_Year}'"
df_IDS_GRP_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkup_Grp = df_UWS_Group_Data.alias("GroupInfoLookUp").join(
    df_IDS_GRP_MA_DOR.alias("lnk_GRP"),
    on=[
        col("GroupInfoLookUp.GRP_ID") == col("lnk_GRP.GRP_ID"),
        col("GroupInfoLookUp.TAX_YR") == col("lnk_GRP.TAX_YR"),
        col("GroupInfoLookUp.SRC_SYS_CD") == col("lnk_GRP.SRC_SYS_CD"),
    ],
    how="left"
)

df_Lkp_Grp = df_lkup_Grp.select(
    col("GroupInfoLookUp.GRP_ID").alias("GRP_ID"),
    col("GroupInfoLookUp.TAX_YR").alias("TAX_YR"),
    col("GroupInfoLookUp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GroupInfoLookUp.CRT_USER_ID").alias("CRT_USER_ID"),
    col("lnk_GRP.AS_OF_DTM").alias("AS_OF_DTM"),
    col("GroupInfoLookUp.GRP_NM").alias("GRP_NM"),
    col("GroupInfoLookUp.GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
    col("GroupInfoLookUp.GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
    col("GroupInfoLookUp.GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
    col("GroupInfoLookUp.GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    col("GroupInfoLookUp.GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    col("GroupInfoLookUp.GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    col("GroupInfoLookUp.GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
    col("GroupInfoLookUp.GRP_ADDR_ST_CD").alias("GRP_ADDR_ST_CD"),
    col("GroupInfoLookUp.GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
    col("GroupInfoLookUp.GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4"),
)

df_Tr_Format_Grp_Data = df_Lkp_Grp.select(
    col("GRP_ID").alias("GRP_ID"),
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(AsOfDtm).alias("AS_OF_DTM"),
    col("GRP_NM").alias("GRP_NM"),
    col("GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
    rpad(col("GRP_CNTCT_MIDINIT"), 1, " ").alias("GRP_CNTCT_MIDINIT"),
    col("GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
    col("GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    col("GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    col("GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    col("GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
    rpad(col("GRP_ADDR_ZIP_CD_5"), 5, " ").alias("GRP_ADDR_ZIP_CD_5"),
    rpad(col("GRP_ADDR_ZIP_CD_4"), 4, " ").alias("GRP_ADDR_ZIP_CD_4"),
    col("GRP_ADDR_ST_CD").alias("GRP_ST_CD"),
)

df_final = df_Tr_Format_Grp_Data.select(
    "GRP_ID",
    "TAX_YR",
    "SRC_SYS_CD",
    "AS_OF_DTM",
    "GRP_NM",
    "GRP_CNTCT_FIRST_NM",
    "GRP_CNTCT_MIDINIT",
    "GRP_CNTCT_LAST_NM",
    "GRP_ADDR_LN_1",
    "GRP_ADDR_LN_2",
    "GRP_ADDR_LN_3",
    "GRP_ADDR_CITY_NM",
    "GRP_ADDR_ZIP_CD_5",
    "GRP_ADDR_ZIP_CD_4",
    "GRP_ST_CD"
)

write_files(
    df_final,
    f"GRP_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)