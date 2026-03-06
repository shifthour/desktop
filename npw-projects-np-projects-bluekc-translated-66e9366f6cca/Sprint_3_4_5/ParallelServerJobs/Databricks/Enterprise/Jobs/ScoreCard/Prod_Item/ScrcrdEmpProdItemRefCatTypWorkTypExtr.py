# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC 
# MAGIC Modifications:
# MAGIC =====================================================================================================================================================================
# MAGIC  Developer\(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)07/01/2018\(9)5236-Indigo Replacement\(9)    Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)                 Kalyan Neelam                     2018-07-16

# MAGIC Remove duplicates on Scrcrd_Item_Cat, Scrcrd_Item_Typ and Scrcrd_Work_Typ
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ScoreCardOwner = get_widget_value('ScoreCardOwner','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')
APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS = get_widget_value('$APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS','1')
CurrentRunDtm = get_widget_value('CurrentRunDtm','')
Source = get_widget_value('Source','')

jdbc_url_scorecard, jdbc_props_scorecard = get_db_config(scorecard_secret_name)

extract_query_SCRCRD_ITEM_CAT = f"SELECT SCRCRD_ITEM_CAT_ID FROM {ScoreCardOwner}.SCRCRD_ITEM_CAT"
df_SCRCRD_ITEM_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_scorecard)
    .options(**jdbc_props_scorecard)
    .option("query", extract_query_SCRCRD_ITEM_CAT)
    .load()
)

extract_query_SCRCRD_ITEM_TYP = f"SELECT SCRCRD_ITEM_TYP_ID FROM {ScoreCardOwner}.SCRCRD_ITEM_TYP"
df_SCRCRD_ITEM_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_scorecard)
    .options(**jdbc_props_scorecard)
    .option("query", extract_query_SCRCRD_ITEM_TYP)
    .load()
)

extract_query_SCRCRD_WORK_TYP = f"SELECT SCRCRD_WORK_TYP_ID FROM {ScoreCardOwner}.SCRCRD_WORK_TYP"
df_SCRCRD_WORK_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_scorecard)
    .options(**jdbc_props_scorecard)
    .option("query", extract_query_SCRCRD_WORK_TYP)
    .load()
)

schema_Scrcrd_Empl_Prod_Item_Eval = T.StructType([
    T.StructField("EMPL_ID", T.StringType(), nullable=False),
    T.StructField("SCRCRD_ITEM_ID", T.StringType(), nullable=False),
    T.StructField("RCRD_EXTR_DTM", T.TimestampType(), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("SCRCRD_ITEM_CAT_ID", T.StringType(), nullable=False),
    T.StructField("SCRCRD_ITEM_TYP_ID", T.StringType(), nullable=False),
    T.StructField("SCRCRD_WORK_TYP_ID", T.StringType(), nullable=False),
    T.StructField("SRC_SYS_INPT_METH_CD", T.StringType(), nullable=True),
    T.StructField("SRC_SYS_STTUS_CD", T.StringType(), nullable=True),
    T.StructField("GRP_ID", T.StringType(), nullable=True),
    T.StructField("MBR_ID", T.StringType(), nullable=True),
    T.StructField("PROD_ABBR", T.StringType(), nullable=True),
    T.StructField("PROD_LOB_NO", T.StringType(), nullable=True),
    T.StructField("SCCF_NO", T.StringType(), nullable=True),
    T.StructField("SRC_SYS_RCVD_DTM", T.TimestampType(), nullable=False),
    T.StructField("SRC_SYS_CMPLD_DTM", T.TimestampType(), nullable=True),
    T.StructField("SRC_SYS_TRANS_DTM", T.TimestampType(), nullable=False),
    T.StructField("ACTVTY_CT", T.IntegerType(), nullable=True),
    T.StructField("CRT_DTM", T.TimestampType(), nullable=False),
    T.StructField("CRT_USER_ID", T.StringType(), nullable=False),
    T.StructField("LAST_UPDT_DTM", T.TimestampType(), nullable=False),
    T.StructField("LAST_UPDT_USER_ID", T.StringType(), nullable=False)
])

file_path_Scrcrd_Empl_Prod_Item_Eval = f"{adls_path}/<...>/Scrcrd_Empl_Prod_Item_Eval.dat"
df_Scrcrd_Empl_Prod_Item_Eval = (
    spark.read.format("csv")
    .option("sep", "#$")
    .option("quote", "\"")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_Scrcrd_Empl_Prod_Item_Eval)
    .load(file_path_Scrcrd_Empl_Prod_Item_Eval)
)

df_Xfm_Prod_Item = df_Scrcrd_Empl_Prod_Item_Eval.select(
    F.col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID")
)

df_rmv_dups_scrcrd_item_cat = dedup_sort(
    df_Xfm_Prod_Item,
    ["SCRCRD_ITEM_CAT_ID","SCRCRD_ITEM_TYP_ID","SCRCRD_WORK_TYP_ID"],
    [("SCRCRD_ITEM_CAT_ID","A"),("SCRCRD_ITEM_TYP_ID","A"),("SCRCRD_WORK_TYP_ID","A")]
)

df_lkp_Scrcrd_Item_Cat = (
    df_rmv_dups_scrcrd_item_cat.alias("prim")
    .join(
        df_SCRCRD_ITEM_CAT.alias("lkp"),
        F.col("prim.SCRCRD_ITEM_CAT_ID") == F.col("lkp.SCRCRD_ITEM_CAT_ID"),
        "left"
    )
    .select(
        F.col("prim.SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
        F.col("prim.SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
        F.col("prim.SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
        F.col("lkp.SCRCRD_ITEM_CAT_ID").alias("LKP_SCRCRD_ITEM_CAT_ID")
    )
)

df_xfm_Crcrd_Item_Typ = df_lkp_Scrcrd_Item_Cat.select(
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID")
)

df_rmv_dups_Scrcrd_Item_cat_out = df_lkp_Scrcrd_Item_Cat.filter(
    (F.col("LKP_SCRCRD_ITEM_CAT_ID").isNull()) & (F.col("SCRCRD_ITEM_CAT_ID") != "")
).select(
    F.col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID")
)

df_business_logic_Scrcrd_Item_Cat = df_rmv_dups_Scrcrd_Item_cat_out.select(
    F.col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
    F.col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_DESC"),
    F.lit("Y").alias("ACTV_IN"),
    F.lit(CurrentRunDtm).alias("CRT_DTM"),
    F.lit("System").alias("CRT_USER_ID"),
    F.lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    F.lit("System").alias("LAST_UPDT_USER_ID")
)

df_business_logic_Scrcrd_Item_Cat_out = df_business_logic_Scrcrd_Item_Cat.select(
    "SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_CAT_DESC",
    "ACTV_IN",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_business_logic_Scrcrd_Item_Cat_out,
    f"{adls_path}/load/Scrcrd_Item_Cat_Err_{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_lkp_Scrcrd_Item_Typ = (
    df_xfm_Crcrd_Item_Typ.alias("prim")
    .join(
        df_SCRCRD_ITEM_TYP.alias("lkp"),
        F.col("prim.SCRCRD_ITEM_TYP_ID") == F.col("lkp.SCRCRD_ITEM_TYP_ID"),
        "left"
    )
    .select(
        F.col("prim.SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
        F.col("prim.SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
        F.col("lkp.SCRCRD_ITEM_TYP_ID").alias("LKP_SCRCRD_ITEM_TYP_ID")
    )
)

df_rmv_dups_Scrcrd_Item_typ = df_lkp_Scrcrd_Item_Typ.filter(
    (F.col("LKP_SCRCRD_ITEM_TYP_ID").isNull()) & (F.col("SCRCRD_ITEM_TYP_ID") != "")
).select(
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID")
)

df_xfm_Scrcrd_Work_Typ = df_lkp_Scrcrd_Item_Typ.select(
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID")
)

df_bsns_Scrcrd_Item_Typ = df_rmv_dups_Scrcrd_Item_typ.select(
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_DESC"),
    F.lit("Y").alias("ACTV_IN"),
    F.lit(CurrentRunDtm).alias("CRT_DTM"),
    F.lit("System").alias("CRT_USER_ID"),
    F.lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    F.lit("System").alias("LAST_UPDT_USER_ID")
)

df_bsns_Scrcrd_Item_Typ_out = df_bsns_Scrcrd_Item_Typ.select(
    "SCRCRD_ITEM_TYP_ID",
    "SCRCRD_ITEM_TYP_DESC",
    "ACTV_IN",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_bsns_Scrcrd_Item_Typ_out,
    f"{adls_path}/load/Scrcrd_Item_Typ_Err_{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_lkp_Scrcrd_Work_Typ = (
    df_xfm_Scrcrd_Work_Typ.alias("prim")
    .join(
        df_SCRCRD_WORK_TYP.alias("lkp"),
        F.col("prim.SCRCRD_WORK_TYP_ID") == F.col("lkp.SCRCRD_WORK_TYP_ID"),
        "left"
    )
    .select(
        F.col("prim.SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
        F.col("lkp.SCRCRD_WORK_TYP_ID").alias("LKP_SCRCRD_WORK_TYP_ID")
    )
)

df_rmv_dups_Scrcrd_Work_Typ = df_lkp_Scrcrd_Work_Typ.filter(
    (F.col("LKP_SCRCRD_WORK_TYP_ID").isNull()) & (F.col("SCRCRD_WORK_TYP_ID") != "")
).select(
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID")
)

df_bsns_logic_Scrcrd_Work_Typ = df_rmv_dups_Scrcrd_Work_Typ.select(
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_DESC"),
    F.lit("Y").alias("ACTV_IN"),
    F.lit(CurrentRunDtm).alias("CRT_DTM"),
    F.lit("System").alias("CRT_USER_ID"),
    F.lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    F.lit("System").alias("LAST_UPDT_USER_ID")
)

df_bsns_logic_Scrcrd_Work_Typ_out = df_bsns_logic_Scrcrd_Work_Typ.select(
    "SCRCRD_WORK_TYP_ID",
    "SCRCRD_WORK_TYP_DESC",
    "ACTV_IN",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

write_files(
    df_bsns_logic_Scrcrd_Work_Typ_out,
    f"{adls_path}/load/Scrcrd_Work_Typ_Err_{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)