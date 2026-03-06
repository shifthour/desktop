# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from IDS INDV_BE_CCHG_RPLC table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi               4/8/2022       US-500022                   New Job to Extract data from IDS           EnterpriseDev2        Reddy Sanam              04/13/2022

# MAGIC Read all the Data from IDS INDV_BE_CCHG_RPLC Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Write INDV_BE_CCHG_F_RPLC Data into a Sequential file for Load Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_in = f"""SELECT 
IBC.INDV_BE_CCHG_RPLC_SK as INDV_BE_CCHG_SK, 
IBC.INDV_BE_KEY, 
IBC.CCHG_STRT_YR_MO_SK, 
IBC.PRCS_YR_MO_SK, 
IBC.SRC_SYS_CD_SK, 
IBC.CRT_RUN_CYC_EXCTN_SK, 
IBC.LAST_UPDT_RUN_CYC_EXCTN_SK, 
IBC.CCHG_MULT_CAT_GRP_RPLC_PRI_SK as CCHG_MULT_CAT_GRP_PRI_SK, 
IBC.CCHG_MULT_CAT_GRP_RPLC_SEC_SK as CCHG_MULT_CAT_GRP_SEC_SK, 
IBC.CCHG_MULT_CAT_GRP_RPLC_TRTY_SK as CCHG_MULT_CAT_GRP_TRTY_SK, 
IBC.CCHG_MULT_CAT_GRP_RPLC_4TH_SK as CCHG_MULT_CAT_GRP_4TH_SK, 
IBC.CCHG_MULT_CAT_GRP_RPLC_5TH_SK as CCHG_MULT_CAT_GRP_5TH_SK, 
IBC.CCHG_END_YR_MO_SK, 
IBC.CCHG_CT, 
COALESCE(CD1.TRGT_CD, 'UNK') as SRC_SYS_CD,
IBC.VRSN_ID
FROM {IDSOwner}.INDV_BE_CCHG_RPLC AS IBC
LEFT OUTER JOIN
{IDSOwner}.CD_MPPNG AS CD1
ON IBC.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK
WHERE
IBC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_db2_INDV_BE_CCHG_RPLC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_in)
    .load()
)

extract_query_cchg = f"""SELECT 
CCHG_MULT_CAT_GRP_RPLC_SK as CCHG_MULT_CAT_GRP_SK,
CCHG_MULT_CAT_GRP_ID
FROM {IDSOwner}.CCHG_MULT_CAT_GRP_RPLC
"""

df_db2_CCHG_MULT_CAT_GRP_RPLC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_cchg)
    .load()
)

df_grp1 = df_db2_CCHG_MULT_CAT_GRP_RPLC_in
df_grp2 = df_db2_CCHG_MULT_CAT_GRP_RPLC_in
df_grp3 = df_db2_CCHG_MULT_CAT_GRP_RPLC_in
df_grp4 = df_db2_CCHG_MULT_CAT_GRP_RPLC_in
df_grp5 = df_db2_CCHG_MULT_CAT_GRP_RPLC_in

df_lookup = (
    df_db2_INDV_BE_CCHG_RPLC_in.alias("IdsOut")
    .join(df_grp1.alias("Grp1"), F.col("IdsOut.CCHG_MULT_CAT_GRP_PRI_SK") == F.col("Grp1.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_grp2.alias("Grp2"), F.col("IdsOut.CCHG_MULT_CAT_GRP_SEC_SK") == F.col("Grp2.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_grp3.alias("Grp3"), F.col("IdsOut.CCHG_MULT_CAT_GRP_TRTY_SK") == F.col("Grp3.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_grp4.alias("Grp4"), F.col("IdsOut.CCHG_MULT_CAT_GRP_4TH_SK") == F.col("Grp4.CCHG_MULT_CAT_GRP_SK"), "left")
    .join(df_grp5.alias("Grp5"), F.col("IdsOut.CCHG_MULT_CAT_GRP_5TH_SK") == F.col("Grp5.CCHG_MULT_CAT_GRP_SK"), "left")
    .select(
        F.col("IdsOut.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
        F.col("IdsOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("IdsOut.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
        F.col("IdsOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("IdsOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("IdsOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("IdsOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IdsOut.CCHG_MULT_CAT_GRP_PRI_SK").alias("CCHG_MULT_CAT_GRP_PRI_SK"),
        F.col("IdsOut.CCHG_MULT_CAT_GRP_SEC_SK").alias("CCHG_MULT_CAT_GRP_SEC_SK"),
        F.col("IdsOut.CCHG_MULT_CAT_GRP_TRTY_SK").alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
        F.col("IdsOut.CCHG_MULT_CAT_GRP_4TH_SK").alias("CCHG_MULT_CAT_GRP_4TH_SK"),
        F.col("IdsOut.CCHG_MULT_CAT_GRP_5TH_SK").alias("CCHG_MULT_CAT_GRP_5TH_SK"),
        F.col("IdsOut.CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
        F.col("IdsOut.CCHG_CT").alias("CCHG_CT"),
        F.col("IdsOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Grp1.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_1"),
        F.col("Grp2.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_2"),
        F.col("Grp3.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_3"),
        F.col("Grp4.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_4"),
        F.col("Grp5.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID_5"),
        F.col("IdsOut.VRSN_ID").alias("VRSN_ID")
    )
)

df_xfm_data = (
    df_lookup
    .filter((F.col("INDV_BE_CCHG_SK") != 1) & (F.col("INDV_BE_CCHG_SK") != 0))
    .select(
        F.col("INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
        F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CCHG_MULT_CAT_GRP_PRI_SK").alias("CCHG_MULT_CAT_GRP_PRI_SK"),
        F.col("CCHG_MULT_CAT_GRP_SEC_SK").alias("CCHG_MULT_CAT_GRP_SEC_SK"),
        F.col("CCHG_MULT_CAT_GRP_TRTY_SK").alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
        F.col("CCHG_MULT_CAT_GRP_4TH_SK").alias("CCHG_MULT_CAT_GRP_4TH_SK"),
        F.col("CCHG_MULT_CAT_GRP_5TH_SK").alias("CCHG_MULT_CAT_GRP_5TH_SK"),
        F.col("CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
        F.col("CCHG_CT").alias("CCHG_CT"),
        F.when(F.col("CCHG_MULT_CAT_GRP_ID_1").isNull(), F.lit("UNK")).otherwise(F.col("CCHG_MULT_CAT_GRP_ID_1")).alias("CCHG_MULT_CAT_GRP_PRI_ID"),
        F.when(F.col("CCHG_MULT_CAT_GRP_ID_2").isNull(), F.lit("UNK")).otherwise(F.col("CCHG_MULT_CAT_GRP_ID_2")).alias("CCHG_MULT_CAT_GRP_SEC_ID"),
        F.when(F.col("CCHG_MULT_CAT_GRP_ID_3").isNull(), F.lit("UNK")).otherwise(F.col("CCHG_MULT_CAT_GRP_ID_3")).alias("CCHG_MULT_CAT_GRP_TRTY_ID"),
        F.when(F.col("CCHG_MULT_CAT_GRP_ID_4").isNull(), F.lit("UNK")).otherwise(F.col("CCHG_MULT_CAT_GRP_ID_4")).alias("CCHG_MULT_CAT_GRP_4TH_ID"),
        F.when(F.col("CCHG_MULT_CAT_GRP_ID_5").isNull(), F.lit("UNK")).otherwise(F.col("CCHG_MULT_CAT_GRP_ID_5")).alias("CCHG_MULT_CAT_GRP_5TH_ID"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("VRSN_ID").alias("VRSN_ID")
    )
)

df_na_out = spark.createDataFrame(
    [
        (
            1,
            1,
            '175301',
            '175301',
            'NA',
            '1753-01-01',
            '1753-01-01',
            1,
            1,
            1,
            1,
            1,
            '219912',
            0,
            'NA',
            'NA',
            'NA',
            'NA',
            'NA',
            100,
            100,
            100,
            'NA'
        )
    ],
    [
        "INDV_BE_CCHG_SK",
        "INDV_BE_KEY",
        "CCHG_STRT_YR_MO_SK",
        "PRCS_YR_MO_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CCHG_MULT_CAT_GRP_PRI_SK",
        "CCHG_MULT_CAT_GRP_SEC_SK",
        "CCHG_MULT_CAT_GRP_TRTY_SK",
        "CCHG_MULT_CAT_GRP_4TH_SK",
        "CCHG_MULT_CAT_GRP_5TH_SK",
        "CCHG_END_YR_MO_SK",
        "CCHG_CT",
        "CCHG_MULT_CAT_GRP_PRI_ID",
        "CCHG_MULT_CAT_GRP_SEC_ID",
        "CCHG_MULT_CAT_GRP_TRTY_ID",
        "CCHG_MULT_CAT_GRP_4TH_ID",
        "CCHG_MULT_CAT_GRP_5TH_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "VRSN_ID"
    ]
)

df_unk_out = spark.createDataFrame(
    [
        (
            0,
            0,
            '175301',
            '175301',
            'UNK',
            '1753-01-01',
            '1753-01-01',
            0,
            0,
            0,
            0,
            0,
            '219912',
            0,
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            'UNK',
            100,
            100,
            100,
            'UNK'
        )
    ],
    [
        "INDV_BE_CCHG_SK",
        "INDV_BE_KEY",
        "CCHG_STRT_YR_MO_SK",
        "PRCS_YR_MO_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CCHG_MULT_CAT_GRP_PRI_SK",
        "CCHG_MULT_CAT_GRP_SEC_SK",
        "CCHG_MULT_CAT_GRP_TRTY_SK",
        "CCHG_MULT_CAT_GRP_4TH_SK",
        "CCHG_MULT_CAT_GRP_5TH_SK",
        "CCHG_END_YR_MO_SK",
        "CCHG_CT",
        "CCHG_MULT_CAT_GRP_PRI_ID",
        "CCHG_MULT_CAT_GRP_SEC_ID",
        "CCHG_MULT_CAT_GRP_TRTY_ID",
        "CCHG_MULT_CAT_GRP_4TH_ID",
        "CCHG_MULT_CAT_GRP_5TH_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "VRSN_ID"
    ]
)

final_cols = [
    "INDV_BE_CCHG_SK",
    "INDV_BE_KEY",
    "CCHG_STRT_YR_MO_SK",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CCHG_MULT_CAT_GRP_PRI_SK",
    "CCHG_MULT_CAT_GRP_SEC_SK",
    "CCHG_MULT_CAT_GRP_TRTY_SK",
    "CCHG_MULT_CAT_GRP_4TH_SK",
    "CCHG_MULT_CAT_GRP_5TH_SK",
    "CCHG_END_YR_MO_SK",
    "CCHG_CT",
    "CCHG_MULT_CAT_GRP_PRI_ID",
    "CCHG_MULT_CAT_GRP_SEC_ID",
    "CCHG_MULT_CAT_GRP_TRTY_ID",
    "CCHG_MULT_CAT_GRP_4TH_ID",
    "CCHG_MULT_CAT_GRP_5TH_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VRSN_ID"
]

df_xfm_data_sel = df_xfm_data.select(final_cols)
df_na_out_sel = df_na_out.select(final_cols)
df_unk_out_sel = df_unk_out.select(final_cols)

df_fnl_data = df_na_out_sel.unionByName(df_unk_out_sel).unionByName(df_xfm_data_sel)

df_fnl_data = df_fnl_data.withColumn("CCHG_STRT_YR_MO_SK", rpad(F.col("CCHG_STRT_YR_MO_SK"), 6, " "))
df_fnl_data = df_fnl_data.withColumn("PRCS_YR_MO_SK", rpad(F.col("PRCS_YR_MO_SK"), 6, " "))
df_fnl_data = df_fnl_data.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_fnl_data = df_fnl_data.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_fnl_data = df_fnl_data.withColumn("CCHG_END_YR_MO_SK", rpad(F.col("CCHG_END_YR_MO_SK"), 6, " "))

write_files(
    df_fnl_data,
    f"{adls_path}/load/INDV_BE_CCHG_F_RPLC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)