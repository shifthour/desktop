# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwDMProvDirSeq
# MAGIC 
# MAGIC PROCESSING:   Retrieves the data from EDW PDX_NTWK_D table and loads into the Web Provider Directory database
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #      \(9)Change Description                                        \(9)Development Project      Code Reviewer          Date Reviewed       
# MAGIC ---------------------------    --------------------     \(9)------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari\(9)2009-06-03\(9)3500             \(9)Original Programming\(9)\(9)\(9)devlEDWnew                  Brent Leland              06-09-2009   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarrn Gill              06/19/2013        5114                              Create Load File for DM Table PROV_DIR_DM_PDX_NTWK        EnterpriseWrhsDevl       Peter Marshall                9/3/2013

# MAGIC Write PROV_DIR_DM_PDX_NTWK Data into a Sequential file for Load Job EdwDmProvDirDmPdxNtwkLoad.
# MAGIC Read all the Data from PDX_NTWK_D Table
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: EdwDmProvDirDmPdxNtwkExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
LastUpdtDt = get_widget_value('LastUpdtDt','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

SQL_db2_PDX_NTWK_D_Cond1in = f"""
SELECT distinct
B.SRC_SYS_CD,
B.PROV_ID,
B.PDX_NTWK_CD,
B.PROV_SK,
B.PDX_NTWK_DIR_IN,
B.PDX_NTWK_EFF_DT_SK,
B.PDX_NTWK_TERM_DT_SK,
B.PDX_NTWK_NM
FROM {EDWOwner}.PDX_NTWK_D B,
{EDWOwner}.PROV_D PROV_D
WHERE
B.PROV_SK = PROV_D.PROV_SK AND
PROV_D.SRC_SYS_CD = 'NABP' AND
B.PDX_NTWK_CD = 'ESI0021' AND
PROV_D.PROV_PRI_PRCTC_ADDR_ST_CD IN ('MO', 'KS') AND
B.PDX_NTWK_TERM_DT_SK = '9999-12-31' AND
B.PDX_NTWK_DIR_IN = 'Y' AND
B.PDX_NTWK_SK NOT IN (0,1)
"""

df_db2_PDX_NTWK_D_Cond1in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", SQL_db2_PDX_NTWK_D_Cond1in)
    .load()
)

SQL_db2_PDX_NTWK_D_Cond2in = f"""
SELECT distinct
B.SRC_SYS_CD,
B.PROV_ID,
B.PDX_NTWK_CD,
B.PROV_SK,
B.PDX_NTWK_DIR_IN,
B.PDX_NTWK_EFF_DT_SK,
B.PDX_NTWK_TERM_DT_SK,
B.PDX_NTWK_NM
FROM {EDWOwner}.PDX_NTWK_D B,
{EDWOwner}.PROV_D PROV_D
WHERE
B.PROV_SK = PROV_D.PROV_SK AND
B.PDX_NTWK_CD = 'ESI9031' AND
B.PDX_NTWK_TERM_DT_SK = '9999-12-31' AND
((PROV_D.PROV_PRI_PRCTC_ADDR_ST_CD = 'MO' AND PROV_D.PROV_PRI_PRCTC_ADDR_CNTY_NM NOT IN ('CASS', 'CLAY', 'PLATTE', 'JACKSON'))
 OR
 (PROV_D.PROV_PRI_PRCTC_ADDR_ST_CD = 'KS' AND PROV_D.PROV_PRI_PRCTC_ADDR_CNTY_NM NOT IN ('JOHNSON', 'WYANDOTTE'))) AND
B.PDX_NTWK_DIR_IN = 'Y' AND
PROV_D.SRC_SYS_CD = 'NABP' AND
B.PDX_NTWK_SK NOT IN (0,1)
"""

df_db2_PDX_NTWK_D_Cond2in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", SQL_db2_PDX_NTWK_D_Cond2in)
    .load()
)

SQL_db2_PDX_NTWK_D_Cond3in = f"""
SELECT distinct
B.SRC_SYS_CD,
B.PROV_ID,
B.PDX_NTWK_CD,
B.PROV_SK,
B.PDX_NTWK_DIR_IN,
B.PDX_NTWK_EFF_DT_SK,
B.PDX_NTWK_TERM_DT_SK,
B.PDX_NTWK_NM
FROM {EDWOwner}.PDX_NTWK_D B,
{EDWOwner}.PROV_D PROV_D
WHERE
B.PROV_SK = PROV_D.PROV_SK AND
B.PDX_NTWK_CD = 'ESI9031' AND
B.PDX_NTWK_TERM_DT_SK = '9999-12-31' AND
PROV_D.PROV_PRI_PRCTC_ADDR_ST_CD NOT IN ('MO','KS') AND
B.PDX_NTWK_DIR_IN = 'Y' AND
PROV_D.SRC_SYS_CD = 'NABP' AND
B.PDX_NTWK_SK NOT IN (0,1)
"""

df_db2_PDX_NTWK_D_Cond3in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", SQL_db2_PDX_NTWK_D_Cond3in)
    .load()
)

df_Pdx_Funnel = df_db2_PDX_NTWK_D_Cond1in.select(
    "SRC_SYS_CD",
    "PROV_ID",
    "PDX_NTWK_CD",
    "PROV_SK",
    "PDX_NTWK_DIR_IN",
    "PDX_NTWK_EFF_DT_SK",
    "PDX_NTWK_TERM_DT_SK",
    "PDX_NTWK_NM"
).unionByName(
    df_db2_PDX_NTWK_D_Cond3in.select(
        "SRC_SYS_CD",
        "PROV_ID",
        "PDX_NTWK_CD",
        "PROV_SK",
        "PDX_NTWK_DIR_IN",
        "PDX_NTWK_EFF_DT_SK",
        "PDX_NTWK_TERM_DT_SK",
        "PDX_NTWK_NM"
    )
).unionByName(
    df_db2_PDX_NTWK_D_Cond2in.select(
        "SRC_SYS_CD",
        "PROV_ID",
        "PDX_NTWK_CD",
        "PROV_SK",
        "PDX_NTWK_DIR_IN",
        "PDX_NTWK_EFF_DT_SK",
        "PDX_NTWK_TERM_DT_SK",
        "PDX_NTWK_NM"
    )
)

df_xfrm_BusinessLogic = (
    df_Pdx_Funnel
    .withColumn("PROV_ID", F.col("PROV_ID"))
    .withColumn("PDX_NTWK_CD", F.col("PDX_NTWK_CD"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("PROV_SK", F.col("PROV_SK"))
    .withColumn("DIR_IN", F.col("PDX_NTWK_DIR_IN"))
    .withColumn(
        "EFF_DT",
        F.when(
            (trim(F.col("PDX_NTWK_EFF_DT_SK")).isin("NA", "UNK")),
            FORMAT.DATE.EE('1753-01-01', "DATE", "DATE", "SYBTIMESTAMP")
        ).otherwise(
            FORMAT.DATE.EE(F.col("PDX_NTWK_EFF_DT_SK"), "DATE", "DATE", "SYBTIMESTAMP")
        )
    )
    .withColumn(
        "TERM_DT",
        F.when(
            (trim(F.col("PDX_NTWK_TERM_DT_SK")).isin("NA", "UNK")),
            FORMAT.DATE.EE('2199-12-31', "DATE", "DATE", "SYBTIMESTAMP")
        ).otherwise(
            FORMAT.DATE.EE(F.col("PDX_NTWK_TERM_DT_SK"), "DATE", "DATE", "SYBTIMESTAMP")
        )
    )
    .withColumn(
        "PDX_NTWK_NM",
        F.when(F.col("PDX_NTWK_NM").isNull(), F.lit("UNK")).otherwise(F.col("PDX_NTWK_NM"))
    )
    .withColumn("LAST_UPDT_DT", StringToTimestamp(F.lit(LastUpdtDt), "%yyyy-%mm-%dd"))
    .withColumn("DIR_IN", F.rpad(F.col("DIR_IN"), 1, " "))
)

df_final = df_xfrm_BusinessLogic.select(
    "PROV_ID",
    "PDX_NTWK_CD",
    "SRC_SYS_CD",
    "PROV_SK",
    "DIR_IN",
    "EFF_DT",
    "TERM_DT",
    "PDX_NTWK_NM",
    "LAST_UPDT_DT"
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_DIR_DM_PDX_NTWK.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)