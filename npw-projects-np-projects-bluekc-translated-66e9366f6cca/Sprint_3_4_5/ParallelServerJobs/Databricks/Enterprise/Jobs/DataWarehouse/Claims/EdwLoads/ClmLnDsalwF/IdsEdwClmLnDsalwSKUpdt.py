# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                                                                                                           Code                    Date
# MAGIC Developer        Date              Altiris #                                                Change Description                                                        DataStage Proj                                             Reviewer             Reviewed
# MAGIC ----------------------  -------------------   -------------------                              -------------------------------------------------------------------------------------------           -------------------   -                                        -----------------------    -------------------   
# MAGIC Brent Leland    02/16/2006                                                            Originally Programmed
# MAGIC 
# MAGIC Rama Kamjula    09/17/2013     5114                                           Rewritten from Server to Parallel version                                                                                              Pete Marshall 
# MAGIC 
# MAGIC Pooja Sunkara  2014-01-21     5114 Project - Daptiv#688           Fixed the Nullability of column CLM_DIAG_SK                 EnterpriseWrhsDevl                                       Jag Yelavarthi                         2014-01-21
# MAGIC                                                                                                          on Lookup file

# MAGIC This Job do the check if there is any CLM_LN_DSALW sk updates.
# MAGIC Filtering Facets data
# MAGIC Daily load file loaded in IdsEdwBClmLnFLoad to check for the updated SK's
# MAGIC Load: Update
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# DB2ConnectorPX (Stage: db2_B_CLM_LN_F)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_B_CLM_LN_F = f"""
SELECT
 c.SRC_SYS_CD,
 c.CLM_ID,
 c.CLM_LN_SEQ_NO,
 c.CLM_LN_DSALW_TYP_CD,
 c.CLM_LN_DSALW_SK
FROM {EDWOwner}.CLM_LN_DSALW_F c,
     {EDWOwner}.B_CLM_LN_F b
WHERE c.SRC_SYS_CD = b.SRC_SYS_CD
  AND c.CLM_ID = b.CLM_ID
  AND c.CLM_LN_SEQ_NO = b.CLM_LN_SEQ_NO
"""
df_db2_B_CLM_LN_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_B_CLM_LN_F)
    .load()
)

# PxSequentialFile (Stage: seq_CLM_LN_DSALW_F)
schema_seq_CLM_LN_DSALW_F = StructType([
    StructField("CLM_LN_DSALW_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("EXCD_SK", IntegerType(), nullable=False),
    StructField("DSALW_AMT", DecimalType(38,10), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CAT_CD", StringType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CAT_CD_SK", IntegerType(), nullable=False)
])
df_seq_CLM_LN_DSALW_F = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_CLM_LN_DSALW_F)
    .load(f"{adls_path}/load/CLM_LN_DSALW_F.dat")
)

# CTransformerStage (Stage: xfm_Facets)
df_xfm_Facets = (
    df_seq_CLM_LN_DSALW_F
    .filter(trim(col("SRC_SYS_CD")) == "FACETS")
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        col("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK")
    )
)

# PxLookup (Stage: lkp_Codes), primary link = df_db2_B_CLM_LN_F, lookup link = df_xfm_Facets
df_lkp_Codes = (
    df_db2_B_CLM_LN_F.alias("lnk_BClmLnF")
    .join(
        df_xfm_Facets.alias("lnk_ClmLnDsalwF"),
        (
            (col("lnk_BClmLnF.SRC_SYS_CD") == col("lnk_ClmLnDsalwF.SRC_SYS_CD")) &
            (col("lnk_BClmLnF.CLM_ID") == col("lnk_ClmLnDsalwF.CLM_ID")) &
            (col("lnk_BClmLnF.CLM_LN_SEQ_NO") == col("lnk_ClmLnDsalwF.CLM_LN_SEQ_NO")) &
            (col("lnk_BClmLnF.CLM_LN_DSALW_TYP_CD") == col("lnk_ClmLnDsalwF.CLM_LN_DSALW_TYP_CD"))
        ),
        how="left"
    )
    .select(
        col("lnk_BClmLnF.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_BClmLnF.CLM_ID").alias("CLM_ID"),
        col("lnk_BClmLnF.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("lnk_BClmLnF.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
        col("lnk_ClmLnDsalwF.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK_ClmLn"),
        col("lnk_BClmLnF.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK_Bal")
    )
)

# CTransformerStage (Stage: xfm_BusinessLogic)
df_xfm_BusinessLogic_temp = df_lkp_Codes.filter(
    (col("CLM_LN_DSALW_SK_ClmLn").isNotNull()) &
    (col("CLM_LN_DSALW_SK_Bal") != col("CLM_LN_DSALW_SK_ClmLn")) &
    (col("CLM_LN_DSALW_SK_ClmLn") != lit(0))
)
df_xfm_BusinessLogic = df_xfm_BusinessLogic_temp.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    col("CLM_LN_DSALW_SK_ClmLn").alias("CLM_LN_DSALW_SK")
)

# DB2ConnectorPX (Stage: db2_CLM_LN_DSALW_F) - Merge logic
temp_table_name = "STAGING.IdsEdwClmLnDsalwSKUpdt_db2_CLM_LN_DSALW_F_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

(
    df_xfm_BusinessLogic.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql_db2_CLM_LN_DSALW_F = f"""
MERGE INTO {EDWOwner}.CLM_LN_DSALW_F AS T
USING {temp_table_name} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.CLM_ID = S.CLM_ID AND
    T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO AND
    T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD
WHEN MATCHED THEN
    UPDATE SET
        T.SRC_SYS_CD = S.SRC_SYS_CD,
        T.CLM_ID = S.CLM_ID,
        T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO,
        T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD,
        T.CLM_LN_DSALW_SK = S.CLM_LN_DSALW_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        CLM_ID,
        CLM_LN_SEQ_NO,
        CLM_LN_DSALW_TYP_CD,
        CLM_LN_DSALW_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.CLM_ID,
        S.CLM_LN_SEQ_NO,
        S.CLM_LN_DSALW_TYP_CD,
        S.CLM_LN_DSALW_SK
    )
;
"""
execute_dml(merge_sql_db2_CLM_LN_DSALW_F, jdbc_url, jdbc_props)

# Create a DataFrame for rejects (lnk_lnk_ClsLnDsalwSKUpdt_Rej) and write to seq_CLM_LN_DSALW_F_Rej
df_db2_CLM_LN_DSALW_F_rej = spark.createDataFrame(
    [],
    StructType([
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("CLM_ID", StringType(), True),
        StructField("CLM_LN_SEQ_NO", IntegerType(), True),
        StructField("CLM_LN_DSALW_TYP_CD", StringType(), True),
        StructField("CLM_LN_DSALW_SK", IntegerType(), True),
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)
df_db2_CLM_LN_DSALW_F_rej_final = df_db2_CLM_LN_DSALW_F_rej.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "CLM_LN_DSALW_SK",
    "ERRORCODE",
    "ERRORTEXT"
)
write_files(
    df_db2_CLM_LN_DSALW_F_rej_final,
    f"{adls_path}/load/CLM_LN_DSALW_F_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)