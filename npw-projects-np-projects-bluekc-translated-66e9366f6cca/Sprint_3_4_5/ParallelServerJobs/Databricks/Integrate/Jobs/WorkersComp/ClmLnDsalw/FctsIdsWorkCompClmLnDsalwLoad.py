# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:         FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_DSALW table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer           Date             Project          Change Description                                                          Development Project   Code Reviewer     Date Reviewed       
# MAGIC ------------------------   ------------------   ------------------   --------------------------------------------------------------------------------------   ---------------------------------\(9)  --------------------------   -------------------------    \(9)
# MAGIC Kailashnath J      2017-01-18   5628             Original Programming\(9)\(9)\(9)              IntegrateDev2             Hugh Sisson         2017-03-17

# MAGIC Job to load the  DB2 table WORK_COMPNSTN_CLM_LN_DSALW
# MAGIC Read the load file created in the FctsIdsWorkCompClmLnDsalwExtr job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


WorkCompOwner = get_widget_value('WorkCompOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_WORK_COMPNSTN_CLM_LN_DSALW = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_LN_DSALW_EXCD_SK", IntegerType(), False),
    StructField("CLM_LN_DSALW_TYP_CAT_CD_SK", IntegerType(), False),
    StructField("DSALW_AMT", DecimalType(38,10), False)
])

df_seq_WORK_COMPNSTN_CLM_LN_DSALW = (
    spark.read
    .option("delimiter", ",")
    .option("header", "false")
    .schema(schema_seq_WORK_COMPNSTN_CLM_LN_DSALW)
    .csv(f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_DSALW.dat")
)

df_cpy_forBuffer = df_seq_WORK_COMPNSTN_CLM_LN_DSALW.select(
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_DSALW_EXCD_SK",
    "CLM_LN_DSALW_TYP_CAT_CD_SK",
    "DSALW_AMT"
)

df_final = df_cpy_forBuffer.select(
    rpad("CLM_ID", <...>, " ").alias("CLM_ID"),
    "CLM_LN_SEQ_NO",
    rpad("CLM_LN_DSALW_TYP_CD", <...>, " ").alias("CLM_LN_DSALW_TYP_CD"),
    rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD"),
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_DSALW_EXCD_SK",
    "CLM_LN_DSALW_TYP_CAT_CD_SK",
    "DSALW_AMT"
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLnDsalwLoad_IDS_WORK_COMPNSTN_CLM_LN_DSALW_temp",
    jdbc_url,
    jdbc_props
)

df_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLnDsalwLoad_IDS_WORK_COMPNSTN_CLM_LN_DSALW_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DSALW AS T
USING STAGING.FctsIdsWorkCompClmLnDsalwLoad_IDS_WORK_COMPNSTN_CLM_LN_DSALW_temp AS S
ON 
    T.CLM_ID = S.CLM_ID AND 
    T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO AND 
    T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD AND 
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_DSALW_EXCD_SK = S.CLM_LN_DSALW_EXCD_SK,
    T.CLM_LN_DSALW_TYP_CAT_CD_SK = S.CLM_LN_DSALW_TYP_CAT_CD_SK,
    T.DSALW_AMT = S.DSALW_AMT
WHEN NOT MATCHED THEN
    INSERT
    (
        CLM_ID,
        CLM_LN_SEQ_NO,
        CLM_LN_DSALW_TYP_CD,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_SK,
        LAST_UPDT_RUN_CYC_EXCTN_SK,
        CLM_LN_DSALW_EXCD_SK,
        CLM_LN_DSALW_TYP_CAT_CD_SK,
        DSALW_AMT
    )
    VALUES
    (
        S.CLM_ID,
        S.CLM_LN_SEQ_NO,
        S.CLM_LN_DSALW_TYP_CD,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        S.CLM_LN_DSALW_EXCD_SK,
        S.CLM_LN_DSALW_TYP_CAT_CD_SK,
        S.DSALW_AMT
    )
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)