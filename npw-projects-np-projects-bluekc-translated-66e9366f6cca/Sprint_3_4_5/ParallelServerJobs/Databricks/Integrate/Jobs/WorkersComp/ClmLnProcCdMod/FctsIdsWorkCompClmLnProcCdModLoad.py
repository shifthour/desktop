# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims Line ProcCdMod extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_PROC_CD_MOD table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                                           Change Description                 Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)--------------------------------------------------------------------------------------------        -------------------------------------------       ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-02-03\(9)5628 WORK_COMPNSTN_CLM_LN_PROC_CD_MOD\(9)      Original Programming\(9)      Integratedev2                              Kalyan Neelam           2017-02-22    
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_LN_PROC_CD_MOD
# MAGIC Read the load file created in the FctsIdsWorkCompClmLnProcCdModExtr job
# MAGIC Load the file created in FctsIdsWorkCompClmLnProcCdModExtr job into the DB2 table WORK_COMPNSTN_CLM_LN_PROC_CD_MOD
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')

schema_seq_WorkCompClmLnProcCdMod = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", IntegerType(), False),
    StructField("CLM_LN_PROC_CD_MOD_TX", StringType(), False)
])

df_seq_WorkCompClmLnProcCdMod = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_seq_WorkCompClmLnProcCdMod)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_PROC_CD_MOD.dat")
)

df_load_WorkCompClmLnProcCdMod = df_seq_WorkCompClmLnProcCdMod.select(
    df_seq_WorkCompClmLnProcCdMod["CLM_ID"],
    df_seq_WorkCompClmLnProcCdMod["CLM_LN_SEQ_NO"],
    df_seq_WorkCompClmLnProcCdMod["CLM_LN_PROC_CD_MOD_ORDNL_CD"],
    df_seq_WorkCompClmLnProcCdMod["SRC_SYS_CD"],
    df_seq_WorkCompClmLnProcCdMod["CRT_RUN_CYC_EXCTN_SK"],
    df_seq_WorkCompClmLnProcCdMod["LAST_UPDT_RUN_CYC_EXCTN_SK"],
    df_seq_WorkCompClmLnProcCdMod["CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"],
    df_seq_WorkCompClmLnProcCdMod["CLM_LN_PROC_CD_MOD_TX"]
)

df_final = (
    df_load_WorkCompClmLnProcCdMod
    .withColumn("CLM_ID", rpad("CLM_ID", 256, " "))
    .withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD", rpad("CLM_LN_PROC_CD_MOD_ORDNL_CD", 256, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", 256, " "))
    .withColumn("CLM_LN_PROC_CD_MOD_TX", rpad("CLM_LN_PROC_CD_MOD_TX", 256, " "))
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
        "CLM_LN_PROC_CD_MOD_TX"
    )
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLnProcCdModLoad_db2_WorkCompClmLnProcCdMod_temp", jdbc_url, jdbc_props)

df_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLnProcCdModLoad_db2_WorkCompClmLnProcCdMod_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_PROC_CD_MOD as T
USING STAGING.FctsIdsWorkCompClmLnProcCdModLoad_db2_WorkCompClmLnProcCdMod_temp as S
ON
    T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.CLM_LN_PROC_CD_MOD_ORDNL_CD = S.CLM_LN_PROC_CD_MOD_ORDNL_CD
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK = S.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK,
    T.CLM_LN_PROC_CD_MOD_TX = S.CLM_LN_PROC_CD_MOD_TX
WHEN NOT MATCHED THEN
INSERT (
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_LN_PROC_CD_MOD_ORDNL_CD,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    CLM_LN_PROC_CD_MOD_ORDNL_CD_SK,
    CLM_LN_PROC_CD_MOD_TX
)
VALUES (
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_LN_PROC_CD_MOD_ORDNL_CD,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK,
    S.CLM_LN_PROC_CD_MOD_TX
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)