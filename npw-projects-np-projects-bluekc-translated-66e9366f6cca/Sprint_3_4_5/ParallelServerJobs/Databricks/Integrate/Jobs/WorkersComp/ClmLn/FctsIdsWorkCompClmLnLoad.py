# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                 Change Description                                    Development Project          Code Reviewer              Date Reviewed       
# MAGIC ------------------                --------------------     \(9)---------------------------------------------------------------------      -----------------------------------------------------------         ------------------------------\(9)         ------------------------               ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-23\(9)5628 WORK_COMPNSTN_CLM_LN    \(9)            Original Programming\(9)\(9)\(9)Integratedev2                   Kalyan Neelam                2017-03-15
# MAGIC                                                                 (Facets to IDS) ETL Report

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM_LN
# MAGIC Read the load file created in the FactesIdsWorkCompnstnClmLnExtr job
# MAGIC Load the file created in FactesIdsWorkCompnstnClmLnExtr job into the DB2 table WORK_COMPNSTN_CLM_LN
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


WorkCompOwner = get_widget_value('WorkCompOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_seq_WorkCompnstnClmLn = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("PROC_CD_SK", IntegerType(), False),
    StructField("SVC_PROV_SK", IntegerType(), False),
    StructField("CLM_LN_PREAUTH_CD_SK", IntegerType(), False),
    StructField("CLM_LN_PREAUTH_SRC_CD_SK", IntegerType(), False),
    StructField("CLM_LN_RFRL_CD_SK", IntegerType(), False),
    StructField("SVC_END_DT", DateType(), False),
    StructField("SVC_STRT_DT", DateType(), False),
    StructField("AGMNT_PRICE_AMT", DecimalType(), False),
    StructField("ALW_AMT", DecimalType(), False),
    StructField("CHRG_AMT", DecimalType(), False),
    StructField("COINS_AMT", DecimalType(), False),
    StructField("CNSD_CHRG_AMT", DecimalType(), False),
    StructField("COPAY_AMT", DecimalType(), False),
    StructField("DEDCT_AMT", DecimalType(), False),
    StructField("DSALW_AMT", DecimalType(), False),
    StructField("ITS_HOME_DSCNT_AMT", DecimalType(), False),
    StructField("MBR_LIAB_BSS_AMT", DecimalType(), False),
    StructField("NO_RESP_AMT", DecimalType(), True),
    StructField("NON_PAR_SAV_AMT", DecimalType(), True),
    StructField("PATN_RESP_AMT", DecimalType(), True),
    StructField("PAYBL_AMT", DecimalType(), False),
    StructField("PAYBL_TO_SUB_AMT", DecimalType(), False),
    StructField("PAYBL_TO_PROV_AMT", DecimalType(), False),
    StructField("PROC_TBL_PRICE_AMT", DecimalType(), False),
    StructField("PROFL_PRICE_AMT", DecimalType(), False),
    StructField("PROV_WRT_OFF_AMT", DecimalType(), True),
    StructField("RISK_WTHLD_AMT", DecimalType(), False),
    StructField("SVC_PRICE_AMT", DecimalType(), False),
    StructField("SUPLMT_DSCNT_AMT", DecimalType(), False),
    StructField("PREAUTH_ID", StringType(), False),
    StructField("PREAUTH_SVC_SEQ_NO", StringType(), True),
    StructField("RFRL_SVC_SEQ_NO", StringType(), True)
])

df_seq_WorkCompnstnClmLn = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", None)
    .option("inferSchema", False)
    .option("nullValue", None)
    .schema(schema_seq_WorkCompnstnClmLn)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM_LN.dat")
)

df_cpy_forBuffer = df_seq_WorkCompnstnClmLn.select(
    "CLM_ID",
    "SRC_SYS_CD",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD_SK",
    "SVC_PROV_SK",
    "CLM_LN_PREAUTH_CD_SK",
    "CLM_LN_PREAUTH_SRC_CD_SK",
    "CLM_LN_RFRL_CD_SK",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "MBR_LIAB_BSS_AMT",
    "NO_RESP_AMT",
    "NON_PAR_SAV_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_SUB_AMT",
    "PAYBL_TO_PROV_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "PREAUTH_ID",
    "PREAUTH_SVC_SEQ_NO",
    "RFRL_SVC_SEQ_NO"
)

df_db2_WorkCompnstnClmLn = df_cpy_forBuffer
df_db2_WorkCompnstnClmLn = df_db2_WorkCompnstnClmLn.withColumn("CLM_ID", rpad("CLM_ID", <...>, " "))
df_db2_WorkCompnstnClmLn = df_db2_WorkCompnstnClmLn.withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
df_db2_WorkCompnstnClmLn = df_db2_WorkCompnstnClmLn.withColumn("PREAUTH_ID", rpad("PREAUTH_ID", <...>, " "))
df_db2_WorkCompnstnClmLn = df_db2_WorkCompnstnClmLn.withColumn("PREAUTH_SVC_SEQ_NO", rpad("PREAUTH_SVC_SEQ_NO", <...>, " "))
df_db2_WorkCompnstnClmLn = df_db2_WorkCompnstnClmLn.withColumn("RFRL_SVC_SEQ_NO", rpad("RFRL_SVC_SEQ_NO", <...>, " "))

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

drop_sql = "DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLnLoad_db2_WorkCompnstnClmLn_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_db2_WorkCompnstnClmLn.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLnLoad_db2_WorkCompnstnClmLn_temp")
    .mode("append")
    .save()
)

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM_LN AS T
USING STAGING.FctsIdsWorkCompClmLnLoad_db2_WorkCompnstnClmLn_temp AS S
ON (T.CLM_ID = S.CLM_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO)
WHEN MATCHED THEN UPDATE SET
T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
T.PROC_CD_SK = S.PROC_CD_SK,
T.SVC_PROV_SK = S.SVC_PROV_SK,
T.CLM_LN_PREAUTH_CD_SK = S.CLM_LN_PREAUTH_CD_SK,
T.CLM_LN_PREAUTH_SRC_CD_SK = S.CLM_LN_PREAUTH_SRC_CD_SK,
T.CLM_LN_RFRL_CD_SK = S.CLM_LN_RFRL_CD_SK,
T.SVC_END_DT = S.SVC_END_DT,
T.SVC_STRT_DT = S.SVC_STRT_DT,
T.AGMNT_PRICE_AMT = S.AGMNT_PRICE_AMT,
T.ALW_AMT = S.ALW_AMT,
T.CHRG_AMT = S.CHRG_AMT,
T.COINS_AMT = S.COINS_AMT,
T.CNSD_CHRG_AMT = S.CNSD_CHRG_AMT,
T.COPAY_AMT = S.COPAY_AMT,
T.DEDCT_AMT = S.DEDCT_AMT,
T.DSALW_AMT = S.DSALW_AMT,
T.ITS_HOME_DSCNT_AMT = S.ITS_HOME_DSCNT_AMT,
T.MBR_LIAB_BSS_AMT = S.MBR_LIAB_BSS_AMT,
T.NO_RESP_AMT = S.NO_RESP_AMT,
T.NON_PAR_SAV_AMT = S.NON_PAR_SAV_AMT,
T.PATN_RESP_AMT = S.PATN_RESP_AMT,
T.PAYBL_AMT = S.PAYBL_AMT,
T.PAYBL_TO_SUB_AMT = S.PAYBL_TO_SUB_AMT,
T.PAYBL_TO_PROV_AMT = S.PAYBL_TO_PROV_AMT,
T.PROC_TBL_PRICE_AMT = S.PROC_TBL_PRICE_AMT,
T.PROFL_PRICE_AMT = S.PROFL_PRICE_AMT,
T.PROV_WRT_OFF_AMT = S.PROV_WRT_OFF_AMT,
T.RISK_WTHLD_AMT = S.RISK_WTHLD_AMT,
T.SVC_PRICE_AMT = S.SVC_PRICE_AMT,
T.SUPLMT_DSCNT_AMT = S.SUPLMT_DSCNT_AMT,
T.PREAUTH_ID = S.PREAUTH_ID,
T.PREAUTH_SVC_SEQ_NO = S.PREAUTH_SVC_SEQ_NO,
T.RFRL_SVC_SEQ_NO = S.RFRL_SVC_SEQ_NO
WHEN NOT MATCHED THEN INSERT (
CLM_ID,
SRC_SYS_CD,
CLM_LN_SEQ_NO,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
PROC_CD_SK,
SVC_PROV_SK,
CLM_LN_PREAUTH_CD_SK,
CLM_LN_PREAUTH_SRC_CD_SK,
CLM_LN_RFRL_CD_SK,
SVC_END_DT,
SVC_STRT_DT,
AGMNT_PRICE_AMT,
ALW_AMT,
CHRG_AMT,
COINS_AMT,
CNSD_CHRG_AMT,
COPAY_AMT,
DEDCT_AMT,
DSALW_AMT,
ITS_HOME_DSCNT_AMT,
MBR_LIAB_BSS_AMT,
NO_RESP_AMT,
NON_PAR_SAV_AMT,
PATN_RESP_AMT,
PAYBL_AMT,
PAYBL_TO_SUB_AMT,
PAYBL_TO_PROV_AMT,
PROC_TBL_PRICE_AMT,
PROFL_PRICE_AMT,
PROV_WRT_OFF_AMT,
RISK_WTHLD_AMT,
SVC_PRICE_AMT,
SUPLMT_DSCNT_AMT,
PREAUTH_ID,
PREAUTH_SVC_SEQ_NO,
RFRL_SVC_SEQ_NO
)
VALUES (
S.CLM_ID,
S.SRC_SYS_CD,
S.CLM_LN_SEQ_NO,
S.CRT_RUN_CYC_EXCTN_SK,
S.LAST_UPDT_RUN_CYC_EXCTN_SK,
S.PROC_CD_SK,
S.SVC_PROV_SK,
S.CLM_LN_PREAUTH_CD_SK,
S.CLM_LN_PREAUTH_SRC_CD_SK,
S.CLM_LN_RFRL_CD_SK,
S.SVC_END_DT,
S.SVC_STRT_DT,
S.AGMNT_PRICE_AMT,
S.ALW_AMT,
S.CHRG_AMT,
S.COINS_AMT,
S.CNSD_CHRG_AMT,
S.COPAY_AMT,
S.DEDCT_AMT,
S.DSALW_AMT,
S.ITS_HOME_DSCNT_AMT,
S.MBR_LIAB_BSS_AMT,
S.NO_RESP_AMT,
S.NON_PAR_SAV_AMT,
S.PATN_RESP_AMT,
S.PAYBL_AMT,
S.PAYBL_TO_SUB_AMT,
S.PAYBL_TO_PROV_AMT,
S.PROC_TBL_PRICE_AMT,
S.PROFL_PRICE_AMT,
S.PROV_WRT_OFF_AMT,
S.RISK_WTHLD_AMT,
S.SVC_PRICE_AMT,
S.SUPLMT_DSCNT_AMT,
S.PREAUTH_ID,
S.PREAUTH_SVC_SEQ_NO,
S.RFRL_SVC_SEQ_NO
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)