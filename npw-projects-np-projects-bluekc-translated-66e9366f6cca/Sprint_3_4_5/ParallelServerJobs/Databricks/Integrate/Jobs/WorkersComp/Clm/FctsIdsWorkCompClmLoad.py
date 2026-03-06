# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-01\(9)5628 WORK_COMPNSTN_CLM \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-02-27
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC                                                                                                                                    \(9)

# MAGIC Job to load the Target DB2 table WORK_COMPNSTN_CLM
# MAGIC Read the load file created in the FctsIdsWorkCompClmExtr job
# MAGIC Load the file created  into the DB2 table WORK_COMPNSTN_CLM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


env = get_widget_value('Env', '')
runID = get_widget_value('RunID', '')
WorkCompOwner = get_widget_value('WorkCompOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

schema_seq_WorkCompnstnClm = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ADJ_FROM_CLM", StringType(), True),
    StructField("ADJ_TO_CLM", StringType(), True),
    StructField("EXPRNC_CAT_SK", IntegerType(), True),
    StructField("FNCL_LOB_SK", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("NTWK_SK", IntegerType(), True),
    StructField("PROD_SK", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("WORK_COMP_PAYMT_TYP_ID", StringType(), True),
    StructField("CLM_ACDNT_CD_SK", IntegerType(), True),
    StructField("CLM_ACDNT_ST_CD_SK", IntegerType(), True),
    StructField("CLM_FINL_DISP_CD_SK", IntegerType(), True),
    StructField("CLM_INPT_METH_CD_SK", IntegerType(), True),
    StructField("CLM_PAYE_CD_SK", IntegerType(), True),
    StructField("CLM_SVC_PROV_SPEC_CD_SK", IntegerType(), True),
    StructField("CLM_SVC_PROV_TYP_CD_SK", IntegerType(), True),
    StructField("CLM_STTUS_CD_SK", IntegerType(), True),
    StructField("CLM_SUBTYP_CD_SK", IntegerType(), True),
    StructField("CLM_TYP_CD_SK", IntegerType(), True),
    StructField("ACDNT_DT", DateType(), True),
    StructField("INPT_DT", DateType(), True),
    StructField("NEXT_RVW_DT", DateType(), True),
    StructField("PD_DT", DateType(), True),
    StructField("PRCS_DT", DateType(), True),
    StructField("RCVD_DT", DateType(), True),
    StructField("SVC_END_DT", DateType(), True),
    StructField("SVC_STRT_DT", DateType(), True),
    StructField("STTUS_DT", DateType(), True),
    StructField("WORK_UNABLE_BEG_DT", DateType(), True),
    StructField("WORK_UNABLE_END_DT", DateType(), True),
    StructField("ACDNT_AMT", DecimalType(38,10), True),
    StructField("ACTL_PD_AMT", DecimalType(38,10), True),
    StructField("ALW_AMT", DecimalType(38,10), True),
    StructField("CHRG_AMT", DecimalType(38,10), True),
    StructField("COINS_AMT", DecimalType(38,10), True),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("PATN_PD_AMT", DecimalType(38,10), True),
    StructField("PAYBL_AMT", DecimalType(38,10), True),
    StructField("REMIT_SUPRSION_AMT", DecimalType(38,10), True),
    StructField("PATN_ACCT_NO", StringType(), True),
    StructField("RFRNG_PROV_TX", StringType(), True)
])

df_seq_WorkCompnstnClm = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", None)
    .option("escape", None)
    .option("inferSchema", "false")
    .schema(schema_seq_WorkCompnstnClm)
    .load(f"{adls_path}/load/WORK_COMPNSTN_CLM.dat")
)

df_cpy_forBuffer = df_seq_WorkCompnstnClm

df_db2_WorkCompnstnClm = df_cpy_forBuffer

df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("CLM_ID", rpad("CLM_ID", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("ADJ_FROM_CLM", rpad("ADJ_FROM_CLM", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("ADJ_TO_CLM", rpad("ADJ_TO_CLM", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("GRP_ID", rpad("GRP_ID", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("WORK_COMP_PAYMT_TYP_ID", rpad("WORK_COMP_PAYMT_TYP_ID", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("PATN_ACCT_NO", rpad("PATN_ACCT_NO", <...>, " "))
df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.withColumn("RFRNG_PROV_TX", rpad("RFRNG_PROV_TX", <...>, " "))

df_db2_WorkCompnstnClm = df_db2_WorkCompnstnClm.select(
    "CLM_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ADJ_FROM_CLM",
    "ADJ_TO_CLM",
    "EXPRNC_CAT_SK",
    "FNCL_LOB_SK",
    "GRP_ID",
    "NTWK_SK",
    "PROD_SK",
    "SUB_UNIQ_KEY",
    "WORK_COMP_PAYMT_TYP_ID",
    "CLM_ACDNT_CD_SK",
    "CLM_ACDNT_ST_CD_SK",
    "CLM_FINL_DISP_CD_SK",
    "CLM_INPT_METH_CD_SK",
    "CLM_PAYE_CD_SK",
    "CLM_SVC_PROV_SPEC_CD_SK",
    "CLM_SVC_PROV_TYP_CD_SK",
    "CLM_STTUS_CD_SK",
    "CLM_SUBTYP_CD_SK",
    "CLM_TYP_CD_SK",
    "ACDNT_DT",
    "INPT_DT",
    "NEXT_RVW_DT",
    "PD_DT",
    "PRCS_DT",
    "RCVD_DT",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "STTUS_DT",
    "WORK_UNABLE_BEG_DT",
    "WORK_UNABLE_END_DT",
    "ACDNT_AMT",
    "ACTL_PD_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "PATN_PD_AMT",
    "PAYBL_AMT",
    "REMIT_SUPRSION_AMT",
    "PATN_ACCT_NO",
    "RFRNG_PROV_TX"
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FctsIdsWorkCompClmLoad_db2_WorkCompnstnClm_temp",
    jdbc_url,
    jdbc_props
)

df_db2_WorkCompnstnClm.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsWorkCompClmLoad_db2_WorkCompnstnClm_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {WorkCompOwner}.WORK_COMPNSTN_CLM AS T
USING STAGING.FctsIdsWorkCompClmLoad_db2_WorkCompnstnClm_temp AS S
ON T.CLM_ID = S.CLM_ID
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
T.ADJ_FROM_CLM = S.ADJ_FROM_CLM,
T.ADJ_TO_CLM = S.ADJ_TO_CLM,
T.EXPRNC_CAT_SK = S.EXPRNC_CAT_SK,
T.FNCL_LOB_SK = S.FNCL_LOB_SK,
T.GRP_ID = S.GRP_ID,
T.NTWK_SK = S.NTWK_SK,
T.PROD_SK = S.PROD_SK,
T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
T.WORK_COMP_PAYMT_TYP_ID = S.WORK_COMP_PAYMT_TYP_ID,
T.CLM_ACDNT_CD_SK = S.CLM_ACDNT_CD_SK,
T.CLM_ACDNT_ST_CD_SK = S.CLM_ACDNT_ST_CD_SK,
T.CLM_FINL_DISP_CD_SK = S.CLM_FINL_DISP_CD_SK,
T.CLM_INPT_METH_CD_SK = S.CLM_INPT_METH_CD_SK,
T.CLM_PAYE_CD_SK = S.CLM_PAYE_CD_SK,
T.CLM_SVC_PROV_SPEC_CD_SK = S.CLM_SVC_PROV_SPEC_CD_SK,
T.CLM_SVC_PROV_TYP_CD_SK = S.CLM_SVC_PROV_TYP_CD_SK,
T.CLM_STTUS_CD_SK = S.CLM_STTUS_CD_SK,
T.CLM_SUBTYP_CD_SK = S.CLM_SUBTYP_CD_SK,
T.CLM_TYP_CD_SK = S.CLM_TYP_CD_SK,
T.ACDNT_DT = S.ACDNT_DT,
T.INPT_DT = S.INPT_DT,
T.NEXT_RVW_DT = S.NEXT_RVW_DT,
T.PD_DT = S.PD_DT,
T.PRCS_DT = S.PRCS_DT,
T.RCVD_DT = S.RCVD_DT,
T.SVC_END_DT = S.SVC_END_DT,
T.SVC_STRT_DT = S.SVC_STRT_DT,
T.STTUS_DT = S.STTUS_DT,
T.WORK_UNABLE_BEG_DT = S.WORK_UNABLE_BEG_DT,
T.WORK_UNABLE_END_DT = S.WORK_UNABLE_END_DT,
T.ACDNT_AMT = S.ACDNT_AMT,
T.ACTL_PD_AMT = S.ACTL_PD_AMT,
T.ALW_AMT = S.ALW_AMT,
T.CHRG_AMT = S.CHRG_AMT,
T.COINS_AMT = S.COINS_AMT,
T.CNSD_CHRG_AMT = S.CNSD_CHRG_AMT,
T.COPAY_AMT = S.COPAY_AMT,
T.DEDCT_AMT = S.DEDCT_AMT,
T.DSALW_AMT = S.DSALW_AMT,
T.PATN_PD_AMT = S.PATN_PD_AMT,
T.PAYBL_AMT = S.PAYBL_AMT,
T.REMIT_SUPRSION_AMT = S.REMIT_SUPRSION_AMT,
T.PATN_ACCT_NO = S.PATN_ACCT_NO,
T.RFRNG_PROV_TX = S.RFRNG_PROV_TX
WHEN NOT MATCHED THEN INSERT (
CLM_ID,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
ADJ_FROM_CLM,
ADJ_TO_CLM,
EXPRNC_CAT_SK,
FNCL_LOB_SK,
GRP_ID,
NTWK_SK,
PROD_SK,
SUB_UNIQ_KEY,
WORK_COMP_PAYMT_TYP_ID,
CLM_ACDNT_CD_SK,
CLM_ACDNT_ST_CD_SK,
CLM_FINL_DISP_CD_SK,
CLM_INPT_METH_CD_SK,
CLM_PAYE_CD_SK,
CLM_SVC_PROV_SPEC_CD_SK,
CLM_SVC_PROV_TYP_CD_SK,
CLM_STTUS_CD_SK,
CLM_SUBTYP_CD_SK,
CLM_TYP_CD_SK,
ACDNT_DT,
INPT_DT,
NEXT_RVW_DT,
PD_DT,
PRCS_DT,
RCVD_DT,
SVC_END_DT,
SVC_STRT_DT,
STTUS_DT,
WORK_UNABLE_BEG_DT,
WORK_UNABLE_END_DT,
ACDNT_AMT,
ACTL_PD_AMT,
ALW_AMT,
CHRG_AMT,
COINS_AMT,
CNSD_CHRG_AMT,
COPAY_AMT,
DEDCT_AMT,
DSALW_AMT,
PATN_PD_AMT,
PAYBL_AMT,
REMIT_SUPRSION_AMT,
PATN_ACCT_NO,
RFRNG_PROV_TX
) VALUES (
S.CLM_ID,
S.SRC_SYS_CD,
S.CRT_RUN_CYC_EXCTN_SK,
S.LAST_UPDT_RUN_CYC_EXCTN_SK,
S.ADJ_FROM_CLM,
S.ADJ_TO_CLM,
S.EXPRNC_CAT_SK,
S.FNCL_LOB_SK,
S.GRP_ID,
S.NTWK_SK,
S.PROD_SK,
S.SUB_UNIQ_KEY,
S.WORK_COMP_PAYMT_TYP_ID,
S.CLM_ACDNT_CD_SK,
S.CLM_ACDNT_ST_CD_SK,
S.CLM_FINL_DISP_CD_SK,
S.CLM_INPT_METH_CD_SK,
S.CLM_PAYE_CD_SK,
S.CLM_SVC_PROV_SPEC_CD_SK,
S.CLM_SVC_PROV_TYP_CD_SK,
S.CLM_STTUS_CD_SK,
S.CLM_SUBTYP_CD_SK,
S.CLM_TYP_CD_SK,
S.ACDNT_DT,
S.INPT_DT,
S.NEXT_RVW_DT,
S.PD_DT,
S.PRCS_DT,
S.RCVD_DT,
S.SVC_END_DT,
S.SVC_STRT_DT,
S.STTUS_DT,
S.WORK_UNABLE_BEG_DT,
S.WORK_UNABLE_END_DT,
S.ACDNT_AMT,
S.ACTL_PD_AMT,
S.ALW_AMT,
S.CHRG_AMT,
S.COINS_AMT,
S.CNSD_CHRG_AMT,
S.COPAY_AMT,
S.DEDCT_AMT,
S.DSALW_AMT,
S.PATN_PD_AMT,
S.PAYBL_AMT,
S.REMIT_SUPRSION_AMT,
S.PATN_ACCT_NO,
S.RFRNG_PROV_TX
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)