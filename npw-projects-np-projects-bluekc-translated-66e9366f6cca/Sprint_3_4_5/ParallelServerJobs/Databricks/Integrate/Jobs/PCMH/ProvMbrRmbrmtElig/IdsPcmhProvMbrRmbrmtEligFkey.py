# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : IdsBCBSSCClmLoad2Seq
# MAGIC 
# MAGIC DESCRIPTION:      FKey job for IDS PCMH_PROV_MBR_RMBRMT_ELIG table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #          Change Description                                                     Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                    --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------   
# MAGIC Santosh Bokka            2013-06-02              4917                          Original Programming                                                IntegrateNewDevl              Kalyan Neelam             2013-06-27
# MAGIC                                                                                                                                                                                          IntegrateNewDevl             Sharon Andrew             2013-10-25
# MAGIC Santosh Bokka            2014-02-27              TFS - 8255             Changed Source CD SK lookup for recycle                  IntegrateNewDevl             Bhoomi Dasari              3/5/2014


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','TreoMbrPCPAttrbtnExtr.TreoMbrPCPAttrbtn.uniq')
Logging = get_widget_value('Logging','Y')
RunCycle = get_widget_value('RunCycle','')

schema_PcmhProvMbrRmbrmt = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("PCMH_PROV_MBR_RMBRMT_ELIG_SK", IntegerType(), False),
    StructField("PROV_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("AS_OF_YR_MO_SK", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("PROV_GRP_PROV_SK", IntegerType(), False),
    StructField("PROD_SH_NM_DLVRY_METH_CD_SK", IntegerType(), False),
    StructField("ELIG_FOR_RMBRMT_IN", StringType(), False),
    StructField("GRP_ELIG_IN", StringType(), False),
    StructField("MBR_MED_COV_ACTV_IN", StringType(), False),
    StructField("MBR_MED_COV_PRI_IN", StringType(), False),
    StructField("MBR_1_ACTV_PCMH_PROV_IN", StringType(), False),
    StructField("MBR_SEL_PCP_IN", StringType(), False),
    StructField("PCMH_AUTO_ASG_PCP_IN", StringType(), False),
    StructField("PROV_GRP_CLM_RQRMT_IN", StringType(), False),
    StructField("MBR_UNIQ_KEY_ORIG_EFF_DT_SK", StringType(), False),
    StructField("CLM_SVC_DT_SK", StringType(), False),
    StructField("INDV_BE_KEY", DecimalType(38,10), False),
    StructField("MBR_ELIG_STRT_DT_SK", StringType(), False),
    StructField("MBR_ELIG_END_DT_SK", StringType(), False),
    StructField("CAP_SK", IntegerType(), False),
    StructField("PAYMT_CMPL_DT_SK", StringType(), False),
    StructField("PAYMT_EXCL_RSN_DESC", StringType(), False),
    StructField("PROV_SK", IntegerType(), False)
])

df_PcmhProvMbrRmbrmt = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_PcmhProvMbrRmbrmt)
    .csv(f"{adls_path}/key/{InFile}")
)

df_Key = df_PcmhProvMbrRmbrmt

df_recycle = (
    df_Key
    .filter(F.col("ERR_CT") > 0)
    .select(
        GetRecycleKey(F.col("PCMH_PROV_MBR_RMBRMT_ELIG_SK")).alias("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
        F.rpad(F.col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
        F.rpad(F.col("PROV_ID"), <...>, " ").alias("PROV_ID"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.rpad(F.col("AS_OF_YR_MO_SK"), 6, " ").alias("AS_OF_YR_MO_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.col("PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
        F.rpad(F.col("ELIG_FOR_RMBRMT_IN"), 1, " ").alias("ELIG_FOR_RMBRMT_IN"),
        F.rpad(F.col("GRP_ELIG_IN"), 1, " ").alias("GRP_ELIG_IN"),
        F.rpad(F.col("MBR_MED_COV_ACTV_IN"), 1, " ").alias("MBR_MED_COV_ACTV_IN"),
        F.rpad(F.col("MBR_MED_COV_PRI_IN"), 1, " ").alias("MBR_MED_COV_PRI_IN"),
        F.rpad(F.col("MBR_1_ACTV_PCMH_PROV_IN"), 1, " ").alias("MBR_1_ACTV_PCMH_PROV_IN"),
        F.rpad(F.col("MBR_SEL_PCP_IN"), 1, " ").alias("MBR_SEL_PCP_IN"),
        F.rpad(F.col("PCMH_AUTO_ASG_PCP_IN"), 1, " ").alias("PCMH_AUTO_ASG_PCP_IN"),
        F.rpad(F.col("PROV_GRP_CLM_RQRMT_IN"), 1, " ").alias("PROV_GRP_CLM_RQRMT_IN"),
        F.rpad(F.col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"), 10, " ").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
        F.rpad(F.col("CLM_SVC_DT_SK"), 10, " ").alias("CLM_SVC_DT_SK"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.rpad(F.col("MBR_ELIG_STRT_DT_SK"), 10, " ").alias("MBR_ELIG_STRT_DT_SK"),
        F.rpad(F.col("MBR_ELIG_END_DT_SK"), 10, " ").alias("MBR_ELIG_END_DT_SK"),
        F.col("CAP_SK").alias("CAP_SK"),
        F.rpad(F.col("PAYMT_CMPL_DT_SK"), 10, " ").alias("PAYMT_CMPL_DT_SK"),
        F.rpad(F.col("PAYMT_EXCL_RSN_DESC"), <...>, " ").alias("PAYMT_EXCL_RSN_DESC")
    )
)

df_fkey = (
    df_Key
    .filter((F.col("ERR_CT") == 0) | (F.col("PASS_THRU_IN") == "Y"))
    .select(
        F.col("PCMH_PROV_MBR_RMBRMT_ELIG_SK").alias("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
        F.rpad(F.col("PROV_ID"), <...>, " ").alias("PROV_ID"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.rpad(F.col("AS_OF_YR_MO_SK"), 6, " ").alias("AS_OF_YR_MO_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
        F.col("PROV_SK").alias("PROV_SK"),
        F.col("PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
        F.rpad(F.col("ELIG_FOR_RMBRMT_IN"), 1, " ").alias("ELIG_FOR_RMBRMT_IN"),
        F.rpad(F.col("GRP_ELIG_IN"), 1, " ").alias("GRP_ELIG_IN"),
        F.rpad(F.col("MBR_MED_COV_ACTV_IN"), 1, " ").alias("MBR_MED_COV_ACTV_IN"),
        F.rpad(F.col("MBR_MED_COV_PRI_IN"), 1, " ").alias("MBR_MED_COV_PRI_IN"),
        F.rpad(F.col("MBR_1_ACTV_PCMH_PROV_IN"), 1, " ").alias("MBR_1_ACTV_PCMH_PROV_IN"),
        F.rpad(F.col("MBR_SEL_PCP_IN"), 1, " ").alias("MBR_SEL_PCP_IN"),
        F.rpad(F.col("PCMH_AUTO_ASG_PCP_IN"), 1, " ").alias("PCMH_AUTO_ASG_PCP_IN"),
        F.rpad(F.col("PROV_GRP_CLM_RQRMT_IN"), 1, " ").alias("PROV_GRP_CLM_RQRMT_IN"),
        F.rpad(F.col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"), 10, " ").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
        F.rpad(F.col("CLM_SVC_DT_SK"), 10, " ").alias("CLM_SVC_DT_SK"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.rpad(F.col("MBR_ELIG_STRT_DT_SK"), 10, " ").alias("MBR_ELIG_STRT_DT_SK"),
        F.rpad(F.col("MBR_ELIG_END_DT_SK"), 10, " ").alias("MBR_ELIG_END_DT_SK"),
        F.col("CAP_SK").alias("CAP_SK"),
        F.rpad(F.col("PAYMT_CMPL_DT_SK"), 10, " ").alias("PAYMT_CMPL_DT_SK"),
        F.rpad(F.col("PAYMT_EXCL_RSN_DESC"), <...>, " ").alias("PAYMT_EXCL_RSN_DESC")
    )
)

df_defaultUNK_source = df_Key.limit(1)
df_defUNK = df_defaultUNK_source.select(
    F.lit(0).alias("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
    F.rpad(F.lit("UNK"), <...>, " ").alias("PROV_ID"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.rpad(F.lit("UNK"), 6, " ").alias("AS_OF_YR_MO_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROV_GRP_PROV_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit(0).alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
    F.rpad(F.lit("U"), 1, " ").alias("ELIG_FOR_RMBRMT_IN"),
    F.rpad(F.lit("U"), 1, " ").alias("GRP_ELIG_IN"),
    F.rpad(F.lit("U"), 1, " ").alias("MBR_MED_COV_ACTV_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_MED_COV_PRI_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_1_ACTV_PCMH_PROV_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_SEL_PCP_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("PCMH_AUTO_ASG_PCP_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("PROV_GRP_CLM_RQRMT_IN"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
    F.rpad(F.lit("UNK"), 10, " ").alias("CLM_SVC_DT_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_ELIG_STRT_DT_SK"),
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("MBR_ELIG_END_DT_SK"),
    F.lit(0).alias("CAP_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("PAYMT_CMPL_DT_SK"),
    F.rpad(F.lit("UNK"), <...>, " ").alias("PAYMT_EXCL_RSN_DESC")
)

df_defaultNA_source = df_Key.limit(1)
df_defNA = df_defaultNA_source.select(
    F.lit(1).alias("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
    F.rpad(F.lit("NA"), <...>, " ").alias("PROV_ID"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.rpad(F.lit("NA"), 6, " ").alias("AS_OF_YR_MO_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROV_GRP_PROV_SK"),
    F.lit(1).alias("PROV_SK"),
    F.lit(1).alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
    F.rpad(F.lit("N"), 1, " ").alias("ELIG_FOR_RMBRMT_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("GRP_ELIG_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_MED_COV_ACTV_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_MED_COV_PRI_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_1_ACTV_PCMH_PROV_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("MBR_SEL_PCP_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("PCMH_AUTO_ASG_PCP_IN"),
    F.rpad(F.lit("N"), 1, " ").alias("PROV_GRP_CLM_RQRMT_IN"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("CLM_SVC_DT_SK"),
    F.lit(1).alias("INDV_BE_KEY"),
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("MBR_ELIG_STRT_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_ELIG_END_DT_SK"),
    F.lit(1).alias("CAP_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("PAYMT_CMPL_DT_SK"),
    F.rpad(F.lit("NA"), <...>, " ").alias("PAYMT_EXCL_RSN_DESC")
)

df_recycle_out = df_recycle.select(
    "PCMH_PROV_MBR_RMBRMT_ELIG_SK",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PROV_ID",
    "MBR_UNIQ_KEY",
    "AS_OF_YR_MO_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "GRP_SK",
    "MBR_SK",
    "PROV_GRP_PROV_SK",
    "PROV_SK",
    "PROD_SH_NM_DLVRY_METH_CD_SK",
    "ELIG_FOR_RMBRMT_IN",
    "GRP_ELIG_IN",
    "MBR_MED_COV_ACTV_IN",
    "MBR_MED_COV_PRI_IN",
    "MBR_1_ACTV_PCMH_PROV_IN",
    "MBR_SEL_PCP_IN",
    "PCMH_AUTO_ASG_PCP_IN",
    "PROV_GRP_CLM_RQRMT_IN",
    "MBR_UNIQ_KEY_ORIG_EFF_DT_SK",
    "CLM_SVC_DT_SK",
    "INDV_BE_KEY",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "CAP_SK",
    "PAYMT_CMPL_DT_SK",
    "PAYMT_EXCL_RSN_DESC"
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector = (
    df_fkey.unionByName(df_defUNK)
           .unionByName(df_defNA)
)

df_collector_out = df_collector.select(
    "PCMH_PROV_MBR_RMBRMT_ELIG_SK",
    "PROV_ID",
    "MBR_UNIQ_KEY",
    "AS_OF_YR_MO_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "GRP_SK",
    "MBR_SK",
    "PROV_GRP_PROV_SK",
    "PROV_SK",
    "PROD_SH_NM_DLVRY_METH_CD_SK",
    "ELIG_FOR_RMBRMT_IN",
    "GRP_ELIG_IN",
    "MBR_MED_COV_ACTV_IN",
    "MBR_MED_COV_PRI_IN",
    "MBR_1_ACTV_PCMH_PROV_IN",
    "MBR_SEL_PCP_IN",
    "PCMH_AUTO_ASG_PCP_IN",
    "PROV_GRP_CLM_RQRMT_IN",
    "MBR_UNIQ_KEY_ORIG_EFF_DT_SK",
    "CLM_SVC_DT_SK",
    "INDV_BE_KEY",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "CAP_SK",
    "PAYMT_CMPL_DT_SK",
    "PAYMT_EXCL_RSN_DESC"
)

write_files(
    df_collector_out,
    f"{adls_path}/load/PCMH_PROV_MBR_RMBRMT_ELIG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)