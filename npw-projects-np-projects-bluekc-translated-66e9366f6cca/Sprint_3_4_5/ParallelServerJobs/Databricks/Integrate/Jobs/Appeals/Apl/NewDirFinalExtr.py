# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC PROCESSING:    Creates appeals data
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Ravi Singh          2018- 08-18                   Initial program                                                           MCAS-5796              IntegrateDev1             Kalyan Neelam         2018-09-24

# MAGIC Apply business logic
# MAGIC Writing Sequential File to /pkey
# MAGIC This container is used in:
# MAGIC Evicore,NDBH and Telligen Appeals
# MAGIC ......Extr
# MAGIC 
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
$IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/AplMcasPkey
# COMMAND ----------

schema_idsaplextr_ndbh = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), False),
    StructField("CRT_USER_SK", StringType(), False),
    StructField("CUR_PRI_USER_SK", StringType(), False),
    StructField("CUR_SEC_USER_SK", StringType(), False),
    StructField("CUR_TRTY_USER_SK", StringType(), False),
    StructField("GRP_SK", StringType(), False),
    StructField("LAST_UPDT_USER_SK", StringType(), False),
    StructField("MBR_SK", StringType(), False),
    StructField("SUBGRP_SK", StringType(), False),
    StructField("PROD_SK", StringType(), False),
    StructField("PROD_SH_NM_SK", StringType(), False),
    StructField("SUB_SK", StringType(), False),
    StructField("APL_CAT_CD_SK", StringType(), False),
    StructField("APL_CUR_DCSN_CD_SK", StringType(), False),
    StructField("APL_CUR_STTUS_CD_SK", StringType(), False),
    StructField("APL_INITN_METH_CD_SK", StringType(), False),
    StructField("APL_MBR_HOME_ADDR_ST_CD_SK", StringType(), False),
    StructField("APL_SUBTYP_CD_SK", StringType(), False),
    StructField("APL_TYP_CD_SK", StringType(), False),
    StructField("CLS_PLN_PROD_CAT_CD_SK", StringType(), False),
    StructField("CUR_APL_LVL_CD_SK", StringType(), False),
    StructField("CUR_APL_LVL_EXPDTD_IN", StringType(), False),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("CUR_STTUS_DTM", TimestampType(), False),
    StructField("END_DT_SK", StringType(), False),
    StructField("INITN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("NEXT_RVW_DT_SK", StringType(), False),
    StructField("CUR_APL_LVL_SEQ_NO", StringType(), False),
    StructField("CUR_STTUS_SEQ_NO", StringType(), False),
    StructField("NEXT_RVW_INTRVL_NO", StringType(), False),
    StructField("APL_DESC", StringType(), True),
    StructField("APL_SUM_DESC", StringType(), True)
])

df_idsaplextr_ndbh = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_idsaplextr_ndbh)
    .load(f"{adls_path}/verified/{SrcSysCd}_NDBH_Interm.dat.{RunID}")
)

df_businessrules = df_idsaplextr_ndbh.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("CUR_PRI_USER_SK").alias("CUR_PRI_USER_SK"),
    F.col("CUR_SEC_USER_SK").alias("CUR_SEC_USER_SK"),
    F.col("CUR_TRTY_USER_SK").alias("CUR_TRTY_USER_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
    F.col("APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
    F.col("APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
    F.col("APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
    F.col("APL_MBR_HOME_ADDR_ST_CD_SK").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
    F.col("APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
    F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
    F.to_timestamp(
        F.concat(F.date_format(F.col("CRT_DTM"), "yyyy-MM-dd HH:mm:ss"), F.lit(".000000")),
        "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ).alias("CRT_DTM"),
    F.to_timestamp(
        F.concat(F.date_format(F.col("CUR_STTUS_DTM"), "yyyy-MM-dd HH:mm:ss"), F.lit(".000000")),
        "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ).alias("CUR_STTUS_DTM"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("INITN_DT_SK").alias("INITN_DT_SK"),
    F.to_timestamp(
        F.concat(F.date_format(F.col("LAST_UPDT_DTM"), "yyyy-MM-dd HH:mm:ss"), F.lit(".000000")),
        "yyyy-MM-dd HH:mm:ss.SSSSSS"
    ).alias("LAST_UPDT_DTM"),
    F.col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    F.col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
    F.col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
    F.col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
    F.col("APL_DESC").alias("APL_DESC"),
    F.col("APL_SUM_DESC").alias("APL_SUM_DESC")
)

params_aplmcaspkey = {
    "CurrRunCycle": CurrRunCycle
}

df_aplmcaspkey = AplMcasPkey(df_businessrules, params_aplmcaspkey)

df_final = df_aplmcaspkey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("APL_SK"),
    F.col("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_USER_SK"),
    F.col("CUR_PRI_USER_SK"),
    F.col("CUR_SEC_USER_SK"),
    F.col("CUR_TRTY_USER_SK"),
    F.col("GRP_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SK"),
    F.col("PROD_SH_NM_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.col("APL_CAT_CD_SK"),
    F.col("APL_CUR_DCSN_CD_SK"),
    F.col("APL_CUR_STTUS_CD_SK"),
    F.col("APL_INITN_METH_CD_SK"),
    F.col("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APL_SUBTYP_CD_SK"),
    F.col("APL_TYP_CD_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CUR_APL_LVL_CD_SK"),
    F.rpad(F.col("CUR_APL_LVL_EXPDTD_IN"), 1, " ").alias("CUR_APL_LVL_EXPDTD_IN"),
    F.col("CRT_DTM"),
    F.col("CUR_STTUS_DTM"),
    F.rpad(F.col("END_DT_SK"), 10, " ").alias("END_DT_SK"),
    F.rpad(F.col("INITN_DT_SK"), 10, " ").alias("INITN_DT_SK"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("NEXT_RVW_DT_SK"), 10, " ").alias("NEXT_RVW_DT_SK"),
    F.col("CUR_APL_LVL_SEQ_NO"),
    F.col("CUR_STTUS_SEQ_NO"),
    F.col("NEXT_RVW_INTRVL_NO"),
    F.col("APL_DESC"),
    F.col("APL_SUM_DESC")
)

write_files(
    df_final,
    f"{adls_path}/key/NewDirAplExtr.Apl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)