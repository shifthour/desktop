# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsInvcSubSbsdyFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC INPUTS:   File created by FctsIncomeInvcSubSbsdyExtr
# MAGIC                     
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam        2014-06-20               Initial programming                                                                             5235                 IntegrateNewDevl              Bhoomi Dasari            6/21/2014 
# MAGIC Goutham Kalidindi             2021-03-24      Changed Datatype length for field                                                    358186             IntegrateDev1                     Jeyaprasanna             2021-03-31
# MAGIC                                                                                              BLIV_ID
# MAGIC                                                                                                char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, monotonically_increasing_id, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
OutFile = get_widget_value('OutFile','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_IdsInvcSubSubsdy = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("INVC_SUB_SBSDY_SK", IntegerType(), False),
    StructField("BILL_INVC_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_BILL_CMPNT_ID", StringType(), False),
    StructField("COV_DUE_DT", StringType(), False),
    StructField("COV_STRT_DT_SK", StringType(), False),
    StructField("INVC_SUB_SBSDY_PRM_TYP_CD", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_DT", TimestampType(), False),
    StructField("INVC_SUB_SBSDY_BILL_DISP_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("COV_END_DT_SK", StringType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CSPI_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("INVC_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("INVC_SUB_FMLY_CNTR_CD", StringType(), False),
    StructField("SUB_SBSDY_AMT", DecimalType(38,10), False)
])

df_IdsInvcSubSubsdy = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsInvcSubSubsdy)
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_stagevar = (
    df_IdsInvcSubSubsdy
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svProd", GetFkeyProd(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("PROD_ID"), Logging))
    .withColumn("svInvcSubPrmTypCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), F.lit("INVOICE SUBSCRIBER PREMIUM TYPE"), col("INVC_SUB_SBSDY_PRM_TYP_CD"), Logging))
    .withColumn("svCovDueDt", GetFkeyDate(F.lit("IDS"), col("INVC_SUB_SBSDY_SK"), col("COV_DUE_DT"), Logging))
    .withColumn("svCovStrtDt", GetFkeyDate(F.lit("IDS"), col("INVC_SUB_SBSDY_SK"), col("COV_STRT_DT_SK"), Logging))
    .withColumn("svCovEndDt", GetFkeyDate(F.lit("IDS"), col("INVC_SUB_SBSDY_SK"), col("COV_END_DT_SK"), Logging))
    .withColumn("svCls", GetFkeyCls(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("GRP_ID"), col("CLS_ID"), Logging))
    .withColumn("svClsPln", GetFkeyClsPln(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("CLS_PLN_ID"), Logging))
    .withColumn("svGrp", GetFkeyGrp(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("GRP_ID"), Logging))
    .withColumn("svInvc", GetFkeyInvc(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("INVC_ID"), Logging))
    .withColumn("svSub", GetFkeySub(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("SUB_UNIQ_KEY"), Logging))
    .withColumn("svSubGrp", GetFkeySubgrp(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), col("GRP_ID"), col("SUBGRP_ID"), Logging))
    .withColumn("svInvcSubFmlyCntrCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), F.lit("SUBSCRIBER FAMILY INDICATOR"), col("INVC_SUB_FMLY_CNTR_CD"), Logging))
    .withColumn("svInvcSubBillDispCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SUB_SBSDY_SK"), F.lit("BILLING DISPOSITION"), col("INVC_SUB_SBSDY_BILL_DISP_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INVC_SUB_SBSDY_SK")))
)

# For DataStage @INROWNUM=1 constraints, we take the first row in the dataset.
df_ForeignKey_stagevar_firstRow = df_ForeignKey_stagevar.limit(1)

df_ForeignKeyFkey = (
    df_ForeignKey_stagevar
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("INVC_SUB_SBSDY_SK").alias("INVC_SUB_SBSDY_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        col("svCovDueDt").alias("COV_DUE_DT_SK"),
        col("svCovStrtDt").alias("COV_STRT_DT_SK"),
        col("INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
        col("CRT_DT").alias("CRT_TS"),
        col("INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCls").alias("CLS_SK"),
        col("svClsPln").alias("CLS_PLN_SK"),
        col("svGrp").alias("GRP_SK"),
        col("svInvc").alias("INVC_SK"),
        col("svProd").alias("PROD_SK"),
        col("svSubGrp").alias("SUBGRP_SK"),
        col("svSub").alias("SUB_SK"),
        col("svInvcSubBillDispCd").alias("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
        col("svInvcSubFmlyCntrCd").alias("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
        col("svInvcSubPrmTypCd").alias("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
        col("svCovEndDt").alias("COV_END_DT_SK"),
        col("SUB_SBSDY_AMT").alias("SUB_SBSDY_AMT")
    )
)

df_ForeignKeyDefaultUNK = (
    df_ForeignKey_stagevar_firstRow
    .select(
        lit(0).alias("INVC_SUB_SBSDY_SK"),
        lit("UNK").alias("BILL_INVC_ID"),
        lit(0).alias("SUB_UNIQ_KEY"),
        lit("UNK").alias("CLS_PLN_ID"),
        lit("UNK").alias("PROD_ID"),
        lit("UNK").alias("PROD_BILL_CMPNT_ID"),
        lit("1753-01-01").alias("COV_DUE_DT_SK"),
        lit("1753-01-01").alias("COV_STRT_DT_SK"),
        lit("UNK").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
        lit("1753-01-01 00:00:00.000").alias("CRT_TS"),
        lit("UNK").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
        lit(0).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLS_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("INVC_SK"),
        lit(0).alias("PROD_SK"),
        lit(0).alias("SUBGRP_SK"),
        lit(0).alias("SUB_SK"),
        lit(0).alias("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
        lit(0).alias("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
        lit(0).alias("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
        lit("2199-12-31").alias("COV_END_DT_SK"),
        lit(0).alias("SUB_SBSDY_AMT")
    )
)

df_ForeignKeyDefaultNA = (
    df_ForeignKey_stagevar_firstRow
    .select(
        lit(1).alias("INVC_SUB_SBSDY_SK"),
        lit("NA").alias("BILL_INVC_ID"),
        lit(1).alias("SUB_UNIQ_KEY"),
        lit("NA").alias("CLS_PLN_ID"),
        lit("NA").alias("PROD_ID"),
        lit("NA").alias("PROD_BILL_CMPNT_ID"),
        lit("1753-01-01").alias("COV_DUE_DT_SK"),
        lit("1753-01-01").alias("COV_STRT_DT_SK"),
        lit("NA").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
        lit("1753-01-01 00:00:00.000").alias("CRT_TS"),
        lit("NA").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
        lit(1).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLS_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("INVC_SK"),
        lit(1).alias("PROD_SK"),
        lit(1).alias("SUBGRP_SK"),
        lit(1).alias("SUB_SK"),
        lit(1).alias("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
        lit(1).alias("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
        lit(1).alias("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
        lit("2199-12-31").alias("COV_END_DT_SK"),
        lit(0).alias("SUB_SBSDY_AMT")
    )
)

df_ForeignKeyRecycle = (
    df_ForeignKey_stagevar
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("INVC_SUB_SBSDY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("INVC_SUB_SBSDY_SK").alias("INVC_SUB_SBSDY_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        rpad(col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
        rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
        rpad(col("PROD_BILL_CMPNT_ID"), 4, " ").alias("PROD_BILL_CMPNT_ID"),
        rpad(col("COV_DUE_DT"), 10, " ").alias("COV_DUE_DT"),
        rpad(col("COV_STRT_DT_SK"), 10, " ").alias("COV_STRT_DT_SK"),
        col("INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
        col("CRT_DT").alias("CRT_DT"),
        col("INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(col("COV_END_DT_SK"), 10, " ").alias("COV_END_DT_SK"),
        rpad(col("CLS_ID"), 4, " ").alias("CLS_ID"),
        rpad(col("CSPI_ID"), 8, " ").alias("CSPI_ID"),
        rpad(col("GRP_ID"), 8, " ").alias("GRP_ID"),
        col("INVC_ID").alias("INVC_ID"),
        rpad(col("SUBGRP_ID"), 4, " ").alias("SUBGRP_ID"),
        rpad(col("INVC_SUB_FMLY_CNTR_CD"), 1, " ").alias("INVC_SUB_FMLY_CNTR_CD"),
        col("SUB_SBSDY_AMT").alias("SUB_SBSDY_AMT")
    )
)

write_files(
    df_ForeignKeyRecycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_Collector = df_ForeignKeyFkey.unionByName(df_ForeignKeyDefaultUNK).unionByName(df_ForeignKeyDefaultNA)

# Final select to preserve the same column order (26 columns):
df_Collector_final = (
    df_Collector
    .select(
        col("INVC_SUB_SBSDY_SK"),
        col("BILL_INVC_ID"),
        col("SUB_UNIQ_KEY"),
        col("CLS_PLN_ID"),
        col("PROD_ID"),
        col("PROD_BILL_CMPNT_ID"),
        rpad(col("COV_DUE_DT_SK"), 10, " ").alias("COV_DUE_DT_SK"),
        rpad(col("COV_STRT_DT_SK"), 10, " ").alias("COV_STRT_DT_SK"),
        col("INVC_SUB_SBSDY_PRM_TYP_CD"),
        col("CRT_TS"),
        col("INVC_SUB_SBSDY_BILL_DISP_CD"),
        col("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLS_SK"),
        col("CLS_PLN_SK"),
        col("GRP_SK"),
        col("INVC_SK"),
        col("PROD_SK"),
        col("SUBGRP_SK"),
        col("SUB_SK"),
        col("INVC_SUB_SBSDY_BILL_DISP_CD_SK"),
        col("INVC_SUB_SBSDY_FMLY_CNTR_CD_SK"),
        col("INVC_SUB_SBSDY_PRM_TYP_CD_SK"),
        rpad(col("COV_END_DT_SK"), 10, " ").alias("COV_END_DT_SK"),
        col("SUB_SBSDY_AMT")
    )
)

write_files(
    df_Collector_final,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)