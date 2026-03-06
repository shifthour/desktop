# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                       Project #                    Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------               ----------------                   ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     12/02/2006                Initial program                                                               CustSvc/3028            devlIDS30                          Steph Goddard          2/13/07
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                      3567 Primary Key        devlIDScur                          Steph Goddard         05/06/2008
# MAGIC                                                               Added source system to load file name
# MAGIC Ralph Tucker       07/24/2008              Moved stage varables logic to                                      TTR313/15                devlIDS                               Steph Goddard         07/25/2008
# MAGIC                                                               CustSvcTaskTypeCd to fix error recycle prob  
# MAGIC 
# MAGIC Manasa Andru      2015-02-03              Changed the logic in all the SK fields in the                TFS - 2655                 IntegrateNewDevl                Kalyan Neelam          2015-02-03
# MAGIC                                                               InkRecycle link to load the correct values in
# MAGIC                                                                  the hf_recycle file.
# MAGIC 
# MAGIC Manasa Andru       2015-11-10            Updated the code for RCVD_DT_SK field to get              TFS - 10727          IntegrateDev2                     Kalyan Neelam          2015-11-13
# MAGIC                                                               1753-01-01 for blank/null or invalid date values when
# MAGIC                                                                   the routine returns UNK value.

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all customer service foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','20070112929')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_IdsCustSvcTaskFkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_ID", StringType(), nullable=False),
    StructField("TASK_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CUST_SVC_SK", IntegerType(), nullable=False),
    StructField("GRP_SK", StringType(), nullable=False),
    StructField("INPT_USER_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_USER_SK", StringType(), nullable=False),
    StructField("MBR_SK", StringType(), nullable=False),
    StructField("PROD_SK", StringType(), nullable=False),
    StructField("PROV_SK", StringType(), nullable=False),
    StructField("SUBGRP_SK", StringType(), nullable=False),
    StructField("SUB_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_CAT_CD_SK", StringType(), nullable=False),
    StructField("CS_TASK_CLS_PLN_PROD_CAT_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_CLSR_PRF_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_CUST_TYP_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_FINL_ACTN_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_ITS_TYP_CD_SK", StringType(), nullable=False),
    StructField("CS_TASK_LTR_RECPNT_TYP_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_PG_TYP_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_PRTY_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_STTUS_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_STTUS_RSN_CD_SK", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_SUBJ_CD_SK", StringType(), nullable=False),
    StructField("CMPLNT_IN", StringType(), nullable=False),
    StructField("CLSD_DT_SK", StringType(), nullable=False),
    StructField("INPT_DTM", TimestampType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("NEXT_RVW_DT_SK", StringType(), nullable=False),
    StructField("RCVD_DT_SK", StringType(), nullable=False),
    StructField("NEXT_RVW_INTRVL_DAYS", IntegerType(), nullable=False),
    StructField("CUST_ID", StringType(), nullable=False),
    StructField("CUST_SVC_TASK_USER_SITE_ID", StringType(), nullable=False),
    StructField("INPT_USER_CC_ID", StringType(), nullable=False),
    StructField("SUM_DESC", StringType(), nullable=False),
    StructField("SGSG_ID", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("USUS_ID", StringType(), nullable=False),
    StructField("CSTK_ASSIGN_USID", StringType(), nullable=False),
    StructField("ATUF_DATE1", TimestampType(), nullable=False),
    StructField("CSTS_STS_DTM", TimestampType(), nullable=False),
    StructField("CSTS_STS", StringType(), nullable=False),
    StructField("CSTK_MCTR_SUBJ_1", StringType(), nullable=False),
    StructField("CSTK_STS", StringType(), nullable=False)
])

df_IdsCustSvcTaskFkey = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsCustSvcTaskFkey)
    .csv(f"{adls_path}/key/{InFile}")
)

df_PurgeTrnCustSrvTask = (
    df_IdsCustSvcTaskFkey
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svAppUsrSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("INPT_USER_SK"), F.lit(Logging)))
    .withColumn("svLastUpdUserSk", GetFkeyAppUsr(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("LAST_UPDT_USER_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskNextRvwDtSk", GetFkeyDate("IDS", F.col("CUST_SVC_TASK_SK"), F.col("NEXT_RVW_DT_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskRcvdDtSk", GetFkeyDate("IDS", F.col("CUST_SVC_TASK_SK"), F.col("RCVD_DT_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskCatCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK CATEGORY"), F.col("CUST_SVC_TASK_CAT_CD_SK"), F.lit(Logging)))
    .withColumn("svCsTaskClsPlnProdCatCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CLASS PLAN PRODUCT CATEGORY"), F.col("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskClsrPrfCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK CLOSURE PROOF"), F.col("CUST_SVC_TASK_CLSR_PRF_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskCustTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK CUSTOMER TYPE"), F.col("CUST_SVC_TASK_CUST_TYP_CD_SK"), F.lit(Logging)))
    .withColumn("svCsTaskRecpntTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK SUBJECT TYPE"), F.col("CS_TASK_LTR_RECPNT_TYP_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskPgTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK PAGE"), F.col("CUST_SVC_TASK_PG_TYP_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskPrtyCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK PRIORITY"), F.col("CUST_SVC_TASK_PRTY_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskSttusCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK STATUS"), F.col("CUST_SVC_TASK_STTUS_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskSttusRsnCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK ROUTE REASON"), F.col("CUST_SVC_TASK_STTUS_RSN_CD_SK"), F.lit(Logging)))
    .withColumn("svCustSvcTaskSubjCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK SUBJECT"), F.col("CUST_SVC_TASK_SUBJ_CD_SK"), F.lit(Logging)))
    .withColumn("svGrpsk", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("GRGR_ID"), F.lit(Logging)))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("MBR_SK"), F.lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("PROD_SK"), F.lit(Logging)))
    .withColumn("svProvSk", GetFkeyProv(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("PROV_SK"), F.lit(Logging)))
    .withColumn("svSubGrpSk", GetFkeySubgrp(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("GRGR_ID"), F.col("SGSG_ID"), F.lit(Logging)))
    .withColumn("svSubSk", GetFkeySub(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("SUB_SK"), F.lit(Logging)))
    .withColumn("svCustSvcSk", GetFkeyCustSvc(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.col("CUST_SVC_ID"), F.lit(Logging)))
    .withColumn("svClsdDtSk", GetFkeyDate("IDS", F.col("CUST_SVC_TASK_SK"), F.col("CLSD_DT_SK"), F.lit(Logging)))
    .withColumn("svFinlActnCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"), F.lit("CUSTOMER SERVICE TASK FINAL ACTION"), F.col("CUST_SVC_TASK_FINL_ACTN_CD_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CUST_SVC_TASK_SK")))
)

df_CustSrvTaskOut = df_PurgeTrnCustSrvTask.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svCustSvcSk").alias("CUST_SVC_SK"),
    F.when((F.col("GRGR_ID") == 0) | (F.col("GRGR_ID").isNull()), F.lit(1))
     .otherwise(F.col("svGrpsk")).alias("GRP_SK"),
    F.col("svAppUsrSk").alias("INPT_USER_SK"),
    F.col("svLastUpdUserSk").alias("LAST_UPDT_USER_SK"),
    F.when((F.col("MBR_SK") == 0) | (F.col("MBR_SK").isNull()), F.lit(1))
     .otherwise(F.col("svMbrSk")).alias("MBR_SK"),
    F.when((F.col("PROD_SK") == 0) | (F.col("PROD_SK").isNull()), F.lit(1))
     .otherwise(F.col("svProdSk")).alias("PROD_SK"),
    F.when((F.col("PROV_SK") == 0) | (F.col("PROV_SK").isNull()), F.lit(1))
     .otherwise(F.col("svProvSk")).alias("PROV_SK"),
    F.when(((F.col("SGSG_ID") == 0) | (F.col("SGSG_ID").isNull())) |
           ((F.col("GRGR_ID") == 0) | (F.col("GRGR_ID").isNull())),
           F.lit(1)).otherwise(F.col("svSubGrpSk")).alias("SUBGRP_SK"),
    F.when((F.col("SUB_SK") == 0) | (F.col("SUB_SK").isNull()), F.lit(1))
     .otherwise(F.col("svSubSk")).alias("SUB_SK"),
    F.col("svCustSvcTaskCatCdSk").alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.col("svCsTaskClsPlnProdCatCdSk").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("svCustSvcTaskClsrPrfCdSk").alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
    F.col("svCustSvcTaskCustTypCdSk").alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
    F.col("svFinlActnCdSk").alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
    F.when(F.col("SRC_SYS_CD") == "NPS",
           GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                        F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                        F.col("CUST_SVC_TASK_ITS_TYP_CD_SK"), F.lit(Logging)))
     .when(F.col("USUS_ID") == "ITSHSCS",
           GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                        F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                        F.lit("HOST"), F.lit(Logging)))
     .when(F.col("USUS_ID") == "ITS",
           GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                        F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                        F.lit("HOME"), F.lit(Logging)))
     .when(F.col("USUS_ID") == "ITSHOST",
           GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                        F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                        F.lit("HOST"), F.lit(Logging)))
     .when((F.col("CSTK_MCTR_SUBJ_1") == "SxI2") | (F.col("CSTK_MCTR_SUBJ_1") == "SxI3"),
           GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                        F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                        F.lit("HOST"), F.lit(Logging)))
     .otherwise(
         GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                      F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                      F.lit("NONE"), F.lit(Logging))
     ).alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
    F.col("svCsTaskRecpntTypCdSk").alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
    F.col("svCustSvcTaskPgTypCdSk").alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
    F.col("svCustSvcTaskPrtyCdSk").alias("CUST_SVC_TASK_PRTY_CD_SK"),
    F.col("svCustSvcTaskSttusCdSk").alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.col("svCustSvcTaskSttusRsnCdSk").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.col("svCustSvcTaskSubjCdSk").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    F.col("CMPLNT_IN").alias("CMPLNT_IN"),
    F.col("svClsdDtSk").alias("CLSD_DT_SK"),
    F.col("INPT_DTM").alias("INPT_DTM"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("svCustSvcTaskNextRvwDtSk").alias("NEXT_RVW_DT_SK"),
    F.when(F.col("svCustSvcTaskRcvdDtSk") == "UNK", F.lit("1753-01-01"))
     .otherwise(F.col("svCustSvcTaskRcvdDtSk")).alias("RCVD_DT_SK"),
    F.col("NEXT_RVW_INTRVL_DAYS").alias("NEXT_RVW_INTRVL_DAYS"),
    F.col("CUST_ID").alias("CUST_ID"),
    F.col("CUST_SVC_TASK_USER_SITE_ID").alias("CUST_SVC_TASK_USER_SITE_ID"),
    F.col("INPT_USER_CC_ID").alias("INPT_USER_CC_ID"),
    F.col("SUM_DESC").alias("SUM_DESC")
)

df_lnkRecycle = (
    df_PurgeTrnCustSrvTask
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("CUST_SVC_TASK_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
        F.when((F.col("GRGR_ID") == 0) | (F.col("GRGR_ID").isNull()), F.lit(1))
         .otherwise(F.col("svGrpsk")).alias("GRP_SK"),
        F.col("svAppUsrSk").alias("INPT_USER_SK"),
        F.col("svLastUpdUserSk").alias("LAST_UPDT_USER_SK"),
        F.when((F.col("MBR_SK") == 0) | (F.col("MBR_SK").isNull()), F.lit(1))
         .otherwise(F.col("svMbrSk")).alias("MBR_SK"),
        F.when((F.col("PROD_SK") == 0) | (F.col("PROD_SK").isNull()), F.lit(1))
         .otherwise(F.col("svProdSk")).alias("PROD_SK"),
        F.when((F.col("PROV_SK") == 0) | (F.col("PROV_SK").isNull()), F.lit(1))
         .otherwise(F.col("svProvSk")).alias("PROV_SK"),
        F.when(((F.col("SGSG_ID") == 0) | (F.col("SGSG_ID").isNull())) |
               ((F.col("GRGR_ID") == 0) | (F.col("GRGR_ID").isNull())),
               F.lit(1)).otherwise(F.col("svSubGrpSk")).alias("SUBGRP_SK"),
        F.when((F.col("SUB_SK") == 0) | (F.col("SUB_SK").isNull()), F.lit(1))
         .otherwise(F.col("svSubSk")).alias("SUB_SK"),
        F.col("svCustSvcTaskCatCdSk").alias("CUST_SVC_TASK_CAT_CD_SK"),
        F.col("svCsTaskClsPlnProdCatCdSk").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("svCustSvcTaskClsrPrfCdSk").alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
        F.col("svCustSvcTaskCustTypCdSk").alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
        F.col("svFinlActnCdSk").alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
        F.when(F.col("SRC_SYS_CD") == "NPS",
               GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                            F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                            F.col("CUST_SVC_TASK_ITS_TYP_CD_SK"), F.lit(Logging)))
         .when(F.col("USUS_ID") == "ITSHSCS",
               GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                            F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                            F.lit("HOST"), F.lit(Logging)))
         .when(F.col("USUS_ID") == "ITS",
               GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                            F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                            F.lit("HOME"), F.lit(Logging)))
         .when(F.col("USUS_ID") == "ITSHOST",
               GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                            F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                            F.lit("HOST"), F.lit(Logging)))
         .when((F.col("CSTK_MCTR_SUBJ_1") == "SxI2") | (F.col("CSTK_MCTR_SUBJ_1") == "SxI3"),
               GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                            F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                            F.lit("HOST"), F.lit(Logging)))
         .otherwise(
             GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("CUST_SVC_TASK_SK"),
                          F.lit("CUSTOMER SERVICE TASK ITS TYPE"),
                          F.lit("NONE"), F.lit(Logging))
         ).alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
        F.col("svCsTaskRecpntTypCdSk").alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
        F.col("svCustSvcTaskPgTypCdSk").alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
        F.col("svCustSvcTaskPrtyCdSk").alias("CUST_SVC_TASK_PRTY_CD_SK"),
        F.col("svCustSvcTaskSttusCdSk").alias("CUST_SVC_TASK_STTUS_CD_SK"),
        F.col("svCustSvcTaskSttusRsnCdSk").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
        F.col("svCustSvcTaskSubjCdSk").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
        F.col("CMPLNT_IN").alias("CMPLNT_IN"),
        F.col("svClsdDtSk").alias("CLSD_DT_SK"),
        F.col("INPT_DTM").alias("INPT_DTM"),
        F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
        F.col("svCustSvcTaskNextRvwDtSk").alias("NEXT_RVW_DT_SK"),
        F.col("svCustSvcTaskRcvdDtSk").alias("RCVD_DT_SK"),
        F.col("NEXT_RVW_INTRVL_DAYS").alias("NEXT_RVW_INTRVL_DAYS"),
        F.col("CUST_ID").alias("CUST_ID"),
        F.col("CUST_SVC_TASK_USER_SITE_ID").alias("CUST_SVC_TASK_USER_SITE_ID"),
        F.col("INPT_USER_CC_ID").alias("INPT_USER_CC_ID"),
        F.col("SUM_DESC").alias("SUM_DESC")
    )
)

df_Recycle_Keys = (
    df_PurgeTrnCustSrvTask
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
    )
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Recycle_Keys,
    "hf_custsvc_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_temp_with_rn = df_PurgeTrnCustSrvTask.withColumn(
    "rn",
    F.row_number().over(Window.orderBy(F.lit(1)))
)

df_DefaultUNK = df_temp_with_rn.filter(F.col("rn") == 1).select(
    F.lit(0).alias("CUST_SVC_TASK_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CUST_SVC_ID"),
    F.lit(0).alias("TASK_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CUST_SVC_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("INPT_USER_SK"),
    F.lit(0).alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.lit(0).alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
    F.lit(0).alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_PRTY_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.lit(0).alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    F.lit("U").alias("CMPLNT_IN"),
    F.lit("UNK").alias("CLSD_DT_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("INPT_DTM"),
    F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
    F.lit("UNK").alias("NEXT_RVW_DT_SK"),
    F.lit("UNK").alias("RCVD_DT_SK"),
    F.lit(0).alias("NEXT_RVW_INTRVL_DAYS"),
    F.lit("UNK").alias("CUST_ID"),
    F.lit("UNK").alias("CUST_SVC_TASK_USER_SITE_ID"),
    F.lit("UNK").alias("INPT_USER_CC_ID"),
    F.lit("UNK").alias("SUM_DESC")
)

df_DefaultNA = df_temp_with_rn.filter(F.col("rn") == 1).select(
    F.lit(1).alias("CUST_SVC_TASK_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CUST_SVC_ID"),
    F.lit(1).alias("TASK_SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CUST_SVC_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("INPT_USER_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("PROV_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("CUST_SVC_TASK_CAT_CD_SK"),
    F.lit(1).alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
    F.lit(1).alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_PRTY_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_STTUS_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
    F.lit(1).alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    F.lit("X").alias("CMPLNT_IN"),
    F.lit("NA").alias("CLSD_DT_SK"),
    F.lit("1753-01-01-00.00.00.000000").alias("INPT_DTM"),
    F.lit("1753-01-01-00.00.00.000000").alias("LAST_UPDT_DTM"),
    F.lit("NA").alias("NEXT_RVW_DT_SK"),
    F.lit("NA").alias("RCVD_DT_SK"),
    F.lit(1).alias("NEXT_RVW_INTRVL_DAYS"),
    F.lit("NA").alias("CUST_ID"),
    F.lit("NA").alias("CUST_SVC_TASK_USER_SITE_ID"),
    F.lit("NA").alias("INPT_USER_CC_ID"),
    F.lit("NA").alias("SUM_DESC")
)

df_Collector = (
    df_CustSrvTaskOut
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

final_cols = [
    "CUST_SVC_TASK_SK",
    "SRC_SYS_CD_SK",
    "CUST_SVC_ID",
    "TASK_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_SK",
    "GRP_SK",
    "INPT_USER_SK",
    "LAST_UPDT_USER_SK",
    "MBR_SK",
    "PROD_SK",
    "PROV_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "CUST_SVC_TASK_CAT_CD_SK",
    "CS_TASK_CLS_PLN_PROD_CAT_CD_SK",
    "CUST_SVC_TASK_CLSR_PRF_CD_SK",
    "CUST_SVC_TASK_CUST_TYP_CD_SK",
    "CUST_SVC_TASK_FINL_ACTN_CD_SK",
    "CUST_SVC_TASK_ITS_TYP_CD_SK",
    "CS_TASK_LTR_RECPNT_TYP_CD_SK",
    "CUST_SVC_TASK_PG_TYP_CD_SK",
    "CUST_SVC_TASK_PRTY_CD_SK",
    "CUST_SVC_TASK_STTUS_CD_SK",
    "CUST_SVC_TASK_STTUS_RSN_CD_SK",
    "CUST_SVC_TASK_SUBJ_CD_SK",
    "CMPLNT_IN",
    "CLSD_DT_SK",
    "INPT_DTM",
    "LAST_UPDT_DTM",
    "NEXT_RVW_DT_SK",
    "RCVD_DT_SK",
    "NEXT_RVW_INTRVL_DAYS",
    "CUST_ID",
    "CUST_SVC_TASK_USER_SITE_ID",
    "INPT_USER_CC_ID",
    "SUM_DESC"
]

df_final = df_Collector.select(final_cols)

# Apply rpad for char/varchar columns
df_final = (
    df_final
    .withColumn("CUST_SVC_ID", F.rpad(F.col("CUST_SVC_ID"), 255, " "))
    .withColumn("GRP_SK", F.rpad(F.col("GRP_SK"), 255, " "))
    .withColumn("INPT_USER_SK", F.rpad(F.col("INPT_USER_SK"), 255, " "))
    .withColumn("LAST_UPDT_USER_SK", F.rpad(F.col("LAST_UPDT_USER_SK"), 255, " "))
    .withColumn("MBR_SK", F.rpad(F.col("MBR_SK"), 255, " "))
    .withColumn("PROD_SK", F.rpad(F.col("PROD_SK"), 255, " "))
    .withColumn("PROV_SK", F.rpad(F.col("PROV_SK"), 255, " "))
    .withColumn("SUBGRP_SK", F.rpad(F.col("SUBGRP_SK"), 255, " "))
    .withColumn("SUB_SK", F.rpad(F.col("SUB_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_CAT_CD_SK", F.rpad(F.col("CUST_SVC_TASK_CAT_CD_SK"), 255, " "))
    .withColumn("CS_TASK_CLS_PLN_PROD_CAT_CD_SK", F.rpad(F.col("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_CLSR_PRF_CD_SK", F.rpad(F.col("CUST_SVC_TASK_CLSR_PRF_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_CUST_TYP_CD_SK", F.rpad(F.col("CUST_SVC_TASK_CUST_TYP_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_FINL_ACTN_CD_SK", F.rpad(F.col("CUST_SVC_TASK_FINL_ACTN_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_ITS_TYP_CD_SK", F.rpad(F.col("CUST_SVC_TASK_ITS_TYP_CD_SK"), 255, " "))
    .withColumn("CS_TASK_LTR_RECPNT_TYP_CD_SK", F.rpad(F.col("CS_TASK_LTR_RECPNT_TYP_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_PG_TYP_CD_SK", F.rpad(F.col("CUST_SVC_TASK_PG_TYP_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_PRTY_CD_SK", F.rpad(F.col("CUST_SVC_TASK_PRTY_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_STTUS_CD_SK", F.rpad(F.col("CUST_SVC_TASK_STTUS_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_STTUS_RSN_CD_SK", F.rpad(F.col("CUST_SVC_TASK_STTUS_RSN_CD_SK"), 255, " "))
    .withColumn("CUST_SVC_TASK_SUBJ_CD_SK", F.rpad(F.col("CUST_SVC_TASK_SUBJ_CD_SK"), 255, " "))
    .withColumn("CMPLNT_IN", F.rpad(F.col("CMPLNT_IN"), 1, " "))
    .withColumn("CLSD_DT_SK", F.rpad(F.col("CLSD_DT_SK"), 10, " "))
    .withColumn("NEXT_RVW_DT_SK", F.rpad(F.col("NEXT_RVW_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " "))
    .withColumn("CUST_ID", F.rpad(F.col("CUST_ID"), 255, " "))
    .withColumn("CUST_SVC_TASK_USER_SITE_ID", F.rpad(F.col("CUST_SVC_TASK_USER_SITE_ID"), 255, " "))
    .withColumn("INPT_USER_CC_ID", F.rpad(F.col("INPT_USER_CC_ID"), 255, " "))
    .withColumn("SUM_DESC", F.rpad(F.col("SUM_DESC"), 255, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)