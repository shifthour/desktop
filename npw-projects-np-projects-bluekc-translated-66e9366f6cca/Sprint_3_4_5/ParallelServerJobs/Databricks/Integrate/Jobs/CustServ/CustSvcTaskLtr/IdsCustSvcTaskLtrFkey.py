# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 10/21/08 10:40:52 Batch  14905_38463 PROMOTE bckcetl ids20 dsadm rc for brent  
# MAGIC ^1_1 10/21/08 10:17:42 Batch  14905_37069 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 10/20/08 12:51:44 Batch  14904_46312 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_1 10/20/08 10:48:46 Batch  14904_38931 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_3 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
# MAGIC ^1_3 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_2 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent
# MAGIC ^1_2 05/21/07 13:43:57 Batch  14386_49443 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_2 05/21/07 13:37:43 Batch  14386_49068 INIT bckcett testIDS30 dsadm bls for rt
# MAGIC ^1_1 05/21/07 10:17:11 Batch  14386_37038 INIT bckcett testIDS30 dsadm bls for rt
# MAGIC ^1_3 05/08/07 14:17:42 Batch  14373_51469 PROMOTE bckcett testIDS30 u03651 steph for ralph
# MAGIC ^1_3 05/08/07 14:04:19 Batch  14373_50664 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 04/06/07 14:05:51 Batch  14341_50754 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_1 04/05/07 13:31:14 Batch  14340_48677 INIT bckcett devlIDS30 dsadm dsadm
# MAGIC ^1_2 04/04/07 08:38:14 Batch  14339_31097 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/30/07 10:52:52 Batch  14334_39177 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #                     Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------                   ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     12/02/2007                Initial program                                                                                    CustSvc/3028           devlIDS30                          Steph Goddard          02/13/2007
# MAGIC Brent Leland         02/29/2008              Added error recycle                                                                           3567 Primary Key       devlIDScur                          Steph Goddard         05/06/2008
# MAGIC                                                               Added source system to load file name

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC End of Facets to IDS extraction of CUST_SVC_TASK_LTR
# MAGIC 
# MAGIC No data in physical table of CUST_SVC_TASK
# MAGIC This hash file is written to by all customer service foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col, lit, row_number, rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
RunCycle = get_widget_value('RunCycle','100')
InFile = get_widget_value('InFile','')
RunID = get_widget_value('RunID','20070112929')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

schema_IdsCustSrvTaskLtrFkey = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CUST_SVC_TASK_LTR_SK", IntegerType(), False),
    StructField("CUST_SVC_ID", StringType(), False),
    StructField("TASK_SEQ_NO", IntegerType(), False),
    StructField("CUST_SVC_TASK_LTR_STYLE_CD_SK", IntegerType(), False),
    StructField("LTR_SEQ_NO", IntegerType(), False),
    StructField("LTR_DEST_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CRT_BY_USER_SK", IntegerType(), False),
    StructField("CUST_SVC_TASK_SK", IntegerType(), False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), False),
    StructField("CUST_SVC_TASK_LTR_REPRT_STTUS_", IntegerType(), False),
    StructField("CUST_SVC_TASK_LTR_TYP_CD_SK", IntegerType(), False),
    StructField("CRT_DT_SK", StringType(), False),
    StructField("LAST_UPDT_DT_SK", StringType(), False),
    StructField("MAILED_DT_SK", StringType(), False),
    StructField("PRTED_DT_SK", StringType(), False),
    StructField("RQST_DT_SK", StringType(), False),
    StructField("SUBMT_DT_SK", StringType(), False),
    StructField("MAILED_IN", StringType(), False),
    StructField("PRTED_IN", StringType(), False),
    StructField("RQST_IN", StringType(), False),
    StructField("SUBMT_IN", StringType(), False),
    StructField("RECPNT_NM", StringType(), True),
    StructField("RECPNT_ADDR_LN_1", StringType(), True),
    StructField("RECPNT_ADDR_LN_2", StringType(), True),
    StructField("RECPNT_ADDR_LN_3", StringType(), True),
    StructField("RECPNT_CITY_NM", StringType(), True),
    StructField("CS_TASK_LTR_RECPNT_ST_CD_SK", IntegerType(), False),
    StructField("RECPNT_POSTAL_CD", StringType(), True),
    StructField("RECPNT_CNTY_NM", StringType(), True),
    StructField("RECPNT_PHN_NO", StringType(), True),
    StructField("RECPNT_PHN_NO_EXT", StringType(), False),
    StructField("RECPNT_FAX_NO", StringType(), True),
    StructField("REF_NM", StringType(), True),
    StructField("REF_ADDR_LN_1", StringType(), True),
    StructField("REF_ADDR_LN_2", StringType(), True),
    StructField("REF_ADDR_LN_3", StringType(), True),
    StructField("REF_CITY_NM", StringType(), True),
    StructField("CUST_SVC_TASK_LTR_REF_ST_CD_SK", IntegerType(), False),
    StructField("REF_POSTAL_CD", StringType(), True),
    StructField("REF_CNTY_NM", StringType(), True),
    StructField("REF_PHN_NO", StringType(), False),
    StructField("REF_PHN_NO_EXT", StringType(), True),
    StructField("REF_FAX_NO", StringType(), True),
    StructField("SEND_NM", StringType(), True),
    StructField("SEND_ADDR_LN_1", StringType(), True),
    StructField("SEND_ADDR_LN_2", StringType(), True),
    StructField("SEND_ADDR_LN_3", StringType(), True),
    StructField("SEND_CITY_NM", StringType(), True),
    StructField("CUST_SVC_TASK_LTR_SEND_ST_CD_S", IntegerType(), False),
    StructField("SEND_POSTAL_CD", StringType(), True),
    StructField("SEND_CNTY_NM", StringType(), True),
    StructField("SEND_PHN_NO", StringType(), True),
    StructField("SEND_PHN_NO_EXT", StringType(), True),
    StructField("SEND_FAX_NO", StringType(), True),
    StructField("EXPL_TX", StringType(), True),
    StructField("LTR_TX_1", StringType(), True),
    StructField("LTR_TX_2", StringType(), True),
    StructField("LTR_TX_3", StringType(), True),
    StructField("LTR_TX_4", StringType(), True),
    StructField("MSG_TX", StringType(), True)
])

df_IdsCustSrvTaskLtrFkey = (
    spark.read
    .option("header", False)
    .option("quote", '"')
    .schema(schema_IdsCustSrvTaskLtrFkey)
    .csv(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(lit(1))

df_Tx = (
    df_IdsCustSrvTaskLtrFkey
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svCustSvcTaskLtrSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("ATTACHMENT TYPE"),
                    col("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svAppUsrSk",
                GetFkeyAppUsr(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("CRT_BY_USER_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svLastUpdUserSk",
                GetFkeyAppUsr(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("LAST_UPDT_USER_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svCustSvcTaskLtrReprtSttusSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("CLAIM LETTER REPRINT STATUS"),
                    col("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
                    lit(Logging)
                )
    )
    .withColumn("svCustSvcTaskLtrTypSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("CLAIM LETTER TYPE"),
                    col("CUST_SVC_TASK_LTR_TYP_CD_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svCrtDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("CRT_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svlastUpdtDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("LAST_UPDT_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svMailedDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("MAILED_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svPrtedDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("PRTED_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svRqstDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("RQST_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svSubmtDtSk",
                GetFkeyDate(
                    lit("IDS"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("SUBMT_DT_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svCsTaskLtrRecpntStSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("STATE"),
                    col("CS_TASK_LTR_RECPNT_ST_CD_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svCustSvcTaskLtrReftSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("STATE"),
                    col("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
                    lit(Logging)
                )
    )
    .withColumn("svCustSvcTaskLtrSendStSk",
                GetFkeyCodes(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    lit("STATE"),
                    col("CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
                    lit(Logging)
                )
    )
    .withColumn("svCustSvcTaskSk",
                GetFkeyCustSvcTask(
                    col("SRC_SYS_CD"),
                    col("CUST_SVC_TASK_LTR_SK"),
                    col("CUST_SVC_ID"),
                    col("TASK_SEQ_NO"),
                    lit(Logging)
                )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CUST_SVC_TASK_LTR_SK")))
    .withColumn("rownum", row_number().over(w))
)

df_CustSrvTaskLtrOut = df_Tx.filter((col("ErrCount") == 0) | (col("PassThru") == "Y")).select(
    col("CUST_SVC_TASK_LTR_SK").alias("CUST_SVC_TASK_LTR_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    col("svCustSvcTaskLtrSk").alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
    col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    col("LTR_DEST_ID").alias("LTR_DEST_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svAppUsrSk").alias("CRT_BY_USER_SK"),
    col("svCustSvcTaskSk").alias("CUST_SVC_TASK_SK"),
    col("svLastUpdUserSk").alias("LAST_UPDT_USER_SK"),
    col("svCustSvcTaskLtrReprtSttusSk").alias("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
    col("svCustSvcTaskLtrTypSk").alias("CUST_SVC_TASK_LTR_TYP_CD_SK"),
    col("svCrtDtSk").alias("CRT_DT_SK"),
    col("svlastUpdtDtSk").alias("LAST_UPDT_DT_SK"),
    col("svMailedDtSk").alias("MAILED_DT_SK"),
    col("svPrtedDtSk").alias("PRTED_DT_SK"),
    col("svRqstDtSk").alias("RQST_DT_SK"),
    col("svSubmtDtSk").alias("SUBMT_DT_SK"),
    col("MAILED_IN").alias("MAILED_IN"),
    col("PRTED_IN").alias("PRTED_IN"),
    col("RQST_IN").alias("RQST_IN"),
    col("SUBMT_IN").alias("SUBMT_IN"),
    col("RECPNT_NM").alias("RECPNT_NM"),
    col("RECPNT_ADDR_LN_1").alias("RECPNT_ADDR_LN_1"),
    col("RECPNT_ADDR_LN_2").alias("RECPNT_ADDR_LN_2"),
    col("RECPNT_ADDR_LN_3").alias("RECPNT_ADDR_LN_3"),
    col("RECPNT_CITY_NM").alias("RECPNT_CITY_NM"),
    col("svCsTaskLtrRecpntStSk").alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
    col("RECPNT_POSTAL_CD").alias("RECPNT_POSTAL_CD"),
    col("RECPNT_CNTY_NM").alias("RECPNT_CNTY_NM"),
    col("RECPNT_PHN_NO").alias("RECPNT_PHN_NO"),
    col("RECPNT_PHN_NO_EXT").alias("RECPNT_PHN_NO_EXT"),
    col("RECPNT_FAX_NO").alias("RECPNT_FAX_NO"),
    col("REF_NM").alias("REF_NM"),
    col("REF_ADDR_LN_1").alias("REF_ADDR_LN_1"),
    col("REF_ADDR_LN_2").alias("REF_ADDR_LN_2"),
    col("REF_ADDR_LN_3").alias("REF_ADDR_LN_3"),
    col("REF_CITY_NM").alias("REF_CITY_NM"),
    col("svCustSvcTaskLtrReftSk").alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
    col("REF_POSTAL_CD").alias("REF_POSTAL_CD"),
    col("REF_CNTY_NM").alias("REF_CNTY_NM"),
    col("REF_PHN_NO").alias("REF_PHN_NO"),
    col("REF_PHN_NO_EXT").alias("REF_PHN_NO_EXT"),
    col("REF_FAX_NO").alias("REF_FAX_NO"),
    col("SEND_NM").alias("SEND_NM"),
    col("SEND_ADDR_LN_1").alias("SEND_ADDR_LN_1"),
    col("SEND_ADDR_LN_2").alias("SEND_ADDR_LN_2"),
    col("SEND_ADDR_LN_3").alias("SEND_ADDR_LN_3"),
    col("SEND_CITY_NM").alias("SEND_CITY_NM"),
    col("svCustSvcTaskLtrSendStSk").alias("CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
    col("SEND_POSTAL_CD").alias("SEND_POSTAL_CD"),
    col("SEND_CNTY_NM").alias("SEND_CNTY_NM"),
    col("SEND_PHN_NO").alias("SEND_PHN_NO"),
    col("SEND_PHN_NO_EXT").alias("SEND_PHN_NO_EXT"),
    col("SEND_FAX_NO").alias("SEND_FAX_NO"),
    col("EXPL_TX").alias("EXPL_TX"),
    col("LTR_TX_1").alias("LTR_TX_1"),
    col("LTR_TX_2").alias("LTR_TX_2"),
    col("LTR_TX_3").alias("LTR_TX_3"),
    col("LTR_TX_4").alias("LTR_TX_4"),
    col("MSG_TX").alias("MSG_TX")
)

df_lnkRecycle = df_Tx.filter(col("ErrCount") > 0).select(
    GetRecycleKey(col("CUST_SVC_TASK_LTR_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    (col("ERR_CT") + lit(1)).alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CUST_SVC_TASK_LTR_SK").alias("CUST_SVC_TASK_LTR_SK"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    col("CUST_SVC_TASK_LTR_STYLE_CD_SK").alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
    col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
    col("LTR_DEST_ID").alias("LTR_DEST_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
    col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("CUST_SVC_TASK_LTR_REPRT_STTUS_").alias("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
    col("CUST_SVC_TASK_LTR_TYP_CD_SK").alias("CUST_SVC_TASK_LTR_TYP_CD_SK"),
    col("CRT_DT_SK").alias("CRT_DT_SK"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    col("MAILED_DT_SK").alias("MAILED_DT_SK"),
    col("PRTED_DT_SK").alias("PRTED_DT_SK"),
    col("RQST_DT_SK").alias("RQST_DT_SK"),
    col("SUBMT_DT_SK").alias("SUBMT_DT_SK"),
    col("MAILED_IN").alias("MAILED_IN"),
    col("PRTED_IN").alias("PRTED_IN"),
    col("RQST_IN").alias("RQST_IN"),
    col("SUBMT_IN").alias("SUBMT_IN"),
    col("RECPNT_NM").alias("RECPNT_NM"),
    col("RECPNT_ADDR_LN_1").alias("RECPNT_ADDR_LN_1"),
    col("RECPNT_ADDR_LN_2").alias("RECPNT_ADDR_LN_2"),
    col("RECPNT_ADDR_LN_3").alias("RECPNT_ADDR_LN_3"),
    col("RECPNT_CITY_NM").alias("RECPNT_CITY_NM"),
    col("CS_TASK_LTR_RECPNT_ST_CD_SK").alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
    col("RECPNT_POSTAL_CD").alias("RECPNT_POSTAL_CD"),
    col("RECPNT_CNTY_NM").alias("RECPNT_CNTY_NM"),
    col("RECPNT_PHN_NO").alias("RECPNT_PHN_NO"),
    col("RECPNT_PHN_NO_EXT").alias("RECPNT_PHN_NO_EXT"),
    col("RECPNT_FAX_NO").alias("RECPNT_FAX_NO"),
    col("REF_NM").alias("REF_NM"),
    col("REF_ADDR_LN_1").alias("REF_ADDR_LN_1"),
    col("REF_ADDR_LN_2").alias("REF_ADDR_LN_2"),
    col("REF_ADDR_LN_3").alias("REF_ADDR_LN_3"),
    col("REF_CITY_NM").alias("REF_CITY_NM"),
    col("CUST_SVC_TASK_LTR_REF_ST_CD_SK").alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
    col("REF_POSTAL_CD").alias("REF_POSTAL_CD"),
    col("REF_CNTY_NM").alias("REF_CNTY_NM"),
    col("REF_PHN_NO").alias("REF_PHN_NO"),
    col("REF_PHN_NO_EXT").alias("REF_PHN_NO_EXT"),
    col("REF_FAX_NO").alias("REF_FAX_NO"),
    col("SEND_NM").alias("SEND_NM"),
    col("SEND_ADDR_LN_1").alias("SEND_ADDR_LN_1"),
    col("SEND_ADDR_LN_2").alias("SEND_ADDR_LN_2"),
    col("SEND_ADDR_LN_3").alias("SEND_ADDR_LN_3"),
    col("SEND_CITY_NM").alias("SEND_CITY_NM"),
    col("CUST_SVC_TASK_LTR_SEND_ST_CD_S").alias("CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
    col("SEND_POSTAL_CD").alias("SEND_POSTAL_CD"),
    col("SEND_CNTY_NM").alias("SEND_CNTY_NM"),
    col("SEND_PHN_NO").alias("SEND_PHN_NO"),
    col("SEND_PHN_NO_EXT").alias("SEND_PHN_NO_EXT"),
    col("SEND_FAX_NO").alias("SEND_FAX_NO"),
    col("EXPL_TX").alias("EXPL_TX"),
    col("LTR_TX_1").alias("LTR_TX_1"),
    col("LTR_TX_2").alias("LTR_TX_2"),
    col("LTR_TX_3").alias("LTR_TX_3"),
    col("LTR_TX_4").alias("LTR_TX_4"),
    col("MSG_TX").alias("MSG_TX")
)

df_RecycleKeys = df_Tx.filter(col("ErrCount") > 0).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("TASK_SEQ_NO").alias("TASK_SEQ_NO")
)

df_DefaultUNK = df_Tx.filter(col("rownum") == 1).select(
    lit(0).alias("CUST_SVC_TASK_LTR_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CUST_SVC_ID"),
    lit(0).alias("TASK_SEQ_NO"),
    lit(0).alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
    lit(0).alias("LTR_SEQ_NO"),
    lit("UNK").alias("LTR_DEST_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CRT_BY_USER_SK"),
    lit(0).alias("CUST_SVC_TASK_SK"),
    lit(0).alias("LAST_UPDT_USER_SK"),
    lit(0).alias("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
    lit(0).alias("CUST_SVC_TASK_LTR_TYP_CD_SK"),
    lit("UNK").alias("CRT_DT_SK"),
    lit("UNK").alias("LAST_UPDT_DT_SK"),
    lit("UNK").alias("MAILED_DT_SK"),
    lit("UNK").alias("PRTED_DT_SK"),
    lit("UNK").alias("RQST_DT_SK"),
    lit("UNK").alias("SUBMT_DT_SK"),
    lit("U").alias("MAILED_IN"),
    lit("U").alias("PRTED_IN"),
    lit("U").alias("RQST_IN"),
    lit("U").alias("SUBMT_IN"),
    lit("UNK").alias("RECPNT_NM"),
    lit("UNK").alias("RECPNT_ADDR_LN_1"),
    lit("UNK").alias("RECPNT_ADDR_LN_2"),
    lit("UNK").alias("RECPNT_ADDR_LN_3"),
    lit("UNK").alias("RECPNT_CITY_NM"),
    lit(0).alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
    lit("UNK").alias("RECPNT_POSTAL_CD"),
    lit("UNK").alias("RECPNT_CNTY_NM"),
    lit("UNK").alias("RECPNT_PHN_NO"),
    lit("UNK").alias("RECPNT_PHN_NO_EXT"),
    lit("UNK").alias("RECPNT_FAX_NO"),
    lit("UNK").alias("REF_NM"),
    lit("UNK").alias("REF_ADDR_LN_1"),
    lit("UNK").alias("REF_ADDR_LN_2"),
    lit("UNK").alias("REF_ADDR_LN_3"),
    lit("UNK").alias("REF_CITY_NM"),
    lit(0).alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
    lit("UNK").alias("REF_POSTAL_CD"),
    lit("UNK").alias("REF_CNTY_NM"),
    lit("UNK").alias("REF_PHN_NO"),
    lit("UNK").alias("REF_PHN_NO_EXT"),
    lit("UNK").alias("REF_FAX_NO"),
    lit("UNK").alias("SEND_NM"),
    lit("UNK").alias("SEND_ADDR_LN_1"),
    lit("UNK").alias("SEND_ADDR_LN_2"),
    lit("UNK").alias("SEND_ADDR_LN_3"),
    lit("UNK").alias("SEND_CITY_NM"),
    lit(0).alias("CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
    lit("UNK").alias("SEND_POSTAL_CD"),
    lit("UNK").alias("SEND_CNTY_NM"),
    lit("UNK").alias("SEND_PHN_NO"),
    lit("UNK").alias("SEND_PHN_NO_EXT"),
    lit("UNK").alias("SEND_FAX_NO"),
    lit("UNK").alias("EXPL_TX"),
    lit("UNK").alias("LTR_TX_1"),
    lit("UNK").alias("LTR_TX_2"),
    lit("UNK").alias("LTR_TX_3"),
    lit("UNK").alias("LTR_TX_4"),
    lit("UNK").alias("MSG_TX")
)

df_DefaultNA = df_Tx.filter(col("rownum") == 1).select(
    lit(1).alias("CUST_SVC_TASK_LTR_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CUST_SVC_ID"),
    lit(1).alias("TASK_SEQ_NO"),
    lit(1).alias("CUST_SVC_TASK_LTR_STYLE_CD_SK"),
    lit(1).alias("LTR_SEQ_NO"),
    lit("NA").alias("LTR_DEST_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CRT_BY_USER_SK"),
    lit(1).alias("CUST_SVC_TASK_SK"),
    lit(1).alias("LAST_UPDT_USER_SK"),
    lit(1).alias("CUST_SVC_TASK_LTR_REPRT_STTUS_"),
    lit(1).alias("CUST_SVC_TASK_LTR_TYP_CD_SK"),
    lit("NA").alias("CRT_DT_SK"),
    lit("NA").alias("LAST_UPDT_DT_SK"),
    lit("NA").alias("MAILED_DT_SK"),
    lit("NA").alias("PRTED_DT_SK"),
    lit("NA").alias("RQST_DT_SK"),
    lit("NA").alias("SUBMT_DT_SK"),
    lit("X").alias("MAILED_IN"),
    lit("X").alias("PRTED_IN"),
    lit("X").alias("RQST_IN"),
    lit("X").alias("SUBMT_IN"),
    lit("NA").alias("RECPNT_NM"),
    lit("NA").alias("RECPNT_ADDR_LN_1"),
    lit("NA").alias("RECPNT_ADDR_LN_2"),
    lit("NA").alias("RECPNT_ADDR_LN_3"),
    lit("NA").alias("RECPNT_CITY_NM"),
    lit(1).alias("CS_TASK_LTR_RECPNT_ST_CD_SK"),
    lit("NA").alias("RECPNT_POSTAL_CD"),
    lit("NA").alias("RECPNT_CNTY_NM"),
    lit("NA").alias("RECPNT_PHN_NO"),
    lit("NA").alias("RECPNT_PHN_NO_EXT"),
    lit("NA").alias("RECPNT_FAX_NO"),
    lit("NA").alias("REF_NM"),
    lit("NA").alias("REF_ADDR_LN_1"),
    lit("NA").alias("REF_ADDR_LN_2"),
    lit("NA").alias("REF_ADDR_LN_3"),
    lit("NA").alias("REF_CITY_NM"),
    lit(1).alias("CUST_SVC_TASK_LTR_REF_ST_CD_SK"),
    lit("NA").alias("REF_POSTAL_CD"),
    lit("NA").alias("REF_CNTY_NM"),
    lit("NA").alias("REF_PHN_NO"),
    lit("NA").alias("REF_PHN_NO_EXT"),
    lit("NA").alias("REF_FAX_NO"),
    lit("NA").alias("SEND_NM"),
    lit("NA").alias("SEND_ADDR_LN_1"),
    lit("NA").alias("SEND_ADDR_LN_2"),
    lit("NA").alias("SEND_ADDR_LN_3"),
    lit("NA").alias("SEND_CITY_NM"),
    lit(1).alias("CUST_SVC_TASK_LTR_SEND_ST_CD_S"),
    lit("NA").alias("SEND_POSTAL_CD"),
    lit("NA").alias("SEND_CNTY_NM"),
    lit("NA").alias("SEND_PHN_NO"),
    lit("NA").alias("SEND_PHN_NO_EXT"),
    lit("NA").alias("SEND_FAX_NO"),
    lit("NA").alias("EXPL_TX"),
    lit("NA").alias("LTR_TX_1"),
    lit("NA").alias("LTR_TX_2"),
    lit("NA").alias("LTR_TX_3"),
    lit("NA").alias("LTR_TX_4"),
    lit("NA").alias("MSG_TX")
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

write_files(
    df_RecycleKeys,
    "hf_custsvc_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Collector = (
    df_CustSrvTaskLtrOut
    .union(
        df_DefaultUNK.select(df_CustSrvTaskLtrOut.columns)
    )
    .union(
        df_DefaultNA.select(df_CustSrvTaskLtrOut.columns)
    )
)

df_final = (
    df_Collector
    .withColumn("CRT_DT_SK", rpad(col("CRT_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", rpad(col("LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("MAILED_DT_SK", rpad(col("MAILED_DT_SK"), 10, " "))
    .withColumn("PRTED_DT_SK", rpad(col("PRTED_DT_SK"), 10, " "))
    .withColumn("RQST_DT_SK", rpad(col("RQST_DT_SK"), 10, " "))
    .withColumn("SUBMT_DT_SK", rpad(col("SUBMT_DT_SK"), 10, " "))
    .withColumn("MAILED_IN", rpad(col("MAILED_IN"), 1, " "))
    .withColumn("PRTED_IN", rpad(col("PRTED_IN"), 1, " "))
    .withColumn("RQST_IN", rpad(col("RQST_IN"), 1, " "))
    .withColumn("SUBMT_IN", rpad(col("SUBMT_IN"), 1, " "))
    .withColumn("RECPNT_PHN_NO_EXT", rpad(col("RECPNT_PHN_NO_EXT"), 5, " "))
    .withColumn("REF_PHN_NO_EXT", rpad(col("REF_PHN_NO_EXT"), 5, " "))
    .withColumn("SEND_PHN_NO_EXT", rpad(col("SEND_PHN_NO_EXT"), 5, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK_LTR.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)