# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                2/14/2007        Cust Svc/3028           Originally Programmed                          devlEDW10              
# MAGIC Ralph Tucker                   6/19/2007    15 - Prod Sup                 CUST_SVC_TASK_SUBJ_CD_SK
# MAGIC                                                                                                      was defaulting to space changed to 0  devlEDW10
# MAGIC Ralph Tucker                   6/26/2007    15 - Prod Sup                 CUST_SVC_TASK_EXTRNL_SUB_ID devlEDW10
# MAGIC                                                                                                      must be limited to 20 chars output
# MAGIC Ralph Tucker                   9/18/2007     15 - Prod Sup                Changed queries to use drvr table         devlEDW10                   Steph Goddard            09/20/2007
# MAGIC 
# MAGIC Rama Kamjula               12/13/2013           5114                           Rewritten from Server 
# MAGIC                                                                                                             to Parallel version                  EnterpriseWrhsDevl                Jag Yelavarthi              2014-01-29
# MAGIC 
# MAGIC Manasa Andru                 2015-09-21       TFS - 9694                  Added NullCheck condition to              EnterpriseDev2             Kalyan Neelam             2015-10-02
# MAGIC                                                                                                            GRP_SK, SUB_SK,
# MAGIC                                                                                             GRP_ID, CUST_SVC_TASK_SUBJ_CD_SK


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CUST_SVC_TASK_CSTM_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \nCUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_SK,\nCOALESCE(CD.TRGT_CD,'UNK' )  SRC_SYS_CD,\nCUST_SVC_DTL.CUST_SVC_TASK_SK,\nCUST_SVC_DTL.CUST_SVC_ID,\nCUST_SVC_DTL.TASK_SEQ_NO,\nCUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK,\nCUST_SVC_DTL.CSTM_DTL_UNIQ_ID,\nCUST_SVC_DTL.CRT_RUN_CYC_EXCTN_SK,\nCUST_SVC_DTL.CSTM_DTL_DT_1_SK,\nCUST_SVC_DTL.CSTM_DTL_MNY_1,\nCUST_SVC_DTL.CSTM_DTL_NO_1,\nCUST_SVC_DTL.CSTM_DTL_TX_1,\nCUST_SVC_DTL.CSTM_DTL_TX_2,\nCUST_SVC_DTL.CSTM_DTL_TX_3,\nCUST_SVC_DTL.CSTM_DTL_DT_2_SK,\nCUST_SVC_DTL.CRT_DTM\n\nFROM \n"
        + IDSOwner
        + ".CUST_SVC_TASK_CSTM_DTL CUST_SVC_DTL INNER JOIN  "
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG  ON    CUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK=CD_MPPNG.CD_MPPNG_SK \nAND CD_MPPNG.TRGT_CD='FUND'    \n\nINNER JOIN  "
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR    ON  CUST_SVC_DTL.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \nAND CUST_SVC_DTL.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n\nLEFT JOIN  "
        + IDSOwner
        + ".CD_MPPNG CD\n\nON  CUST_SVC_DTL.SRC_SYS_CD_SK  =  CD.CD_MPPNG_SK"
    )
    .load()
)

df_lnk_Extract = df_db2_CUST_SVC_TASK_CSTM_DTL

df_db2_MaxSttusDtm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nMAX(CUST_SVC_STTUS.STTUS_DTM)  STTUS_DTM,\nCD_MPPNG.TRGT_CD\nFROM\n"
        + IDSOwner
        + ".CUST_SVC_TASK_STTUS CUST_SVC_STTUS,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_TASK.CUST_SVC_TASK_SK=CUST_SVC_STTUS.CUST_SVC_TASK_SK  \n      AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK=CD_MPPNG.CD_MPPNG_SK  \n      AND CD_MPPNG.TRGT_CD='CLSD' \n\nGROUP BY\nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nCD_MPPNG.TRGT_CD"
    )
    .load()
)

df_lnk_MaxSttusDtm = df_db2_MaxSttusDtm

df_db2_SubjCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \n\nCUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_SK,\nCD_MPPNG.SRC_CD\n\n FROM \n\n"
        + IDSOwner
        + ".CUST_SVC_TASK_CSTM_DTL CUST_SVC_DTL,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\n\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_DTL.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK  \n      AND CUST_SVC_TASK.CUST_SVC_TASK_SUBJ_CD_SK=CD_MPPNG.CD_MPPNG_SK"
    )
    .load()
)

df_lnk_SubjCd = df_db2_SubjCd

df_db2_Age = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \n\nTASK.CUST_SVC_TASK_SK,\nTASK.CLSD_DT_SK,TASK.INPT_DTM,\nTASK.RCVD_DT_SK \n\nFROM\n\n"
        + IDSOwner
        + ".CUST_SVC_TASK TASK,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\nWHERE TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID"
    )
    .load()
)

df_lnk_Age = df_db2_Age

df_db2_MinSttusDtm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \n\nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nMIN(CUST_SVC_STTUS.STTUS_DTM)  STTUS_DTM,\nCD_MPPNG.TRGT_CD,\nCUST_SVC_STTUS.CUST_SVC_TASK_SK   CUST_SVC_TASK_SK1\n\nFROM \n"
        + IDSOwner
        + ".CUST_SVC_TASK_STTUS CUST_SVC_STTUS,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_STTUS.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK  \n      AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK=CD_MPPNG.CD_MPPNG_SK  \n      AND CD_MPPNG.TRGT_CD='CLSD' \n\nGROUP BY\nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nCD_MPPNG.TRGT_CD,"
        + " CUST_SVC_STTUS.CUST_SVC_TASK_SK"
    )
    .load()
)

df_lnk_MinSttusDtm = df_db2_MinSttusDtm

df_lnk_MinStusDtLkup = df_lnk_MinSttusDtm.select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("STTUS_DTM").alias("STTUS_DTM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
)
df_lnk_MinSttusDtmLkp = df_lnk_MinSttusDtm.select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.col("STTUS_DTM").alias("STTUS_DTM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("CUST_SVC_TASK_SK1").alias("CUST_SVC_TASK_SK1"),
)

df_db2_GrpID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \nCUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_SK,\nCUST_SVC_TASK.GRP_SK,\nCUST_SVC_TASK.SUB_SK,\nCUST_SVC_TASK.CUST_SVC_TASK_SUBJ_CD_SK,\nGRP.GRP_ID \nFROM \n"
        + IDSOwner
        + ".CUST_SVC_TASK_CSTM_DTL CUST_SVC_DTL,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".GRP GRP,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_DTL.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK  \n      AND CUST_SVC_TASK.GRP_SK=GRP.GRP_SK  \n      AND CUST_SVC_DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK=CD_MPPNG.CD_MPPNG_SK  \n      AND CD_MPPNG.TRGT_CD='FUND'"
    )
    .load()
)

df_lnk_GrpID = df_db2_GrpID

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT    CD_MPPNG_SK,\nTRGT_CD,\nTRGT_CD_NM\n\n\nFROM   "
        + IDSOwner
        + ".CD_MPPNG"
    )
    .load()
)

df_Copy_635_in = df_db2_CD_MPPNG

df_lnk_CdMppng = df_Copy_635_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_lnk_CustSvcDtlCd = df_Copy_635_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_lnk_GrpIdLkp = (
    df_lnk_GrpID.alias("lnk_GrpID")
    .join(
        df_lnk_CdMppng.alias("lnk_CdMppng"),
        F.col("lnk_GrpID.CUST_SVC_TASK_SUBJ_CD_SK") == F.col("lnk_CdMppng.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_GrpID.CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        F.col("lnk_GrpID.GRP_SK").alias("GRP_SK"),
        F.col("lnk_GrpID.SUB_SK").alias("SUB_SK"),
        F.col("lnk_GrpID.GRP_ID").alias("GRP_ID"),
        F.col("lnk_CdMppng.TRGT_CD").alias("CUST_SVC_TASK_SUBJ_CD"),
        F.col("lnk_CdMppng.TRGT_CD_NM").alias("CUST_SVC_TASK_SUBJ_NM"),
        F.col("lnk_GrpID.CUST_SVC_TASK_SUBJ_CD_SK").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
    )
)

df_db2_CustSvcTaskSttus = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nCUST_SVC_STTUS.STTUS_SEQ_NO,\nCD_MPPNG.TRGT_CD,CUST_SVC_STTUS.STTUS_DTM\n \nFROM\n"
        + IDSOwner
        + ".CUST_SVC_TASK_STTUS CUST_SVC_STTUS,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_STTUS.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK  \n      AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK=CD_MPPNG.CD_MPPNG_SK  \n      AND CD_MPPNG.TRGT_CD<>'CLSD'\norder by\nCUST_SVC_STTUS.CUST_SVC_TASK_SK asc,\nCUST_SVC_STTUS.STTUS_SEQ_NO asc"
    )
    .load()
)

df_lnk_SttusNo = df_db2_CustSvcTaskSttus

df_db2_SttusSeqNo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \n\nCUST_SVC_STTUS.CUST_SVC_TASK_SK,\nCUST_SVC_STTUS.STTUS_DTM,\nCD_MPPNG.TRGT_CD,\nCUST_SVC_STTUS.STTUS_SEQ_NO\n\n FROM\n\n "
        + IDSOwner
        + ".CUST_SVC_TASK_STTUS CUST_SVC_STTUS,\n"
        + IDSOwner
        + ".CUST_SVC_TASK CUST_SVC_TASK,\n"
        + IDSOwner
        + ".CD_MPPNG CD_MPPNG,\n"
        + IDSOwner
        + ".W_CUST_SVC_DRVR DRVR\n\nWHERE CUST_SVC_TASK.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n      AND CUST_SVC_TASK.CUST_SVC_ID       = DRVR.CUST_SVC_ID\n      AND CUST_SVC_STTUS.CUST_SVC_TASK_SK=CUST_SVC_TASK.CUST_SVC_TASK_SK  \n      AND CUST_SVC_STTUS.CUST_SVC_TASK_STTUS_CD_SK=CD_MPPNG.CD_MPPNG_SK  \n      AND CD_MPPNG.TRGT_CD='CLSD'"
    )
    .load()
)

df_lnk_SttusSeqNo = df_db2_SttusSeqNo

df_DSLink617 = (
    df_lnk_MinStusDtLkup.alias("lnk_MinStusDtLkup")
    .join(
        df_lnk_SttusSeqNo.alias("lnk_SttusSeqNo"),
        [
            F.col("lnk_MinStusDtLkup.CUST_SVC_TASK_SK") == F.col(
                "lnk_SttusSeqNo.CUST_SVC_TASK_SK"
            ),
            F.col("lnk_MinStusDtLkup.TRGT_CD") == F.col("lnk_SttusSeqNo.TRGT_CD"),
            F.col("lnk_MinStusDtLkup.STTUS_DTM") == F.col("lnk_SttusSeqNo.STTUS_DTM"),
        ],
        "left",
    )
    .select(
        F.col("lnk_MinStusDtLkup.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_SttusSeqNo.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
    )
)

df_lnk_SttusSeqNoLkup = df_DSLink617.select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.when(F.col("STTUS_SEQ_NO").isNull(), F.lit(0)).otherwise(F.col("STTUS_SEQ_NO")).alias("STTUS_SEQ_NO"),
)

df_lnk_CalcMinSttusDtm = (
    df_lnk_SttusNo.alias("lnk_SttusNo")
    .join(
        df_lnk_SttusSeqNoLkup.alias("lnk_SttusSeqNoLkup"),
        F.col("lnk_SttusNo.CUST_SVC_TASK_SK")
        == F.col("lnk_SttusSeqNoLkup.CUST_SVC_TASK_SK"),
        "left",
    )
    .select(
        F.col("lnk_SttusNo.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_SttusNo.STTUS_SEQ_NO").alias("STTUS_SEQ_NO_SttusNo"),
        F.col("lnk_SttusSeqNoLkup.STTUS_SEQ_NO").alias("STTUS_SEQ_NO_SttusSeqNoLkup"),
        F.col("lnk_SttusNo.STTUS_DTM").alias("STTUS_DTM"),
    )
)

df_DSLink629 = df_lnk_CalcMinSttusDtm.filter(
    F.col("STTUS_SEQ_NO_SttusNo") > F.col("STTUS_SEQ_NO_SttusSeqNoLkup")
).select(
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    F.lit("Y").alias("REOPEN_IN"),
    F.when(
        F.col("STTUS_SEQ_NO_SttusNo") > F.col("STTUS_SEQ_NO_SttusSeqNoLkup"),
        FORMAT_DATE_EE(
            F.col("STTUS_DTM"), "DB2", "TIMESTAMP", "CCYY-MM-DD"
        ),  # Assuming user-defined function
    )
    .otherwise("NA")
    .alias("STTUS_DTM"),
    F.col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK_1"),
)

df_lnk_ReopenLkp = df_DSLink629.groupBy(
    "CUST_SVC_TASK_SK", "REOPEN_IN", "CUST_SVC_TASK_SK_1"
).agg(F.min("STTUS_DTM").alias("STTUS_DTM_MIN"))

df_lnk_Codes = (
    df_lnk_Extract.alias("lnk_Extract")
    .join(
        df_lnk_MaxSttusDtm.alias("lnk_MaxSttusDtm"),
        F.col("lnk_Extract.CUST_SVC_TASK_SK")
        == F.col("lnk_MaxSttusDtm.CUST_SVC_TASK_SK"),
        "left",
    )
    .join(
        df_lnk_SubjCd.alias("lnk_SubjCd"),
        F.col("lnk_Extract.CUST_SVC_TASK_CSTM_DTL_SK")
        == F.col("lnk_SubjCd.CUST_SVC_TASK_CSTM_DTL_SK"),
        "left",
    )
    .join(
        df_lnk_Age.alias("lnk_Age"),
        F.col("lnk_Extract.CUST_SVC_TASK_SK") == F.col("lnk_Age.CUST_SVC_TASK_SK"),
        "left",
    )
    .join(
        df_lnk_MinSttusDtmLkp.alias("lnk_MinSttusDtmLkp"),
        F.col("lnk_Extract.CUST_SVC_TASK_SK")
        == F.col("lnk_MinSttusDtmLkp.CUST_SVC_TASK_SK"),
        "left",
    )
    .join(
        df_lnk_ReopenLkp.alias("lnk_ReopenLkp"),
        F.col("lnk_Extract.CUST_SVC_TASK_SK")
        == F.col("lnk_ReopenLkp.CUST_SVC_TASK_SK"),
        "left",
    )
    .join(
        df_lnk_CustSvcDtlCd.alias("lnk_CustSvcDtlCd"),
        F.col("lnk_Extract.CUST_SVC_TASK_CSTM_DTL_CD_SK")
        == F.col("lnk_CustSvcDtlCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_lnk_GrpIdLkp.alias("lnk_GrpIdLkp"),
        F.col("lnk_Extract.CUST_SVC_TASK_CSTM_DTL_SK")
        == F.col("lnk_GrpIdLkp.CUST_SVC_TASK_CSTM_DTL_SK"),
        "left",
    )
    .select(
        F.col("lnk_Extract.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        F.col("lnk_MinSttusDtmLkp.STTUS_DTM").alias("STTUS_DTM_MinSttusDtm"),
        F.col("lnk_MinSttusDtmLkp.CUST_SVC_TASK_SK1").alias("CUST_SVC_TASK_SK_MinSttusDtm"),
        F.col("lnk_MaxSttusDtm.STTUS_DTM").alias("STTUS_DTM_MaxSttusDtm"),
        F.col("lnk_SubjCd.SRC_CD").alias("SRC_CD"),
        F.col("lnk_ReopenLkp.STTUS_DTM_MIN").alias("STTUS_DTM_ReopenLkp"),
        F.col("lnk_ReopenLkp.REOPEN_IN").alias("REOPEN_IN_ReopenLkp"),
        F.col("lnk_ReopenLkp.CUST_SVC_TASK_SK_1").alias("CUST_SVC_TASK_SK_ReopenLkp"),
        F.col("lnk_GrpIdLkp.GRP_SK").alias("GRP_SK"),
        F.col("lnk_GrpIdLkp.SUB_SK").alias("SUB_SK"),
        F.col("lnk_GrpIdLkp.GRP_ID").alias("GRP_ID"),
        F.col("lnk_GrpIdLkp.CUST_SVC_TASK_SUBJ_CD").alias("CUST_SVC_TASK_SUBJ_CD"),
        F.col("lnk_GrpIdLkp.CUST_SVC_TASK_SUBJ_NM").alias("CUST_SVC_TASK_SUBJ_NM"),
        F.col("lnk_GrpIdLkp.CUST_SVC_TASK_SUBJ_CD_SK").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
        F.col("lnk_CustSvcDtlCd.TRGT_CD").alias("TRGT_CD_CustSvcDtlCd"),
        F.col("lnk_Age.CLSD_DT_SK").alias("CLSD_DT_SK"),
        F.col("lnk_Age.INPT_DTM").alias("INPT_DTM"),
        F.col("lnk_Age.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("lnk_Extract.CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
        F.col("lnk_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Extract.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_Extract.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
        F.col("lnk_Extract.CUST_SVC_TASK_CSTM_DTL_CD_SK").alias("CUST_SVC_TASK_CSTM_DTL_CD_SK"),
        F.col("lnk_Extract.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
        F.col("lnk_Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Extract.CRT_DTM").alias("CRT_DTM"),
        F.col("lnk_Extract.CSTM_DTL_DT_1_SK").alias("CSTM_DTL_DT_1_SK"),
        F.col("lnk_Extract.CSTM_DTL_DT_2_SK").alias("CSTM_DTL_DT_2_SK"),
        F.col("lnk_Extract.CSTM_DTL_MNY_1").alias("CSTM_DTL_MNY_1"),
        F.col("lnk_Extract.CSTM_DTL_NO_1").alias("CSTM_DTL_NO_1"),
        F.col("lnk_Extract.CSTM_DTL_TX_1").alias("CSTM_DTL_TX_1"),
        F.col("lnk_Extract.CSTM_DTL_TX_2").alias("CSTM_DTL_TX_2"),
        F.col("lnk_Extract.CSTM_DTL_TX_3").alias("CSTM_DTL_TX_3"),
    )
)

df_BusinessRules_in = df_lnk_Codes.withColumn(
    "svInptDtm", TimestampToDate(F.col("INPT_DTM"))
).withColumn(
    "svRcvdDtSk",
    F.when(
        (F.col("RCVD_DT_SK") == "NA")
        | (F.col("RCVD_DT_SK") == "UNK")
        | (trim(F.col("RCVD_DT_SK")) == ""),
        F.lit("2199-01-01"),
    ).otherwise(StringToDate(F.col("RCVD_DT_SK"), "%yyyy-%mm-%dd")),
).withColumn(
    "svClsdDtSk",
    F.when(
        (F.col("CLSD_DT_SK") == "NA")
        | (F.col("CLSD_DT_SK") == "UNK")
        | (trim(F.col("CLSD_DT_SK")) == ""),
        F.lit("2199-01-01"),
    ).otherwise(StringToDate(F.col("CLSD_DT_SK"), "%yyyy-%mm-%dd")),
)

df_lnk_NA = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            0,
            "NA",
            "1753-01-01 00:00:00",
            "1753-01-01",
            "1753-01-01",
            1,
            1,
            "NA",
            "NA",
            "N",
            "N",
            "N",
            "1753-01-01 00:00:000",
            "1753-01-01",
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            0,
            0,
            "",
            "NA",
            "NA",
            "NA",
            "NA",
            100,
            100,
            1,
        )
    ],
    [
        "CUST_SVC_TASK_FUND_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_CSTM_DTL_CD",
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "SUB_SK",
        "CUST_SVC_TASK_SUBJ_CD",
        "CUST_SVC_TASK_SUBJ_NM",
        "CAP_IN",
        "MULT_CLOSING_IN",
        "REOPEN_IN",
        "CUST_SVC_TASK_FUND_CRT_DTM",
        "CS_TASK_RTRN_FUND_DEP_DT_SK",
        "CS_TASK_RTRN_FUND_RCVD_DT_SK",
        "REOPEN_DT_SK",
        "CUST_SVC_TASK_RTRN_CHK_AMT",
        "CUST_SVC_TASK_RTRN_CHK_NO",
        "TASK_AGE",
        "TASK_CT",
        "CUST_SVC_TASK_EXTRNL_GRP_TX",
        "CUST_SVC_TASK_EXTRNL_SUB_ID",
        "CUST_SVC_TASK_PATN_NM",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_SUBJ_CD_SK",
    ],
)

df_lnk_UNK = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            0,
            "UNK",
            "1753-01-01 00:00:00",
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            "UNK",
            "UNK",
            "N",
            "N",
            "N",
            "1753-01-01 00:00:000",
            "1753-01-01",
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            0,
            0,
            "",
            "UNK",
            "UNK",
            "UNK",
            100,
            100,
            0,
        )
    ],
    [
        "CUST_SVC_TASK_FUND_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "CUST_SVC_TASK_SEQ_NO",
        "CUST_SVC_TASK_CSTM_DTL_CD",
        "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "SUB_SK",
        "CUST_SVC_TASK_SUBJ_CD",
        "CUST_SVC_TASK_SUBJ_NM",
        "CAP_IN",
        "MULT_CLOSING_IN",
        "REOPEN_IN",
        "CUST_SVC_TASK_FUND_CRT_DTM",
        "CS_TASK_RTRN_FUND_DEP_DT_SK",
        "CS_TASK_RTRN_FUND_RCVD_DT_SK",
        "REOPEN_DT_SK",
        "CUST_SVC_TASK_RTRN_CHK_AMT",
        "CUST_SVC_TASK_RTRN_CHK_NO",
        "TASK_AGE",
        "TASK_CT",
        "CUST_SVC_TASK_EXTRNL_GRP_TX",
        "CUST_SVC_TASK_EXTRNL_SUB_ID",
        "CUST_SVC_TASK_PATN_NM",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_SUBJ_CD_SK",
    ],
)

df_lnk_CustSvcTaskFundF_Load = df_BusinessRules_in.filter(
    (F.col("CUST_SVC_TASK_SK") != 0) & (F.col("CUST_SVC_TASK_SK") != 1)
).select(
    F.col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_FUND_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.when(
        F.col("TRGT_CD_CustSvcDtlCd").isNull()
        | (trim(F.col("TRGT_CD_CustSvcDtlCd")) == ""),
        F.lit("NA"),
    ).otherwise(F.col("TRGT_CD_CustSvcDtlCd")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.col("CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.lit("EDWRunCycleDate").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("EDWRunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(
        F.col("GRP_SK").isNull() | (trim(F.col("GRP_SK")) == ""),
        F.lit(0),
    ).otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.when(
        F.col("SUB_SK").isNull() | (trim(F.col("SUB_SK")) == ""),
        F.lit(0),
    ).otherwise(F.col("SUB_SK")).alias("SUB_SK"),
    F.when(
        F.col("CUST_SVC_TASK_SUBJ_CD").isNull()
        | (trim(F.col("CUST_SVC_TASK_SUBJ_CD")) == ""),
        F.lit(" "),
    ).otherwise(F.col("CUST_SVC_TASK_SUBJ_CD")).alias("CUST_SVC_TASK_SUBJ_CD"),
    F.when(
        F.col("CUST_SVC_TASK_SUBJ_NM").isNull()
        | (trim(F.col("CUST_SVC_TASK_SUBJ_NM")) == ""),
        F.lit(" "),
    ).otherwise(F.col("CUST_SVC_TASK_SUBJ_NM")).alias("CUST_SVC_TASK_SUBJ_NM"),
    F.when(
        (Count(F.col("CSTM_DTL_TX_1"), F.lit("CAPITULATION")) != 0)
        | (Count(F.col("CSTM_DTL_TX_1"), F.lit("CAPITATION")) != 0),
        F.lit("Y"),
    ).otherwise(F.lit("N")).alias("CAP_IN"),
    F.when(
        F.col("CUST_SVC_TASK_SK_MinSttusDtm").isNull(),
        F.lit("N"),
    )
    .otherwise(
        F.when(
            (F.col("STTUS_DTM_MaxSttusDtm") == "")
            | F.col("STTUS_DTM_MaxSttusDtm").isNull()
            | (F.col("STTUS_DTM_MinSttusDtm") == "")
            | F.col("STTUS_DTM_MinSttusDtm").isNull(),
            F.lit("N"),
        ).otherwise(
            F.when(
                DaysSinceFromDate(
                    StringToDate(F.col("STTUS_DTM_MaxSttusDtm"), "%yyyy-%mm-%dd"),
                    StringToDate(F.col("STTUS_DTM_MinSttusDtm"), "%yyyy-%mm-%dd"),
                )
                > 1,
                F.lit("Y"),
            ).otherwise(F.lit("N"))
        )
    )
    .alias("MULT_CLOSING_IN"),
    F.when(
        F.col("CUST_SVC_TASK_SK_ReopenLkp").isNull(),
        F.lit("N"),
    ).otherwise(F.col("REOPEN_IN_ReopenLkp")).alias("REOPEN_IN"),
    F.col("CRT_DTM").alias("CUST_SVC_TASK_FUND_CRT_DTM"),
    F.when(
        (F.col("SRC_CD").isNotNull() | (trim(F.col("SRC_CD")) != ""))
        & ((F.col("SRC_CD") == "RA03") | (F.col("SRC_CD") == "RA07")),
        F.col("CSTM_DTL_DT_1_SK"),
    ).otherwise(F.lit("1753-01-01")).alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
    F.when(
        (F.col("SRC_CD").isNotNull() | (trim(F.col("SRC_CD")) != ""))
        & (
            (F.col("SRC_CD") == "RA05")
            | (F.col("SRC_CD") == "RA06")
            | (F.col("SRC_CD") == "RA08")
        ),
        F.col("CSTM_DTL_DT_1_SK"),
    ).otherwise(F.lit("1753-01-01")).alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
    F.when(
        F.col("CUST_SVC_TASK_SK_ReopenLkp").isNull(),
        F.lit("NA"),
    ).otherwise(F.col("STTUS_DTM_ReopenLkp")).alias("REOPEN_DT_SK"),
    F.col("CSTM_DTL_MNY_1").alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
    F.col("CSTM_DTL_NO_1").alias("CUST_SVC_TASK_RTRN_CHK_NO"),
    F.when(
        (F.col("CLSD_DT_SK") == "NA")
        | (F.col("CLSD_DT_SK") == "UNK")
        | (F.col("CLSD_DT_SK") == ""),
        F.lit(100000),
    )
    .otherwise(
        F.when(
            (F.col("svRcvdDtSk") == "2199-01-01") | (F.col("svInptDtm") == "2199-01-01"),
            F.lit(100000),
        )
        .otherwise(
            F.when(
                (F.col("CLSD_DT_SK") != "1753-01-01")
                & (
                    DaysSinceFromDate(F.col("svInptDtm"), F.col("svRcvdDtSk")) + F.lit(1)
                    < 0
                ),
                DaysSinceFromDate(StringToDate(F.col("CLSD_DT_SK"), "%yyyy-%mm-%dd"), F.col("svInptDtm"))
                + F.lit(1),
            )
            .otherwise(
                F.when(
                    (F.col("CLSD_DT_SK") != "1753-01-01")
                    & (
                        DaysSinceFromDate(F.col("svInptDtm"), F.col("svRcvdDtSk"))
                        + F.lit(1)
                        > -1
                    ),
                    DaysSinceFromDate(
                        StringToDate(F.col("CLSD_DT_SK"), "%yyyy-%mm-%dd"),
                        F.col("svRcvdDtSk"),
                    )
                    + F.lit(1),
                )
                .otherwise(
                    F.when(
                        (F.col("CLSD_DT_SK") == "1753-01-01")
                        & (
                            DaysSinceFromDate(F.col("svInptDtm"), F.col("svRcvdDtSk"))
                            + F.lit(1)
                            < 0
                        ),
                        DaysSinceFromDate(
                            F.col("svInptDtm"),
                            F.col("svInptDtm"),
                        )
                        + F.lit(1),
                    )
                    .otherwise(
                        F.when(
                            (F.col("CLSD_DT_SK") == "1753-01-01")
                            & (
                                DaysSinceFromDate(
                                    F.col("svInptDtm"), F.col("svRcvdDtSk")
                                )
                                + F.lit(1)
                                > -1
                            ),
                            DaysSinceFromDate(
                                StringToDate(EDWRunCycleDate, "%yyyy-%mm-%dd"),
                                StringToDate(F.col("RCVD_DT_SK"), "%yyyy-%mm-%dd"),
                            )
                            + F.lit(1),
                        ).otherwise(F.lit(100000))
                    )
                )
            )
        )
    )
    .alias("TASK_AGE"),
    F.lit(1).alias("TASK_CT"),
    F.when(
        F.col("CSTM_DTL_TX_3").isNull() | (trim(F.col("CSTM_DTL_TX_3")) == ""),
        F.lit(" "),
    ).otherwise(trim(F.col("CSTM_DTL_TX_3"))).alias("CUST_SVC_TASK_EXTRNL_GRP_TX"),
    F.when(
        F.col("CSTM_DTL_TX_2").isNull() | (trim(F.col("CSTM_DTL_TX_2")) == ""),
        F.lit(" "),
    ).otherwise(trim(F.col("CSTM_DTL_TX_2"))[0:20]).alias("CUST_SVC_TASK_EXTRNL_SUB_ID"),
    F.when(
        F.col("CSTM_DTL_TX_1").isNull() | (trim(F.col("CSTM_DTL_TX_1")) == ""),
        F.lit(" "),
    ).otherwise(trim(F.col("CSTM_DTL_TX_1"))).alias("CUST_SVC_TASK_PATN_NM"),
    F.when(
        F.col("GRP_ID").isNull() | (trim(F.col("GRP_ID")) == ""),
        F.lit("UNK"),
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.lit("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("CUST_SVC_TASK_SUBJ_CD_SK").isNull()
        | (trim(F.col("CUST_SVC_TASK_SUBJ_CD_SK")) == ""),
        F.lit(0),
    ).otherwise(F.col("CUST_SVC_TASK_SUBJ_CD_SK")).alias("CUST_SVC_TASK_SUBJ_CD_SK"),
)

common_cols = [
    "CUST_SVC_TASK_FUND_SK",
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CUST_SVC_TASK_SEQ_NO",
    "CUST_SVC_TASK_CSTM_DTL_CD",
    "CUST_SVC_TASK_CSTM_DTL_UNIQ_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "SUB_SK",
    "CUST_SVC_TASK_SUBJ_CD",
    "CUST_SVC_TASK_SUBJ_NM",
    "CAP_IN",
    "MULT_CLOSING_IN",
    "REOPEN_IN",
    "CUST_SVC_TASK_FUND_CRT_DTM",
    "CS_TASK_RTRN_FUND_DEP_DT_SK",
    "CS_TASK_RTRN_FUND_RCVD_DT_SK",
    "REOPEN_DT_SK",
    "CUST_SVC_TASK_RTRN_CHK_AMT",
    "CUST_SVC_TASK_RTRN_CHK_NO",
    "TASK_AGE",
    "TASK_CT",
    "CUST_SVC_TASK_EXTRNL_GRP_TX",
    "CUST_SVC_TASK_EXTRNL_SUB_ID",
    "CUST_SVC_TASK_PATN_NM",
    "GRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CUST_SVC_TASK_SUBJ_CD_SK",
]

df_lnk_NA_final = df_lnk_NA.select(common_cols)
df_lnk_UNK_final = df_lnk_UNK.select(common_cols)
df_lnk_CustSvcTaskFundF_Load_final = df_lnk_CustSvcTaskFundF_Load.select(common_cols)

df_fnl_NA_UNK = df_lnk_NA_final.union(df_lnk_CustSvcTaskFundF_Load_final).union(df_lnk_UNK_final)

df_seq_CUST_SVC_TASK_FUND_F = df_fnl_NA_UNK.select(common_cols)

df_seq_CUST_SVC_TASK_FUND_F = df_seq_CUST_SVC_TASK_FUND_F.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "CAP_IN", F.rpad(F.col("CAP_IN"), 1, " ")
).withColumn(
    "MULT_CLOSING_IN", F.rpad(F.col("MULT_CLOSING_IN"), 1, " ")
).withColumn(
    "REOPEN_IN", F.rpad(F.col("REOPEN_IN"), 1, " ")
).withColumn(
    "CS_TASK_RTRN_FUND_DEP_DT_SK", F.rpad(F.col("CS_TASK_RTRN_FUND_DEP_DT_SK"), 10, " ")
).withColumn(
    "CS_TASK_RTRN_FUND_RCVD_DT_SK", F.rpad(F.col("CS_TASK_RTRN_FUND_RCVD_DT_SK"), 10, " ")
).withColumn(
    "REOPEN_DT_SK", F.rpad(F.col("REOPEN_DT_SK"), 10, " ")
)

write_files(
    df_seq_CUST_SVC_TASK_FUND_F,
    f"{adls_path}/load/CUST_SVC_TASK_FUND_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)