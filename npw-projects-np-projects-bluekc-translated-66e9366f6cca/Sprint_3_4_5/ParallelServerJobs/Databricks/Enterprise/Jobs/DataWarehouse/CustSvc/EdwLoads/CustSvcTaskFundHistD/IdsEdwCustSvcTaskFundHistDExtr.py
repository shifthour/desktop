# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *******************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC DESCRIPTION:
# MAGIC The job compares the Previous EDW values of  CUST_SVC_TASK_RTRN_CHK_NO, CUST_SVC_TASK_RTRN_CHK_AMT,CS_TASK_RTRN_FUND_DEP_DT_SK and CS_TASK_RTRN_FUND_RCVD_DT_SK  with the IDS values we are bringing in and changes the corresponding indicators according to the business rules.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                2/14/2007           Cust Svc/3028           Originally Programmed                             devlEDW10              Steph Goddard            7/24/07
# MAGIC Ralph Tucker                   2/4/2008      15                                   Fixed hash file name; was missing the  devlEDW                     Steph Goddard             2/4/08
# MAGIC                                                                                                      'h' in 'hf' in the ETL.
# MAGIC 
# MAGIC Raj Mangalampally           12/20/2013     5114             Original Programmming (Serv to Parallel Conv)    EnterpriseWrhsDevl       Jaf Yelavarthi                 2014-01-29

# MAGIC Extract from IDS table CUST_SVC_TASK_CSTM_DTL and Inner Join on Driver Table - W_CUST_SVC_DRVR and CUST_SVC_TASK
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1)CUST_SVC_TASK_CSTM_DTL_CD_SK
# MAGIC 2)CUST_SVC_TASK_FUND_SK
# MAGIC Job Name; IdsEdwCustSvcTaskFundHistDExtr
# MAGIC 
# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwCustSvcTaskFundHistDExtr
# MAGIC 
# MAGIC Table:
# MAGIC CUST_SVC_TASK_FUND_HIST_D
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Read from IDS (db2_CUST_SVC_TASK_FUND_HIST_D_in)
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
df_db2_CUST_SVC_TASK_FUND_HIST_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option(
        "query",
        "SELECT "
        "DTL.CUST_SVC_TASK_CSTM_DTL_SK, "
        "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, "
        "DTL.CUST_SVC_TASK_SK, "
        "DTL.SRC_SYS_CD_SK, "
        "DTL.CUST_SVC_ID, "
        "DTL.TASK_SEQ_NO, "
        "DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK, "
        "DTL.CSTM_DTL_UNIQ_ID, "
        "DTL.CRT_RUN_CYC_EXCTN_SK, "
        "DTL.CSTM_DTL_DT_1_SK, "
        "DTL.CSTM_DTL_MNY_1, "
        "DTL.CSTM_DTL_NO_1, "
        "TASK.CUST_SVC_TASK_SUBJ_CD_SK "
        "FROM " + IDSOwner + ".CUST_SVC_TASK_CSTM_DTL DTL "
        "LEFT JOIN " + IDSOwner + ".CD_MPPNG CD "
        "ON DTL.SRC_SYS_CD_SK = CD.CD_MPPNG_SK, "
        + IDSOwner + ".CD_MPPNG CD_MPPNG, "
        + IDSOwner + ".CUST_SVC_TASK TASK, "
        + IDSOwner + ".W_CUST_SVC_DRVR DRVR "
        "WHERE DTL.CUST_SVC_TASK_SK=TASK.CUST_SVC_TASK_SK "
        "AND DTL.CUST_SVC_TASK_CSTM_DTL_CD_SK=CD_MPPNG.CD_MPPNG_SK "
        "AND CD_MPPNG.TRGT_CD='FUND' "
        "AND DTL.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK "
        "AND DTL.CUST_SVC_ID = DRVR.CUST_SVC_ID "
        "ORDER BY DTL.CUST_SVC_TASK_SK DESC"
    )
    .load()
)

# Read from EDW (db2_CUST_SVC_TASK_FUND_HIST_D)
jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
df_db2_CUST_SVC_TASK_FUND_HIST_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option(
        "query",
        "SELECT "
        "HIST.CUST_SVC_TASK_FUND_SK, "
        "HIST.EDW_RCRD_STRT_DT_SK, "
        "HIST.SRC_SYS_CD, "
        "HIST.CUST_SVC_ID, "
        "HIST.CUST_SVC_TASK_SEQ_NO, "
        "HIST.CUST_SVC_TASK_CSTM_DTL_CD, "
        "HIST.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID, "
        "HIST.CRT_RUN_CYC_EXCTN_DT_SK, "
        "HIST.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
        "HIST.EDW_CUR_RCRD_IN, "
        "HIST.EDW_RCRD_END_DT_SK, "
        "HIST.CUST_SVC_TASK_RTRN_CHK_NO, "
        "HIST.CS_TASK_RTRN_CHK_NO_CHG_IN, "
        "HIST.CUST_SVC_TASK_RTRN_CHK_AMT, "
        "HIST.CS_TASK_RTRN_CHK_AMT_CHG_IN, "
        "HIST.CS_TASK_RTRN_FUND_DEP_DT_SK, "
        "HIST.CS_TASK_FUND_DEP_DT_CHG_IN, "
        "HIST.CS_TASK_RTRN_FUND_RCVD_DT_SK, "
        "HIST.CS_TASK_FUND_RCVD_DT_CHG_IN "
        "FROM " + EDWOwner + ".CUST_SVC_TASK_FUND_HIST_D HIST "
        "WHERE HIST.EDW_CUR_RCRD_IN='Y'"
    )
    .load()
)

# Read from IDS (db2_CD_MPPNG_Extr1)
df_db2_CD_MPPNG_Extr1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option(
        "query",
        "SELECT "
        "CD_MPPNG_SK, "
        "SRC_CD, "
        "COALESCE(TRGT_CD,'NA') TRGT_CD, "
        "COALESCE(TRGT_CD_NM,'NA') TRGT_CD_NM "
        "FROM " + IDSOwner + ".CD_MPPNG"
    )
    .load()
)

# Copy_CodesData
df_Ref_CstmDtlCdCd_Lkup = df_db2_CD_MPPNG_Extr1.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_SrcCd_Lkup = df_db2_CD_MPPNG_Extr1.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

# lkp_CDMA_Codes
df_lkp_joined = (
    df_db2_CUST_SVC_TASK_FUND_HIST_D_in.alias("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC")
    .join(
        df_Ref_SrcCd_Lkup.alias("Ref_SrcCd_Lkup"),
        F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CUST_SVC_TASK_SUBJ_CD_SK")
        == F.col("Ref_SrcCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CstmDtlCdCd_Lkup.alias("Ref_CstmDtlCdCd_Lkup"),
        F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CUST_SVC_TASK_CSTM_DTL_CD_SK")
        == F.col("Ref_CstmDtlCdCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_CUST_SVC_TASK_FUND_HIST_D.alias("Ref_CustFundHistEdwPrev_Lkup"),
        F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CUST_SVC_TASK_CSTM_DTL_SK")
        == F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_FUND_SK"),
        "left"
    )
)

df_lkp_CDMA_Codes = df_lkp_joined.select(
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_CSTM_DTL_SK"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CSTM_DTL_UNIQ_ID").alias("CSTM_DTL_UNIQ_ID"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CSTM_DTL_DT_1_SK").alias("CSTM_DTL_DT_1_SK"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CSTM_DTL_MNY_1").alias("CSTM_DTL_MNY_1"),
    F.col("Ink_IdsEdwCustSvcTaskFundHistDExtr_inABC.CSTM_DTL_NO_1").alias("CSTM_DTL_NO_1"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_FUND_SK").alias("CUST_SVC_TASK_FUND_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.SRC_SYS_CD").alias("SRC_SYS_CD_PREV"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_ID").alias("CUST_SVC_ID_PREV"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_RTRN_CHK_NO").alias("CUST_SVC_TASK_RTRN_CHK_NO"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_RTRN_CHK_NO_CHG_IN").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_RTRN_CHK_AMT").alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_RTRN_CHK_AMT_CHG_IN").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_RTRN_FUND_DEP_DT_SK").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_FUND_DEP_DT_CHG_IN").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_RTRN_FUND_RCVD_DT_SK").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CS_TASK_FUND_RCVD_DT_CHG_IN").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
    F.col("Ref_CstmDtlCdCd_Lkup.TRGT_CD").alias("TRGT_CD"),
    F.col("Ref_SrcCd_Lkup.SRC_CD").alias("SRC_CD"),  # Overwrites previous with the same name from joined if any
    F.col("Ref_CustFundHistEdwPrev_Lkup.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Ref_CustFundHistEdwPrev_Lkup.CUST_SVC_TASK_CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID")
)

# xfm_BusinessRules (CTransformerStage)
df_transform = (
    df_lkp_CDMA_Codes
    .withColumn(
        "DeptDtChk",
        F.when(
            (F.col("SRC_CD") == "RA03") | (F.col("SRC_CD") == "RA07"),
            F.col("CSTM_DTL_DT_1_SK")
        ).otherwise(F.lit("1753-01-01"))
    )
    .withColumn(
        "RcvdDtChk",
        F.when(
            (F.col("SRC_CD") == "RA05") | (F.col("SRC_CD") == "RA06") | (F.col("SRC_CD") == "RA08"),
            F.col("CSTM_DTL_DT_1_SK")
        ).otherwise(F.lit("1753-01-01"))
    )
    .withColumn(
        "ChkNo",
        F.when(
            F.col("CSTM_DTL_NO_1") != F.col("CUST_SVC_TASK_RTRN_CHK_NO"), F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "ChkAmt",
        F.when(
            F.col("CSTM_DTL_MNY_1") != F.col("CUST_SVC_TASK_RTRN_CHK_AMT"), F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "DepDtSk",
        F.when(
            F.col("DeptDtChk") != F.col("CS_TASK_RTRN_FUND_DEP_DT_SK"), F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "RcvdDtSk",
        F.when(
            F.col("RcvdDtChk") != F.col("CS_TASK_RTRN_FUND_RCVD_DT_SK"), F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "CreateNewRecord",
        F.when(
            (F.col("ChkNo") == "Y")
            | (F.col("ChkAmt") == "Y")
            | (F.col("DepDtSk") == "Y")
            | (F.col("RcvdDtSk") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCustSvcTaskNull",
        F.when(F.col("CUST_SVC_TASK_FUND_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
)

# Produce multiple outputs from the transformer logic

# df_New
df_New = (
    df_transform
    .filter(F.col("CUST_SVC_TASK_FUND_SK").isNull())
    .select(
        F.col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_FUND_SK"),
        F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.when(F.trim(F.col("TRGT_CD")) == "", F.lit("NA")).otherwise(F.col("TRGT_CD")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
        F.col("CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("Y").alias("EDW_CUR_RCRD_IN"),
        F.lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
        F.col("CSTM_DTL_NO_1").alias("CUST_SVC_TASK_RTRN_CHK_NO"),
        F.lit("N").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
        F.col("CSTM_DTL_MNY_1").alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
        F.lit("N").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
        F.col("DeptDtChk").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
        F.lit("N").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
        F.col("RcvdDtChk").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
        F.lit("N").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# df_old
df_old = (
    df_transform
    .filter(
        F.col("CUST_SVC_TASK_FUND_SK").isNotNull()
        & (F.col("CreateNewRecord") == "Y")
        & (F.col("CUST_SVC_TASK_FUND_SK") != 0)
        & (F.col("CUST_SVC_TASK_FUND_SK") != 1)
    )
    .select(
        F.when(
            F.col("CUST_SVC_TASK_FUND_SK").isNull(),
            F.col("CUST_SVC_TASK_FUND_SK")
        ).otherwise(F.col("CUST_SVC_TASK_FUND_SK")).alias("CUST_SVC_TASK_FUND_SK"),
        F.col("EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
        F.col("SRC_SYS_CD_PREV").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID_PREV").alias("CUST_SVC_ID"),
        F.col("CUST_SVC_TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.when(F.trim(F.col("TRGT_CD")) == "", F.lit("NA")).otherwise(F.col("TRGT_CD")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
        F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("N").alias("EDW_CUR_RCRD_IN"),
        F.expr("FIND.DATE.EE('" + EDWRunCycleDate + "', -1, 'D', 'X', 'CCYY-MM-DD')").alias("EDW_RCRD_END_DT_SK"),
        F.col("CUST_SVC_TASK_RTRN_CHK_NO").alias("CUST_SVC_TASK_RTRN_CHK_NO"),
        F.col("CS_TASK_RTRN_CHK_NO_CHG_IN").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
        F.col("CUST_SVC_TASK_RTRN_CHK_AMT").alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
        F.col("CS_TASK_RTRN_CHK_AMT_CHG_IN").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
        F.col("CS_TASK_RTRN_FUND_DEP_DT_SK").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
        F.col("CS_TASK_FUND_DEP_DT_CHG_IN").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
        F.col("CS_TASK_RTRN_FUND_RCVD_DT_SK").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
        F.col("CS_TASK_FUND_RCVD_DT_CHG_IN").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# df_updated
df_updated = (
    df_transform
    .filter(
        F.col("CUST_SVC_TASK_FUND_SK").isNotNull()
        & (F.col("CreateNewRecord") == "Y")
        & (F.col("CUST_SVC_TASK_FUND_SK") != 0)
        & (F.col("CUST_SVC_TASK_FUND_SK") != 1)
    )
    .select(
        F.col("CUST_SVC_TASK_CSTM_DTL_SK").alias("CUST_SVC_TASK_FUND_SK"),
        F.lit(EDWRunCycleDate).alias("EDW_RCRD_STRT_DT_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        F.when(F.trim(F.col("TRGT_CD")) == "", F.lit("NA")).otherwise(F.col("TRGT_CD")).alias("CUST_SVC_TASK_CSTM_DTL_CD"),
        F.col("CSTM_DTL_UNIQ_ID").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("Y").alias("EDW_CUR_RCRD_IN"),
        F.lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
        F.col("CSTM_DTL_NO_1").alias("CUST_SVC_TASK_RTRN_CHK_NO"),
        F.when(F.col("ChkNo") == "Y", F.lit("Y")).otherwise(F.lit("N")).alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
        F.col("CSTM_DTL_MNY_1").alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
        F.when(F.col("ChkAmt") == "Y", F.lit("Y")).otherwise(F.lit("N")).alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
        F.col("DeptDtChk").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
        F.when(F.col("DepDtSk") == "Y", F.lit("Y")).otherwise(F.lit("N")).alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
        F.col("RcvdDtChk").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
        F.when(F.col("RcvdDtSk") == "Y", F.lit("Y")).otherwise(F.lit("N")).alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# df_UNK => first row only
df_UNK_base = df_transform.limit(1)
df_UNK = df_UNK_base.select(
    F.lit(0).alias("CUST_SVC_TASK_FUND_SK"),
    F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CUST_SVC_ID"),
    F.lit(0).alias("CUST_SVC_TASK_SEQ_NO"),
    F.lit("UNK").alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.lit("1753-01-01 00:00:00.000000").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("N").alias("EDW_CUR_RCRD_IN"),
    F.lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    F.lit(0).alias("CUST_SVC_TASK_RTRN_CHK_NO"),
    F.lit("N").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
    F.lit(0).alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
    F.lit("N").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
    F.lit("1753-01-01").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
    F.lit("N").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
    F.lit("1753-01-01").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
    F.lit("N").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# df_NA => first row only
df_NA_base = df_transform.limit(1)
df_NA = df_NA_base.select(
    F.lit(1).alias("CUST_SVC_TASK_FUND_SK"),
    F.lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CUST_SVC_ID"),
    F.lit(0).alias("CUST_SVC_TASK_SEQ_NO"),
    F.lit("NA").alias("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.lit("1753-01-01 00:00:00.000000").alias("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("N").alias("EDW_CUR_RCRD_IN"),
    F.lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
    F.lit(0).alias("CUST_SVC_TASK_RTRN_CHK_NO"),
    F.lit("N").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
    F.lit(0).alias("CUST_SVC_TASK_RTRN_CHK_AMT"),
    F.lit("N").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
    F.lit("1753-01-01").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
    F.lit("N").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
    F.lit("1753-01-01").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
    F.lit("N").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Funnel (fnl_NA_UNK)
df_funnel = df_NA.unionByName(df_New, allowMissingColumns=True)\
    .unionByName(df_old, allowMissingColumns=True)\
    .unionByName(df_updated, allowMissingColumns=True)\
    .unionByName(df_UNK, allowMissingColumns=True)

# Reorder for the final select and apply rpad for char/varchar columns
final_df = df_funnel.select(
    F.col("CUST_SVC_TASK_FUND_SK"),
    F.rpad(F.col("EDW_RCRD_STRT_DT_SK"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CUST_SVC_ID"),
    F.col("CUST_SVC_TASK_SEQ_NO"),
    F.col("CUST_SVC_TASK_CSTM_DTL_CD"),
    F.col("CUST_SVC_TASK_CSTM_DTL_UNIQ_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("EDW_CUR_RCRD_IN"), 1, " ").alias("EDW_CUR_RCRD_IN"),
    F.rpad(F.col("EDW_RCRD_END_DT_SK"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
    F.col("CUST_SVC_TASK_RTRN_CHK_NO"),
    F.rpad(F.col("CS_TASK_RTRN_CHK_NO_CHG_IN"), 1, " ").alias("CS_TASK_RTRN_CHK_NO_CHG_IN"),
    F.col("CUST_SVC_TASK_RTRN_CHK_AMT"),
    F.rpad(F.col("CS_TASK_RTRN_CHK_AMT_CHG_IN"), 1, " ").alias("CS_TASK_RTRN_CHK_AMT_CHG_IN"),
    F.rpad(F.col("CS_TASK_RTRN_FUND_DEP_DT_SK"), 10, " ").alias("CS_TASK_RTRN_FUND_DEP_DT_SK"),
    F.rpad(F.col("CS_TASK_FUND_DEP_DT_CHG_IN"), 1, " ").alias("CS_TASK_FUND_DEP_DT_CHG_IN"),
    F.rpad(F.col("CS_TASK_RTRN_FUND_RCVD_DT_SK"), 10, " ").alias("CS_TASK_RTRN_FUND_RCVD_DT_SK"),
    F.rpad(F.col("CS_TASK_FUND_RCVD_DT_CHG_IN"), 1, " ").alias("CS_TASK_FUND_RCVD_DT_CHG_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# seq_CUST_SVC_TASK_FUND_HIST_D_csv_load
write_files(
    final_df,
    f"{adls_path}/load/CUST_SVC_TASK_FUND_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)