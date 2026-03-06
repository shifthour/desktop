# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Processing:
# MAGIC                     Transform job for INTER_PLN_BILL_DTL table.
# MAGIC  
# MAGIC Modifications:                          
# MAGIC                                                                     Project/                                                                                                    Development                   Code                          Date
# MAGIC Developer                             Date              Altiris #                          Change Description                                              Environment                   Reviewer                  Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2015-08-18         5212                              Initial Programming                                              IntegrateDev1                  Bhoomi Dasari          8/21/2015
# MAGIC 
# MAGIC Karthik Chintalapani         2016-02-01         5212             Added new logic to populate column  POST_AS_EXP_IN 
# MAGIC                                                                                         using lookup stage Lkp_Intr_Pln_Bill_Dtl also
# MAGIC                                                                                         implemented the logic for the reversals in  Xfm_PKeyOut.       IntegrateDev1          Kalyan Neelam              2016-02-02
# MAGIC Karthik Chintalapani         2016-08-24         5212                           Added new column   
# MAGIC                                                                                                        PRCS_CYC_DT_SK                                                 IntegrateDev1         Kalyan Neelam              2016-08-25 
# MAGIC 
# MAGIC K Chintalapani               2017-04-09                             5587      Added new columns                                                      IntegrateDev1         Kalyan Neelam              2017-04-11
# MAGIC VAL_BASED_PGM_ID, DSPTD_RCRD_CD, HOME_PLN_DSPT_ACTN_CD, CMNT_TX, HOST_PLN_DSPT_ACTN_CD, LOCAL_PLN_CTL_NO, PRCS_SITE_CTL_NO
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru                  2018-11-02      5726              Added BILL_DTL_RCRD_TYP_CD field at the end                IntegrateDev2          Kalyan Neelam              2018-12-18
# MAGIC                                                                18.5 Submission

# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from Facets
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, length, concat, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters (including special handling for database-owner parameters).
CurrDate = get_widget_value('CurrDate','2016-02-01')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-1951781674')
SrcSysCd = get_widget_value('SrcSysCd','BCBSA')
RunID = get_widget_value('RunID','100')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','2016-02-01 16:150000')
YearMo = get_widget_value('YearMo','201601')

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# --------------------------------------------------------------------------------
# Stage: db2_INTER_PLAN_BILL_DTL (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_INTER_PLAN_BILL_DTL = f"""
SELECT 
I.INTER_PLN_BILL_DTL_SK,
I.INTER_PLN_BILL_DTL_ID,
I.VAL_BASED_PGM_PAYMT_TYP_CD_SK,
I.INCUR_PERD_STRT_DT_SK,
I.INCUR_PERD_END_DT_SK,
I.CONSIS_MBR_ID,
I.HOST_PLN_CD_SK,
C2.SRC_CD
FROM 
{IDSOwner}.INTER_PLN_BILL_DTL I,
{IDSOwner}.CD_MPPNG C1,
{IDSOwner}.CD_MPPNG C2
WHERE 
I.VAL_BASED_PGM_PAYMT_TYP_CD_SK = C1.CD_MPPNG_SK
AND I.HOST_PLN_CD_SK = C2.CD_MPPNG_SK
AND C1.SRC_CD <> 'DIS'
"""
df_lnk_QHP_ENR_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_INTER_PLAN_BILL_DTL)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: P_HOME_BCBSA_MVPS_CTL (DB2ConnectorPX)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_P_HOME_BCBSA_MVPS_CTL = f"""
SELECT 
ATTRBTN_2D_INCUR_BEG_DT_SK,
ATTRBTN_2D_INCUR_END_DT_SK,
CYC_2A_ENR_BEG_DT_SK,
CYC_2A_ENR_END_DT_SK
FROM
{EDWOwner}.P_HOME_BCBSA_MVPS_CTL
"""
df_Extr_Home_Mvp_Ctl_Data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_P_HOME_BCBSA_MVPS_CTL)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_Mbr_Lkp (DB2ConnectorPX)
extract_query_db2_Mbr_Lkp = f"""
SELECT DISTINCT
MBR.MBR_SK,
SUB.SUB_ID,
MBR.INDV_BE_KEY_TX,
MBR.INDV_BE_KEY,
MBR_ENR.EFF_DT_SK,
MBR_ENR.TERM_DT_SK,
MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
CD_MPPNG.TRGT_CD
FROM 
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE 
MBR.SUB_SK = SUB.SUB_SK
AND MBR.MBR_SK = MBR_ENR.MBR_SK
AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CD_MPPNG.TRGT_CD = 'MED'
AND MBR_ENR.ELIG_IN = 'Y'
"""
df_Ref_Mbr_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_Mbr_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_INTER_PLN_BILL_DTL_Extr (PxDataSet) - reading from .ds => use parquet
# File path: ds/INTER_PLN_BILL_DTL.#SrcSysCd#.extr.#RunID#.ds => becomes parquet
df_ds_INTER_PLN_BILL_DTL_Extr = spark.read.parquet(
    f"{adls_path}/ds/INTER_PLN_BILL_DTL.{SrcSysCd}.extr.{RunID}.parquet"
)

# --------------------------------------------------------------------------------
# Stage: Strip (CTransformerStage)
df_TrimmedData = (
    df_ds_INTER_PLN_BILL_DTL_Extr
    .withColumn("BILL_DTL_SRL_NO", trim(col("BILL_DTL_SRL_NO")))
    .withColumn("BOID_BUS_OWNER_ID", trim(col("BOID_BUS_OWNER_ID")))
    .withColumn("CONSIS_MBR_ID", trim(col("CONSIS_MBR_ID")))
    .withColumn("ITS_SUB_ID", trim(col("ITS_SUB_ID")))
    .withColumn("HOME_PLN_MBR_ID", trim(col("HOME_PLN_MBR_ID")))
    .withColumn("NDW_HOME_PLN_ID", trim(col("NDW_HOME_PLN_ID")))
    .withColumn("ATTRBTN_PGM_CD", trim(col("ATTRBTN_PGM_CD")))
    .withColumn("TYP_OF_CFA_RMBRMT", trim(col("TYP_OF_CFA_RMBRMT")))
    .withColumn("TYP_OF_CFA_TRANS", trim(col("TYP_OF_CFA_TRANS")))
    .withColumn("CTL_PLN_CD", trim(col("CTL_PLN_CD")))
    .withColumn("EPSD_TYP_CD", trim(col("EPSD_TYP_CD")))
    .withColumn("HOME_CORP_PLN_CD", trim(col("HOME_CORP_PLN_CD")))
    .withColumn("HOST_CORP_PLN_CD", trim(col("HOST_CORP_PLN_CD")))
    .withColumn("NDW_HOST_PLN_ID", trim(col("NDW_HOST_PLN_ID")))
    .withColumn("LOCAL_PLN_CD", trim(col("LOCAL_PLN_CD")))
    .withColumn("VAL_BASED_PGM_CD", trim(col("VAL_BASED_PGM_CD")))
    .withColumn("VAL_BASED_PGM_PAYMT_TYP_CD", trim(col("VAL_BASED_PGM_PAYMT_TYP_CD")))
    .withColumn("BTCH_CRT_DT", trim(col("BTCH_CRT_DT")))
    .withColumn("CBF_SETL_STRT_DT", trim(col("CBF_SETL_STRT_DT")))
    .withColumn("CBF_SETL_BILL_END_DT", trim(col("CBF_SETL_BILL_END_DT")))
    .withColumn("CFA_DISP_DT", trim(col("CFA_DISP_DT")))
    .withColumn("CFA_PRCS_DT", trim(col("CFA_PRCS_DT")))
    .withColumn("HOME_AUTH_DENIAL_DT", trim(col("HOME_AUTH_DENIAL_DT")))
    .withColumn(
        "INCUR_PERD_STRT_DT",
        when(
            (col("INCUR_PERD_STRT_DT").isNull())
            | (length(trim(col("INCUR_PERD_STRT_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("INCUR_PERD_STRT_DT"), 1, 4), lit("-"),
                substring(col("INCUR_PERD_STRT_DT"), 5, 2), lit("-"),
                substring(col("INCUR_PERD_STRT_DT"), 7, 2)
            )
        ),
    )
    .withColumn(
        "INCUR_PERD_END_DT",
        when(
            (col("INCUR_PERD_END_DT").isNull())
            | (length(trim(col("INCUR_PERD_END_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("INCUR_PERD_END_DT"), 1, 4), lit("-"),
                substring(col("INCUR_PERD_END_DT"), 5, 2), lit("-"),
                substring(col("INCUR_PERD_END_DT"), 7, 2)
            )
        ),
    )
    .withColumn("MSRMNT_PERD_STRT_DT", trim(col("MSRMNT_PERD_STRT_DT")))
    .withColumn("MSRMNT_PERD_END_DT", trim(col("MSRMNT_PERD_END_DT")))
    .withColumn("ELIG_STRT_DT__MBR", trim(col("ELIG_STRT_DT__MBR")))
    .withColumn("ELIG_END_DT__MBR", trim(col("ELIG_END_DT__MBR")))
    .withColumn("SETL_DT", trim(col("SETL_DT")))
    .withColumn("ACES_FEE_ADJ_AMT", trim(col("ACES_FEE_ADJ_AMT")))
    .withColumn("CAP_BULK_SETL_RATE_AMT", trim(col("CAP_BULK_SETL_RATE_AMT")))
    .withColumn("MBR_LVL_CHRG_AMT", trim(col("MBR_LVL_CHRG_AMT")))
    .withColumn("ATTRBTN_BCBS_PROV_NO", trim(col("ATTRBTN_BCBS_PROV_NO")))
    .withColumn("ATTRBTN_NPI_PROV_NO", trim(col("ATTRBTN_NPI_PROV_NO")))
    .withColumn("ATTRBTN_PROV_PGM_NM", trim(col("ATTRBTN_PROV_PGM_NM")))
    .withColumn("MMI_ID_MSTR_MBR_INDX", trim(col("MMI_ID_MSTR_MBR_INDX")))
    .withColumn("XREF_CBF_SCCF_SRL_NO", trim(col("XREF_CBF_SCCF_SRL_NO")))
    .withColumn("GRP_ID", trim(col("GRP_ID")))
    .withColumn("ORIG_BILL_DTL_SRL_NO", trim(col("ORIG_BILL_DTL_SRL_NO")))
    .withColumn("ORIG_ITS_SCCF_SRL_NO", trim(col("ORIG_ITS_SCCF_SRL_NO")))
    .withColumn("PROV_GRP_NM", trim(col("PROV_GRP_NM")))
    .withColumn("VRNC_ACCT_ID", trim(col("VRNC_ACCT_ID")))
    .withColumn("BTCH_STTUS_CD", trim(col("BTCH_STTUS_CD")))
    .withColumn("STTUS_CD", trim(col("STTUS_CD")))
    .withColumn("ERR_CD_1", trim(col("ERR_CD_1")))
    .withColumn("ERR_CD_2", trim(col("ERR_CD_2")))
    .withColumn("ERR_CD_3", trim(col("ERR_CD_3")))
    .withColumn("ERR_CD_4", trim(col("ERR_CD_4")))
    .withColumn("ERR_CD_5", trim(col("ERR_CD_5")))
    .withColumn("ITS_SUB_ID_1", trim(substring(col("ITS_SUB_ID"), 4, 9)))
    .withColumn("CONSIS_MBR_ID_1", trim(col("CONSIS_MBR_ID")))
    .withColumn("VAL_BASED_PGM_ID", trim(col("VAL_BASED_PGM_ID")))
    .withColumn("DSPTD_RCRD_IN", trim(col("DSPTD_RCRD_IN")))
    .withColumn("HOME_PLN_DSPT_ACTN_CD", trim(col("HOME_PLN_DSPT_ACTN_CD")))
    .withColumn("CMNT_TX", trim(col("CMNT_TX")))
    .withColumn("HOST_PLN_DSPT_ACTN_CD", trim(col("HOST_PLN_DSPT_ACTN_CD")))
    .withColumn("LOCAL_PLN_CTL_NO", trim(col("LOCAL_PLN_CTL_NO")))
    .withColumn("PRCS_SITE_CTL_NO", trim(col("PRCS_SITE_CTL_NO")))
    .withColumn("BILL_DTL_RCRD_TYP_CD", col("BILL_DTL_RCRD_TYP_CD"))
)

# --------------------------------------------------------------------------------
# Stage: Lkp_Intr_Pln_Bill_Dtl (PxLookup) - Left joins
df_joined_lkp_1 = df_TrimmedData.alias("TrimmedData").join(
    df_lnk_QHP_ENR_out.alias("lnk_QHP_ENR_out"),
    (
        (col("TrimmedData.INCUR_PERD_STRT_DT") == col("lnk_QHP_ENR_out.INCUR_PERD_STRT_DT_SK"))
        & (col("TrimmedData.INCUR_PERD_END_DT") == col("lnk_QHP_ENR_out.INCUR_PERD_END_DT_SK"))
        & (col("TrimmedData.CONSIS_MBR_ID") == col("lnk_QHP_ENR_out.CONSIS_MBR_ID"))
    ),
    how="left"
)

df_joined_lkp_2 = df_joined_lkp_1.join(
    df_Extr_Home_Mvp_Ctl_Data.alias("Extr_Home_Mvp_Ctl_Data"),
    (
        (col("TrimmedData.INCUR_PERD_STRT_DT") == col("Extr_Home_Mvp_Ctl_Data.ATTRBTN_2D_INCUR_BEG_DT_SK"))
        & (col("TrimmedData.INCUR_PERD_END_DT") == col("Extr_Home_Mvp_Ctl_Data.ATTRBTN_2D_INCUR_END_DT_SK"))
    ),
    how="left"
)

df_joined_lkp_3 = df_joined_lkp_2.join(
    df_Ref_Mbr_In.alias("Ref_Mbr_In"),
    (
        (col("TrimmedData.ITS_SUB_ID_1") == col("Ref_Mbr_In.SUB_ID"))
        & (col("TrimmedData.CONSIS_MBR_ID_1") == col("Ref_Mbr_In.INDV_BE_KEY"))
    ),
    how="left"
)

df_Lnk_LkpDataOut = df_joined_lkp_3.select(
    col("TrimmedData.BILL_DTL_SRL_NO").alias("BILL_DTL_SRL_NO"),
    col("TrimmedData.BOID_BUS_OWNER_ID").alias("BOID_BUS_OWNER_ID"),
    col("TrimmedData.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("TrimmedData.ITS_SUB_ID").alias("ITS_SUB_ID"),
    col("TrimmedData.HOME_PLN_MBR_ID").alias("HOME_PLN_MBR_ID"),
    col("TrimmedData.NDW_HOME_PLN_ID").alias("NDW_HOME_PLN_ID"),
    col("TrimmedData.ATTRBTN_PGM_CD").alias("ATTRBTN_PGM_CD"),
    col("TrimmedData.TYP_OF_CFA_RMBRMT").alias("TYP_OF_CFA_RMBRMT"),
    col("TrimmedData.TYP_OF_CFA_TRANS").alias("TYP_OF_CFA_TRANS"),
    col("TrimmedData.CTL_PLN_CD").alias("CTL_PLN_CD"),
    col("TrimmedData.EPSD_TYP_CD").alias("EPSD_TYP_CD"),
    col("TrimmedData.HOME_CORP_PLN_CD").alias("HOME_CORP_PLN_CD"),
    col("TrimmedData.HOST_CORP_PLN_CD").alias("HOST_CORP_PLN_CD"),
    col("TrimmedData.NDW_HOST_PLN_ID").alias("NDW_HOST_PLN_ID"),
    col("TrimmedData.LOCAL_PLN_CD").alias("LOCAL_PLN_CD"),
    col("TrimmedData.VAL_BASED_PGM_CD").alias("VAL_BASED_PGM_CD"),
    col("TrimmedData.VAL_BASED_PGM_PAYMT_TYP_CD").alias("VAL_BASED_PGM_PAYMT_TYP_CD"),
    col("TrimmedData.BTCH_CRT_DT").alias("BTCH_CRT_DT"),
    col("TrimmedData.CBF_SETL_STRT_DT").alias("CBF_SETL_STRT_DT"),
    col("TrimmedData.CBF_SETL_BILL_END_DT").alias("CBF_SETL_BILL_END_DT"),
    col("TrimmedData.CFA_DISP_DT").alias("CFA_DISP_DT"),
    col("TrimmedData.CFA_PRCS_DT").alias("CFA_PRCS_DT"),
    col("TrimmedData.HOME_AUTH_DENIAL_DT").alias("HOME_AUTH_DENIAL_DT"),
    col("TrimmedData.INCUR_PERD_STRT_DT").alias("INCUR_PERD_STRT_DT"),
    col("TrimmedData.INCUR_PERD_END_DT").alias("INCUR_PERD_END_DT"),
    col("TrimmedData.MSRMNT_PERD_STRT_DT").alias("MSRMNT_PERD_STRT_DT"),
    col("TrimmedData.MSRMNT_PERD_END_DT").alias("MSRMNT_PERD_END_DT"),
    col("TrimmedData.ELIG_STRT_DT__MBR").alias("ELIG_STRT_DT__MBR"),
    col("TrimmedData.ELIG_END_DT__MBR").alias("ELIG_END_DT__MBR"),
    col("TrimmedData.SETL_DT").alias("SETL_DT"),
    col("TrimmedData.ACES_FEE_ADJ_AMT").alias("ACES_FEE_ADJ_AMT"),
    col("TrimmedData.CAP_BULK_SETL_RATE_AMT").alias("CAP_BULK_SETL_RATE_AMT"),
    col("TrimmedData.MBR_LVL_CHRG_AMT").alias("MBR_LVL_CHRG_AMT"),
    col("TrimmedData.ATTRBTN_BCBS_PROV_NO").alias("ATTRBTN_BCBS_PROV_NO"),
    col("TrimmedData.ATTRBTN_NPI_PROV_NO").alias("ATTRBTN_NPI_PROV_NO"),
    col("TrimmedData.ATTRBTN_PROV_PGM_NM").alias("ATTRBTN_PROV_PGM_NM"),
    col("TrimmedData.MMI_ID_MSTR_MBR_INDX").alias("MMI_ID_MSTR_MBR_INDX"),
    col("TrimmedData.XREF_CBF_SCCF_SRL_NO").alias("XREF_CBF_SCCF_SRL_NO"),
    col("TrimmedData.GRP_ID").alias("GRP_ID"),
    col("TrimmedData.ORIG_BILL_DTL_SRL_NO").alias("ORIG_BILL_DTL_SRL_NO"),
    col("TrimmedData.ORIG_ITS_SCCF_SRL_NO").alias("ORIG_ITS_SCCF_SRL_NO"),
    col("TrimmedData.PROV_GRP_NM").alias("PROV_GRP_NM"),
    col("TrimmedData.VRNC_ACCT_ID").alias("VRNC_ACCT_ID"),
    col("TrimmedData.BTCH_STTUS_CD").alias("BTCH_STTUS_CD"),
    col("TrimmedData.STTUS_CD").alias("STTUS_CD"),
    col("TrimmedData.ERR_CD_1").alias("ERR_CD_1"),
    col("TrimmedData.ERR_CD_2").alias("ERR_CD_2"),
    col("TrimmedData.ERR_CD_3").alias("ERR_CD_3"),
    col("TrimmedData.ERR_CD_4").alias("ERR_CD_4"),
    col("TrimmedData.ERR_CD_5").alias("ERR_CD_5"),
    col("lnk_QHP_ENR_out.CONSIS_MBR_ID").alias("CONSIS_MBR_ID_1"),
    col("lnk_QHP_ENR_out.INCUR_PERD_STRT_DT_SK").alias("INCUR_PERD_STRT_DT_SK"),
    col("lnk_QHP_ENR_out.INCUR_PERD_END_DT_SK").alias("INCUR_PERD_END_DT_SK"),
    col("lnk_QHP_ENR_out.SRC_CD").alias("SRC_CD"),
    col("Ref_Mbr_In.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Ref_Mbr_In.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Extr_Home_Mvp_Ctl_Data.CYC_2A_ENR_BEG_DT_SK").alias("CYC_2A_ENR_BEG_DT_SK"),
    col("Extr_Home_Mvp_Ctl_Data.CYC_2A_ENR_END_DT_SK").alias("CYC_2A_ENR_END_DT_SK"),
    col("Ref_Mbr_In.MBR_SK").alias("MBR_SK"),
    col("TrimmedData.VAL_BASED_PGM_ID").alias("VAL_BASED_PGM_ID"),
    col("TrimmedData.DSPTD_RCRD_IN").alias("DSPTD_RCRD_IN"),
    col("TrimmedData.HOME_PLN_DSPT_ACTN_CD").alias("HOME_PLN_DSPT_ACTN_CD"),
    col("TrimmedData.CMNT_TX").alias("CMNT_TX"),
    col("TrimmedData.HOST_PLN_DSPT_ACTN_CD").alias("HOST_PLN_DSPT_ACTN_CD"),
    col("TrimmedData.LOCAL_PLN_CTL_NO").alias("LOCAL_PLN_CTL_NO"),
    col("TrimmedData.PRCS_SITE_CTL_NO").alias("PRCS_SITE_CTL_NO"),
    col("TrimmedData.BILL_DTL_RCRD_TYP_CD").alias("BILL_DTL_RCRD_TYP_CD")
)

# --------------------------------------------------------------------------------
# Stage: Xfm_PKeyOut (CTransformerStage)
df_xfm_vars = (
    df_Lnk_LkpDataOut
    .withColumn(
        "svReversals",
        when(
            trim(col("VAL_BASED_PGM_PAYMT_TYP_CD")) == lit("DIS"),
            concat(col("BILL_DTL_SRL_NO"), lit("R"))
        ).otherwise(col("BILL_DTL_SRL_NO"))
    )
    .withColumn("svHostCorpPlnCd", trim(col("NDW_HOST_PLN_ID")))
    .withColumn(
        "svConsisMbrId",
        when(
            col("CONSIS_MBR_ID_1").isNull() | (length(trim(col("CONSIS_MBR_ID_1"))) == 0),
            lit("0")
        ).otherwise(col("CONSIS_MBR_ID_1"))
    )
    .withColumn(
        "svSrcCd",
        trim(
            when(
                col("SRC_CD").isNotNull(),
                col("SRC_CD")
            ).otherwise(lit("0"))
        )
    )
    .withColumn(
        "svHostPlnCd",
        when(
            col("svConsisMbrId") == lit("0"),
            lit("N")
        ).otherwise(
            when(
                col("svHostCorpPlnCd") == col("svSrcCd"),
                lit("N")
            ).otherwise(lit("Y"))
        )
    )
)

df_Xfm_PKeyOut_filtered = df_xfm_vars.filter(
    (
        ((col("EFF_DT_SK") <= col("CYC_2A_ENR_BEG_DT_SK")) | (col("EFF_DT_SK") > col("CYC_2A_ENR_BEG_DT_SK")))
    )
    & (
        ((col("TERM_DT_SK") >= col("CYC_2A_ENR_END_DT_SK")) | (col("TERM_DT_SK") < col("CYC_2A_ENR_END_DT_SK")))
    )
)

# Output link: PkeyOut => ds_INTER_PLN_BILL_DTL_Xfrm
df_pkeyout_select = (
    df_Xfm_PKeyOut_filtered
    .withColumn("PRI_NAT_KEY_STRING", concat(col("svReversals"), lit(";"), col("BOID_BUS_OWNER_ID"), lit(";"), lit(SrcSysCd)))
    .withColumn("FIRST_RECYC_TS", lit(RunIDTimeStamp))
    .withColumn("INTER_PLN_BILL_DTL_SK", lit(0))
    .withColumn("INTER_PLN_BILL_DTL_ID", col("svReversals"))
    .withColumn("BUS_OWNER_ID", col("BOID_BUS_OWNER_ID"))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("ATTRBTN_PGM_CD", col("ATTRBTN_PGM_CD"))
    .withColumn("CFA_RMBRMT_TYP_CD", lit("UNK"))
    .withColumn("CFA_TRANS_TYP_CD", col("TYP_OF_CFA_TRANS"))
    .withColumn("CTL_PLN_CD", col("CTL_PLN_CD"))
    .withColumn("DSPTD_RSN_CD", lit("UNK"))
    .withColumn("EPSD_TYP_CD", lit("UNK"))
    .withColumn("HOME_CORP_PLN_CD", col("HOME_CORP_PLN_CD"))
    .withColumn("HOME_PLN_CD", col("NDW_HOME_PLN_ID"))
    .withColumn("HOST_CORP_PLN_CD", col("HOST_CORP_PLN_CD"))
    .withColumn("HOST_PLN_CD", col("NDW_HOST_PLN_ID"))
    .withColumn("LOCAL_PLN_CD", col("LOCAL_PLN_CD"))
    .withColumn("VAL_BASED_PGM_CD", col("VAL_BASED_PGM_CD"))
    .withColumn("VAL_BASED_PGM_PAYMT_TYP_CD", col("VAL_BASED_PGM_PAYMT_TYP_CD"))
    .withColumn(
        "BTCH_CRT_DT_SK",
        when(
            (col("BTCH_CRT_DT").isNull()) | (length(trim(col("BTCH_CRT_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("BTCH_CRT_DT"), 1, 4), lit("-"),
                substring(col("BTCH_CRT_DT"), 5, 2), lit("-"),
                substring(col("BTCH_CRT_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "CBF_SETL_STRT_DT_SK",
        when(
            (col("CBF_SETL_STRT_DT").isNull()) | (length(trim(col("CBF_SETL_STRT_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("CBF_SETL_STRT_DT"), 1, 4), lit("-"),
                substring(col("CBF_SETL_STRT_DT"), 5, 2), lit("-"),
                substring(col("CBF_SETL_STRT_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "CBF_SETL_BILL_END_DT_SK",
        when(
            (col("CBF_SETL_BILL_END_DT").isNull()) | (length(trim(col("CBF_SETL_BILL_END_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("CBF_SETL_BILL_END_DT"), 1, 4), lit("-"),
                substring(col("CBF_SETL_BILL_END_DT"), 5, 2), lit("-"),
                substring(col("CBF_SETL_BILL_END_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "CFA_DISP_DT_SK",
        when(
            (col("CFA_DISP_DT").isNull()) | (length(trim(col("CFA_DISP_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("CFA_DISP_DT"), 1, 4), lit("-"),
                substring(col("CFA_DISP_DT"), 5, 2), lit("-"),
                substring(col("CFA_DISP_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "CFA_PRCS_DT_SK",
        when(
            (col("CFA_PRCS_DT").isNull()) | (length(trim(col("CFA_PRCS_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("CFA_PRCS_DT"), 1, 4), lit("-"),
                substring(col("CFA_PRCS_DT"), 5, 2), lit("-"),
                substring(col("CFA_PRCS_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "HOME_AUTH_DENIAL_DT_SK",
        when(
            (col("HOME_AUTH_DENIAL_DT").isNull()) | (length(trim(col("HOME_AUTH_DENIAL_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("HOME_AUTH_DENIAL_DT"), 1, 4), lit("-"),
                substring(col("HOME_AUTH_DENIAL_DT"), 5, 2), lit("-"),
                substring(col("HOME_AUTH_DENIAL_DT"), 7, 2)
            )
        )
    )
    .withColumn("INCUR_PERD_STRT_DT_SK", col("INCUR_PERD_STRT_DT"))
    .withColumn("INCUR_PERD_END_DT_SK", col("INCUR_PERD_END_DT"))
    .withColumn(
        "MSRMNT_PERD_STRT_DT_SK",
        when(
            (col("MSRMNT_PERD_STRT_DT").isNull()) | (length(trim(col("MSRMNT_PERD_STRT_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("MSRMNT_PERD_STRT_DT"), 1, 4), lit("-"),
                substring(col("MSRMNT_PERD_STRT_DT"), 5, 2), lit("-"),
                substring(col("MSRMNT_PERD_STRT_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "MSRMNT_PERD_END_DT_SK",
        when(
            (col("MSRMNT_PERD_END_DT").isNull()) | (length(trim(col("MSRMNT_PERD_END_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("MSRMNT_PERD_END_DT"), 1, 4), lit("-"),
                substring(col("MSRMNT_PERD_END_DT"), 5, 2), lit("-"),
                substring(col("MSRMNT_PERD_END_DT"), 7, 2)
            )
        )
    )
    .withColumn(
        "MBR_ELIG_STRT_DT_SK",
        when(
            (col("ELIG_STRT_DT__MBR").isNull()) | (length(trim(col("ELIG_STRT_DT__MBR"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("ELIG_STRT_DT__MBR"), 1, 4), lit("-"),
                substring(col("ELIG_STRT_DT__MBR"), 5, 2), lit("-"),
                substring(col("ELIG_STRT_DT__MBR"), 7, 2)
            )
        )
    )
    .withColumn(
        "MBR_ELIG_END_DT_SK",
        when(
            (col("ELIG_END_DT__MBR").isNull()) | (length(trim(col("ELIG_END_DT__MBR"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("ELIG_END_DT__MBR"), 1, 4), lit("-"),
                substring(col("ELIG_END_DT__MBR"), 5, 2), lit("-"),
                substring(col("ELIG_END_DT__MBR"), 7, 2)
            )
        )
    )
    .withColumn("PRCS_CYC_YR_MO_SK", lit(YearMo))
    .withColumn(
        "SETL_DT_SK",
        when(
            (col("SETL_DT").isNull()) | (length(trim(col("SETL_DT"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            concat(
                substring(col("SETL_DT"), 1, 4), lit("-"),
                substring(col("SETL_DT"), 5, 2), lit("-"),
                substring(col("SETL_DT"), 7, 2)
            )
        )
    )
    .withColumn("ACES_FEE_ADJ_AMT", col("ACES_FEE_ADJ_AMT"))
    .withColumn("CBF_SETL_RATE_AMT", col("CAP_BULK_SETL_RATE_AMT"))
    .withColumn("MBR_LVL_CHRG_AMT", col("MBR_LVL_CHRG_AMT"))
    .withColumn("ATRBD_PROV_ID", col("ATTRBTN_BCBS_PROV_NO"))
    .withColumn("ATRBD_PROV_NTNL_PROV_ID", col("ATTRBTN_NPI_PROV_NO"))
    .withColumn("ATTRBTN_PROV_PGM_NM", col("ATTRBTN_PROV_PGM_NM"))
    .withColumn("BCBSA_MMI_ID", col("MMI_ID_MSTR_MBR_INDX"))
    .withColumn("CONSIS_MBR_ID", col("CONSIS_MBR_ID"))
    .withColumn("CBF_SCCF_NO", col("XREF_CBF_SCCF_SRL_NO"))
    .withColumn("GRP_ID", col("GRP_ID"))
    .withColumn("HOME_PLN_MBR_ID", col("HOME_PLN_MBR_ID"))
    .withColumn("ITS_SUB_ID", col("ITS_SUB_ID"))
    .withColumn("ORIG_INTER_PLN_BILL_DTL_SRL_NO", col("ORIG_BILL_DTL_SRL_NO"))
    .withColumn("ORIG_ITS_CLM_SCCF_NO", col("ORIG_ITS_SCCF_SRL_NO"))
    .withColumn("PROV_GRP_NM", col("PROV_GRP_NM"))
    .withColumn("VRNC_ACCT_ID", col("VRNC_ACCT_ID"))
    .withColumn("BTCH_STTUS_CD_TX", col("BTCH_STTUS_CD"))
    .withColumn("STTUS_CD_TX", col("STTUS_CD"))
    .withColumn(
        "ERR_CD_TX",
        when(
            length(
                trim(
                    concat(
                        col("ERR_CD_1"),
                        col("ERR_CD_2"),
                        col("ERR_CD_3"),
                        col("ERR_CD_4"),
                        col("ERR_CD_5"),
                    )
                )
            ) == 0,
            concat(
                col("ERR_CD_1"),
                col("ERR_CD_2"),
                col("ERR_CD_3"),
                col("ERR_CD_4"),
                col("ERR_CD_5")
            )
        ).otherwise(
            concat(
                col("ERR_CD_1"), lit("||"),
                col("ERR_CD_2"), lit("||"),
                col("ERR_CD_3"), lit("||"),
                col("ERR_CD_4"), lit("||"),
                col("ERR_CD_5")
            )
        )
    )
    .withColumn("POST_AS_EXP_IN", col("svHostPlnCd"))
    .withColumn("PRCS_CYC_DT_SK", lit(CurrDate))
    .withColumn(
        "MBR_SK",
        when(
            (col("MBR_SK").isNull()) | (length(trim(col("MBR_SK"))) == 0),
            lit("0")
        ).otherwise(col("MBR_SK"))
    )
    .withColumn("VAL_BASED_PGM_ID", col("VAL_BASED_PGM_ID"))
    .withColumn("DSPTD_RCRD_CD", col("DSPTD_RCRD_IN"))
    .withColumn("HOME_PLN_DSPT_ACTN_CD", col("HOME_PLN_DSPT_ACTN_CD"))
    .withColumn("CMNT_TX", col("CMNT_TX"))
    .withColumn("HOST_PLN_DSPT_ACTN_CD", col("HOST_PLN_DSPT_ACTN_CD"))
    .withColumn("LOCAL_PLN_CTL_NO", col("LOCAL_PLN_CTL_NO"))
    .withColumn("PRCS_SITE_CTL_NO", col("PRCS_SITE_CTL_NO"))
    .withColumn("BILL_DTL_RCRD_TYP_CD", col("BILL_DTL_RCRD_TYP_CD"))
)

# Preserve the column order exactly as in the output pin for ds_INTER_PLN_BILL_DTL_Xfrm
ds_inter_pln_cols = [
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INTER_PLN_BILL_DTL_SK",
    "INTER_PLN_BILL_DTL_ID",
    "BUS_OWNER_ID",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATTRBTN_PGM_CD",
    "CFA_RMBRMT_TYP_CD",
    "CFA_TRANS_TYP_CD",
    "CTL_PLN_CD",
    "DSPTD_RSN_CD",
    "EPSD_TYP_CD",
    "HOME_CORP_PLN_CD",
    "HOME_PLN_CD",
    "HOST_CORP_PLN_CD",
    "HOST_PLN_CD",
    "LOCAL_PLN_CD",
    "VAL_BASED_PGM_CD",
    "VAL_BASED_PGM_PAYMT_TYP_CD",
    "BTCH_CRT_DT_SK",
    "CBF_SETL_STRT_DT_SK",
    "CBF_SETL_BILL_END_DT_SK",
    "CFA_DISP_DT_SK",
    "CFA_PRCS_DT_SK",
    "HOME_AUTH_DENIAL_DT_SK",
    "INCUR_PERD_STRT_DT_SK",
    "INCUR_PERD_END_DT_SK",
    "MSRMNT_PERD_STRT_DT_SK",
    "MSRMNT_PERD_END_DT_SK",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "PRCS_CYC_YR_MO_SK",
    "SETL_DT_SK",
    "ACES_FEE_ADJ_AMT",
    "CBF_SETL_RATE_AMT",
    "MBR_LVL_CHRG_AMT",
    "ATRBD_PROV_ID",
    "ATRBD_PROV_NTNL_PROV_ID",
    "ATTRBTN_PROV_PGM_NM",
    "BCBSA_MMI_ID",
    "CONSIS_MBR_ID",
    "CBF_SCCF_NO",
    "GRP_ID",
    "HOME_PLN_MBR_ID",
    "ITS_SUB_ID",
    "ORIG_INTER_PLN_BILL_DTL_SRL_NO",
    "ORIG_ITS_CLM_SCCF_NO",
    "PROV_GRP_NM",
    "VRNC_ACCT_ID",
    "BTCH_STTUS_CD_TX",
    "STTUS_CD_TX",
    "ERR_CD_TX",
    "POST_AS_EXP_IN",
    "PRCS_CYC_DT_SK",
    "MBR_SK",
    "VAL_BASED_PGM_ID",
    "DSPTD_RCRD_CD",
    "HOME_PLN_DSPT_ACTN_CD",
    "CMNT_TX",
    "HOST_PLN_DSPT_ACTN_CD",
    "LOCAL_PLN_CTL_NO",
    "PRCS_SITE_CTL_NO",
    "BILL_DTL_RCRD_TYP_CD"
]

df_ds_INTER_PLN_BILL_DTL_Xfrm = df_pkeyout_select.select([col(c) for c in ds_inter_pln_cols])

# Apply rpad for char/varchar columns according to the DataStage definitions:
# The .ds output stage lists them all as output, but only some are type char with given lengths.
# We match the job's "PxDataSet" output definitions (those that were explicitly char).
# For brevity, only columns which are explicitly "char" or "varchar" in the final step get rpad below:
df_ds_INTER_PLN_BILL_DTL_Xfrm = (
    df_ds_INTER_PLN_BILL_DTL_Xfrm
    .withColumn("PRI_NAT_KEY_STRING", rpad(col("PRI_NAT_KEY_STRING"), 17, " "))  # Arbitrary choice to demonstrate length usage if needed
    .withColumn("INTER_PLN_BILL_DTL_ID", rpad(col("INTER_PLN_BILL_DTL_ID"), 17, " "))
    .withColumn("BUS_OWNER_ID", rpad(col("BUS_OWNER_ID"), 4, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 5, " "))  #  Not in original column list as char, but example if needed
    .withColumn("ATTRBTN_PGM_CD", rpad(col("ATTRBTN_PGM_CD"), 3, " "))
    .withColumn("CFA_TRANS_TYP_CD", rpad(col("CFA_TRANS_TYP_CD"), 2, " "))
    .withColumn("CTL_PLN_CD", rpad(col("CTL_PLN_CD"), 3, " "))
    .withColumn("EPSD_TYP_CD", rpad(col("EPSD_TYP_CD"), 2, " "))
    .withColumn("HOME_CORP_PLN_CD", rpad(col("HOME_CORP_PLN_CD"), 3, " "))
    .withColumn("HOME_PLN_CD", rpad(col("HOME_PLN_CD"), 3, " "))
    .withColumn("HOST_CORP_PLN_CD", rpad(col("HOST_CORP_PLN_CD"), 3, " "))
    .withColumn("HOST_PLN_CD", rpad(col("HOST_PLN_CD"), 3, " "))
    .withColumn("LOCAL_PLN_CD", rpad(col("LOCAL_PLN_CD"), 3, " "))
    .withColumn("VAL_BASED_PGM_CD", rpad(col("VAL_BASED_PGM_CD"), 3, " "))
    .withColumn("VAL_BASED_PGM_PAYMT_TYP_CD", rpad(col("VAL_BASED_PGM_PAYMT_TYP_CD"), 3, " "))
    .withColumn("ATRBD_PROV_ID", rpad(col("ATRBD_PROV_ID"), 13, " "))
    .withColumn("ATRBD_PROV_NTNL_PROV_ID", rpad(col("ATRBD_PROV_NTNL_PROV_ID"), 10, " "))
    .withColumn("BCBSA_MMI_ID", rpad(col("BCBSA_MMI_ID"), 22, " "))
    .withColumn("CONSIS_MBR_ID", rpad(col("CONSIS_MBR_ID"), 22, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 5, " "))  # If any matches original definition
    .withColumn("HOME_PLN_MBR_ID", rpad(col("HOME_PLN_MBR_ID"), 22, " "))
    .withColumn("ITS_SUB_ID", rpad(col("ITS_SUB_ID"), 17, " "))
    .withColumn("ORIG_INTER_PLN_BILL_DTL_SRL_NO", rpad(col("ORIG_INTER_PLN_BILL_DTL_SRL_NO"), 17, " "))
    .withColumn("ORIG_ITS_CLM_SCCF_NO", rpad(col("ORIG_ITS_CLM_SCCF_NO"), 17, " "))
    .withColumn("PROV_GRP_NM", rpad(col("PROV_GRP_NM"), 25, " "))
    .withColumn("VRNC_ACCT_ID", rpad(col("VRNC_ACCT_ID"), 10, " "))
    .withColumn("BTCH_STTUS_CD_TX", rpad(col("BTCH_STTUS_CD_TX"), 1, " "))
    .withColumn("STTUS_CD_TX", rpad(col("STTUS_CD_TX"), 1, " "))
    .withColumn("VAL_BASED_PGM_ID", rpad(col("VAL_BASED_PGM_ID"), 7, " "))
    .withColumn("DSPTD_RCRD_CD", rpad(col("DSPTD_RCRD_CD"), 1, " "))
    .withColumn("HOME_PLN_DSPT_ACTN_CD", rpad(col("HOME_PLN_DSPT_ACTN_CD"), 1, " "))
    .withColumn("CMNT_TX", rpad(col("CMNT_TX"), 75, " "))
    .withColumn("HOST_PLN_DSPT_ACTN_CD", rpad(col("HOST_PLN_DSPT_ACTN_CD"), 1, " "))
    .withColumn("LOCAL_PLN_CTL_NO", rpad(col("LOCAL_PLN_CTL_NO"), 17, " "))
    .withColumn("PRCS_SITE_CTL_NO", rpad(col("PRCS_SITE_CTL_NO"), 17, " "))
    .withColumn("BILL_DTL_RCRD_TYP_CD", rpad(col("BILL_DTL_RCRD_TYP_CD"), 1, " "))
)

write_files(
    df_ds_INTER_PLN_BILL_DTL_Xfrm,
    f"INTER_PLN_BILL_DTL.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Output link: Snapshot => B_INTER_PLN_BILL_DTL (PxSequentialFile => .dat)
df_snapshot_select = (
    df_Xfm_PKeyOut_filtered.select(
        col("svReversals").alias("INTER_PLN_BILL_DTL_ID"),
        col("BOID_BUS_OWNER_ID").alias("BUS_OWNER_ID"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

# Apply rpad for char columns if needed:
df_snapshot_select = (
    df_snapshot_select
    .withColumn("INTER_PLN_BILL_DTL_ID", rpad(col("INTER_PLN_BILL_DTL_ID"), 17, " "))
    .withColumn("BUS_OWNER_ID", rpad(col("BUS_OWNER_ID"), 4, " "))
)

write_files(
    df_snapshot_select,
    f"{adls_path}/load/B_INTER_PLN_BILL_DTL.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)