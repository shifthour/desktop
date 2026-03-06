# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:  Extracting data from PRVCY_DSCLSUR_ACCTG
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                3/26/2007           CDS Sunset/3279           Originally Programmed                     devlEDW10                  Steph Goddard            3/29/07
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               12/16/2013        5114                              Create Load File for EDW Table PRVCY_DSCLSUR_ACCTG_F        EnterpriseWhseDevl  Peter Marshall               1/7/2014

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwPrvcyDsclsurAcctgFExtr
# MAGIC Read from source table PRVCY_DSCLSUR_ACCTG
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC PRVCY_DSCLSUR_ACCTG_RQST_CD_SK,
# MAGIC PRVCY_DSCLS_ACCT_STS_RSN_CD_SK,
# MAGIC PRVCY_MBR_SRC_CD_SK,
# MAGIC Write PRVCY_DSCLSUR_ACCTG_F Data into a Sequential file for Load Job IdsEdwPrvcyDsclsurAcctgFLoad
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Obtain JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ----------------------------------------------------------------------------
# Stage: db2_PRVCY_DSCLSUR_ACCTG_in (DB2ConnectorPX)
# ----------------------------------------------------------------------------
df_db2_PRVCY_DSCLSUR_ACCTG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT 
PRVCY.PRVCY_DSCLSUR_ACCTG_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
PRVCY.PRVCY_MBR_UNIQ_KEY,
PRVCY.RQST_NO,
PRVCY.MBR_SK,
PRVCY.PRVCY_DSCLSUR_ACCTG_RQST_CD_SK,
PRVCY.PRVCY_DSCLS_ACCT_STS_RSN_CD_SK,
PRVCY.PRVCY_MBR_SRC_CD_SK,
PRVCY.PRVCY_EXTRNL_MBR_SK,
PRVCY.CRT_DT_SK,
PRVCY.FROM_DT_SK,
PRVCY.RCVD_DT_SK,
PRVCY.RQST_DT_SK,
PRVCY.STTUS_CD_TX,
PRVCY.TO_DT_SK,
PRVCY.RQST_DESC,
PRVCY.STTUS_DT_SK,
PRVCY.SRC_SYS_LAST_UPDT_DT_SK,
PRVCY.SRC_SYS_LAST_UPDT_USER_SK
FROM #$IDSOwner#.PRVCY_DSCLSUR_ACCTG PRVCY
LEFT JOIN #$IDSOwner#.CD_MPPNG CD
ON PRVCY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"""
    )
    .load()
)

# Alias for the primary link
df_db2_PRVCY_DSCLSUR_ACCTG_in = df_db2_PRVCY_DSCLSUR_ACCTG_in.alias("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc")

# ----------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_Extr (DB2ConnectorPX)
# ----------------------------------------------------------------------------
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM #$IDSOwner#.CD_MPPNG"""
    )
    .load()
)

# ----------------------------------------------------------------------------
# Stage: cpy (PxCopy)
# ----------------------------------------------------------------------------
# This copy stage produces three outputs referencing the same input columns.
df_cpy_Ref_PrvcyDsclsurAcctgRqstCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
).alias("Ref_PrvcyDsclsurAcctgRqstCdLkup")

df_cpy_Ref_PrvcyMbrSrcCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
).alias("Ref_PrvcyMbrSrcCdLkup")

df_cpy_Ref_PrvcyDsclsurAcctStsRsnCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
).alias("Ref_PrvcyDsclsurAcctStsRsnCdLkup")

# ----------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# ----------------------------------------------------------------------------
df_lkp_Codes = (
    df_db2_PRVCY_DSCLSUR_ACCTG_in
    .join(
        df_cpy_Ref_PrvcyDsclsurAcctStsRsnCdLkup,
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_DSCLS_ACCT_STS_RSN_CD_SK")
        == F.col("Ref_PrvcyDsclsurAcctStsRsnCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_Ref_PrvcyDsclsurAcctgRqstCdLkup,
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_DSCLSUR_ACCTG_RQST_CD_SK")
        == F.col("Ref_PrvcyDsclsurAcctgRqstCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_Ref_PrvcyMbrSrcCdLkup,
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_MBR_SRC_CD_SK")
        == F.col("Ref_PrvcyMbrSrcCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_DSCLSUR_ACCTG_SK").alias("PRVCY_DSCLSUR_ACCTG_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.RQST_NO").alias("RQST_NO"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("Ref_PrvcyDsclsurAcctgRqstCdLkup.TRGT_CD").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD"),
        F.col("Ref_PrvcyDsclsurAcctgRqstCdLkup.TRGT_CD_NM").alias("PRVCY_DSCLSUR_ACCTG_RQST_NM"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.RQST_DT_SK").alias("RQST_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.RQST_DESC").alias("RQST_DESC"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.STTUS_CD_TX").alias("STTUS_CD_TX"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.STTUS_DT_SK").alias("STTUS_DT_SK"),
        F.col("Ref_PrvcyDsclsurAcctStsRsnCdLkup.TRGT_CD").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD"),
        F.col("Ref_PrvcyDsclsurAcctStsRsnCdLkup.TRGT_CD_NM").alias("PRVCY_DSCLS_ACCT_STS_RSN_NM"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.TO_DT_SK").alias("TO_DT_SK"),
        F.col("Ref_PrvcyMbrSrcCdLkup.TRGT_CD").alias("PRVCY_MBR_SRC_CD"),
        F.col("Ref_PrvcyMbrSrcCdLkup.TRGT_CD_NM").alias("PRVCY_MBR_SRC_NM"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_DSCLSUR_ACCTG_RQST_CD_SK").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_DSCLS_ACCT_STS_RSN_CD_SK").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.FROM_DT_SK").alias("FROM_DT_SK"),
        F.col("lnk_IdsEdwPrvcyDsclsurAcctgFExtr_InAbc.RCVD_DT_SK").alias("RCVD_DT_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: xfm_BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------
# Split into three outputs based on constraints:

# 1) Main link: lnk_Main
df_xfm_BusinessLogic_lnk_Main = (
    df_lkp_Codes
    .filter(
        (F.col("PRVCY_DSCLSUR_ACCTG_SK") != 0)
        & (F.col("PRVCY_DSCLSUR_ACCTG_SK") != 1)
    )
    .select(
        F.col("PRVCY_DSCLSUR_ACCTG_SK").alias("PRVCY_DSCLSUR_ACCTG_SK"),
        F.when(
            (F.col("SRC_SYS_CD").isNull()) | (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("RQST_NO").alias("PRVCY_DSCLSUR_ACCTG_RQST_NO"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("CRT_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_CRT_DT_SK"),
        F.col("FROM_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_FROM_DT_SK"),
        F.col("RCVD_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_RCVD_DT_SK"),
        F.when(
            (F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD").isNull())
            | (F.length(trim(F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD")).alias("PRVCY_DSCLSUR_ACCTG_RQST_CD"),
        F.when(
            (F.col("PRVCY_DSCLSUR_ACCTG_RQST_NM").isNull())
            | (F.length(trim(F.col("PRVCY_DSCLSUR_ACCTG_RQST_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_DSCLSUR_ACCTG_RQST_NM")).alias("PRVCY_DSCLSUR_ACCTG_RQST_NM"),
        F.col("RQST_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_RQST_DT_SK"),
        F.col("RQST_DESC").alias("PRVCY_DSCLSUR_ACCTG_RQST_DESC"),
        F.col("STTUS_CD_TX").alias("PRVCY_DSCLS_ACCT_STTUS_CD_TX"),
        F.col("STTUS_DT_SK").alias("PRVCY_DSCLS_ACCT_STTUS_DT_SK"),
        F.when(
            (F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD").isNull())
            | (F.length(trim(F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD")).alias("PRVCY_DSCLS_ACCT_STS_RSN_CD"),
        F.when(
            (F.col("PRVCY_DSCLS_ACCT_STS_RSN_NM").isNull())
            | (F.length(trim(F.col("PRVCY_DSCLS_ACCT_STS_RSN_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_DSCLS_ACCT_STS_RSN_NM")).alias("PRVCY_DSCLS_ACCT_STS_RSN_NM"),
        F.col("TO_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_TO_DT_SK"),
        F.when(
            (F.col("PRVCY_MBR_SRC_CD").isNull())
            | (F.length(trim(F.col("PRVCY_MBR_SRC_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_MBR_SRC_CD")).alias("PRVCY_MBR_SRC_CD"),
        F.when(
            (F.col("PRVCY_MBR_SRC_NM").isNull())
            | (F.length(trim(F.col("PRVCY_MBR_SRC_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRVCY_MBR_SRC_NM")).alias("PRVCY_MBR_SRC_NM"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
        F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
        F.col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK")
    )
)

# 2) NA link
# Constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Produce exactly one row of hard-coded values.
df_xfm_BusinessLogic_NA = (
    df_lkp_Codes
    .limit(1)
    .select(
        F.lit(1).alias("PRVCY_DSCLSUR_ACCTG_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_DSCLSUR_ACCTG_RQST_NO"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_CRT_DT_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_FROM_DT_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_RCVD_DT_SK"),
        F.lit("NA").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD"),
        F.lit("NA").alias("PRVCY_DSCLSUR_ACCTG_RQST_NM"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_RQST_DT_SK"),
        F.lit("").alias("PRVCY_DSCLSUR_ACCTG_RQST_DESC"),
        F.lit("").alias("PRVCY_DSCLS_ACCT_STTUS_CD_TX"),
        F.lit("1753-01-01").alias("PRVCY_DSCLS_ACCT_STTUS_DT_SK"),
        F.lit("NA").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD"),
        F.lit("NA").alias("PRVCY_DSCLS_ACCT_STS_RSN_NM"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_TO_DT_SK"),
        F.lit("NA").alias("PRVCY_MBR_SRC_CD"),
        F.lit("NA").alias("PRVCY_MBR_SRC_NM"),
        F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
        F.lit(1).alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
        F.lit(1).alias("PRVCY_MBR_SRC_CD_SK")
    )
)

# 3) UNK link
# Constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Produce exactly one row of hard-coded values.
df_xfm_BusinessLogic_UNK = (
    df_lkp_Codes
    .limit(1)
    .select(
        F.lit(0).alias("PRVCY_DSCLSUR_ACCTG_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
        F.lit(0).alias("PRVCY_DSCLSUR_ACCTG_RQST_NO"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_CRT_DT_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_FROM_DT_SK"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_RCVD_DT_SK"),
        F.lit("UNK").alias("PRVCY_DSCLSUR_ACCTG_RQST_CD"),
        F.lit("UNK").alias("PRVCY_DSCLSUR_ACCTG_RQST_NM"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_RQST_DT_SK"),
        F.lit("").alias("PRVCY_DSCLSUR_ACCTG_RQST_DESC"),
        F.lit("").alias("PRVCY_DSCLS_ACCT_STTUS_CD_TX"),
        F.lit("1753-01-01").alias("PRVCY_DSCLS_ACCT_STTUS_DT_SK"),
        F.lit("UNK").alias("PRVCY_DSCLS_ACCT_STS_RSN_CD"),
        F.lit("UNK").alias("PRVCY_DSCLS_ACCT_STS_RSN_NM"),
        F.lit("1753-01-01").alias("PRVCY_DSCLSUR_ACCTG_TO_DT_SK"),
        F.lit("UNK").alias("PRVCY_MBR_SRC_CD"),
        F.lit("UNK").alias("PRVCY_MBR_SRC_NM"),
        F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
        F.lit(0).alias("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
        F.lit(0).alias("PRVCY_MBR_SRC_CD_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: fnl_dataLinks (PxFunnel)
# ----------------------------------------------------------------------------
df_fnl_dataLinks = df_xfm_BusinessLogic_NA.unionByName(df_xfm_BusinessLogic_lnk_Main).unionByName(df_xfm_BusinessLogic_UNK)

# ----------------------------------------------------------------------------
# Stage: seq_PRVCY_DSCLSUR_ACCTG_F_Load (PxSequentialFile)
# ----------------------------------------------------------------------------
# Apply rpad for char(10) columns before writing.

rpad10 = lambda c: F.rpad(F.col(c), 10, " ")

df_final = df_fnl_dataLinks.select(
    F.col("PRVCY_DSCLSUR_ACCTG_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_NO"),
    rpad10("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad10("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("PRVCY_EXTRNL_MBR_SK"),
    rpad10("PRVCY_DSCLSUR_ACCTG_CRT_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_CRT_DT_SK"),
    rpad10("PRVCY_DSCLSUR_ACCTG_FROM_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_FROM_DT_SK"),
    rpad10("PRVCY_DSCLSUR_ACCTG_RCVD_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_RCVD_DT_SK"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_NM"),
    rpad10("PRVCY_DSCLSUR_ACCTG_RQST_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_RQST_DT_SK"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_DESC"),
    F.col("PRVCY_DSCLS_ACCT_STTUS_CD_TX"),
    rpad10("PRVCY_DSCLS_ACCT_STTUS_DT_SK").alias("PRVCY_DSCLS_ACCT_STTUS_DT_SK"),
    F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD"),
    F.col("PRVCY_DSCLS_ACCT_STS_RSN_NM"),
    rpad10("PRVCY_DSCLSUR_ACCTG_TO_DT_SK").alias("PRVCY_DSCLSUR_ACCTG_TO_DT_SK"),
    F.col("PRVCY_MBR_SRC_CD"),
    F.col("PRVCY_MBR_SRC_NM"),
    rpad10("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_DSCLSUR_ACCTG_RQST_CD_SK"),
    F.col("PRVCY_DSCLS_ACCT_STS_RSN_CD_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PRVCY_DSCLSUR_ACCTG_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)