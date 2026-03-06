# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME: FctsIdsWorkCompClmLnExtr
# MAGIC CALLED BY: FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-14\(9)5628 WORK_COMPNSTN_CLM_LN \(9)    Original Programming\(9)\(9)\(9)          IntegrateDev2                          Kalyan Neelam           2017-03-15 
# MAGIC                                                                 (Facets to IDS) ETL Report
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-12\(9)5628 WORK_COMPNSTN_CLM \(9)    Reversal Logic updated\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-05-22   
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_LN.dat to be send to IDS WORK_COMPNSTN_CLM_LN table
# MAGIC Join CMC_SBSB_SUBSC, CMC_MEME_MEMBER and CMC_GRGR_GROUP  for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC B_WORK_COMPNSTN_CLM_LN.dat load file have the key fields from ids table WORK_COMPNSTN_CLM_LN for Balancing Report
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, when, concat, rpad, trim, concat_ws, regexp_replace, to_date, expr, union_by_name
from pyspark.sql.functions import concat as spark_concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# ----------------------------------------------------------------------------
# Parameter Definitions
# ----------------------------------------------------------------------------
facets_secret_name = get_widget_value('facets_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
workcompowner_secret_name = get_widget_value('workcompowner_secret_name','')
RunCycle = get_widget_value('RunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
DrivTable = get_widget_value('DrivTable','')

# ----------------------------------------------------------------------------
# Stage: WorkCompnstnClmLn (DB2ConnectorPX, Database = IDS)
# ----------------------------------------------------------------------------
jdbc_url_WorkCompnstnClmLn, jdbc_props_WorkCompnstnClmLn = get_db_config(ids_secret_name)
query_WorkCompnstnClmLn = """
SELECT
CLM_ID,
CLM_LN_SEQ_NO,
CRT_RUN_CYC_EXCTN_SK
FROM #$WorkCompOwner#.WORK_COMPNSTN_CLM_LN
WHERE SRC_SYS_CD = 'FACETS'
"""
df_WorkCompnstnClmLn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_WorkCompnstnClmLn)
    .options(**jdbc_props_WorkCompnstnClmLn)
    .option("query", query_WorkCompnstnClmLn)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: PROC_CD (DB2ConnectorPX, Database = IDS)
# ----------------------------------------------------------------------------
jdbc_url_PROC_CD, jdbc_props_PROC_CD = get_db_config(ids_secret_name)
query_PROC_CD = """
SELECT
PROC_CD,
PROC_CD_SK
FROM #$IDSOwner#.PROC_CD
WHERE PROC_CD_CAT_CD = 'MED'
"""
df_ProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_PROC_CD)
    .options(**jdbc_props_PROC_CD)
    .option("query", query_PROC_CD)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: PROV (DB2ConnectorPX, Database = IDS)
# ----------------------------------------------------------------------------
jdbc_url_PROV, jdbc_props_PROV = get_db_config(ids_secret_name)
query_PROV = f"""
SELECT
PROV_ID,
PROV_SK
FROM #$IDSOwner#.PROV
WHERE SRC_SYS_CD_SK = {SrcSysCdSk}
"""
df_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_PROV)
    .options(**jdbc_props_PROV)
    .option("query", query_PROV)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Ds_ReversalClaim (PxDataSet) -> Read from parquet
# ----------------------------------------------------------------------------
ds_ReversalClaim_schema = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CLCL_CUR_STS", StringType(), True),
    StructField("CLCL_PAID_DT", StringType(), True)
])
df_Ds_ReversalClaim = spark.read.schema(ds_ReversalClaim_schema).parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

# ----------------------------------------------------------------------------
# Stage: FacetsDB_Input (ODBCConnectorPX)
# ----------------------------------------------------------------------------
jdbc_url_FacetsDB_Input, jdbc_props_FacetsDB_Input = get_db_config(facets_secret_name)
query_FacetsDB_Input = f"""
SELECT
CLMLN.CLCL_ID,
CLMLN.CDML_SEQ_NO,
RTRIM(LTRIM(CLMLN.PRPR_ID)) AS PRPR_ID,
CLMLN.CDML_CUR_STS,
CAST(CLMLN.IPCD_ID as VARCHAR(20)) AS IPCD_ID,
CLMLN.CDML_FROM_DT,
CLMLN.CDML_TO_DT,
CLMLN.CDML_CHG_AMT,
CLMLN.CDML_CONSIDER_CHG,
CLMLN.CDML_ALLOW,
CLMLN.CDML_DED_AMT,
CLMLN.CDML_COPAY_AMT,
CLMLN.CDML_COINS_AMT,
CLMLN.CDML_RISK_WH_AMT,
CLMLN.CDML_PAID_AMT,
CLMLN.CDML_DISALL_AMT,
CLMLN.CDML_AG_PRICE,
CLMLN.CDML_PF_PRICE,
CLMLN.CDML_IP_PRICE,
CLMLN.CDML_SE_PRICE,
CLMLN.CDML_SUP_DISC_AMT,
CLMLN.CDML_ITS_DISC_AMT,
CLMLN.CDML_SB_PYMT_AMT,
CLMLN.CDML_PR_PYMT_AMT,
CLMLN.CDML_REFSV_SEQ_NO,
CLMLN.CDML_UMAUTH_ID,
CLMLN.CDML_AUTHSV_SEQ_NO,
CLMLN.CDML_OOP_CALC_BASE,
CLMLN.CDML_REF_IND,
CLMLN.CDML_PC_IND,
CLMLN.CDML_PRE_AUTH_IND
FROM #$FacetsOwner#.CMC_CDML_CL_LINE AS CLMLN
INNER JOIN tempdb..{DrivTable} Drvr
ON Drvr.CLM_ID = CLMLN.CLCL_ID
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FacetsDB_Input)
    .options(**jdbc_props_FacetsDB_Input)
    .option("query", query_FacetsDB_Input)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX, Database = IDS)
# ----------------------------------------------------------------------------
jdbc_url_CD_MPPNG, jdbc_props_CD_MPPNG = get_db_config(ids_secret_name)
query_CD_MPPNG = """
SELECT DISTINCT
SRC_CD,
CD_MPPNG_SK,
SRC_DOMAIN_NM,
TRGT_DOMAIN_NM
FROM #$IDSOwner#.CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
AND SRC_CLCTN_CD = 'FACETS DBO'
AND SRC_DOMAIN_NM IN ('CLAIM LINE PREAUTHORIZATION', 'CLAIM LINE PREAUTHORIZATION SOURCE','CLAIM LINE REFERRAL')
AND TRGT_CLCTN_CD = 'IDS'
AND TRGT_DOMAIN_NM  IN ('CLAIM LINE PREAUTHORIZATION', 'CLAIM LINE PREAUTHORIZATION SOURCE','CLAIM LINE REFERRAL')
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CD_MPPNG)
    .options(**jdbc_props_CD_MPPNG)
    .option("query", query_CD_MPPNG)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Flt_CdMppng (PxFilter)
# ----------------------------------------------------------------------------
df_Flt_CdMppng_PreAuth = df_CD_MPPNG.filter(
    (col("SRC_DOMAIN_NM") == "CLAIM LINE PREAUTHORIZATION") & (col("TRGT_DOMAIN_NM") == "CLAIM LINE PREAUTHORIZATION")
).select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_Flt_CdMppng_PreAuthSrc = df_CD_MPPNG.filter(
    (col("SRC_DOMAIN_NM") == "CLAIM LINE PREAUTHORIZATION SOURCE") & (col("TRGT_DOMAIN_NM") == "CLAIM LINE PREAUTHORIZATION SOURCE")
).select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_Flt_CdMppng_Rfrl = df_CD_MPPNG.filter(
    (col("SRC_DOMAIN_NM") == "CLAIM LINE REFERRAL") & (col("TRGT_DOMAIN_NM") == "CLAIM LINE REFERRAL")
).select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# ----------------------------------------------------------------------------
# Stage: Lkp_twoDBs (PxLookup) - Primary link + multiple left lookups
# ----------------------------------------------------------------------------
df_Lkp_twoDBs = (
    df_FacetsDB_Input.alias("Lnk_Facets")
    .join(
        df_WorkCompnstnClmLn.alias("Lnk_WorkCompClmLn"),
        (col("Lnk_Facets.CLCL_ID") == col("Lnk_WorkCompClmLn.CLM_ID")) &
        (col("Lnk_Facets.CDML_SEQ_NO") == col("Lnk_WorkCompClmLn.CLM_LN_SEQ_NO")),
        "left"
    )
    .join(
        df_Flt_CdMppng_PreAuth.alias("PreAuth"),
        col("Lnk_Facets.CDML_PC_IND") == col("PreAuth.SRC_CD"),
        "left"
    )
    .join(
        df_Flt_CdMppng_PreAuthSrc.alias("PreAuthSrc"),
        col("Lnk_Facets.CDML_PRE_AUTH_IND") == col("PreAuthSrc.SRC_CD"),
        "left"
    )
    .join(
        df_Flt_CdMppng_Rfrl.alias("Rfrl"),
        col("Lnk_Facets.CDML_REF_IND") == col("Rfrl.SRC_CD"),
        "left"
    )
    .join(
        df_ProcCd.alias("Lnk_ProcCd"),
        col("Lnk_Facets.IPCD_ID") == col("Lnk_ProcCd.PROC_CD"),
        "left"
    )
    .join(
        df_Prov.alias("Lnk_Prov"),
        col("Lnk_Facets.PRPR_ID") == col("Lnk_Prov.PROV_ID"),
        "left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        col("Lnk_Facets.CLCL_ID") == col("Lnk_ReversalDs.CLCL_ID"),
        "left"
    )
)

df_Lkp_twoDBs_sel = df_Lkp_twoDBs.select(
    col("Lnk_Facets.CLCL_ID").alias("CLCL_ID"),
    col("Lnk_Facets.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    col("Lnk_Facets.PRPR_ID").alias("PRPR_ID"),
    col("Lnk_Facets.CDML_CUR_STS").alias("CDML_CUR_STS"),
    col("Lnk_Facets.CDML_FROM_DT").alias("CDML_FROM_DT"),
    col("Lnk_Facets.CDML_TO_DT").alias("CDML_TO_DT"),
    col("Lnk_Facets.CDML_CHG_AMT").alias("CDML_CHG_AMT"),
    col("Lnk_Facets.CDML_CONSIDER_CHG").alias("CDML_CONSIDER_CHG"),
    col("Lnk_Facets.CDML_ALLOW").alias("CDML_ALLOW"),
    col("Lnk_Facets.CDML_DED_AMT").alias("CDML_DED_AMT"),
    col("Lnk_Facets.CDML_COPAY_AMT").alias("CDML_COPAY_AMT"),
    col("Lnk_Facets.CDML_COINS_AMT").alias("CDML_COINS_AMT"),
    col("Lnk_Facets.CDML_RISK_WH_AMT").alias("CDML_RISK_WH_AMT"),
    col("Lnk_Facets.CDML_PAID_AMT").alias("CDML_PAID_AMT"),
    col("Lnk_Facets.CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    col("Lnk_Facets.CDML_AG_PRICE").alias("CDML_AG_PRICE"),
    col("Lnk_Facets.CDML_PF_PRICE").alias("CDML_PF_PRICE"),
    col("Lnk_Facets.CDML_IP_PRICE").alias("CDML_IP_PRICE"),
    col("Lnk_Facets.CDML_SE_PRICE").alias("CDML_SE_PRICE"),
    col("Lnk_Facets.CDML_SUP_DISC_AMT").alias("CDML_SUP_DISC_AMT"),
    col("Lnk_Facets.CDML_ITS_DISC_AMT").alias("CDML_ITS_DISC_AMT"),
    col("Lnk_Facets.CDML_SB_PYMT_AMT").alias("CDML_SB_PYMT_AMT"),
    col("Lnk_Facets.CDML_PR_PYMT_AMT").alias("CDML_PR_PYMT_AMT"),
    col("Lnk_Facets.CDML_REFSV_SEQ_NO").alias("CDML_REFSV_SEQ_NO"),
    col("Lnk_Facets.CDML_UMAUTH_ID").alias("CDML_UMAUTH_ID"),
    col("Lnk_Facets.CDML_AUTHSV_SEQ_NO").alias("CDML_AUTHSV_SEQ_NO"),
    col("Lnk_Facets.CDML_OOP_CALC_BASE").alias("CDML_OOP_CALC_BASE"),
    col("Lnk_Facets.CDML_REF_IND").alias("CDML_REF_IND"),
    col("Lnk_Facets.CDML_PC_IND").alias("CDML_PC_IND"),
    col("Lnk_Facets.CDML_PRE_AUTH_IND").alias("CDML_PRE_AUTH_IND"),
    col("Lnk_WorkCompClmLn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("PreAuth.CD_MPPNG_SK").alias("CD_MPPNG_SK_PRE_AUTH"),
    col("PreAuthSrc.CD_MPPNG_SK").alias("CD_MPPNG_SK_PRE_AUTH_SRC"),
    col("Rfrl.CD_MPPNG_SK").alias("CD_MPPNG_SK_RFRL"),
    col("Lnk_ProcCd.PROC_CD_SK").alias("PROC_CD_SK"),
    col("Lnk_Prov.PROV_SK").alias("PROV_SK"),
    col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse")
)

# ----------------------------------------------------------------------------
# Stage: Xfm_Reversal (CTransformerStage)
# Output link1: Lnk_All (no filter), link2: Lnk_Reversal (filter on CLCL_ID_Reverse)
# ----------------------------------------------------------------------------

df_Xfm_Reversal_Lnk_All = df_Lkp_twoDBs_sel.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("PRPR_ID"),
    col("CDML_CUR_STS"),
    col("CDML_FROM_DT"),
    col("CDML_TO_DT"),
    col("CDML_CHG_AMT"),
    col("CDML_CONSIDER_CHG"),
    col("CDML_ALLOW"),
    col("CDML_DED_AMT"),
    col("CDML_COPAY_AMT"),
    col("CDML_COINS_AMT"),
    col("CDML_RISK_WH_AMT"),
    col("CDML_PAID_AMT"),
    col("CDML_DISALL_AMT"),
    col("CDML_AG_PRICE"),
    col("CDML_PF_PRICE"),
    col("CDML_IP_PRICE"),
    col("CDML_SE_PRICE"),
    col("CDML_SUP_DISC_AMT"),
    col("CDML_ITS_DISC_AMT"),
    col("CDML_SB_PYMT_AMT"),
    col("CDML_PR_PYMT_AMT"),
    col("CDML_REFSV_SEQ_NO"),
    col("CDML_UMAUTH_ID"),
    col("CDML_AUTHSV_SEQ_NO"),
    col("CDML_OOP_CALC_BASE"),
    col("CDML_REF_IND"),
    col("CDML_PC_IND"),
    col("CDML_PRE_AUTH_IND"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CD_MPPNG_SK_PRE_AUTH"),
    col("CD_MPPNG_SK_PRE_AUTH_SRC"),
    col("CD_MPPNG_SK_RFRL"),
    col("PROC_CD_SK"),
    col("PROV_SK"),
    col("CLCL_ID_Reverse")
)

isReversalCondition = (col("CLCL_ID_Reverse").isNotNull() & (trim(col("CLCL_ID_Reverse")) != ''))

df_Xfm_Reversal_Lnk_Reversal = df_Lkp_twoDBs_sel.filter(isReversalCondition).select(
    spark_concat(trim(col("CLCL_ID")), lit("R")).alias("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("PRPR_ID"),
    col("CDML_CUR_STS"),
    col("CDML_FROM_DT"),
    col("CDML_TO_DT"),
    col("CDML_CHG_AMT"),
    col("CDML_CONSIDER_CHG"),
    col("CDML_ALLOW"),
    col("CDML_DED_AMT"),
    col("CDML_COPAY_AMT"),
    col("CDML_COINS_AMT"),
    col("CDML_RISK_WH_AMT"),
    col("CDML_PAID_AMT"),
    col("CDML_DISALL_AMT"),
    col("CDML_AG_PRICE"),
    col("CDML_PF_PRICE"),
    col("CDML_IP_PRICE"),
    col("CDML_SE_PRICE"),
    col("CDML_SUP_DISC_AMT"),
    col("CDML_ITS_DISC_AMT"),
    col("CDML_SB_PYMT_AMT"),
    col("CDML_PR_PYMT_AMT"),
    col("CDML_REFSV_SEQ_NO"),
    col("CDML_UMAUTH_ID"),
    col("CDML_AUTHSV_SEQ_NO"),
    col("CDML_OOP_CALC_BASE"),
    col("CDML_REF_IND"),
    col("CDML_PC_IND"),
    col("CDML_PRE_AUTH_IND"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CD_MPPNG_SK_PRE_AUTH"),
    col("CD_MPPNG_SK_PRE_AUTH_SRC"),
    col("CD_MPPNG_SK_RFRL"),
    col("PROC_CD_SK"),
    col("PROV_SK"),
    col("CLCL_ID_Reverse")
)

# ----------------------------------------------------------------------------
# Stage: Fnl_All_Reverse (PxFunnel => union)
# ----------------------------------------------------------------------------
df_Fnl_All_Reverse_pre = df_Xfm_Reversal_Lnk_All.unionByName(df_Xfm_Reversal_Lnk_Reversal)

df_Fnl_All_Reverse = df_Fnl_All_Reverse_pre.select(
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("PRPR_ID"),
    col("CDML_CUR_STS"),
    col("CDML_FROM_DT"),
    col("CDML_TO_DT"),
    col("CDML_CHG_AMT"),
    col("CDML_CONSIDER_CHG"),
    col("CDML_ALLOW"),
    col("CDML_DED_AMT"),
    col("CDML_COPAY_AMT"),
    col("CDML_COINS_AMT"),
    col("CDML_RISK_WH_AMT"),
    col("CDML_PAID_AMT"),
    col("CDML_DISALL_AMT"),
    col("CDML_AG_PRICE"),
    col("CDML_PF_PRICE"),
    col("CDML_IP_PRICE"),
    col("CDML_SE_PRICE"),
    col("CDML_SUP_DISC_AMT"),
    col("CDML_ITS_DISC_AMT"),
    col("CDML_SB_PYMT_AMT"),
    col("CDML_PR_PYMT_AMT"),
    col("CDML_REFSV_SEQ_NO"),
    col("CDML_UMAUTH_ID"),
    col("CDML_AUTHSV_SEQ_NO"),
    col("CDML_OOP_CALC_BASE"),
    col("CDML_REF_IND"),
    col("CDML_PC_IND"),
    col("CDML_PRE_AUTH_IND"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CD_MPPNG_SK_PRE_AUTH"),
    col("CD_MPPNG_SK_PRE_AUTH_SRC"),
    col("CD_MPPNG_SK_RFRL"),
    col("PROC_CD_SK"),
    col("PROV_SK"),
    col("CLCL_ID_Reverse")
)

# ----------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
# Output Pins: Lnk_WorkCompClmLn, Lnk_WorkCompnstnClmLnBalReport
# ----------------------------------------------------------------------------

negateCondition = when(isReversalCondition, lit(-1)).otherwise(lit(1))
df_BusinessLogic = df_Fnl_All_Reverse.alias("Lnk_BusinessLogic")

df_BusinessLogic_Lnk_WorkCompClmLn = df_BusinessLogic.select(
    # CLM_ID (rpad(12))
    rpad(col("Lnk_BusinessLogic.CLCL_ID"), 12, " ").alias("CLM_ID"),
    # SRC_SYS_CD
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    # CLM_LN_SEQ_NO
    col("Lnk_BusinessLogic.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # CRT_RUN_CYC_EXCTN_SK
    when(col("Lnk_BusinessLogic.CRT_RUN_CYC_EXCTN_SK").isNull(), lit(RunCycle)).otherwise(col("Lnk_BusinessLogic.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # PROC_CD_SK
    when(col("Lnk_BusinessLogic.PROC_CD_SK").isNull(), lit(0)).otherwise(col("Lnk_BusinessLogic.PROC_CD_SK")).alias("PROC_CD_SK"),
    # SVC_PROV_SK
    when(col("Lnk_BusinessLogic.PROV_SK").isNotNull(), col("Lnk_BusinessLogic.PROV_SK"))
    .otherwise(
        when(
            (col("Lnk_BusinessLogic.CDML_CUR_STS").isin("11","15","81","82","93","97","99")),
            lit(1)
        ).otherwise(lit(0))
    ).alias("SVC_PROV_SK"),
    # CLM_LN_PREAUTH_CD_SK
    when(trim(col("Lnk_BusinessLogic.CDML_PC_IND")) == "", lit(1))
    .otherwise(
        when(col("Lnk_BusinessLogic.CD_MPPNG_SK_PRE_AUTH").isNull(), lit(0))
        .otherwise(col("Lnk_BusinessLogic.CD_MPPNG_SK_PRE_AUTH"))
    ).alias("CLM_LN_PREAUTH_CD_SK"),
    # CLM_LN_PREAUTH_SRC_CD_SK
    when(trim(col("Lnk_BusinessLogic.CDML_PRE_AUTH_IND")) == "", lit(1))
    .otherwise(
        when(col("Lnk_BusinessLogic.CD_MPPNG_SK_PRE_AUTH_SRC").isNull(), lit(0))
        .otherwise(col("Lnk_BusinessLogic.CD_MPPNG_SK_PRE_AUTH_SRC"))
    ).alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    # CLM_LN_RFRL_CD_SK
    when(trim(col("Lnk_BusinessLogic.CDML_REF_IND")) == "", lit(1))
    .otherwise(
        when(col("Lnk_BusinessLogic.CD_MPPNG_SK_RFRL").isNull(), lit(0))
        .otherwise(col("Lnk_BusinessLogic.CD_MPPNG_SK_RFRL"))
    ).alias("CLM_LN_RFRL_CD_SK"),
    # SVC_END_DT
    TimestampToDate(col("Lnk_BusinessLogic.CDML_TO_DT")).alias("SVC_END_DT"),
    # SVC_STRT_DT
    TimestampToDate(col("Lnk_BusinessLogic.CDML_FROM_DT")).alias("SVC_STRT_DT"),
    # AGMNT_PRICE_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_AG_PRICE") * -1).otherwise(col("Lnk_BusinessLogic.CDML_AG_PRICE")).alias("AGMNT_PRICE_AMT"),
    # ALW_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_ALLOW") * -1).otherwise(col("Lnk_BusinessLogic.CDML_ALLOW")).alias("ALW_AMT"),
    # CHRG_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_CHG_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_CHG_AMT")).alias("CHRG_AMT"),
    # COINS_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_COINS_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_COINS_AMT")).alias("COINS_AMT"),
    # CNSD_CHRG_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_CONSIDER_CHG") * -1).otherwise(col("Lnk_BusinessLogic.CDML_CONSIDER_CHG")).alias("CNSD_CHRG_AMT"),
    # COPAY_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_COPAY_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_COPAY_AMT")).alias("COPAY_AMT"),
    # DEDCT_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_DED_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_DED_AMT")).alias("DEDCT_AMT"),
    # DSALW_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_DISALL_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_DISALL_AMT")).alias("DSALW_AMT"),
    # ITS_HOME_DSCNT_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_ITS_DISC_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_ITS_DISC_AMT")).alias("ITS_HOME_DSCNT_AMT"),
    # MBR_LIAB_BSS_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_OOP_CALC_BASE") * -1).otherwise(col("Lnk_BusinessLogic.CDML_OOP_CALC_BASE")).alias("MBR_LIAB_BSS_AMT"),
    # NO_RESP_AMT
    lit(None).alias("NO_RESP_AMT"),
    # NON_PAR_SAV_AMT
    lit(None).alias("NON_PAR_SAV_AMT"),
    # PATN_RESP_AMT
    lit(None).alias("PATN_RESP_AMT"),
    # PAYBL_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_PAID_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_PAID_AMT")).alias("PAYBL_AMT"),
    # PAYBL_TO_SUB_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_SB_PYMT_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_SB_PYMT_AMT")).alias("PAYBL_TO_SUB_AMT"),
    # PAYBL_TO_PROV_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_PR_PYMT_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_PR_PYMT_AMT")).alias("PAYBL_TO_PROV_AMT"),
    # PROC_TBL_PRICE_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_IP_PRICE") * -1).otherwise(col("Lnk_BusinessLogic.CDML_IP_PRICE")).alias("PROC_TBL_PRICE_AMT"),
    # PROFL_PRICE_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_PF_PRICE") * -1).otherwise(col("Lnk_BusinessLogic.CDML_PF_PRICE")).alias("PROFL_PRICE_AMT"),
    # PROV_WRT_OFF_AMT
    lit(None).alias("PROV_WRT_OFF_AMT"),
    # RISK_WTHLD_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_RISK_WH_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_RISK_WH_AMT")).alias("RISK_WTHLD_AMT"),
    # SVC_PRICE_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_SE_PRICE") * -1).otherwise(col("Lnk_BusinessLogic.CDML_SE_PRICE")).alias("SVC_PRICE_AMT"),
    # SUPLMT_DSCNT_AMT
    when(isReversalCondition, col("Lnk_BusinessLogic.CDML_SUP_DISC_AMT") * -1).otherwise(col("Lnk_BusinessLogic.CDML_SUP_DISC_AMT")).alias("SUPLMT_DSCNT_AMT"),
    # PREAUTH_ID (rpad(9))
    rpad(col("Lnk_BusinessLogic.CDML_UMAUTH_ID"), 9, " ").alias("PREAUTH_ID"),
    # PREAUTH_SVC_SEQ_NO
    col("Lnk_BusinessLogic.CDML_AUTHSV_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    # RFRL_SVC_SEQ_NO
    col("Lnk_BusinessLogic.CDML_REFSV_SEQ_NO").alias("RFRL_SVC_SEQ_NO")
)

df_BusinessLogic_Lnk_WorkCompnstnClmLnBalReport = df_BusinessLogic.select(
    rpad(col("Lnk_BusinessLogic.CLCL_ID"), 12, " ").alias("CLM_ID"),
    col("Lnk_BusinessLogic.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    lit(SrcSysCd).alias("SRC_SYS_CD")
)

# ----------------------------------------------------------------------------
# Stage: WORK_COMPNSTN_CLM_LN (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
write_files(
    df_BusinessLogic_Lnk_WorkCompClmLn,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_LN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: B_WORK_COMPNSTN_CLM_LN (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
write_files(
    df_BusinessLogic_Lnk_WorkCompnstnClmLnBalReport,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_LN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)