# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: CustSvcIdCardPhnExtrSeq (FacetsBCBSCustSvcIdCardPhnMbrCntl)
# MAGIC 
# MAGIC Process Description: Extract Data from Facets BCBS(GRP_PROD_PHN) and Load it to IDS (CLS_PLN_CUST_SVC_ID_CARD_PHN)
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                Project/                                                                                                                        Code                  Date
# MAGIC Developer         Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Santosh Bokka   9/03/2013    4930        Originally Programmed                                                                                 Kalyan Neelam   2013-10-29
# MAGIC Prabhu ES          2022-03-11    S2S         MSSQL ODBC conn params added            IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic
# MAGIC Extract Facets BCBS Extension Data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, expr, length
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
LastRunDt = get_widget_value("LastRunDt","")
BeginDate = get_widget_value("BeginDate","")
EndDate = get_widget_value("EndDate","")
SrcSysCd = get_widget_value("SrcSysCd","")

# --------------------------------------------------------------------------------
# Stage: GRP_PROD_PHN (ODBCConnector) - Reading from BCBS database
# --------------------------------------------------------------------------------
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_bcbs = f"""
SELECT DISTINCT  
  GRP_PROD_PHN.GRGR_ID,
  GRP_PROD_PHN.CSCS_ID,
  GRP_PROD_PHN.CSPI_ID,
  GRP_PROD_PHN.PDPD_ID,
  GRP_PROD_PHN.GRP_PROD_PHN_EFF_DT,
  GRP_PROD_PHN.PHN_NBR_KEY,
  GRP_PROD_PHN.PHN_USE_TYP_CD,
  GRP_PROD_PHN.GRP_PROD_PHN_END_DT,
  GRP_PROD_PHN.AUDIT_TS,
  GRP_PROD_PHN.USER_ID
FROM {BCBSOwner}.GRP_PROD_PHN AS GRP_PROD_PHN
INNER JOIN {BCBSOwner}.PHN_NBR AS PHN_NBR
  ON GRP_PROD_PHN.PHN_NBR_KEY = PHN_NBR.PHN_NBR_KEY
WHERE 
  GRP_PROD_PHN.AUDIT_TS >= '{BeginDate}' 
  AND GRP_PROD_PHN.AUDIT_TS <= '{EndDate}'
"""
df_GRP_PROD_PHN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_bcbs)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: IDS_CD_MPPNG (DB2Connector) - Reading from IDS database
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_idscd = f"""
SELECT 
  SRC_CD, 
  SRC_SYS_CD, 
  TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_DOMAIN_NM = 'CUSTOMER SERVICE ID CARD PHONE USE'
"""
df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_idscd)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_cust_svc_id_card_phn_use_trgt_cd (CHashedFileStage) 
#          Scenario A: Intermediate hashed file replaced with direct dedup
# --------------------------------------------------------------------------------
# Primary keys: SRC_CD, SRC_SYS_CD
df_cust_svc_id_card_phn_use_lkup = dedup_sort(
    df_IDS_CD_MPPNG,
    partition_cols=["SRC_CD","SRC_SYS_CD"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: IDS_CLS_PLN (DB2Connector) - Reading from IDS database
# --------------------------------------------------------------------------------
extract_query_clspln = f"""
SELECT
  PROD.PROD_ID,
  max(DTL.EFF_DT_SK) AS EFF_DT_SK
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.CLS_PLN_DTL DTL,
     {IDSOwner}.CLS_PLN PLN
WHERE
  DTL.PROD_SK = PROD.PROD_SK
  AND DTL.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND DTL.EFF_DT_SK <= '{RunDate}'
GROUP BY PROD.PROD_ID
"""
df_IDS_CLS_PLN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clspln)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: IDS_CLS_PLN_DTL (DB2Connector) - Reading from IDS database
# --------------------------------------------------------------------------------
extract_query_clspln_dtl = f"""
SELECT
  PROD.PROD_ID,
  DTL.EFF_DT_SK,
  DTL.CLS_PLN_DTL_PROD_CAT_CD_SK,
  CM.SRC_CD
FROM 
  {IDSOwner}.PROD PROD,
  {IDSOwner}.CLS_PLN_DTL DTL,
  {IDSOwner}.CLS_PLN PLN,
  {IDSOwner}.CD_MPPNG CM
WHERE
  DTL.PROD_SK = PROD.PROD_SK
  AND DTL.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND DTL.CLS_PLN_DTL_PROD_CAT_CD_SK = CM.CD_MPPNG_SK
  AND DTL.EFF_DT_SK <= '{RunDate}'
"""
df_IDS_CLS_PLN_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clspln_dtl)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_cls_pln_dtl_max_dt (CHashedFileStage)
#          Scenario A: Intermediate hashed file replaced with direct dedup
# --------------------------------------------------------------------------------
# Primary keys: PROD_ID, EFF_DT_SK
df_hf_cls_pln_dtl_max_dt = dedup_sort(
    df_IDS_CLS_PLN_DTL,
    partition_cols=["PROD_ID","EFF_DT_SK"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: Transformer_18 
#   Left join: df_IDS_CLS_PLN (alias cls_pln_dtl) with df_hf_cls_pln_dtl_max_dt (alias link)
# --------------------------------------------------------------------------------
df_Transformer_18 = (
    df_IDS_CLS_PLN.alias("cls_pln_dtl")
    .join(
        df_hf_cls_pln_dtl_max_dt.alias("link"),
        (col("cls_pln_dtl.PROD_ID") == col("link.PROD_ID")) & 
        (col("cls_pln_dtl.EFF_DT_SK") == col("link.EFF_DT_SK")),
        "left"
    )
    .select(
        col("cls_pln_dtl.PROD_ID").alias("PROD_ID"),
        when(col("link.PROD_ID").isNull(), lit(0)).otherwise(col("link.CLS_PLN_DTL_PROD_CAT_CD_SK")).alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        when(col("link.PROD_ID").isNull(), lit(0)).otherwise(col("link.SRC_CD")).alias("SRC_CD")
    )
)

# --------------------------------------------------------------------------------
# Stage: hf_cls_pln_cust_id_prod_cat_cd (CHashedFileStage)
#          Scenario A: Intermediate hashed file replaced with direct dedup
# --------------------------------------------------------------------------------
# Primary key: PROD_ID
df_hf_cls_pln_cust_id_prod_cat_cd = dedup_sort(
    df_Transformer_18,
    partition_cols=["PROD_ID"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: Business_Rule (CTransformerStage)
#   Primary link: df_GRP_PROD_PHN (alias Grp_Prod_Phn)
#   Lookup link: df_hf_cls_pln_cust_id_prod_cat_cd (alias prod_cat_cd)
#   Lookup link: df_cust_svc_id_card_phn_use_lkup (alias cust_svc_id_card_phn_use_lkup)
# --------------------------------------------------------------------------------
df_Business_Rule = (
    df_GRP_PROD_PHN.alias("Grp_Prod_Phn")
    .join(
        df_hf_cls_pln_cust_id_prod_cat_cd.alias("prod_cat_cd"),
        (col("Grp_Prod_Phn.PDPD_ID") == col("prod_cat_cd.PROD_ID")),
        "left"
    )
    .join(
        df_cust_svc_id_card_phn_use_lkup.alias("cust_svc_id_card_phn_use_lkup"),
        (
            (col("Grp_Prod_Phn.PHN_USE_TYP_CD") == col("cust_svc_id_card_phn_use_lkup.SRC_CD")) &
            (col("SrcSysCd") == col("cust_svc_id_card_phn_use_lkup.SRC_SYS_CD"))
        ),
        "left"
    )
    .select(
        when(col("Grp_Prod_Phn.GRGR_ID").isNull(), lit("NA")).otherwise(col("Grp_Prod_Phn.GRGR_ID")).alias("GRP_ID"),
        when(col("Grp_Prod_Phn.CSCS_ID").isNull(), lit("NA")).otherwise(col("Grp_Prod_Phn.CSCS_ID")).alias("CLS_ID"),
        when(col("Grp_Prod_Phn.CSPI_ID").isNull(), lit("NA")).otherwise(col("Grp_Prod_Phn.CSPI_ID")).alias("CLS_PLN_ID"),
        when(col("Grp_Prod_Phn.PDPD_ID").isNull(), lit("NA")).otherwise(col("Grp_Prod_Phn.PDPD_ID")).alias("PROD_ID"),
        when(
            (col("Grp_Prod_Phn.GRP_PROD_PHN_EFF_DT").isNull()) | (trim(col("Grp_Prod_Phn.GRP_PROD_PHN_EFF_DT")) == lit(""))
        , lit("1753-01-01")).otherwise(
            expr("FORMAT.DATE(Grp_Prod_Phn.GRP_PROD_PHN_EFF_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')")
        ).alias("EFF_DT_SK"),
        when(
            (col("Grp_Prod_Phn.PHN_NBR_KEY").isNull()) | (col("Grp_Prod_Phn.PHN_NBR_KEY") == lit(""))
        , lit("1")).otherwise(col("Grp_Prod_Phn.PHN_NBR_KEY")).alias("CUST_SVC_ID_CARD_PHN_KEY"),
        col("Grp_Prod_Phn.PHN_USE_TYP_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        lit("0").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
        when(
            (col("Grp_Prod_Phn.GRP_PROD_PHN_END_DT").isNull()) | (trim(col("Grp_Prod_Phn.GRP_PROD_PHN_END_DT")) == lit(""))
        , lit("2199-12-31")).otherwise(
            expr("FORMAT.DATE(Grp_Prod_Phn.GRP_PROD_PHN_END_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')")
        ).alias("TERM_DT_SK"),
        col("prod_cat_cd.CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
        col("prod_cat_cd.SRC_CD").alias("SRC_CD"),
        when(col("cust_svc_id_card_phn_use_lkup.TRGT_CD").isNull(), lit("UNK"))
          .otherwise(col("cust_svc_id_card_phn_use_lkup.TRGT_CD")).alias("CUST_SVC_ID_CARD_PHN_USE_Code")
    )
)

# --------------------------------------------------------------------------------
# Stage: DateSk (CTransformerStage)
#   Primary link: df_Business_Rule
#   Build 4 stage variables, then produce final columns
# --------------------------------------------------------------------------------
df_DateSk_intermediate = (
    df_Business_Rule
    .withColumn(
        "svEffdt",
        expr('GetFkeyDate("IDS", CLS_PLN_CUST_SVC_IDCARD_PHN_SK, EFF_DT_SK, "X")')
    )
    .withColumn(
        "svTermdt",
        expr('GetFkeyDate("IDS", CLS_PLN_CUST_SVC_IDCARD_PHN_SK, TERM_DT_SK, "X")')
    )
    .withColumn(
        "svPhnKey",
        # Matches the separate phone-key logic (stage var). 
        # The job's final column uses simpler logic, but we must not skip. 
        when(
            length(trim(col("CUST_SVC_ID_CARD_PHN_KEY")))==0,
            lit("00000000000")
        ).otherwise(
            expr('trim(right("00000000000" : Oconv(trim(CUST_SVC_ID_CARD_PHN_KEY * 1), "MDO"), 11))')
        )
    )
    .withColumn(
        "svCustSvcIdCardPhnUseCd",
        when(col("CUST_SVC_ID_CARD_PHN_USE_CD").isNull(), lit("UNK")).otherwise(col("CUST_SVC_ID_CARD_PHN_USE_CD"))
    )
)

df_DateSk = df_DateSk_intermediate.select(
    lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(RunDate).alias("FIRST_RECYC_DT"),
    lit("0").alias("ERR_CT"),
    lit("0").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    expr('"FACETS" : ";" : GRP_ID : ";" : "NA" : ";" : CLS_ID : ";" : CLS_PLN_ID : ";" : PROD_ID : ";" : EFF_DT_SK : ";" : CUST_SVC_ID_CARD_PHN_KEY : ";" : svCustSvcIdCardPhnUseCd').alias("PRI_KEY_STRING"),
    col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    col("GRP_ID").alias("GRP_ID"),
    lit("NA").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    when(trim(col("svEffdt"))==lit("UNK"), lit("1753-01-01")).otherwise(col("svEffdt")).alias("EFF_DT_SK"),
    col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("svCustSvcIdCardPhnUseCd").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(trim(col("svTermdt"))==lit("UNK"), lit("2199-12-31")).otherwise(col("svTermdt")).alias("PROD_PHN_END_DT"),
    col("CLS_PLN_DTL_PROD_CAT_CD_SK").alias("CLS_PLN_DTL_PROD_CAT_CD_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("CUST_SVC_ID_CARD_PHN_USE_Code").alias("CUST_SVC_ID_CARD_PHN_USE_Code")
)

# --------------------------------------------------------------------------------
# Stage: hf_cls_pln_cust_svc_id_card_phn (CHashedFileStage) in scenario B
#   This is read-modify-write. We treat this hashed file as a persistent table in IDS
#   Named: IDS.dummy_hf_cls_pln_cust_svc_id_card_phn
#   We'll load it to do the left lookup in PrimaryKey stage.
# --------------------------------------------------------------------------------
jdbc_url_ids_lookup, jdbc_props_ids_lookup = get_db_config(ids_secret_name)
extract_query_dummy_hash = """
SELECT
  SRC_SYS_CD,
  GRP_ID,
  SUBGRP_ID,
  CLS_ID,
  CLS_PLN_ID,
  PROD_ID,
  EFF_DT_SK,
  CUST_SVC_ID_CARD_PHN_KEY,
  CUST_SVC_ID_CARD_PHN_USE_CD,
  CRT_RUN_CYC_EXCTN_SK,
  CLS_PLN_CUST_SVC_IDCARD_PHN_SK
FROM IDS.dummy_hf_cls_pln_cust_svc_id_card_phn
"""
df_hf_cls_pln_cust_svc_id_card_phn_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_lookup)
    .options(**jdbc_props_ids_lookup)
    .option("query", extract_query_dummy_hash)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
#   Left join with df_hf_cls_pln_cust_svc_id_card_phn_lookup as "lkup"
#   Primary link: df_DateSk as "Transform"
#   Stage variable: SK => if isnull(lkup...) then KeyMgtGetNextValueConcurrent(...) else ...
#   We translate KeyMgtGetNextValueConcurrent => SurrogateKeyGen.
# --------------------------------------------------------------------------------
df_PrimaryKey_join = (
    df_DateSk.alias("Transform")
    .join(
        df_hf_cls_pln_cust_svc_id_card_phn_lookup.alias("lkup"),
        (
            (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) &
            (col("Transform.GRP_ID") == col("lkup.GRP_ID")) &
            (col("Transform.SUBGRP_ID") == col("lkup.SUBGRP_ID")) &
            (col("Transform.CLS_ID") == col("lkup.CLS_ID")) &
            (col("Transform.CLS_PLN_ID") == col("lkup.CLS_PLN_ID")) &
            (col("Transform.PROD_ID") == col("lkup.PROD_ID")) &
            (col("Transform.EFF_DT_SK") == col("lkup.EFF_DT_SK")) &
            (col("Transform.CUST_SVC_ID_CARD_PHN_KEY") == col("lkup.CUST_SVC_ID_CARD_PHN_KEY")) &
            (col("Transform.CUST_SVC_ID_CARD_PHN_USE_CD") == col("lkup.CUST_SVC_ID_CARD_PHN_USE_CD"))
        ),
        "left"
    )
)

# SurrogateKeyGen for the KeyMgtGetNextValueConcurrent usage:
df_enriched = df_PrimaryKey_join
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CLS_PLN_CUST_SVC_IDCARD_PHN_SK",<schema>,<secret_name>)

# Now select columns needed for "Key" link (no constraint):
df_Key = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    # "SK" is populated by SurrogateKeyGen into "CLS_PLN_CUST_SVC_IDCARD_PHN_SK"
    col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK"),
    col("Transform.GRP_ID").alias("GRP_ID"),
    col("Transform.SUBGRP_ID").alias("SUBGRP_ID"),
    col("Transform.CLS_ID").alias("CLS_ID"),
    col("Transform.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Transform.PROD_ID").alias("PROD_ID"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("Transform.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    when(col("lkup.CLS_PLN_CUST_SVC_IDCARD_PHN_SK").isNull(), col("Transform.CRT_RUN_CYC_EXCTN_SK"))
      .otherwise(lit("<unused>")).alias("CRT_RUN_CYC_EXCTN_SK"),  # We will overwrite properly below
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.PROD_PHN_END_DT").alias("TERM_DT_SK"),
    col("Transform.SRC_CD").alias("CLS_PLN_DTL_PROD_CAT_CD"),  # from the JSON
)

# We also must update "CRT_RUN_CYC_EXCTN_SK" with "NewCrtRunCycExtcnSk" logic:
# "NewCrtRunCycExtcnSk" = if isNull(lkup.CLS_PLN_CUST_SVC_IDCARD_PHN_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
df_Key = df_Key.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("lkup.CLS_PLN_CUST_SVC_IDCARD_PHN_SK").isNull(), lit(CurrRunCycle))
     .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# --------------------------------------------------------------------------------
# Output link "updt" (constraint is IsNull(lkup.CLS_PLN_CUST_SVC_IDCARD_PHN_SK)=@TRUE) => these are new inserts
# We gather columns for the hashed file re-write => scenario B => upsert to dummy table
# --------------------------------------------------------------------------------
df_updt = df_enriched.filter(col("lkup.CLS_PLN_CUST_SVC_IDCARD_PHN_SK").isNull()).select(
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.GRP_ID").alias("GRP_ID"),
    col("Transform.SUBGRP_ID").alias("SUBGRP_ID"),
    col("Transform.CLS_ID").alias("CLS_ID"),
    col("Transform.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Transform.PROD_ID").alias("PROD_ID"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("Transform.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CLS_PLN_CUST_SVC_IDCARD_PHN_SK").alias("CLS_PLN_CUST_SVC_IDCARD_PHN_SK")
)

# Write df_updt to a STAGING table, then MERGE into IDS.dummy_hf_cls_pln_cust_svc_id_card_phn
staging_table = "STAGING.FctsBCBSClsPlnCustSvcIdCardPhnExtr_hf_cls_pln_cust_svc_id_card_phn_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {staging_table}", jdbc_url_ids_lookup, jdbc_props_ids_lookup)

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_ids_lookup)
    .options(**jdbc_props_ids_lookup)
    .option("dbtable", staging_table)
    .mode("append")
    .save()
)

# Build MERGE statement with 9-column PK
merge_sql = f"""
MERGE INTO IDS.dummy_hf_cls_pln_cust_svc_id_card_phn AS tgt
USING {staging_table} AS src
ON
  tgt.SRC_SYS_CD = src.SRC_SYS_CD
  AND tgt.GRP_ID = src.GRP_ID
  AND tgt.SUBGRP_ID = src.SUBGRP_ID
  AND tgt.CLS_ID = src.CLS_ID
  AND tgt.CLS_PLN_ID = src.CLS_PLN_ID
  AND tgt.PROD_ID = src.PROD_ID
  AND tgt.EFF_DT_SK = src.EFF_DT_SK
  AND tgt.CUST_SVC_ID_CARD_PHN_KEY = src.CUST_SVC_ID_CARD_PHN_KEY
  AND tgt.CUST_SVC_ID_CARD_PHN_USE_CD = src.CUST_SVC_ID_CARD_PHN_USE_CD

WHEN MATCHED THEN
  UPDATE SET
    tgt.CRT_RUN_CYC_EXCTN_SK = src.CRT_RUN_CYC_EXCTN_SK,
    tgt.CLS_PLN_CUST_SVC_IDCARD_PHN_SK = src.CLS_PLN_CUST_SVC_IDCARD_PHN_SK

WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    GRP_ID,
    SUBGRP_ID,
    CLS_ID,
    CLS_PLN_ID,
    PROD_ID,
    EFF_DT_SK,
    CUST_SVC_ID_CARD_PHN_KEY,
    CUST_SVC_ID_CARD_PHN_USE_CD,
    CRT_RUN_CYC_EXCTN_SK,
    CLS_PLN_CUST_SVC_IDCARD_PHN_SK
  )
  VALUES (
    src.SRC_SYS_CD,
    src.GRP_ID,
    src.SUBGRP_ID,
    src.CLS_ID,
    src.CLS_PLN_ID,
    src.PROD_ID,
    src.EFF_DT_SK,
    src.CUST_SVC_ID_CARD_PHN_KEY,
    src.CUST_SVC_ID_CARD_PHN_USE_CD,
    src.CRT_RUN_CYC_EXCTN_SK,
    src.CLS_PLN_CUST_SVC_IDCARD_PHN_SK
  )
;
"""
execute_dml(merge_sql, jdbc_url_ids_lookup, jdbc_props_ids_lookup)

# --------------------------------------------------------------------------------
# Stage: FacetsBCBSClsPlnIdPhnNoExtr (CSeqFileStage)
#   Input: df_Key
#   Write to a .dat file in directory "key" => use adls_path
# --------------------------------------------------------------------------------

# Final select with correct column order from the "Key" link
df_final = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLS_PLN_CUST_SVC_IDCARD_PHN_SK",
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "EFF_DT_SK",
    "CUST_SVC_ID_CARD_PHN_KEY",
    "CUST_SVC_ID_CARD_PHN_USE_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TERM_DT_SK",
    "CLS_PLN_DTL_PROD_CAT_CD"
)

# Apply rpad for char/varchar columns.
# From the metadata, we see some columns are char with given lengths:
#   INSRT_UPDT_CD (char(10)), DISCARD_IN (char(1)), PASS_THRU_IN(char(1)), ...
#   GRP_ID(char(8)), CLS_ID(char(4)), CLS_PLN_ID(char(8)), PROD_ID(char(8)), EFF_DT_SK(char(10))
#   We'll rpad them to their lengths. Others are varchar with no length specified, we also rpad to safe length (you could adjust as needed).
df_final = df_final \
    .withColumn("INSRT_UPDT_CD", expr("rpad(INSRT_UPDT_CD, 10, ' ')")) \
    .withColumn("DISCARD_IN", expr("rpad(DISCARD_IN, 1, ' ')")) \
    .withColumn("PASS_THRU_IN", expr("rpad(PASS_THRU_IN, 1, ' ')")) \
    .withColumn("GRP_ID", expr("rpad(GRP_ID, 8, ' ')")) \
    .withColumn("CLS_ID", expr("rpad(CLS_ID, 4, ' ')")) \
    .withColumn("CLS_PLN_ID", expr("rpad(CLS_PLN_ID, 8, ' ')")) \
    .withColumn("PROD_ID", expr("rpad(PROD_ID, 8, ' ')")) \
    .withColumn("EFF_DT_SK", expr("rpad(EFF_DT_SK, 10, ' ')"))

# Write the final file
out_path = f"{adls_path}/key/FacetsBCBSClsPlnIdPhnNoExtr.ClsPlnIdPhnNo.dat.{RunID}"
write_files(
    df_final,
    out_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)  

# End of generated PySpark script