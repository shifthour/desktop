# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSCommonClmErrMbrRecycCntl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCBS Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                    2018-03-18              5828                          Original Programming                                                       IntegrateDev2                 Kalyan Neelam           2018-03-19

# MAGIC Added After SQL to update Reversal Records
# MAGIC Added After SQL to update Reversal Records
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameter retrieval
SrcSysCd = get_widget_value('SrcSysCd','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
Logging = get_widget_value('Logging','')
EDWOwner = get_widget_value('EDWOwner','')
RunDate = get_widget_value('RunDate','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')

ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

# Database configs
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# ---------------------------------------------------
# STAGE: IDS_LKP (DB2Connector)
# We create a separate DataFrame per output pin (EXPRNC_CAT_1, FNCL_LOB_1, GRP_1, PROD_1).
# ---------------------------------------------------
query_EXPRNC_CAT_1 = f"""
SELECT 
EXPRNC_CAT_SK,
EXPRNC_CAT_CD,
TRGT_CD  FUND_CAT_CD,
'Y'    as   EXPRNC_IND
FROM {IDSOwner}.EXPRNC_CAT,
     {IDSOwner}.CD_MPPNG
WHERE EXPRNC_CAT_FUND_CAT_CD_SK = CD_MPPNG_SK+0
"""
df_EXPRNC_CAT_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_EXPRNC_CAT_1)
    .load()
)

query_FNCL_LOB_1 = f"""
SELECT 
FNCL_LOB_SK,
FNCL_LOB_CD,
'Y'   as    FNCL_LOB_IND
FROM {IDSOwner}.FNCL_LOB
"""
df_FNCL_LOB_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_FNCL_LOB_1)
    .load()
)

query_GRP_1 = f"""
SELECT 
GRP_SK,
GRP_ID
FROM {IDSOwner}.GRP
"""
df_GRP_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_GRP_1)
    .load()
)

query_PROD_1 = f"""
SELECT 
PROD_SK,
PROD_ID,
PROD.PROD_SH_NM_SK,
CD.TRGT_CD  as  PROD_ST_CD,
PROD_SH_NM
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.PROD_SH_NM SH,
     {IDSOwner}.CD_MPPNG CD 
WHERE 
PROD.PROD_SH_NM_SK = SH.PROD_SH_NM_SK
and PROD.PROD_ST_CD_SK = CD.CD_MPPNG_SK+0
"""
df_PROD_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROD_1)
    .load()
)

# "Hash_1" is an intermediate hashed file stage (Scenario A). We remove duplicates on primary keys for each link.
#   PROD -> PK=PROD_SK, FNCL_LOB -> PK=FNCL_LOB_SK, EXPRNC_CAT -> PK=EXPRNC_CAT_SK, GRP -> PK=GRP_SK.

df_EXPRNC_CAT_1_dedup = dedup_sort(
    df_EXPRNC_CAT_1,
    partition_cols=["EXPRNC_CAT_SK"],
    sort_cols=[]
)
df_FNCL_LOB_1_dedup = dedup_sort(
    df_FNCL_LOB_1,
    partition_cols=["FNCL_LOB_SK"],
    sort_cols=[]
)
df_GRP_1_dedup = dedup_sort(
    df_GRP_1,
    partition_cols=["GRP_SK"],
    sort_cols=[]
)
df_PROD_1_dedup = dedup_sort(
    df_PROD_1,
    partition_cols=["PROD_SK"],
    sort_cols=[]
)

# ---------------------------------------------------
# STAGE: IDS_CLM (DB2Connector)
# ---------------------------------------------------
query_IDS_CLM = f"""
SELECT
CLM.CLM_SK
FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = '{SrcSysCd}'
  AND CLM.MBR_SK = 0
"""
df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_CLM)
    .load()
)

# hf_bcbscommon_recycle_ids_clm: scenario A -> deduplicate on PK=CLM_SK
df_IDS_CLM_dedup = dedup_sort(
    df_IDS_CLM,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

# ---------------------------------------------------
# STAGE: EDW_CLM_F (DB2Connector)
# ---------------------------------------------------
query_EDW_CLM = f"""
SELECT
CLM_SK
FROM {EDWOwner}.CLM_F
WHERE SRC_SYS_CD = '{SrcSysCd}'
  AND MBR_SK = 0
"""
df_EDW_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_EDW_CLM)
    .load()
)

# hf_bcbscommon_recycle_edw_clm_f: scenario A -> deduplicate on PK=CLM_SK
df_EDW_CLM_dedup = dedup_sort(
    df_EDW_CLM,
    partition_cols=["CLM_SK"],
    sort_cols=[]
)

# ---------------------------------------------------
# STAGE: ids (DB2Connector) with multiple output pins => mbr, sub_alpha_pfx, mbr_enroll
# ---------------------------------------------------
query_mbr = f"""
SELECT MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
       SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
"""
df_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_mbr)
    .load()
)

query_sub_alpha_pfx = f"""
SELECT 
SUB_UNIQ_KEY,
ALPHA_PFX_CD
FROM 
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.ALPHA_PFX PFX,
{IDSOwner}.W_DRUG_ENR DRUG 
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
AND MBR.SUB_SK = SUB.SUB_SK 
AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""
df_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_sub_alpha_pfx)
    .load()
)

query_mbr_enroll = f"""
SELECT 
DRUG.CLM_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
SUBGRP.SUBGRP_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
CMPNT.PROD_ID  
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.MBR_ENR                  MBR, 
     {IDSOwner}.CD_MPPNG                MAP1,
     {IDSOwner}.CLS                     CLS, 
     {IDSOwner}.SUBGRP                  SUBGRP,
     {IDSOwner}.CLS_PLN                 PLN, 
     {IDSOwner}.CD_MPPNG               MAP2, 
     {IDSOwner}.PROD_CMPNT           CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT  BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT            CAT,
     {IDSOwner}.FNCL_LOB               LOB 
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('MED')
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND  CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND  MAP2.TRGT_CD= 'PDBL'
AND  DRUG.FILL_DT_SK between  CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND  CMPNT.PROD_CMPNT_EFF_DT_SK= (
     SELECT MAX (CMPNT2.PROD_CMPNT_EFF_DT_SK)
     FROM {IDSOwner}.PROD_CMPNT CMPNT2
     WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
       AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
       AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
)
AND  CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND  DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND  BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND  BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
     SELECT MAX (BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK)
     FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
     WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
       AND CMPNT.PROD_CMPNT_Pfx_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
       AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
       AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
       AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
)
AND  BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
AND  BILL_CMPNT.FNCL_LOB_SK   = LOB.FNCL_LOB_SK
"""
df_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_mbr_enroll)
    .load()
)

# "Hash" stage is scenario A. Dedup each link by its primary key (mbr => MBR_UNIQ_KEY, sub_alpha_pfx => SUB_UNIQ_KEY, mbr_enroll => CLM_ID).
df_mbr_dedup = dedup_sort(
    df_mbr,
    partition_cols=["MBR_UNIQ_KEY"],
    sort_cols=[]
)
df_sub_alpha_pfx_dedup = dedup_sort(
    df_sub_alpha_pfx,
    partition_cols=["SUB_UNIQ_KEY"],
    sort_cols=[]
)
df_mbr_enroll_dedup = dedup_sort(
    df_mbr_enroll,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

# ---------------------------------------------------
# STAGE: BCBSCommonClmLanding (CSeqFileStage) reading a .dat file
# ---------------------------------------------------
# The schema must match the columns in the stage's output pin, in the same order.
schema_BCBSSC = T.StructType([
    T.StructField("CLM_SK", T.IntegerType(), False),
    T.StructField("CLM_ID", T.StringType(), False),
    T.StructField("SRC_SYS_CD", T.StringType(), False),
    T.StructField("CLM_TYP_CD", T.StringType(), False),
    T.StructField("CLM_SUBTYP_CD", T.StringType(), False),
    T.StructField("CLM_SVC_STRT_DT_SK", T.StringType(), False),
    T.StructField("SRC_SYS_GRP_PFX", T.StringType(), False),
    T.StructField("SRC_SYS_GRP_ID", T.StringType(), False),
    T.StructField("SRC_SYS_GRP_SFX", T.StringType(), False),
    T.StructField("SUB_SSN", T.StringType(), False),
    T.StructField("PATN_LAST_NM", T.StringType(), False),
    T.StructField("PATN_FIRST_NM", T.StringType(), False),
    T.StructField("PATN_GNDR_CD", T.StringType(), False),
    T.StructField("PATN_BRTH_DT_SK", T.StringType(), False),
    T.StructField("MBR_UNIQ_KEY", T.IntegerType(), False),
    T.StructField("GRP_ID", T.StringType(), False),
    T.StructField("SUB_ID", T.StringType(), False),
    T.StructField("MBR_SFX_NO", T.StringType(), True),
    T.StructField("SUB_UNIQ_KEY", T.IntegerType(), False),
    T.StructField("EFF_DT_SK", T.StringType(), False)
])

filePath_BCBSSC = f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}"
df_BCBSSC = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_BCBSSC)
    .load(filePath_BCBSSC)
)

# ---------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
# Primary link: df_BCBSSC
# Lookup link: df_mbr_dedup (left join on BCBSSC.MBR_UNIQ_KEY = mbr_dedup.MBR_UNIQ_KEY)
# Stage variable: svSubCk = if isnull(mbr_dedup.MBR_UNIQ_KEY) then df_BCBSSC.SUB_UNIQ_KEY else mbr_dedup.SUB_UNIQ_KEY
# Output columns => BCBSCommonClmTrns
# ---------------------------------------------------
df_BusinessRules_join = df_BCBSSC.alias("BCBSSC").join(
    df_mbr_dedup.alias("mbr_lkup"),
    on=(F.col("BCBSSC.MBR_UNIQ_KEY") == F.col("mbr_lkup.MBR_UNIQ_KEY")),
    how="left"
)

df_BusinessRules = df_BusinessRules_join.withColumn(
    "svSubCk",
    F.when(F.isnull(F.col("mbr_lkup.MBR_UNIQ_KEY")), F.col("BCBSSC.SUB_UNIQ_KEY")).otherwise(F.col("mbr_lkup.SUB_UNIQ_KEY"))
)

df_BCBSCommonClmTrns = df_BusinessRules.select(
    F.col("BCBSSC.CLM_SK").alias("CLM_SK"),
    F.col("BCBSSC.CLM_ID").alias("CLM_ID"),
    F.col("svSubCk").alias("SUB_CK"),
    F.col("BCBSSC.SUB_ID").alias("SUB_ID"),
    F.col("BCBSSC.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("BCBSSC.GRP_ID").alias("GRP"),
    F.col("BCBSSC.MBR_UNIQ_KEY").alias("MBR_CK"),
    F.col("BCBSSC.MBR_SFX_NO").alias("MBR_SFX_NO"),
)

# ---------------------------------------------------
# STAGE: ids_subgrp (DB2Connector) with two output pins => subgrp_med, subgrp_dntl
# ---------------------------------------------------
query_subgrp_med = f"""
SELECT 
CLM_ID,
SUBGRP_ID,
CLS_ID,
CLS_PLN_ID,
PROD_ID
FROM 
(
  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  1 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
  and MBR.ELIG_IN = 'Y'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('MED')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  2 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and MBR.ELIG_IN = 'Y'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('MED')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  3 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
  and MBR.ELIG_IN = 'N'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('MED')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  4 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and MBR.ELIG_IN = 'N'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('MED')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK
)
ORDER BY CLM_ID, "Order" DESC
"""

df_subgrp_med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_subgrp_med)
    .load()
)

query_subgrp_dntl = f"""
SELECT 
CLM_ID,
SUBGRP_ID,
CLS_ID,
CLS_PLN_ID,
PROD_ID
FROM 
(
  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  1 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
  and MBR.ELIG_IN = 'Y'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('DNTL')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  2 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and MBR.ELIG_IN = 'Y'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('DNTL')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  3 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
  and MBR.ELIG_IN = 'N'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('DNTL')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK

  UNION

  SELECT 
  DRUG.CLM_ID,
  SUBGRP.SUBGRP_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  CMPNT.PROD_ID,
  4 as Order
  FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.MBR_ENR MBR, 
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.CLS CLS, 
  {IDSOwner}.CLS_PLN PLN, 
  {IDSOwner}.PROD_CMPNT CMPNT
  WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  and MBR.ELIG_IN = 'N'
  and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  and MAP1.TRGT_CD IN ('DNTL')
  and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  and MBR.CLS_SK = CLS.CLS_SK
  and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK
)
ORDER BY CLM_ID, "Order" DESC
"""

df_subgrp_dntl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_subgrp_dntl)
    .load()
)

# hf_subgrp = scenario A => deduplicate on PK=CLM_ID for both subgrp_med & subgrp_dntl
df_subgrp_med_dedup = dedup_sort(
    df_subgrp_med,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)
df_subgrp_dntl_dedup = dedup_sort(
    df_subgrp_dntl,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

# ---------------------------------------------------
# STAGE: alpha_pfx (CTransformerStage)
# Primary link => df_BCBSCommonClmTrns
# Lookup links => sub_alpha_pfx_lkup => df_sub_alpha_pfx_dedup (left join on SUB_CK=SUB_UNIQ_KEY)
#               => mbr_enr_lkup => df_mbr_enroll_dedup (left join on CLM_ID=CLM_ID)
#               => subgrpdntl => df_subgrp_dntl_dedup (left join on CLM_ID=CLM_ID)
#               => subgrpmed => df_subgrp_med_dedup (left join on CLM_ID=CLM_ID)
# Output => ClmCrfIn1
# ---------------------------------------------------
df_alpha_pfx_0 = df_BCBSCommonClmTrns.alias("BCBSCommonClmTrns")

df_alpha_pfx_1 = df_alpha_pfx_0.join(
    df_sub_alpha_pfx_dedup.alias("sub_alpha_pfx_lkup"),
    on=(F.col("BCBSCommonClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")),
    how="left"
)

df_alpha_pfx_2 = df_alpha_pfx_1.join(
    df_mbr_enroll_dedup.alias("mbr_enr_lkup"),
    on=(F.col("BCBSCommonClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID")),
    how="left"
)

df_alpha_pfx_3 = df_alpha_pfx_2.join(
    df_subgrp_dntl_dedup.alias("subgrpdntl"),
    on=(F.col("BCBSCommonClmTrns.CLM_ID") == F.col("subgrpdntl.CLM_ID")),
    how="left"
)

df_alpha_pfx_4 = df_alpha_pfx_3.join(
    df_subgrp_med_dedup.alias("subgrpmed"),
    on=(F.col("BCBSCommonClmTrns.CLM_ID") == F.col("subgrpmed.CLM_ID")),
    how="left"
)

df_ClmCrfIn1 = df_alpha_pfx_4.select(
    F.col("BCBSCommonClmTrns.CLM_SK").alias("CLM_SK"),
    F.col("BCBSCommonClmTrns.CLM_ID").alias("CLM_ID"),
    F.when(F.isnull(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")), F.lit("UNK")).otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")).alias("ALPHA_PFX_CD"),
    F.when(F.isnull(F.col("subgrpmed.CLM_ID")),
           F.when(F.isnull(F.col("subgrpdntl.CLM_ID")), F.lit("UNK")).otherwise(F.col("subgrpdntl.CLS_ID"))
          ).otherwise(F.col("subgrpmed.CLS_ID")).alias("CLS"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK")).otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")).alias("CLS_PLN"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK")).otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")).alias("EXPRNC_CAT"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK")).otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")).alias("FNCL_LOB_NO"),
    F.col("BCBSCommonClmTrns.GRP").alias("GRP"),
    F.col("BCBSCommonClmTrns.MBR_CK").alias("MBR_CK"),
    F.when(F.isnull(F.col("mbr_enr_lkup.CLM_ID")), F.lit("UNK")).otherwise(trim(F.col("mbr_enr_lkup.PROD_ID"))).alias("PROD"),
    F.when(F.isnull(F.col("subgrpmed.CLM_ID")),
           F.when(F.isnull(F.col("subgrpdntl.CLM_ID")), F.lit("UNK")).otherwise(F.col("subgrpdntl.SUBGRP_ID"))
          ).otherwise(F.col("subgrpmed.SUBGRP_ID")).alias("SUBGRP"),
    F.col("BCBSCommonClmTrns.SUB_CK").alias("SUB_CK"),
    F.col("BCBSCommonClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("BCBSCommonClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("BCBSCommonClmTrns.SUB_ID").alias("SUB_ID")
)

# ---------------------------------------------------
# STAGE: Trns1 (CTransformerStage)
# Primary link => df_ClmCrfIn1
# Lookup link => df_IDS_CLM_dedup on (ClmCrfIn1.CLM_SK=IDS_CLM.CLM_SK), 'left'
# StageVariables => handle multiple calls to user-defined functions, produce final columns for 3 outputs:
#   1) ClmFkeyOut1 -> "CLM_Update" with constraint IsNull(IDS_CLM.CLM_SK) = FALSE
#   2) Lnk_PClmMtch -> "Seq_PClmMtchSk"
#   3) Lnk_IDS_CLM -> "Trn_CLM"
# ---------------------------------------------------
df_Trns1_join = df_ClmCrfIn1.alias("ClmCrfIn1").join(
    df_IDS_CLM_dedup.alias("IDS_CLM"),
    on=(F.col("ClmCrfIn1.CLM_SK") == F.col("IDS_CLM.CLM_SK")),
    how="left"
)

# Stage variables (expressions). We replicate them by direct calls in a select. 
# We'll interpret them as needed in the final columns. 
# Their logic is about calls to "GetFkey..." which are assumed to be user-defined functions returning integer SKs, etc.
# We also have "SrcSysCd" logic. We'll keep them exactly as expressions that produce final columns. 

# We'll define the transformations:
df_Trns1_vars = df_Trns1_join.withColumn(
    "SrcSysCdMbr",
    F.when(
        F.isnull(F.lit(SrcSysCd)).__or__(F.length(trim(F.lit(SrcSysCd))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.when(trim(F.lit(SrcSysCd)).isin("PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"), F.lit("FACETS")).otherwise(F.lit(SrcSysCd))
    )
).withColumn(
    "SrcSysCdProd",
    F.when(
        F.isnull(F.lit(SrcSysCd)).__or__(F.length(trim(F.lit(SrcSysCd))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.when(trim(F.lit(SrcSysCd)).isin("PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","ESI","OPTUMRX","MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"), F.lit("FACETS")).otherwise(F.lit(SrcSysCd))
    )
).withColumn(
    "ClsPlnSk",
    GetFkeyClsPln(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.CLS_PLN"), F.lit(Logging))
).withColumn(
    "ClsSk",
    GetFkeyCls(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.col("ClmCrfIn1.CLS"), F.lit(Logging))
).withColumn(
    "ExpCatCdSk",
    F.when(
        F.col("SrcSysCd") == F.lit("NPS"),
        GetFkeyExprncCat(F.lit("FACETS"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.EXPRNC_CAT"), F.lit(Logging))
    ).otherwise(
        GetFkeyExprncCat(F.col("SrcSysCdProd"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.EXPRNC_CAT"), F.lit(Logging))
    )
).withColumn(
    "GrpSk",
    GetFkeyGrp(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.lit(Logging))
).withColumn(
    "MbrSk",
    GetFkeyMbr(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.MBR_CK"), F.lit(Logging))
).withColumn(
    "ProdSk",
    GetFkeyProd(F.col("SrcSysCdProd"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.PROD"), F.lit(Logging))
).withColumn(
    "SubGrpSk",
    GetFkeySubgrp(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.GRP"), F.col("ClmCrfIn1.SUBGRP"), F.lit(Logging))
).withColumn(
    "SubSk",
    GetFkeySub(F.col("SrcSysCdMbr"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.SUB_CK"), F.lit(Logging))
).withColumn(
    "PlnAlphPfxSk",
    GetFkeyAlphaPfx(F.lit("FACETS"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.ALPHA_PFX_CD"), F.lit(Logging))
).withColumn(
    "FinancialLOB",
    GetFkeyFnclLob(F.lit("PSI"), F.col("ClmCrfIn1.CLM_SK"), F.col("ClmCrfIn1.FNCL_LOB_NO"), F.lit(Logging))
)

# We now split to produce outputs:

df_ClmFkeyOut1_pre = df_Trns1_vars.filter(F.col("IDS_CLM.CLM_SK").isNotNull()).select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PlnAlphPfxSk").alias("ALPHA_PFX_SK"),
    F.col("ClsSk").alias("CLS_SK"),
    F.col("ClsPlnSk").alias("CLS_PLN_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FinancialLOB").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.col("ProdSk").alias("PROD_SK"),
    F.col("SubGrpSk").alias("SUBGRP_SK"),
    F.col("SubSk").alias("SUB_SK"),
    F.col("ClmCrfIn1.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("ClmCrfIn1.SUB_ID").alias("SUB_ID")
)

# Apply rpad for char columns (MBR_SFX_NO=>char(2), SUB_ID=>char(14)) in correct order:
df_ClmFkeyOut1 = (
    df_ClmFkeyOut1_pre
    .withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"), 2, " "))
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), 14, " "))
    .select(
        "CLM_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALPHA_PFX_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "EXPRNC_CAT_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "MBR_SK",
        "PROD_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "MBR_SFX_NO",
        "SUB_ID"
    )
)

df_Lnk_PClmMtch = df_Trns1_vars.select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK")
)

df_Lnk_IDS_CLM = df_Trns1_vars.select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PlnAlphPfxSk").alias("ALPHA_PFX_SK"),
    F.col("ClsSk").alias("CLS_SK"),
    F.col("ClsPlnSk").alias("CLS_PLN_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FinancialLOB").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.col("ProdSk").alias("PROD_SK"),
    F.col("SubGrpSk").alias("SUBGRP_SK"),
    F.col("SubSk").alias("SUB_SK"),
    F.rpad(F.col("ClmCrfIn1.MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.rpad(F.col("ClmCrfIn1.SUB_ID"), 14, " ").alias("SUB_ID")
)

# ---------------------------------------------------
# STAGE: CLM_Update (DB2Connector) - Merge into #$IDSOwner#.CLM
# We'll first create a staging table "STAGING.BCBSCOMMONERRCLMMBRUPD_CLM_UPDATE_temp" and then do the MERGE.
# ---------------------------------------------------
temp_table_CLM_Update = "STAGING.BCBSCOMMONERRCLMMBRUPD_CLM_UPDATE_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_Update}", jdbc_url_ids, jdbc_props_ids)

(
    df_ClmFkeyOut1
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_CLM_Update)
    .mode("overwrite")
    .save()
)

merge_sql_CLM_Update = f"""MERGE INTO {IDSOwner}.CLM AS CLM

USING
(
SELECT 
CLM.CLM_SK,
CLM1.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM1.ALPHA_PFX_SK,
CLM1.CLS_SK,
CLM1.CLS_PLN_SK,
CLM1.EXPRNC_CAT_SK,
CLM1.FNCL_LOB_SK,
CLM1.GRP_SK,
CLM1.MBR_SK,
CLM1.PROD_SK,
CLM1.SUBGRP_SK,
CLM1.SUB_SK,
CLM1.MBR_SFX_NO,
CLM1.SUB_ID

FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD,
     {IDSOwner}.CD_MPPNG AS CD1,
     {IDSOwner}.CD_MPPNG AS CD2,
     (
      SELECT 
      CLM.CLM_ID,
      CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
      CLM.ALPHA_PFX_SK,
      CLM.CLS_SK,
      CLM.CLS_PLN_SK,
      CLM.EXPRNC_CAT_SK,
      CLM.FNCL_LOB_SK,
      CLM.GRP_SK,
      CLM.MBR_SK,
      CLM.PROD_SK,
      CLM.SUBGRP_SK,
      CLM.SUB_SK,
      CLM.MBR_SFX_NO,
      CLM.SUB_ID
      
      FROM {IDSOwner}.CLM CLM,
           {IDSOwner}.CD_MPPNG CD,
           {IDSOwner}.CD_MPPNG CD1,
           {IDSOwner}.CD_MPPNG CD2
      
      WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
       AND CD.TRGT_CD = '{SrcSysCd}'
       AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
       AND CD1.TRGT_CD = 'A09'
       AND CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
       AND CD2.TRGT_CD = 'RX'
       AND CLM.MBR_SK <> 0
     ) AS CLM1

WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = '{SrcSysCd}'
  AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
  AND CD1.TRGT_CD = 'A08'
  AND CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
  AND CD2.TRGT_CD = 'RX'
  AND CLM.MBR_SK = 0
  AND REPLACE(CLM.CLM_ID,'R','') = CLM1.CLM_ID
) AS CLM1

ON CLM.CLM_SK = CLM1.CLM_SK

WHEN MATCHED THEN

UPDATE SET CLM.LAST_UPDT_RUN_CYC_EXCTN_SK = CLM1.LAST_UPDT_RUN_CYC_EXCTN_SK,
           CLM.ALPHA_PFX_SK = CLM1.ALPHA_PFX_SK,
           CLM.CLS_SK = CLM1.CLS_SK,
           CLM.CLS_PLN_SK = CLM1.CLS_PLN_SK,
           CLM.EXPRNC_CAT_SK = CLM1.EXPRNC_CAT_SK,
           CLM.FNCL_LOB_SK = CLM1.FNCL_LOB_SK,
           CLM.GRP_SK = CLM1.GRP_SK,
           CLM.MBR_SK = CLM1.MBR_SK,
           CLM.PROD_SK = CLM1.PROD_SK,
           CLM.SUBGRP_SK = CLM1.SUBGRP_SK,
           CLM.SUB_SK = CLM1.SUB_SK,
           CLM.MBR_SFX_NO = CLM1.MBR_SFX_NO,
           CLM.SUB_ID = CLM1.SUB_ID;
"""
execute_dml(merge_sql_CLM_Update, jdbc_url_ids, jdbc_props_ids)

# ---------------------------------------------------
# STAGE: Seq_PClmMtchSk (CSeqFileStage) writing #SrcSysCd1#_PClmMbrErrRecyc_Delete.dat
# Input: df_Lnk_PClmMtch
# ---------------------------------------------------
df_seq_PClmMtchSk = df_Lnk_PClmMtch.select("CLM_SK")

filePath_PClmMtch = f"{adls_path}/verified/{SrcSysCd1}_PClmMbrErrRecyc_Delete.dat"
write_files(
    df_seq_PClmMtchSk,
    filePath_PClmMtch,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ---------------------------------------------------
# STAGE: Trn_CLM (CTransformerStage)
# Primary link => df_Lnk_IDS_CLM
# Lookup links => df_PROD_1_dedup => left on (Lnk_IDS_CLM.PROD_SK=PROD_1_dedup.PROD_SK),
#                 df_FNCL_LOB_1_dedup => left on (Lnk_IDS_CLM.FNCL_LOB_SK=FNCL_LOB_1_dedup.FNCL_LOB_SK),
#                 df_EXPRNC_CAT_1_dedup => left on (Lnk_IDS_CLM.EXPRNC_CAT_SK=EXPRNC_CAT_1_dedup.EXPRNC_CAT_SK),
#                 df_GRP_1_dedup => left on (Lnk_IDS_CLM.GRP_SK=GRP_1_dedup.GRP_SK),
#                 df_EDW_CLM_dedup => left on (Lnk_IDS_CLM.CLM_SK=EDW_CLM_dedup.CLM_SK).
# Output => Lnk_EDW_CLM with constraint IsNull(EDW_CLM.CLM_SK)=false
# ---------------------------------------------------
df_trn_clm_0 = df_Lnk_IDS_CLM.alias("Lnk_IDS_CLM").join(
    df_PROD_1_dedup.alias("PROD"),
    on=(F.col("Lnk_IDS_CLM.PROD_SK") == F.col("PROD.PROD_SK")),
    how="left"
).join(
    df_FNCL_LOB_1_dedup.alias("FNCL_LOB"),
    on=(F.col("Lnk_IDS_CLM.FNCL_LOB_SK") == F.col("FNCL_LOB.FNCL_LOB_SK")),
    how="left"
).join(
    df_EXPRNC_CAT_1_dedup.alias("EXPRNC_CAT"),
    on=(F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK") == F.col("EXPRNC_CAT.EXPRNC_CAT_SK")),
    how="left"
).join(
    df_GRP_1_dedup.alias("GRP"),
    on=(F.col("Lnk_IDS_CLM.GRP_SK") == F.col("GRP.GRP_SK")),
    how="left"
).join(
    df_EDW_CLM_dedup.alias("EDW_CLM"),
    on=(F.col("Lnk_IDS_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK")),
    how="left"
)

df_Lnk_EDW_CLM_pre = df_trn_clm_0.filter(F.col("EDW_CLM.CLM_SK").isNotNull()).select(
    F.col("Lnk_IDS_CLM.CLM_SK").alias("CLM_SK"),
    F.rpad(F.lit(RunDate),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_IDS_CLM.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IDS_CLM.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.col("Lnk_IDS_CLM.CLS_SK").alias("CLS_SK"),
    F.col("Lnk_IDS_CLM.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
    F.col("Lnk_IDS_CLM.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("Lnk_IDS_CLM.GRP_SK").alias("GRP_SK"),
    F.col("Lnk_IDS_CLM.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_IDS_CLM.PROD_SK").alias("PROD_SK"),
    F.col("Lnk_IDS_CLM.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("Lnk_IDS_CLM.SUB_SK").alias("SUB_SK"),
    F.col("Lnk_IDS_CLM.MBR_SFX_NO").alias("CLM_MBR_SFX_NO"),
    F.when(
        F.isnull(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")).__or__(F.trim(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")) == ""),
        F.lit("NA")
    ).otherwise(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")).alias("EXPRNC_CAT_CD"),
    F.when(
        F.isnull(F.col("EXPRNC_CAT.FUND_CAT_CD")).__or__(F.trim(F.col("EXPRNC_CAT.FUND_CAT_CD")) == ""),
        F.lit("NA")
    ).otherwise(F.col("EXPRNC_CAT.FUND_CAT_CD")).alias("FUND_CAT_CD"),
    F.when(
        F.isnull(F.col("FNCL_LOB.FNCL_LOB_CD")).__or__(F.trim(F.col("FNCL_LOB.FNCL_LOB_CD")) == ""),
        F.lit("0000")
    ).otherwise(F.col("FNCL_LOB.FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
    F.when(
        F.isnull(F.col("GRP.GRP_ID")).__or__(F.trim(F.col("GRP.GRP_ID")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("GRP.GRP_ID")).alias("GRP_ID"),
    F.when(F.isnull(F.col("PROD.PROD_SH_NM_SK")), F.lit(0)).otherwise(F.col("PROD.PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
    F.when(
        F.isnull(F.col("PROD.PROD_SH_NM")).__or__(F.trim(F.col("PROD.PROD_SH_NM"))==""),
        F.lit(" ")
    ).otherwise(F.col("PROD.PROD_SH_NM")).alias("PROD_SH_NM"),
    F.when(
        F.isnull(F.col("PROD.PROD_ST_CD")).__or__(F.trim(F.col("PROD.PROD_ST_CD"))==""),
        F.lit("NA")
    ).otherwise(F.col("PROD.PROD_ST_CD")).alias("PROD_ST_CD"),
    F.lit("N").alias("CLM_MEDIGAP_IN")
)

# Some columns are char(...) => we apply rpad if specified in the stage definition:
# "CLM_MEDIGAP_IN" => char(1)
df_Lnk_EDW_CLM = df_Lnk_EDW_CLM_pre.withColumn(
    "CLM_MEDIGAP_IN",
    F.rpad("CLM_MEDIGAP_IN", 1, " ")
)

# ---------------------------------------------------
# STAGE: EDW_CLM_Update (DB2Connector) - Merge into #$EDWOwner#.CLM_F
# We create temp table STAGING.BCBSCOMMONERRCLMMBRUPD_EDW_CLM_UPDATE_temp, then MERGE
# ---------------------------------------------------
temp_table_EDW_CLM_Update = "STAGING.BCBSCOMMONERRCLMMBRUPD_EDW_CLM_UPDATE_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_EDW_CLM_Update}", jdbc_url_edw, jdbc_props_edw)

(
    df_Lnk_EDW_CLM
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", temp_table_EDW_CLM_Update)
    .mode("overwrite")
    .save()
)

merge_sql_EDW_CLM_Update = f"""MERGE INTO {EDWOwner}.CLM_F AS CLM

USING
(
SELECT 
CLM.CLM_SK,
CLM1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
CLM1.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM1.ALPHA_PFX_SK,
CLM1.CLS_SK,
CLM1.CLS_PLN_SK,
CLM1.EXPRNC_CAT_SK,
CLM1.FNCL_LOB_SK,
CLM1.GRP_SK,
CLM1.MBR_SK,
CLM1.PROD_SK,
CLM1.SUBGRP_SK,
CLM1.SUB_SK,
CLM1.CLM_MBR_SFX_NO,
CLM1.EXPRNC_CAT_CD,
CLM1.FUND_CAT_CD,
CLM1.FNCL_LOB_CD,
CLM1.GRP_ID,
CLM1.PROD_SH_NM_SK,
CLM1.PROD_SH_NM,
CLM1.PROD_ST_CD,
CLM1.CLM_MEDIGAP_IN

FROM {EDWOwner}.CLM_F AS CLM,
     (
      SELECT 
      CLM_ID,
      LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
      LAST_UPDT_RUN_CYC_EXCTN_SK,
      ALPHA_PFX_SK,
      CLS_SK,
      CLS_PLN_SK,
      EXPRNC_CAT_SK,
      FNCL_LOB_SK,
      GRP_SK,
      MBR_SK,
      PROD_SK,
      SUBGRP_SK,
      SUB_SK,
      CLM_MBR_SFX_NO,
      EXPRNC_CAT_CD,
      FUND_CAT_CD,
      FNCL_LOB_CD,
      GRP_ID,
      PROD_SH_NM_SK,
      PROD_SH_NM,
      PROD_ST_CD,
      CLM_MEDIGAP_IN
      FROM {EDWOwner}.CLM_F
      WHERE SRC_SYS_CD = '{SrcSysCd}'
        AND CLM_STTUS_CD = 'A09'
        AND CLM_SUBTYP_CD = 'RX'
        AND MBR_SK <> 0
     ) AS CLM1

WHERE CLM.SRC_SYS_CD = '{SrcSysCd}'
  AND CLM.CLM_STTUS_CD = 'A08'
  AND CLM.CLM_SUBTYP_CD = 'RX'
  AND CLM.MBR_SK = 0
  AND REPLACE(CLM.CLM_ID,'R','') = CLM1.CLM_ID
) AS CLM1

ON CLM.CLM_SK = CLM1.CLM_SK

WHEN MATCHED THEN

UPDATE SET CLM.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = CLM1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
           CLM.LAST_UPDT_RUN_CYC_EXCTN_SK = CLM1.LAST_UPDT_RUN_CYC_EXCTN_SK,
           CLM.ALPHA_PFX_SK = CLM1.ALPHA_PFX_SK,
           CLM.CLS_SK = CLM1.CLS_SK,
           CLM.CLS_PLN_SK = CLM1.CLS_PLN_SK,
           CLM.EXPRNC_CAT_SK = CLM1.EXPRNC_CAT_SK,
           CLM.FNCL_LOB_SK = CLM1.FNCL_LOB_SK,
           CLM.GRP_SK = CLM1.GRP_SK,
           CLM.MBR_SK = CLM1.MBR_SK,
           CLM.PROD_SK = CLM1.PROD_SK,
           CLM.SUBGRP_SK = CLM1.SUBGRP_SK,
           CLM.SUB_SK = CLM1.SUB_SK,
           CLM.CLM_MBR_SFX_NO = CLM1.CLM_MBR_SFX_NO,
           CLM.EXPRNC_CAT_CD = CLM1.EXPRNC_CAT_CD,
           CLM.FUND_CAT_CD = CLM1.FUND_CAT_CD,
           CLM.FNCL_LOB_CD = CLM1.FNCL_LOB_CD,
           CLM.GRP_ID = CLM1.GRP_ID,
           CLM.PROD_SH_NM_SK = CLM1.PROD_SH_NM_SK,
           CLM.PROD_SH_NM = CLM1.PROD_SH_NM,
           CLM.PROD_ST_CD = CLM1.PROD_ST_CD,
           CLM.CLM_MEDIGAP_IN = CLM1.CLM_MEDIGAP_IN;
"""
execute_dml(merge_sql_EDW_CLM_Update, jdbc_url_edw, jdbc_props_edw)

# ---------------------------------------------------
# End of job.
# ---------------------------------------------------