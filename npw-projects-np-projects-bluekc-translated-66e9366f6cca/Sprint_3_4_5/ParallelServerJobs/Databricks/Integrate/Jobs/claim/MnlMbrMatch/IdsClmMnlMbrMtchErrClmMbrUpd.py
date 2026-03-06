# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  Update CLM table based on landing file generated in Extr job
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                    2018-09-28             5828                          Original Programming                                                       IntegrateDev2

# MAGIC Added After SQL to update Reversal Records
# MAGIC Added After SQL to update Reversal Records
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
    BooleanType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IncludeSrcSysCd = get_widget_value('IncludeSrcSysCd','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
Logging = get_widget_value('Logging','')
RunDate = get_widget_value('RunDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# ------------------------------------------------------------------------------------------------
# STAGE: IDS_LKP (DB2Connector) - Database = IDS
# ------------------------------------------------------------------------------------------------

query_EXPRNC_CAT_1 = f"""
SELECT 
 EXPRNC_CAT_SK,
 EXPRNC_CAT_CD,
 TRGT_CD  FUND_CAT_CD,
 'Y' as EXPRNC_IND
FROM {IDSOwner}.EXPRNC_CAT,
     {IDSOwner}.CD_MPPNG
WHERE EXPRNC_CAT_FUND_CAT_CD_SK = CD_MPPNG_SK+0
"""

df_IDS_LKP_EXPRNC_CAT_1 = (
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
 'Y' as FNCL_LOB_IND
FROM {IDSOwner}.FNCL_LOB
"""

df_IDS_LKP_FNCL_LOB_1 = (
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

df_IDS_LKP_GRP_1 = (
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
 CD.TRGT_CD as PROD_ST_CD,
 PROD_SH_NM
FROM {IDSOwner}.PROD PROD,
     {IDSOwner}.PROD_SH_NM SH,
     {IDSOwner}.CD_MPPNG CD
WHERE 
 PROD.PROD_SH_NM_SK = SH.PROD_SH_NM_SK
 and PROD.PROD_ST_CD_SK = CD.CD_MPPNG_SK+0
"""

df_IDS_LKP_PROD_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROD_1)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: Hash_1 (CHashedFileStage) - Scenario A
# Deduplicate each input DataFrame on the primary key columns before feeding to next stage
# ------------------------------------------------------------------------------------------------

df_hash_1_prod = df_IDS_LKP_PROD_1.dropDuplicates(["PROD_SK"])
df_hash_1_fncl_lob = df_IDS_LKP_FNCL_LOB_1.dropDuplicates(["FNCL_LOB_SK"])
df_hash_1_exprnc_cat = df_IDS_LKP_EXPRNC_CAT_1.dropDuplicates(["EXPRNC_CAT_SK"])
df_hash_1_grp = df_IDS_LKP_GRP_1.dropDuplicates(["GRP_SK"])

# ------------------------------------------------------------------------------------------------
# STAGE: IDS_CLM (DB2Connector) - Database = IDS
# ------------------------------------------------------------------------------------------------

query_IDS_CLM = f"""
SELECT
 CLM.CLM_SK
FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD IN ('{IncludeSrcSysCd}')
  AND CLM.MBR_SK = 0
"""

df_IDS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_CLM)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: hf_mnlmbrmtch_recycle_ids_clm (CHashedFileStage) - Scenario A
# ------------------------------------------------------------------------------------------------

df_hf_mnlmbrmtch_recycle_ids_clm = df_IDS_CLM.dropDuplicates(["CLM_SK"])

# ------------------------------------------------------------------------------------------------
# STAGE: EDW_CLM_F (DB2Connector) - Database = EDW
# ------------------------------------------------------------------------------------------------

query_EDW_CLM_F = f"""
SELECT
 CLM_SK
FROM {EDWOwner}.CLM_F
WHERE SRC_SYS_CD in ('{IncludeSrcSysCd}')
  AND MBR_SK = 0
"""

df_EDW_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_EDW_CLM_F)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: hf_mnlmbrmtch_recycle_edw_clm_f (CHashedFileStage) - Scenario A
# ------------------------------------------------------------------------------------------------

df_hf_mnlmbrmtch_recycle_edw_clm_f = df_EDW_CLM_F.dropDuplicates(["CLM_SK"])

# ------------------------------------------------------------------------------------------------
# STAGE: ids (DB2Connector) - Database = IDS
# Three output pins => mbr, sub_alpha_pfx, mbr_enroll
# ------------------------------------------------------------------------------------------------

query_mbr = f"""
SELECT MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
       SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
"""

df_ids_mbr = (
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

df_ids_sub_alpha_pfx = (
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
     {IDSOwner}.MBR_ENR MBR,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CLS CLS,
     {IDSOwner}.SUBGRP SUBGRP,
     {IDSOwner}.CLS_PLN PLN,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.PROD_CMPNT CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT CAT,
     {IDSOwner}.FNCL_LOB LOB
WHERE 
 DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
 and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
 and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
 and MAP1.TRGT_CD IN ('MED')
 and MBR.CLS_SK = CLS.CLS_SK
 and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
 and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
 AND MBR.PROD_SK = CMPNT.PROD_SK
 AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
 AND MAP2.TRGT_CD= 'PDBL'
 AND DRUG.FILL_DT_SK between CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
 AND CMPNT.PROD_CMPNT_EFF_DT_SK= (
   SELECT MAX (CMPNT2.PROD_CMPNT_EFF_DT_SK )
   FROM {IDSOwner}.PROD_CMPNT CMPNT2
   WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
     AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
     AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
 )
 AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
 AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
 AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
 AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
   SELECT MAX (BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK )
   FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
   WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
     AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
     AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
     AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
     AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
 )
 AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
 AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""

df_ids_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_mbr_enroll)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: Hash (CHashedFileStage) - Scenario A
# Deduplicate each input DataFrame on the primary key columns
# ------------------------------------------------------------------------------------------------

df_hash_mbr_lkup = df_ids_mbr.dropDuplicates(["MBR_UNIQ_KEY"])
df_hash_sub_alpha_pfx_lkup = df_ids_sub_alpha_pfx.dropDuplicates(["SUB_UNIQ_KEY"])
df_hash_mbr_enr_lkup = df_ids_mbr_enroll.dropDuplicates(["CLM_ID"])

# ------------------------------------------------------------------------------------------------
# STAGE: MnlMbrMtchClmLanding (CSeqFileStage) - reading from verified folder
# ------------------------------------------------------------------------------------------------

file_schema_mnlmbr = StructType([
    StructField("CLM_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CLM_TYP_CD", StringType(), False),
    StructField("CLM_SUBTYP_CD", StringType(), False),
    StructField("CLM_SVC_STRT_DT_SK", StringType(), False),
    StructField("SRC_SYS_GRP_PFX", StringType(), False),
    StructField("SRC_SYS_GRP_ID", StringType(), False),
    StructField("SRC_SYS_GRP_SFX", StringType(), False),
    StructField("SUB_SSN", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_GNDR_CD", StringType(), False),
    StructField("PATN_BRTH_DT_SK", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("EFF_DT_SK", StringType(), False)
])

df_MnlMbrMtchClmLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")  # Not explicitly stated, default to '|' to avoid collisions
    .option("quote", "\"")
    .schema(file_schema_mnlmbr)
    .load(f"{adls_path}/verified/Mnl_Mbr_Match_ErrClm_Landing.dat.{RunID}")
)

# ------------------------------------------------------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
# Primary link: BCBSSC => df_MnlMbrMtchClmLanding
# Lookup link: mbr_lkup => df_hash_mbr_lkup
# left join on BCBSSC.MBR_UNIQ_KEY = mbr_lkup.MBR_UNIQ_KEY
# Stage var: svSubCk
# Output pin: BCBSCommonClmTrns
# ------------------------------------------------------------------------------------------------

df_BusinessRules_lk = df_MnlMbrMtchClmLanding.alias("BCBSSC").join(
    df_hash_mbr_lkup.alias("mbr_lkup"),
    F.col("BCBSSC.MBR_UNIQ_KEY") == F.col("mbr_lkup.MBR_UNIQ_KEY"),
    "left"
)

df_BusinessRules_withvars = df_BusinessRules_lk.withColumn(
    "svSubCk",
    F.when(F.col("mbr_lkup.MBR_UNIQ_KEY").isNull(), F.col("BCBSSC.SUB_UNIQ_KEY"))
     .otherwise(F.col("mbr_lkup.SUB_UNIQ_KEY"))
)

df_BusinessRules = df_BusinessRules_withvars.select(
    F.col("BCBSSC.CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("BCBSSC.CLM_ID"), 18, " ").alias("CLM_ID"),         # char(18)
    F.col("svSubCk").alias("SUB_CK"),
    F.col("BCBSSC.SUB_ID").alias("SUB_ID"),
    F.col("BCBSSC.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.rpad(F.col("BCBSSC.GRP_ID"), 8, " ").alias("GRP"),             # char(8)
    F.col("BCBSSC.MBR_UNIQ_KEY").alias("MBR_CK"),
    F.rpad(F.col("BCBSSC.MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),  # char(2)
    F.col("BCBSSC.SRC_SYS_CD").alias("SRC_SYS_CD")
).alias("BCBSCommonClmTrns")

# ------------------------------------------------------------------------------------------------
# STAGE: ids_subgrp (DB2Connector) => Database = IDS
# Two output pins => subgrp_med, subgrp_dntl
# ------------------------------------------------------------------------------------------------

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
ORDER BY
 CLM_ID,
 [Order] DESC
"""

df_ids_subgrp_med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_subgrp_med.replace("[Order]", "ORDER"))
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
ORDER BY
 CLM_ID,
 [Order] DESC
"""

df_ids_subgrp_dntl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_subgrp_dntl.replace("[Order]", "ORDER"))
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: hf_subgrp (CHashedFileStage) - Scenario A
# Deduplicate
# ------------------------------------------------------------------------------------------------

df_hf_subgrp_med = df_ids_subgrp_med.dropDuplicates(["CLM_ID"])
df_hf_subgrp_dntl = df_ids_subgrp_dntl.dropDuplicates(["CLM_ID"])

# ------------------------------------------------------------------------------------------------
# STAGE: alpha_pfx (CTransformerStage)
# Primary link: BCBSCommonClmTrns => df_BusinessRules
# Lookups: sub_alpha_pfx_lkup => df_hash_sub_alpha_pfx_lkup,
#          mbr_enr_lkup => df_hash_mbr_enr_lkup,
#          subgrpdntl => df_hf_subgrp_dntl,
#          subgrpmed => df_hf_subgrp_med
# ------------------------------------------------------------------------------------------------

df_alpha_pfx_0 = df_BusinessRules.alias("BCBSCommonClmTrns")

df_alpha_pfx_1 = df_alpha_pfx_0.join(
    df_hash_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
    F.col("BCBSCommonClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
    "left"
).join(
    df_hash_mbr_enr_lkup.alias("mbr_enr_lkup"),
    F.col("BCBSCommonClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
    "left"
).join(
    df_hf_subgrp_dntl.alias("subgrpdntl"),
    F.col("BCBSCommonClmTrns.CLM_ID") == F.col("subgrpdntl.CLM_ID"),
    "left"
).join(
    df_hf_subgrp_med.alias("subgrpmed"),
    F.col("BCBSCommonClmTrns.CLM_ID") == F.col("subgrpmed.CLM_ID"),
    "left"
)

df_alpha_pfx = df_alpha_pfx_1.select(
    F.col("BCBSCommonClmTrns.CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("BCBSCommonClmTrns.CLM_ID"),18," ").alias("CLM_ID"),
    F.rpad(
        F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit("UNK"))
         .otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")), 3, " "
    ).alias("ALPHA_PFX_CD"),  # char(3)
    F.rpad(
        F.when(F.col("subgrpmed.CLM_ID").isNull(),
               F.when(F.col("subgrpdntl.CLM_ID").isNull(), F.lit("UNK"))
                .otherwise(F.col("subgrpdntl.CLS_ID")))
         .otherwise(F.col("subgrpmed.CLS_ID")),
        4, " "
    ).alias("CLS"),  # char(4)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")),
        8, " "
    ).alias("CLS_PLN"),  # char(8)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")),
        4, " "
    ).alias("EXPRNC_CAT"),  # char(4)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")),
        4, " "
    ).alias("FNCL_LOB_NO"),  # char(4)
    F.rpad(F.col("BCBSCommonClmTrns.GRP"),8," ").alias("GRP"),  # char(8)
    F.col("BCBSCommonClmTrns.MBR_CK").alias("MBR_CK"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(trim(F.col("mbr_enr_lkup.PROD_ID"))),
        8, " "
    ).alias("PROD"),  # char(8)
    F.rpad(
        F.when(F.col("subgrpmed.CLM_ID").isNull(),
               F.when(F.col("subgrpdntl.CLM_ID").isNull(), F.lit("UNK"))
                .otherwise(F.col("subgrpdntl.SUBGRP_ID")))
         .otherwise(F.col("subgrpmed.SUBGRP_ID")),
        4, " "
    ).alias("SUBGRP"),  # char(4)
    F.col("BCBSCommonClmTrns.SUB_CK").alias("SUB_CK"),
    F.rpad(F.col("BCBSCommonClmTrns.CLM_TYP_CD"),1," ").alias("CLM_TYP_CD"),  # char(1)
    F.rpad(F.col("BCBSCommonClmTrns.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),  # char(2)
    F.rpad(F.col("BCBSCommonClmTrns.SUB_ID"),14," ").alias("SUB_ID"),  # char(14)
    F.col("BCBSCommonClmTrns.SRC_SYS_CD").alias("SRC_SYS_CD")
).alias("ClmCrfIn1")

# ------------------------------------------------------------------------------------------------
# STAGE: Trns1 (CTransformerStage)
# PrimaryLink: ClmCrfIn1 => df_alpha_pfx
# LookupLink: IDS_CLM => df_hf_mnlmbrmtch_recycle_ids_clm
# left join on ClmCrfIn1.CLM_SK = IDS_CLM.CLM_SK
# We define multiple stage variables and produce 3 outputs
# ------------------------------------------------------------------------------------------------

df_trns1_lookup = df_alpha_pfx.alias("ClmCrfIn1").join(
    df_hf_mnlmbrmtch_recycle_ids_clm.alias("IDS_CLM"),
    F.col("ClmCrfIn1.CLM_SK") == F.col("IDS_CLM.CLM_SK"),
    "left"
)

df_trns1_vars = df_trns1_lookup.withColumn(
    "SrcSysCdMbr",
    F.when(
        F.col("ClmCrfIn1.SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("ClmCrfIn1.SRC_SYS_CD"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.when(F.trim(F.col("ClmCrfIn1.SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","MOHSAIC","CAREADVANCE","ESI","OPTUMRX",
            "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
        ), F.lit("FACETS")).otherwise(F.col("ClmCrfIn1.SRC_SYS_CD"))
    )
).withColumn(
    "SrcSysCdProd",
    F.when(
        F.col("ClmCrfIn1.SRC_SYS_CD").isNull() | (F.length(F.trim(F.col("ClmCrfIn1.SRC_SYS_CD"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.when(F.trim(F.col("ClmCrfIn1.SRC_SYS_CD")).isin(
            "PCT","WELLDYNERX","PCS","CAREMARK","ARGUS","EDC","OT@2","ADOL","ESI","OPTUMRX","MCSOURCE","MCAID",
            "MEDTRAK","BCBSSC","BCA","BCBSA","SAVRX","LDI","EYEMED","CVS","LUMERIS","MEDIMPACT"
        ), F.lit("FACETS")).otherwise(F.col("ClmCrfIn1.SRC_SYS_CD"))
    )
)

df_enriched = df_trns1_vars.withColumn(
    "ClsPlnSk",
    F.expr("GetFkeyClsPln(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.CLS_PLN, Logging)")
).withColumn(
    "ClsSk",
    F.expr("GetFkeyCls(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.CLS, Logging)")
).withColumn(
    "ExpCatCdSk",
    F.expr("GetFkeyExprncCat(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.EXPRNC_CAT, Logging)")
).withColumn(
    "GrpSk",
    F.expr("GetFkeyGrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, Logging)")
).withColumn(
    "MbrSk",
    F.expr("GetFkeyMbr(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.MBR_CK, Logging)")
).withColumn(
    "ProdSk",
    F.expr("GetFkeyProd(SrcSysCdProd, ClmCrfIn1.CLM_SK, ClmCrfIn1.PROD, Logging)")
).withColumn(
    "SubGrpSk",
    F.expr("GetFkeySubgrp(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.GRP, ClmCrfIn1.SUBGRP, Logging)")
).withColumn(
    "SubSk",
    F.expr("GetFkeySub(SrcSysCdMbr, ClmCrfIn1.CLM_SK, ClmCrfIn1.SUB_CK, Logging)")
).withColumn(
    "PlnAlphPfxSk",
    F.expr("GetFkeyAlphaPfx('FACETS', ClmCrfIn1.CLM_SK, ClmCrfIn1.ALPHA_PFX_CD, Logging)")
).withColumn(
    "FinancialLOB",
    F.expr('GetFkeyFnclLob("PSI", ClmCrfIn1.CLM_SK, ClmCrfIn1.FNCL_LOB_NO, Logging)')
)

df_ClmFkeyOut1_pre = df_enriched.filter(~F.col("IDS_CLM.CLM_SK").isNull())

df_ClmFkeyOut1 = df_ClmFkeyOut1_pre.select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
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
    F.rpad(F.col("ClmCrfIn1.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.rpad(F.col("ClmCrfIn1.SUB_ID"),14," ").alias("SUB_ID")
)

df_Lnk_PClmMtch = df_enriched.select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK")
)

df_Lnk_IDS_CLM = df_enriched.select(
    F.col("ClmCrfIn1.CLM_SK").alias("CLM_SK"),
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
    F.rpad(F.col("ClmCrfIn1.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.rpad(F.col("ClmCrfIn1.SUB_ID"),14," ").alias("SUB_ID")
)

# ------------------------------------------------------------------------------------------------
# STAGE: CLM_Update (DB2Connector) => Database = IDS => MERGE
# Write df_ClmFkeyOut1 into a temporary table and then merge
# ------------------------------------------------------------------------------------------------

temp_table_clm_update = "STAGING.IdsClmMnlMbrMtchErrClmMbrUpd_CLM_Update_temp"
merge_sql_clm_update = f"""
MERGE INTO {IDSOwner}.CLM AS CLM
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
       AND CD.TRGT_CD = '{IncludeSrcSysCd}'
       AND CLM.CLM_STTUS_CD_SK = CD1.CD_MPPNG_SK
       AND CD1.TRGT_CD = 'A09'
       AND CLM.CLM_SUBTYP_CD_SK = CD2.CD_MPPNG_SK
       AND CD2.TRGT_CD = 'RX'
       AND CLM.MBR_SK <> 0
     ) AS CLM1
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
 AND CD.TRGT_CD = '{IncludeSrcSysCd}'
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

execute_dml(f"DROP TABLE IF EXISTS {temp_table_clm_update}", jdbc_url_ids, jdbc_props_ids)

df_ClmFkeyOut1.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_clm_update) \
    .mode("overwrite") \
    .save()

execute_dml(merge_sql_clm_update, jdbc_url_ids, jdbc_props_ids)

# ------------------------------------------------------------------------------------------------
# STAGE: Seq_PClmMtchSk (CSeqFileStage) - Write df_Lnk_PClmMtch to file
# ------------------------------------------------------------------------------------------------

write_files(
    df_Lnk_PClmMtch.select(F.col("CLM_SK")), 
    f"{adls_path}/verified/MnlMbrMatch_PClmMbrErrRecyc_Delete.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------------------------
# STAGE: Trn_CLM (CTransformerStage)
# Primary Link: Lnk_IDS_CLM => df_Lnk_IDS_CLM
# Lookups: PROD => df_hash_1_prod, FNCL_LOB => df_hash_1_fncl_lob, EXPRNC_CAT => df_hash_1_exprnc_cat, GRP => df_hash_1_grp, EDW_CLM => df_hf_mnlmbrmtch_recycle_edw_clm_f
# Produce Lnk_EDW_CLM
# ------------------------------------------------------------------------------------------------

df_trn_clm_0 = df_Lnk_IDS_CLM.alias("Lnk_IDS_CLM").join(
    df_hash_1_prod.alias("PROD"),
    F.col("Lnk_IDS_CLM.PROD_SK") == F.col("PROD.PROD_SK"),
    "left"
).join(
    df_hash_1_fncl_lob.alias("FNCL_LOB"),
    F.col("Lnk_IDS_CLM.FNCL_LOB_SK") == F.col("FNCL_LOB.FNCL_LOB_SK"),
    "left"
).join(
    df_hash_1_exprnc_cat.alias("EXPRNC_CAT"),
    F.col("Lnk_IDS_CLM.EXPRNC_CAT_SK") == F.col("EXPRNC_CAT.EXPRNC_CAT_SK"),
    "left"
).join(
    df_hash_1_grp.alias("GRP"),
    F.col("Lnk_IDS_CLM.GRP_SK") == F.col("GRP.GRP_SK"),
    "left"
).join(
    df_hf_mnlmbrmtch_recycle_edw_clm_f.alias("EDW_CLM"),
    F.col("Lnk_IDS_CLM.CLM_SK") == F.col("EDW_CLM.CLM_SK"),
    "left"
)

df_Lnk_EDW_CLM_pre = df_trn_clm_0.filter(~F.col("EDW_CLM.CLM_SK").isNull())

df_Lnk_EDW_CLM = df_Lnk_EDW_CLM_pre.select(
    F.col("Lnk_IDS_CLM.CLM_SK").alias("CLM_SK"),
    F.rpad(F.col("RunDate"),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
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
        F.col("EXPRNC_CAT.EXPRNC_CAT_CD").isNull() | (F.trim(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")) == ""),
        F.lit("NA")
    ).otherwise(F.col("EXPRNC_CAT.EXPRNC_CAT_CD")).alias("EXPRNC_CAT_CD"),
    F.when(
        F.col("EXPRNC_CAT.FUND_CAT_CD").isNull() | (F.trim(F.col("EXPRNC_CAT.FUND_CAT_CD")) == ""),
        F.lit("NA")
    ).otherwise(F.col("EXPRNC_CAT.FUND_CAT_CD")).alias("FUND_CAT_CD"),
    F.when(
        F.col("FNCL_LOB.FNCL_LOB_CD").isNull() | (F.trim(F.col("FNCL_LOB.FNCL_LOB_CD")) == ""),
        F.lit("0000")
    ).otherwise(F.col("FNCL_LOB.FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
    F.when(
        F.col("GRP.GRP_ID").isNull() | (F.trim(F.col("GRP.GRP_ID")) == ""),
        F.lit("UNK")
    ).otherwise(F.col("GRP.GRP_ID")).alias("GRP_ID"),
    F.when(
        F.col("PROD.PROD_SH_NM_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("PROD.PROD_SH_NM_SK")).alias("PROD_SH_NM_SK"),
    F.when(
        F.col("PROD.PROD_SH_NM").isNull() | (F.trim(F.col("PROD.PROD_SH_NM")) == ""),
        F.lit(" ")
    ).otherwise(F.col("PROD.PROD_SH_NM")).alias("PROD_SH_NM"),
    F.when(
        F.col("PROD.PROD_ST_CD").isNull() | (F.trim(F.col("PROD.PROD_ST_CD")) == ""),
        F.lit("NA")
    ).otherwise(F.col("PROD.PROD_ST_CD")).alias("PROD_ST_CD"),
    F.rpad(F.lit("N"),1," ").alias("CLM_MEDIGAP_IN")  # char(1)
)

# ------------------------------------------------------------------------------------------------
# STAGE: EDW_CLM_Update (DB2Connector) => Database = EDW => MERGE
# Write df_Lnk_EDW_CLM into a temp table then run merge
# ------------------------------------------------------------------------------------------------

temp_table_edw_clm_update = "STAGING.IdsClmMnlMbrMtchErrClmMbrUpd_EDW_CLM_Update_temp"
merge_sql_edw_clm_update = f"""
MERGE INTO {EDWOwner}.CLM_F AS CLM
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
      WHERE SRC_SYS_CD = '{IncludeSrcSysCd}'
       AND CLM_STTUS_CD = 'A09'
       AND CLM_SUBTYP_CD = 'RX'
       AND MBR_SK <> 0
     ) AS CLM1
WHERE CLM.SRC_SYS_CD = '{IncludeSrcSysCd}'
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

execute_dml(f"DROP TABLE IF EXISTS {temp_table_edw_clm_update}", jdbc_url_edw, jdbc_props_edw)

df_Lnk_EDW_CLM.write.format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable", temp_table_edw_clm_update) \
    .mode("overwrite") \
    .save()

execute_dml(merge_sql_edw_clm_update, jdbc_url_edw, jdbc_props_edw)