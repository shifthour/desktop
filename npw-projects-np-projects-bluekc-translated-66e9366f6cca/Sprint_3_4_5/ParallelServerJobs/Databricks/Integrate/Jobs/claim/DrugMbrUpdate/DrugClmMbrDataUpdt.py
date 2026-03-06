# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC © Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:   
# MAGIC 
# MAGIC PROCESSING:  This job is run monthly to update any missing SK values that were not available when the initial claim came in.  This is run for both PCS and ARGUS claims.  The source is passed in to indicate which type of claims we are pulling.  Find any claim where the CLM.FNCL_LOB_SK = 0 OR CLM.PROD_SK = 0 OR CLM.CLS_SK = 0 OR  CLM.CLS_PLN_SK = 0 or clm.SUBGRP_SK = 0 OR CLM.MBR_SK =0  OR CLM.SUB_SK = 0 OR CLM.EXPRNC_CAT_SK=0.  An additional pull grabs member enrollment information for the claims that have 0 in the SK values we are concerned with.  The enrollment pull populates the missing data and then is used to populate the claim information.  A direct update to the claim tables on IDS, EDW, and DataMart is then done.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #              Change Description                                                                                           Development Project      Code Reviewer          Date Reviewed       ------------------              --------------------     ------------------------             -----------------------------------------------------------------------                                                --------------------------------       -----------------------------        ------------------------       
# MAGIC Steph Goddard\(9)2007-10-01       TT4566 IAD     \(9)Original Programming                                                                                        devlIDS30                       Brent Leland              10-08-2007            
# MAGIC Steph Goddard        2007-11-05       prod supp                       Put purge date in for claim mart claims                                                              devlIDS30                       Brent Leland              11-05-2007
# MAGIC SAndrew                  2008-11-18      TTR394                 \(9) Added new hash file hf_drugclmupd_alpha_lkup                                            devlIDSnew                    Steph Goddard           11/19/2008
# MAGIC \(9)                                  \(9)                              \(9)Added updates to the alpha prefix sk field on the IDS  CLM and and EDW CLM_F   
# MAGIC \(9)                                  \(9)                               \(9)Added update to ALPHA_PFX_CD on the DM_CLM_DM table
# MAGIC \(9)                                  \(9)                               \(9)Removed PurgeDate parameter
# MAGIC \(9)\(9)                                                                Added hash files so lookup would not block update
# MAGIC SAndrew                  2013-12-05       BreakFix                        Added RunCyc parm and update the IDS.CLM Last Updt Run Cycle                 prod                               
# MAGIC                                                                                                Added the update of the EDW.LAST_UPDT_RUN_CYC_EXCTN_DT_SK

# MAGIC Update missing member enrollment information from drug claims
# MAGIC match up member enrollment information with group, product, etc.
# MAGIC hf_drugclmupdt_mbrdata is created in previous job DrugClmFindMbrExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, CharType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all parameter values
CurrentDate = get_widget_value('CurrentDate','2007-10-01')
Source = get_widget_value('Source','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

# Prepare JDBC connections where needed
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

# ------------------------------------------------------------------------------
# Stage: LOOKUPS (DB2Connector -> IDS)
# ------------------------------------------------------------------------------
extract_query_drug_enr_driver = f"""
SELECT DRUG.CLM_ID,
       DRUG.FILL_DT_SK,
       DRUG.MBR_UNIQ_KEY,
       MBR_ENR.EFF_DT_SK,
       MBR_ENR.CLS_PLN_SK,
       MBR_ENR.CLS_SK,
       MBR_ENR.PROD_SK,
       MBR_ENR.ELIG_IN,
       MBR_ENR.TERM_DT_SK,
       GRP.GRP_SK,
       GRP.GRP_ID,
       GRP.GRP_UNIQ_KEY,
       GRP.GRP_NM,
       PROD.PROD_ID,
       PROD_BILL.EXPRNC_CAT_SK,
       PROD_BILL.FNCL_LOB_SK,
       PROD.PROD_SH_NM_SK,
       PROD.PROD_ST_CD_SK,
       SUB.SUB_UNIQ_KEY,
       DRVR.CLM_SK,
       CLM.STTUS_DT_SK,
       LOB.FNCL_LOB_CD,
       CAT.EXPRNC_CAT_CD
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.MBR_ENR MBR_ENR,
     {IDSOwner}.PROD PROD,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.W_DRUG_CLM DRVR,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.FNCL_LOB LOB,
     {IDSOwner}.EXPRNC_CAT CAT,
     {IDSOwner}.PROD_CMPNT PROD_CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT PROD_BILL
WHERE DRUG.CLM_ID = DRVR.CLM_ID
  AND DRVR.CLM_SK = CLM.CLM_SK
  AND DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR.MBR_SK = MBR_ENR.MBR_SK
  AND DRUG.FILL_DT_SK >= MBR_ENR.EFF_DT_SK
  AND DRUG.FILL_DT_SK <= MBR_ENR.TERM_DT_SK
  AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = 'MED'
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND DRUG.FILL_DT_SK >= PROD.PROD_EFF_DT_SK
  AND DRUG.FILL_DT_SK <= PROD.PROD_TERM_DT_SK
  AND MBR_ENR.PROD_SK = PROD_CMPNT.PROD_SK
  AND DRUG.FILL_DT_SK >= PROD_CMPNT.PROD_CMPNT_EFF_DT_SK
  AND DRUG.FILL_DT_SK <= PROD_CMPNT.PROD_CMPNT_TERM_DT_SK
  AND PROD_CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD='PDBL'
  AND PROD_CMPNT.PROD_CMPNT_PFX_ID = PROD_BILL.PROD_CMPNT_PFX_ID
  AND (PROD_BILL.PROD_BILL_CMPNT_ID = 'MED' OR PROD_BILL.PROD_BILL_CMPNT_ID = 'MED1')
  AND DRUG.FILL_DT_SK >= PROD_BILL.PROD_BILL_CMPNT_EFF_DT_SK
  AND DRUG.FILL_DT_SK <= PROD_BILL.PROD_BILL_CMPNT_TERM_DT_SK
  AND PROD_BILL.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
  AND PROD_BILL.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""
df_drug_enr_driver = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_drug_enr_driver)
    .load()
)

extract_query_gender = f"""
SELECT CD_MPPNG.CD_MPPNG_SK, CD_MPPNG.TRGT_CD, CD_MPPNG.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.TRGT_DOMAIN_NM = 'GENDER'
"""
df_gender = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_gender)
    .load()
)

extract_query_mbr_relshp = f"""
SELECT CD_MPPNG.CD_MPPNG_SK, CD_MPPNG.TRGT_CD, CD_MPPNG.TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.TRGT_DOMAIN_NM = 'MEMBER RELATIONSHIP'
"""
df_mbr_relshp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_relshp)
    .load()
)

extract_query_cls = f"""
SELECT CLS.CLS_SK, CLS.CLS_ID, CLS.CLS_DESC
FROM {IDSOwner}.CLS CLS
"""
df_cls = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cls)
    .load()
)

extract_query_cls_pln = f"""
SELECT CLS_PLN.CLS_PLN_SK, CLS_PLN.CLS_PLN_ID, CLS_PLN.CLS_PLN_DESC
FROM {IDSOwner}.CLS_PLN CLS_PLN
"""
df_cls_pln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cls_pln)
    .load()
)

extract_query_alpha_pfx = f"""
SELECT ALPHA.ALPHA_PFX_SK,
       ALPHA.ALPHA_PFX_CD
FROM {IDSOwner}.ALPHA_PFX ALPHA
"""
df_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_alpha_pfx)
    .load()
)

# Deduplicate each lookup DF from the "hash_files" stage (Scenario A) by their key columns
df_gender_dedup = dedup_sort(df_gender, ["CD_MPPNG_SK"], [])
df_mbr_relshp_dedup = dedup_sort(df_mbr_relshp, ["CD_MPPNG_SK"], [])
df_cls_dedup = dedup_sort(df_cls, ["CLS_SK"], [])
df_cls_pln_dedup = dedup_sort(df_cls_pln, ["CLS_PLN_SK"], [])
df_alpha_pfx_dedup = dedup_sort(df_alpha_pfx, ["ALPHA_PFX_SK"], [])

# ------------------------------------------------------------------------------
# Stage: DataMart_in (CODBCStage -> #$ClmMartOwner#.CLM_DM_CLM)
# Because of placeholders, we read the full table and join later
# ------------------------------------------------------------------------------
df_datamart_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("query", f"SELECT CLM_DM_CLM.SRC_SYS_CD, CLM_DM_CLM.CLM_ID FROM {ClmMartOwner}.CLM_DM_CLM")
    .load()
)

# ------------------------------------------------------------------------------
# Stage: hf_drugclmupdt_mbrdata (CHashedFileStage -> Scenario C, but used as Source)
# Translate to reading from Parquet
# ------------------------------------------------------------------------------
df_hf_drugclmupdt_mbrdata = spark.read.parquet("hf_drugclmupdt_mbrdata.parquet")

# ------------------------------------------------------------------------------
# Stage: trans (CTransformerStage)
# Primary link: drug_enr_driver
# Lookup link: member_lkup (left join on MBR_UNIQ_KEY)
# Constraint: keep only matched rows => isNull(...) = false => effectively inner join
# ------------------------------------------------------------------------------
df_member = (
    df_drug_enr_driver.alias("drug_enr_driver")
    .join(
        df_hf_drugclmupdt_mbrdata.alias("member_lkup"),
        F.col("drug_enr_driver.MBR_UNIQ_KEY") == F.col("member_lkup.MBR_UNIQ_KEY"),
        "left",
    )
    .filter(F.col("member_lkup.MBR_UNIQ_KEY").isNotNull())
    .select(
        F.col("drug_enr_driver.CLM_ID").alias("CLM_ID"),
        F.col("drug_enr_driver.FILL_DT_SK").cast(CharType(10)).alias("FILL_DT_SK"),
        F.col("member_lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("member_lkup.CLM_SUB_ID").alias("CLM_SUB_ID"),
        F.col("member_lkup.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("member_lkup.MBR_SK").alias("MBR_SK"),
        F.col("member_lkup.SUB_SK").alias("SUB_SK"),
        F.col("member_lkup.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("member_lkup.BRTH_DT_SK").cast(CharType(10)).alias("BRTH_DT_SK"),
        F.col("member_lkup.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("member_lkup.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("member_lkup.FIRST_NM").alias("FIRST_NM"),
        F.col("member_lkup.MIDINIT").cast(CharType(1)).alias("MIDINIT"),
        F.col("member_lkup.LAST_NM").alias("LAST_NM"),
        F.col("member_lkup.SSN").alias("SSN"),
        F.col("member_lkup.MBR_AGE").alias("MBR_AGE"),
        F.col("drug_enr_driver.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("drug_enr_driver.CLS_SK").alias("CLS_SK"),
        F.col("drug_enr_driver.PROD_SK").alias("PROD_SK"),
        F.col("drug_enr_driver.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
        F.col("drug_enr_driver.GRP_ID").alias("GRP_ID"),
        F.col("drug_enr_driver.GRP_NM").alias("GRP_NM"),
        F.col("drug_enr_driver.PROD_ID").cast(CharType(8)).alias("PROD_ID"),
        F.col("drug_enr_driver.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("drug_enr_driver.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("drug_enr_driver.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("drug_enr_driver.PROD_ST_CD_SK").alias("PROD_ST_CD_SK"),
        F.col("drug_enr_driver.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("drug_enr_driver.GRP_SK").alias("GRP_SK"),
        F.col("drug_enr_driver.CLM_SK").alias("CLM_SK"),
        F.col("drug_enr_driver.STTUS_DT_SK").cast(CharType(10)).alias("STTUS_DT_SK"),
        F.col("drug_enr_driver.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("drug_enr_driver.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("member_lkup.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    )
)

# ------------------------------------------------------------------------------
# Stage: lookup_data (CTransformerStage)
# Primary link: member
# Lookup links: gender_lkup, mbr_relshp_lkup, cls_lkup, cls_pln_lkup, alpha_prefix, datamart_lookup
# ------------------------------------------------------------------------------
df_lookup_data = (
    df_member.alias("member")
    .join(
        df_gender_dedup.alias("gender_lkup"),
        F.col("member.MBR_GNDR_CD_SK") == F.col("gender_lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_mbr_relshp_dedup.alias("mbr_relshp_lkup"),
        F.col("member.MBR_RELSHP_CD_SK") == F.col("mbr_relshp_lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cls_dedup.alias("cls_lkup"),
        F.col("member.CLS_SK") == F.col("cls_lkup.CLS_SK"),
        "left",
    )
    .join(
        df_cls_pln_dedup.alias("cls_pln_lkup"),
        F.col("member.CLS_PLN_SK") == F.col("cls_pln_lkup.CLS_PLN_SK"),
        "left",
    )
    .join(
        df_datamart_lookup.alias("datamart_lookup"),
        [
            F.col("member.CLM_ID") == F.col("datamart_lookup.CLM_ID"),
            F.col(Source) == F.col("datamart_lookup.SRC_SYS_CD"),
        ],
        "left",
    )
    .join(
        df_alpha_pfx_dedup.alias("alpha_prefix"),
        F.col("member.ALPHA_PFX_SK") == F.col("alpha_prefix.ALPHA_PFX_SK"),
        "left",
    )
)

# -----------------------------
# Output pin: DataMartClmInitUpdt (constraint: datamart_lookup.CLM_ID not null)
# -----------------------------
df_DataMartClmInitUpdt = df_lookup_data.filter(
    F.col("datamart_lookup.CLM_ID").isNotNull()
).select(
    F.col("member.CLM_ID").alias("CLM_ID"),
    F.col(Source).alias("SRC_SYS_CD"),
    F.col("member.BRTH_DT_SK").alias("MBR_BRTH_DT"),
    F.col("member.CLM_SUB_ID").alias("CLM_SUB_ID"),
    F.col("member.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("member.GRP_ID").alias("GRP_ID"),
    F.col("member.GRP_NM").alias("GRP_NM"),
    F.col("member.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("member.FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("member.MIDINIT").alias("MBR_MIDINIT"),
    F.col("member.LAST_NM").alias("MBR_LAST_NM"),
    F.col("member.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
)

# -----------------------------
# Output pin: DataMartClmUpdt (constraint: datamart_lookup.CLM_ID not null)
# -----------------------------
df_DataMartClmUpdt = df_lookup_data.filter(
    F.col("datamart_lookup.CLM_ID").isNotNull()
).select(
    F.col(Source).alias("SRC_SYS_CD"),
    F.col("member.CLM_ID").alias("CLM_ID"),
    F.col("member.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("member.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("member.FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("member.MIDINIT").alias("MBR_MIDINIT"),
    F.col("member.LAST_NM").alias("MBR_LAST_NM"),
    F.when(F.col("gender_lkup.TRGT_CD").isNull(), F.lit("UNK"))
     .otherwise(F.col("gender_lkup.TRGT_CD"))
     .alias("MBR_GNDR_CD"),
    F.when(F.col("gender_lkup.TRGT_CD_NM").isNull(), F.lit("UNK"))
     .otherwise(F.col("gender_lkup.TRGT_CD_NM"))
     .alias("MBR_GNDR_NM"),
    F.lit(CurrentDate).alias("DM_LAST_UPDT_DT"),
    F.when(F.col("cls_lkup.CLS_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("cls_lkup.CLS_ID"))
     .alias("CLS_ID"),
    F.when(F.col("cls_lkup.CLS_DESC").isNull(), F.lit("UNK"))
     .otherwise(F.col("cls_lkup.CLS_DESC"))
     .alias("CLS_DESC"),
    F.when(F.col("cls_pln_lkup.CLS_PLN_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("cls_pln_lkup.CLS_PLN_ID"))
     .alias("CLS_PLN_ID"),
    F.when(F.col("cls_pln_lkup.CLS_PLN_DESC").isNull(), F.lit("UNK"))
     .otherwise(F.col("cls_pln_lkup.CLS_PLN_DESC"))
     .alias("CLS_PLN_DESC"),
    F.col("member.GRP_ID").alias("GRP_ID"),
    F.when(F.col("mbr_relshp_lkup.TRGT_CD").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_relshp_lkup.TRGT_CD"))
     .alias("MBR_RELSHP_CD"),
    F.col("member.PROD_ID").alias("PROD_ID"),
    F.when(F.col("alpha_prefix.ALPHA_PFX_CD").isNull(), F.lit("UNK"))
     .otherwise(F.col("alpha_prefix.ALPHA_PFX_CD"))
     .alias("ALPHA_PFX_CD"),
)

# -----------------------------
# Output pin: EDW_out (no constraint)
# -----------------------------
df_EDW_out = df_lookup_data.select(
    F.col("member.CLM_SK").alias("CLM_SK"),
    F.col("member.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.when(F.col("member.CLS_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.CLS_SK"))
     .alias("CLS_SK"),
    F.when(F.col("member.CLS_PLN_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.CLS_PLN_SK"))
     .alias("CLS_PLN_SK"),
    F.when(F.col("member.EXPRNC_CAT_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.EXPRNC_CAT_SK"))
     .alias("EXPRNC_CAT_SK"),
    F.when(F.col("member.FNCL_LOB_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.FNCL_LOB_SK"))
     .alias("FNCL_LOB_SK"),
    F.when(F.col("member.GRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.GRP_SK"))
     .alias("GRP_SK"),
    F.when(F.col("member.MBR_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.MBR_SK"))
     .alias("MBR_SK"),
    F.when(F.col("member.SUB_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.SUB_SK"))
     .alias("SUB_SK"),
    F.when(F.col("member.PROD_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.PROD_SK"))
     .alias("PROD_SK"),
    F.when(F.col("member.SUBGRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.SUBGRP_SK"))
     .alias("SUBGRP_SK"),
    F.col("member.MBR_AGE").alias("CLM_MBR_AGE"),
    F.col("member.GRP_ID").alias("GRP_ID"),
    F.col("member.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("member.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
    F.lit(CurrentDate).cast(CharType(10)).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
)

# -----------------------------
# Output pin: IDS_out (no constraint)
# -----------------------------
df_IDS_out = df_lookup_data.select(
    F.col("member.CLM_SK").alias("CLM_SK"),
    F.when(F.col("member.CLS_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.CLS_SK"))
     .alias("CLS_SK"),
    F.when(F.col("member.CLS_PLN_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.CLS_PLN_SK"))
     .alias("CLS_PLN_SK"),
    F.when(F.col("member.EXPRNC_CAT_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.EXPRNC_CAT_SK"))
     .alias("EXPRNC_CAT_SK"),
    F.when(F.col("member.FNCL_LOB_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.FNCL_LOB_SK"))
     .alias("FNCL_LOB_SK"),
    F.when(F.col("member.GRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.GRP_SK"))
     .alias("GRP_SK"),
    F.when(F.col("member.MBR_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.MBR_SK"))
     .alias("MBR_SK"),
    F.when(F.col("member.SUB_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.SUB_SK"))
     .alias("SUB_SK"),
    F.when(F.col("member.PROD_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.PROD_SK"))
     .alias("PROD_SK"),
    F.when(F.col("member.SUBGRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("member.SUBGRP_SK"))
     .alias("SUBGRP_SK"),
    F.col("member.MBR_AGE").alias("MBR_AGE"),
    F.col("member.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

# ------------------------------------------------------------------------------
# Next: hf_drugclmupd_dm_clm (Scenario A deduplicate) => CLM_DM_CLM merge
# ------------------------------------------------------------------------------
df_DataMartClmUpdt_dedup = dedup_sort(
    df_DataMartClmUpdt,
    ["SRC_SYS_CD", "CLM_ID"],
    [],
)

# MERGE into #$ClmMartOwner#.CLM_DM_CLM
temp_table_dm_clm = "STAGING.DrugClmMbrDataUpdt_CLM_DM_CLM_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_dm_clm}")  # Physical table drop
df_DataMartClmUpdt_dedup.write.jdbc(
    url=jdbc_url_clmmart,
    table=temp_table_dm_clm,
    mode="overwrite",
    properties=jdbc_props_clmmart,
)
merge_sql_dm_clm = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM AS T
USING {temp_table_dm_clm} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
  UPDATE SET 
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.MBR_GNDR_CD = S.MBR_GNDR_CD,
    T.MBR_GNDR_NM = S.MBR_GNDR_NM,
    T.DM_LAST_UPDT_DT = S.DM_LAST_UPDT_DT,
    T.CLS_ID = S.CLS_ID,
    T.CLS_DESC = S.CLS_DESC,
    T.CLS_PLN_ID = S.CLS_PLN_ID,
    T.CLS_PLN_DESC = S.CLS_PLN_DESC,
    T.GRP_ID = S.GRP_ID,
    T.MBR_RELSHP_CD = S.MBR_RELSHP_CD,
    T.PROD_ID = S.PROD_ID,
    T.ALPHA_PFX_CD = S.ALPHA_PFX_CD
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    GRP_UNIQ_KEY,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    MBR_GNDR_CD,
    MBR_GNDR_NM,
    DM_LAST_UPDT_DT,
    CLS_ID,
    CLS_DESC,
    CLS_PLN_ID,
    CLS_PLN_DESC,
    GRP_ID,
    MBR_RELSHP_CD,
    PROD_ID,
    ALPHA_PFX_CD
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.GRP_UNIQ_KEY,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.MBR_GNDR_CD,
    S.MBR_GNDR_NM,
    S.DM_LAST_UPDT_DT,
    S.CLS_ID,
    S.CLS_DESC,
    S.CLS_PLN_ID,
    S.CLS_PLN_DESC,
    S.GRP_ID,
    S.MBR_RELSHP_CD,
    S.PROD_ID,
    S.ALPHA_PFX_CD
  )
;
"""
execute_dml(merge_sql_dm_clm, jdbc_url_clmmart, jdbc_props_clmmart)

# ------------------------------------------------------------------------------
# Next: hf_drugclmupd_dm_clm_init (Scenario A deduplicate) => CLM_DM_INIT_CLM merge
# ------------------------------------------------------------------------------
df_DataMartClmInitUpdt_dedup = dedup_sort(
    df_DataMartClmInitUpdt,
    ["CLM_ID", "SRC_SYS_CD"],
    [],
)

temp_table_dm_clm_init = "STAGING.DrugClmMbrDataUpdt_CLM_DM_INIT_CLM_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_dm_clm_init}")
df_DataMartClmInitUpdt_dedup.write.jdbc(
    url=jdbc_url_clmmart,
    table=temp_table_dm_clm_init,
    mode="overwrite",
    properties=jdbc_props_clmmart,
)
merge_sql_dm_clm_init = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING {temp_table_dm_clm_init} AS S
ON 
    T.CLM_ID = S.CLM_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET 
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.CLM_SUB_ID = S.CLM_SUB_ID,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.GRP_NM = S.GRP_NM,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_FIRST_NM = S.MBR_FIRST_NM,
    T.MBR_MIDINIT = S.MBR_MIDINIT,
    T.MBR_LAST_NM = S.MBR_LAST_NM,
    T.SUB_UNIQ_KEY = S.SUB_UNIQ_KEY
WHEN NOT MATCHED THEN
  INSERT (
    CLM_ID,
    SRC_SYS_CD,
    MBR_BRTH_DT,
    CLM_SUB_ID,
    GRP_UNIQ_KEY,
    GRP_ID,
    GRP_NM,
    MBR_UNIQ_KEY,
    MBR_FIRST_NM,
    MBR_MIDINIT,
    MBR_LAST_NM,
    SUB_UNIQ_KEY
  )
  VALUES (
    S.CLM_ID,
    S.SRC_SYS_CD,
    S.MBR_BRTH_DT,
    S.CLM_SUB_ID,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.GRP_NM,
    S.MBR_UNIQ_KEY,
    S.MBR_FIRST_NM,
    S.MBR_MIDINIT,
    S.MBR_LAST_NM,
    S.SUB_UNIQ_KEY
  )
;
"""
execute_dml(merge_sql_dm_clm_init, jdbc_url_clmmart, jdbc_props_clmmart)

# ------------------------------------------------------------------------------
# Next: hf_drugclmupd_ids_clm (Scenario A deduplicate) => IDS table merge
# ------------------------------------------------------------------------------
df_IDS_out_dedup = dedup_sort(
    df_IDS_out,
    ["CLM_SK"],
    [],
)

temp_table_ids_clm = "STAGING.DrugClmMbrDataUpdt_IDS_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_ids_clm}")
df_IDS_out_dedup.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_ids_clm,
    mode="overwrite",
    properties=jdbc_props_ids,
)
merge_sql_ids_clm = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING {temp_table_ids_clm} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.CLS_SK = S.CLS_SK,
    T.CLS_PLN_SK = S.CLS_PLN_SK,
    T.EXPRNC_CAT_SK = S.EXPRNC_CAT_SK,
    T.FNCL_LOB_SK = S.FNCL_LOB_SK,
    T.GRP_SK = S.GRP_SK,
    T.MBR_SK = S.MBR_SK,
    T.SUB_SK = S.SUB_SK,
    T.PROD_SK = S.PROD_SK,
    T.SUBGRP_SK = S.SUBGRP_SK,
    T.MBR_AGE = S.MBR_AGE,
    T.ALPHA_PFX_SK = S.ALPHA_PFX_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    CLM_SK,
    CLS_SK,
    CLS_PLN_SK,
    EXPRNC_CAT_SK,
    FNCL_LOB_SK,
    GRP_SK,
    MBR_SK,
    SUB_SK,
    PROD_SK,
    SUBGRP_SK,
    MBR_AGE,
    ALPHA_PFX_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.CLM_SK,
    S.CLS_SK,
    S.CLS_PLN_SK,
    S.EXPRNC_CAT_SK,
    S.FNCL_LOB_SK,
    S.GRP_SK,
    S.MBR_SK,
    S.SUB_SK,
    S.PROD_SK,
    S.SUBGRP_SK,
    S.MBR_AGE,
    S.ALPHA_PFX_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK
  )
;
"""
execute_dml(merge_sql_ids_clm, jdbc_url_ids, jdbc_props_ids)

# ------------------------------------------------------------------------------
# Next: hf_drugclmupd_edw_clm (Scenario A deduplicate) => EDW table merge
# ------------------------------------------------------------------------------
df_EDW_out_dedup = dedup_sort(
    df_EDW_out,
    ["CLM_SK"],
    [],
)

temp_table_edw_clm_f = "STAGING.DrugClmMbrDataUpdt_EDW_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table_edw_clm_f}")
df_EDW_out_dedup.write.jdbc(
    url=jdbc_url_edw,
    table=temp_table_edw_clm_f,
    mode="overwrite",
    properties=jdbc_props_edw,
)
merge_sql_edw_clm = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING {temp_table_edw_clm_f} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.ALPHA_PFX_SK = S.ALPHA_PFX_SK,
    T.CLS_SK = S.CLS_SK,
    T.CLS_PLN_SK = S.CLS_PLN_SK,
    T.EXPRNC_CAT_SK = S.EXPRNC_CAT_SK,
    T.FNCL_LOB_SK = S.FNCL_LOB_SK,
    T.GRP_SK = S.GRP_SK,
    T.MBR_SK = S.MBR_SK,
    T.SUB_SK = S.SUB_SK,
    T.PROD_SK = S.PROD_SK,
    T.SUBGRP_SK = S.SUBGRP_SK,
    T.CLM_MBR_AGE = S.CLM_MBR_AGE,
    T.GRP_ID = S.GRP_ID,
    T.FNCL_LOB_CD = S.FNCL_LOB_CD,
    T.EXPRNC_CAT_CD = S.EXPRNC_CAT_CD,
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
WHEN NOT MATCHED THEN
  INSERT (
    CLM_SK,
    ALPHA_PFX_SK,
    CLS_SK,
    CLS_PLN_SK,
    EXPRNC_CAT_SK,
    FNCL_LOB_SK,
    GRP_SK,
    MBR_SK,
    SUB_SK,
    PROD_SK,
    SUBGRP_SK,
    CLM_MBR_AGE,
    GRP_ID,
    FNCL_LOB_CD,
    EXPRNC_CAT_CD,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK
  )
  VALUES (
    S.CLM_SK,
    S.ALPHA_PFX_SK,
    S.CLS_SK,
    S.CLS_PLN_SK,
    S.EXPRNC_CAT_SK,
    S.FNCL_LOB_SK,
    S.GRP_SK,
    S.MBR_SK,
    S.SUB_SK,
    S.PROD_SK,
    S.SUBGRP_SK,
    S.CLM_MBR_AGE,
    S.GRP_ID,
    S.FNCL_LOB_CD,
    S.EXPRNC_CAT_CD,
    S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
  )
;
"""
execute_dml(merge_sql_edw_clm, jdbc_url_edw, jdbc_props_edw)