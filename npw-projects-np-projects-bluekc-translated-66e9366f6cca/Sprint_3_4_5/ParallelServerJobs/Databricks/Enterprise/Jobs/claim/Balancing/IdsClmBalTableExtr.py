# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/23/09 13:40:03 Batch  15089_49206 PROMOTE bckcetl edw10 dsadm bls fo ron
# MAGIC ^1_1 04/23/09 13:24:19 Batch  15089_48262 INIT bckcett devlEDW dsadm BLS FOR ON
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:    Extract all EDW Balancing Table load files from IDS using the EDW CLaim Driver Table.
# MAGIC 
# MAGIC PROCESSING:  This jobs asumes that the Driver table is NOT truncated in the Regular Clm Control Job
# MAGIC 
# MAGIC OUTPUTS:    Load file for IDS    - B_CLM_F
# MAGIC                                                       B_CLM_LN_F
# MAGIC                                                       B_CLM_F2
# MAGIC                                                       B_CLM_COB_F
# MAGIC                                                       B_CLM_LN_COB_F
# MAGIC                                                       B_PCA_CLM_F
# MAGIC                                                       B_PCA_CLM_LN_F
# MAGIC 
# MAGIC                                                       
# MAGIC                                                        
# MAGIC 
# MAGIC                                                     - W_EDW_PCA_ETL_DRVR.dat
# MAGIC                                                     EDW - W_CLM_DEL
# MAGIC                                                        W_PCA_CLM_DEL.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                   Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          2009-03-31                                     Original Programming.                                                                                               devlEDW                       Brent Leland              04-09-2009

# MAGIC CLM_F Balancing
# MAGIC CLM_LN_F Balancing
# MAGIC CLM_F2 Balancing
# MAGIC CLM_COB Balancing
# MAGIC CLM_LN_COB Balancing
# MAGIC PCA_CLM Balancing
# MAGIC PCA_CLM_LN Balancing
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSFilePath = get_widget_value("IDSFilePath","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: CLM_IDS (DB2Connector -> Output link "CLM")
# --------------------------------------------------------------------------------
extract_query_CLM_IDS = f"""
SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM.CLM_ID as CLM_ID,
       CLM.MBR_SK as MBR_SK,
       CLM.EXPRNC_CAT_SK as EXPRNC_CAT_SK,
       CLM.FNCL_LOB_SK as FNCL_LOB_SK,
       CLM.SVC_STRT_DT_SK as SVC_STRT_DT_SK,
       CLM.PAYBL_AMT as PAYBL_AMT,
       CLM.CLM_CT as CLM_CT,
       CLM.GRP_SK as GRP_SK
FROM {IDSOwner}.CLM CLM, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID = CLM.CLM_ID

{IDSOwner}.CLM CLM, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_CLM_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_IDS)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_CLM_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: CLM_Trans (CTransformerStage)
#   Primary link: CLM (df_CLM_IDS)
#   Lookup links: SrcSysCd, Exprnc_Cat, Fncl_Lob, Grp (all from df_CLM_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_CLM = df_CLM_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd")
)

df_Exprnc_Cat_CLM = df_CLM_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_ExprncCat"),
    F.col("TRGT_CD").alias("TRGT_CD_ExprncCat"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_ExprncCat")
)

df_Fncl_Lob_CLM = df_CLM_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_FnclLob"),
    F.col("TRGT_CD").alias("TRGT_CD_FnclLob"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_FnclLob")
)

df_Grp_CLM = df_CLM_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_Grp"),
    F.col("TRGT_CD").alias("TRGT_CD_Grp"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_Grp")
)

df_CLM_Trans_joined = (
    df_CLM_IDS.alias("CLM")
    .join(df_SrcSysCd_CLM.alias("SrcSysCd"),
          F.col("CLM.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd"),
          "left")
    .join(df_Exprnc_Cat_CLM.alias("Exprnc_Cat"),
          F.col("CLM.EXPRNC_CAT_SK") == F.col("Exprnc_Cat.CD_MPPNG_SK_ExprncCat"),
          "left")
    .join(df_Fncl_Lob_CLM.alias("Fncl_Lob"),
          F.col("CLM.FNCL_LOB_SK") == F.col("Fncl_Lob.CD_MPPNG_SK_FnclLob"),
          "left")
    .join(df_Grp_CLM.alias("Grp"),
          F.col("CLM.GRP_SK") == F.col("Grp.CD_MPPNG_SK_Grp"),
          "left")
)

df_CLM_Trans = df_CLM_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd"))
     .alias("SRC_SYS_CD"),
    F.col("CLM.CLM_ID").alias("CLM_ID"),
    F.col("CLM.MBR_SK").alias("MBR_SK"),
    F.when(F.col("Exprnc_Cat.TRGT_CD_ExprncCat").isNull(), F.lit("UNK"))
     .otherwise(F.col("Exprnc_Cat.TRGT_CD_ExprncCat"))
     .alias("EXPRNC_CAT_CD"),
    F.when(F.col("Fncl_Lob.TRGT_CD_FnclLob").isNull(), F.lit("UNK"))
     .otherwise(F.col("Fncl_Lob.TRGT_CD_FnclLob"))
     .alias("FNCL_LOB_CD"),
    F.col("CLM.SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK_RAW"),
    F.concat(
        F.substring("CLM.SVC_STRT_DT_SK", 1, 4),
        F.substring("CLM.SVC_STRT_DT_SK", 6, 2)
    ).alias("CLM_SVC_STRT_YR_MO_SK_RAW"),
    F.col("CLM.PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.col("CLM.CLM_CT").alias("CLM_CT"),
    F.when(F.col("Grp.TRGT_CD_Grp").isNull(), F.lit("UNK"))
     .otherwise(F.col("Grp.TRGT_CD_Grp"))
     .alias("GRP_ID")
)

# Apply rpad for char/varchar columns, based on metadata from JSON
#   CLM_SVC_STRT_DT_SK: char(10)
#   CLM_SVC_STRT_YR_MO_SK: char(6)
#   Other columns are assumed varchar if not numeric. Lengths unknown => use <...> for unknowns.
df_CLM_Trans_final = df_CLM_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),            # varchar => unknown length
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),                   # varchar => unknown length
    F.col("MBR_SK").alias("MBR_SK"),                                       # numeric
    F.rpad(F.col("EXPRNC_CAT_CD"), <...>, " ").alias("EXPRNC_CAT_CD"),     # varchar => unknown length
    F.rpad(F.col("FNCL_LOB_CD"), <...>, " ").alias("FNCL_LOB_CD"),         # varchar => unknown length
    F.rpad(F.col("CLM_SVC_STRT_DT_SK_RAW"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("CLM_SVC_STRT_YR_MO_SK_RAW"), 6, " ").alias("CLM_SVC_STRT_YR_MO_SK"),
    F.col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),                         # numeric
    F.col("CLM_CT").alias("CLM_CT"),                                       # numeric
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID")                    # varchar => unknown length
)

write_files(
    df_CLM_Trans_final,
    f"{adls_path}/load/B_CLM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CLM_F2_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_CLM_F2_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: CLM_IDS2 (DB2Connector -> Output link "CLM")
# --------------------------------------------------------------------------------
extract_query_CLM_IDS2 = f"""
SELECT CLM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM.CLM_ID as CLM_ID,
       CLM.MBR_SK as MBR_SK,
       CLM.EXPRNC_CAT_SK as EXPRNC_CAT_SK,
       CLM.FNCL_LOB_SK as FNCL_LOB_SK,
       CLM.SVC_STRT_DT_SK as SVC_STRT_DT_SK,
       CLM.PAYBL_AMT as PAYBL_AMT,
       CLM.CLM_CT as CLM_CT,
       CLM.GRP_SK as GRP_SK
FROM {IDSOwner}.CLM CLM, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID=CLM.CLM_ID

{IDSOwner}.CLM CLM, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_CLM_IDS2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_IDS2)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_F2_Trans (CTransformerStage)
#   Primary link: CLM (df_CLM_IDS2)
#   Lookup link: SrcSysCd (df_CLM_F2_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_CLM_F2 = df_CLM_F2_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_F2"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_F2"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_F2")
)

df_CLM_F2_Trans_joined = (
    df_CLM_IDS2.alias("CLM")
    .join(df_SrcSysCd_CLM_F2.alias("SrcSysCd"),
          F.col("CLM.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_F2"),
          "left")
)

df_CLM_F2_Trans = df_CLM_F2_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_F2").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_F2"))
     .alias("SRC_SYS_CD"),
    F.col("CLM.CLM_ID").alias("CLM_ID")
)

# Apply rpad if char/varchar, length unknown => <...> for unknown
df_CLM_F2_Trans_final = df_CLM_F2_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID")
)

write_files(
    df_CLM_F2_Trans_final,
    f"{adls_path}/load/B_CLM_F2.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CLM_LN_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_CLM_LN_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: CLM_LN_IDS (DB2Connector -> Output link "CLM_LN")
# --------------------------------------------------------------------------------
extract_query_CLM_LN_IDS = f"""
SELECT CLM_LN.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_LN.CLM_ID as CLM_ID,
       CLM_LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       CLM_LN.PROC_CD_SK as PROC_CD_SK,
       CLM_LN.CLM_LN_RVNU_CD_SK as CLM_LN_RVNU_CD_SK,
       CLM_LN.ALW_AMT as ALW_AMT,
       CLM_LN.CHRG_AMT as CHRG_AMT,
       CLM_LN.PAYBL_AMT as PAYBL_AMT
FROM {IDSOwner}.CLM_LN CLM_LN, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM_LN.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID=CLM_LN.CLM_ID

{IDSOwner}.CLM_LN CLM_LN, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_CLM_LN_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_LN_IDS)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_LN_Trans (CTransformerStage)
#   Primary link: CLM_LN (df_CLM_LN_IDS)
#   Lookup link: SrcSysCd (df_CLM_LN_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_CLM_LN = df_CLM_LN_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_LN"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_LN"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_LN")
)

df_CLM_LN_Trans_joined = (
    df_CLM_LN_IDS.alias("CLM_LN")
    .join(df_SrcSysCd_CLM_LN.alias("SrcSysCd"),
          F.col("CLM_LN.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_LN"),
          "left")
)

df_CLM_LN_Trans = df_CLM_LN_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_LN").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_LN"))
     .alias("SRC_SYS_CD"),
    F.col("CLM_LN.CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN.CLM_LN_RVNU_CD_SK").alias("CLM_LN_RVNU_CD_SK"),
    F.col("CLM_LN.CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("CLM_LN.PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("CLM_LN.ALW_AMT").alias("CLM_LN_ALW_AMT"),
    F.col("CLM_LN.PROC_CD_SK").alias("CLM_LN_PROC_CD_SK")
)

# rpad char/varchar; lengths unknown => <...>
df_CLM_LN_Trans_final = df_CLM_LN_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_RVNU_CD_SK").alias("CLM_LN_RVNU_CD_SK"),
    F.col("CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
    F.col("CLM_LN_PROC_CD_SK").alias("CLM_LN_PROC_CD_SK")
)

write_files(
    df_CLM_LN_Trans_final,
    f"{adls_path}/load/B_CLM_LN_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CLM_COB (DB2Connector -> Output link "CLM_COB")
# --------------------------------------------------------------------------------
extract_query_CLM_COB = f"""
SELECT CLM_COB.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_COB.CLM_ID as CLM_ID,
       CLM_COB.CLM_COB_TYP_CD_SK as CLM_COB_TYP_CD_SK,
       CLM_COB.ALW_AMT as ALW_AMT,
       CLM_COB.PD_AMT as PD_AMT
FROM {IDSOwner}.CLM_COB CLM_COB, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM_COB.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID = CLM_COB.CLM_ID

{IDSOwner}.CLM_COB CLM_COB, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_CLM_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_COB)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_COB_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_CLM_COB_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: CLM_COB_Trans (CTransformerStage)
#   Primary link: CLM_COB (df_CLM_COB)
#   Lookup links: SrcSysCd, CobTypCd
# --------------------------------------------------------------------------------
df_SrcSysCd_COB = df_CLM_COB_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_COB"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_COB"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_COB")
)
df_CobTypCd_COB = df_CLM_COB_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_CobTypCd_COB"),
    F.col("TRGT_CD").alias("TRGT_CD_CobTypCd_COB"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CobTypCd_COB")
)

df_CLM_COB_Trans_joined = (
    df_CLM_COB.alias("CLM_COB")
    .join(df_SrcSysCd_COB.alias("SrcSysCd"),
          F.col("CLM_COB.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_COB"),
          "left")
    .join(df_CobTypCd_COB.alias("CobTypCd"),
          F.col("CLM_COB.CLM_COB_TYP_CD_SK") == F.col("CobTypCd.CD_MPPNG_SK_CobTypCd_COB"),
          "left")
)

df_CLM_COB_Trans = df_CLM_COB_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_COB").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_COB"))
     .alias("SRC_SYS_CD"),
    F.col("CLM_COB.CLM_ID").alias("CLM_ID"),
    F.when(F.col("CobTypCd.TRGT_CD_CobTypCd_COB").isNull(), F.lit("UNK"))
     .otherwise(F.col("CobTypCd.TRGT_CD_CobTypCd_COB"))
     .alias("CLM_COB_TYP_CD"),
    F.col("CLM_COB.ALW_AMT").alias("CLM_COB_ALW_AMT"),
    F.col("CLM_COB.PD_AMT").alias("CLM_COB_PD_AMT")
)

# rpad char/varchar; unknown => <...>
df_CLM_COB_Trans_final = df_CLM_COB_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_COB_TYP_CD"), <...>, " ").alias("CLM_COB_TYP_CD"),
    F.col("CLM_COB_ALW_AMT").alias("CLM_COB_ALW_AMT"),
    F.col("CLM_COB_PD_AMT").alias("CLM_COB_PD_AMT")
)

write_files(
    df_CLM_COB_Trans_final,
    f"{adls_path}/load/B_CLM_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: CLM_LN_COB_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_CLM_LN_COB_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: CLM_LN_COB (DB2Connector -> Output link "CLM_LN_COB")
# --------------------------------------------------------------------------------
extract_query_CLM_LN_COB = f"""
SELECT CLM_LN_COB.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_LN_COB.CLM_ID as CLM_ID,
       CLM_LN_COB.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       CLM_LN_COB.CLM_LN_COB_TYP_CD_SK as CLM_LN_COB_TYP_CD_SK,
       CLM_LN_COB.ALW_AMT as ALW_AMT,
       CLM_LN_COB.PD_AMT as PD_AMT
FROM {IDSOwner}.CLM_LN_COB CLM_LN_COB, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM_LN_COB.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID=CLM_LN_COB.CLM_ID

{IDSOwner}.CLM_LN_COB CLM_LN_COB, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_CLM_LN_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM_LN_COB)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_LN_COB_Trans (CTransformerStage)
#   Primary link: CLM_LN_COB (df_CLM_LN_COB)
#   Lookup links: SrcSysCd, CobTypCd (df_CLM_LN_COB_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_LN_COB = df_CLM_LN_COB_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_LN_COB"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_LN_COB"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_LN_COB")
)
df_CobTypCd_LN_COB = df_CLM_LN_COB_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_CobTypCd_LN_COB"),
    F.col("TRGT_CD").alias("TRGT_CD_CobTypCd_LN_COB"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_CobTypCd_LN_COB")
)

df_CLM_LN_COB_Trans_joined = (
    df_CLM_LN_COB.alias("CLM_LN_COB")
    .join(df_SrcSysCd_LN_COB.alias("SrcSysCd"),
          F.col("CLM_LN_COB.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_LN_COB"),
          "left")
    .join(df_CobTypCd_LN_COB.alias("CobTypCd"),
          F.col("CLM_LN_COB.CLM_LN_COB_TYP_CD_SK") == F.col("CobTypCd.CD_MPPNG_SK_CobTypCd_LN_COB"),
          "left")
)

df_CLM_LN_COB_Trans = df_CLM_LN_COB_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_LN_COB").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_LN_COB"))
     .alias("SRC_SYS_CD"),
    F.col("CLM_LN_COB.CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_COB.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(F.col("CobTypCd.TRGT_CD_CobTypCd_LN_COB").isNull(), F.lit("UNK"))
     .otherwise(F.col("CobTypCd.TRGT_CD_CobTypCd_LN_COB"))
     .alias("CLM_LN_COB_TYP_CD"),
    F.col("CLM_LN_COB.ALW_AMT").alias("CLM_LN_COB_ALW_AMT"),
    F.col("CLM_LN_COB.PD_AMT").alias("CLM_LN_COB_PD_AMT")
)

# rpad char/varchar; unknown => <...>
df_CLM_LN_COB_Trans_final = df_CLM_LN_COB_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_COB_TYP_CD"), <...>, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("CLM_LN_COB_ALW_AMT").alias("CLM_LN_COB_ALW_AMT"),
    F.col("CLM_LN_COB_PD_AMT").alias("CLM_LN_COB_PD_AMT")
)

write_files(
    df_CLM_LN_COB_Trans_final,
    f"{adls_path}/load/B_CLM_LN_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: PCA_CLM (DB2Connector -> Output link "PCA_CLM")
# --------------------------------------------------------------------------------
extract_query_PCA_CLM = f"""
SELECT CLM_PCA.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_PCA.CLM_ID as CLM_ID,
       CLM_PCA.TOT_CNSD_AMT as TOT_CNSD_AMT,
       CLM_PCA.TOT_PD_AMT as TOT_PD_AMT
FROM {IDSOwner}.CLM_PCA CLM_PCA, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM_PCA.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID=CLM_PCA.CLM_ID

{IDSOwner}.CLM_PCA CLM_PCA, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_PCA_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PCA_CLM)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: PCA_CLM_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_PCA_CLM_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: PCA_CLM_Trans (CTransformerStage)
#   Primary link: PCA_CLM (df_PCA_CLM)
#   Lookup link: SrcSysCd (df_PCA_CLM_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_PCA_CLM = df_PCA_CLM_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_PCA_CLM"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_PCA_CLM"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_PCA_CLM")
)

df_PCA_CLM_Trans_joined = (
    df_PCA_CLM.alias("PCA_CLM")
    .join(df_SrcSysCd_PCA_CLM.alias("SrcSysCd"),
          F.col("PCA_CLM.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_PCA_CLM"),
          "left")
)

df_PCA_CLM_Trans = df_PCA_CLM_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_PCA_CLM").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_PCA_CLM"))
     .alias("SRC_SYS_CD"),
    F.col("PCA_CLM.CLM_ID").alias("CLM_ID"),
    F.col("PCA_CLM.TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
    F.col("PCA_CLM.TOT_PD_AMT").alias("TOT_PD_AMT")
)

# rpad char/varchar; unknown => <...>
df_PCA_CLM_Trans_final = df_PCA_CLM_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT")
)

write_files(
    df_PCA_CLM_Trans_final,
    f"{adls_path}/load/B_PCA_CLM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: PCA_CLM_LN_Codes (CHashedFileStage -> Scenario C => read parquet)
# --------------------------------------------------------------------------------
df_PCA_CLM_LN_Codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# --------------------------------------------------------------------------------
# Stage: PCA_CLM_LN (DB2Connector -> Output link "PCA_CLM_LN")
# --------------------------------------------------------------------------------
extract_query_PCA_CLM_LN = f"""
SELECT CLM_LN_PCA.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       CLM_LN_PCA.CLM_ID as CLM_ID,
       CLM_LN_PCA.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
       CLM_LN_PCA.PD_AMT as PD_AMT
FROM {IDSOwner}.CLM_LN_PCA CLM_LN_PCA, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
WHERE DRIVER.SRC_SYS_CD_SK = CLM_LN_PCA.SRC_SYS_CD_SK
  AND DRIVER.CLM_ID=CLM_LN_PCA.CLM_ID

{IDSOwner}.CLM_LN_PCA CLM_LN_PCA, {IDSOwner}.W_EDW_ETL_DRVR DRIVER
"""
df_PCA_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PCA_CLM_LN)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: PCA_CLM_LN_Trans (CTransformerStage)
#   Primary link: PCA_CLM_LN (df_PCA_CLM_LN)
#   Lookup link: SrcSysCd (df_PCA_CLM_LN_Codes)
# --------------------------------------------------------------------------------
df_SrcSysCd_PCA_CLM_LN = df_PCA_CLM_LN_Codes.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_SrcSysCd_PCA_CLM_LN"),
    F.col("TRGT_CD").alias("TRGT_CD_SrcSysCd_PCA_CLM_LN"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_SrcSysCd_PCA_CLM_LN")
)

df_PCA_CLM_LN_Trans_joined = (
    df_PCA_CLM_LN.alias("PCA_CLM_LN")
    .join(df_SrcSysCd_PCA_CLM_LN.alias("SrcSysCd"),
          F.col("PCA_CLM_LN.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK_SrcSysCd_PCA_CLM_LN"),
          "left")
)

df_PCA_CLM_LN_Trans = df_PCA_CLM_LN_Trans_joined.select(
    F.when(F.col("SrcSysCd.TRGT_CD_SrcSysCd_PCA_CLM_LN").isNull(), F.lit("UNK"))
     .otherwise(F.col("SrcSysCd.TRGT_CD_SrcSysCd_PCA_CLM_LN"))
     .alias("SRC_SYS_CD"),
    F.col("PCA_CLM_LN.CLM_ID").alias("CLM_ID"),
    F.col("PCA_CLM_LN.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("PCA_CLM_LN.PD_AMT").alias("CLM_LN_PCA_PD_AMT")
)

# rpad char/varchar; unknown => <...>
df_PCA_CLM_LN_Trans_final = df_PCA_CLM_LN_Trans.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PCA_PD_AMT").alias("CLM_LN_PCA_PD_AMT")
)

write_files(
    df_PCA_CLM_LN_Trans_final,
    f"{adls_path}/load/B_PCA_CLM_LN_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)