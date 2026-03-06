# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmAltPayeDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw  -  prepares files to compare and update edw claim alternate payee dimension table
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_ALT_PAYE
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_ALT_PAYE_D
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - CDMA code table - resolve code lookups
# MAGIC                 hf_CLM_ALT_PAYE_D_ids - hash file from ids
# MAGIC                 hf_CLM_ALT_PAYE_D_edw - hash file from edw
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC               
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                    
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          --------------------
# MAGIC Tom Harrocks                          08/02/2004-   Originally Programmed
# MAGIC Oliver Nielsen                          12/06/2004    Parameterize Hash File Names
# MAGIC Brent leland                             03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC Brent Leland                          05/11/2006    Removed trim() on codes and IDs
# MAGIC  
# MAGIC Leandrew  Moore                  09/06/2013                p5114                                                      rewrite in parallel                                   EnterpriseWrhsDevl                                Peter Marshall                  12/16/2013

# MAGIC Extract job for CLM_ALT_PAYE_D
# MAGIC Read from source table 
# MAGIC CLM_ALT_PAYE.
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write CLM_ALT_PAYE_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_CLM_ALT_PAYE_in = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLM_ALT_PAYE_SK, COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD, PAYE.CLM_ID, PAYE.CLM_SK, ALT_PAYE_NM, ADDR_LN_1, ADDR_LN_2, ADDR_LN_3, CITY_NM, CLM_ALT_PAYE_ST_CD_SK, POSTAL_CD, CNTY_NM, CLM_ALT_PAYE_CTRY_CD_SK, PHN_NO, FAX_NO, FAX_NO_EXT, EMAIL_ADDR, PAYE.CRT_RUN_CYC_EXCTN_SK, PAYE.LAST_UPDT_RUN_CYC_EXCTN_SK, PHN_NO_EXT FROM {IDSOwner}.CLM_ALT_PAYE PAYE INNER JOIN {IDSOwner}.W_EDW_ETL_DRVR DRVR ON PAYE.CLM_ID = DRVR.CLM_ID AND PAYE.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK LEFT JOIN {IDSOwner}.CD_MPPNG CD ON CD.CD_MPPNG_SK = PAYE.SRC_SYS_CD_SK")
    .load()
)

df_db2_CD_MPPNG_Extr = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG")
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_Extr

df_cpy_cd_mppng_lnl_refClmAltPayeCtry = df_cpy_cd_mppng.select(
  F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
  F.col("TRGT_CD").alias("TRGT_CD"),
  F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_cpy_cd_mppng_lnk_refClmAltPayeState = df_cpy_cd_mppng.select(
  F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
  F.col("TRGT_CD").alias("TRGT_CD"),
  F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
  df_db2_CLM_ALT_PAYE_in.alias("lnk_IdsEdwClmAltPayeDExtr_InABC")
  .join(
    df_cpy_cd_mppng_lnk_refClmAltPayeState.alias("lnk_refClmAltPayeState"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ALT_PAYE_ST_CD_SK") == F.col("lnk_refClmAltPayeState.CD_MPPNG_SK"),
    "left"
  )
  .join(
    df_cpy_cd_mppng_lnl_refClmAltPayeCtry.alias("lnl_refClmAltPayeCtry"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ALT_PAYE_CTRY_CD_SK") == F.col("lnl_refClmAltPayeCtry.CD_MPPNG_SK"),
    "left"
  )
  .select(
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.ALT_PAYE_NM").alias("ALT_PAYE_NM"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CITY_NM").alias("CITY_NM"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ALT_PAYE_ST_CD_SK").alias("CLM_ALT_PAYE_ST_CD_SK"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CNTY_NM").alias("CNTY_NM"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CLM_ALT_PAYE_CTRY_CD_SK").alias("CLM_ALT_PAYE_CTRY_CD_SK"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.PHN_NO").alias("PHN_NO"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.FAX_NO").alias("FAX_NO"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.EMAIL_ADDR").alias("EMAIL_ADDR"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsEdwClmAltPayeDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnl_refClmAltPayeCtry.TRGT_CD").alias("CLM_ALT_PAYE_CTRY_CD"),
    F.col("lnl_refClmAltPayeCtry.TRGT_CD_NM").alias("CLM_ALT_PAYE_CTRY_CD_NM"),
    F.col("lnk_refClmAltPayeState.TRGT_CD").alias("CLM_ALT_PAYE_ST_CD"),
    F.col("lnk_refClmAltPayeState.TRGT_CD_NM").alias("CLM_ALT_PAYE_ST_CD_NM")
  )
)

df_xfm_BusinessLogic_input = df_lkp_Codes

df_xfm_BusinessLogic = df_xfm_BusinessLogic_input

w = Window.orderBy(F.lit(1))
df_xfm_BusinessLogic_with_rn = df_xfm_BusinessLogic.withColumn("rownum", F.row_number().over(w))

df_xfm_BusinessLogic_lnk_Detail = df_xfm_BusinessLogic.select(
  F.col("CLM_ALT_PAYE_SK").alias("CLM_ALT_PAYE_SK"),
  F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
  F.col("CLM_ID").alias("CLM_ID"),
  F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
  F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
  F.col("ALT_PAYE_NM").alias("CLM_ALT_PAYE_NM"),
  F.col("ADDR_LN_1").alias("CLM_ALT_PAYE_ADDR_LN_1"),
  F.col("ADDR_LN_2").alias("CLM_ALT_PAYE_ADDR_LN_2"),
  F.col("ADDR_LN_3").alias("CLM_ALT_PAYE_ADDR_LN_3"),
  F.col("CITY_NM").alias("CLM_ALT_PAYE_CITY_NM"),
  F.col("CLM_ALT_PAYE_ST_CD").alias("CLM_ALT_PAYE_ST_CD"),
  F.col("CLM_ALT_PAYE_ST_CD_NM").alias("CLM_ALT_PAYE_ST_CD_NM"),
  F.col("POSTAL_CD").alias("CLM_ALT_PAYE_POSTAL_CD"),
  F.col("CNTY_NM").alias("CLM_ALT_PAYE_CNTY_NM"),
  trim(F.col("CLM_ALT_PAYE_CTRY_CD")).alias("CLM_ALT_PAYE_CTRY_CD"),
  trim(F.col("CLM_ALT_PAYE_CTRY_CD_NM")).alias("CLM_ALT_PAYE_CTRY_CD_NM"),
  F.col("PHN_NO").alias("CLM_ALT_PAYE_PHN_NO"),
  F.col("PHN_NO_EXT").alias("CLM_ALT_PAYE_PHN_NO_EXT"),
  F.col("FAX_NO").alias("CLM_ALT_PAYE_FAX_NO"),
  F.col("FAX_NO_EXT").alias("CLM_ALT_PAYE_FAX_NO_EXT"),
  F.col("EMAIL_ADDR").alias("CLM_ALT_PAYE_EMAIL_ADDR"),
  F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
  F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  F.col("CLM_ALT_PAYE_CTRY_CD_SK").alias("CLM_ALT_PAYE_CTRY_CD_SK"),
  F.col("CLM_ALT_PAYE_ST_CD_SK").alias("CLM_ALT_PAYE_ST_CD_SK"),
  F.col("CLM_SK").alias("CLM_SK")
)

df_xfm_BusinessLogic_lnk_Na = df_xfm_BusinessLogic_with_rn.filter("rownum = 1").select(
  F.lit(1).alias("CLM_ALT_PAYE_SK"),
  F.lit("NA").alias("SRC_SYS_CD"),
  F.lit("NA").alias("CLM_ID"),
  F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
  F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
  F.lit("NA").alias("CLM_ALT_PAYE_NM"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_1"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_2"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_3"),
  F.lit("NA").alias("CLM_ALT_PAYE_CITY_NM"),
  F.lit("NA").alias("CLM_ALT_PAYE_ST_CD"),
  F.lit("NA").alias("CLM_ALT_PAYE_ST_CD_NM"),
  F.lit("NA         ").alias("CLM_ALT_PAYE_POSTAL_CD"),
  F.lit("NA").alias("CLM_ALT_PAYE_CNTY_NM"),
  F.lit("NA").alias("CLM_ALT_PAYE_CTRY_CD"),
  F.lit("NA").alias("CLM_ALT_PAYE_CTRY_CD_NM"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_PHN_NO"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_PHN_NO_EXT"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_FAX_NO"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_FAX_NO_EXT"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_EMAIL_ADDR"),
  F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
  F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  F.lit(1).alias("CLM_ALT_PAYE_CTRY_CD_SK"),
  F.lit(1).alias("CLM_ALT_PAYE_ST_CD_SK"),
  F.lit(1).alias("CLM_SK")
)

df_xfm_BusinessLogic_lnk_Unk = df_xfm_BusinessLogic_with_rn.filter("rownum = 1").select(
  F.lit(0).alias("CLM_ALT_PAYE_SK"),
  F.lit("UNK").alias("SRC_SYS_CD"),
  F.lit("UNK").alias("CLM_ID"),
  F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
  F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
  F.lit("UNK").alias("CLM_ALT_PAYE_NM"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_1"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_2"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_ADDR_LN_3"),
  F.lit("UNK").alias("CLM_ALT_PAYE_CITY_NM"),
  F.lit("UNK").alias("CLM_ALT_PAYE_ST_CD"),
  F.lit("UNK").alias("CLM_ALT_PAYE_ST_CD_NM"),
  F.lit("UNK        ").alias("CLM_ALT_PAYE_POSTAL_CD"),
  F.lit("UNK").alias("CLM_ALT_PAYE_CNTY_NM"),
  F.lit("UNK").alias("CLM_ALT_PAYE_CTRY_CD"),
  F.lit("UNK").alias("CLM_ALT_PAYE_CTRY_CD_NM"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_PHN_NO"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_PHN_NO_EXT"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_FAX_NO"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_FAX_NO_EXT"),
  F.lit(None).cast(StringType()).alias("CLM_ALT_PAYE_EMAIL_ADDR"),
  F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
  F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  F.lit(0).alias("CLM_ALT_PAYE_CTRY_CD_SK"),
  F.lit(0).alias("CLM_ALT_PAYE_ST_CD_SK"),
  F.lit(0).alias("CLM_SK")
)

df_Fnl_Cap_Fund_D = df_xfm_BusinessLogic_lnk_Detail.unionByName(df_xfm_BusinessLogic_lnk_Na).unionByName(df_xfm_BusinessLogic_lnk_Unk)

df_Fnl_Cap_Fund_D_final = df_Fnl_Cap_Fund_D.select(
  "CLM_ALT_PAYE_SK",
  "SRC_SYS_CD",
  "CLM_ID",
  "CRT_RUN_CYC_EXCTN_DT_SK",
  "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
  "CLM_ALT_PAYE_NM",
  "CLM_ALT_PAYE_ADDR_LN_1",
  "CLM_ALT_PAYE_ADDR_LN_2",
  "CLM_ALT_PAYE_ADDR_LN_3",
  "CLM_ALT_PAYE_CITY_NM",
  "CLM_ALT_PAYE_ST_CD",
  "CLM_ALT_PAYE_ST_CD_NM",
  "CLM_ALT_PAYE_POSTAL_CD",
  "CLM_ALT_PAYE_CNTY_NM",
  "CLM_ALT_PAYE_CTRY_CD",
  "CLM_ALT_PAYE_CTRY_CD_NM",
  "CLM_ALT_PAYE_PHN_NO",
  "CLM_ALT_PAYE_PHN_NO_EXT",
  "CLM_ALT_PAYE_FAX_NO",
  "CLM_ALT_PAYE_FAX_NO_EXT",
  "CLM_ALT_PAYE_EMAIL_ADDR",
  "CRT_RUN_CYC_EXCTN_SK",
  "LAST_UPDT_RUN_CYC_EXCTN_SK",
  "CLM_ALT_PAYE_CTRY_CD_SK",
  "CLM_ALT_PAYE_ST_CD_SK",
  "CLM_SK"
)

df_Fnl_Cap_Fund_D_final = df_Fnl_Cap_Fund_D_final.withColumn(
  "CRT_RUN_CYC_EXCTN_DT_SK",
  F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
  "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
  F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
  "CLM_ALT_PAYE_POSTAL_CD",
  F.rpad(F.col("CLM_ALT_PAYE_POSTAL_CD"), 11, " ")
)

write_files(
  df_Fnl_Cap_Fund_D_final,
  f"{adls_path}/load/CLM_ALT_PAYE_D.dat",
  delimiter=",",
  mode="overwrite",
  is_pqruet=False,
  header=False,
  quote="^",
  nullValue=None
)