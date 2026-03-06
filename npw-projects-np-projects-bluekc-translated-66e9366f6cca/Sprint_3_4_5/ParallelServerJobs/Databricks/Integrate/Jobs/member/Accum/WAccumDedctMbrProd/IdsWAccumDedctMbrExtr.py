# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 13:39:53 Batch  14638_49197 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/10/08 10:01:22 Batch  14620_36086 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/29/07 14:51:28 Batch  14547_53489 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 09/25/07 16:07:19 Batch  14513_58043 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 09/18/07 09:05:03 Batch  14506_32892 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/23/07 12:38:45 Batch  14480_45530 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_5 03/28/07 15:08:03 Batch  14332_54485 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 03/23/07 14:23:20 Batch  14327_51805 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/22/07 14:57:24 Batch  14326_53850 PROMOTE bckcetl ids20 dsadm bls for s andrew
# MAGIC ^1_3 03/22/07 14:54:02 Batch  14326_53648 INIT bckcett testIDS30 dsadm bls for s andrew
# MAGIC ^1_2 03/19/07 14:18:22 Batch  14323_51509 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_1 03/19/07 09:49:37 Batch  14323_35385 INIT bckcett testIDS30 dsadm bls for s andrew
# MAGIC ^1_4 03/06/07 19:41:19 Batch  14310_70881 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_4 03/06/07 19:18:09 Batch  14310_69492 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 03/06/07 18:31:04 Batch  14310_66666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 03/05/07 16:11:54 Batch  14309_58315 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 03/05/07 15:21:51 Batch  14309_55315 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 03/02/07 18:21:33 Batch  14306_66098 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_9 02/27/07 12:24:30 Batch  14303_44673 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 02/25/07 13:49:35 Batch  14301_49781 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 02/25/07 12:32:05 Batch  14301_45129 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_7 02/16/07 13:41:45 Batch  14292_49307 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_6 02/16/07 09:36:56 Batch  14292_34620 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 02/16/07 09:33:44 Batch  14292_34426 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 02/15/07 12:55:19 Batch  14291_46523 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 02/15/07 12:53:30 Batch  14291_46411 INIT bckcett testIDS30 u10157 sa
# MAGIC 
# MAGIC hf_mbr_prcs_accm_elig_prods_all_dates;hf_mbr_prcs_accm_elig_prods_max_date;hf_mbr_prcs_accm_mbr_rltnshp
# MAGIC 
# MAGIC JOB NAME:          IdsWAccumDedctMbrProdExtr
# MAGIC 
# MAGIC  
# MAGIC                           
# MAGIC PROCESSING:    Selects eligible members whose medical or dental products have both a product prefix id of DV and EBCL. 
# MAGIC                             These will be loaded to a working table W_ACCUM_DEDCT_MBR_PROD
# MAGIC                           
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              SAndrew           2007-01-29        Original development

# MAGIC ** Retrieve all currently elig members and their prod_sk.  
# MAGIC ** Runs in IdsMbrProcessingAccumExtrSeq.
# MAGIC ** Load for table W_ACCUM_DEDCT_MBR_PROD is Load/Replace.
# MAGIC Table load REPLACED in sequencer IdsMbrProcessingAccumExtrSeq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


AccumYear = get_widget_value('AccumYear','2006')
AccumYearFirstDay = get_widget_value('AccumYearFirstDay','2006-01-01')
AccumYearLastDay = get_widget_value('AccumYearLastDay','2006-12-31')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_cd_mppng = f"""
SELECT
 MAP.CD_MPPNG_SK,
 MAP.TRGT_CD
FROM {IDSOwner}.CD_MPPNG MAP
WHERE LTRIM(RTRIM(MAP.TRGT_DOMAIN_NM)) = 'MEMBER RELATIONSHIP'
"""

df_cd_mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_cd_mppng)
    .load()
)

extract_query_elig_prods_all_dates = f"""
SELECT
 MBR.MBR_SK,
 MBR.MBR_UNIQ_KEY,
 MBR.SUB_SK,
 ENR.GRP_SK,
 MAP1.TRGT_CD,
 ENR.TERM_DT_SK,
 ENR.PROD_SK,
 MBR.MBR_RELSHP_CD_SK
FROM {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE MBR.MBR_SK=ENR.MBR_SK
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK=MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ('MED','DNTL')
  AND ENR.ELIG_IN='Y'
  AND ENR.EFF_DT_SK <= '{AccumYearLastDay}'
  AND ENR.TERM_DT_SK >= '{AccumYearFirstDay}'
GROUP BY
 MBR.MBR_SK,
 MBR.MBR_UNIQ_KEY,
 MBR.SUB_SK,
 ENR.GRP_SK,
 MAP1.TRGT_CD,
 ENR.PROD_SK,
 MBR.MBR_RELSHP_CD_SK,
 ENR.TERM_DT_SK
"""

df_elig_prods_all_dates = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_elig_prods_all_dates)
    .load()
)

extract_query_driver = f"""
SELECT
 MBR.MBR_SK,
 MBR.MBR_UNIQ_KEY,
 MBR.SUB_SK,
 ENR.GRP_SK,
 MAP1.TRGT_CD,
 max(ENR.TERM_DT_SK) as MAX_TERM_DT_SK
FROM {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE MBR.MBR_SK=ENR.MBR_SK
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK=MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ('MED','DNTL')
  AND ENR.ELIG_IN='Y'
  AND ENR.EFF_DT_SK <= '{AccumYearLastDay}'
  AND ENR.TERM_DT_SK >= '{AccumYearFirstDay}'
GROUP BY
 MBR.MBR_SK,
 MBR.MBR_UNIQ_KEY,
 MBR.SUB_SK,
 ENR.GRP_SK,
 MAP1.TRGT_CD
"""

df_driver = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_driver)
    .load()
)

df_hf_mbr_prcs_accm_mbr_rltnshp = dedup_sort(
    df_cd_mppng,
    ["CD_MPPNG_SK"],
    []
)

df_elig_prods_all_dates_2 = df_elig_prods_all_dates.select(
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUB_SK"),
    F.col("GRP_SK"),
    F.col("TRGT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("TERM_DT_SK").alias("MBR_TERM_DT"),
    F.col("PROD_SK"),
    F.col("MBR_RELSHP_CD_SK")
)

df_elig_prods_all_dates_2 = dedup_sort(
    df_elig_prods_all_dates_2,
    ["MBR_SK","MBR_UNIQ_KEY","SUB_SK","GRP_SK","MBR_ENR_CLS_PLN_PROD_CAT_CD","MBR_TERM_DT"],
    []
)

df_elig_driver = df_driver.select(
    F.col("MBR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUB_SK"),
    F.col("GRP_SK"),
    F.col("TRGT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("MAX_TERM_DT_SK").alias("MBR_TERM_DT")
)

df_elig_driver = dedup_sort(
    df_elig_driver,
    ["MBR_SK","MBR_UNIQ_KEY","SUB_SK","GRP_SK","MBR_ENR_CLS_PLN_PROD_CAT_CD","MBR_TERM_DT"],
    []
)

df_bld_term_dt = (
    df_elig_driver.alias("elig_driver")
    .join(
        df_elig_prods_all_dates_2.alias("elig_prods_all_dates_2"),
        (
            (F.col("elig_driver.MBR_SK") == F.col("elig_prods_all_dates_2.MBR_SK")) &
            (F.col("elig_driver.MBR_UNIQ_KEY") == F.col("elig_prods_all_dates_2.MBR_UNIQ_KEY")) &
            (F.col("elig_driver.SUB_SK") == F.col("elig_prods_all_dates_2.SUB_SK")) &
            (F.col("elig_driver.GRP_SK") == F.col("elig_prods_all_dates_2.GRP_SK")) &
            (F.col("elig_driver.MBR_ENR_CLS_PLN_PROD_CAT_CD") == F.col("elig_prods_all_dates_2.MBR_ENR_CLS_PLN_PROD_CAT_CD")) &
            (F.col("elig_driver.MBR_TERM_DT") == F.col("elig_prods_all_dates_2.MBR_TERM_DT"))
        ),
        how="left"
    )
    .select(
        F.col("elig_driver.MBR_SK").alias("MBR_SK"),
        F.col("elig_driver.SUB_SK").alias("SUB_SK"),
        F.when(F.col("elig_prods_all_dates_2.PROD_SK").isNull(), F.lit(0)).otherwise(F.col("elig_prods_all_dates_2.PROD_SK")).alias("PROD_SK"),
        F.lit(AccumYear).alias("DEDCT_YR"),
        F.col("elig_driver.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("elig_driver.GRP_SK").alias("GRP_SK"),
        F.when(F.col("elig_prods_all_dates_2.MBR_RELSHP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("elig_prods_all_dates_2.MBR_RELSHP_CD_SK")).alias("MBR_RELSHP_CD_SK"),
        F.when(
            F.length(trim(F.col("elig_prods_all_dates_2.MBR_TERM_DT"))) == 0,
            F.lit("UNK")
        ).otherwise(
            F.when(
                F.col("elig_prods_all_dates_2.MBR_TERM_DT") > AccumYearLastDay,
                F.lit(AccumYearLastDay)
            ).otherwise(F.col("elig_prods_all_dates_2.MBR_TERM_DT"))
        ).alias("MBR_TERM_DT"),
        F.lit("UNK").alias("PROD_CMPNT_PFX_ID_DV"),
        F.lit("UNK").alias("PROD_CMPNT_PFX_ID_EBCL")
    )
)

df_Transformer_411 = (
    df_bld_term_dt.alias("mbr_elig")
    .join(
        df_hf_mbr_prcs_accm_mbr_rltnshp.alias("mbr_relationshp"),
        F.col("mbr_elig.MBR_RELSHP_CD_SK") == F.col("mbr_relationshp.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("mbr_elig.MBR_SK").alias("MBR_SK"),
        F.col("mbr_elig.SUB_SK").alias("SUB_SK"),
        F.col("mbr_elig.PROD_SK").alias("PROD_SK"),
        F.col("mbr_elig.DEDCT_YR").alias("DEDCT_YR"),
        F.col("mbr_elig.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("mbr_elig.GRP_SK").alias("GRP_SK"),
        F.when(
            F.length(trim(F.col("mbr_relationshp.TRGT_CD"))) == 0,
            F.lit("UNK")
        ).otherwise(
            trim(F.col("mbr_relationshp.TRGT_CD"))
        ).alias("MBR_RELSHP_CD"),
        F.col("mbr_elig.MBR_TERM_DT").alias("MBR_TERM_DT"),
        F.lit("UNK").alias("PROD_CMPNT_PFX_ID_DV"),
        F.lit("UNK").alias("PROD_CMPNT_PFX_ID_EBCL")
    )
)

df_final = df_Transformer_411.select(
    F.col("MBR_SK"),
    F.col("SUB_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("DEDCT_YR"), 4, " ").alias("DEDCT_YR"),
    F.col("MBR_UNIQ_KEY"),
    F.col("GRP_SK"),
    F.rpad(F.col("MBR_RELSHP_CD"), 100, " ").alias("MBR_RELSHP_CD"),
    F.rpad(F.col("MBR_TERM_DT"), 10, " ").alias("MBR_TERM_DT"),
    F.rpad(F.col("PROD_CMPNT_PFX_ID_DV"), 50, " ").alias("PROD_CMPNT_PFX_ID_DV"),
    F.rpad(F.col("PROD_CMPNT_PFX_ID_EBCL"), 50, " ").alias("PROD_CMPNT_PFX_ID_EBCL")
)

write_files(
    df_final,
    f"{adls_path}/load/W_ACCUM_DEDCT_MBR_PROD.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)