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
# MAGIC ^1_15 03/28/07 15:08:03 Batch  14332_54485 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 03/23/07 14:23:20 Batch  14327_51805 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/19/07 12:07:11 Batch  14323_43638 PROMOTE bckcetl ids20 dsadm bls for s andrew
# MAGIC ^1_2 03/19/07 11:59:15 Batch  14323_43161 INIT bckcett testIDS30 dsadm bls for s andrew
# MAGIC ^1_1 03/19/07 09:49:37 Batch  14323_35385 INIT bckcett testIDS30 dsadm bls for s andrew
# MAGIC ^1_13 03/06/07 19:41:19 Batch  14310_70881 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_13 03/06/07 19:18:09 Batch  14310_69492 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_13 03/06/07 18:31:04 Batch  14310_66666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_12 03/05/07 16:11:54 Batch  14309_58315 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_11 03/05/07 15:21:51 Batch  14309_55315 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_10 03/02/07 18:21:33 Batch  14306_66098 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_9 02/27/07 12:24:30 Batch  14303_44673 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 02/25/07 13:49:35 Batch  14301_49781 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 02/25/07 12:32:05 Batch  14301_45129 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_7 02/16/07 13:41:45 Batch  14292_49307 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_6 02/16/07 09:36:56 Batch  14292_34620 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_6 02/16/07 09:33:44 Batch  14292_34426 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 02/15/07 12:55:19 Batch  14291_46523 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 02/15/07 12:53:30 Batch  14291_46411 INIT bckcett testIDS30 u10157 sa
# MAGIC 
# MAGIC JOB NAME:          IdsWAccumDedctMbrProdExtr
# MAGIC  
# MAGIC                           
# MAGIC PROCESSING:    Selects eligible members whose medical or dental products have both a product prefix id of DV and EBCL. 
# MAGIC                             These will be loaded to a working table W_ACCUM_DEDCT_MBR_PROD
# MAGIC                           
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              SAndrew           2007-01-29        Original development

# MAGIC ** Retrieve all currently elig members and their prod_sk.  Join the prod_sk to the hash files to get dv and ebcl product component information.   
# MAGIC ** Members who have both DV and EBCL qualify.
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
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


AccumYear = get_widget_value('AccumYear','2006')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_elig_ebcl = f"""
SELECT distinct
 W_ACCUM.MBR_SK,
 W_ACCUM.MBR_UNIQ_KEY,
 W_ACCUM.SUB_SK,
 W_ACCUM.GRP_SK,
 W_ACCUM.PROD_SK,
 W_ACCUM.MBR_TERM_DT,
 W_ACCUM.MBR_RELSHP_CD,
 CMPNT.PROD_CMPNT_PFX_ID
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM,
     {IDSOwner}.PROD PROD,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.PROD_CMPNT CMPNT
WHERE W_ACCUM.PROD_SK = PROD.PROD_SK
  AND W_ACCUM.MBR_TERM_DT between PROD.PROD_EFF_DT_SK and PROD.PROD_TERM_DT_SK
  AND PROD.PROD_ACCUM_SFX_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ( 'MED' , 'DNTL')
  AND PROD.PROD_SK = CMPNT.PROD_SK
  AND W_ACCUM.MBR_TERM_DT between CMPNT.PROD_CMPNT_EFF_DT_SK and CMPNT.PROD_CMPNT_TERM_DT_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD = 'EBCL'
"""

df_IDS_data_elig_ebcl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_elig_ebcl)
    .load()
)

extract_query_elig_dv = f"""
SELECT distinct
 W_ACCUM.MBR_SK,
 W_ACCUM.MBR_UNIQ_KEY,
 W_ACCUM.SUB_SK,
 W_ACCUM.GRP_SK,
 W_ACCUM.PROD_SK,
 W_ACCUM.MBR_TERM_DT,
 W_ACCUM.MBR_RELSHP_CD,
 CMPNT.PROD_CMPNT_PFX_ID
FROM {IDSOwner}.W_ACCUM_DEDCT_MBR_PROD W_ACCUM,
     {IDSOwner}.PROD PROD,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.PROD_CMPNT CMPNT
WHERE W_ACCUM.PROD_SK = PROD.PROD_SK
  AND W_ACCUM.MBR_TERM_DT between PROD.PROD_EFF_DT_SK and PROD.PROD_TERM_DT_SK
  AND PROD.PROD_ACCUM_SFX_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ( 'MED' , 'DNTL')
  AND PROD.PROD_SK = CMPNT.PROD_SK
  AND W_ACCUM.MBR_TERM_DT between CMPNT.PROD_CMPNT_EFF_DT_SK and CMPNT.PROD_CMPNT_TERM_DT_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD = 'DV'
"""

df_IDS_data_elig_dv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_elig_dv)
    .load()
)

df_bld_term_dt = df_IDS_data_elig_dv.select(
    col("MBR_SK"),
    col("MBR_UNIQ_KEY"),
    col("SUB_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("MBR_TERM_DT"),
    col("MBR_RELSHP_CD"),
    trim(col("PROD_CMPNT_PFX_ID")).alias("PROD_CMPNT_PFX_ID_DV")
)

df_mbr_dv_elig = df_bld_term_dt.dropDuplicates([
    "MBR_SK",
    "MBR_UNIQ_KEY",
    "SUB_SK",
    "GRP_SK",
    "PROD_SK",
    "MBR_TERM_DT",
    "MBR_RELSHP_CD"
])

df_bld_term_dt2 = df_IDS_data_elig_ebcl.select(
    col("MBR_SK"),
    col("MBR_UNIQ_KEY"),
    col("SUB_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("MBR_TERM_DT"),
    col("MBR_RELSHP_CD"),
    trim(col("PROD_CMPNT_PFX_ID")).alias("PROD_CMPNT_PFX_ID_EBCL")
)

df_ebcl = df_bld_term_dt2.dropDuplicates([
    "MBR_SK",
    "MBR_UNIQ_KEY",
    "SUB_SK",
    "GRP_SK",
    "PROD_SK",
    "MBR_TERM_DT",
    "MBR_RELSHP_CD"
])

df_join = df_mbr_dv_elig.alias("mbr_dv_elig").join(
    df_ebcl.alias("ebcl"),
    [
        col("mbr_dv_elig.MBR_SK") == col("ebcl.MBR_SK"),
        col("mbr_dv_elig.MBR_UNIQ_KEY") == col("ebcl.MBR_UNIQ_KEY"),
        col("mbr_dv_elig.SUB_SK") == col("ebcl.SUB_SK"),
        col("mbr_dv_elig.GRP_SK") == col("ebcl.GRP_SK"),
        col("mbr_dv_elig.PROD_SK") == col("ebcl.PROD_SK"),
        col("mbr_dv_elig.MBR_TERM_DT") == col("ebcl.MBR_TERM_DT"),
        col("mbr_dv_elig.MBR_RELSHP_CD") == col("ebcl.MBR_RELSHP_CD")
    ],
    how="left"
).filter(col("ebcl.MBR_SK").isNotNull())

df_load_file = df_join.select(
    col("mbr_dv_elig.MBR_SK").alias("MBR_SK"),
    col("mbr_dv_elig.SUB_SK").alias("SUB_SK"),
    col("mbr_dv_elig.PROD_SK").alias("PROD_SK"),
    lit(AccumYear).alias("DEDCT_YR"),
    col("mbr_dv_elig.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("mbr_dv_elig.GRP_SK").alias("GRP_SK"),
    col("mbr_dv_elig.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    col("mbr_dv_elig.MBR_TERM_DT").alias("MBR_TERM_DT"),
    trim(col("mbr_dv_elig.PROD_CMPNT_PFX_ID_DV")).alias("PROD_CMPNT_PFX_ID_DV"),
    trim(col("ebcl.PROD_CMPNT_PFX_ID_EBCL")).alias("PROD_CMPNT_PFX_ID_EBCL")
)

df_load_file = df_load_file.withColumn("MBR_TERM_DT", rpad(col("MBR_TERM_DT"), 10, " "))
df_load_file = df_load_file.withColumn("DEDCT_YR", rpad(col("DEDCT_YR"), 4, " "))
df_load_file = df_load_file.withColumn("MBR_RELSHP_CD", rpad(col("MBR_RELSHP_CD"), <...>, " "))
df_load_file = df_load_file.withColumn("PROD_CMPNT_PFX_ID_DV", rpad(col("PROD_CMPNT_PFX_ID_DV"), <...>, " "))
df_load_file = df_load_file.withColumn("PROD_CMPNT_PFX_ID_EBCL", rpad(col("PROD_CMPNT_PFX_ID_EBCL"), <...>, " "))

write_files(
    df_load_file,
    f"{adls_path}/load/W_ACCUM_DEDCT_MBR_PROD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)