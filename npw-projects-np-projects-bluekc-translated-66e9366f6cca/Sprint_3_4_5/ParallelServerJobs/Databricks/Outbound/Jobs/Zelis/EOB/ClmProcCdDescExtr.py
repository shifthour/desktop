# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Proc code record data from Facets and generates a data file for RedCard.
# MAGIC 
# MAGIC Job Name: ClmProcCdDescExtr
# MAGIC Called By:RedCardEobExtrCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #      Change Description                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                           --------------------     ------------------------      -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                     2020-12-05            RedCard                     Original Devlopment                                                                OutboundDev3               Jaideep Mankala       12/27/2020
# MAGIC Raja Gummadi                     2021-02-04            343913                 Added void logic                                                                            OutboundDev3                  Jaideep Mankala        02/04/2021
# MAGIC Megan Conway                 2022-03-09               	                      S2S Remediation - MSSQL connection parameters added	   OutboundDev5	Ken Bradmon	2022-05-19


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CLMPDDT = get_widget_value('CLMPDDT','')

schema_Sequential_File_14 = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("CDOCID", StringType(), False)
])

df_Sequential_File_14 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_Sequential_File_14)
    .load(f"{adls_path_publish}/external/Zelis_EOB_temp.dat")
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

extract_query_CLM = f"""
SELECT
CDML.CLCL_ID,
CDML.CDML_SEQ_NO,
SEDS.SESE_ID,
SEDS.SEDS_DESC,
CLCL.CLCL_CL_SUB_TYPE,
CDML.CDML_POS_IND,
CCCC.CKPY_REF_ID,
CDML.IPCD_ID
FROM
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CDML_CL_LINE CDML,
{FacetsOwner}.CMC_SEDS_SE_DESC SEDS,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC
WHERE
CLCL.CLCL_ID = CDML.CLCL_ID 
AND CDML.SESE_ID = SEDS.SESE_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.CLCL_CL_TYPE = 'M'
AND CLCL.CLCL_PAID_DT  IN (
  SELECT 
    (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) 
  FROM (
    SELECT 
      (CASE WHEN MAX(CKPY_PAY_DT)IS NULL 
      THEN (
        SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
        FROM CMC_BPID_INDIC 
        WHERE SYIN_REF_ID IN (
          SELECT SYIN_REF_ID 
          FROM CER_SYIN_INST 
          WHERE SYIN_INST IN (
            SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
            FROM  CER_SYIN_INST  CER_SYIN_INST 
            WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
          )
        )
      )  
      ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
    FROM CMC_BPID_INDIC 
    WHERE SYIN_INST IN (
      SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
      FROM  CER_SYIN_INST  CER_SYIN_INST 
      WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
    )
  )A
)
UNION
SELECT
CDML.CLCL_ID,
CDML.CDDL_SEQ_NO AS CDML_SEQ_NO,
CGDS.CGCG_ID AS SESE_ID,
CGDS.CGDS_DESC AS SEDS_DESC,
CLCL.CLCL_CL_SUB_TYPE,
' ' AS CDML_POS_IND,
CCCC.CKPY_REF_ID,
' ' AS IPCD_ID
FROM
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CDDL_CL_LINE CDML,
{FacetsOwner}.CMC_CGDS_DESC CGDS,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC
WHERE
CLCL.CLCL_ID = CDML.CLCL_ID 
AND CDML.CGCG_ID = CGDS.CGCG_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.CLCL_CL_TYPE = 'D'
AND CLCL.CLCL_PAID_DT  IN (
  SELECT 
    (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) 
  FROM (
    SELECT 
      (CASE WHEN MAX(CKPY_PAY_DT)IS NULL 
      THEN (
        SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
        FROM CMC_BPID_INDIC 
        WHERE SYIN_REF_ID IN (
          SELECT SYIN_REF_ID 
          FROM CER_SYIN_INST 
          WHERE SYIN_INST IN (
            SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
            FROM  CER_SYIN_INST  CER_SYIN_INST 
            WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
          )
        )
      )  
      ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
    FROM CMC_BPID_INDIC 
    WHERE SYIN_INST IN (
      SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
      FROM  CER_SYIN_INST  CER_SYIN_INST 
      WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
    )
  )A
)
"""

df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CLM)
    .load()
)

extract_query_Copy_of_CLM = f"""
SELECT
CDML.CLCL_ID,
CDML.CDML_SEQ_NO,
SEDS.SESE_ID,
SEDS.SEDS_DESC,
CLCL.CLCL_CL_SUB_TYPE,
CDML.CDML_POS_IND,
CCCC.CKPY_REF_ID,
CDML.IPCD_ID
FROM
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CDML_CL_LINE CDML,
{FacetsOwner}.CMC_SEDS_SE_DESC SEDS,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC
WHERE
CLCL.CLCL_ID = CDML.CLCL_ID 
AND CDML.SESE_ID = SEDS.SESE_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.CLCL_CL_TYPE = 'M'
AND CCCC.CKPY_REF_ID IN (
  SELECT DISTINCT CBI.CKPY_REF_ID 
  FROM {FacetsOwner}.CMC_BPID_INDIC CBI 
  WHERE CBI.BPID_PRINTED_DT IN (
    SELECT 
      (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) 
    FROM (
      SELECT 
        (CASE WHEN MAX(CKPY_PAY_DT)IS NULL 
        THEN (
          SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
          FROM CMC_BPID_INDIC 
          WHERE SYIN_REF_ID IN (
            SELECT SYIN_REF_ID 
            FROM CER_SYIN_INST 
            WHERE SYIN_INST IN (
              SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
              FROM  CER_SYIN_INST  CER_SYIN_INST 
              WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
            )
          )
        )  
        ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
      FROM CMC_BPID_INDIC 
      WHERE SYIN_INST IN (
        SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
        FROM  CER_SYIN_INST  CER_SYIN_INST 
        WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
      )
    )A
  )
)
UNION
SELECT
CDML.CLCL_ID,
CDML.CDDL_SEQ_NO AS CDML_SEQ_NO,
CGDS.CGCG_ID AS SESE_ID,
CGDS.CGDS_DESC AS SEDS_DESC,
CLCL.CLCL_CL_SUB_TYPE,
' ' AS CDML_POS_IND,
CCCC.CKPY_REF_ID,
' ' AS IPCD_ID
FROM
{FacetsOwner}.CMC_CLCL_CLAIM CLCL,
{FacetsOwner}.CMC_CDDL_CL_LINE CDML,
{FacetsOwner}.CMC_CGDS_DESC CGDS,
{FacetsOwner}.CMC_CLCK_CLM_CHECK CCCC
WHERE
CLCL.CLCL_ID = CDML.CLCL_ID 
AND CDML.CGCG_ID = CGDS.CGCG_ID
AND CCCC.CLCL_ID = CLCL.CLCL_ID
AND CLCL.CLCL_CL_TYPE = 'D'
AND CCCC.CKPY_REF_ID IN (
  SELECT DISTINCT CBI.CKPY_REF_ID 
  FROM {FacetsOwner}.CMC_BPID_INDIC CBI 
  WHERE CBI.BPID_PRINTED_DT IN (
    SELECT 
      (CASE WHEN '{CLMPDDT}' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '{CLMPDDT}' END) 
    FROM (
      SELECT 
        (CASE WHEN MAX(CKPY_PAY_DT)IS NULL 
        THEN (
          SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
          FROM CMC_BPID_INDIC 
          WHERE SYIN_REF_ID IN (
            SELECT SYIN_REF_ID 
            FROM CER_SYIN_INST 
            WHERE SYIN_INST IN (
              SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
              FROM  CER_SYIN_INST  CER_SYIN_INST 
              WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
            )
          )
        )  
        ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
      FROM CMC_BPID_INDIC 
      WHERE SYIN_INST IN (
        SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
        FROM  CER_SYIN_INST  CER_SYIN_INST 
        WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'
      )
    )A
  )
)
"""

df_Copy_of_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Copy_of_CLM)
    .load()
)

df_funnel_21 = df_CLM.select(
    "CLCL_ID",
    "CDML_SEQ_NO",
    "SESE_ID",
    "SEDS_DESC",
    "CLCL_CL_SUB_TYPE",
    "CDML_POS_IND",
    "CKPY_REF_ID",
    "IPCD_ID"
).union(
    df_Copy_of_CLM.select(
        "CLCL_ID",
        "CDML_SEQ_NO",
        "SESE_ID",
        "SEDS_DESC",
        "CLCL_CL_SUB_TYPE",
        "CDML_POS_IND",
        "CKPY_REF_ID",
        "IPCD_ID"
    )
)

df_remove_duplicates_16 = dedup_sort(df_funnel_21, ["CLCL_ID","SESE_ID"], [])

df_lookup_15 = (
    df_remove_duplicates_16.alias("lnk_meme_ck")
    .join(
        df_Sequential_File_14.alias("DSLink16"),
        F.col("lnk_meme_ck.CLCL_ID") == F.col("DSLink16.CLCL_ID"),
        "inner"
    )
    .select(
        F.col("lnk_meme_ck.CLCL_ID").alias("CLCL_ID"),
        F.col("lnk_meme_ck.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("DSLink16.CDOCID").alias("CDOCID"),
        F.col("lnk_meme_ck.SESE_ID").alias("SESE_ID"),
        F.col("lnk_meme_ck.SEDS_DESC").alias("SEDS_DESC"),
        F.col("lnk_meme_ck.CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE"),
        F.col("lnk_meme_ck.CDML_POS_IND").alias("CDML_POS_IND")
    )
)

df_transformer_1 = (
    df_lookup_15
    .withColumn(
        "svProcCd",
        F.when(
            F.col("CLCL_CL_SUB_TYPE") == 'H',
            F.when(F.col("CDML_POS_IND") == 'I', 'INP')
            .when(F.col("CDML_POS_IND") == 'O', 'OUT')
            .otherwise('NONE')
        ).otherwise(F.col("SESE_ID"))
    )
    .withColumn(
        "svProcCdDesc",
        F.when(F.col("svProcCd") == 'NONE', 'NONE')
        .when(
            F.col("CLCL_CL_SUB_TYPE") == 'H',
            F.when(F.col("CDML_POS_IND") == 'I', 'Inpatient Care')
            .when(F.col("CDML_POS_IND") == 'O', 'Outpatient Ancillary Services')
            .otherwise(F.col("SEDS_DESC"))
        )
        .otherwise(F.col("SEDS_DESC"))
    )
    .select(
        F.lit('20').alias("CRCRDTYP"),
        F.lit('02').alias("CRCRDVRSN"),
        F.col("CDOCID").alias("CDOCID"),
        F.col("svProcCd").alias("CPROCCD"),
        F.col("svProcCdDesc").alias("CPROCCDDESC")
    )
)

df_remove_duplicates_19 = dedup_sort(df_transformer_1, ["CDOCID","CPROCCD"], [])

df_final = df_remove_duplicates_19.select(
    F.rpad(F.col("CRCRDTYP"), 2, " ").alias("CRCRDTYP"),
    F.rpad(F.col("CRCRDVRSN"), 2, " ").alias("CRCRDVRSN"),
    F.rpad(F.col("CDOCID"), 25, " ").alias("CDOCID"),
    F.rpad(F.col("CPROCCD"), 10, " ").alias("CPROCCD"),
    F.rpad(F.col("CPROCCDDESC"), 1000, " ").alias("CPROCCDDESC")
)

write_files(
    df_final,
    f"{adls_path_publish}/external/Zelis_EOBClaimProcCdDesc20.dat",
    delimiter="\t",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)