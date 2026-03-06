# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job Name:  FctsCodeCmpr1Extr
# MAGIC 
# MAGIC Purpose:  Read Facets code tables and compare to CDMA code sets.  Capture differences for emailing metadata analyst.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer	Date		Project/Altiris #		Change Description					Development Project		Reviewer		Review Date
# MAGIC =========================================================================================================================================================================================
# MAGIC Saikiran Subbagari	2018-10-11	Production Support		Original Programming				IntegrateDev2		Abhiram Dasarathy	2018-11-06
# MAGIC Saikiran Subbagari	2018-12-19	Production Support		Join on SRC_DMN_NM                          			IntegrateDev2		Abhiram Dasarathy	2018-12-26
# MAGIC Prabhu ES	2022-02-24	S2S Remediation		MSSQL connection parameters added   			IntegrateDev5                          
# MAGIC 							Change Sybase stage to ODBC
# MAGIC                                                                                                                                                      Added drop temp table in CMC_MCTR_CD_TRANS stage, 
# MAGIC 							After SQL statement
# MAGIC Brent Leland	03-26-2022	Syb 2 SQL                		Added column alias to CMC_MCTR_CD_TRAN		IntegrateDev5		Ken Bradmon	2022-05-02
# MAGIC                                                                                                 			Corrected column length CMC_MECB_COB.MCRE_NAME

# MAGIC Code Domains
# MAGIC 
# MAGIC GROUP EDI ACCOUNT VENDOR
# MAGIC CAPITATION COPAYMENT TYPE
# MAGIC CLAIM LETTER TYPE
# MAGIC PROVIDER MESSAGE
# MAGIC MEMBER COB OTHER CARRIER IDENTIFIER
# MAGIC Compare Facets codes to CDMA and log extra codes in Facets.
# MAGIC 
# MAGIC This process runs in development weekdays and emails the output to Metadata Support.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, concat, rpad, substring
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TempTable = get_widget_value('TempTable','')

# CD_MPPNG (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT SRC_SYS_CD,SRC_DOMAIN_NM,SRC_CD FROM {IDSOwner}.CD_MPPNG WHERE SRC_SYS_CD='FACETS'"
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_CD_MPPNG -> two output links
df_xfm_CD_MPPNG_base = (
    df_CD_MPPNG
    .withColumn("SRC_LKUP_VAL", when(length(trim(col("SRC_CD"))) == 0, lit("@$)")).otherwise(Upcase(trim(col("SRC_CD")))))
    .withColumn("SRC_SYS_CD", when(length(trim(col("SRC_SYS_CD"))) == 0, lit("FACETS")).otherwise(col("SRC_SYS_CD")))
    .withColumn("SRC_DMN_NM", trim(col("SRC_DOMAIN_NM")))
    .withColumn("Dummy", lit("1"))
)

df_xfm_CD_MPPNG_out_1 = df_xfm_CD_MPPNG_base.select(
    col("SRC_LKUP_VAL"),
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("Dummy")
)

df_xfm_CD_MPPNG_out_2 = df_xfm_CD_MPPNG_base.select(
    col("SRC_SYS_CD"),
    col("SRC_DMN_NM"),
    col("SRC_LKUP_VAL"),
    col("Dummy")
)

# CD_MPPNG_Ext (PxSequentialFile)
write_files(
    df_xfm_CD_MPPNG_out_2.select("SRC_SYS_CD","SRC_DMN_NM","SRC_LKUP_VAL","Dummy"),
    f"{adls_path}/load/CDMA_CD_MPPNG.txt",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# cp_CD_MPPNG (PxCopy)
df_cp_CD_MPPNG = df_xfm_CD_MPPNG_out_1.select("SRC_LKUP_VAL","SRC_SYS_CD","SRC_DMN_NM","Dummy")

# CMC_MCRE_RELAT_ENT (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT DISTINCT
               gmr.MCRE_ID,
               gmr.MCRE_NAME
FROM {FacetsOwner}.CMC_GRGR_GROUP g,
     {FacetsOwner}.CMC_GRRE_RELATION gr,
     {FacetsOwner}.CMC_MCRE_RELAT_ENT gmr
WHERE gr.GRRE_CATEGORY = 'OT'
  AND g.GRGR_CK = gr.GRGR_CK
  AND gr.GRRE_MCTR_TYPE = 'EDIV'
  AND gr.GRRE_EFF_DT <= getdate()
  AND gr.GRRE_TERM_DT >= getdate()
  AND gr.MCRE_GRRE_ID = gmr.MCRE_ID
"""
df_CMC_MCRE_RELAT_ENT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_trim_MCRE_ID
df_xfm_trim_MCRE_ID = (
    df_CMC_MCRE_RELAT_ENT
    .withColumn("SRC_LKUP_VAL", Upcase(trim(col("MCRE_ID"))))
    .withColumn("SRC_DMN_NM", lit("GROUP EDI ACCOUNT VENDOR"))
    .withColumn("MCRE_NAME", rpad(col("MCRE_NAME"), 50, " "))
)

# join_EDI_Vendor (leftouterjoin)
df_join_EDI_Vendor = df_xfm_trim_MCRE_ID.alias("lnk_xfm_trim_MCRE_ID").join(
    df_cp_CD_MPPNG.alias("lnk_join_EDI_Vendor_in"),
    (col("lnk_xfm_trim_MCRE_ID.SRC_LKUP_VAL") == col("lnk_join_EDI_Vendor_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trim_MCRE_ID.SRC_DMN_NM") == col("lnk_join_EDI_Vendor_in.SRC_DMN_NM")),
    "left"
)

df_join_EDI_Vendor_out = df_join_EDI_Vendor.select(
    col("lnk_xfm_trim_MCRE_ID.SRC_LKUP_VAL").alias("SRC_LKUP_VAL"),
    col("lnk_xfm_trim_MCRE_ID.SRC_DMN_NM").alias("SRC_DMN_NM"),
    col("lnk_xfm_trim_MCRE_ID.MCRE_NAME").alias("MCRE_NAME"),
    col("lnk_join_EDI_Vendor_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_join_EDI_Vendor_in.Dummy").alias("Dummy")
)

# xfm_EDI_Vendor
df_EDI_Vendor = (
    df_join_EDI_Vendor_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("SRC_LKUP_VAL"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("MCRE_NAME").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(lit("GROUP EDI ACCOUNT VENDOR"), 80, " "))
)

df_EDI_Vendor_out = df_EDI_Vendor.select("Code","Description","Source_System","Domain")

# CMC_BSDE_DESC (ODBCConnectorPX)
extract_query = f"""
SELECT DISTINCT
               BSDE_TYPE,
               BSDE_DESC
FROM {FacetsOwner}.CMC_BSDE_DESC
WHERE BSDE_REC_TYPE = 'BSDL'
"""
df_CMC_BSDE_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_trim_BSDE_TYPE
df_xfm_trim_BSDE_TYPE = (
    df_CMC_BSDE_DESC
    .withColumn("SRC_LKUP_VAL", Upcase(trim(col("BSDE_TYPE"))))
    .withColumn("BSDE_DESC", col("BSDE_DESC"))
    .withColumn("SRC_DMN_NM", lit("CAPITATION COPAYMENT TYPE"))
)

# join_CAP_COPAY (leftouterjoin)
df_join_CAP_COPAY = df_xfm_trim_BSDE_TYPE.alias("lnk_xfm_trim_BSDE_TYPE_out").join(
    df_cp_CD_MPPNG.alias("lnk_CAP_COPAY_in"),
    (col("lnk_xfm_trim_BSDE_TYPE_out.SRC_LKUP_VAL") == col("lnk_CAP_COPAY_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trim_BSDE_TYPE_out.SRC_DMN_NM") == col("lnk_CAP_COPAY_in.SRC_DMN_NM")),
    "left"
)

df_join_CAP_COPAY_out = df_join_CAP_COPAY.select(
    col("lnk_xfm_trim_BSDE_TYPE_out.SRC_LKUP_VAL").alias("BSDE_TYPE"),
    col("lnk_xfm_trim_BSDE_TYPE_out.BSDE_DESC").alias("BSDE_DESC"),
    col("lnk_CAP_COPAY_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_CAP_COPAY_in.Dummy").alias("Dummy")
)

# xfm_CAP_COPAY
df_CAP_COPAY = (
    df_join_CAP_COPAY_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("BSDE_TYPE"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("BSDE_DESC").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(lit("CAPITATION COPAYMENT TYPE"), 80, " "))
)

df_CAP_COPAY_out = df_CAP_COPAY.select("Code","Description","Source_System","Domain")

# CER_ATLD_LET_DOC_D (ODBCConnectorPX)
extract_query = f"""
SELECT DISTINCT
               ATLD_ID,
               ATLD_DESC
FROM {FacetsOwner}.CER_ATLD_LET_DOC_D
"""
df_CER_ATLD_LET_DOC_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_trim_ATLD_ID
df_xfm_trim_ATLD_ID = (
    df_CER_ATLD_LET_DOC_D
    .withColumn("SRC_LKUP_VAL", Upcase(trim(col("ATLD_ID"))))
    .withColumn("ATLD_DESC", col("ATLD_DESC"))
    .withColumn("SRC_DMN_NM", lit("CLAIM LETTER TYPE"))
)

# Join_72 (leftouterjoin)
df_Join_72 = df_xfm_trim_ATLD_ID.alias("lnk_xfm_trim_ATLD_ID_out").join(
    df_cp_CD_MPPNG.alias("lnk_LETTER_TYPE_in"),
    (col("lnk_xfm_trim_ATLD_ID_out.SRC_LKUP_VAL") == col("lnk_LETTER_TYPE_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trim_ATLD_ID_out.SRC_DMN_NM") == col("lnk_LETTER_TYPE_in.SRC_DMN_NM")),
    "left"
)

df_Join_72_out = df_Join_72.select(
    col("lnk_xfm_trim_ATLD_ID_out.SRC_LKUP_VAL").alias("ATLD_ID"),
    col("lnk_xfm_trim_ATLD_ID_out.ATLD_DESC").alias("ATLD_DESC"),
    col("lnk_LETTER_TYPE_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_LETTER_TYPE_in.Dummy").alias("Dummy")
)

# xfm_LETTER_TYPE
df_LETTER_TYPE = (
    df_Join_72_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("ATLD_ID"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("ATLD_DESC").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(lit("CLAIM LETTER TYPE"), 80, " "))
)

df_LETTER_TYPE_out = df_LETTER_TYPE.select("Code","Description","Source_System","Domain")

# CMC_WMDS_DESC (ODBCConnectorPX)
extract_query = f"""
SELECT DISTINCT
               WMDS_SEQ_NO,
               WMDS_TEXT1
FROM {FacetsOwner}.CMC_WMDS_DESC
WHERE WMDS_REC_TYPE = 'PRPR'
"""
df_CMC_WMDS_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_trim_WMDS_SEQ_NO
df_xfm_trim_WMDS_SEQ_NO = (
    df_CMC_WMDS_DESC
    .withColumn("SRC_LKUP_VAL", Upcase(trim(col("WMDS_SEQ_NO"))))
    .withColumn("WMDS_TEXT1", rpad(col("WMDS_TEXT1"), 70, " "))
    .withColumn("SRC_DMN_NM", lit("PROVIDER MESSAGE"))
)

# join_PROVIDER_MSG (leftouterjoin)
df_join_PROVIDER_MSG = df_xfm_trim_WMDS_SEQ_NO.alias("lnk_xfm_trim_WMDS_SEQ_NO_out").join(
    df_cp_CD_MPPNG.alias("lnk_PROVIDER_MSG_in"),
    (col("lnk_xfm_trim_WMDS_SEQ_NO_out.SRC_LKUP_VAL") == col("lnk_PROVIDER_MSG_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trim_WMDS_SEQ_NO_out.SRC_DMN_NM") == col("lnk_PROVIDER_MSG_in.SRC_DMN_NM")),
    "left"
)

df_join_PROVIDER_MSG_out = df_join_PROVIDER_MSG.select(
    col("lnk_xfm_trim_WMDS_SEQ_NO_out.SRC_LKUP_VAL").alias("WMDS_SEQ_NO"),
    col("lnk_xfm_trim_WMDS_SEQ_NO_out.WMDS_TEXT1").alias("WMDS_TEXT1"),
    col("lnk_PROVIDER_MSG_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_PROVIDER_MSG_in.Dummy").alias("Dummy")
)

# xfm_PROVIDER_MSG
df_PROVIDER_MSG = (
    df_join_PROVIDER_MSG_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("WMDS_SEQ_NO"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("WMDS_TEXT1").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(lit("PROVIDER MESSAGE"), 80, " "))
)

df_PROVIDER_MSG_out = df_PROVIDER_MSG.select("Code","Description","Source_System","Domain")

# CMC_MCTR_CD_TRANS (ODBCConnectorPX)
extract_query = f"""
SELECT DISTINCT
               m.MCTR_VALUE,
               m.MCTR_DESC,
               t.CDMA_DOMAIN as DOMAIN_NM
FROM {FacetsOwner}.CMC_MCTR_CD_TRANS m,
     tempdb..#{TempTable}#   t
WHERE m.MCTR_ENTITY = t.MCTR_ENTITY
  AND m.MCTR_TYPE   = t.MCTR_TYPE
"""
df_CMC_MCTR_CD_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_MCTR_VALUE
df_xfm_MCTR_VALUE = (
    df_CMC_MCTR_CD_TRANS
    .withColumn("SRC_LKUP_VAL", when(length(trim(col("MCTR_VALUE"))) == 0, lit("@$)")).otherwise(Upcase(trim(col("MCTR_VALUE")))))
    .withColumn("MCTR_DESC", col("MCTR_DESC"))
    .withColumn("SRC_DMN_NM", trim(col("DOMAIN_NM")))
)

# join_CODE_TYPES (leftouterjoin)
df_join_CODE_TYPES = df_xfm_MCTR_VALUE.alias("lnk_xfm_trm_MCTR_VALUE_out").join(
    df_cp_CD_MPPNG.alias("lnk_CODE_TYPES_in"),
    (col("lnk_xfm_trm_MCTR_VALUE_out.SRC_LKUP_VAL") == col("lnk_CODE_TYPES_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trm_MCTR_VALUE_out.SRC_DMN_NM") == col("lnk_CODE_TYPES_in.SRC_DMN_NM")),
    "left"
)

df_join_CODE_TYPES_out = df_join_CODE_TYPES.select(
    col("lnk_xfm_trm_MCTR_VALUE_out.SRC_LKUP_VAL").alias("MCTR_VALUE"),
    col("lnk_xfm_trm_MCTR_VALUE_out.MCTR_DESC").alias("MCTR_DESC"),
    col("lnk_xfm_trm_MCTR_VALUE_out.SRC_DMN_NM").alias("DOMAIN_NM"),
    col("lnk_CODE_TYPES_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_CODE_TYPES_in.Dummy").alias("Dummy")
)

# xfm_CODE_TYPES
df_CODE_TYPES = (
    df_join_CODE_TYPES_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("MCTR_VALUE"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("MCTR_DESC").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(col("DOMAIN_NM"), 80, " "))
)

df_CODE_TYPES_out = df_CODE_TYPES.select("Code","Description","Source_System","Domain")

# CMC_MECB_COB (ODBCConnectorPX)
extract_query = f"""
SELECT DISTINCT
               c.MCRE_ID as MCRE_ID,
               e.MCRE_NAME as MCRE_NAME
FROM {FacetsOwner}.CMC_MECB_COB c,
     {FacetsOwner}.CMC_MCRE_RELAT_ENT e
WHERE e.MCRE_ID = c.MCRE_ID
  AND e.MCRE_TYPE = 'CC'
"""
df_CMC_MECB_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# xfm_trm_MCRE_ID
df_xfm_trm_MCRE_ID = (
    df_CMC_MECB_COB
    .withColumn("SRC_LKUP_VAL", Upcase(trim(col("MCRE_ID"))))
    .withColumn("MCRE_NAME", rpad(col("MCRE_NAME"), 70, " "))
    .withColumn("SRC_DMN_NM", lit("MEMBER COB OTHER CARRIER IDENTIFIER"))
)

# join_OTH_COB_CAR (leftouterjoin)
df_join_OTH_COB_CAR = df_xfm_trm_MCRE_ID.alias("lnk_xfm_trm_MCRE_ID_out").join(
    df_cp_CD_MPPNG.alias("lnk_OTH_COB_CAR_in"),
    (col("lnk_xfm_trm_MCRE_ID_out.SRC_LKUP_VAL") == col("lnk_OTH_COB_CAR_in.SRC_LKUP_VAL")) &
    (col("lnk_xfm_trm_MCRE_ID_out.SRC_DMN_NM") == col("lnk_OTH_COB_CAR_in.SRC_DMN_NM")),
    "left"
)

df_join_OTH_COB_CAR_out = df_join_OTH_COB_CAR.select(
    col("lnk_xfm_trm_MCRE_ID_out.SRC_LKUP_VAL").alias("MCRE_ID"),
    col("lnk_xfm_trm_MCRE_ID_out.MCRE_NAME").alias("MCRE_NAME"),
    col("lnk_OTH_COB_CAR_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_OTH_COB_CAR_in.Dummy").alias("Dummy")
)

# xfm_OTH_COB_CAR
df_OTH_COB_CAR = (
    df_join_OTH_COB_CAR_out
    .filter(trim(col("Dummy")) != "1")
    .withColumn("Code", concat(lit("|"), col("MCRE_ID"), lit("|")))
    .withColumn("Description", rpad(Upcase(col("MCRE_NAME").substr(1,80)), 80, " "))
    .withColumn("Source_System", lit("FACETS"))
    .withColumn("Domain", rpad(lit("MEMBER COB OTHER CARRIER IDENTIFIER"), 80, " "))
)

df_OTH_COB_CAR_out = df_OTH_COB_CAR.select("Code","Description","Source_System","Domain")

# fl_New_Codes (PxFunnel)
df_fl_New_Codes = (
    df_CAP_COPAY_out
    .unionByName(df_LETTER_TYPE_out)
    .unionByName(df_PROVIDER_MSG_out)
    .unionByName(df_OTH_COB_CAR_out)
    .unionByName(df_CODE_TYPES_out)
    .unionByName(df_EDI_Vendor_out)
    .orderBy("Domain","Code")
)

# CDMA_Code_Diff (PxSequentialFile)
write_files(
    df_fl_New_Codes.select("Code","Description","Source_System","Domain"),
    f"{adls_path}/load/processed/CDMA_Code_Diff.{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote=None,
    nullValue=None
)