# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 06/19/09 14:44:48 Batch  15146_53151 PROMOTE bckcetl:31540 ids20 dsadm rc for steph 
# MAGIC ^1_2 06/19/09 14:29:48 Batch  15146_52211 INIT bckcett:31540 testIDS dsadm rc for steph 
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/19/07 16:25:25 Batch  14568_59149 PROMOTE bckcetl ids20 dsadm rc for oliver 
# MAGIC ^1_1 11/19/07 16:20:39 Batch  14568_58862 INIT bckcett testIDS30 dsadm rc for oliver 
# MAGIC ^1_2 11/13/07 13:18:55 Batch  14562_47947 PROMOTE bckcett testIDS30 u03651 steph for Ollie
# MAGIC ^1_2 11/13/07 12:57:35 Batch  14562_46657 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 11/06/07 11:53:28 Batch  14555_42843 INIT bckcett devlIDS30 u10913 Ollie move Privacy from devlIDS30 to testIDS30
# MAGIC ^1_2 06/06/07 09:07:58 Batch  14402_32891 PROMOTE bckcett devlIDS30 u10913 Ollie move from testIDSnew to devl for Naren
# MAGIC ^1_2 06/06/07 09:05:36 Batch  14402_32750 INIT bckcett testIDSnew u10913 Ollie move from testNEW to devl for Naren
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsPrvcyMbrRelshpExtr
# MAGIC 
# MAGIC PROCESSING: Pulling data from FHP_PMER_MEMBER_R
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari               3/10/2007            CDS Sunset/3279          Originally Programmed                              devlIDS30                   Steph Goddard         3/29/2007       
# MAGIC Naren Gerapaty               6/2007              Prod Supp                     Added FHD_EXFM lookup to Facets          devlIDS30           
# MAGIC Bhoomi Dasari                2/18/2009           Prod Supp/15                 Made logic changes to                             devlIDS
# MAGIC                                                                                                           'PRVCY_MBR_SRC_CD_SK'
# MAGIC Bhoomi Dasari               2/25/2009           Prod Supp/15                 Added 'RunDate' paramter instead            devlIDS                      Steph Goddard         02/28/2009
# MAGIC                                                                                                          of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari              3/16/2009           ProdSupp/15                  Updated Mbr_sk &                                   devlIDS                          Steph Goddard        04/01/2009
# MAGIC                                                                                                         Prvcy_Mbr_Src_Cd_Sk 
# MAGIC 
# MAGIC Steph Goddard            4/2/10                    3556 CDC                   changed IDS lookup to Facets,               IntegrateCurDevl              SAndrew                 04/07/2010
# MAGIC                                                                                                       avoiding having to use CDC tables
# MAGIC                                                                                                       changed SQL to only pull records needed 
# MAGIC Steph Goddard           7/14/10                TTR-689                       changed primary key counter from IDS_SK   RebuildIntNewDevl
# MAGIC                                                                                                       to PRVCY_MBR_RELSHP_SK
# MAGIC 
# MAGIC Anoop Nair                2022-03-08         S2S Remediation      Added FACETS DSN Connection parameters      IntegrateDev5		Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, trim, upper, length, rpad, regexp_replace, date_format
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_1 = f"""
SELECT 
PMER.PMED_CKE,
PMER.PMER_SEQ_NO,
PMER.PMER_CREATE_DTM,
PMER.PMER_EFF_DT,
PMER.PMER_TERM_DTM,
PMER.PMER_PZCD_TYPE,
PMER.PMER_LAST_UPD_DTM,
PMER.PMER_LAST_USUS_ID,
PMER.PMER_ENEN_CKE

FROM 
{FacetsOwner}.FHP_PMER_MEMBER_R PMER,
{FacetsOwner}.FHP_PMPR_REP_D PMPR

WHERE
PMER.PMER_ENEN_CKE = PMPR.PMPR_CKE
"""

df_ODBC_Connector_116_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = f"""
SELECT ENEN.ENEN_CKE,
       ENEN.EXEN_REC 
FROM {FacetsOwner}.FHD_ENEN_ENTITY_D ENEN,
     {FacetsOwner}.FHP_PMER_MEMBER_R PMER,
     {FacetsOwner}.FHP_PMPR_REP_D PMPR
WHERE ENEN.EXEN_REC = 0 
  AND ENEN.ENEN_CKE = PMER.PMED_CKE 
  AND PMER.PMER_ENEN_CKE = PMPR.PMPR_CKE
"""

df_ODBC_Connector_116_EntityExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_2)
    .load()
)

extract_query_3 = f"""
SELECT PMER.PMED_CKE,
       EXFM.MEME_CK 
FROM {FacetsOwner}.FHP_PMER_MEMBER_R PMER,
     {FacetsOwner}.FHD_EXEN_BASE_D EXEN,
     {FacetsOwner}.FHD_EXFM_FA_MEMB_D EXFM,
     {FacetsOwner}.FHP_PMPR_REP_D PMPR
WHERE PMER.PMED_CKE=EXEN.ENEN_CKE  
  AND EXEN.EXEN_REC=EXFM.EXEN_REC 
  AND PMER.PMER_ENEN_CKE = PMPR.PMPR_CKE
"""

df_ODBC_Connector_116_FHD_EXFM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_3)
    .load()
)

df_StripFields = df_ODBC_Connector_116_Extract.select(
    col("PMED_CKE").alias("PMED_CKE"),
    col("PMER_SEQ_NO").alias("PMER_SEQ_NO"),
    date_format(col("PMER_CREATE_DTM"), "yyyy-MM-dd").alias("PMER_CREATE_DTM"),
    date_format(col("PMER_EFF_DT"), "yyyy-MM-dd").alias("PMER_EFF_DT"),
    date_format(col("PMER_TERM_DTM"), "yyyy-MM-dd").alias("PMER_TERM_DTM"),
    regexp_replace(col("PMER_PZCD_TYPE"), "[\\x0A\\x0D\\x09]", "").alias("PMER_PZCD_TYPE"),
    regexp_replace(col("PMER_LAST_USUS_ID"), "[\\x0A\\x0D\\x09]", "").alias("PMER_LAST_USUS_ID"),
    date_format(col("PMER_LAST_UPD_DTM"), "yyyy-MM-dd").alias("PMER_LAST_UPD_DTM"),
    col("PMER_ENEN_CKE").alias("PMER_ENEN_CKE")
)

df_hf_prvcy_mbr_relshp_memeck_lkup = spark.read.parquet(f"{adls_path}/hf_prvcy_mbr_relshp_memeck_lkup.parquet")

df_hf_pvcy_mbr_relshp_enen_lkup = spark.read.parquet(f"{adls_path}/hf_pvcy_mbr_relshp_enen_lkup.parquet")

df_BusinessRules_tmp = (
    df_StripFields.alias("Strip")
    .join(
        df_hf_prvcy_mbr_relshp_memeck_lkup.alias("meme_lookup"),
        on=[col("Strip.PMED_CKE") == col("meme_lookup.PMED_CKE")],
        how="left"
    )
    .join(
        df_hf_pvcy_mbr_relshp_enen_lkup.alias("EntityLkup"),
        on=[col("Strip.PMED_CKE") == col("EntityLkup.ENEN_CKE")],
        how="left"
    )
)

df_BusinessRules = df_BusinessRules_tmp.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(RunDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    (lit("FACETS") + lit(";") + col("Strip.PMED_CKE").cast(StringType()) + lit(";") + col("Strip.PMER_SEQ_NO").cast(StringType())).alias("PRI_KEY_STRING"),
    lit(0).alias("PRVCY_MBR_RELSHP_SK"),
    col("Strip.PMED_CKE").alias("PRVCY_MBR_UNIQ_KEY"),
    col("Strip.PMER_SEQ_NO").alias("SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("meme_lookup.PMED_CKE").isNull() | (trim(col("meme_lookup.PMED_CKE")) == ""), lit("NA"))
    .otherwise(col("meme_lookup.MEME_CK")).alias("MBR_SK"),
    when((col("Strip.PMED_CKE") == 0) | col("Strip.PMED_CKE").isNull(), lit("NA"))
    .otherwise(col("Strip.PMED_CKE")).alias("PRVCY_EXTRNL_MBR_SK"),
    col("Strip.PMER_ENEN_CKE").alias("PRVCY_PRSNL_REP_SK"),
    col("Strip.PMER_CREATE_DTM").alias("CRT_DT_SK"),
    col("Strip.PMER_EFF_DT").alias("EFF_DT_SK"),
    col("Strip.PMER_TERM_DTM").alias("TERM_DT_SK"),
    when((trim(col("Strip.PMER_PZCD_TYPE")) == "") | col("Strip.PMER_PZCD_TYPE").isNull(), lit("NA"))
    .otherwise(upper(trim(col("Strip.PMER_PZCD_TYPE")))).alias("PRSNL_REP_RELSHP_CD_SK"),
    when(col("EntityLkup.ENEN_CKE").isNotNull(), lit("N")).otherwise(lit("F")).alias("PRVCY_MBR_SRC_CD_SK"),
    col("Strip.PMER_LAST_UPD_DTM").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("Strip.PMER_LAST_USUS_ID").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    col("Strip.PMER_ENEN_CKE").alias("PRVCY_PRSNL_REP_UNIQ_KEY")
)

df_hf_prvcy_mbr_relshp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, PRVCY_MBR_RELSHP_SK FROM dummy_hf_prvcy_mbr_relshp")
    .load()
)

df_PrimaryKey_tmp = df_BusinessRules.alias("Transform").join(
    df_hf_prvcy_mbr_relshp.alias("lkup"),
    on=[
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.PRVCY_MBR_UNIQ_KEY") == col("lkup.PRVCY_MBR_UNIQ_KEY"),
        col("Transform.SEQ_NO") == col("lkup.SEQ_NO")
    ],
    how="left"
)

df_PrimaryKey = df_PrimaryKey_tmp.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("lkup.PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
    col("Transform.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    col("Transform.SEQ_NO").alias("SEQ_NO"),
    when(col("lkup.PRVCY_MBR_RELSHP_SK").isNull(), lit(CurrRunCycle))
    .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.MBR_SK").alias("MBR_SK"),
    col("Transform.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    col("Transform.PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
    col("Transform.CRT_DT_SK").alias("CRT_DT_SK"),
    col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Transform.TERM_DT_SK").alias("TERM_DT_SK"),
    col("Transform.PRSNL_REP_RELSHP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
    when(col("Transform.PRVCY_MBR_SRC_CD_SK") == "N", lit("N")).otherwise(lit("F")).alias("PRVCY_MBR_SRC_CD_SK"),
    col("Transform.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("Transform.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK")
)

df_enriched = df_PrimaryKey.withColumn("PRVCY_MBR_RELSHP_SK", col("PRVCY_MBR_RELSHP_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_MBR_RELSHP_SK",<schema>,<secret_name>)

df_key = df_enriched.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), <...>, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PRVCY_MBR_RELSHP_SK"),
    col("PRVCY_MBR_UNIQ_KEY"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK"),
    col("PRVCY_EXTRNL_MBR_SK"),
    col("PRVCY_PRSNL_REP_SK"),
    col("CRT_DT_SK"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("PRVCY_MBR_RELSHP_REP_CD_SK"),
    col("PRVCY_MBR_SRC_CD_SK"),
    rpad(col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("SRC_SYS_LAST_UPDT_USER_SK")
)

write_files(
    df_key,
    f"{adls_path}/key/FctsPrvcyMbrRelshpExtr.PrvcyMbrRelshp.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_updt = df_enriched.filter(col("lkup.PRVCY_MBR_RELSHP_SK").isNull()).select(
    col("SRC_SYS_CD"),
    col("PRVCY_MBR_UNIQ_KEY"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_MBR_RELSHP_SK")
)

(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("dbtable", "STAGING.FctsPrvcyMbrRelshpExtr_hf_prvcy_mbr_relshp_updt_temp")
    .mode("overwrite")
    .save()
)

execute_dml(
    """
MERGE INTO dummy_hf_prvcy_mbr_relshp AS T
USING STAGING.FctsPrvcyMbrRelshpExtr_hf_prvcy_mbr_relshp_updt_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.PRVCY_MBR_UNIQ_KEY = S.PRVCY_MBR_UNIQ_KEY
   AND T.SEQ_NO = S.SEQ_NO
WHEN MATCHED THEN
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_MBR_RELSHP_SK = S.PRVCY_MBR_RELSHP_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PRVCY_MBR_UNIQ_KEY, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, PRVCY_MBR_RELSHP_SK)
  VALUES (S.SRC_SYS_CD, S.PRVCY_MBR_UNIQ_KEY, S.SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_MBR_RELSHP_SK);
""",
    jdbc_url_facets,
    jdbc_props_facets
)