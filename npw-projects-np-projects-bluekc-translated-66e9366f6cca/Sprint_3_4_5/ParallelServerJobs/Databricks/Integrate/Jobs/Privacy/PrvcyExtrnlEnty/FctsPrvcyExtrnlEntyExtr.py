# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 06/19/09 14:44:48 Batch  15146_53151 PROMOTE bckcetl:31540 ids20 dsadm rc for steph 
# MAGIC ^1_1 06/19/09 14:29:48 Batch  15146_52211 INIT bckcett:31540 testIDS dsadm rc for steph 
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
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
# MAGIC JOB NAME:  FctsPrvcyExtrnlEntyExtr
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty               2/25/2007           FHP/3028                      Originally Programmed                              devlIDS30                  Steph Goddard            3/29/2007
# MAGIC      
# MAGIC Bhoomi Dasari                 2/17/2009           Prod Supp/15                 Added new filter criteria                            devlIDS     
# MAGIC    
# MAGIC Bhoomi Dasari                2/25/2009         Prod Supp/15                 Added 'RunDate' paramter instead             devlIDS                      Steph Goddard              02/28/2009
# MAGIC                                                                                                         of Format.Date routine
# MAGIC 
# MAGIC Bhoomi Dasari               3/20/2009          Prod Supp/15                 Updated conditions in the extract SQL       devlIDS                       Steph Goddard             04/01/2009
# MAGIC 
# MAGIC Anoop Nair                2022-03-08         S2S Remediation      Added BCBS and FACETS DSN Connection parameters      IntegrateDev5
# MAGIC Vikas Abbu                2022-05-25         S2S Remediation      Removed timestamp in temp table name                    IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Extrnl Enty Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Using the same hash file (hf_extrnl_enty) in 
# MAGIC FctsPrvcyExtrnlEntyExtr
# MAGIC FctsPrvcyExtrnlPrsnExtr
# MAGIC FctsPrvcyExtrnlOrgExtr
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
from pyspark.sql.functions import col, lit, when, regexp_replace, date_format, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# No other system libraries are imported.

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
TableTimestamp = get_widget_value('TableTimestamp','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# --------------------------------------------------------------------------------
# Stage: hf_extrnl_enty (CHashedFileStage) - Scenario B: replaced by dummy table read
# --------------------------------------------------------------------------------
df_hf_extrnl_enty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_EXTRNL_ENTY_UNIQ_KEY, CRT_RUN_CYC_EXCTN_SK, PRVCY_EXTRNL_ENTY_SK FROM dummy_hf_extrnl_enty")
    .load()
)

# --------------------------------------------------------------------------------
# Stage: FacetsPrvcyExtrnlEnty (ODBCConnector)
# --------------------------------------------------------------------------------
df_FacetsPrvcyExtrnlEnty = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT ENEN.ENEN_CKE, ENEN.ENCT_CTYP, ENEN.ENEN_LAST_USUS_ID, ENEN.ENEN_LAST_UPD_DTM "
        f"FROM {FacetsOwner}.FHD_ENEN_ENTITY_D ENEN, tempdb..TMP_PRVCY_CDC_EE_DRVR TMP "
        f"WHERE TMP.ENEN_CKE = ENEN.ENEN_CKE"
    )
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripFields (CTransformerStage)
# --------------------------------------------------------------------------------
df_StripFields = (
    df_FacetsPrvcyExtrnlEnty
    .withColumn("ENEN_CKE", col("ENEN_CKE"))
    .withColumn("ENCT_CTYP", regexp_replace(col("ENCT_CTYP"), "[\r\n\t]", ""))
    .withColumn("ENEN_LAST_USUS_ID", regexp_replace(col("ENEN_LAST_USUS_ID"), "[\r\n\t]", ""))
    .withColumn("ENEN_LAST_UPD_DTM", date_format(col("ENEN_LAST_UPD_DTM"), "yyyy-MM-dd"))
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_StripFields
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("svEnenCke", trim(col("ENEN_CKE")))
    .withColumn("PRI_KEY_STRING", concat_ws(";", lit("FACETS"), col("svEnenCke")))
    .withColumn("PRVCY_EXTRNL_ENTY_SK", lit(0))
    .withColumn("PRVCY_EXTRNL_ENTY_UNIQ_KEY", col("svEnenCke"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("PRVCY_EXTRNL_ENTY_CLS_TYP_CD", col("ENCT_CTYP"))
    .withColumn("SRC_SYS_LAST_UPDT_DT", col("ENEN_LAST_UPD_DTM"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", col("ENEN_LAST_USUS_ID"))
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage) - Left join with hf_extrnl_enty dummy table
# --------------------------------------------------------------------------------
df_joined = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_extrnl_enty.alias("lkup"),
        (
            (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"))
            & (col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY") == col("lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"))
        ),
        how="left"
    )
)

df_joined = (
    df_joined
    .withColumn(
        "NewCurrRunCycExtcnSk",
        when(col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "TMP_PRVCY_EXTRNL_ENTY_SK",
        when(col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull(), lit(None).cast(IntegerType()))
        .otherwise(col("lkup.PRVCY_EXTRNL_ENTY_SK").cast(IntegerType()))
    )
)

df_enriched = (
    df_joined
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", col("Transform.JOB_EXCTN_RCRD_ERR_SK"))
    .withColumn("INSRT_UPDT_CD", col("Transform.INSRT_UPDT_CD"))
    .withColumn("DISCARD_IN", col("Transform.DISCARD_IN"))
    .withColumn("PASS_THRU_IN", col("Transform.PASS_THRU_IN"))
    .withColumn("FIRST_RECYC_DT", col("Transform.FIRST_RECYC_DT"))
    .withColumn("ERR_CT", col("Transform.ERR_CT"))
    .withColumn("RECYCLE_CT", col("Transform.RECYCLE_CT"))
    .withColumn("SRC_SYS_CD", col("Transform.SRC_SYS_CD"))
    .withColumn("PRI_KEY_STRING", col("Transform.PRI_KEY_STRING"))
    .withColumn("PRVCY_EXTRNL_ENTY_SK", col("TMP_PRVCY_EXTRNL_ENTY_SK"))
    .withColumn("PRVCY_EXTRNL_ENTY_UNIQ_KEY", col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("NewCurrRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    .withColumn("PRVCY_EXTRNL_ENTY_CLS_TYP_CD", col("Transform.PRVCY_EXTRNL_ENTY_CLS_TYP_CD"))
    .withColumn("SRC_SYS_LAST_UPDT_DT", col("Transform.SRC_SYS_LAST_UPDT_DT"))
    .withColumn("SRC_SYS_LAST_UPDT_USER", col("Transform.SRC_SYS_LAST_UPDT_USER"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'PRVCY_EXTRNL_ENTY_SK',<schema>,<secret_name>)

# Rows for updating dummy_hf_extrnl_enty were originally constrained with isNull(lkup.PRVCY_EXTRNL_ENTY_SK)
df_hf_extrnl_enty_updt = (
    df_joined.filter(col("lkup.PRVCY_EXTRNL_ENTY_SK").isNull())
    .select(
        col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Transform.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        col("TMP_PRVCY_EXTRNL_ENTY_SK").alias("PRVCY_EXTRNL_ENTY_SK")
    )
)

# ------------------------------------------------------------------------------
# Stage: IdsPrvcyExtrnlEntyExtr (CSeqFileStage) - Writing the final file
# ------------------------------------------------------------------------------
df_final_write = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PRVCY_EXTRNL_ENTY_SK",
    "PRVCY_EXTRNL_ENTY_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_EXTRNL_ENTY_CLS_TYP_CD",
    "SRC_SYS_LAST_UPDT_DT",
    "SRC_SYS_LAST_UPDT_USER"
)

df_final_write = (
    df_final_write
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PRVCY_EXTRNL_ENTY_CLS_TYP_CD", rpad(col("PRVCY_EXTRNL_ENTY_CLS_TYP_CD"), 4, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT", rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " "))
)

write_files(
    df_final_write,
    f"{adls_path}/key/FctsPrvcyExtrnlEntyExtr.PrvcyExtrnlEnty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------------------------------------------------------------------------------
# Stage: hf_extrnl_enty_updt (CHashedFileStage) - Scenario B: replaced by dummy table write
# ---------------------------------------------------------------------------------------------------------------------
df_hf_extrnl_enty_updt.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "dummy_hf_extrnl_enty") \
    .mode("append") \
    .save()