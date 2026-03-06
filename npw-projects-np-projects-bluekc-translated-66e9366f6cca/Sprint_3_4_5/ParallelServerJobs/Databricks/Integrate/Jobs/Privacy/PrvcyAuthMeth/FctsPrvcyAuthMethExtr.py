# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:56:51 Batch  14390_43020 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 05/25/07 11:52:00 Batch  14390_42723 INIT bckcett testIDSnew dsadm bls for on
# MAGIC ^1_1 05/17/07 12:03:56 Batch  14382_43445 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:42:32 Batch  14382_34957 PROMOTE bckcett testIDSnew u10913 Ollie move from devl to test
# MAGIC ^1_1 05/17/07 09:38:32 Batch  14382_34718 INIT bckcett devlIDS30 u10913 Ollie Move from devl to test
# MAGIC ^1_1 04/27/07 12:54:12 Batch  14362_46467 INIT bckcett devlIDS30 u10913 O. Nielsen move from devl to 4.3 Environment
# MAGIC 
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsPrvcyAuthMethExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_UMUM_UTIL_MGT and CMC_GRGR_GROUP for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets - CMC_CSSC_CUSTOMER
# MAGIC                    Facets - CMC_CSTK_TASK
# MAGIC                    Facets - CMC_CSCI_CONTACT
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              NullOptCode
# MAGIC                           
# MAGIC 
# MAGIC PROCESSING:  Extract, transform, and primary keying for Custmoer Service subject area.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    06/2006  -  Originally Program
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Steph Goddard                 7/14/10         TTR-689                          updated standards; changed IDS_SK    RebuildIntNewDevl      SAndrew                           2010-09-27
# MAGIC                                                                                                         lookup to PRVCY_CONF_COMM_SK
# MAGIC 
# MAGIC Anoop Nair                2022-03-07         S2S Remediation                  Added FACETS DSN Connection parameters      IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Privacy Authorization Method Data
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, trim, rpad, isnull
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

facets_secret_name = get_widget_value('facets_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')

# --------------------------------------------------------------------------------
# Stage: FacetsPrvcyAuthMeth (ODBCConnector)
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query = f"SELECT PMAT_ID, PMAT_PZCD_TYPE, PMAT_DESC FROM {FacetsOwner}.FHP_PMAT_AUTH_D"
df_FacetsPrvcyAuthMeth = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripFields (CTransformerStage)
# --------------------------------------------------------------------------------
df_StripFields = (
    df_FacetsPrvcyAuthMeth
    .select(
        strip_field(col("PMAT_ID")).alias("PMAT_ID"),
        strip_field(col("PMAT_PZCD_TYPE")).alias("PMAT_PZCD_TYPE"),
        strip_field(col("PMAT_DESC")).alias("PMAT_DESC")
    )
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
    .withColumn("PRI_KEY_STRING", concat(lit("FACETS;"), trim(col("PMAT_ID"))))
    .withColumn("PRVCY_AUTH_METH_SK", lit(0))
    .withColumn("PRVCY_AUTH_ID", trim(col("PMAT_ID")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("PRVCY_AUTH_METH_TYP_CD_SK", trim(col("PMAT_PZCD_TYPE")))
    .withColumn("AUTH_METH_DESC", col("PMAT_DESC"))
)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_auth_meth (CHashedFileStage) - Scenario B read
#         Converted to a read from dummy_hf_prvcy_auth_meth
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_dummy_hf_prvcy_auth_meth = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, PRVCY_AUTH_ID, CRT_RUN_CYC_EXCTN_SK, PRVCY_AUTH_METH_SK FROM dummy_hf_prvcy_auth_meth")
    .load()
)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
#         Left join with dummy_hf_prvcy_auth_meth (originally hashed file lookup)
# --------------------------------------------------------------------------------
df_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_dummy_hf_prvcy_auth_meth.alias("lkup"),
        (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) & (col("Transform.PRVCY_AUTH_ID") == col("lkup.PRVCY_AUTH_ID")),
        "left"
    )
)

df_join = (
    df_join
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK_temp",
        when(col("lkup.PRVCY_AUTH_METH_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK_temp", lit(CurrRunCycle))
    .withColumn(
        "PRVCY_AUTH_METH_SK_temp",
        when(col("lkup.PRVCY_AUTH_METH_SK").isNull(), lit(None).cast("int")).otherwise(col("lkup.PRVCY_AUTH_METH_SK"))
    )
)

df_enriched = df_join.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("PRVCY_AUTH_METH_SK_temp").alias("PRVCY_AUTH_METH_SK"),
    col("Transform.PRVCY_AUTH_ID").alias("PRVCY_AUTH_ID"),
    col("CRT_RUN_CYC_EXCTN_SK_temp").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_temp").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.PRVCY_AUTH_METH_TYP_CD_SK").alias("PRVCY_AUTH_METH_TYP_CD_SK"),
    col("Transform.AUTH_METH_DESC").alias("AUTH_METH_DESC"),
    col("lkup.PRVCY_AUTH_METH_SK").alias("lkup_PRVCY_AUTH_METH_SK")  # Keep for filter on updt link
)

# Apply SurrogateKeyGen to replace any null PRVCY_AUTH_METH_SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PRVCY_AUTH_METH_SK",<schema>,<secret_name>)

# --------------------------------------------------------------------------------
# Stage: IdsPrvcyAuthMethExtr (CSeqFileStage) - Write the "Key" link output
# --------------------------------------------------------------------------------
df_idsPrvcyAuthMethExtr = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PRVCY_AUTH_METH_SK"),
    col("PRVCY_AUTH_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_AUTH_METH_TYP_CD_SK"),
    col("AUTH_METH_DESC")
)
write_files(
    df_idsPrvcyAuthMethExtr,
    f"{adls_path}/key/FctsPrvcyAuthMethExtr.PrvcyAuthMeth.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: hf_prvcy_auth_meth_updt (CHashedFileStage - same hashed file) - Scenario B write
# --------------------------------------------------------------------------------
df_updt = df_enriched.filter(col("lkup_PRVCY_AUTH_METH_SK").isNull()).select(
    col("SRC_SYS_CD"),
    col("PRVCY_AUTH_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PRVCY_AUTH_METH_SK")
)

temp_table = "STAGING.FctsPrvcyAuthMethExtr_hf_prvcy_auth_meth_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_ids, jdbc_props_ids)

(
    df_updt.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO dummy_hf_prvcy_auth_meth AS T
USING {temp_table} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.PRVCY_AUTH_ID = S.PRVCY_AUTH_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PRVCY_AUTH_METH_SK = S.PRVCY_AUTH_METH_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, PRVCY_AUTH_ID, CRT_RUN_CYC_EXCTN_SK, PRVCY_AUTH_METH_SK)
  VALUES (S.SRC_SYS_CD, S.PRVCY_AUTH_ID, S.CRT_RUN_CYC_EXCTN_SK, S.PRVCY_AUTH_METH_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)