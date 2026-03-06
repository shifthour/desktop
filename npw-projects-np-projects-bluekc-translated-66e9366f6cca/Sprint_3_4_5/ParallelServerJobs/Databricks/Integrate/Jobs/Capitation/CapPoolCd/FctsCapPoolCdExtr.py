# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsCapPoolCdExtr
# MAGIC CALLED BY:  FctsCapExtrSeq
# MAGIC DESCRIPTION:    Pulls data from CMC_CRFS_SCHED_DESC  to a landing file for the IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer 	Date   		Project/Altiris #		Change Description					Development Project		Code Reviewer	Date Reviewed       
# MAGIC =========================================================================================================================================================================================
# MAGIC Ralph Tucker    	2011-03-08        	TTR-1058               		Originally Programmed                                             		IntegrateNewDevl                    	SAndrew                 	2011-04-11
# MAGIC Prabhu ES         	2022-02-25        	S2S Remediation     		MSSQL connection parameters added                    		IntegrateDev5		Ken Bradmon	2022-06-07

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract From Facets
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# COMMAND ----------
# Retrieve parameter values
CurrRunCycle = get_widget_value("CurrRunCycle","")
FacetsOwner = get_widget_value("$FacetsOwner","$PROJDEF")
facets_secret_name = get_widget_value("facets_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
CurrDate = get_widget_value("CurrDate","")

# COMMAND ----------
# Stage: hf_cap_pool_cd_lkup (CHashedFileStage) - Scenario B read from dummy table
# Dummy table name: dummy_hf_cap_pool_cd
jdbc_url_dummy_hf_cap_pool_cd, jdbc_props_dummy_hf_cap_pool_cd = get_db_config(<...>)
extract_query_hf_cap_pool_cd = """
SELECT
  SRC_SYS_CD,
  CAP_POOL_CD,
  CAP_POOL_CD_NM,
  CRT_RUN_CYC_EXCTN_SK,
  CAP_POOL_CD_SK
FROM dummy_hf_cap_pool_cd
"""
df_hf_cap_pool_cd_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy_hf_cap_pool_cd)
    .options(**jdbc_props_dummy_hf_cap_pool_cd)
    .option("query", extract_query_hf_cap_pool_cd)
    .load()
)

# COMMAND ----------
# Stage: CMC_CRPL_POOL_DESC (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cmc_crpl_pool_desc = f"SELECT CRPL_POOL_ID, CRPL_DESC FROM {FacetsOwner}.CMC_CRPL_POOL_DESC"
df_CMC_CRPL_POOL_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_crpl_pool_desc)
    .load()
)

# COMMAND ----------
# Stage: StripFields (CTransformerStage)
df_StripFields = (
    df_CMC_CRPL_POOL_DESC
    .withColumn("CRPL_POOL_ID", strip_field(F.col("CRPL_POOL_ID")))
    .withColumn("CRPL_DESC", strip_field(F.col("CRPL_DESC")))
)

# COMMAND ----------
# Stage: BusinessRules (CTransformerStage)
df_BusinessRules = (
    df_StripFields
    .filter(F.length(F.trim(F.col("CRPL_POOL_ID"))) > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat_ws(";", F.lit(SrcSysCd), F.trim(F.col("CRPL_POOL_ID"))))
    .withColumn("CAP_POOL_CD", F.trim(F.col("CRPL_POOL_ID")))
    .withColumn(
        "CAP_POOL_NM", 
        F.when(F.length(F.trim(F.col("CRPL_DESC"))) == 0, F.lit("UNKNOWN"))
         .otherwise(F.upper(F.trim(F.col("CRPL_DESC"))))
    )
)

# COMMAND ----------
# Stage: PrimaryKey (CTransformerStage) - joins with hf_cap_pool_cd_lkup (left join)
df_join = (
    df_BusinessRules.alias("br")
    .join(
        df_hf_cap_pool_cd_lkup.alias("lkup"),
        (
            (F.col("br.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) 
            & (F.col("br.CAP_POOL_CD") == F.col("lkup.CAP_POOL_CD"))
        ),
        "left"
    )
)

df_enriched = df_join.select(
    F.col("br.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("br.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("br.DISCARD_IN").alias("DISCARD_IN"),
    F.col("br.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("br.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("br.ERR_CT").alias("ERR_CT"),
    F.col("br.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("br.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("br.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.when(
        F.trim(F.col("br.CAP_POOL_CD")) == F.lit("UNK"), F.lit(0)
    ).when(
        F.trim(F.col("br.CAP_POOL_CD")) == F.lit("NA"), F.lit(1)
    ).when(
        (F.col("lkup.CAP_POOL_CD_SK").isNotNull()) & (F.length(F.trim(F.col("lkup.CAP_POOL_CD_SK"))) > 0),
        F.col("lkup.CAP_POOL_CD_SK")
    ).otherwise(None).alias("SK"),
    F.col("br.CAP_POOL_CD").alias("CAP_POOL_CD"),
    F.when(
        F.col("lkup.CAP_POOL_CD_SK").isNull(),
        F.lit(CurrRunCycle)
    ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("NewCrtRunCycExtcnSk"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("br.CAP_POOL_NM").alias("CAP_POOL_NM"),
    F.col("lkup.CAP_POOL_CD_SK").alias("lkup_CAP_POOL_CD_SK"),
    F.col("lkup.CAP_POOL_CD").alias("lkup_CAP_POOL_CD")
)

# Since KeyMgtGetNextValueConcurrent was referenced, enforce SurrogateKeyGen call afterward
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# COMMAND ----------
# Output link "updt" => filter where IsNull(lkup_CAP_POOL_CD) == True
df_updt = (
    df_enriched
    .filter(F.col("lkup_CAP_POOL_CD").isNull())
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CAP_POOL_CD").alias("CAP_POOL_CD"),
        F.col("CAP_POOL_NM").alias("CAP_POOL_CD_NM"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("SK").alias("CAP_POOL_CD_SK")
    )
)

# Stage: hf_cap_pool_cd (CHashedFileStage) - Scenario B write to dummy table "dummy_hf_cap_pool_cd"
jdbc_url_updt, jdbc_props_updt = get_db_config(<...>)
# Create a physical temp table
execute_dml("DROP TABLE IF EXISTS STAGING.FctsCapPoolCdExtr_hf_cap_pool_cd_temp", jdbc_url_updt, jdbc_props_updt)

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_updt)
    .options(**jdbc_props_updt)
    .option("dbtable", "STAGING.FctsCapPoolCdExtr_hf_cap_pool_cd_temp")
    .mode("overwrite")
    .save()
)

merge_sql_hf_cap_pool_cd = """
MERGE INTO dummy_hf_cap_pool_cd AS T
USING STAGING.FctsCapPoolCdExtr_hf_cap_pool_cd_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.CAP_POOL_CD = S.CAP_POOL_CD
WHEN MATCHED THEN
    -- do nothing
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, CAP_POOL_CD, CAP_POOL_CD_NM, CRT_RUN_CYC_EXCTN_SK, CAP_POOL_CD_SK)
    VALUES (S.SRC_SYS_CD, S.CAP_POOL_CD, S.CAP_POOL_CD_NM, S.CRT_RUN_CYC_EXCTN_SK, S.CAP_POOL_CD_SK);
"""
execute_dml(merge_sql_hf_cap_pool_cd, jdbc_url_updt, jdbc_props_updt)

# COMMAND ----------
# Output link "Key" => goes to IdsCapPoolCdPkey (CSeqFileStage)
df_Key = (
    df_enriched
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("SK").alias("CAP_POOL_CD_SK"),
        F.col("CAP_POOL_CD"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CAP_POOL_NM")
    )
)

# Apply rpad for char/varchar columns in the final output
df_Key = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    F.col("CAP_POOL_CD_SK"),
    F.rpad(F.col("CAP_POOL_CD"), 4, " ").alias("CAP_POOL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CAP_POOL_NM"), <...>, " ").alias("CAP_POOL_NM")
)

# COMMAND ----------
# Stage: IdsCapPoolCdPkey (CSeqFileStage) - write to .dat file
write_files(
    df_Key,
    f"{adls_path}/key/FctsPoolCdExtr.PoolCd.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)