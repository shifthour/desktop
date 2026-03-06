# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011, 2022  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsCapSchdCdExtr
# MAGIC CALLED BY:  FctsCapExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_CRFS_SCHED_DESC  to a landing file for the IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer 	Date   		Project/Altiris #		Change Description					Development Project		Code Reviewer	Date Reviewed       
# MAGIC =========================================================================================================================================================================================
# MAGIC Ralph Tucker          	2011-03-08               	TTR-1058               		Originally Programmed                                              		IntegrateNewDevl                   	SAndrew                    	2011-04-11
# MAGIC Prabhu ES              	 2022-02-25              	S2S Remediation     		MSSQL connection parameters added                   		IntegrateDev5		Ken Bradmon	2022-06-07

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
from pyspark.sql.functions import col, lit, when, length, upper, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


OutFile = get_widget_value('OutFile','FctsSchdlCdExtr.SchdCd.dat')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
CurrDate = get_widget_value('CurrDate','')

ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_hf_cap_schd_cd_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query","SELECT SRC_SYS_CD, CAP_SCHD_CD, CAP_SCHD_CD_NM, CRT_RUN_CYC_EXCTN_SK, CAP_SCHD_CD_SK FROM IDS.dummy_hf_cap_schd_cd")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_CMC_CRFS_SCHD_DESC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT CRFS_SCHD_ID, CRFS_DESC FROM {FacetsOwner}.CMC_CRFS_SCHD_DESC")
    .load()
)

df_StripFields = df_CMC_CRFS_SCHD_DESC.select(
    strip_field(col("CRFS_SCHD_ID")).alias("CRFS_SCHD_ID"),
    strip_field(col("CRFS_DESC")).alias("CRFS_DESC")
)

df_BusinessRules = df_StripFields.filter(length(trim(col("CRFS_SCHD_ID"))) > 0)
df_BusinessRules = (
    df_BusinessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(lit(SrcSysCd), lit(";"), trim(col("CRFS_SCHD_ID"))))
    .withColumn("CAP_SCHD_CD", trim(col("CRFS_SCHD_ID")))
    .withColumn("CAP_SCHD_NM", when(length(trim(col("CRFS_DESC"))) == 0, lit("UNKNOWN"))
                .otherwise(trim(upper(col("CRFS_DESC")))))
)

df_primarykey_pre = df_BusinessRules.alias("Transform").join(
    df_hf_cap_schd_cd_lkup.alias("lkup"),
    (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) & (col("Transform.CAP_SCHD_CD") == col("lkup.CAP_SCHD_CD")),
    "left"
)

df_primarykey_pre = df_primarykey_pre.withColumn(
    "SK",
    when(trim(col("Transform.CAP_SCHD_CD")) == "UNK", lit(0))
    .when(trim(col("Transform.CAP_SCHD_CD")) == "NA", lit(1))
    .when((col("lkup.CAP_SCHD_CD_SK").isNull()) | (length(trim(col("lkup.CAP_SCHD_CD_SK"))) == 0), lit(None))
    .otherwise(col("lkup.CAP_SCHD_CD_SK"))
)

df_primarykey_pre = df_primarykey_pre.withColumn(
    "NewCrtRunCycExtcnSk",
    when(col("lkup.CAP_SCHD_CD_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = df_primarykey_pre
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

df_primarykey = df_enriched

df_key = df_primarykey.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SK").alias("CAP_SHCD_CD_SK"),
    col("Transform.CAP_SCHD_CD").alias("CAP_SCHD_CD"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.CAP_SCHD_NM").alias("CAP_SCHD_NM")
)

df_key = df_key.withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("CAP_SCHD_CD", rpad(col("CAP_SCHD_CD"), 4, " ")) \
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 255, " ")) \
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), 255, " ")) \
    .withColumn("CAP_SCHD_NM", rpad(col("CAP_SCHD_NM"), 255, " "))

df_updt = df_primarykey.filter(col("lkup.CAP_SCHD_CD").isNull())
df_updt_sel = df_updt.select(
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.CAP_SCHD_CD").alias("CAP_SCHD_CD"),
    col("Transform.CAP_SCHD_NM").alias("CAP_SCHD_CD_NM"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("CAP_SCHD_CD_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsCapSchdCdExtr_hf_cap_schd_cd_temp", jdbc_url_ids, jdbc_props_ids)

df_updt_sel.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsCapSchdCdExtr_hf_cap_schd_cd_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO IDS.dummy_hf_cap_schd_cd AS T
USING STAGING.FctsCapSchdCdExtr_hf_cap_schd_cd_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.CAP_SCHD_CD = S.CAP_SCHD_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CAP_SCHD_CD_NM = S.CAP_SCHD_CD_NM,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CAP_SCHD_CD_SK = S.CAP_SCHD_CD_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, CAP_SCHD_CD, CAP_SCHD_CD_NM, CRT_RUN_CYC_EXCTN_SK, CAP_SCHD_CD_SK)
  VALUES (S.SRC_SYS_CD, S.CAP_SCHD_CD, S.CAP_SCHD_CD_NM, S.CRT_RUN_CYC_EXCTN_SK, S.CAP_SCHD_CD_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

write_files(
    df_key,
    f"{adls_path}/key/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)