# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  BCBSAhyEligTypExtr
# MAGIC CALLED BY:  ECommerceExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from AHY_ELIG_TYP and creates primary keying process   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-01-12                Initial programming                                                                               3863                devlIDScur                         Steph Goddard          02/12/2009
# MAGIC 
# MAGIC Bhoomi Dasari       2009-04-16               Added Ebbcbs parameters                                                                    3863                devlIdscur                          Steph Goddard          04/17/2009
# MAGIC 
# MAGIC Tim Sieg                2022-04-14               Adding ODBC Connection                                                                S2S Remediation     IntegrateDev5		Ken Bradmon	2022-06-01

# MAGIC Primary Key process
# MAGIC Writing Sequential File to /key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Ecommerce AHY_ELIG_TYP Data
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunDate = get_widget_value('RunDate','')
EBbcbsOwner = get_widget_value('EBbcbsOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_bcbs = f"SELECT ELIG_TYP_CD, ELIG_TYP_DESC, USER_ID, AUDIT_TS FROM {EBbcbsOwner}.AHY_ELIG_TYP"
df_AHY_ELIG_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_bcbs)
    .load()
)

df_strip_field = df_AHY_ELIG_TYP.select(
    F.when(
        F.col("ELIG_TYP_CD").isNull() | (F.length(F.col("ELIG_TYP_CD")) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("ELIG_TYP_CD"))).alias("AHY_ELIG_TYP_ID"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ELIG_TYP_CD")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("AHY_ELIG_TYP_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    strip_field(F.col("ELIG_TYP_DESC")).alias("AHY_ELIG_TYP_DESC"),
    F.date_format(F.col("AUDIT_TS"), "yyyy-MM-dd").alias("AUDIT_TS"),
    strip_field(F.col("USER_ID")).alias("USER_ID")
)

# Read/Write hashed file "hf_ahy_elig_typ" under Scenario B -> replaced by dummy table "dummy_hf_ahy_elig_typ"
dummy_jdbc_url, dummy_jdbc_props = get_db_config(<...>)  # Unresolved secret name as required by scenario
df_hf_ahy_elig_typ = (
    spark.read.format("jdbc")
    .option("url", dummy_jdbc_url)
    .options(**dummy_jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, AHY_ELIG_TYP_ID, CRT_RUN_CYC_EXCTN_SK, AHY_ELIG_TYP_SK FROM dummy_hf_ahy_elig_typ")
    .load()
)

df_joined = df_strip_field.alias("Transform").join(
    df_hf_ahy_elig_typ.alias("lkup"),
    [
        F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
        F.col("Transform.AHY_ELIG_TYP_ID") == F.col("lkup.AHY_ELIG_TYP_ID")
    ],
    "left"
)

df_enriched = df_joined.withColumn(
    "AHY_ELIG_TYP_SK",
    F.when(F.isnull(F.col("lkup.AHY_ELIG_TYP_SK")), F.lit(None)).otherwise(F.col("lkup.AHY_ELIG_TYP_SK"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK_new",
    F.when(F.isnull(F.col("lkup.AHY_ELIG_TYP_SK")), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"AHY_ELIG_TYP_SK",<schema>,<secret_name>) 

df_enriched = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("AHY_ELIG_TYP_SK").alias("AHY_ELIG_TYP_SK"),
    F.col("Transform.AHY_ELIG_TYP_ID").alias("AHY_ELIG_TYP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.AHY_ELIG_TYP_DESC").alias("AHY_ELIG_TYP_DESC"),
    F.col("Transform.AUDIT_TS").alias("AUDIT_TS"),
    F.col("Transform.USER_ID").alias("USER_ID"),
    F.col("lkup.AHY_ELIG_TYP_SK").alias("lkup_AHY_ELIG_TYP_SK")  # for filtering new inserts
)

# Split out rows to insert (Constraint: IsNull(lkup.AHY_ELIG_TYP_SK) = @TRUE)
df_to_insert = df_enriched.filter(F.isnull(F.col("lkup_AHY_ELIG_TYP_SK"))).select(
    F.col("SRC_SYS_CD"),
    F.col("AHY_ELIG_TYP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("AHY_ELIG_TYP_SK")
)

# Write df_enriched -> "IdsAhyEligTypExtr"
df_final = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("AHY_ELIG_TYP_SK"),
    F.col("AHY_ELIG_TYP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AHY_ELIG_TYP_DESC"),
    F.rpad(F.col("AUDIT_TS"),10," ").alias("AUDIT_TS"),
    F.rpad(F.col("USER_ID"),15," ").alias("USER_ID")
)

output_path = f"{adls_path}/key/IdsAhyEligTypExtr.AhyEligTyp.dat.{RunID}"
write_files(
    df_final,
    output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Merge logic to handle rows inserted into dummy_hf_ahy_elig_typ
# 1) Create STAGING table
merge_temp_table = "STAGING.BCBSAhyEligTypExtr_hf_ahy_elig_typ_updt_temp"
drop_sql = f"DROP TABLE IF EXISTS {merge_temp_table}"
execute_dml(drop_sql, dummy_jdbc_url, dummy_jdbc_props)

df_to_insert.write.format("jdbc").option("url", dummy_jdbc_url).options(**dummy_jdbc_props).option("dbtable", merge_temp_table).mode("overwrite").save()

merge_sql = f"""
MERGE dummy_hf_ahy_elig_typ as T
USING {merge_temp_table} as S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.AHY_ELIG_TYP_ID = S.AHY_ELIG_TYP_ID)
WHEN NOT MATCHED THEN INSERT
(
  SRC_SYS_CD,
  AHY_ELIG_TYP_ID,
  CRT_RUN_CYC_EXCTN_SK,
  AHY_ELIG_TYP_SK
)
VALUES
(
  S.SRC_SYS_CD,
  S.AHY_ELIG_TYP_ID,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.AHY_ELIG_TYP_SK
);
"""
execute_dml(merge_sql, dummy_jdbc_url, dummy_jdbc_props)