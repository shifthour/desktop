# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: UWSIhmPgmHierExtr
# MAGIC 
# MAGIC CALLED BY:  AlineoIndvBeLtrExtrCntl
# MAGIC 
# MAGIC PROCESSING:  Extracts data from from file received from Alineo and creates a file that is used by the IdsIIhmPgmHierFkey job that creates the file the is used to load the IDS IHM_PGM_HIER table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Judy Reynolds                 06-01-2010      4297  - Alineo                Original Programming                                                                            IntegrateWrhsDevl        Steph Goddard            07/15/2010
# MAGIC 
# MAGIC Judy Reynolds                 08-03-2010      4297 - Alineo                 Modified to support changes to the primary key                                   IntegrateNewDevl          Steph Goddard            08/18/2010
# MAGIC                                                                                                      and field name changes.                                                                   
# MAGIC Kalyan Neelam              2014-11-14          TFS 9558                     Added balancing snapshot file                                                              IntegrateNewDevl        Bhoomi Dasari              12/09/2014

# MAGIC Extract data from UWS table IHM_PGM_HIER
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC UWSIhmPgmHierExtr - extracts data from UWS table IHM_PGM_HIER and create IHM_PGM_HIER table in IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat, rpad
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
RunID = get_widget_value('RunID','')
SourceSK = get_widget_value('SourceSK','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)

df_hf_ihm_pgm_hier_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT PGM_ID, SBPRG_ID, RISK_SVRTY_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, IHM_PGM_HIER_SK FROM dummy_hf_ihm_pgm_hier")
    .load()
)

extract_query_UWS = f"SELECT PGM_ID, SBPRG_ID, RISK_SVRTY_CD, IHM_PGM_HIER_NO, IHM_PGM_HIER_RANK_NO, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.IHM_PGM_HIER"
df_UWS_IHM_PGM_HIER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_UWS)
    .load()
)

df_Transform = df_UWS_IHM_PGM_HIER.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("ALINEO").alias("SRC_SYS_CD"),
    concat(col("PGM_ID"), lit(";"), col("SBPRG_ID"), lit(";"), lit("ALINEO")).alias("PRI_KEY_STRING"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("NA").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    lit("NA").alias("SRC_SYS_LAST_UPDT_USER"),
    col("PGM_ID"),
    col("SBPRG_ID"),
    col("RISK_SVRTY_CD"),
    col("IHM_PGM_HIER_NO"),
    col("IHM_PGM_HIER_RANK_NO"),
    col("USER_ID"),
    col("LAST_UPDT_DT_SK")
)

df_snapshot = df_UWS_IHM_PGM_HIER.select(
    col("PGM_ID"),
    col("SBPRG_ID"),
    col("RISK_SVRTY_CD"),
    lit(SourceSK).alias("SRC_SYS_CD_SK")
)

df_snapshot_select = df_snapshot.select("PGM_ID", "SBPRG_ID", "RISK_SVRTY_CD", "SRC_SYS_CD_SK")
df_snapshot_select = df_snapshot_select.withColumn("PGM_ID", rpad("PGM_ID", <...>, " "))
df_snapshot_select = df_snapshot_select.withColumn("SBPRG_ID", rpad("SBPRG_ID", <...>, " "))
df_snapshot_select = df_snapshot_select.withColumn("RISK_SVRTY_CD", rpad("RISK_SVRTY_CD", <...>, " "))
df_snapshot_select = df_snapshot_select.withColumn("SRC_SYS_CD_SK", rpad("SRC_SYS_CD_SK", <...>, " "))

write_files(
    df_snapshot_select,
    f"{adls_path}/load/B_IHM_PGM_HIER.ALINEO.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_primaryKey_join = df_Transform.alias("t").join(
    df_hf_ihm_pgm_hier_lkup.alias("l"),
    (
        (col("t.PGM_ID") == col("l.PGM_ID"))
        & (col("t.SBPRG_ID") == col("l.SBPRG_ID"))
        & (col("t.RISK_SVRTY_CD") == col("l.RISK_SVRTY_CD"))
        & (col("t.SRC_SYS_CD") == col("l.SRC_SYS_CD"))
    ),
    "left"
)

df_enriched = df_primaryKey_join.select(
    col("t.JOB_EXCTN_RCRD_ERR_SK"),
    col("t.INSRT_UPDT_CD"),
    col("t.DISCARD_IN"),
    col("t.PASS_THRU_IN"),
    col("t.FIRST_RECYC_DT"),
    col("t.ERR_CT"),
    col("t.RECYCLE_CT"),
    col("t.SRC_SYS_CD"),
    col("t.PRI_KEY_STRING"),
    col("l.IHM_PGM_HIER_SK").alias("l_IHM_PGM_HIER_SK"),
    col("l.CRT_RUN_CYC_EXCTN_SK").alias("l_CRT_RUN_CYC_EXCTN_SK"),
    col("t.PGM_ID"),
    col("t.SBPRG_ID"),
    col("t.RISK_SVRTY_CD"),
    col("t.IHM_PGM_HIER_NO"),
    col("t.IHM_PGM_HIER_RANK_NO"),
    col("t.USER_ID"),
    col("t.LAST_UPDT_DT_SK")
)

df_enriched = df_enriched.withColumn(
    "IHM_PGM_HIER_SK",
    when(col("l_IHM_PGM_HIER_SK").isNull(), lit(None).cast(IntegerType())).otherwise(col("l_IHM_PGM_HIER_SK"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    when(col("l_IHM_PGM_HIER_SK").isNull(), lit(CurrRunCycle).cast(IntegerType())).otherwise(col("l_CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    lit(CurrRunCycle).cast(IntegerType())
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"IHM_PGM_HIER_SK",<schema>,<secret_name>)

df_updt = df_enriched.filter(col("l_IHM_PGM_HIER_SK").isNull()).select(
    col("PGM_ID"),
    col("SBPRG_ID"),
    col("RISK_SVRTY_CD"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("IHM_PGM_HIER_SK")
)

df_updt.write.jdbc(
    url=jdbc_url,
    table="dummy_hf_ihm_pgm_hier",
    mode="append",
    properties=jdbc_props
)

df_key = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "IHM_PGM_HIER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PGM_ID",
    "SBPRG_ID",
    "RISK_SVRTY_CD",
    "IHM_PGM_HIER_NO",
    "IHM_PGM_HIER_RANK_NO",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

df_key = df_key.withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
df_key = df_key.withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
df_key = df_key.withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
df_key = df_key.withColumn("LAST_UPDT_DT_SK", rpad("LAST_UPDT_DT_SK", 10, " "))
df_key = df_key.withColumn("PGM_ID", rpad("PGM_ID", <...>, " "))
df_key = df_key.withColumn("SBPRG_ID", rpad("SBPRG_ID", <...>, " "))
df_key = df_key.withColumn("RISK_SVRTY_CD", rpad("RISK_SVRTY_CD", <...>, " "))
df_key = df_key.withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
df_key = df_key.withColumn("USER_ID", rpad("USER_ID", <...>, " "))

df_key_select = df_key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "IHM_PGM_HIER_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PGM_ID",
    "SBPRG_ID",
    "RISK_SVRTY_CD",
    "IHM_PGM_HIER_NO",
    "IHM_PGM_HIER_RANK_NO",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

write_files(
    df_key_select,
    f"{adls_path}/key/UWSIhmPgmHierExtr.IhmPgmHier.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)