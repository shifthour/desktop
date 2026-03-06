# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  WebUserInfoAHYEvtSchedulingExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Processes the EVENT PAR GROUP data from WEB User Info and writes out a key file to load into the IDS EVT_PAR_GRP table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Manasa Andru        2012-03-25        4830 - AHY 3.0      Initial Programming                                                                         IntegrateCurDevl            SAndrew                    2012-04-24
# MAGIC Kalyan Neelam         2014-03-19          TFS 7876             Updated updt o/p link in Pkey transformer to use                         IntegrateNewDevl       Bhoomi Dasari            3/25/2014  
# MAGIC                                                                                   GRP_ID and SRC_SYS_CD columns from Transform link instead of lkup link.

# MAGIC IDS PAR GROUP Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


WebUserInformationOwner = get_widget_value('WebUserInformationOwner','')
webuserinformation_secret_name = get_widget_value('webuserinformation_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','100')
SrcSysCd = get_widget_value('SrcSysCd','WEBUSERINFO')
CurrDate = get_widget_value('CurrDate','2010-11-04')
CurrDateTimestamp = get_widget_value('CurrDateTimestamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(webuserinformation_secret_name)
df_PAR_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT PAR_GRP.GRP_ID, PAR_GRP.GRP_CNTCT_NM, PAR_GRP.GRP_CNTCT_CELL_PHN_NO, PAR_GRP.GRP_CNTCT_WORK_PHN_NO, PAR_GRP.GRP_COLOR_KEY_DESC, PAR_GRP.GRP_ACTV_IN, PAR_GRP.EFF_DT, PAR_GRP.TERM_DT, PAR_GRP.LAST_UPDT_USER_ID, PAR_GRP.LAST_UPDT_DTM FROM {WebUserInformationOwner}.PAR_GRP"
    )
    .load()
)

df_businessRules_transform = (
    df_PAR_GRP
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(trim(F.col("GRP_ID")), F.lit(";"), F.lit(SrcSysCd))
    )
    .withColumn("EVT_PAR_GRP_SK", F.lit(0))
    .withColumn(
        "GRP_ID",
        F.when(
            F.col("GRP_ID").isNull() | (F.length(trim(F.col("GRP_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("GRP_ID")))
    )
    .withColumn(
        "GRP_ACTV_IN",
        F.when(
            F.col("GRP_ACTV_IN").isNull() | (F.length(trim(F.col("GRP_ACTV_IN"))) == 0),
            F.lit("N")
        )
        .when(trim(F.col("GRP_ACTV_IN")) == F.lit("0"), F.lit("N"))
        .when(trim(F.col("GRP_ACTV_IN")) == F.lit("1"), F.lit("Y"))
        .otherwise(F.lit("N"))
    )
    .withColumn(
        "EFF_DT",
        F.when(
            F.col("EFF_DT").isNull() | (F.length(trim(F.col("EFF_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(trim(F.col("EFF_DT")))
    )
    .withColumn(
        "TERM_DT",
        F.when(
            F.col("TERM_DT").isNull() | (F.length(trim(F.col("TERM_DT"))) == 0),
            F.lit("2199-12-31")
        ).otherwise(trim(F.col("TERM_DT")))
    )
    .withColumn(
        "GRP_COLOR_KEY_DESC",
        F.when(
            F.col("GRP_COLOR_KEY_DESC").isNull() | (F.length(trim(F.col("GRP_COLOR_KEY_DESC"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("GRP_COLOR_KEY_DESC")))
    )
    .withColumn(
        "GRP_CNTCT_NM",
        F.when(
            F.col("GRP_CNTCT_NM").isNull() | (F.length(trim(F.col("GRP_CNTCT_NM"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("GRP_CNTCT_NM")))
    )
    .withColumn(
        "GRP_CNTCT_CELL_PHN_NO",
        F.when(
            F.col("GRP_CNTCT_CELL_PHN_NO").isNull() | (F.length(trim(F.col("GRP_CNTCT_CELL_PHN_NO"))) == 0),
            F.lit("0000000000")
        ).otherwise(trim(F.col("GRP_CNTCT_CELL_PHN_NO")))
    )
    .withColumn(
        "GRP_CNTCT_WORK_PHN_NO",
        F.when(
            F.col("GRP_CNTCT_WORK_PHN_NO").isNull() | (F.length(trim(F.col("GRP_CNTCT_WORK_PHN_NO"))) == 0),
            F.lit("0000000000")
        ).otherwise(trim(F.col("GRP_CNTCT_WORK_PHN_NO")))
    )
    .withColumn(
        "LAST_UPDT_DTM",
        F.when(
            F.col("LAST_UPDT_DTM").isNull() | (F.length(trim(F.col("LAST_UPDT_DTM"))) == 0),
            F.lit(CurrDateTimestamp)
        ).otherwise(trim(F.col("LAST_UPDT_DTM")))
    )
    .withColumn(
        "LAST_UPDT_USER_ID",
        F.when(
            F.col("LAST_UPDT_USER_ID").isNull() | (F.length(trim(F.col("LAST_UPDT_USER_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("LAST_UPDT_USER_ID")))
    )
)

df_businessRules_snapshot = df_businessRules_transform.select(
    trim(F.col("GRP_ID")).alias("GRP_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

write_files(
    df_businessRules_snapshot.select(
        rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
        F.col("SRC_SYS_CD_SK")
    ),
    f"{adls_path}/load/B_EVT_PAR_GRP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_evt_par_grp_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT GRP_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, EVT_PAR_GRP_SK FROM IDS.dummy_hf_evt_par_grp"
    )
    .load()
)

df_pkey_in = df_businessRules_transform.alias("Transform")
df_hf_evt_par_grp_lkup_alias = df_hf_evt_par_grp_lkup.alias("lkup")

df_pkey_joined = df_pkey_in.join(
    df_hf_evt_par_grp_lkup_alias,
    (
        (F.col("Transform.GRP_ID") == F.col("lkup.GRP_ID"))
        & (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
    ),
    "left"
)

df_enriched = df_pkey_joined
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.EVT_PAR_GRP_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "CurrRunCycleVal",
    F.lit(CurrRunCycle)
)

df_pkey_key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("EVT_PAR_GRP_SK"),
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycleVal").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("Transform.GRP_ACTV_IN"), 1, " ").alias("GRP_ACTV_IN"),
    rpad(F.col("Transform.EFF_DT"), 10, " ").alias("EFF_DT_SK"),
    rpad(F.col("Transform.TERM_DT"), 10, " ").alias("TERM_DT_SK"),
    F.col("Transform.GRP_COLOR_KEY_DESC").alias("GRP_COLOR_KEY_DESC"),
    F.col("Transform.GRP_CNTCT_NM").alias("GRP_CNTCT_NM"),
    F.col("Transform.GRP_CNTCT_CELL_PHN_NO").alias("GRP_CNTCT_CELL_PHN_NO"),
    F.col("Transform.GRP_CNTCT_WORK_PHN_NO").alias("GRP_CNTCT_WORK_PHN_NO"),
    F.col("Transform.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Transform.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

write_files(
    df_pkey_key.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("EVT_PAR_GRP_SK"),
        F.col("GRP_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_ACTV_IN"),
        F.col("EFF_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("GRP_COLOR_KEY_DESC"),
        F.col("GRP_CNTCT_NM"),
        F.col("GRP_CNTCT_CELL_PHN_NO"),
        F.col("GRP_CNTCT_WORK_PHN_NO"),
        F.col("LAST_UPDT_DTM"),
        F.col("LAST_UPDT_USER_ID")
    ),
    f"{adls_path}/key/WebUserInfoEvtParGrpExtr.EvtParGrp.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_pkey_updt = df_enriched.filter(
    F.col("lkup.EVT_PAR_GRP_SK").isNull()
).select(
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("EVT_PAR_GRP_SK")
)

temp_table_name = "STAGING.WebUserInfoEvtParGrpExtr_hf_evt_par_grp_updt_temp"
merge_target = "IDS.dummy_hf_evt_par_grp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)
df_pkey_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {merge_target} as target
USING {temp_table_name} as source
ON target.GRP_ID = source.GRP_ID AND target.SRC_SYS_CD = source.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    target.CRT_RUN_CYC_EXCTN_SK = source.CRT_RUN_CYC_EXCTN_SK,
    target.EVT_PAR_GRP_SK = source.EVT_PAR_GRP_SK
WHEN NOT MATCHED THEN
  INSERT (
    GRP_ID,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    EVT_PAR_GRP_SK
  )
  VALUES (
    source.GRP_ID,
    source.SRC_SYS_CD,
    source.CRT_RUN_CYC_EXCTN_SK,
    source.EVT_PAR_GRP_SK
  );
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)