# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  FctsAplCntctExtr
# MAGIC DESCRIPTION:   Pulls data from CMC_APCT_CONTACTS for loading into IDS APL_CNTCT.
# MAGIC 
# MAGIC PROCESSING:    Creates appeals contact data
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/02/2007                	3028		Initial program                                                              		devlIDS30            	Steph Goddard           8/23/07       
# MAGIC Bhoomi Dasari    		10/18/2007                 	3028		Added Balancing Snapshot                                         		devlIDS30          	Steph Goddard           10/18/2007
# MAGIC Jag Yelavarthi    		2015-01-19                  TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"                		IntegrateNewDevl   	Kalyan Neelam  	2015-01-27
# MAGIC Prabhu ES         		2022-02-25                  S2S 		Remediation-MSSQL conn param added            		IntegrateDev5	Ken Bradmon	2022-05-20

# MAGIC Added Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Contact Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

query_extract_FACETS = (
    "SELECT \n"
    "APCT.APAP_ID,\n"
    "APCT.APCT_SEQ_NO,\n"
    "APCT.APCT_MCTR_CAT,\n"
    "APCT.MCRE_ID\n"
    "FROM \n"
    f"{FacetsOwner}.CMC_APCT_CONTACTS APCT,\n"
    f"{FacetsOwner}.CMC_APAP_APPEALS APAP\n"
    "WHERE\n"
    "APCT.APAP_ID = APAP.APAP_ID\n"
    f"AND APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'\n"
    f"AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}'"
)

df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_extract_FACETS)
    .load()
)

query_mcreid = (
    "SELECT MCRE_ID, MCRE_NAME "
    f"FROM {FacetsOwner}.CMC_MCRE_RELAT_ENT CMC_MCRE_RELAT_ENT"
)

df_FACETS_mcreid = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_mcreid)
    .load()
)

df_hf_apl_relat_ent_dedup = dedup_sort(
    df_FACETS_mcreid,
    ["MCRE_ID"],
    []
)

df_Strip = (
    df_FACETS_Extract.alias("Extract")
    .join(
        df_hf_apl_relat_ent_dedup.alias("mcrelkup"),
        F.col("Extract.MCRE_ID") == F.col("mcrelkup.MCRE_ID"),
        "left"
    )
    .select(
        Convert("CHAR(10):CHAR(13):CHAR(9)", "", F.col("Extract.APAP_ID")).alias("APAP_ID"),
        F.col("Extract.APCT_SEQ_NO").alias("APCT_SEQ_NO"),
        Convert("CHAR(10):CHAR(13):CHAR(9)", "", F.col("Extract.APCT_MCTR_CAT")).alias("APCT_MCTR_CAT"),
        Convert("CHAR(10):CHAR(13):CHAR(9)", "", F.col("Extract.MCRE_ID")).alias("MCRE_ID"),
        Convert("CHAR(10):CHAR(13):CHAR(9)", "", F.col("mcrelkup.MCRE_NAME")).alias("MCRE_NAME")
    )
)

RowPassThru = "Y"
svSrcSysCd = "FACETS"

df_BusinessRules = (
    df_Strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit(RowPassThru))
    .withColumn("FIRST_RECYC_DT", F.lit(RunDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(svSrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.lit(svSrcSysCd),
            trim(F.col("APAP_ID")),
            F.col("APCT_SEQ_NO")
        )
    )
    .withColumn("APL_CNTCT_SK", F.lit(0))
    .withColumn("APL_ID", trim(F.col("APAP_ID")))
    .withColumn("SEQ_NO", F.col("APCT_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "APL_SK",
        F.when(
            (F.length(trim(F.col("APAP_ID"))) == 0) | (F.col("APAP_ID").isNull()),
            F.lit("NA")
        ).otherwise(trim(F.col("APAP_ID")))
    )
    .withColumn(
        "APL_CNTCT_CAT_CD_SK",
        F.when(
            (F.length(trim(F.col("APCT_MCTR_CAT"))) == 0) | (F.col("APCT_MCTR_CAT").isNull()),
            F.lit("NA")
        ).otherwise(trim(F.col("APCT_MCTR_CAT")))
    )
    .withColumn(
        "APL_CNTCT_ID",
        F.when(
            (F.length(trim(F.col("MCRE_ID"))) == 0) | (F.col("MCRE_ID").isNull()),
            F.lit(None)
        ).otherwise(trim(F.col("MCRE_ID")))
    )
    .withColumn(
        "APL_CNTCT_NM",
        F.when(
            (F.length(trim(F.col("MCRE_NAME"))) == 0) | (F.col("MCRE_NAME").isNull()),
            F.lit(None)
        ).otherwise(trim(F.upper(F.col("MCRE_NAME"))))
    )
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_dummy_hf_apl_cntct = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, APL_ID, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, APL_CNTCT_SK FROM dummy_hf_apl_cntct"
    )
    .load()
)

df_PrimaryKey = (
    df_BusinessRules.alias("Transform")
    .join(
        df_dummy_hf_apl_cntct.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.APL_ID") == F.col("lkup.APL_ID"),
            F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO")
        ],
        "left"
    )
    .withColumn("SK", F.col("lkup.APL_CNTCT_SK"))
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(
            F.col("lkup.APL_CNTCT_SK").isNull(),
            F.lit(CurrRunCycle)
        ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_PrimaryKey,<DB sequence name>,"SK",<schema>,<secret_name>)

df_updt = df_enriched.filter(F.col("lkup.APL_CNTCT_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("APL_CNTCT_SK")
)

df_key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("APL_CNTCT_SK"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.APL_SK").alias("APL_SK"),
    F.col("Transform.APL_CNTCT_CAT_CD_SK").alias("APL_CNTCT_CAT_CD_SK"),
    F.col("Transform.APL_CNTCT_ID").alias("APL_CNTCT_ID"),
    F.col("Transform.APL_CNTCT_NM").alias("APL_CNTCT_NM"),
    F.col("lkup.APL_CNTCT_SK").alias("_discard_lkupval")
)

write_files(
    df_key.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.rpad(F.col("SRC_SYS_CD"),<...>," ").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("APL_CNTCT_SK"),
        F.rpad(F.col("APL_ID"),<...>," ").alias("APL_ID"),
        F.col("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("APL_SK"),<...>," ").alias("APL_SK"),
        F.rpad(F.col("APL_CNTCT_CAT_CD_SK"),<...>," ").alias("APL_CNTCT_CAT_CD_SK"),
        F.rpad(F.col("APL_CNTCT_ID"),<...>," ").alias("APL_CNTCT_ID"),
        F.rpad(F.col("APL_CNTCT_NM"),<...>," ").alias("APL_CNTCT_NM")
    ),
    f"{adls_path}/key/FctsAplCntctExtr.AplCntct.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.FctsAplCntctExtr_hf_apl_cntct_updt_temp",
    jdbc_url_ids,
    jdbc_props_ids
)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsAplCntctExtr_hf_apl_cntct_updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_updt = (
    "MERGE INTO dummy_hf_apl_cntct AS T "
    "USING STAGING.FctsAplCntctExtr_hf_apl_cntct_updt_temp AS S "
    "ON T.SRC_SYS_CD=S.SRC_SYS_CD AND T.APL_ID=S.APL_ID AND T.SEQ_NO=S.SEQ_NO "
    "WHEN MATCHED THEN "
    "  UPDATE SET "
    "    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK, "
    "    T.APL_CNTCT_SK=S.APL_CNTCT_SK "
    "WHEN NOT MATCHED THEN "
    "  INSERT (SRC_SYS_CD, APL_ID, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, APL_CNTCT_SK) "
    "  VALUES (S.SRC_SYS_CD, S.APL_ID, S.SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.APL_CNTCT_SK);"
)
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

query_extract_SnapShot_FACETS = (
    "SELECT \n"
    "APCT.APAP_ID,\n"
    "APCT.APCT_SEQ_NO,\n"
    "APCT.MCRE_ID\n"
    "FROM \n"
    f"{FacetsOwner}.CMC_APCT_CONTACTS APCT,\n"
    f"{FacetsOwner}.CMC_APAP_APPEALS APAP\n"
    "WHERE\n"
    "APCT.APAP_ID = APAP.APAP_ID\n"
    f"AND APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'\n"
    f"AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}'"
)

df_SnapShot_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_extract_SnapShot_FACETS)
    .load()
)

df_Rules = (
    df_SnapShot_FACETS_Extract.alias("Extract")
    .withColumn("SRC_SYS_CD_SK", GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X"))
    .select(
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        trim(Convert("CHAR(10):CHAR(13):CHAR(9)", "", F.col("Extract.APAP_ID"))).alias("APL_ID"),
        F.col("Extract.APCT_SEQ_NO").alias("SEQ_NO"),
        F.when(
            (F.length(trim(F.col("Extract.MCRE_ID"))) == 0) | (F.col("Extract.MCRE_ID").isNull()),
            F.lit(None)
        ).otherwise(trim(F.col("Extract.MCRE_ID"))).alias("APL_CNTCT_ID")
    )
)

write_files(
    df_Rules.select(
        F.col("SRC_SYS_CD_SK"),
        F.col("APL_ID"),
        F.col("SEQ_NO"),
        F.col("APL_CNTCT_ID")
    ),
    f"{adls_path}/load/B_APL_CNTCT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)