# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 05/12/09 13:30:39 Batch  15108_48642 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 05/12/09 13:07:49 Batch  15108_47273 INIT bckcett:31540 testIDS dsadm BLS FOR RT
# MAGIC ^1_1 04/28/09 15:30:57 Batch  15094_55861 PROMOTE bckcett testIDS u03651 steph for Ralph
# MAGIC ^1_1 04/28/09 15:26:41 Batch  15094_55604 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsUMIpRvwExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMIR_REVIEW to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                             Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                       ----------------------------------              ---------------------------------               -------------------------
# MAGIC    
# MAGIC Bhoomi Dasari                 03/09/2009              3808                               Originally Programmed                                                      devlIDS                           Steph Goddard                     03/30/2009
# MAGIC 
# MAGIC Prabhu ES                       2022-03-07              S2S Remediation             MSSQL ODBC conn params added                        IntegrateDev5		Harsha Ravuri		06-14-2022

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Snapshot used for balancing
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
DriverTable = get_widget_value('DriverTable','TMP_IDS_UM')
RunID = get_widget_value('RunID','20090323')
CurrDate = get_widget_value('CurrDate','2009-03-23')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
TmpOutFile = get_widget_value('TmpOutFile','IdsUmIpRvwExtr.tmp')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, UM_REF_ID, UM_IP_RVW_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_IP_RVW_SK FROM IDS.dummy_hf_um_ip_rvw")
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_cmc_umir_review = f"""
SELECT 
UMIR.UMUM_REF_ID,
UMIR.UMIR_SEQ_NO,
UMIR.UMDX_REF_DTM,
UMIR.UMIR_REVIEW_DT,
UMIR.UMIR_CAT_CURR,
UMIR.UMIR_LOS_REQ_TOT,
UMIR.UMIR_LOS_AUTH_TOT,
UMIR.UMIR_LOS_NORM,
UMIR.UMIR_MCTR_LSOR,
UMIR.UMIR_LOS_AVERAGE
FROM {FacetsOwner}.CMC_UMIR_REVIEW UMIR,
     tempdb..{DriverTable} as T
WHERE UMIR.UMUM_REF_ID = T.UM_REF_ID
"""
df_CMC_UMIR_REVIEW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cmc_umir_review)
    .load()
)

df_Strip = (
    df_CMC_UMIR_REVIEW
    .withColumn("UMUM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UMIR_SEQ_NO", F.col("UMIR_SEQ_NO"))
    .withColumn("UMIR_MCTR_LSOR", trim(strip_field(F.col("UMIR_MCTR_LSOR"))))
    .withColumn("UMIR_CAT_CURR", trim(strip_field(F.col("UMIR_CAT_CURR"))))
    .withColumn("UMDX_REF_DTM", F.col("UMDX_REF_DTM"))
    .withColumn("UMIR_REVIEW_DT", F.col("UMIR_REVIEW_DT"))
    .withColumn("UMIR_LOS_AUTH_TOT", F.col("UMIR_LOS_AUTH_TOT"))
    .withColumn("UMIR_LOS_AVERAGE", F.col("UMIR_LOS_AVERAGE"))
    .withColumn("UMIR_LOS_NORM", F.col("UMIR_LOS_NORM"))
    .withColumn("UMIR_LOS_REQ_TOT", F.col("UMIR_LOS_REQ_TOT"))
)

df_BusinessRules = (
    df_Strip
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("UMUM_REF_ID"), F.lit(";"), F.col("UMIR_SEQ_NO")))
    .withColumn("UM_IP_RVW_SK", F.lit(0))
    .withColumn("UM_REF_ID", F.col("UMUM_REF_ID"))
    .withColumn("UM_IP_RVW_SEQ_NO", F.col("UMIR_SEQ_NO"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("UMIR_MCTR_LSOR", F.when((F.col("UMIR_MCTR_LSOR").isNull()) | (F.length(F.col("UMIR_MCTR_LSOR")) == 0), F.lit("NA")).otherwise(F.col("UMIR_MCTR_LSOR")))
    .withColumn("UMIR_CAT_CURR", F.when((F.col("UMIR_CAT_CURR").isNull()) | (F.length(F.col("UMIR_CAT_CURR")) == 0), F.lit("NA")).otherwise(F.col("UMIR_CAT_CURR")))
    .withColumn("DIAG_SET_CRT_DTM", F.col("UMDX_REF_DTM"))
    .withColumn("RVW_DT_SK", F.col("UMIR_REVIEW_DT"))
    .withColumn("AUTH_TOT_LOS_NO", F.when((F.col("UMIR_LOS_AUTH_TOT").isNull()) | (F.length(F.col("UMIR_LOS_AUTH_TOT")) == 0), F.lit(0)).otherwise(F.col("UMIR_LOS_AUTH_TOT")))
    .withColumn("AVG_TOT_LOS_NO", F.when((F.col("UMIR_LOS_AVERAGE").isNull()) | (F.length(F.col("UMIR_LOS_AVERAGE")) == 0), F.lit(0)).otherwise(F.col("UMIR_LOS_AVERAGE")))
    .withColumn("NRMTV_TOT_LOS_NO", F.when((F.col("UMIR_LOS_NORM").isNull()) | (F.length(F.col("UMIR_LOS_NORM")) == 0), F.lit(0)).otherwise(F.col("UMIR_LOS_NORM")))
    .withColumn("RQST_TOT_LOS_NO", F.when((F.col("UMIR_LOS_REQ_TOT").isNull()) | (F.length(F.col("UMIR_LOS_REQ_TOT")) == 0), F.lit(0)).otherwise(F.col("UMIR_LOS_REQ_TOT")))
)

df_join = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_lkup.alias("lkup"),
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
        & (F.col("Transform.UM_REF_ID") == F.col("lkup.UM_REF_ID"))
        & (F.col("Transform.UM_IP_RVW_SEQ_NO") == F.col("lkup.UM_IP_RVW_SEQ_NO")),
        "left"
    )
)

df_enriched = (
    df_join
    .withColumn("SK", F.when(F.col("lkup.UM_IP_RVW_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.UM_IP_RVW_SK")))
    .withColumn("NewCrtRunCycExtcnSk", F.when(F.col("lkup.UM_IP_RVW_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")))
    .withColumn("CurrRunCycle", F.lit(CurrRunCycle))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("UM_IP_RVW_SK"),
    F.col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Transform.UM_IP_RVW_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("Transform.UMIR_MCTR_LSOR"), 4, " ").alias("UMIR_MCTR_LSOR"),
    F.rpad(F.col("Transform.UMIR_CAT_CURR"), 1, " ").alias("UMIR_CAT_CURR"),
    F.col("Transform.DIAG_SET_CRT_DTM").alias("DIAG_SET_CRT_DTM"),
    F.rpad(F.col("Transform.RVW_DT_SK"), 10, " ").alias("RVW_DT_SK"),
    F.col("Transform.AUTH_TOT_LOS_NO").alias("AUTH_TOT_LOS_NO"),
    F.col("Transform.AVG_TOT_LOS_NO").alias("AVG_TOT_LOS_NO"),
    F.col("Transform.NRMTV_TOT_LOS_NO").alias("NRMTV_TOT_LOS_NO"),
    F.col("Transform.RQST_TOT_LOS_NO").alias("RQST_TOT_LOS_NO")
)

df_updt = (
    df_enriched
    .filter(F.col("lkup.UM_IP_RVW_SK").isNull())
    .select(
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.UM_REF_ID").alias("UM_REF_ID"),
        F.col("Transform.UM_IP_RVW_SEQ_NO").alias("UM_IP_RVW_SEQ_NO"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("SK").alias("UM_IP_RVW_SK")
    )
)

execute_dml("DROP TABLE IF EXISTS STAGING.FctsUMIpRvwExtr_hf_write_temp", jdbc_url_ids, jdbc_props_ids)

df_updt.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.FctsUMIpRvwExtr_hf_write_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO IDS.dummy_hf_um_ip_rvw AS T
USING STAGING.FctsUMIpRvwExtr_hf_write_temp AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.UM_REF_ID = S.UM_REF_ID
    AND T.UM_IP_RVW_SEQ_NO = S.UM_IP_RVW_SEQ_NO
)
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.UM_IP_RVW_SK = S.UM_IP_RVW_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        UM_REF_ID,
        UM_IP_RVW_SEQ_NO,
        CRT_RUN_CYC_EXCTN_SK,
        UM_IP_RVW_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.UM_REF_ID,
        S.UM_IP_RVW_SEQ_NO,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.UM_IP_RVW_SK
    );
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

write_files(
    df_Key,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

query_facets_cmc_umir_review_2 = f"""
SELECT 
UMIR.UMUM_REF_ID,
UMIR.UMIR_SEQ_NO
FROM {FacetsOwner}.CMC_UMIR_REVIEW UMIR,
     tempdb..{DriverTable} as T
WHERE UMIR.UMUM_REF_ID = T.UM_REF_ID
"""
df_Facets_CMC_UMIR_REVIEW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_facets_cmc_umir_review_2)
    .load()
)

df_Strip2 = (
    df_Facets_CMC_UMIR_REVIEW
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("UM_REF_ID", trim(strip_field(F.col("UMUM_REF_ID"))))
    .withColumn("UM_IP_RVW_SEQ_NO", F.col("UMIR_SEQ_NO"))
)

df_B_UM_IP_RVW = df_Strip2.select("SRC_SYS_CD_SK", "UM_REF_ID", "UM_IP_RVW_SEQ_NO")

write_files(
    df_B_UM_IP_RVW,
    f"{adls_path}/load/B_UM_IP_RVW.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)