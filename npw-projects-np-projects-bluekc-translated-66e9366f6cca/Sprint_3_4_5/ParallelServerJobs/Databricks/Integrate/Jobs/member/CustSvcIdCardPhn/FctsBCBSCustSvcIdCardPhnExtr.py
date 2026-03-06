# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  FctsBCBSCustSvcIdCardPhnExtr
# MAGIC Called By: CustSvcIdCardPhnExtrSeq (FacetsBCBSCustSvcIdCardPhnMbrCntl)
# MAGIC 
# MAGIC Process Description: Pull data from Facets BCBS (PHN_NBR) and load it to IDS(CUST_SVC_ID_CARD_PHN) 
# MAGIC  
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Santosh Bokka       9/3/2013             4390                   Originally Programmed                                                                 IDSNEWDEVL                 Kalyan Neelam          2013-10-29   
# MAGIC Prabhu ES              2022-03-11           S2S                    MSSQL ODBC conn params added                                            IntegrateDev5	Ken Bradmon	2022-06-03

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets BCBS Extension Data
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, concat, rpad
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# -----------------------------------------------------------------------------
# Parameter Retrieval
# -----------------------------------------------------------------------------
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
LastRunDt = get_widget_value('LastRunDt','')
EndDate = get_widget_value('EndDate','')
BeginDate = get_widget_value('BeginDate','')

# -----------------------------------------------------------------------------
# Stage: PHN_NBR (ODBCConnector) → df_PHN_NBR
# -----------------------------------------------------------------------------
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_phn_nbr = (
    f"SELECT DISTINCT "
    f"PHN_NBR.PHN_NBR_KEY,"
    f"PHN_NBR.PHN_NBR_AREA_CD,"
    f"PHN_NBR.PHN_NBR_EXCH_NO,"
    f"PHN_NBR.PHN_NBR_LINE_NO,"
    f"PHN_NBR.PHN_NBR_EXT_NO,"
    f"PHN_NBR.PHN_NBR_FRGN_NO,"
    f"PHN_NBR.PHN_NBR_UNLSTD_IN,"
    f"PHN_NBR.PHN_NBR_TS,"
    f"PHN_NBR.PHN_NBR_USR,"
    f"PHN_NBR.PHN_NBR_TYP_CD "
    f"FROM {BCBSOwner}.PHN_NBR as PHN_NBR "
    f"INNER JOIN {BCBSOwner}.GRP_PROD_PHN as GRP_PROD_PHN on GRP_PROD_PHN.PHN_NBR_KEY = PHN_NBR.PHN_NBR_KEY "
    f"INNER JOIN {BCBSOwner}.PHN_NBR_TYP as PHN_NBR_TYP on PHN_NBR.PHN_NBR_TYP_CD = PHN_NBR_TYP.PHN_NBR_TYP_CD "
    f"WHERE PHN_NBR.PHN_NBR_TS >= '{BeginDate}' AND PHN_NBR.PHN_NBR_TS <= '{EndDate}'"
)

df_PHN_NBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_phn_nbr)
    .load()
)

# -----------------------------------------------------------------------------
# Stage: StripField (CTransformerStage) → df_stripped
# -----------------------------------------------------------------------------
df_stripped = (
    df_PHN_NBR
    .withColumn("PHN_NBR_KEY", trim(col("PHN_NBR_KEY")))
    .withColumn("PHN_NBR_AREA_CD", trim(strip_field(col("PHN_NBR_AREA_CD"))))
    .withColumn("PHN_NBR_EXCH_NO", trim(strip_field(col("PHN_NBR_EXCH_NO"))))
    .withColumn("PHN_NBR_LINE_NO", trim(strip_field(col("PHN_NBR_LINE_NO"))))
    .withColumn("PHN_NBR_EXT_NO", trim(strip_field(col("PHN_NBR_EXT_NO"))))
    .withColumn("PHN_NBR_FRGN_NO", trim(strip_field(col("PHN_NBR_FRGN_NO"))))
    .withColumn("PHN_NBR_UNLSTD_IN", trim(strip_field(col("PHN_NBR_UNLSTD_IN").substr(1, 1))))
    .withColumn("PHN_NBR_TS", col("PHN_NBR_TS"))
    .withColumn("PHN_NBR_USR", trim(strip_field(col("PHN_NBR_USR"))))
    .withColumn("PHN_NBR_TYP_CD", trim(strip_field(col("PHN_NBR_TYP_CD"))))
)

# -----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage) → df_businessRules
# -----------------------------------------------------------------------------
df_businessRules = df_stripped.filter(
    (col("PHN_NBR_KEY").isNotNull()) | (length(col("PHN_NBR_KEY")) != 0)
)

df_businessRules = (
    df_businessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat(lit("FACETS;"), col("PHN_NBR_KEY")))
    .withColumn("CUST_SVC_ID_CARD_PHN_SK", lit(0))
    .withColumn("CUST_SVC_ID_CARD_PHN_KEY", col("PHN_NBR_KEY"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_NO_TYP_CD",
        when(col("PHN_NBR_TYP_CD").isNull(), lit(0)).otherwise(col("PHN_NBR_TYP_CD"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_UNLST_IN",
        when(
            (length(col("PHN_NBR_UNLSTD_IN")) == 0) | (col("PHN_NBR_UNLSTD_IN").isNull()),
            lit("N")
        ).otherwise(col("PHN_NBR_UNLSTD_IN"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_AREA_CD",
        when(
            (length(col("PHN_NBR_AREA_CD")) == 0) | (col("PHN_NBR_AREA_CD").isNull()),
            lit(" ")
        ).otherwise(col("PHN_NBR_AREA_CD"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_EXCH_NO",
        when(
            (length(col("PHN_NBR_EXCH_NO")) == 0) | (col("PHN_NBR_EXCH_NO").isNull()),
            lit(" ")
        ).otherwise(col("PHN_NBR_EXCH_NO"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_LN_NO",
        when(
            (length(col("PHN_NBR_LINE_NO")) == 0) | (col("PHN_NBR_LINE_NO").isNull()),
            lit(" ")
        ).otherwise(col("PHN_NBR_LINE_NO"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHNEXT_NO",
        when(
            (length(col("PHN_NBR_EXT_NO")) == 0) | (col("PHN_NBR_EXT_NO").isNull()),
            lit(" ")
        ).otherwise(col("PHN_NBR_EXT_NO"))
    )
    .withColumn(
        "CUST_SVC_ID_CARD_PHN_FRGN_NO",
        when(
            (length(col("PHN_NBR_FRGN_NO")) == 0) | (col("PHN_NBR_FRGN_NO").isNull()),
            lit(" ")
        ).otherwise(col("PHN_NBR_FRGN_NO"))
    )
)

# -----------------------------------------------------------------------------
# Stage: hf_cust_svc_id_card_phn (CHashedFileStage) - Scenario B
# Replace with reading from dummy table <dummy_hf_cust_svc_id_card_phn>.
# -----------------------------------------------------------------------------
df_hf_cust_svc_id_card_phn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option(
        "query",
        "SELECT SRC_SYS_CD, CUST_SVC_ID_CARD_PHN_KEY, CRT_RUN_CYC_EXCTN_SK, CUST_SVC_ID_CARD_PHN_SK "
        "FROM dummy_hf_cust_svc_id_card_phn"
    )
    .load()
)

# -----------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage) → Join (left) df_businessRules & df_hf_cust_svc_id_card_phn
# -----------------------------------------------------------------------------
df_joined = df_businessRules.alias("Transform").join(
    df_hf_cust_svc_id_card_phn.alias("lkup"),
    [
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.CUST_SVC_ID_CARD_PHN_KEY") == col("lkup.CUST_SVC_ID_CARD_PHN_KEY")
    ],
    how="left"
)

df_enriched = (
    df_joined
    .withColumn(
        "SK",
        when(col("lkup.CUST_SVC_ID_CARD_PHN_SK").isNull(), lit(None))
        .otherwise(col("lkup.CUST_SVC_ID_CARD_PHN_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup.CUST_SVC_ID_CARD_PHN_SK").isNull(), lit(CurrRunCycle))
        .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
)

# Apply SurrogateKeyGen in place of KeyMgtGetNextValueConcurrent
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# -----------------------------------------------------------------------------
# Output link "Key" => FacetsBCBSCustSvcIdCardPhnExtr (CSeqFileStage)
# -----------------------------------------------------------------------------
df_key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SK").alias("CUST_SVC_ID_CARD_PHN_SK"),
    col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CUST_SVC_ID_CARD_PHN_NO_TYP_CD").alias("CUST_SVC_ID_CARD_PHN_NO_TYP_CD"),
    rpad(col("CUST_SVC_ID_CARD_PHN_UNLST_IN"), 1, " ").alias("CUST_SVC_ID_CARD_PHN_UNLST_IN"),
    rpad(col("CUST_SVC_ID_CARD_PHN_AREA_CD"), 3, " ").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
    rpad(col("CUST_SVC_ID_CARD_PHN_EXCH_NO"), 3, " ").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
    rpad(col("CUST_SVC_ID_CARD_PHN_LN_NO"), 4, " ").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
    rpad(col("CUST_SVC_ID_CARD_PHNEXT_NO"), 5, " ").alias("CUST_SVC_ID_CARD_PHNEXT_NO"),
    rpad(col("CUST_SVC_ID_CARD_PHN_FRGN_NO"), 22, " ").alias("CUST_SVC_ID_CARD_PHN_FRGN_NO")
)

output_path = f"{adls_path}/key/FacetsBCBSCustSvcIdCardPhnExtr.CustSvcIdCardPhn.dat.{RunID}"
write_files(
    df_key,
    output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Output link "updt" => hf_cust_svc_id_card_phn_updt (CHashedFileStage) - Scenario B write
# -----------------------------------------------------------------------------
df_updt = df_enriched.filter(col("lkup.CUST_SVC_ID_CARD_PHN_SK").isNull()).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("CUST_SVC_ID_CARD_PHN_SK")
)

# Write df_updt to a staging table, then MERGE into dummy_hf_cust_svc_id_card_phn
temp_table_name = "STAGING.FctsBCBSCustSvcIdCardPhnExtr_hf_cust_svc_id_card_phn_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_bcbs, jdbc_props_bcbs)

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE dummy_hf_cust_svc_id_card_phn AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.CUST_SVC_ID_CARD_PHN_KEY = S.CUST_SVC_ID_CARD_PHN_KEY "
    f"WHEN MATCHED THEN "
    f"  UPDATE SET T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    f"              T.CUST_SVC_ID_CARD_PHN_SK = S.CUST_SVC_ID_CARD_PHN_SK "
    f"WHEN NOT MATCHED THEN "
    f"  INSERT (SRC_SYS_CD, CUST_SVC_ID_CARD_PHN_KEY, CRT_RUN_CYC_EXCTN_SK, CUST_SVC_ID_CARD_PHN_SK) "
    f"  VALUES (S.SRC_SYS_CD, S.CUST_SVC_ID_CARD_PHN_KEY, S.CRT_RUN_CYC_EXCTN_SK, S.CUST_SVC_ID_CARD_PHN_SK);"
)
execute_dml(merge_sql, jdbc_url_bcbs, jdbc_props_bcbs)