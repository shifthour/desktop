# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               06/20/2013        5114                             Load DM Table PROV_DIR_DM_PROV                              EnterpriseWrhsDevl                    Peter Marshall              9/3/2013
# MAGIC 
# MAGIC Mohan Karnati          2019-04-16         NP                         Adding PROV_TXNMY_CD in  seq_PROV_DIR_DM_
# MAGIC                                                                                                     PROV_csv_load  and populating it to target table                    EnterpriseDev1                     Jaideep Mankala         04/24/2019

# MAGIC Job Name: EdwDmProvDirDmProvLoad
# MAGIC Read Load File created in the EdwDmProvDirDmProvtExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the PROV_DIR_DM_FCLTY_TYP_CT Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter definitions
WebProvDirOwner = get_widget_value('WebProvDirOwner','')
webprovdir_secret_name = get_widget_value('webprovdir_secret_name','')
ProvDirRecordCount = get_widget_value('ProvDirRecordCount','')
ProvDirArraySize = get_widget_value('ProvDirArraySize','')

# Define schema for seq_PROV_DIR_DM_PROV_csv_load
schema_seq_PROV_DIR_DM_PROV_csv_load = StructType([
    StructField("PROV_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CMN_PRCT_GNDR_CD", StringType(), True),
    StructField("CMN_PRCT_GNDR_NM", StringType(), True),
    StructField("FCLTY_TYP_CD", StringType(), True),
    StructField("FCLTY_TYP_NM", StringType(), True),
    StructField("PROV_BILL_SVC_ID", StringType(), True),
    StructField("PROV_BILL_SVC_NM", StringType(), True),
    StructField("PROV_ENTY_CD", StringType(), True),
    StructField("PROV_ENTY_NM", StringType(), True),
    StructField("PROV_PRCTC_TYP_CD", StringType(), True),
    StructField("PROV_PRCTC_TYP_NM", StringType(), True),
    StructField("PROV_REL_GRP_PROV_ID", StringType(), True),
    StructField("PROV_REL_GRP_PROV_NM", StringType(), True),
    StructField("PROV_REL_IPA_PROV_ID", StringType(), True),
    StructField("PROV_REL_IPA_PROV_NM", StringType(), True),
    StructField("PROV_SPEC_CD", StringType(), True),
    StructField("PROV_SPEC_NM", StringType(), True),
    StructField("PROV_TYP_CD", StringType(), True),
    StructField("PROV_TYP_NM", StringType(), True),
    StructField("ACTV_IN", StringType(), True),
    StructField("CNTGS_CNTY_PROV_IN", StringType(), True),
    StructField("ITS_HOME_GNRC_PROV_IN", StringType(), True),
    StructField("LEAPFROG_IN", StringType(), True),
    StructField("LOCAL_BCBSKC_PROV_IN", StringType(), True),
    StructField("PAR_PROV_IN", StringType(), True),
    StructField("CMN_PRCT_BRTH_DT", TimestampType(), True),
    StructField("CMN_PRCT_LAST_CRDTL_DT", TimestampType(), True),
    StructField("CMN_PRCT_FIRST_NM", StringType(), True),
    StructField("CMN_PRCT_MIDINIT", StringType(), True),
    StructField("CMN_PRCT_LAST_NM", StringType(), True),
    StructField("CMN_PRCT_ID", StringType(), True),
    StructField("CMN_PRCT_SSN", StringType(), True),
    StructField("CMN_PRCT_TTL", StringType(), True),
    StructField("PROV_ADDR_ID", StringType(), True),
    StructField("PROV_NM", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("PROV_TXNMY_CD", StringType(), True)
])

# Read seq_PROV_DIR_DM_PROV_csv_load
df_seq_PROV_DIR_DM_PROV_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_DIR_DM_PROV_csv_load)
    .load(f"{adls_path}/load/PROV_DIR_DM_PROV.dat")
)

# cpy_forBuffer (PxCopy) - pass-through of columns
df_cpy_forBuffer = df_seq_PROV_DIR_DM_PROV_csv_load.select(
    col("PROV_ID").alias("PROV_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    col("CMN_PRCT_GNDR_NM").alias("CMN_PRCT_GNDR_NM"),
    col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    col("FCLTY_TYP_NM").alias("FCLTY_TYP_NM"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    col("PROV_BILL_SVC_NM").alias("PROV_BILL_SVC_NM"),
    col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    col("PROV_ENTY_NM").alias("PROV_ENTY_NM"),
    col("PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    col("PROV_PRCTC_TYP_NM").alias("PROV_PRCTC_TYP_NM"),
    col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    col("PROV_REL_IPA_PROV_ID").alias("PROV_REL_IPA_PROV_ID"),
    col("PROV_REL_IPA_PROV_NM").alias("PROV_REL_IPA_PROV_NM"),
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    col("PROV_TYP_NM").alias("PROV_TYP_NM"),
    col("ACTV_IN").alias("ACTV_IN"),
    col("CNTGS_CNTY_PROV_IN").alias("CNTGS_CNTY_PROV_IN"),
    col("ITS_HOME_GNRC_PROV_IN").alias("ITS_HOME_GNRC_PROV_IN"),
    col("LEAPFROG_IN").alias("LEAPFROG_IN"),
    col("LOCAL_BCBSKC_PROV_IN").alias("LOCAL_BCBSKC_PROV_IN"),
    col("PAR_PROV_IN").alias("PAR_PROV_IN"),
    col("CMN_PRCT_BRTH_DT").alias("CMN_PRCT_BRTH_DT"),
    col("CMN_PRCT_LAST_CRDTL_DT").alias("CMN_PRCT_LAST_CRDTL_DT"),
    col("CMN_PRCT_FIRST_NM").alias("CMN_PRCT_FIRST_NM"),
    col("CMN_PRCT_MIDINIT").alias("CMN_PRCT_MIDINIT"),
    col("CMN_PRCT_LAST_NM").alias("CMN_PRCT_LAST_NM"),
    col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    col("CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_NM").alias("PROV_NM"),
    col("PROV_NPI").alias("PROV_NPI"),
    col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("PROV_TXNMY_CD").alias("PROV_TXNMY_CD")
)

# Apply rpad for char/varchar columns
df_cpy_forBuffer = (
    df_cpy_forBuffer
    .withColumn("PROV_ID", rpad(col("PROV_ID"), 255, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 255, " "))
    .withColumn("CMN_PRCT_GNDR_CD", rpad(col("CMN_PRCT_GNDR_CD"), 255, " "))
    .withColumn("CMN_PRCT_GNDR_NM", rpad(col("CMN_PRCT_GNDR_NM"), 255, " "))
    .withColumn("FCLTY_TYP_CD", rpad(col("FCLTY_TYP_CD"), 255, " "))
    .withColumn("FCLTY_TYP_NM", rpad(col("FCLTY_TYP_NM"), 55, " "))
    .withColumn("PROV_BILL_SVC_ID", rpad(col("PROV_BILL_SVC_ID"), 255, " "))
    .withColumn("PROV_BILL_SVC_NM", rpad(col("PROV_BILL_SVC_NM"), 255, " "))
    .withColumn("PROV_ENTY_CD", rpad(col("PROV_ENTY_CD"), 255, " "))
    .withColumn("PROV_ENTY_NM", rpad(col("PROV_ENTY_NM"), 255, " "))
    .withColumn("PROV_PRCTC_TYP_CD", rpad(col("PROV_PRCTC_TYP_CD"), 255, " "))
    .withColumn("PROV_PRCTC_TYP_NM", rpad(col("PROV_PRCTC_TYP_NM"), 255, " "))
    .withColumn("PROV_REL_GRP_PROV_ID", rpad(col("PROV_REL_GRP_PROV_ID"), 255, " "))
    .withColumn("PROV_REL_GRP_PROV_NM", rpad(col("PROV_REL_GRP_PROV_NM"), 255, " "))
    .withColumn("PROV_REL_IPA_PROV_ID", rpad(col("PROV_REL_IPA_PROV_ID"), 255, " "))
    .withColumn("PROV_REL_IPA_PROV_NM", rpad(col("PROV_REL_IPA_PROV_NM"), 255, " "))
    .withColumn("PROV_SPEC_CD", rpad(col("PROV_SPEC_CD"), 255, " "))
    .withColumn("PROV_SPEC_NM", rpad(col("PROV_SPEC_NM"), 255, " "))
    .withColumn("PROV_TYP_CD", rpad(col("PROV_TYP_CD"), 255, " "))
    .withColumn("PROV_TYP_NM", rpad(col("PROV_TYP_NM"), 255, " "))
    .withColumn("ACTV_IN", rpad(col("ACTV_IN"), 1, " "))
    .withColumn("CNTGS_CNTY_PROV_IN", rpad(col("CNTGS_CNTY_PROV_IN"), 1, " "))
    .withColumn("ITS_HOME_GNRC_PROV_IN", rpad(col("ITS_HOME_GNRC_PROV_IN"), 1, " "))
    .withColumn("LEAPFROG_IN", rpad(col("LEAPFROG_IN"), 1, " "))
    .withColumn("LOCAL_BCBSKC_PROV_IN", rpad(col("LOCAL_BCBSKC_PROV_IN"), 1, " "))
    .withColumn("PAR_PROV_IN", rpad(col("PAR_PROV_IN"), 1, " "))
    .withColumn("CMN_PRCT_FIRST_NM", rpad(col("CMN_PRCT_FIRST_NM"), 255, " "))
    .withColumn("CMN_PRCT_MIDINIT", rpad(col("CMN_PRCT_MIDINIT"), 1, " "))
    .withColumn("CMN_PRCT_LAST_NM", rpad(col("CMN_PRCT_LAST_NM"), 255, " "))
    .withColumn("CMN_PRCT_ID", rpad(col("CMN_PRCT_ID"), 255, " "))
    .withColumn("CMN_PRCT_SSN", rpad(col("CMN_PRCT_SSN"), 255, " "))
    .withColumn("CMN_PRCT_TTL", rpad(col("CMN_PRCT_TTL"), 10, " "))
    .withColumn("PROV_ADDR_ID", rpad(col("PROV_ADDR_ID"), 255, " "))
    .withColumn("PROV_NM", rpad(col("PROV_NM"), 255, " "))
    .withColumn("PROV_NPI", rpad(col("PROV_NPI"), 255, " "))
    .withColumn("PROV_TAX_ID", rpad(col("PROV_TAX_ID"), 255, " "))
    .withColumn("PROV_TXNMY_CD", rpad(col("PROV_TXNMY_CD"), 255, " "))
)

# Select final column order before writing to database
df_final_for_db = df_cpy_forBuffer.select(
    "PROV_ID","SRC_SYS_CD","CMN_PRCT_GNDR_CD","CMN_PRCT_GNDR_NM","FCLTY_TYP_CD",
    "FCLTY_TYP_NM","PROV_BILL_SVC_ID","PROV_BILL_SVC_NM","PROV_ENTY_CD","PROV_ENTY_NM",
    "PROV_PRCTC_TYP_CD","PROV_PRCTC_TYP_NM","PROV_REL_GRP_PROV_ID","PROV_REL_GRP_PROV_NM",
    "PROV_REL_IPA_PROV_ID","PROV_REL_IPA_PROV_NM","PROV_SPEC_CD","PROV_SPEC_NM",
    "PROV_TYP_CD","PROV_TYP_NM","ACTV_IN","CNTGS_CNTY_PROV_IN","ITS_HOME_GNRC_PROV_IN",
    "LEAPFROG_IN","LOCAL_BCBSKC_PROV_IN","PAR_PROV_IN","CMN_PRCT_BRTH_DT","CMN_PRCT_LAST_CRDTL_DT",
    "CMN_PRCT_FIRST_NM","CMN_PRCT_MIDINIT","CMN_PRCT_LAST_NM","CMN_PRCT_ID","CMN_PRCT_SSN",
    "CMN_PRCT_TTL","PROV_ADDR_ID","PROV_NM","PROV_NPI","PROV_TAX_ID","LAST_UPDT_DT",
    "PROV_TXNMY_CD"
)

# Connect and write to staging table, then merge
jdbc_url, jdbc_props = get_db_config(webprovdir_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.EdwDmProvDirDmProvLoad_ODBC_PROV_DIR_DM_PROV_out_temp", jdbc_url, jdbc_props)

df_final_for_db.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdwDmProvDirDmProvLoad_ODBC_PROV_DIR_DM_PROV_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE {WebProvDirOwner}.PROV_DIR_DM_PROV AS T "
    f"USING STAGING.EdwDmProvDirDmProvLoad_ODBC_PROV_DIR_DM_PROV_out_temp AS S "
    f"ON (T.PROV_ID = S.PROV_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CMN_PRCT_GNDR_CD = S.CMN_PRCT_GNDR_CD, "
    f"T.CMN_PRCT_GNDR_NM = S.CMN_PRCT_GNDR_NM, "
    f"T.FCLTY_TYP_CD = S.FCLTY_TYP_CD, "
    f"T.FCLTY_TYP_NM = S.FCLTY_TYP_NM, "
    f"T.PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID, "
    f"T.PROV_BILL_SVC_NM = S.PROV_BILL_SVC_NM, "
    f"T.PROV_ENTY_CD = S.PROV_ENTY_CD, "
    f"T.PROV_ENTY_NM = S.PROV_ENTY_NM, "
    f"T.PROV_PRCTC_TYP_CD = S.PROV_PRCTC_TYP_CD, "
    f"T.PROV_PRCTC_TYP_NM = S.PROV_PRCTC_TYP_NM, "
    f"T.PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID, "
    f"T.PROV_REL_GRP_PROV_NM = S.PROV_REL_GRP_PROV_NM, "
    f"T.PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID, "
    f"T.PROV_REL_IPA_PROV_NM = S.PROV_REL_IPA_PROV_NM, "
    f"T.PROV_SPEC_CD = S.PROV_SPEC_CD, "
    f"T.PROV_SPEC_NM = S.PROV_SPEC_NM, "
    f"T.PROV_TYP_CD = S.PROV_TYP_CD, "
    f"T.PROV_TYP_NM = S.PROV_TYP_NM, "
    f"T.ACTV_IN = S.ACTV_IN, "
    f"T.CNTGS_CNTY_PROV_IN = S.CNTGS_CNTY_PROV_IN, "
    f"T.ITS_HOME_GNRC_PROV_IN = S.ITS_HOME_GNRC_PROV_IN, "
    f"T.LEAPFROG_IN = S.LEAPFROG_IN, "
    f"T.LOCAL_BCBSKC_PROV_IN = S.LOCAL_BCBSKC_PROV_IN, "
    f"T.PAR_PROV_IN = S.PAR_PROV_IN, "
    f"T.CMN_PRCT_BRTH_DT = S.CMN_PRCT_BRTH_DT, "
    f"T.CMN_PRCT_LAST_CRDTL_DT = S.CMN_PRCT_LAST_CRDTL_DT, "
    f"T.CMN_PRCT_FIRST_NM = S.CMN_PRCT_FIRST_NM, "
    f"T.CMN_PRCT_MIDINIT = S.CMN_PRCT_MIDINIT, "
    f"T.CMN_PRCT_LAST_NM = S.CMN_PRCT_LAST_NM, "
    f"T.CMN_PRCT_ID = S.CMN_PRCT_ID, "
    f"T.CMN_PRCT_SSN = S.CMN_PRCT_SSN, "
    f"T.CMN_PRCT_TTL = S.CMN_PRCT_TTL, "
    f"T.PROV_ADDR_ID = S.PROV_ADDR_ID, "
    f"T.PROV_NM = S.PROV_NM, "
    f"T.PROV_NPI = S.PROV_NPI, "
    f"T.PROV_TAX_ID = S.PROV_TAX_ID, "
    f"T.LAST_UPDT_DT = S.LAST_UPDT_DT, "
    f"T.PROV_TXNMY_CD = S.PROV_TXNMY_CD "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(PROV_ID,SRC_SYS_CD,CMN_PRCT_GNDR_CD,CMN_PRCT_GNDR_NM,FCLTY_TYP_CD,FCLTY_TYP_NM,"
    f"PROV_BILL_SVC_ID,PROV_BILL_SVC_NM,PROV_ENTY_CD,PROV_ENTY_NM,PROV_PRCTC_TYP_CD,"
    f"PROV_PRCTC_TYP_NM,PROV_REL_GRP_PROV_ID,PROV_REL_GRP_PROV_NM,PROV_REL_IPA_PROV_ID,"
    f"PROV_REL_IPA_PROV_NM,PROV_SPEC_CD,PROV_SPEC_NM,PROV_TYP_CD,PROV_TYP_NM,ACTV_IN,"
    f"CNTGS_CNTY_PROV_IN,ITS_HOME_GNRC_PROV_IN,LEAPFROG_IN,LOCAL_BCBSKC_PROV_IN,PAR_PROV_IN,"
    f"CMN_PRCT_BRTH_DT,CMN_PRCT_LAST_CRDTL_DT,CMN_PRCT_FIRST_NM,CMN_PRCT_MIDINIT,CMN_PRCT_LAST_NM,"
    f"CMN_PRCT_ID,CMN_PRCT_SSN,CMN_PRCT_TTL,PROV_ADDR_ID,PROV_NM,PROV_NPI,PROV_TAX_ID,"
    f"LAST_UPDT_DT,PROV_TXNMY_CD) VALUES("
    f"S.PROV_ID,S.SRC_SYS_CD,S.CMN_PRCT_GNDR_CD,S.CMN_PRCT_GNDR_NM,S.FCLTY_TYP_CD,"
    f"S.FCLTY_TYP_NM,S.PROV_BILL_SVC_ID,S.PROV_BILL_SVC_NM,S.PROV_ENTY_CD,S.PROV_ENTY_NM,"
    f"S.PROV_PRCTC_TYP_CD,S.PROV_PRCTC_TYP_NM,S.PROV_REL_GRP_PROV_ID,S.PROV_REL_GRP_PROV_NM,"
    f"S.PROV_REL_IPA_PROV_ID,S.PROV_REL_IPA_PROV_NM,S.PROV_SPEC_CD,S.PROV_SPEC_NM,"
    f"S.PROV_TYP_CD,S.PROV_TYP_NM,S.ACTV_IN,S.CNTGS_CNTY_PROV_IN,S.ITS_HOME_GNRC_PROV_IN,"
    f"S.LEAPFROG_IN,S.LOCAL_BCBSKC_PROV_IN,S.PAR_PROV_IN,S.CMN_PRCT_BRTH_DT,"
    f"S.CMN_PRCT_LAST_CRDTL_DT,S.CMN_PRCT_FIRST_NM,S.CMN_PRCT_MIDINIT,S.CMN_PRCT_LAST_NM,"
    f"S.CMN_PRCT_ID,S.CMN_PRCT_SSN,S.CMN_PRCT_TTL,S.PROV_ADDR_ID,S.PROV_NM,S.PROV_NPI,"
    f"S.PROV_TAX_ID,S.LAST_UPDT_DT,S.PROV_TXNMY_CD);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)

# Create empty reject DataFrame (AddToRejectRows) and write out
df_reject_odbc_prov_dir_dm_prov_out = spark.createDataFrame(
    [], 
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

df_reject_odbc_prov_dir_dm_prov_out = (
    df_reject_odbc_prov_dir_dm_prov_out
    .withColumn("ERRORCODE", rpad(col("ERRORCODE"), 255, " "))
    .withColumn("ERRORTEXT", rpad(col("ERRORTEXT"), 255, " "))
)

df_reject_odbc_prov_dir_dm_prov_out = df_reject_odbc_prov_dir_dm_prov_out.select("ERRORCODE", "ERRORTEXT")

write_files(
    df_reject_odbc_prov_dir_dm_prov_out,
    f"{adls_path}/load/PROV_DIR_DM_PROV_Rej.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)