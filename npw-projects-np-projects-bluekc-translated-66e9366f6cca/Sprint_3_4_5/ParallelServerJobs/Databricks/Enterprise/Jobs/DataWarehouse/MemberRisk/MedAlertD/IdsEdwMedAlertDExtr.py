# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClnclCncptMthExtr1Seq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restart, no other steps necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                Project/                                                                                                 Code                  Date
# MAGIC Developer        Date               Altiris #           Change Description              Environment                     Reviewer            Reviewed
# MAGIC ----------------------  --------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    03/25/2008    3036              Original program                                                              Steph Goddard   06/02/2008
# MAGIC 
# MAGIC Pooja Sunkara  10/31/2013   5114              Rewrite in Parallel                 EnterpriseWrhsDevl          Jag Yelavarthi     2013-12-12

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwMedAlertDExtr
# MAGIC Monthly EDW MED_ALERT_D extract from IDS
# MAGIC Write MED_ALERT_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table
# MAGIC MED_ALERT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_1 = (
    f"SELECT "
    f"MED_ALERT.MED_ALERT_SK,"
    f"MED_ALERT.SRC_SYS_CD_SK,"
    f"MED_ALERT.MED_ALERT_ID,"
    f"MED_ALERT.BCBSKC_CLNCL_PGM_TYP_CD_SK,"
    f"MED_ALERT.MED_ALERT_TYP_CD_SK,"
    f"MED_ALERT.SH_DESC "
    f"FROM {IDSOwner}.MED_ALERT MED_ALERT "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)

df_db2_MED_ALERT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = (
    f"SELECT "
    f"CD_MPPNG_SK,"
    f"TRGT_CD,"
    f"TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG"
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Cpy_CdMppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_Cpy_CdMppng.alias("ref_CdMppng_src")
df_ref_MedAlertTypCd = df_Cpy_CdMppng.alias("ref_CdMppng_medalert")
df_ref_PgmTypCd = df_Cpy_CdMppng.alias("ref_CdMppng_pgm")

df_Lkp_Codes = (
    df_db2_MED_ALERT_in.alias("Ink_IdsEdwMedAlertDExtr_inABC")
    .join(
        df_ref_SrcSysCd.alias("ref_SrcSysCd"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_MedAlertTypCd.alias("ref_MedAlertTypCd"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.MED_ALERT_TYP_CD_SK") == F.col("ref_MedAlertTypCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_ref_PgmTypCd.alias("ref_PgmTypCd"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.BCBSKC_CLNCL_PGM_TYP_CD_SK") == F.col("ref_PgmTypCd.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.MED_ALERT_SK").alias("MED_ALERT_SK"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.MED_ALERT_ID").alias("MED_ALERT_ID"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.BCBSKC_CLNCL_PGM_TYP_CD_SK").alias("BCBSKC_CLNCL_PGM_TYP_CD_SK"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.MED_ALERT_TYP_CD_SK").alias("MED_ALERT_TYP_CD_SK"),
        F.col("Ink_IdsEdwMedAlertDExtr_inABC.SH_DESC").alias("SH_DESC"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_PgmTypCd.TRGT_CD").alias("BCBSKC_CLNCL_PGM_TYP_CD"),
        F.col("ref_PgmTypCd.TRGT_CD_NM").alias("BCBSKC_CLNCL_PGM_TYP_NM"),
        F.col("ref_MedAlertTypCd.TRGT_CD").alias("MED_ALERT_TYP_CD"),
        F.col("ref_MedAlertTypCd.TRGT_CD_NM").alias("MED_ALERT_TYP_NM"),
    )
)

df_outMain = (
    df_Lkp_Codes.where((F.col("MED_ALERT_SK") != 0) & (F.col("MED_ALERT_SK") != 1))
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")),
    )
    .withColumn("MED_ALERT_ID", F.col("MED_ALERT_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn(
        "BCBSKC_CLNCL_PGM_TYP_CD",
        F.when(F.col("BCBSKC_CLNCL_PGM_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_CD")),
    )
    .withColumn(
        "BCBSKC_CLNCL_PGM_TYP_NM",
        F.when(F.col("BCBSKC_CLNCL_PGM_TYP_NM").isNull(), F.lit("UNK")).otherwise(F.col("BCBSKC_CLNCL_PGM_TYP_NM")),
    )
    .withColumn("MED_ALERT_SH_DESC", F.col("SH_DESC"))
    .withColumn(
        "MED_ALERT_TYP_CD",
        F.when(F.col("MED_ALERT_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("MED_ALERT_TYP_CD")),
    )
    .withColumn(
        "MED_ALERT_TYP_NM",
        F.when(F.col("MED_ALERT_TYP_NM").isNull(), F.lit("UNK")).otherwise(F.col("MED_ALERT_TYP_NM")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("BCBSKC_CLNCL_PGM_TYP_CD_SK", F.col("BCBSKC_CLNCL_PGM_TYP_CD_SK"))
    .withColumn("MED_ALERT_TYP_CD_SK", F.col("MED_ALERT_TYP_CD_SK"))
)

df_naLink = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "1753-01-01",
            "1753-01-01",
            "NA",
            "NA",
            None,
            "NA",
            "NA",
            100,
            100,
            1,
            1,
        )
    ],
    [
        "MED_ALERT_SK",
        "SRC_SYS_CD",
        "MED_ALERT_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BCBSKC_CLNCL_PGM_TYP_CD",
        "BCBSKC_CLNCL_PGM_TYP_NM",
        "MED_ALERT_SH_DESC",
        "MED_ALERT_TYP_CD",
        "MED_ALERT_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BCBSKC_CLNCL_PGM_TYP_CD_SK",
        "MED_ALERT_TYP_CD_SK",
    ],
)

df_unkLink = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            "1753-01-01",
            "UNK",
            "UNK",
            None,
            "UNK",
            "UNK",
            100,
            100,
            0,
            0,
        )
    ],
    [
        "MED_ALERT_SK",
        "SRC_SYS_CD",
        "MED_ALERT_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "BCBSKC_CLNCL_PGM_TYP_CD",
        "BCBSKC_CLNCL_PGM_TYP_NM",
        "MED_ALERT_SH_DESC",
        "MED_ALERT_TYP_CD",
        "MED_ALERT_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BCBSKC_CLNCL_PGM_TYP_CD_SK",
        "MED_ALERT_TYP_CD_SK",
    ],
)

df_FnlData = (
    df_outMain.unionByName(df_naLink)
    .unionByName(df_unkLink)
)

df_final = df_FnlData.select(
    "MED_ALERT_SK",
    "SRC_SYS_CD",
    "MED_ALERT_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD",
    "BCBSKC_CLNCL_PGM_TYP_NM",
    "MED_ALERT_SH_DESC",
    "MED_ALERT_TYP_CD",
    "MED_ALERT_TYP_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BCBSKC_CLNCL_PGM_TYP_CD_SK",
    "MED_ALERT_TYP_CD_SK",
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/MED_ALERT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)