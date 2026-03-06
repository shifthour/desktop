# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 02/01/08 12:08:08 Batch  14642_43698 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_1 11/02/07 14:51:45 Batch  14551_53511 PROMOTE bckcetl edw10 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:44:15 Batch  14551_53058 INIT bckcett testEDW10 dsadm bls for on
# MAGIC ^1_1 11/01/07 12:56:04 Batch  14550_46572 PROMOTE bckcett testEDW10 u03651 steffy
# MAGIC ^1_1 11/01/07 12:51:30 Batch  14550_46292 INIT bckcett devlEDW10 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsEdwMedMgtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in EDW
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/12/2007         3264                              Originally Programmed                                 devlEDW10     
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to            devlEDW10                 Steph Goddad             10/26/2007
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

schema_IdsUmIp = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("PRI_DIAG_CD_SK", IntegerType(), nullable=False),
    StructField("UM_IP_CUR_TREAT_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("UM_IP_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("ACTL_LOS_DAYS_QTY", IntegerType(), nullable=False)
])

df_IdsUmIp = spark.read.csv(
    f"{adls_path}/balancing/snapshot/IDS_UM_IP.uniq",
    header=False,
    quote='"',
    sep=<...>,  # Unknown delimiter
    schema=schema_IdsUmIp
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT DIAG_CD.DIAG_CD_SK as DIAG_CD_SK,DIAG_CD.DIAG_CD as DIAG_CD FROM {IDSOwner}.DIAG_CD DIAG_CD"
df_Lookups = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query)
        .load()
)

write_files(
    df_Lookups,
    f"{adls_path}/hf_edw_ip_pri_diag_cd_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")
df_hf_edw_ip_pri_diag_cd_lkup = spark.read.parquet(f"{adls_path}/hf_edw_ip_pri_diag_cd_lkup.parquet")

df_transform = (
    df_IdsUmIp.alias("Snapshot")
    .join(
        df_hf_cdma_codes.alias("SrcSysCdLkup"),
        F.col("Snapshot.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("IpCurTreatCatCdLkup"),
        F.col("Snapshot.UM_IP_CUR_TREAT_CAT_CD_SK") == F.col("IpCurTreatCatCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_cdma_codes.alias("IpSttusCdLkup"),
        F.col("Snapshot.UM_IP_STTUS_CD_SK") == F.col("IpSttusCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_edw_ip_pri_diag_cd_lkup.alias("IpPriDiagCdLkup"),
        F.col("Snapshot.PRI_DIAG_CD_SK") == F.col("IpPriDiagCdLkup.DIAG_CD_SK"),
        "left"
    )
)

df_enriched = df_transform.select(
    F.when(
        (F.col("SrcSysCdLkup.TRGT_CD").isNull()) |
        (F.length(trim(F.col("SrcSysCdLkup.TRGT_CD"))) == 0),
        "NA"
    ).otherwise(F.col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),

    F.col("Snapshot.UM_REF_ID").alias("UM_REF_ID"),

    F.when(
        (F.col("IpPriDiagCdLkup.DIAG_CD").isNull()) |
        (F.length(trim(F.col("IpPriDiagCdLkup.DIAG_CD"))) == 0),
        "NA"
    ).otherwise(F.col("IpPriDiagCdLkup.DIAG_CD")).alias("PRI_DIAG_CD"),

    F.col("Snapshot.ACTL_LOS_DAYS_QTY").alias("UM_IP_ACTL_LOS_DAYS_QTY"),

    F.when(
        (F.col("IpCurTreatCatCdLkup.TRGT_CD").isNull()) |
        (F.length(trim(F.col("IpCurTreatCatCdLkup.TRGT_CD"))) == 0),
        "NA"
    ).otherwise(F.col("IpCurTreatCatCdLkup.TRGT_CD")).alias("UM_IP_CUR_TREAT_CAT_CD"),

    F.when(
        (F.col("IpSttusCdLkup.TRGT_CD").isNull()) |
        (F.length(trim(F.col("IpSttusCdLkup.TRGT_CD"))) == 0),
        "NA"
    ).otherwise(F.col("IpSttusCdLkup.TRGT_CD")).alias("UM_IP_STTUS_CD")
)

df_enriched = df_enriched \
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")) \
    .withColumn("UM_REF_ID", F.rpad(F.col("UM_REF_ID"), <...>, " ")) \
    .withColumn("PRI_DIAG_CD", F.rpad(F.col("PRI_DIAG_CD"), <...>, " ")) \
    .withColumn("UM_IP_CUR_TREAT_CAT_CD", F.rpad(F.col("UM_IP_CUR_TREAT_CAT_CD"), <...>, " ")) \
    .withColumn("UM_IP_STTUS_CD", F.rpad(F.col("UM_IP_STTUS_CD"), <...>, " "))

write_files(
    df_enriched,
    f"{adls_path}/load/B_UM_IP_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)