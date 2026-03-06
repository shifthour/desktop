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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/12/2007         3264                              Originally Programmed                            devlEDW10          
# MAGIC 
# MAGIC Parikshith Chada              08/29/2007         3264                            Changed the Snapshot file Extract to       devlEDW10                     Steph Goddard           10/26/2007
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, when, length, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

schema_IdsUmSvc = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("UM_SVC_SEQ_NO", IntegerType(), nullable=False),
    StructField("PRI_DIAG_CD_SK", IntegerType(), nullable=False),
    StructField("PROC_CD_SK", IntegerType(), nullable=False),
    StructField("UM_SVC_TREAT_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("PD_AMT", DecimalType(10, 2), nullable=False),
    StructField("ALW_UNIT_CT", IntegerType(), nullable=False)
])

df_IdsUmSvc = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("sep", "|")
    .option("quote", "\"")
    .schema(schema_IdsUmSvc)
    .csv(f"{adls_path}/balancing/snapshot/IDS_UM_SVC.uniq")
)

df_Lookups_DiagCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DIAG_CD.DIAG_CD_SK as DIAG_CD_SK,DIAG_CD.DIAG_CD as DIAG_CD FROM {IDSOwner}.DIAG_CD DIAG_CD")
    .load()
)

df_Lookups_ProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT PROC_CD.PROC_CD_SK as PROC_CD_SK,PROC_CD.PROC_CD as PROC_CD FROM {IDSOwner}.PROC_CD PROC_CD")
    .load()
)

df_hf_b_um_ip_lkup_PriDiagCdLkup = dedup_sort(
    df_Lookups_DiagCd,
    partition_cols=["DIAG_CD_SK"],
    sort_cols=[]
)

df_hf_b_um_ip_lkup_ProcCdLkup = dedup_sort(
    df_Lookups_ProcCd,
    partition_cols=["PROC_CD_SK"],
    sort_cols=[]
)

schema_hf_cdma_codes = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), nullable=False),
    StructField("TRGT_CD", StringType(), nullable=True),
    StructField("TRGT_CD_NM", StringType(), nullable=True)
])

df_hf_cdma_codes = (
    spark.read
    .schema(schema_hf_cdma_codes)
    .parquet(f"{adls_path}/hf_cdma_codes.parquet")
)

df_hf_b_um_ip_lkup_SrcSysCdLkup = df_hf_cdma_codes
df_hf_b_um_ip_lkup_SvcTreatCatCdLkup = df_hf_cdma_codes

df_Transform = (
    df_IdsUmSvc.alias("Snapshot")
    .join(
        df_hf_b_um_ip_lkup_SrcSysCdLkup.alias("SrcSysCdLkup"),
        col("Snapshot.SRC_SYS_CD_SK") == col("SrcSysCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_SvcTreatCatCdLkup.alias("SvcTreatCatCdLkup"),
        col("Snapshot.UM_SVC_TREAT_CAT_CD_SK") == col("SvcTreatCatCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_ProcCdLkup.alias("ProcCdLkup"),
        col("Snapshot.PROC_CD_SK") == col("ProcCdLkup.PROC_CD_SK"),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_PriDiagCdLkup.alias("PriDiagCdLkup"),
        col("Snapshot.PRI_DIAG_CD_SK") == col("PriDiagCdLkup.DIAG_CD_SK"),
        "left"
    )
)

df_Transform = df_Transform.select(
    when(
        col("SrcSysCdLkup.TRGT_CD").isNull() | (length(trim(col("SrcSysCdLkup.TRGT_CD"))) == 0),
        lit("NA")
    ).otherwise(col("SrcSysCdLkup.TRGT_CD")).alias("SRC_SYS_CD"),
    col("Snapshot.UM_REF_ID").alias("UM_REF_ID"),
    col("Snapshot.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    col("Snapshot.PD_AMT").alias("UM_SVC_PD_AMT"),
    col("Snapshot.ALW_UNIT_CT").alias("UM_SVC_ALW_UNIT_CT"),
    when(
        col("SvcTreatCatCdLkup.TRGT_CD").isNull() | (length(trim(col("SvcTreatCatCdLkup.TRGT_CD"))) == 0),
        lit("NA")
    ).otherwise(col("SvcTreatCatCdLkup.TRGT_CD")).alias("UM_SVC_TREAT_CAT_CD"),
    when(
        col("PriDiagCdLkup.DIAG_CD").isNull() | (length(trim(col("PriDiagCdLkup.DIAG_CD"))) == 0),
        lit("NA")
    ).otherwise(col("PriDiagCdLkup.DIAG_CD")).alias("PRI_DIAG_CD"),
    when(
        col("ProcCdLkup.PROC_CD").isNull() | (length(trim(col("ProcCdLkup.PROC_CD"))) == 0),
        lit("NA")
    ).otherwise(col("ProcCdLkup.PROC_CD")).alias("PROC_CD")
)

df_Transform = df_Transform.withColumn("PROC_CD", rpad(col("PROC_CD"), 5, " "))

write_files(
    df_Transform,
    f"{adls_path}/load/B_UM_SVC_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)