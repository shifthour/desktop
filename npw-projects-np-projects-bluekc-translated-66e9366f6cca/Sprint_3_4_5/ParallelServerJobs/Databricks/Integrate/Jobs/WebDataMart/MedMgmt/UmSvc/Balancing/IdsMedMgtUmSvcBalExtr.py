# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/11/07 15:00:54 Batch  14529_54061 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_1 10/11/07 14:39:56 Batch  14529_52800 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/10/07 07:56:07 Batch  14528_28571 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/10/07 07:39:54 Batch  14528_27598 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:    IdsWebdmMedMgtBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This pulls data from the Concatenated Source Sequential file coming from IDS and loads into the B table in Web DataMart
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------               ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              07/13/2007         3264                              Originally Programmed                                devlIDS30   
# MAGIC 
# MAGIC Parikshith Chada              08/30/2007         3264                            Changed the Snapshot file Extract to 
# MAGIC                                                                                                          Snapshot table extract


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import col, when, length, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

schema_IDS_UM_SVC = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("UM_REF_ID", StringType(), False),
    StructField("UM_SVC_SEQ_NO", IntegerType(), False),
    StructField("PRI_DIAG_CD_SK", IntegerType(), False),
    StructField("PROC_CD_SK", IntegerType(), False),
    StructField("UM_SVC_TREAT_CAT_CD_SK", IntegerType(), False),
    StructField("PD_AMT", DecimalType(38,10), False),
    StructField("ALW_UNIT_CT", IntegerType(), False)
])

df_IDS_UM_SVC = (
    spark.read.format("csv")
    .option("header","false")
    .option("quote","\"")
    .schema(schema_IDS_UM_SVC)
    .load(f"{adls_path}/balancing/snapshot/IDS_UM_SVC.uniq")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_DiagCd = f"SELECT DIAG_CD.DIAG_CD_SK AS DIAG_CD_SK, DIAG_CD.DIAG_CD AS DIAG_CD FROM {IDSOwner}.DIAG_CD DIAG_CD"
df_Lookups_DiagCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DiagCd)
    .load()
)

extract_query_ProcCd = f"SELECT PROC_CD.PROC_CD_SK AS PROC_CD_SK, PROC_CD.PROC_CD AS PROC_CD FROM {IDSOwner}.PROC_CD PROC_CD"
df_Lookups_ProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ProcCd)
    .load()
)

write_files(
    df_Lookups_DiagCd.select("DIAG_CD_SK","DIAG_CD"),
    "hf_edw_pri_diag_cd_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_Lookups_ProcCd.select("PROC_CD_SK","PROC_CD"),
    "hf_b_um_svc_proc_cd_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

schema_SrcSysCdLkup = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), False),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True)
])

df_hf_b_um_ip_lkup_SrcSysCdLkup = (
    spark.read.format("parquet")
    .schema(schema_SrcSysCdLkup)
    .load("hf_etrnl_cd_mppng.parquet")
)

schema_SvcTreatCatCdLkup = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), False),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True)
])

df_hf_b_um_ip_lkup_SvcTreatCatCdLkup = (
    spark.read.format("parquet")
    .schema(schema_SvcTreatCatCdLkup)
    .load("hf_etrnl_cd_mppng.parquet")
)

schema_ProcCdLkup = StructType([
    StructField("PROC_CD_SK", IntegerType(), False),
    StructField("PROC_CD", StringType(), False)
])

df_hf_b_um_ip_lkup_ProcCdLkup = (
    spark.read.format("parquet")
    .schema(schema_ProcCdLkup)
    .load("hf_b_um_svc_proc_cd_lkup.parquet")
)

schema_PriDiagCdLkup = StructType([
    StructField("DIAG_CD_SK", IntegerType(), False),
    StructField("DIAG_CD", StringType(), False)
])

df_hf_b_um_ip_lkup_PriDiagCdLkup = (
    spark.read.format("parquet")
    .schema(schema_PriDiagCdLkup)
    .load("hf_edw_pri_diag_cd_lkup.parquet")
)

df_transform = (
    df_IDS_UM_SVC.alias("Snapshot")
    .join(
        df_hf_b_um_ip_lkup_SrcSysCdLkup.alias("SrcSysCdLkup"),
        (col("Snapshot.SRC_SYS_CD_SK") == col("SrcSysCdLkup.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_SvcTreatCatCdLkup.alias("SvcTreatCatCdLkup"),
        (col("Snapshot.UM_SVC_TREAT_CAT_CD_SK") == col("SvcTreatCatCdLkup.CD_MPPNG_SK")),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_ProcCdLkup.alias("ProcCdLkup"),
        (col("Snapshot.PROC_CD_SK") == col("ProcCdLkup.PROC_CD_SK")),
        "left"
    )
    .join(
        df_hf_b_um_ip_lkup_PriDiagCdLkup.alias("PriDiagCdLkup"),
        (col("Snapshot.PRI_DIAG_CD_SK") == col("PriDiagCdLkup.DIAG_CD_SK")),
        "left"
    )
)

df_output = df_transform.select(
    when((col("SrcSysCdLkup.TRGT_CD").isNull()) | (length(trim(col("SrcSysCdLkup.TRGT_CD"))) == 0), lit("NA"))
    .otherwise(col("SrcSysCdLkup.TRGT_CD"))
    .alias("SRC_SYS_CD"),
    col("Snapshot.UM_REF_ID").alias("UM_REF_ID"),
    col("Snapshot.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    col("Snapshot.PD_AMT").alias("UM_SVC_PD_AMT"),
    col("Snapshot.ALW_UNIT_CT").alias("UM_SVC_ALW_UNIT_CT"),
    when((col("SvcTreatCatCdLkup.TRGT_CD").isNull()) | (length(trim(col("SvcTreatCatCdLkup.TRGT_CD"))) == 0), lit("NA"))
    .otherwise(col("SvcTreatCatCdLkup.TRGT_CD"))
    .alias("UM_SVC_TREAT_CAT_CD"),
    when((col("PriDiagCdLkup.DIAG_CD").isNull()) | (length(trim(col("PriDiagCdLkup.DIAG_CD"))) == 0), lit("NA"))
    .otherwise(col("PriDiagCdLkup.DIAG_CD"))
    .alias("PRI_DIAG_CD"),
    when((col("ProcCdLkup.PROC_CD").isNull()) | (length(trim(col("ProcCdLkup.PROC_CD"))) == 0), lit("NA"))
    .otherwise(col("ProcCdLkup.PROC_CD"))
    .alias("PROC_CD")
)

df_final = (
    df_output
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), 50, " "))
    .withColumn("UM_REF_ID", rpad(col("UM_REF_ID"), 50, " "))
    .withColumn("UM_SVC_TREAT_CAT_CD", rpad(col("UM_SVC_TREAT_CAT_CD"), 50, " "))
    .withColumn("PRI_DIAG_CD", rpad(col("PRI_DIAG_CD"), 50, " "))
    .withColumn("PROC_CD", rpad(col("PROC_CD"), 5, " "))
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
spark.sql(f"DROP TABLE IF EXISTS STAGING.IdsMedMgtUmSvcBalExtr_BMedMgtDmUmSvc_temp")
df_final.write.jdbc(
    url=jdbc_url_clmmart,
    table="STAGING.IdsMedMgtUmSvcBalExtr_BMedMgtDmUmSvc_temp",
    mode="append",
    properties=jdbc_props_clmmart
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.B_MED_MGT_DM_UM_SVC AS T
USING STAGING.IdsMedMgtUmSvcBalExtr_BMedMgtDmUmSvc_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.UM_REF_ID = S.UM_REF_ID
    AND T.UM_SVC_SEQ_NO = S.UM_SVC_SEQ_NO
WHEN MATCHED THEN
    UPDATE SET
        T.UM_SVC_PD_AMT = S.UM_SVC_PD_AMT,
        T.UM_SVC_ALW_UNIT_CT = S.UM_SVC_ALW_UNIT_CT,
        T.UM_SVC_TREAT_CAT_CD = S.UM_SVC_TREAT_CAT_CD,
        T.PRI_DIAG_CD = S.PRI_DIAG_CD,
        T.PROC_CD = S.PROC_CD
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        UM_REF_ID,
        UM_SVC_SEQ_NO,
        UM_SVC_PD_AMT,
        UM_SVC_ALW_UNIT_CT,
        UM_SVC_TREAT_CAT_CD,
        PRI_DIAG_CD,
        PROC_CD
    )
    VALUES (
        S.SRC_SYS_CD,
        S.UM_REF_ID,
        S.UM_SVC_SEQ_NO,
        S.UM_SVC_PD_AMT,
        S.UM_SVC_ALW_UNIT_CT,
        S.UM_SVC_TREAT_CAT_CD,
        S.PRI_DIAG_CD,
        S.PROC_CD
    )
;
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)