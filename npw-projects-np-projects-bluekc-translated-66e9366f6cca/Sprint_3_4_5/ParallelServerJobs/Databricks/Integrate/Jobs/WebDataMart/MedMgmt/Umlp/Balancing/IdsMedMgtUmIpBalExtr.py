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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when, length, lit
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IdsOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
schema_ids_um_ip = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("UM_REF_ID", StringType(), False),
    StructField("PRI_DIAG_CD_SK", IntegerType(), False),
    StructField("UM_IP_CUR_TREAT_CAT_CD_SK", IntegerType(), False),
    StructField("UM_IP_STTUS_CD_SK", IntegerType(), False),
    StructField("ACTL_LOS_DAYS_QTY", IntegerType(), False)
])
df_IDS_UM_IP = spark.read.option("header", False).option("quote", "\"").schema(schema_ids_um_ip).csv(f"{adls_path}/balancing/snapshot/IDS_UM_IP.uniq")
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_lookups = f"SELECT DIAG_CD.DIAG_CD_SK as DIAG_CD_SK, DIAG_CD.DIAG_CD as DIAG_CD FROM {IdsOwner}.DIAG_CD DIAG_CD"
df_Lookups = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_lookups)
    .load()
)
write_files(df_Lookups, f"{adls_path}/hf_edw_ip_pri_diag_cd_lkup.parquet", ",", "overwrite", True, True, "\"", None)
df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_hf_edw_ip_pri_diag_cd_lkup = spark.read.parquet(f"{adls_path}/hf_edw_ip_pri_diag_cd_lkup.parquet")
df_joined = df_IDS_UM_IP.alias("Snapshot") \
    .join(df_hf_etrnl_cd_mppng.alias("SrcSysCdLkup"), col("Snapshot.SRC_SYS_CD_SK") == col("SrcSysCdLkup.CD_MPPNG_SK"), "left") \
    .join(df_hf_etrnl_cd_mppng.alias("IpCurTreatCatCdLkup"), col("Snapshot.UM_IP_CUR_TREAT_CAT_CD_SK") == col("IpCurTreatCatCdLkup.CD_MPPNG_SK"), "left") \
    .join(df_hf_etrnl_cd_mppng.alias("IpSttusCdLkup"), col("Snapshot.UM_IP_STTUS_CD_SK") == col("IpSttusCdLkup.CD_MPPNG_SK"), "left") \
    .join(df_hf_edw_ip_pri_diag_cd_lkup.alias("IpPriDiagCdLkup"), col("Snapshot.PRI_DIAG_CD_SK") == col("IpPriDiagCdLkup.DIAG_CD_SK"), "left")
SRC_SYS_CD = when(
    (col("SrcSysCdLkup.TRGT_CD").isNull()) | (length(trim(col("SrcSysCdLkup.TRGT_CD"))) == 0),
    lit("NA")
).otherwise(col("SrcSysCdLkup.TRGT_CD"))
UM_REF_ID = col("Snapshot.UM_REF_ID")
PRI_DIAG_CD = when(
    (col("IpPriDiagCdLkup.DIAG_CD").isNull()) | (length(trim(col("IpPriDiagCdLkup.DIAG_CD"))) == 0),
    lit("NA")
).otherwise(col("IpPriDiagCdLkup.DIAG_CD"))
UM_IP_ACTL_LOS_DAYS_QTY = col("Snapshot.ACTL_LOS_DAYS_QTY")
UM_IP_CUR_TREAT_CAT_CD = when(
    (col("IpCurTreatCatCdLkup.TRGT_CD").isNull()) | (length(trim(col("IpCurTreatCatCdLkup.TRGT_CD"))) == 0),
    lit("NA")
).otherwise(col("IpCurTreatCatCdLkup.TRGT_CD"))
UM_IP_STTUS_CD = when(
    (col("IpSttusCdLkup.TRGT_CD").isNull()) | (length(trim(col("IpSttusCdLkup.TRGT_CD"))) == 0),
    lit("NA")
).otherwise(col("IpSttusCdLkup.TRGT_CD"))
df_enriched = df_joined.select(
    SRC_SYS_CD.alias("SRC_SYS_CD"),
    UM_REF_ID.alias("UM_REF_ID"),
    PRI_DIAG_CD.alias("PRI_DIAG_CD"),
    UM_IP_ACTL_LOS_DAYS_QTY.alias("UM_IP_ACTL_LOS_DAYS_QTY"),
    UM_IP_CUR_TREAT_CAT_CD.alias("UM_IP_CUR_TREAT_CAT_CD"),
    UM_IP_STTUS_CD.alias("UM_IP_STTUS_CD")
)
df_enriched = df_enriched.withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " ")) \
    .withColumn("UM_REF_ID", rpad("UM_REF_ID", <...>, " ")) \
    .withColumn("PRI_DIAG_CD", rpad("PRI_DIAG_CD", <...>, " ")) \
    .withColumn("UM_IP_CUR_TREAT_CAT_CD", rpad("UM_IP_CUR_TREAT_CAT_CD", <...>, " ")) \
    .withColumn("UM_IP_STTUS_CD", rpad("UM_IP_STTUS_CD", <...>, " "))
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsMedMgtUmIpBalExtr_BMedMgtDmUmIp_temp", jdbc_url_clmmart, jdbc_props_clmmart)
execute_dml(
    "CREATE TABLE STAGING.IdsMedMgtUmIpBalExtr_BMedMgtDmUmIp_temp "
    "(SRC_SYS_CD VARCHAR(100), UM_REF_ID VARCHAR(100), PRI_DIAG_CD VARCHAR(100), "
    "UM_IP_ACTL_LOS_DAYS_QTY INT, UM_IP_CUR_TREAT_CAT_CD VARCHAR(100), UM_IP_STTUS_CD VARCHAR(100))",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)
df_enriched.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsMedMgtUmIpBalExtr_BMedMgtDmUmIp_temp") \
    .mode("append") \
    .save()
merge_sql = f"""
MERGE INTO {ClmMartOwner}.B_MED_MGT_DM_UM_IP AS T
USING STAGING.IdsMedMgtUmIpBalExtr_BMedMgtDmUmIp_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.UM_REF_ID = S.UM_REF_ID
WHEN MATCHED THEN
  UPDATE SET
    T.PRI_DIAG_CD = S.PRI_DIAG_CD,
    T.UM_IP_ACTL_LOS_DAYS_QTY = S.UM_IP_ACTL_LOS_DAYS_QTY,
    T.UM_IP_CUR_TREAT_CAT_CD = S.UM_IP_CUR_TREAT_CAT_CD,
    T.UM_IP_STTUS_CD = S.UM_IP_STTUS_CD
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, UM_REF_ID, PRI_DIAG_CD, UM_IP_ACTL_LOS_DAYS_QTY, UM_IP_CUR_TREAT_CAT_CD, UM_IP_STTUS_CD)
  VALUES (S.SRC_SYS_CD, S.UM_REF_ID, S.PRI_DIAG_CD, S.UM_IP_ACTL_LOS_DAYS_QTY, S.UM_IP_CUR_TREAT_CAT_CD, S.UM_IP_STTUS_CD);
"""
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)