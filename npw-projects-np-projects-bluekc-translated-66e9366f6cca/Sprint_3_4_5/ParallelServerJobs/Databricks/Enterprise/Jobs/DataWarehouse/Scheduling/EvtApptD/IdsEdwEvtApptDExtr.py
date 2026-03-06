# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwProductExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts all IDS records and loads in to EDW EVT_APPT_D
# MAGIC  table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 08/14/2009         4113                          Originally Programmed                                    devlEDWnew                Steph Goddard             08/19/2009
# MAGIC 
# MAGIC Archana Palivela             11/10/2013        5114                           Originally Programmed (In Parallel)                  EnterpriseWhseDevl

# MAGIC Job name: IdsEdwEvtApptDExtr 
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table EVT_APPT
# MAGIC Write EVT_APPT_D Data into a Sequential file for Load Ready Job.
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
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IdsRunCycle = get_widget_value('IdsRunCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# Prepare JDBC configurations for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
# Prepare JDBC configurations for EDW
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# --------------------------------------------------------------------------------
# Stage: db2_EVT_APPT_Extr (DB2ConnectorPX - Input from IDS)
extract_query_db2_EVT_APPT_Extr = f"""
SELECT 
  EVT_APPT.EVT_APPT_SK,
  EVT_APPT.GRP_ID,
  EVT_APPT.EVT_DT_SK,
  EVT_APPT.EVT_TYP_ID,
  EVT_APPT.SEQ_NO,
  EVT_APPT.EVT_APPT_STRT_TM_TX,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  EVT_APPT.LAST_UPDT_RUN_CYC_EXCTN_SK,
  EVT_APPT.EVT_APPT_RSN_SK,
  EVT_APPT.EVT_LOC_SK,
  EVT_APPT.EVT_STAFF_SK,
  EVT_APPT.EVT_TYP_SK,
  EVT_APPT.GRP_SK,
  EVT_APPT.MBR_SK,
  EVT_APPT.EVT_APPT_DELD_IN,
  EVT_APPT.EVT_APPT_END_TM_TX,
  EVT_APPT.LAST_UPDT_DTM,
  EVT_APPT.LAST_UPDT_USER_ID
FROM {IDSOwner}.EVT_APPT EVT_APPT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON EVT_APPT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE EVT_APPT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsRunCycle}
"""
df_db2_EVT_APPT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EVT_APPT_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_EVT_TYP_Extr (DB2ConnectorPX - Input from IDS)
extract_query_db2_EVT_TYP_Extr = f"""
SELECT 
  EVT_TYP.EVT_TYP_SK,
  EVT_TYP.EVT_TYP_DESC
FROM {IDSOwner}.EVT_TYP EVT_TYP
"""
df_db2_EVT_TYP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EVT_TYP_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_EVT_LOC_Extr (DB2ConnectorPX - Input from IDS)
extract_query_db2_EVT_LOC_Extr = f"""
SELECT 
  EVT_LOC.EVT_LOC_SK,
  EVT_LOC.EVT_LOC_ID,
  EVT_LOC.EVT_LOC_NM
FROM {IDSOwner}.EVT_LOC EVT_LOC
"""
df_db2_EVT_LOC_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EVT_LOC_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_EVT_STAFF_Extr (DB2ConnectorPX - Input from IDS)
extract_query_db2_EVT_STAFF_Extr = f"""
SELECT
  EVT_STAFF.EVT_STAFF_SK,
  EVT_STAFF.EVT_STAFF_ID,
  EVT_STAFF.EVT_STAFF_NM
FROM {IDSOwner}.EVT_STAFF EVT_STAFF
"""
df_db2_EVT_STAFF_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EVT_STAFF_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_EVT_APPT_RSN_Extr (DB2ConnectorPX - Input from IDS)
extract_query_db2_EVT_APPT_RSN_Extr = f"""
SELECT 
  EVT_APPT_RSN.EVT_APPT_RSN_SK,
  EVT_APPT_RSN.EVT_APPT_RSN_NM,
  EVT_APPT_RSN.EVT_APPT_RSN_DESC
FROM {IDSOwner}.EVT_APPT_RSN EVT_APPT_RSN
"""
df_db2_EVT_APPT_RSN_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EVT_APPT_RSN_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: EDW_db2_MBR_D_Extr (DB2ConnectorPX - Input from EDW)
extract_query_EDW_db2_MBR_D_Extr = f"""
SELECT 
  MBR_D.MBR_SK,
  MBR_D.MBR_UNIQ_KEY,
  MBR_D.MBR_FULL_NM
FROM {EDWOwner}.MBR_D MBR_D
"""
df_EDW_db2_MBR_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_EDW_db2_MBR_D_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: EDW_db2_CD_GRP_D_Extr (DB2ConnectorPX - Input from EDW)
extract_query_EDW_db2_CD_GRP_D_Extr = f"""
SELECT 
  GRP_D.GRP_SK,
  GRP_D.GRP_NM
FROM {EDWOwner}.GRP_D GRP_D
"""
df_EDW_db2_CD_GRP_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_EDW_db2_CD_GRP_D_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# Primary link: df_db2_EVT_APPT_Extr as Ink_IdsEdwEvtApptDExtr_InABC
# Lookup links (left joins):
#   - df_db2_EVT_TYP_Extr as Ref_EvtType_Out
#   - df_db2_EVT_LOC_Extr as Lnk_EvtLoc_Out
#   - df_EDW_db2_CD_GRP_D_Extr as Lnk_GrpD_Out
#   - df_EDW_db2_MBR_D_Extr as Lnk_MbrD_Out
#   - df_db2_EVT_APPT_RSN_Extr as Lnk_EvtAPptRsn_Out
#   - df_db2_EVT_STAFF_Extr as Lnk_EvtStaff_Out
df_lkp_Codes_joined = (
    df_db2_EVT_APPT_Extr.alias("Ink_IdsEdwEvtApptDExtr_InABC")
    .join(
        df_db2_EVT_TYP_Extr.alias("Ref_EvtType_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_TYP_SK")
        == F.col("Ref_EvtType_Out.EVT_TYP_SK"),
        "left",
    )
    .join(
        df_db2_EVT_LOC_Extr.alias("Lnk_EvtLoc_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_LOC_SK")
        == F.col("Lnk_EvtLoc_Out.EVT_LOC_SK"),
        "left",
    )
    .join(
        df_EDW_db2_CD_GRP_D_Extr.alias("Lnk_GrpD_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.GRP_SK")
        == F.col("Lnk_GrpD_Out.GRP_SK"),
        "left",
    )
    .join(
        df_EDW_db2_MBR_D_Extr.alias("Lnk_MbrD_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.MBR_SK")
        == F.col("Lnk_MbrD_Out.MBR_SK"),
        "left",
    )
    .join(
        df_db2_EVT_APPT_RSN_Extr.alias("Lnk_EvtAPptRsn_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_RSN_SK")
        == F.col("Lnk_EvtAPptRsn_Out.EVT_APPT_RSN_SK"),
        "left",
    )
    .join(
        df_db2_EVT_STAFF_Extr.alias("Lnk_EvtStaff_Out"),
        F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_STAFF_SK")
        == F.col("Lnk_EvtStaff_Out.EVT_STAFF_SK"),
        "left",
    )
)

# Select columns needed by the lookup output link
df_lkp_Codes = df_lkp_Codes_joined.select(
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_SK").alias("EVT_APPT_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.GRP_ID").alias("GRP_ID"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_DT_SK").alias("EVT_DT_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_TYP_ID").alias("EVT_TYP_ID"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.SEQ_NO").alias("SEQ_NO"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_Source"),
    F.col("Lnk_MbrD_Out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_GrpD_Out.GRP_NM").alias("GRP_NM"),
    F.col("Lnk_EvtLoc_Out.EVT_LOC_ID").alias("EVT_LOC_ID"),
    F.col("Lnk_EvtLoc_Out.EVT_LOC_NM").alias("EVT_LOC_NM"),
    F.col("Lnk_EvtStaff_Out.EVT_STAFF_ID").alias("EVT_STAFF_ID"),
    F.col("Lnk_EvtStaff_Out.EVT_STAFF_NM").alias("EVT_STAFF_NM"),
    F.col("Ref_EvtType_Out.EVT_TYP_DESC").alias("EVT_TYP_DESC"),
    F.col("Lnk_EvtAPptRsn_Out.EVT_APPT_RSN_NM").alias("EVT_APPT_RSN_NM"),
    F.col("Lnk_EvtAPptRsn_Out.EVT_APPT_RSN_DESC").alias("EVT_APPT_RSN_DESC"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_LOC_SK").alias("EVT_LOC_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_STAFF_SK").alias("EVT_STAFF_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_TYP_SK").alias("EVT_TYP_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.GRP_SK").alias("GRP_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.MBR_SK").alias("MBR_SK"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_DELD_IN").alias("EVT_APPT_DELD_IN"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Ink_IdsEdwEvtApptDExtr_InABC.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
)

# --------------------------------------------------------------------------------
# Stage: xmf_businessLogic (CTransformerStage)
#  - Lnk_Main_Out constraint: EVT_APPT_SK != 0 AND EVT_APPT_SK != 1
df_xmf_businesslogic_main = (
    df_lkp_Codes.filter((F.col("EVT_APPT_SK") != 0) & (F.col("EVT_APPT_SK") != 1))
    .select(
        F.col("EVT_APPT_SK").alias("EVT_APPT_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("EVT_DT_SK").alias("EVT_DT_SK"),
        F.col("EVT_TYP_ID").alias("EVT_TYP_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("EVT_APPT_STRT_TM_TX").alias("EVT_APPT_STRT_TM_TX"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EVT_APPT_RSN_SK").alias("EVT_APPT_RSN_SK"),
        F.col("EVT_LOC_SK").alias("EVT_LOC_SK"),
        F.col("EVT_STAFF_SK").alias("EVT_STAFF_SK"),
        F.col("EVT_TYP_SK").alias("EVT_TYP_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("EVT_APPT_DELD_IN").alias("EVT_APPT_DELD_IN"),
        F.col("LAST_UPDT_DTM").alias("EVT_APPT_LAST_UPDT_DTM"),
        F.when(F.col("MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
        F.col("EVT_APPT_END_TM_TX").alias("EVT_APPT_END_TM_TX"),
        F.col("LAST_UPDT_USER_ID").alias("EVT_APPT_LAST_UPDT_USER_ID"),
        F.when(F.col("EVT_APPT_RSN_DESC").isNull(), F.lit("UNK")).otherwise(F.col("EVT_APPT_RSN_DESC")).alias("EVT_APPT_RSN_DESC"),
        F.when(F.col("EVT_APPT_RSN_NM").isNull(), F.lit("UNK")).otherwise(F.col("EVT_APPT_RSN_NM")).alias("EVT_APPT_RSN_NM"),
        F.when(F.col("EVT_LOC_ID").isNull(), F.lit("UNK")).otherwise(F.col("EVT_LOC_ID")).alias("EVT_LOC_ID"),
        F.when(F.col("EVT_LOC_NM").isNull(), F.lit("UNK")).otherwise(F.col("EVT_LOC_NM")).alias("EVT_LOC_NM"),
        F.when(F.col("EVT_STAFF_ID").isNull(), F.lit("UNK")).otherwise(F.col("EVT_STAFF_ID")).alias("EVT_STAFF_ID"),
        F.when(F.col("EVT_STAFF_NM").isNull(), F.lit("UNK")).otherwise(F.col("EVT_STAFF_NM")).alias("EVT_STAFF_NM"),
        F.when(F.col("EVT_TYP_DESC").isNull(), F.lit("UNK")).otherwise(F.col("EVT_TYP_DESC")).alias("EVT_TYP_DESC"),
        F.when(F.col("GRP_NM").isNull(), F.lit("UNK")).otherwise(F.col("GRP_NM")).alias("GRP_NM"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK_Source").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

#  - Lnk_UNK_Out constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
#    Produce exactly one row with specified column values
data_unk = [{
    "EVT_APPT_SK": 0,
    "GRP_ID": "UNK",
    "EVT_DT_SK": "1753-01-01",
    "EVT_TYP_ID": "UNK",
    "SEQ_NO": 0,
    "EVT_APPT_STRT_TM_TX": "",
    "SRC_SYS_CD": "UNK",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "EVT_APPT_RSN_SK": 0,
    "EVT_LOC_SK": 0,
    "EVT_STAFF_SK": 0,
    "EVT_TYP_SK": 0,
    "GRP_SK": 0,
    "MBR_SK": 0,
    "EVT_APPT_DELD_IN": "N",
    "EVT_APPT_LAST_UPDT_DTM": "1753-01-01 00:00:00.000",
    "MBR_UNIQ_KEY": 0,
    "EVT_APPT_END_TM_TX": "",
    "EVT_APPT_LAST_UPDT_USER_ID": "UNK",
    "EVT_APPT_RSN_DESC": "",
    "EVT_APPT_RSN_NM": "UNK",
    "EVT_LOC_ID": "UNK",
    "EVT_LOC_NM": "UNK",
    "EVT_STAFF_ID": "UNK",
    "EVT_STAFF_NM": "UNK",
    "EVT_TYP_DESC": "",
    "GRP_NM": "UNK",
    "CRT_RUN_CYC_EXCTN_SK": 100,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 100,
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK": 100
}]
df_xmf_businesslogic_unk = spark.createDataFrame(data_unk)

#  - Lnk_NA_Out constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
#    Produce exactly one row with specified column values
data_na = [{
    "EVT_APPT_SK": 1,
    "GRP_ID": "NA",
    "EVT_DT_SK": "1753-01-01",
    "EVT_TYP_ID": "NA",
    "SEQ_NO": 0,
    "EVT_APPT_STRT_TM_TX": "",
    "SRC_SYS_CD": "NA",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "EVT_APPT_RSN_SK": 1,
    "EVT_LOC_SK": 1,
    "EVT_STAFF_SK": 1,
    "EVT_TYP_SK": 1,
    "GRP_SK": 1,
    "MBR_SK": 1,
    "EVT_APPT_DELD_IN": "N",
    "EVT_APPT_LAST_UPDT_DTM": "1753-01-01 00:00:00.000",
    "MBR_UNIQ_KEY": 1,
    "EVT_APPT_END_TM_TX": "",
    "EVT_APPT_LAST_UPDT_USER_ID": "NA",
    "EVT_APPT_RSN_DESC": "",
    "EVT_APPT_RSN_NM": "NA",
    "EVT_LOC_ID": "NA",
    "EVT_LOC_NM": "NA",
    "EVT_STAFF_ID": "NA",
    "EVT_STAFF_NM": "NA",
    "EVT_TYP_DESC": "",
    "GRP_NM": "NA",
    "CRT_RUN_CYC_EXCTN_SK": 100,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 100,
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK": 100
}]
df_xmf_businesslogic_na = spark.createDataFrame(data_na)

# --------------------------------------------------------------------------------
# Stage: Fnl_Main (PxFunnel) - union all three flows
df_Fnl_Main = df_xmf_businesslogic_main.unionByName(df_xmf_businesslogic_unk).unionByName(df_xmf_businesslogic_na)

# --------------------------------------------------------------------------------
# Stage: Seq_EVT_APPT_D_Load (PxSequentialFile)
# Final select with the same column order, plus rpad for char-type columns
df_final = (
    df_Fnl_Main
    .select(
        "EVT_APPT_SK",
        "GRP_ID",
        "EVT_DT_SK",
        "EVT_TYP_ID",
        "SEQ_NO",
        "EVT_APPT_STRT_TM_TX",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "EVT_APPT_RSN_SK",
        "EVT_LOC_SK",
        "EVT_STAFF_SK",
        "EVT_TYP_SK",
        "GRP_SK",
        "MBR_SK",
        "EVT_APPT_DELD_IN",
        "EVT_APPT_LAST_UPDT_DTM",
        "MBR_UNIQ_KEY",
        "EVT_APPT_END_TM_TX",
        "EVT_APPT_LAST_UPDT_USER_ID",
        "EVT_APPT_RSN_DESC",
        "EVT_APPT_RSN_NM",
        "EVT_LOC_ID",
        "EVT_LOC_NM",
        "EVT_STAFF_ID",
        "EVT_STAFF_NM",
        "EVT_TYP_DESC",
        "GRP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
    .withColumn("EVT_DT_SK", rpad(F.col("EVT_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("EVT_APPT_DELD_IN", rpad(F.col("EVT_APPT_DELD_IN"), 1, " "))
)

# Write to EVT_APPT_D.dat in "load" folder with required properties
write_files(
    df_final,
    f"{adls_path}/load/EVT_APPT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)