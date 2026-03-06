# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwProductExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts all IDS records and loads in to EDW BILL_COMSN_D table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 08/14/2009         4113                          Originally Programmed                                    devlEDWnew                Steph Goddard             08/19/2009
# MAGIC 
# MAGIC Archana Palivela             08/04/2013        5114                           Originally Programmed (In Parallel)                  EnterpriseWhseDevl      Jag Yelavarthi              2013-12-22

# MAGIC Job name: IdsEdwBillComsnDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table BILL_COMSN
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC BNF_VNDR_TYP_CD_SK
# MAGIC Write BILL_COMSN_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_BillComsn_Extr = f"""
SELECT BILL_COMSN.BILL_COMSN_SK,
       BILL_COMSN.SRC_SYS_CD_SK,
       BILL_COMSN.BILL_ENTY_UNIQ_KEY,
       BILL_COMSN.CLS_PLN_ID,
       BILL_COMSN.FEE_DSCNT_ID,
       BILL_COMSN.SEQ_NO,
       BILL_COMSN.BILL_ENTY_SK,
       BILL_COMSN.CLS_PLN_SK,
       BILL_COMSN.COMSN_ARGMT_SK,
       BILL_COMSN.FEE_DSCNT_SK,
       BILL_COMSN.EFF_DT_SK,
       BILL_COMSN.TERM_DT_SK,
       BILL_COMSN.PRM_PCT,
       BILL_ENTY.GRP_SK,
       GRP.GRP_ID
FROM {IDSOwner}.BILL_COMSN BILL_COMSN,
     {IDSOwner}.BILL_ENTY BILL_ENTY,
     {IDSOwner}.GRP GRP
WHERE BILL_COMSN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
  AND BILL_COMSN.BILL_ENTY_SK = BILL_ENTY.BILL_ENTY_SK
  AND BILL_ENTY.GRP_SK = GRP.GRP_SK
"""
df_db2_BillComsn_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_BillComsn_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

extract_query_DB2_COMSN_ARGMT = f"""
SELECT 
  COMSN_ARGMT.COMSN_ARGMT_SK,
  COMSN_ARGMT.COMSN_ARGMT_ID
FROM {IDSOwner}.COMSN_ARGMT COMSN_ARGMT
"""
df_DB2_COMSN_ARGMT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_COMSN_ARGMT)
    .load()
)

df_lkp_Codes = (
    df_db2_BillComsn_Extr.alias("Ink_IdsEdwBillComsnDExtr_InABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_BnfVndrTyp"),
        F.col("Ink_IdsEdwBillComsnDExtr_InABC.SRC_SYS_CD_SK") == F.col("Ref_BnfVndrTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_DB2_COMSN_ARGMT.alias("RefComsnTyp"),
        F.col("Ink_IdsEdwBillComsnDExtr_InABC.COMSN_ARGMT_SK") == F.col("RefComsnTyp.COMSN_ARGMT_SK"),
        "left"
    )
)

df_lkp_Codes_out = df_lkp_Codes.select(
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.BILL_COMSN_SK").alias("BILL_COMSN_SK"),
    F.col("Ref_BnfVndrTyp.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.SEQ_NO").alias("BILL_COMSN_SEQ_NO"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.GRP_SK").alias("GRP_SK"),
    F.col("RefComsnTyp.COMSN_ARGMT_ID").alias("COMSN_ARGMT_ID"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.EFF_DT_SK").alias("BILL_COMSN_EFF_DT_SK"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.TERM_DT_SK").alias("BILL_COMSN_TERM_DT_SK"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.GRP_ID").alias("GRP_ID"),
    F.col("Ink_IdsEdwBillComsnDExtr_InABC.PRM_PCT").alias("PRM_PCT")
)

df_xmf_businessLogic_in = df_lkp_Codes_out

df_main_out = (
    df_xmf_businessLogic_in
    .filter((F.col("BILL_COMSN_SK") != 0) & (F.col("BILL_COMSN_SK") != 1))
    .select(
        F.col("BILL_COMSN_SK").alias("BILL_COMSN_SK"),
        F.when(trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        F.col("BILL_COMSN_SEQ_NO").alias("BILL_COMSN_SEQ_NO"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.when(
            F.col("COMSN_ARGMT_ID").isNull() | (F.length(trim(F.col("COMSN_ARGMT_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("COMSN_ARGMT_ID")).alias("COMSN_ARGMT_ID"),
        F.col("BILL_COMSN_EFF_DT_SK").alias("BILL_COMSN_EFF_DT_SK"),
        (F.col("PRM_PCT") * 0.01).alias("BILL_COMSN_PRM_DCML_PCT"),
        F.col("PRM_PCT").alias("BILL_COMSN_PRM_PCT"),
        F.col("BILL_COMSN_TERM_DT_SK").alias("BILL_COMSN_TERM_DT_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_unk_temp = df_xmf_businessLogic_in.limit(1)
df_unk_out = df_unk_temp.select(
    F.lit(0).alias("BILL_COMSN_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(0).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit("UNK").alias("CLS_PLN_ID"),
    F.lit("UNK").alias("FEE_DSCNT_ID"),
    F.lit(0).alias("BILL_COMSN_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("BILL_ENTY_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("FEE_DSCNT_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit("UNK").alias("COMSN_ARGMT_ID"),
    F.lit("1753-01-01").alias("BILL_COMSN_EFF_DT_SK"),
    F.lit(0).alias("BILL_COMSN_PRM_DCML_PCT"),
    F.lit(0).alias("BILL_COMSN_PRM_PCT"),
    F.lit("1753-01-01").alias("BILL_COMSN_TERM_DT_SK"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_na_temp = df_xmf_businessLogic_in.limit(1)
df_na_out = df_na_temp.select(
    F.lit(1).alias("BILL_COMSN_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(1).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit("NA").alias("CLS_PLN_ID"),
    F.lit("NA").alias("FEE_DSCNT_ID"),
    F.lit(0).alias("BILL_COMSN_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("BILL_ENTY_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("FEE_DSCNT_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit("NA").alias("COMSN_ARGMT_ID"),
    F.lit("1753-01-01").alias("BILL_COMSN_EFF_DT_SK"),
    F.lit(0).alias("BILL_COMSN_PRM_DCML_PCT"),
    F.lit(0).alias("BILL_COMSN_PRM_PCT"),
    F.lit("1753-01-01").alias("BILL_COMSN_TERM_DT_SK"),
    F.lit("NA").alias("GRP_ID"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_fnl_main = df_main_out.unionByName(df_unk_out).unionByName(df_na_out)

df_fnl_main_ordered = df_fnl_main.select(
    F.col("BILL_COMSN_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("BILL_COMSN_SEQ_NO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("GRP_SK"),
    F.col("COMSN_ARGMT_ID"),
    F.rpad(F.col("BILL_COMSN_EFF_DT_SK"), 10, " ").alias("BILL_COMSN_EFF_DT_SK"),
    F.col("BILL_COMSN_PRM_DCML_PCT"),
    F.col("BILL_COMSN_PRM_PCT"),
    F.rpad(F.col("BILL_COMSN_TERM_DT_SK"), 10, " ").alias("BILL_COMSN_TERM_DT_SK"),
    F.col("GRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_fnl_main_ordered,
    f"{adls_path}/load/BILL_COMSN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)