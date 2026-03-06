# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2008, 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC SEQ Job- PdeAcctgRptSubmtCntrFCntl
# MAGIC PROCESSING:  This Job Extracts the data from P2P Source File OPTUMRX.PBM.RPT_DDPS_P2P_PDE_ACC_COV* and loads the data into EDW Table PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F
# MAGIC 
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                              Date                 Project/Altiris #                Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------               -------------------    -----------------------------------    ---------------------------------------------------------                 ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Rekha Radhakrishna                     2022-02-03           408843                               Initial Programming                                                       EnterpriseDev3                     Jaideep Mankala         02/24/2022


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    LongType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# 1) Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F  (DB2 Connector)
df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT FILE_ID, SUBMT_CNTR_SEQ_ID, PLN_BNF_PCKG_SEQ_ID, DTL_SEQ_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_DT_SK, PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK FROM {EDWOwner}.K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F"
    )
    .load()
)

# 2) Seq_PdeAcctgRptSubmtCntrPlnBnfPckg_DTL_F (PxSequentialFile) - reading from verified
schema_Seq_PdeAcctgRptSubmtCntrPlnBnfPckg_DTL_F = StructType([
    StructField("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK", IntegerType(), nullable=False),
    StructField("FILE_ID", StringType(), nullable=False),
    StructField("SUBMT_CNTR_SEQ_ID", StringType(), nullable=False),
    StructField("PLN_BNF_PCKG_SEQ_ID", StringType(), nullable=False),
    StructField("DTL_SEQ_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), nullable=False),
    StructField("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK", IntegerType(), nullable=False),
    StructField("SUBMT_CMS_CNTR_ID", StringType(), nullable=False),
    StructField("PLN_BNF_PCKG_ID", StringType(), nullable=False),
    StructField("DRUG_COV_STTUS_CD", StringType(), nullable=False),
    StructField("CUR_MCARE_BNFCRY_ID", StringType(), nullable=False),
    StructField("LAST_SUBMT_MCARE_BNFCRY_ID", StringType(), nullable=False),
    StructField("LAST_SUBMT_CARDHLDR_ID", StringType(), nullable=False),
    StructField("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT", DateType(), nullable=False),
    StructField("RX_CT", IntegerType(), nullable=False),
    StructField("NET_INGR_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_DISPNS_FEE_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_SLS_TAX_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_GROS_DRUG_CST_BFR_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_GROS_DRUG_CST_AFTR_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_TOT_GROS_DRUG_CST_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_PATN_PAY_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_OTHR_TRUE_OOP_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_LOW_INCM_CST_SHARING_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_TRUE_OOP_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_PATN_LIAB_REDC_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_COV_PLN_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_NCOV_PLN_PD_AMT", DecimalType(38,10), nullable=False),
    StructField("ORIG_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("ADJ_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("DEL_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("CATO_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("ATCHMT_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("NON_CATO_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("NONSTD_FMT_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("OUT_OF_NTWK_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("CMS_CNTR_ID", StringType(), nullable=False),
    StructField("SUBMT_DUE_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_EST_PT_OF_SALE_RBT_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_VCCN_ADM_FEE_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_RPTD_GAP_DSCNT_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])

df_Seq_PdeAcctgRptSubmtCntrPlnBnfPckg_DTL_F = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_Seq_PdeAcctgRptSubmtCntrPlnBnfPckg_DTL_F)
    .csv(f"{adls_path}/verified/PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F.txt")
)

# 3) xfm_RecId (CTransformerStage)
df_xfm_RecId = df_Seq_PdeAcctgRptSubmtCntrPlnBnfPckg_DTL_F.select(
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK"),
    F.col("SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
    F.col("CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    F.col("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT").alias("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT"),
    F.col("RX_CT").alias("RX_CT"),
    F.col("NET_INGR_CST_AMT").alias("NET_INGR_CST_AMT"),
    F.col("NET_DISPNS_FEE_AMT").alias("NET_DISPNS_FEE_AMT"),
    F.col("NET_SLS_TAX_AMT").alias("NET_SLS_TAX_AMT"),
    F.col("NET_GROS_DRUG_CST_BFR_AMT").alias("NET_GROS_DRUG_CST_BFR_AMT"),
    F.col("NET_GROS_DRUG_CST_AFTR_AMT").alias("NET_GROS_DRUG_CST_AFTR_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_PATN_PAY_AMT").alias("NET_PATN_PAY_AMT"),
    F.col("NET_OTHR_TRUE_OOP_AMT").alias("NET_OTHR_TRUE_OOP_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_TRUE_OOP_AMT").alias("NET_TRUE_OOP_AMT"),
    F.col("NET_PATN_LIAB_REDC_AMT").alias("NET_PATN_LIAB_REDC_AMT"),
    F.col("NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("NET_NCOV_PLN_PD_AMT").alias("NET_NCOV_PLN_PD_AMT"),
    F.col("ORIG_RX_DRUG_EVT_CT").alias("ORIG_RX_DRUG_EVT_CT"),
    F.col("ADJ_RX_DRUG_EVT_CT").alias("ADJ_RX_DRUG_EVT_CT"),
    F.col("DEL_RX_DRUG_EVT_CT").alias("DEL_RX_DRUG_EVT_CT"),
    F.col("CATO_RX_DRUG_EVT_CT").alias("CATO_RX_DRUG_EVT_CT"),
    F.col("ATCHMT_RX_DRUG_EVT_CT").alias("ATCHMT_RX_DRUG_EVT_CT"),
    F.col("NON_CATO_RX_DRUG_EVT_CT").alias("NON_CATO_RX_DRUG_EVT_CT"),
    F.col("NONSTD_FMT_RX_DRUG_EVT_CT").alias("NONSTD_FMT_RX_DRUG_EVT_CT"),
    F.col("OUT_OF_NTWK_RX_DRUG_EVT_CT").alias("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
    F.col("CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("NET_EST_PT_OF_SALE_RBT_AMT").alias("NET_EST_PT_OF_SALE_RBT_AMT"),
    F.col("NET_VCCN_ADM_FEE_AMT").alias("NET_VCCN_ADM_FEE_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_AMT").alias("NET_RPTD_GAP_DSCNT_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT").alias("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# 4) RmDup (PxRemDup)
df_RmDup = dedup_sort(
    df_xfm_RecId,
    ["FILE_ID","SUBMT_CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","DTL_SEQ_ID","SRC_SYS_CD"],
    []
)

# 5) Copy (PxCopy) -> produces two outputs
df_Copy = df_RmDup
df_Copy_Remove_Dup = df_Copy.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD")
)
df_Copy_AllCol_Join = df_Copy.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID"),
    F.col("DRUG_COV_STTUS_CD"),
    F.col("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_CARDHLDR_ID"),
    F.col("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT"),
    F.col("RX_CT"),
    F.col("NET_INGR_CST_AMT"),
    F.col("NET_DISPNS_FEE_AMT"),
    F.col("NET_SLS_TAX_AMT"),
    F.col("NET_GROS_DRUG_CST_BFR_AMT"),
    F.col("NET_GROS_DRUG_CST_AFTR_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_PATN_PAY_AMT"),
    F.col("NET_OTHR_TRUE_OOP_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_TRUE_OOP_AMT"),
    F.col("NET_PATN_LIAB_REDC_AMT"),
    F.col("NET_COV_PLN_PD_AMT"),
    F.col("NET_NCOV_PLN_PD_AMT"),
    F.col("ORIG_RX_DRUG_EVT_CT"),
    F.col("ADJ_RX_DRUG_EVT_CT"),
    F.col("DEL_RX_DRUG_EVT_CT"),
    F.col("CATO_RX_DRUG_EVT_CT"),
    F.col("ATCHMT_RX_DRUG_EVT_CT"),
    F.col("NON_CATO_RX_DRUG_EVT_CT"),
    F.col("NONSTD_FMT_RX_DRUG_EVT_CT"),
    F.col("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
    F.col("CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT"),
    F.col("NET_EST_PT_OF_SALE_RBT_AMT"),
    F.col("NET_VCCN_ADM_FEE_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# 6) Remove_Duplicates_14 (PxRemDup)
df_Remove_Duplicates_14 = dedup_sort(
    df_Copy_Remove_Dup,
    ["FILE_ID","SUBMT_CNTR_SEQ_ID","PLN_BNF_PCKG_SEQ_ID","DTL_SEQ_ID","SRC_SYS_CD"],
    []
)

df_Lnk_RmDup = df_Remove_Duplicates_14.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD")
)

# 7) Jn1_NKey (PxJoin) => left outer join
df_Jn1_NKey = df_Lnk_RmDup.alias("LNK_RMDUP").join(
    df_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F.alias("LNK_KTABLEIN"),
    (
        (F.col("LNK_RMDUP.FILE_ID") == F.col("LNK_KTABLEIN.FILE_ID")) &
        (F.col("LNK_RMDUP.SUBMT_CNTR_SEQ_ID") == F.col("LNK_KTABLEIN.SUBMT_CNTR_SEQ_ID")) &
        (F.col("LNK_RMDUP.PLN_BNF_PCKG_SEQ_ID") == F.col("LNK_KTABLEIN.PLN_BNF_PCKG_SEQ_ID")) &
        (F.col("LNK_RMDUP.DTL_SEQ_ID") == F.col("LNK_KTABLEIN.DTL_SEQ_ID")) &
        (F.col("LNK_RMDUP.SRC_SYS_CD") == F.col("LNK_KTABLEIN.SRC_SYS_CD"))
    ),
    "left"
)
df_Jn1_NKey_selected = df_Jn1_NKey.select(
    F.col("LNK_RMDUP.FILE_ID").alias("FILE_ID"),
    F.col("LNK_RMDUP.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("LNK_RMDUP.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("LNK_RMDUP.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("LNK_RMDUP.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LNK_KTABLEIN.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LNK_KTABLEIN.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK")
)

# 8) Transformer (CTransformerStage)
# Split into two outputs: Lnk_KTableLoad (new) and Lnk_Jn (existing)
df_Transformer_in_insert = (
    df_Jn1_NKey_selected
    .filter(
       (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK").isNull()) |
       (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK") == 0)
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK", F.lit(None).cast(LongType()))
)

df_Transformer_in_insert = SurrogateKeyGen(
    df_Transformer_in_insert,
    <DB sequence name>,
    "PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK",
    <schema>,
    <secret_name>
)

df_Transformer_Lnk_KTableLoad = df_Transformer_in_insert.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK")
)

df_Transformer_in_update = df_Jn1_NKey_selected.filter(
    ~(
      (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK").isNull()) |
      (F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK") == 0)
    )
)
# Keep existing CRT_RUN_CYC_EXCTN_DT_SK, keep existing PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK
df_Transformer_Lnk_Jn = df_Transformer_in_update.select(
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK")
)

# 9) Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F_Load (DB2ConnectorPX) => Upsert logic
df_to_upsert = df_Transformer_Lnk_KTableLoad  # The stage only shows inserts, but per requirement we do a merge.

temp_table_name = "STAGING.PdeAcctgRptSubmtCntrPlnBnfPckgDtlFExtr_Db2_K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_to_upsert
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F AS T
USING {temp_table_name} AS S
ON
    T.FILE_ID = S.FILE_ID
    AND T.SUBMT_CNTR_SEQ_ID = S.SUBMT_CNTR_SEQ_ID
    AND T.PLN_BNF_PCKG_SEQ_ID = S.PLN_BNF_PCKG_SEQ_ID
    AND T.DTL_SEQ_ID = S.DTL_SEQ_ID
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK = S.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK
WHEN NOT MATCHED THEN
    INSERT (
        FILE_ID,
        SUBMT_CNTR_SEQ_ID,
        PLN_BNF_PCKG_SEQ_ID,
        DTL_SEQ_ID,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_DT_SK,
        PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK
    )
    VALUES (
        S.FILE_ID,
        S.SUBMT_CNTR_SEQ_ID,
        S.PLN_BNF_PCKG_SEQ_ID,
        S.DTL_SEQ_ID,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK
    );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# 10) Jn2Nkey (PxJoin) => an inner join of df_Copy_AllCol_Join and df_Transformer_Lnk_Jn
df_Jn2Nkey = df_Copy_AllCol_Join.alias("Lnk_AllCol_Join").join(
    df_Transformer_Lnk_Jn.alias("Lnk_Jn"),
    (
        (F.col("Lnk_AllCol_Join.FILE_ID") == F.col("Lnk_Jn.FILE_ID")) &
        (F.col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID") == F.col("Lnk_Jn.SUBMT_CNTR_SEQ_ID")) &
        (F.col("Lnk_AllCol_Join.PLN_BNF_PCKG_SEQ_ID") == F.col("Lnk_Jn.PLN_BNF_PCKG_SEQ_ID")) &
        (F.col("Lnk_AllCol_Join.DTL_SEQ_ID") == F.col("Lnk_Jn.DTL_SEQ_ID")) &
        (F.col("Lnk_AllCol_Join.SRC_SYS_CD") == F.col("Lnk_Jn.SRC_SYS_CD"))
    ),
    "inner"
)
df_Jn2Nkey_selected = df_Jn2Nkey.select(
    F.col("Lnk_AllCol_Join.FILE_ID").alias("FILE_ID"),
    F.col("Lnk_AllCol_Join.SUBMT_CNTR_SEQ_ID").alias("SUBMT_CNTR_SEQ_ID"),
    F.col("Lnk_AllCol_Join.PLN_BNF_PCKG_SEQ_ID").alias("PLN_BNF_PCKG_SEQ_ID"),
    F.col("Lnk_AllCol_Join.DTL_SEQ_ID").alias("DTL_SEQ_ID"),
    F.col("Lnk_AllCol_Join.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_AllCol_Join.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK"),
    F.col("Lnk_AllCol_Join.SUBMT_CMS_CNTR_ID").alias("SUBMT_CMS_CNTR_ID"),
    F.col("Lnk_AllCol_Join.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_AllCol_Join.DRUG_COV_STTUS_CD").alias("DRUG_COV_STTUS_CD"),
    F.col("Lnk_AllCol_Join.CUR_MCARE_BNFCRY_ID").alias("CUR_MCARE_BNFCRY_ID"),
    F.col("Lnk_AllCol_Join.LAST_SUBMT_MCARE_BNFCRY_ID").alias("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("Lnk_AllCol_Join.LAST_SUBMT_CARDHLDR_ID").alias("LAST_SUBMT_CARDHLDR_ID"),
    F.col("Lnk_AllCol_Join.ERLST_RX_DRUG_EVT_ATCHMT_PT_DT").alias("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT"),
    F.col("Lnk_AllCol_Join.RX_CT").alias("RX_CT"),
    F.col("Lnk_AllCol_Join.NET_INGR_CST_AMT").alias("NET_INGR_CST_AMT"),
    F.col("Lnk_AllCol_Join.NET_DISPNS_FEE_AMT").alias("NET_DISPNS_FEE_AMT"),
    F.col("Lnk_AllCol_Join.NET_SLS_TAX_AMT").alias("NET_SLS_TAX_AMT"),
    F.col("Lnk_AllCol_Join.NET_GROS_DRUG_CST_BFR_AMT").alias("NET_GROS_DRUG_CST_BFR_AMT"),
    F.col("Lnk_AllCol_Join.NET_GROS_DRUG_CST_AFTR_AMT").alias("NET_GROS_DRUG_CST_AFTR_AMT"),
    F.col("Lnk_AllCol_Join.NET_TOT_GROS_DRUG_CST_AMT").alias("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("Lnk_AllCol_Join.NET_PATN_PAY_AMT").alias("NET_PATN_PAY_AMT"),
    F.col("Lnk_AllCol_Join.NET_OTHR_TRUE_OOP_AMT").alias("NET_OTHR_TRUE_OOP_AMT"),
    F.col("Lnk_AllCol_Join.NET_LOW_INCM_CST_SHARING_AMT").alias("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("Lnk_AllCol_Join.NET_TRUE_OOP_AMT").alias("NET_TRUE_OOP_AMT"),
    F.col("Lnk_AllCol_Join.NET_PATN_LIAB_REDC_AMT").alias("NET_PATN_LIAB_REDC_AMT"),
    F.col("Lnk_AllCol_Join.NET_COV_PLN_PD_AMT").alias("NET_COV_PLN_PD_AMT"),
    F.col("Lnk_AllCol_Join.NET_NCOV_PLN_PD_AMT").alias("NET_NCOV_PLN_PD_AMT"),
    F.col("Lnk_AllCol_Join.ORIG_RX_DRUG_EVT_CT").alias("ORIG_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.ADJ_RX_DRUG_EVT_CT").alias("ADJ_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.DEL_RX_DRUG_EVT_CT").alias("DEL_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.CATO_RX_DRUG_EVT_CT").alias("CATO_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.ATCHMT_RX_DRUG_EVT_CT").alias("ATCHMT_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.NON_CATO_RX_DRUG_EVT_CT").alias("NON_CATO_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.NONSTD_FMT_RX_DRUG_EVT_CT").alias("NONSTD_FMT_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.OUT_OF_NTWK_RX_DRUG_EVT_CT").alias("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.CMS_CNTR_ID").alias("CMS_CNTR_ID"),
    F.col("Lnk_AllCol_Join.SUBMT_DUE_AMT").alias("SUBMT_DUE_AMT"),
    F.col("Lnk_AllCol_Join.NET_EST_PT_OF_SALE_RBT_AMT").alias("NET_EST_PT_OF_SALE_RBT_AMT"),
    F.col("Lnk_AllCol_Join.NET_VCCN_ADM_FEE_AMT").alias("NET_VCCN_ADM_FEE_AMT"),
    F.col("Lnk_AllCol_Join.NET_RPTD_GAP_DSCNT_AMT").alias("NET_RPTD_GAP_DSCNT_AMT"),
    F.col("Lnk_AllCol_Join.NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT").alias("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
    F.col("Lnk_AllCol_Join.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_AllCol_Join.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Jn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_Jn.PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK").alias("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK")
)

# 11) XFM (CTransformerStage)
df_XFM = df_Jn2Nkey_selected.select(
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK"),
    F.col("FILE_ID"),
    F.col("SUBMT_CNTR_SEQ_ID"),
    F.col("PLN_BNF_PCKG_SEQ_ID"),
    F.col("DTL_SEQ_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK"),
    F.col("SUBMT_CMS_CNTR_ID"),
    F.col("PLN_BNF_PCKG_ID"),
    F.col("DRUG_COV_STTUS_CD"),
    F.col("CUR_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_MCARE_BNFCRY_ID"),
    F.col("LAST_SUBMT_CARDHLDR_ID"),
    F.col("ERLST_RX_DRUG_EVT_ATCHMT_PT_DT"),
    F.col("RX_CT"),
    F.col("NET_INGR_CST_AMT"),
    F.col("NET_DISPNS_FEE_AMT"),
    F.col("NET_SLS_TAX_AMT"),
    F.col("NET_GROS_DRUG_CST_BFR_AMT"),
    F.col("NET_GROS_DRUG_CST_AFTR_AMT"),
    F.col("NET_TOT_GROS_DRUG_CST_AMT"),
    F.col("NET_PATN_PAY_AMT"),
    F.col("NET_OTHR_TRUE_OOP_AMT"),
    F.col("NET_LOW_INCM_CST_SHARING_AMT"),
    F.col("NET_TRUE_OOP_AMT"),
    F.col("NET_PATN_LIAB_REDC_AMT"),
    F.col("NET_COV_PLN_PD_AMT"),
    F.col("NET_NCOV_PLN_PD_AMT"),
    F.col("ORIG_RX_DRUG_EVT_CT"),
    F.col("ADJ_RX_DRUG_EVT_CT"),
    F.col("DEL_RX_DRUG_EVT_CT"),
    F.col("CATO_RX_DRUG_EVT_CT"),
    F.col("ATCHMT_RX_DRUG_EVT_CT"),
    F.col("NON_CATO_RX_DRUG_EVT_CT"),
    F.col("NONSTD_FMT_RX_DRUG_EVT_CT"),
    F.col("OUT_OF_NTWK_RX_DRUG_EVT_CT"),
    F.col("CMS_CNTR_ID"),
    F.col("SUBMT_DUE_AMT"),
    F.col("NET_EST_PT_OF_SALE_RBT_AMT"),
    F.col("NET_VCCN_ADM_FEE_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_AMT"),
    F.col("NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# 12) Seq_PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F_Load (PxSequentialFile) - writing to verified
# Prepare final DataFrame with rpad for char/varchar
df_final = df_XFM
df_final = df_final.withColumn("FILE_ID", F.rpad(F.col("FILE_ID"), 100, " "))
df_final = df_final.withColumn("SUBMT_CNTR_SEQ_ID", F.rpad(F.col("SUBMT_CNTR_SEQ_ID"), 100, " "))
df_final = df_final.withColumn("PLN_BNF_PCKG_SEQ_ID", F.rpad(F.col("PLN_BNF_PCKG_SEQ_ID"), 100, " "))
df_final = df_final.withColumn("DTL_SEQ_ID", F.rpad(F.col("DTL_SEQ_ID"), 100, " "))
df_final = df_final.withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 100, " "))
df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("SUBMT_CMS_CNTR_ID", F.rpad(F.col("SUBMT_CMS_CNTR_ID"), 100, " "))
df_final = df_final.withColumn("PLN_BNF_PCKG_ID", F.rpad(F.col("PLN_BNF_PCKG_ID"), 100, " "))
df_final = df_final.withColumn("DRUG_COV_STTUS_CD", F.rpad(F.col("DRUG_COV_STTUS_CD"), 100, " "))
df_final = df_final.withColumn("CUR_MCARE_BNFCRY_ID", F.rpad(F.col("CUR_MCARE_BNFCRY_ID"), 100, " "))
df_final = df_final.withColumn("LAST_SUBMT_MCARE_BNFCRY_ID", F.rpad(F.col("LAST_SUBMT_MCARE_BNFCRY_ID"), 100, " "))
df_final = df_final.withColumn("LAST_SUBMT_CARDHLDR_ID", F.rpad(F.col("LAST_SUBMT_CARDHLDR_ID"), 100, " "))
df_final = df_final.withColumn("CMS_CNTR_ID", F.rpad(F.col("CMS_CNTR_ID"), 100, " "))

df_final = df_final.select(
    "PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_SK",
    "FILE_ID",
    "SUBMT_CNTR_SEQ_ID",
    "PLN_BNF_PCKG_SEQ_ID",
    "DTL_SEQ_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_SK",
    "SUBMT_CMS_CNTR_ID",
    "PLN_BNF_PCKG_ID",
    "DRUG_COV_STTUS_CD",
    "CUR_MCARE_BNFCRY_ID",
    "LAST_SUBMT_MCARE_BNFCRY_ID",
    "LAST_SUBMT_CARDHLDR_ID",
    "ERLST_RX_DRUG_EVT_ATCHMT_PT_DT",
    "RX_CT",
    "NET_INGR_CST_AMT",
    "NET_DISPNS_FEE_AMT",
    "NET_SLS_TAX_AMT",
    "NET_GROS_DRUG_CST_BFR_AMT",
    "NET_GROS_DRUG_CST_AFTR_AMT",
    "NET_TOT_GROS_DRUG_CST_AMT",
    "NET_PATN_PAY_AMT",
    "NET_OTHR_TRUE_OOP_AMT",
    "NET_LOW_INCM_CST_SHARING_AMT",
    "NET_TRUE_OOP_AMT",
    "NET_PATN_LIAB_REDC_AMT",
    "NET_COV_PLN_PD_AMT",
    "NET_NCOV_PLN_PD_AMT",
    "ORIG_RX_DRUG_EVT_CT",
    "ADJ_RX_DRUG_EVT_CT",
    "DEL_RX_DRUG_EVT_CT",
    "CATO_RX_DRUG_EVT_CT",
    "ATCHMT_RX_DRUG_EVT_CT",
    "NON_CATO_RX_DRUG_EVT_CT",
    "NONSTD_FMT_RX_DRUG_EVT_CT",
    "OUT_OF_NTWK_RX_DRUG_EVT_CT",
    "CMS_CNTR_ID",
    "SUBMT_DUE_AMT",
    "NET_EST_PT_OF_SALE_RBT_AMT",
    "NET_VCCN_ADM_FEE_AMT",
    "NET_RPTD_GAP_DSCNT_AMT",
    "NET_RPTD_GAP_DSCNT_RX_DRUG_EVT_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/verified/PDE_ACCTG_RPT_SUBMT_CNTR_PLN_BNF_PCKG_DTL_F_Load.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)