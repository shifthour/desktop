# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for MBR_MCARE_EVT_AUDIT table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam         2020-10-05               Original Prgramming                                                                                                    IntegrateDev2                  Reddy Sanam            2020-10-05

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read db2_K_MBR_DNTL_RWRD_ACCUM Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter parsing
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
RunID = get_widget_value('RunID','100')

# --------------------------------------------------------------------------------
# Stage: MBR_MCARE_EVT_AUDIT_xfrm (PxDataSet)
# --------------------------------------------------------------------------------
df_MBR_MCARE_EVT_AUDIT_xfrm = spark.read.parquet(
    f"{adls_path}/ds/MBR_MCARE_EVT_AUDIT.xfrm.{RunID}.parquet"
)

# --------------------------------------------------------------------------------
# Stage: cp_pk (PxCopy)
# --------------------------------------------------------------------------------
df_cp_pk_input = df_MBR_MCARE_EVT_AUDIT_xfrm

# Output link: lnk_Transforms_Out
df_lnk_Transforms_Out = df_cp_pk_input.select(
    "MBR_MCARE_EVT_ROW_AUDIT_ID",
    "SRC_SYS_CD_SK"
)

# Output link: Lnk_cp_Out
df_Lnk_cp_Out = df_cp_pk_input.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYCLE_TS",
    "MBR_MCARE_EVT_ROW_AUDIT_ID",
    "SRC_SYS_CD_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_MCARE_EVT_ACTN_CD",
    "MBR_SK",
    "SRC_SYS_CRT_USER",
    "MBR_UNIQ_KEY",
    "MBR_MCARE_EVT_CD",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "SRC_SYS_CRT_DT",
    "MCARE_EVT_ADTNL_EFF_DT",
    "MCARE_EVT_ADTNL_TERM_DT",
    "MBR_RESDNC_ST_NM",
    "MBR_RESDNC_CNTY_NM",
    "MBR_MCARE_ID",
    "BILL_GRP_UNIQ_KEY",
    "MCARE_CNTR_ID",
    "PART_A_RISK_ADJ_FCTR",
    "PART_B_RISK_ADJ_FCTR",
    "SGNTR_DT",
    "MCARE_ELECTN_TYP_ID",
    "PLN_BNF_PCKG_ID",
    "SEG_ID",
    "PART_D_RISK_ADJ_FCTR",
    "RISK_ADJ_FCTR_TYP_ID",
    "PRM_WTHLD_OPT_ID",
    "PART_C_PRM_AMT",
    "PART_D_PRM_AMT",
    "PRIOR_COM_OVRD_ID",
    "ENR_SRC_ID",
    "UNCOV_MO_CT",
    "PART_D_ID",
    "PART_D_GRP_ID",
    "PART_D_RX_BIN_ID",
    "PART_D_RX_PCN_ID",
    "SEC_DRUG_INSUR_IN",
    "SEC_DRUG_INSUR_ID",
    "SEC_DRUG_INSUR_GRP_ID",
    "SEC_DRUG_INSUR_BIN_ID",
    "SEC_DRUG_INSUR_PCN_ID",
    "PART_D_SBSDY_ID",
    "COPAY_CAT_ID",
    "PART_D_LOW_INCM_PRM_SBSDY_AMT",
    "PART_D_LATE_ENR_PNLTY_AMT",
    "PART_D_LATE_ENR_PNLTY_WAIVED_AMT",
    "PART_D_LATE_ENR_PNLTY_SBSDY_AMT",
    "AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID",
    "PART_D_RISK_ADJ_FCTR_TYP_ID",
    "IC_MDL_IN",
    "IC_MDL_BNF_STTUS_TX",
    "IC_MDL_END_DT_RSN_TX",
    "MBR_MCARE_EVT_ATCHMT_DTM"
)

# --------------------------------------------------------------------------------
# Stage: rdup_Natural_Keys (PxRemDup)
# --------------------------------------------------------------------------------
df_lnk_Natural_Keys_out = dedup_sort(
    df_lnk_Transforms_Out,
    ["MBR_MCARE_EVT_ROW_AUDIT_ID", "SRC_SYS_CD_SK"],
    [("MBR_MCARE_EVT_ROW_AUDIT_ID", "A"), ("SRC_SYS_CD_SK", "A")]
)

# --------------------------------------------------------------------------------
# Stage: db2_K_MBR_MCARE_EVT_AUDIT_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT MBR_MCARE_EVT_ROW_AUDIT_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, MBR_MCARE_EVT_AUDIT_SK "
    f"FROM {IDSOwner}.K_MBR_MCARE_EVT_AUDIT"
)
df_db2_K_MBR_MCARE_EVT_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: jn_MbrMcareEvtAudit (PxJoin - leftouterjoin)
# --------------------------------------------------------------------------------
df_jn_MbrMcareEvtAudit = (
    df_lnk_Natural_Keys_out.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_MBR_MCARE_EVT_AUDIT_in.alias("Pkey_out"),
        on=[
            F.col("lnk_Natural_Keys_out.MBR_MCARE_EVT_ROW_AUDIT_ID")
            == F.col("Pkey_out.MBR_MCARE_EVT_ROW_AUDIT_ID"),
            F.col("lnk_Natural_Keys_out.SRC_SYS_CD_SK")
            == F.col("Pkey_out.SRC_SYS_CD_SK"),
        ],
        how="left",
    )
    .select(
        F.col("lnk_Natural_Keys_out.MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Pkey_out.MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    )
)

# --------------------------------------------------------------------------------
# Stage: xfrm_PKEYgen (CTransformerStage)
# --------------------------------------------------------------------------------
# Rename input column to detect null prior to SurrogateKeyGen
df_xfrm_pre = df_jn_MbrMcareEvtAudit.withColumnRenamed(
    "MBR_MCARE_EVT_AUDIT_SK", "INPUT_MBR_MCARE_EVT_AUDIT_SK"
)

df_enriched = (
    df_xfrm_pre.withColumn(
        "MBR_MCARE_EVT_AUDIT_SK",
        F.when(F.col("INPUT_MBR_MCARE_EVT_AUDIT_SK").isNull(), F.lit(None))
        .otherwise(F.col("INPUT_MBR_MCARE_EVT_AUDIT_SK")),
    )
    .withColumn(
        "svRunCyle",
        F.when(F.col("INPUT_MBR_MCARE_EVT_AUDIT_SK").isNull(), F.lit(IDSRunCycle))
        .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")),
    )
)

# Surrogate key fill for MBR_MCARE_EVT_AUDIT_SK where null
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_MCARE_EVT_AUDIT_SK",<schema>,<secret_name>)

# Output link: lnk_Pkey_out
df_lnk_Pkey_out = df_enriched.select(
    F.col("MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
)

# Output link: lnk_KMbrMcareEvtAudit (Constraint => IsNull(input MBR_MCARE_EVT_AUDIT_SK))
df_lnk_KMbrMcareEvtAudit = (
    df_enriched.filter(F.col("INPUT_MBR_MCARE_EVT_AUDIT_SK").isNull())
    .select(
        F.col("MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    )
)

# --------------------------------------------------------------------------------
# Stage: db2_K_MBR_MCARE_EVT_AUDIT_Load (DB2ConnectorPX)
# Append/Insert -> replicate as a merge with WHEN MATCHED THEN DO NOTHING
# --------------------------------------------------------------------------------
df_db2_K_MBR_MCARE_EVT_AUDIT_Load_temp = "STAGING.IdsMbrMcareEctAuditPkey_db2_K_MBR_MCARE_EVT_AUDIT_Load_temp"

# 1) Drop temp table
execute_dml(
    f"DROP TABLE IF EXISTS {df_db2_K_MBR_MCARE_EVT_AUDIT_Load_temp}",
    jdbc_url,
    jdbc_props
)

# 2) Create temp table (physical) via write
df_lnk_KMbrMcareEvtAudit.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", df_db2_K_MBR_MCARE_EVT_AUDIT_Load_temp) \
    .mode("append") \
    .save()

# 3) Merge into target
merge_sql = f"""
MERGE {IDSOwner}.K_MBR_MCARE_EVT_AUDIT AS T
USING {df_db2_K_MBR_MCARE_EVT_AUDIT_Load_temp} AS S
ON 
    T.MBR_MCARE_EVT_ROW_AUDIT_ID = S.MBR_MCARE_EVT_ROW_AUDIT_ID
    AND T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.MBR_MCARE_EVT_AUDIT_SK = S.MBR_MCARE_EVT_AUDIT_SK
WHEN MATCHED THEN 
    UPDATE SET
        -- Do nothing (append-only simulation)
        T.MBR_MCARE_EVT_ROW_AUDIT_ID = T.MBR_MCARE_EVT_ROW_AUDIT_ID
WHEN NOT MATCHED THEN
    INSERT (MBR_MCARE_EVT_ROW_AUDIT_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, MBR_MCARE_EVT_AUDIT_SK)
    VALUES (S.MBR_MCARE_EVT_ROW_AUDIT_ID, S.SRC_SYS_CD_SK, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_MCARE_EVT_AUDIT_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# Stage: jn_PKey (PxJoin - innerjoin)
# --------------------------------------------------------------------------------
df_jn_PKey = (
    df_Lnk_cp_Out.alias("Lnk_cp_Out")
    .join(
        df_lnk_Pkey_out.alias("lnk_Pkey_out"),
        on=[
            F.col("Lnk_cp_Out.MBR_MCARE_EVT_ROW_AUDIT_ID")
            == F.col("lnk_Pkey_out.MBR_MCARE_EVT_ROW_AUDIT_ID"),
            F.col("Lnk_cp_Out.SRC_SYS_CD_SK")
            == F.col("lnk_Pkey_out.SRC_SYS_CD_SK"),
        ],
        how="inner",
    )
    .select(
        F.col("Lnk_cp_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_cp_Out.FIRST_RECYCLE_TS").alias("FIRST_RECYCLE_TS"),
        F.col("lnk_Pkey_out.MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
        F.col("Lnk_cp_Out.MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.col("Lnk_cp_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_cp_Out.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("Lnk_cp_Out.MBR_MCARE_EVT_ACTN_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
        F.col("Lnk_cp_Out.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_cp_Out.SRC_SYS_CRT_USER").alias("SRC_SYS_CRT_USER"),
        F.col("Lnk_cp_Out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_cp_Out.MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
        F.col("Lnk_cp_Out.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Lnk_cp_Out.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("Lnk_cp_Out.SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
        F.col("Lnk_cp_Out.MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
        F.col("Lnk_cp_Out.MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
        F.col("Lnk_cp_Out.MBR_RESDNC_ST_NM").alias("MBR_RESDNC_ST_NM"),
        F.col("Lnk_cp_Out.MBR_RESDNC_CNTY_NM").alias("MBR_RESDNC_CNTY_NM"),
        F.col("Lnk_cp_Out.MBR_MCARE_ID").alias("MBR_MCARE_ID"),
        F.col("Lnk_cp_Out.BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
        F.col("Lnk_cp_Out.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
        F.col("Lnk_cp_Out.PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
        F.col("Lnk_cp_Out.PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
        F.col("Lnk_cp_Out.SGNTR_DT").alias("SGNTR_DT"),
        F.col("Lnk_cp_Out.MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
        F.col("Lnk_cp_Out.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
        F.col("Lnk_cp_Out.SEG_ID").alias("SEG_ID"),
        F.col("Lnk_cp_Out.PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
        F.col("Lnk_cp_Out.RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
        F.col("Lnk_cp_Out.PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
        F.col("Lnk_cp_Out.PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
        F.col("Lnk_cp_Out.PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
        F.col("Lnk_cp_Out.PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
        F.col("Lnk_cp_Out.ENR_SRC_ID").alias("ENR_SRC_ID"),
        F.col("Lnk_cp_Out.UNCOV_MO_CT").alias("UNCOV_MO_CT"),
        F.col("Lnk_cp_Out.PART_D_ID").alias("PART_D_ID"),
        F.col("Lnk_cp_Out.PART_D_GRP_ID").alias("PART_D_GRP_ID"),
        F.col("Lnk_cp_Out.PART_D_RX_BIN_ID").alias("PART_D_RX_BIN_ID"),
        F.col("Lnk_cp_Out.PART_D_RX_PCN_ID").alias("PART_D_RX_PCN_ID"),
        F.col("Lnk_cp_Out.SEC_DRUG_INSUR_IN").alias("SEC_DRUG_INSUR_IN"),
        F.col("Lnk_cp_Out.SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
        F.col("Lnk_cp_Out.SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
        F.col("Lnk_cp_Out.SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
        F.col("Lnk_cp_Out.SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
        F.col("Lnk_cp_Out.PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
        F.col("Lnk_cp_Out.COPAY_CAT_ID").alias("COPAY_CAT_ID"),
        F.col("Lnk_cp_Out.PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
        F.col("Lnk_cp_Out.PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
        F.col("Lnk_cp_Out.PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
        F.col("Lnk_cp_Out.PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
        F.col("Lnk_cp_Out.AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
        F.col("Lnk_cp_Out.PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
        F.col("Lnk_cp_Out.IC_MDL_IN").alias("IC_MDL_IN"),
        F.col("Lnk_cp_Out.IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
        F.col("Lnk_cp_Out.IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
        F.col("Lnk_cp_Out.MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM"),
    )
)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_MCARE_EVT_AUDIT (PxSequentialFile)
# Write final file with column order + rpad for char columns
# --------------------------------------------------------------------------------
df_seq_MBR_MCARE_EVT_AUDIT = df_jn_PKey
df_seq_MBR_MCARE_EVT_AUDIT = df_seq_MBR_MCARE_EVT_AUDIT.withColumn(
    "SEC_DRUG_INSUR_IN",
    rpad(F.col("SEC_DRUG_INSUR_IN"), 1, " ")
).withColumn(
    "IC_MDL_IN",
    rpad(F.col("IC_MDL_IN"), 1, " ")
)

# Selecting in exact output order
df_seq_MBR_MCARE_EVT_AUDIT = df_seq_MBR_MCARE_EVT_AUDIT.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYCLE_TS",
    "MBR_MCARE_EVT_AUDIT_SK",
    "MBR_MCARE_EVT_ROW_AUDIT_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CRT_DT_SK",
    "MBR_MCARE_EVT_ACTN_CD",
    "MBR_SK",
    "SRC_SYS_CRT_USER",
    "MBR_UNIQ_KEY",
    "MBR_MCARE_EVT_CD",
    "EFF_DT_SK",
    "TERM_DT_SK",
    "SRC_SYS_CRT_DT",
    "MCARE_EVT_ADTNL_EFF_DT",
    "MCARE_EVT_ADTNL_TERM_DT",
    "MBR_RESDNC_ST_NM",
    "MBR_RESDNC_CNTY_NM",
    "MBR_MCARE_ID",
    "BILL_GRP_UNIQ_KEY",
    "MCARE_CNTR_ID",
    "PART_A_RISK_ADJ_FCTR",
    "PART_B_RISK_ADJ_FCTR",
    "SGNTR_DT",
    "MCARE_ELECTN_TYP_ID",
    "PLN_BNF_PCKG_ID",
    "SEG_ID",
    "PART_D_RISK_ADJ_FCTR",
    "RISK_ADJ_FCTR_TYP_ID",
    "PRM_WTHLD_OPT_ID",
    "PART_C_PRM_AMT",
    "PART_D_PRM_AMT",
    "PRIOR_COM_OVRD_ID",
    "ENR_SRC_ID",
    "UNCOV_MO_CT",
    "PART_D_ID",
    "PART_D_GRP_ID",
    "PART_D_RX_BIN_ID",
    "PART_D_RX_PCN_ID",
    "SEC_DRUG_INSUR_IN",
    "SEC_DRUG_INSUR_ID",
    "SEC_DRUG_INSUR_GRP_ID",
    "SEC_DRUG_INSUR_BIN_ID",
    "SEC_DRUG_INSUR_PCN_ID",
    "PART_D_SBSDY_ID",
    "COPAY_CAT_ID",
    "PART_D_LOW_INCM_PRM_SBSDY_AMT",
    "PART_D_LATE_ENR_PNLTY_AMT",
    "PART_D_LATE_ENR_PNLTY_WAIVED_AMT",
    "PART_D_LATE_ENR_PNLTY_SBSDY_AMT",
    "AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID",
    "PART_D_RISK_ADJ_FCTR_TYP_ID",
    "IC_MDL_IN",
    "IC_MDL_BNF_STTUS_TX",
    "IC_MDL_END_DT_RSN_TX",
    "MBR_MCARE_EVT_ATCHMT_DTM"
)

write_files(
    df_seq_MBR_MCARE_EVT_AUDIT,
    f"{adls_path}/key/MBR_MCARE_EVT_AUDIT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)