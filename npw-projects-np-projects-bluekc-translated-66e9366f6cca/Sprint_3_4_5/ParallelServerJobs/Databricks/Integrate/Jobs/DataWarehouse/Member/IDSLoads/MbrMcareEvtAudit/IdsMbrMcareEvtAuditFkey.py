# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Kalyan Neelam         2020-10-05               Original Prgramming                                                                                                    IntegrateDev2                  Reddy Sanam            2020-10-05

# MAGIC Perform Foreign Key Lookups to populate MBR_DNTL_RWRD_ACCUM table.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC FKEY failures are written into this flat file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','100')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','FKeysFailure.FctsIdsMbrMcareEvtAuditCntl.100')
RunCycle = get_widget_value('RunCycle','100')

DSJobName = "IdsMbrMcareEvtAuditFkey"

# --------------------------------------------------------------------------------
# Stage: seq_MBR_MCARE_EVT_AUDIT (PxSequentialFile) - Read .dat file
# --------------------------------------------------------------------------------
schema_seq_MBR_MCARE_EVT_AUDIT = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYCLE_TS", TimestampType(), True),
    StructField("MBR_MCARE_EVT_AUDIT_SK", IntegerType(), True),
    StructField("MBR_MCARE_EVT_ROW_AUDIT_ID", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("SRC_SYS_CRT_DT_SK", StringType(), True),
    StructField("MBR_MCARE_EVT_ACTN_CD", StringType(), True),
    StructField("MBR_SK", IntegerType(), True),
    StructField("SRC_SYS_CRT_USER", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_MCARE_EVT_CD", StringType(), True),
    StructField("EFF_DT_SK", StringType(), True),
    StructField("TERM_DT_SK", StringType(), True),
    StructField("SRC_SYS_CRT_DT", StringType(), True),
    StructField("MCARE_EVT_ADTNL_EFF_DT", StringType(), True),
    StructField("MCARE_EVT_ADTNL_TERM_DT", StringType(), True),
    StructField("MBR_RESDNC_ST_NM", StringType(), True),
    StructField("MBR_RESDNC_CNTY_NM", StringType(), True),
    StructField("MBR_MCARE_ID", StringType(), True),
    StructField("BILL_GRP_UNIQ_KEY", IntegerType(), True),
    StructField("MCARE_CNTR_ID", StringType(), True),
    StructField("PART_A_RISK_ADJ_FCTR", IntegerType(), True),
    StructField("PART_B_RISK_ADJ_FCTR", IntegerType(), True),
    StructField("SGNTR_DT", StringType(), True),
    StructField("MCARE_ELECTN_TYP_ID", StringType(), True),
    StructField("PLN_BNF_PCKG_ID", StringType(), True),
    StructField("SEG_ID", StringType(), True),
    StructField("PART_D_RISK_ADJ_FCTR", IntegerType(), True),
    StructField("RISK_ADJ_FCTR_TYP_ID", StringType(), True),
    StructField("PRM_WTHLD_OPT_ID", StringType(), True),
    StructField("PART_C_PRM_AMT", DecimalType(38,10), True),
    StructField("PART_D_PRM_AMT", DecimalType(38,10), True),
    StructField("PRIOR_COM_OVRD_ID", StringType(), True),
    StructField("ENR_SRC_ID", StringType(), True),
    StructField("UNCOV_MO_CT", IntegerType(), True),
    StructField("PART_D_ID", StringType(), True),
    StructField("PART_D_GRP_ID", StringType(), True),
    StructField("PART_D_RX_BIN_ID", StringType(), True),
    StructField("PART_D_RX_PCN_ID", StringType(), True),
    StructField("SEC_DRUG_INSUR_IN", StringType(), True),
    StructField("SEC_DRUG_INSUR_ID", StringType(), True),
    StructField("SEC_DRUG_INSUR_GRP_ID", StringType(), True),
    StructField("SEC_DRUG_INSUR_BIN_ID", StringType(), True),
    StructField("SEC_DRUG_INSUR_PCN_ID", StringType(), True),
    StructField("PART_D_SBSDY_ID", StringType(), True),
    StructField("COPAY_CAT_ID", StringType(), True),
    StructField("PART_D_LOW_INCM_PRM_SBSDY_AMT", DecimalType(38,10), True),
    StructField("PART_D_LATE_ENR_PNLTY_AMT", DecimalType(38,10), True),
    StructField("PART_D_LATE_ENR_PNLTY_WAIVED_AMT", DecimalType(38,10), True),
    StructField("PART_D_LATE_ENR_PNLTY_SBSDY_AMT", DecimalType(38,10), True),
    StructField("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID", StringType(), True),
    StructField("PART_D_RISK_ADJ_FCTR_TYP_ID", StringType(), True),
    StructField("IC_MDL_IN", StringType(), True),
    StructField("IC_MDL_BNF_STTUS_TX", StringType(), True),
    StructField("IC_MDL_END_DT_RSN_TX", StringType(), True),
    StructField("MBR_MCARE_EVT_ATCHMT_DTM", TimestampType(), True)
])

df_seq_MBR_MCARE_EVT_AUDIT = (
    spark.read.csv(
        path=f"{adls_path}/key/MBR_MCARE_EVT_AUDIT.{SrcSysCd}.pkey.{RunID}.dat",
        schema=schema_seq_MBR_MCARE_EVT_AUDIT,
        sep=",",
        quote="^",
        header=False
    )
)

# Add the extra column mentioned in the second event-cd join condition
df_seq_MBR_MCARE_EVT_AUDIT = df_seq_MBR_MCARE_EVT_AUDIT.withColumn(
    "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None).cast(IntegerType())
)

# --------------------------------------------------------------------------------
# Stage: db2_MBR_Lkp (DB2ConnectorPX) - Read from IDS database
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_mbr = f"SELECT MBR_UNIQ_KEY, MBR_SK FROM {IDSOwner}.MBR"
df_db2_MBR_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Copy_of_db2_User_Lkp (DB2ConnectorPX) - Read from IDS database
# --------------------------------------------------------------------------------
extract_query_user = f"SELECT USER_ID, SRC_SYS_CD_SK, USER_SK FROM {IDSOwner}.APP_USER"
df_copy_of_db2_User_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_user)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_CD_MPPNG_LkpData (PxDataSet) - Read from .ds => read from Parquet
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet").select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

# --------------------------------------------------------------------------------
# Stage: fltr_FilterData (PxFilter)
# --------------------------------------------------------------------------------
df_Ref_ActnCd_LNk = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' "
    "AND SRC_DOMAIN_NM='AUDIT ACTION' AND TRGT_DOMAIN_NM='AUDIT ACTION'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_Ref_EvtCd_LNk = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' "
    "AND SRC_DOMAIN_NM='MEDICARE EVENT' AND TRGT_DOMAIN_NM='MEDICARE EVENT'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Code_SKs (PxLookup)
# --------------------------------------------------------------------------------
df_seq_in = df_seq_MBR_MCARE_EVT_AUDIT.alias("Lnk_IdsMbrMcareEvtAuditFkey_InAbc")
df_mbr_in = df_db2_MBR_Lkp.alias("Ref_MBR_In")
df_actn_in = df_Ref_ActnCd_LNk.alias("Ref_ActnCd_LNk")
df_evt_in = df_Ref_EvtCd_LNk.alias("Ref_EvtCd_LNk")
df_user_in = df_copy_of_db2_User_Lkp.alias("Ref_User_In")

df_lkp_code_sks = (
    df_seq_in
    .join(df_mbr_in, df_seq_in.MBR_UNIQ_KEY == df_mbr_in.MBR_UNIQ_KEY, "left")
    .join(
        df_actn_in,
        df_seq_in.MBR_MCARE_EVT_ACTN_CD == df_actn_in.SRC_CD,
        "left"
    )
    .join(
        df_evt_in,
        (df_seq_in.MBR_MCARE_EVT_CD == df_evt_in.SRC_CD)
        & (df_seq_in.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK == df_evt_in.CD_MPPNG_SK),
        "left"
    )
    .join(
        df_user_in,
        (df_seq_in.SRC_SYS_CRT_USER == df_user_in.USER_ID)
        & (df_seq_in.SRC_SYS_CD_SK == df_user_in.SRC_SYS_CD_SK),
        "left"
    )
)

df_lkp_code_sks = df_lkp_code_sks.select(
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.FIRST_RECYCLE_TS").alias("FIRST_RECYCLE_TS"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_EVT_AUDIT_SK").alias("MBR_MCARE_EVT_AUDIT_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_EVT_ROW_AUDIT_ID").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("Ref_ActnCd_LNk.CD_MPPNG_SK").alias("MBR_MCARE_EVT_ACTN_CD_SK"),
    F.col("Ref_MBR_In.MBR_SK").alias("MBR_SK"),
    F.col("Ref_User_In.USER_SK").alias("SRC_SYS_CRT_USER_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Ref_EvtCd_LNk.CD_MPPNG_SK").alias("MBR_MCARE_EVT_CD_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SRC_SYS_CRT_DT").alias("SRC_SYS_CRT_DT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MCARE_EVT_ADTNL_EFF_DT").alias("MCARE_EVT_ADTNL_EFF_DT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MCARE_EVT_ADTNL_TERM_DT").alias("MCARE_EVT_ADTNL_TERM_DT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_RESDNC_ST_NM").alias("MBR_RESDNC_ST_NM"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_RESDNC_CNTY_NM").alias("MBR_RESDNC_CNTY_NM"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_ID").alias("MBR_MCARE_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.BILL_GRP_UNIQ_KEY").alias("BILL_GRP_UNIQ_KEY"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MCARE_CNTR_ID").alias("MCARE_CNTR_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_A_RISK_ADJ_FCTR").alias("PART_A_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_B_RISK_ADJ_FCTR").alias("PART_B_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SGNTR_DT").alias("SGNTR_DT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MCARE_ELECTN_TYP_ID").alias("MCARE_ELECTN_TYP_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PLN_BNF_PCKG_ID").alias("PLN_BNF_PCKG_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEG_ID").alias("SEG_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_RISK_ADJ_FCTR").alias("PART_D_RISK_ADJ_FCTR"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.RISK_ADJ_FCTR_TYP_ID").alias("RISK_ADJ_FCTR_TYP_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PRM_WTHLD_OPT_ID").alias("PRM_WTHLD_OPT_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_C_PRM_AMT").alias("PART_C_PRM_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_PRM_AMT").alias("PART_D_PRM_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PRIOR_COM_OVRD_ID").alias("PRIOR_COM_OVRD_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.ENR_SRC_ID").alias("ENR_SRC_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.UNCOV_MO_CT").alias("UNCOV_MO_CT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_ID").alias("PART_D_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_GRP_ID").alias("PART_D_GRP_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_RX_BIN_ID").alias("PART_D_RX_BIN_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_RX_PCN_ID").alias("PART_D_RX_PCN_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEC_DRUG_INSUR_IN").alias("SEC_DRUG_INSUR_IN"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEC_DRUG_INSUR_ID").alias("SEC_DRUG_INSUR_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEC_DRUG_INSUR_GRP_ID").alias("SEC_DRUG_INSUR_GRP_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEC_DRUG_INSUR_BIN_ID").alias("SEC_DRUG_INSUR_BIN_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SEC_DRUG_INSUR_PCN_ID").alias("SEC_DRUG_INSUR_PCN_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_SBSDY_ID").alias("PART_D_SBSDY_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.COPAY_CAT_ID").alias("COPAY_CAT_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_LOW_INCM_PRM_SBSDY_AMT").alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_LATE_ENR_PNLTY_AMT").alias("PART_D_LATE_ENR_PNLTY_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_LATE_ENR_PNLTY_WAIVED_AMT").alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_LATE_ENR_PNLTY_SBSDY_AMT").alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.PART_D_RISK_ADJ_FCTR_TYP_ID").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.IC_MDL_IN").alias("IC_MDL_IN"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.IC_MDL_BNF_STTUS_TX").alias("IC_MDL_BNF_STTUS_TX"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.IC_MDL_END_DT_RSN_TX").alias("IC_MDL_END_DT_RSN_TX"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_EVT_ATCHMT_DTM").alias("MBR_MCARE_EVT_ATCHMT_DTM"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_EVT_ACTN_CD").alias("MBR_MCARE_EVT_ACTN_CD"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.MBR_MCARE_EVT_CD").alias("MBR_MCARE_EVT_CD"),
    F.col("Lnk_IdsMbrMcareEvtAuditFkey_InAbc.SRC_SYS_CRT_USER").alias("SRC_SYS_CRT_USER")
)

# --------------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm = (
    df_lkp_code_sks
    .withColumn(
        "SvActnCdFKeyLkpCheck",
        F.when(
            (F.col("MBR_MCARE_EVT_ACTN_CD_SK").isNull())
            & (F.col("MBR_MCARE_EVT_ACTN_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvEvtCdFKeyLkpCheck",
        F.when(
            (F.col("MBR_MCARE_EVT_CD_SK").isNull())
            & (F.col("MBR_MCARE_EVT_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvUserFKeyLkpCheck",
        F.when(
            F.col("SRC_SYS_CRT_USER_SK").isNull(),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvMbrFKeyLkpCheck",
        F.when(
            F.col("MBR_SK").isNull(),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Main output link columns
df_main_out = (
    df_xfm
    .withColumn(
        "MBR_MCARE_EVT_ACTN_CD_SK",
        F.when(F.col("MBR_MCARE_EVT_ACTN_CD") == F.lit("NA"), F.lit(1))
         .when(F.col("MBR_MCARE_EVT_ACTN_CD") == F.lit("UNK"), F.lit(0))
         .when(F.col("MBR_MCARE_EVT_ACTN_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("MBR_MCARE_EVT_ACTN_CD_SK"))
    )
    .withColumn(
        "MBR_SK",
        F.when(F.col("MBR_UNIQ_KEY") == F.lit("NA"), F.lit(1))
         .when(F.col("MBR_UNIQ_KEY") == F.lit("UNK"), F.lit(0))
         .when(F.col("MBR_SK").isNull(), F.lit(0))
         .otherwise(F.col("MBR_SK"))
    )
    .withColumn(
        "SRC_SYS_CRT_USER_SK",
        F.when(F.col("SRC_SYS_CRT_USER") == F.lit("NA"), F.lit(1))
         .when(F.col("SRC_SYS_CRT_USER") == F.lit("UNK"), F.lit(0))
         .when(F.col("SRC_SYS_CRT_USER_SK").isNull(), F.lit(0))
         .otherwise(F.col("SRC_SYS_CRT_USER_SK"))
    )
    .withColumn(
        "MBR_MCARE_EVT_CD_SK",
        F.when(F.col("MBR_MCARE_EVT_CD") == F.lit("NA"), F.lit(1))
         .when(F.col("MBR_MCARE_EVT_CD") == F.lit("UNK"), F.lit(0))
         .when(F.col("MBR_MCARE_EVT_CD_SK").isNull(), F.lit(0))
         .otherwise(F.col("MBR_MCARE_EVT_CD_SK"))
    )
    .select(
        F.col("MBR_MCARE_EVT_AUDIT_SK"),
        F.col("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CRT_DT_SK"),
        F.col("MBR_MCARE_EVT_ACTN_CD_SK"),
        F.col("MBR_SK"),
        F.col("SRC_SYS_CRT_USER_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("MBR_MCARE_EVT_CD_SK"),
        F.col("EFF_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("SRC_SYS_CRT_DT"),
        F.col("MCARE_EVT_ADTNL_EFF_DT"),
        F.col("MCARE_EVT_ADTNL_TERM_DT"),
        F.col("MBR_RESDNC_ST_NM"),
        F.col("MBR_RESDNC_CNTY_NM"),
        F.col("MBR_MCARE_ID"),
        F.col("BILL_GRP_UNIQ_KEY"),
        F.col("MCARE_CNTR_ID"),
        F.col("PART_A_RISK_ADJ_FCTR"),
        F.col("PART_B_RISK_ADJ_FCTR"),
        F.col("SGNTR_DT"),
        F.col("MCARE_ELECTN_TYP_ID"),
        F.col("PLN_BNF_PCKG_ID"),
        F.col("SEG_ID"),
        F.col("PART_D_RISK_ADJ_FCTR"),
        F.col("RISK_ADJ_FCTR_TYP_ID"),
        F.col("PRM_WTHLD_OPT_ID"),
        F.col("PART_C_PRM_AMT"),
        F.col("PART_D_PRM_AMT"),
        F.col("PRIOR_COM_OVRD_ID"),
        F.col("ENR_SRC_ID"),
        F.col("UNCOV_MO_CT"),
        F.col("PART_D_ID"),
        F.col("PART_D_GRP_ID"),
        F.col("PART_D_RX_BIN_ID"),
        F.col("PART_D_RX_PCN_ID"),
        F.col("SEC_DRUG_INSUR_IN"),
        F.col("SEC_DRUG_INSUR_ID"),
        F.col("SEC_DRUG_INSUR_GRP_ID"),
        F.col("SEC_DRUG_INSUR_BIN_ID"),
        F.col("SEC_DRUG_INSUR_PCN_ID"),
        F.col("PART_D_SBSDY_ID"),
        F.col("COPAY_CAT_ID"),
        F.col("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
        F.col("PART_D_LATE_ENR_PNLTY_AMT"),
        F.col("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
        F.col("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
        F.col("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
        F.col("PART_D_RISK_ADJ_FCTR_TYP_ID"),
        F.col("IC_MDL_IN"),
        F.col("IC_MDL_BNF_STTUS_TX"),
        F.col("IC_MDL_END_DT_RSN_TX"),
        F.col("MBR_MCARE_EVT_ATCHMT_DTM")
    )
)

# "Lnk_MbrMcareEvtAudit_UNK" => only the very first row. We replicate the DataStage expression logic for columns
w = Window.orderBy(F.lit(1))
df_xfm_numbered = df_xfm.withColumn("rownum", F.row_number().over(w))

df_unk_out = (
    df_xfm_numbered.filter(F.col("rownum") == 1)
    .select(
        F.lit(0).alias("MBR_MCARE_EVT_AUDIT_SK"),
        F.lit("UNK").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
        F.lit(0).alias("MBR_MCARE_EVT_ACTN_CD_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(0).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(0).alias("MBR_UNIQ_KEY"),
        F.lit(0).alias("MBR_MCARE_EVT_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("2199-12-31").alias("TERM_DT_SK"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT"),
        F.lit("1753-01-01").alias("MCARE_EVT_ADTNL_EFF_DT"),
        F.lit("2199-12-31").alias("MCARE_EVT_ADTNL_TERM_DT"),
        F.lit("UNK").alias("MBR_RESDNC_ST_NM"),
        F.lit("UNK").alias("MBR_RESDNC_CNTY_NM"),
        F.lit("UNK").alias("MBR_MCARE_ID"),
        F.lit(0).alias("BILL_GRP_UNIQ_KEY"),
        F.lit("UNK").alias("MCARE_CNTR_ID"),
        F.lit(0).alias("PART_A_RISK_ADJ_FCTR"),
        F.lit(0).alias("PART_B_RISK_ADJ_FCTR"),
        F.lit("1753-01-01").alias("SGNTR_DT"),
        F.lit("UNK").alias("MCARE_ELECTN_TYP_ID"),
        F.lit("UNK").alias("PLN_BNF_PCKG_ID"),
        F.lit("UNK").alias("SEG_ID"),
        F.lit(0).alias("PART_D_RISK_ADJ_FCTR"),
        F.lit("UNK").alias("RISK_ADJ_FCTR_TYP_ID"),
        F.lit("UNK").alias("PRM_WTHLD_OPT_ID"),
        F.lit(0).alias("PART_C_PRM_AMT"),
        F.lit(0).alias("PART_D_PRM_AMT"),
        F.lit("UNK").alias("PRIOR_COM_OVRD_ID"),
        F.lit("UNK").alias("ENR_SRC_ID"),
        F.lit(0).alias("UNCOV_MO_CT"),
        F.lit("UNK").alias("PART_D_ID"),
        F.lit("UNK").alias("PART_D_GRP_ID"),
        F.lit("UNK").alias("PART_D_RX_BIN_ID"),
        F.lit("UNK").alias("PART_D_RX_PCN_ID"),
        F.lit("N").alias("SEC_DRUG_INSUR_IN"),
        F.lit("UNK").alias("SEC_DRUG_INSUR_ID"),
        F.lit("UNK").alias("SEC_DRUG_INSUR_GRP_ID"),
        F.lit("UNK").alias("SEC_DRUG_INSUR_BIN_ID"),
        F.lit("UNK").alias("SEC_DRUG_INSUR_PCN_ID"),
        F.lit("UNK").alias("PART_D_SBSDY_ID"),
        F.lit("UNK").alias("COPAY_CAT_ID"),
        F.lit(0).alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
        F.lit("UNK").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
        F.lit("UNK").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
        F.lit("N").alias("IC_MDL_IN"),
        F.lit("UNK").alias("IC_MDL_BNF_STTUS_TX"),
        F.lit("UNK").alias("IC_MDL_END_DT_RSN_TX"),
        F.lit("1753-01-01 00:00:00").alias("MBR_MCARE_EVT_ATCHMT_DTM")
    )
)

# "Lnk_MbrMcareEvtAudit_NA" => also only the first row
df_na_out = (
    df_xfm_numbered.filter(F.col("rownum") == 1)
    .select(
        F.lit(1).alias("MBR_MCARE_EVT_AUDIT_SK"),
        F.lit("NA").alias("MBR_MCARE_EVT_ROW_AUDIT_ID"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT_SK"),
        F.lit(1).alias("MBR_MCARE_EVT_ACTN_CD_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("SRC_SYS_CRT_USER_SK"),
        F.lit(1).alias("MBR_UNIQ_KEY"),
        F.lit(1).alias("MBR_MCARE_EVT_CD_SK"),
        F.lit("1753-01-01").alias("EFF_DT_SK"),
        F.lit("2199-12-31").alias("TERM_DT_SK"),
        F.lit("1753-01-01").alias("SRC_SYS_CRT_DT"),
        F.lit("1753-01-01").alias("MCARE_EVT_ADTNL_EFF_DT"),
        F.lit("2199-12-31").alias("MCARE_EVT_ADTNL_TERM_DT"),
        F.lit("NA").alias("MBR_RESDNC_ST_NM"),
        F.lit("NA").alias("MBR_RESDNC_CNTY_NM"),
        F.lit("NA").alias("MBR_MCARE_ID"),
        F.lit(0).alias("BILL_GRP_UNIQ_KEY"),
        F.lit("NA").alias("MCARE_CNTR_ID"),
        F.lit(0).alias("PART_A_RISK_ADJ_FCTR"),
        F.lit(0).alias("PART_B_RISK_ADJ_FCTR"),
        F.lit("1753-01-01").alias("SGNTR_DT"),
        F.lit("NA").alias("MCARE_ELECTN_TYP_ID"),
        F.lit("NA").alias("PLN_BNF_PCKG_ID"),
        F.lit("NA").alias("SEG_ID"),
        F.lit(0).alias("PART_D_RISK_ADJ_FCTR"),
        F.lit("NA").alias("RISK_ADJ_FCTR_TYP_ID"),
        F.lit("NA").alias("PRM_WTHLD_OPT_ID"),
        F.lit(0).alias("PART_C_PRM_AMT"),
        F.lit(0).alias("PART_D_PRM_AMT"),
        F.lit("NA").alias("PRIOR_COM_OVRD_ID"),
        F.lit("NA").alias("ENR_SRC_ID"),
        F.lit(0).alias("UNCOV_MO_CT"),
        F.lit("NA").alias("PART_D_ID"),
        F.lit("NA").alias("PART_D_GRP_ID"),
        F.lit("NA").alias("PART_D_RX_BIN_ID"),
        F.lit("NA").alias("PART_D_RX_PCN_ID"),
        F.lit("N").alias("SEC_DRUG_INSUR_IN"),
        F.lit("NA").alias("SEC_DRUG_INSUR_ID"),
        F.lit("NA").alias("SEC_DRUG_INSUR_GRP_ID"),
        F.lit("NA").alias("SEC_DRUG_INSUR_BIN_ID"),
        F.lit("NA").alias("SEC_DRUG_INSUR_PCN_ID"),
        F.lit("NA").alias("PART_D_SBSDY_ID"),
        F.lit("NA").alias("COPAY_CAT_ID"),
        F.lit(0).alias("PART_D_LOW_INCM_PRM_SBSDY_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_WAIVED_AMT"),
        F.lit(0).alias("PART_D_LATE_ENR_PNLTY_SBSDY_AMT"),
        F.lit("NA").alias("AGED_DSBLD_MCARE_SEC_PAYER_STTUS_ID"),
        F.lit("NA").alias("PART_D_RISK_ADJ_FCTR_TYP_ID"),
        F.lit("N").alias("IC_MDL_IN"),
        F.lit("NA").alias("IC_MDL_BNF_STTUS_TX"),
        F.lit("NA").alias("IC_MDL_END_DT_RSN_TX"),
        F.lit("1753-01-01 00:00:00").alias("MBR_MCARE_EVT_ATCHMT_DTM")
    )
)

# MbrFKey fail link
df_mbr_fail = df_xfm.filter(F.col("SvMbrFKeyLkpCheck") == F.lit("Y")).select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("MBR").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_UNIQ_KEY")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ActnCd fail link
df_actncd_fail = df_xfm.filter(F.col("SvActnCdFKeyLkpCheck") == F.lit("Y")).select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_MCARE_EVT_ACTN_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# EvtCd fail link
df_evtcd_fail = df_xfm.filter(F.col("SvEvtCdFKeyLkpCheck") == F.lit("Y")).select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_MCARE_EVT_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# User fail link
df_user_fail = df_xfm.filter(F.col("SvUserFKeyLkpCheck") == F.lit("Y")).select(
    F.col("MBR_MCARE_EVT_AUDIT_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("APP_USER").alias("PHYSCL_FILE_NM"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("SRC_SYS_CRT_USER")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: fnl_NA_UNK_Streams (PxFunnel)
# --------------------------------------------------------------------------------
df_funnel_main = df_main_out
df_funnel_unk = df_unk_out
df_funnel_na = df_na_out

funnel_cols = df_main_out.columns

df_fnl_mbrMcareEvt = df_funnel_main.select(funnel_cols).unionByName(
    df_funnel_unk.select(funnel_cols), allowMissingColumns=True
).unionByName(
    df_funnel_na.select(funnel_cols), allowMissingColumns=True
)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_DNTL_RWRD_ACCUM_Fkey (PxSequentialFile) - Write .dat
# --------------------------------------------------------------------------------
# For char/varchar columns that have known length in the JSON, apply rpad. 
# The JSON has "SqlType": "char", "Length": "10" for certain columns. We'll do a small mapping for them:
char_length_map = {
    "SRC_SYS_CRT_DT_SK": 10,
    "EFF_DT_SK": 10,
    "TERM_DT_SK": 10,
    "SRC_SYS_CRT_DT": 10,
    "MCARE_EVT_ADTNL_EFF_DT": 10,
    "MCARE_EVT_ADTNL_TERM_DT": 10,
    "SGNTR_DT": 10,
    "IC_MDL_IN": 1
}
df_write_main = df_fnl_mbrMcareEvt
for c, l in char_length_map.items():
    df_write_main = df_write_main.withColumn(c, F.rpad(F.col(c), l, " "))

write_files(
    df_write_main,
    f"{adls_path}/load/MBR_MCARE_EVT_AUDIT.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: FnlFkeyFailures (PxFunnel)
# --------------------------------------------------------------------------------
df_fail_funnel = df_mbr_fail.unionByName(df_actncd_fail, allowMissingColumns=True)
df_fail_funnel = df_fail_funnel.unionByName(df_evtcd_fail, allowMissingColumns=True)
df_fail_funnel = df_fail_funnel.unionByName(df_user_fail, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: seq_FkeyFailedFile (PxSequentialFile) - Write .dat
# --------------------------------------------------------------------------------
write_files(
    df_fail_funnel,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)