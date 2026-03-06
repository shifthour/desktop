# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC **********************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table
# MAGIC 
# MAGIC 
# MAGIC Called By: OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Initial Development: Peter Gichiri 
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Peter Gichiri              2019-10-02             6131 - PBM REPLACEMENT  to OPTUMRX     Initial Devlopment Integrate                                                            Kalyan Neelam             2019-11-21

# MAGIC IdsMBR_PDX_DENIED_TRANSFkey
# MAGIC Triggered by: OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','OPTUMRX')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','PDX_FileName')

# Hardcode job name since DataStage expressions reference DSJobName
DSJobName = "IdsMbrPdxDeniedClmFkey"

# Schema for seq_mbr_pdx_dined_Pkey input file
schema_seq_mbr_pdx_dined_Pkey = StructType([
    StructField("MBR_PDX_DENIED_TRANS_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PDX_NTNL_PROV_ID", StringType(), nullable=False),
    StructField("TRANS_TYP_CD", StringType(), nullable=False),
    StructField("TRANS_DENIED_DT", DateType(), nullable=False),
    StructField("RX_NO", StringType(), nullable=False),
    StructField("PRCS_DT", DateType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("GRP_SK", IntegerType(), nullable=False),
    StructField("MBR_SK", IntegerType(), nullable=False),
    StructField("NDC_SK", IntegerType(), nullable=False),
    StructField("PRSCRB_PROV_SK", IntegerType(), nullable=False),
    StructField("PROV_SPEC_CD_SK", IntegerType(), nullable=False),
    StructField("SRV_PROV_SK", IntegerType(), nullable=False),
    StructField("SUB_SK", IntegerType(), nullable=False),
    StructField("MEDIA_TYP_CD_SK", StringType(), nullable=False),
    StructField("MBR_GNDR_CD_SK", IntegerType(), nullable=False),
    StructField("MBR_RELSHP_CD_SK", IntegerType(), nullable=False),
    StructField("PDX_RSPN_RSN_CD_SK", StringType(), nullable=False),
    StructField("PDX_RSPN_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("TRANS_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("MAIL_ORDER_IN", StringType(), nullable=False),
    StructField("MBR_BRTH_DT", DateType(), nullable=False),
    StructField("SRC_SYS_CLM_RCVD_DT", DateType(), nullable=False),
    StructField("SRC_SYS_CLM_RCVD_TM", StringType(), nullable=False),
    StructField("SUB_BRTH_DT", DateType(), nullable=False),
    StructField("BILL_RX_DISPENSE_FEE_AMT", DecimalType(38,10), nullable=False),
    StructField("BILL_RX_GROS_APRV_AMT", DecimalType(38,10), nullable=False),
    StructField("BILL_RX_NET_CHK_AMT", DecimalType(38,10), nullable=False),
    StructField("BILL_RX_PATN_PAY_AMT", DecimalType(38,10), nullable=False),
    StructField("INGR_CST_ALW_AMT", DecimalType(38,10), nullable=False),
    StructField("TRANS_MO_NO", IntegerType(), nullable=False),
    StructField("TRANS_YR_NO", IntegerType(), nullable=False),
    StructField("MBR_ID", StringType(), nullable=False),
    StructField("MBR_SFX_NO", StringType(), nullable=False),
    StructField("MBR_FIRST_NM", StringType(), nullable=False),
    StructField("MBR_LAST_NM", StringType(), nullable=False),
    StructField("MBR_SSN", StringType(), nullable=False),
    StructField("PDX_NM", StringType(), nullable=False),
    StructField("PDX_PHN_NO", StringType(), nullable=False),
    StructField("PHYS_DEA_NO", StringType(), nullable=False),
    StructField("PHYS_NTNL_PROV_ID", StringType(), nullable=False),
    StructField("PHYS_FIRST_NM", StringType(), nullable=False),
    StructField("PHYS_LAST_NM", StringType(), nullable=False),
    StructField("PHYS_ST_ADDR_LN", StringType(), nullable=False),
    StructField("PHYS_CITY_NM", StringType(), nullable=False),
    StructField("PHYS_ST_CD", StringType(), nullable=False),
    StructField("PHYS_POSTAL_CD", StringType(), nullable=False),
    StructField("RX_LABEL_TX", StringType(), nullable=False),
    StructField("SVC_PROV_NABP_NM", StringType(), nullable=False),
    StructField("SRC_SYS_CAR_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CLNT_ORG_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CLNT_ORG_NM", StringType(), nullable=False),
    StructField("SRC_SYS_CNTR_ID", StringType(), nullable=False),
    StructField("SRC_SYS_GRP_ID", StringType(), nullable=False),
    StructField("SUB_ID", StringType(), nullable=False),
    StructField("SUB_FIRST_NM", StringType(), nullable=False),
    StructField("SUB_LAST_NM", StringType(), nullable=False),
    StructField("CLNT_ELIG_MBRSH_ID", StringType(), nullable=True),
    StructField("PRSN_NO", StringType(), nullable=True),
    StructField("RELSHP_CD", StringType(), nullable=True),
    StructField("GNDR_CD", StringType(), nullable=True),
    StructField("CLM_RCVD_TM", StringType(), nullable=True),
    StructField("MSG_TYP_CD", StringType(), nullable=True),
    StructField("MSG_TYP", StringType(), nullable=True),
    StructField("MSG_TX", StringType(), nullable=True),
    StructField("CLNT_MBRSH_ID", StringType(), nullable=True),
    StructField("MEDIA_TYP_CD", StringType(), nullable=True),
    StructField("RSPN_CD", StringType(), nullable=True),
    StructField("DESC", StringType(), nullable=True),
    StructField("CHAPTER_ID", StringType(), nullable=True),
    StructField("CHAPTER_DESC", StringType(), nullable=True),
    StructField("PDX_NPI", StringType(), nullable=True),
    StructField("NDC", StringType(), nullable=True),
])

df_seq_mbr_pdx_dined_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_mbr_pdx_dined_Pkey)
    .load(f"{adls_path}/key/MBR_PDX_DENIED_TRANS.{SrcSysCd}.{RunID}.dat")
)

# Read from db2_Grp_Lkp
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_Grp_Lkp = (
    "SELECT DISTINCT\n"
    "G.GRP_SK,\n"
    "G.GRP_ID,\n"
    "M.MBR_UNIQ_KEY AS MBR_UNIQ_KEY,\n"
    "S.SUB_ID||M.MBR_SFX_NO AS MBR_ID\n"
    f"FROM {IDSOwner}.MBR M, {IDSOwner}.SUB S, {IDSOwner}.GRP G\n"
    "WHERE\n"
    "S.SUB_SK = M.SUB_SK AND\n"
    "S.GRP_SK=G.GRP_SK"
)
df_db2_Grp_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_Grp_Lkp)
    .load()
)

# Read from db2_MBR
extract_query_db2_MBR = (
    "SELECT\n"
    "M.MBR_UNIQ_KEY as MBR_UNIQ_KEY,\n"
    "S.SUB_ID||M.MBR_SFX_NO AS MBR_ID,\n"
    "M.MBR_SK AS MBR_SK,\n"
    "S.SUB_SK AS SUB_SK,\n"
    "G.GRP_ID as SRC_SYS_GRP_ID,\n"
    "M.MBR_GNDR_CD_SK MBR_GNDR_CD_SK,\n"
    "M.MBR_RELSHP_CD_SK MBR_RELSHP_CD_SK,\n"
    "M.MBR_SFX_NO MBR_SFX_NO\n"
    f"FROM {IDSOwner}.MBR M, {IDSOwner}.SUB S,{IDSOwner}.GRP G\n"
    "WHERE\n"
    "S.SUB_SK = M.SUB_SK\n"
    "AND \n"
    "S.GRP_SK = G.GRP_SK\n"
    " with ur"
)
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR)
    .load()
)

# Read from db2_NDC
extract_query_db2_NDC = (
    "SELECT \n"
    "NDC_SK,\n"
    "NDC\n"
    f"FROM {IDSOwner}.NDC"
)
df_db2_NDC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_NDC)
    .load()
)

# Read from db2_ds_CD_MPPNG_LkpData
extract_query_db2_ds_CD_MPPNG_LkpData = (
    "SELECT DISTINCT\n"
    "CD_MPPNG_SK,\n"
    "SRC_CD,\n"
    "SRC_CD_NM,\n"
    "SRC_CLCTN_CD,\n"
    "SRC_DRVD_LKUP_VAL,\n"
    "SRC_DOMAIN_NM,\n"
    "SRC_SYS_CD,\n"
    "TRGT_CD,\n"
    "TRGT_CD_NM,\n"
    "TRGT_CLCTN_CD,\n"
    "TRGT_DOMAIN_NM\n"
    f"FROM {IDSOwner}.CD_MPPNG"
)
df_db2_ds_CD_MPPNG_LkpData = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ds_CD_MPPNG_LkpData)
    .load()
)

# Filter stage fltr_FilterData to produce 4 outputs
df_fltr_0 = df_db2_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "MEDIA TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "MEDIA TYPE")
)
df_fltr_1 = df_db2_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "PHARMACY MESSAGE REPONSE TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "PHARMACY MESSAGE REPONSE TYPE")
    & (F.col("SRC_CD") == "R")
    & (F.col("SRC_CD_NM") == "REJECT")
)
df_fltr_2 = df_db2_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "PHARMACY TRANSACTION TYPE")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("SRC_CD_NM") == "PHARMACY CLAIM")
)
df_fltr_3 = df_db2_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_CLCTN_CD") == SrcSysCd)
    & (F.col("SRC_DOMAIN_NM") == "PHARMACY RESPONSE REASON")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("TRGT_DOMAIN_NM") == "PHARMACY RESPONSE REASON")
)

# Perform lookup-style left joins (PxLookup logic)
main_df = df_seq_mbr_pdx_dined_Pkey.alias("main")

df_lkup_fkey = (
    main_df
    # ref0_Media_Type_SK_Lkp join
    .join(
        df_fltr_0.alias("ref0"),
        F.col("main.MEDIA_TYP_CD_SK") == F.col("ref0.SRC_CD"),
        how="left"
    )
    # ref1_Resp_type_cd_lkp join
    .join(
        df_fltr_1.alias("ref1"),
        F.col("main.SRC_SYS_CD") == F.col("ref1.SRC_SYS_CD"),
        how="left"
    )
    # ref2_trans_typ_cd_Lkp join
    .join(
        df_fltr_2.alias("ref2"),
        F.col("main.SRC_SYS_CD") == F.col("ref2.SRC_SYS_CD"),
        how="left"
    )
    # ref_grp_sk_LKp join
    .join(
        df_db2_Grp_Lkp.alias("grp"),
        [
            F.col("main.SRC_SYS_GRP_ID") == F.col("grp.GRP_ID"),
            F.col("main.MBR_ID") == F.col("grp.MBR_ID"),
        ],
        how="left"
    )
    # lnk_mbr_extr join
    .join(
        df_db2_MBR.alias("mbr"),
        [
            F.col("main.SRC_SYS_GRP_ID") == F.col("mbr.SRC_SYS_GRP_ID"),
            F.col("main.MBR_ID") == F.col("mbr.MBR_ID"),
        ],
        how="left"
    )
    # lnk_ndc_extr join
    .join(
        df_db2_NDC.alias("ndc"),
        F.col("main.NDC") == F.col("ndc.NDC"),
        how="left"
    )
    # ref3_rsp_rsn_CD_lk join
    .join(
        df_fltr_3.alias("ref3"),
        F.col("main.PDX_RSPN_RSN_CD_SK") == F.col("ref3.SRC_CD"),
        how="left"
    )
)

# Select columns for output of Lkup_Fkey
df_lkup_fkey_selected = df_lkup_fkey.select(
    F.col("main.MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK"),
    F.col("main.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("main.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("main.TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("main.TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("main.RX_NO").alias("RX_NO"),
    F.col("main.PRCS_DT").alias("PRCS_DT"),
    F.col("main.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("main.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("main.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("grp.GRP_SK").alias("GRP_SK"),
    F.col("mbr.MBR_SK").alias("MBR_SK"),
    F.col("ndc.NDC_SK").alias("NDC_SK"),
    F.col("main.PRSCRB_PROV_SK").alias("PRSCRB_PROV_SK"),
    F.col("main.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("main.SRV_PROV_SK").alias("SRV_PROV_SK"),
    F.col("mbr.SUB_SK").alias("SUB_SK"),
    F.col("ref0.CD_MPPNG_SK").alias("MEDIA_TYP_CD_SK"),
    F.col("main.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("main.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("ref3.CD_MPPNG_SK").alias("PDX_RSPN_RSN_CD_SK"),
    F.col("ref1.CD_MPPNG_SK").alias("PDX_RSPN_TYP_CD_SK"),
    F.col("ref2.CD_MPPNG_SK").alias("TRANS_TYP_CD_SK"),
    F.col("main.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("main.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("main.SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("main.SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("main.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("main.BILL_RX_DISPENSE_FEE_AMT").alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("main.BILL_RX_GROS_APRV_AMT").alias("BILL_RX_GROS_APRV_AMT"),
    F.col("main.BILL_RX_NET_CHK_AMT").alias("BILL_RX_NET_CHK_AMT"),
    F.col("main.BILL_RX_PATN_PAY_AMT").alias("BILL_RX_PATN_PAY_AMT"),
    F.col("main.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("main.TRANS_MO_NO").alias("TRANS_MO_NO"),
    F.col("main.TRANS_YR_NO").alias("TRANS_YR_NO"),
    F.col("main.MBR_ID").alias("MBR_ID"),
    F.col("main.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("main.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("main.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("main.MBR_SSN").alias("MBR_SSN"),
    F.col("main.PDX_NM").alias("PDX_NM"),
    F.col("main.PDX_PHN_NO").alias("PDX_PHN_NO"),
    F.col("main.PHYS_DEA_NO").alias("PHYS_DEA_NO"),
    F.col("main.PHYS_NTNL_PROV_ID").alias("PHYS_NTNL_PROV_ID"),
    F.col("main.PHYS_FIRST_NM").alias("PHYS_FIRST_NM"),
    F.col("main.PHYS_LAST_NM").alias("PHYS_LAST_NM"),
    F.col("main.PHYS_ST_ADDR_LN").alias("PHYS_ST_ADDR_LN"),
    F.col("main.PHYS_CITY_NM").alias("PHYS_CITY_NM"),
    F.col("main.PHYS_ST_CD").alias("PHYS_ST_CD"),
    F.col("main.PHYS_POSTAL_CD").alias("PHYS_POSTAL_CD"),
    F.col("main.RX_LABEL_TX").alias("RX_LABEL_TX"),
    F.col("main.SVC_PROV_NABP_NM").alias("SVC_PROV_NABP_NM"),
    F.col("main.SRC_SYS_CAR_ID").alias("SRC_SYS_CAR_ID"),
    F.col("main.SRC_SYS_CLNT_ORG_ID").alias("SRC_SYS_CLNT_ORG_ID"),
    F.col("main.SRC_SYS_CLNT_ORG_NM").alias("SRC_SYS_CLNT_ORG_NM"),
    F.col("main.SRC_SYS_CNTR_ID").alias("SRC_SYS_CNTR_ID"),
    F.col("main.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("main.SUB_ID").alias("SUB_ID"),
    F.col("main.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("main.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("grp.GRP_ID").alias("GRP_ID"),
    F.col("main.NDC").alias("NDC")
)

# xfm_CheckLkpResults: define stage variables as new columns
df_xfm = (
    df_lkup_fkey_selected
    .withColumn(
        "SvFKeyLkpCheckMediaTypeCd",
        F.when(
            F.trim(
                F.when(F.col("MEDIA_TYP_CD_SK").isNotNull(), F.col("MEDIA_TYP_CD_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckRspCd",
        F.when(
            F.trim(
                F.when(F.col("PDX_RSPN_RSN_CD_SK").isNotNull(), F.col("PDX_RSPN_RSN_CD_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckMsgTypeCd",
        F.when(
            F.trim(
                F.when(F.col("PDX_RSPN_TYP_CD_SK").isNotNull(), F.col("PDX_RSPN_TYP_CD_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckTransTypeCd",
        F.when(
            F.trim(
                F.when(F.col("TRANS_TYP_CD_SK").isNotNull(), F.col("TRANS_TYP_CD_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckGrpCd",
        F.when(
            F.trim(
                F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckMbrCd",
        F.when(
            F.trim(
                F.when(F.col("MBR_SK").isNotNull(), F.col("MBR_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckNDC",
        F.when(
            F.trim(
                F.when(F.col("NDC_SK").isNotNull(), F.col("NDC_SK")).otherwise(F.lit("0"))
            ) == F.lit("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Prepare main output link lnk_pdxdenied_out (no constraint => all rows)
df_lnk_pdxdenied_out = df_xfm.select(
    "MBR_PDX_DENIED_TRANS_SK",
    "MBR_UNIQ_KEY",
    "PDX_NTNL_PROV_ID",
    "TRANS_TYP_CD",
    "TRANS_DENIED_DT",
    "RX_NO",
    "PRCS_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "MBR_SK",
    "NDC_SK",
    "PRSCRB_PROV_SK",
    "PROV_SPEC_CD_SK",
    "SRV_PROV_SK",
    "SUB_SK",
    "MEDIA_TYP_CD_SK",
    "MBR_GNDR_CD_SK",
    "MBR_RELSHP_CD_SK",
    "PDX_RSPN_RSN_CD_SK",
    "PDX_RSPN_TYP_CD_SK",
    "TRANS_TYP_CD_SK",
    "MAIL_ORDER_IN",
    "MBR_BRTH_DT",
    "SRC_SYS_CLM_RCVD_DT",
    "SRC_SYS_CLM_RCVD_TM",
    "SUB_BRTH_DT",
    "BILL_RX_DISPENSE_FEE_AMT",
    "BILL_RX_GROS_APRV_AMT",
    "BILL_RX_NET_CHK_AMT",
    "BILL_RX_PATN_PAY_AMT",
    "INGR_CST_ALW_AMT",
    "TRANS_MO_NO",
    "TRANS_YR_NO",
    "MBR_ID",
    "MBR_SFX_NO",
    "MBR_FIRST_NM",
    "MBR_LAST_NM",
    "MBR_SSN",
    "PDX_NM",
    "PDX_PHN_NO",
    "PHYS_DEA_NO",
    "PHYS_NTNL_PROV_ID",
    "PHYS_FIRST_NM",
    "PHYS_LAST_NM",
    "PHYS_ST_ADDR_LN",
    "PHYS_CITY_NM",
    "PHYS_ST_CD",
    "PHYS_POSTAL_CD",
    "RX_LABEL_TX",
    "SVC_PROV_NABP_NM",
    "SRC_SYS_CAR_ID",
    "SRC_SYS_CLNT_ORG_ID",
    "SRC_SYS_CLNT_ORG_NM",
    "SRC_SYS_CNTR_ID",
    "SRC_SYS_GRP_ID",
    "SUB_ID",
    "SUB_FIRST_NM",
    "SUB_LAST_NM"
)

# rpad for char and varchar columns on the final file
# char(1): MAIL_ORDER_IN
# char(2): MBR_SFX_NO
# all other varchar => use some default length, e.g. 100
def rpad_if_needed(colname, sqltype, length=None):
    if sqltype == "char" and length is not None:
        return F.rpad(F.col(colname), length, " ")
    elif sqltype == "varchar" and length is not None:
        return F.rpad(F.col(colname), length, " ")
    elif sqltype == "char" and length is None:
        return F.rpad(F.col(colname), 1, " ")
    elif sqltype == "varchar" and length is None:
        return F.rpad(F.col(colname), 100, " ")
    else:
        return F.col(colname)

df_final_main = (
    df_lnk_pdxdenied_out
    .withColumn("MAIL_ORDER_IN", rpad_if_needed("MAIL_ORDER_IN", "char", 1))
    .withColumn("MBR_SFX_NO", rpad_if_needed("MBR_SFX_NO", "char", 2)) 
    # All other columns that are varchar have no specified length => default to 100 if needed
    # The columns below are known varchar without a length in the input schema, so apply a default
    .withColumn("PDX_NTNL_PROV_ID", rpad_if_needed("PDX_NTNL_PROV_ID", "varchar"))
    .withColumn("TRANS_TYP_CD", rpad_if_needed("TRANS_TYP_CD", "varchar"))
    .withColumn("RX_NO", rpad_if_needed("RX_NO", "varchar"))
    .withColumn("SRC_SYS_CD", rpad_if_needed("SRC_SYS_CD", "varchar"))
    .withColumn("MBR_ID", rpad_if_needed("MBR_ID", "varchar"))
    .withColumn("MBR_FIRST_NM", rpad_if_needed("MBR_FIRST_NM", "varchar"))
    .withColumn("MBR_LAST_NM", rpad_if_needed("MBR_LAST_NM", "varchar"))
    .withColumn("MBR_SSN", rpad_if_needed("MBR_SSN", "varchar"))
    .withColumn("PDX_NM", rpad_if_needed("PDX_NM", "varchar"))
    .withColumn("PDX_PHN_NO", rpad_if_needed("PDX_PHN_NO", "varchar"))
    .withColumn("PHYS_DEA_NO", rpad_if_needed("PHYS_DEA_NO", "varchar"))
    .withColumn("PHYS_NTNL_PROV_ID", rpad_if_needed("PHYS_NTNL_PROV_ID", "varchar"))
    .withColumn("PHYS_FIRST_NM", rpad_if_needed("PHYS_FIRST_NM", "varchar"))
    .withColumn("PHYS_LAST_NM", rpad_if_needed("PHYS_LAST_NM", "varchar"))
    .withColumn("PHYS_ST_ADDR_LN", rpad_if_needed("PHYS_ST_ADDR_LN", "varchar"))
    .withColumn("PHYS_CITY_NM", rpad_if_needed("PHYS_CITY_NM", "varchar"))
    .withColumn("PHYS_ST_CD", rpad_if_needed("PHYS_ST_CD", "varchar"))
    .withColumn("PHYS_POSTAL_CD", rpad_if_needed("PHYS_POSTAL_CD", "varchar"))
    .withColumn("RX_LABEL_TX", rpad_if_needed("RX_LABEL_TX", "varchar"))
    .withColumn("SVC_PROV_NABP_NM", rpad_if_needed("SVC_PROV_NABP_NM", "varchar"))
    .withColumn("SRC_SYS_CAR_ID", rpad_if_needed("SRC_SYS_CAR_ID", "varchar"))
    .withColumn("SRC_SYS_CLNT_ORG_ID", rpad_if_needed("SRC_SYS_CLNT_ORG_ID", "varchar"))
    .withColumn("SRC_SYS_CLNT_ORG_NM", rpad_if_needed("SRC_SYS_CLNT_ORG_NM", "varchar"))
    .withColumn("SRC_SYS_CNTR_ID", rpad_if_needed("SRC_SYS_CNTR_ID", "varchar"))
    .withColumn("SRC_SYS_GRP_ID", rpad_if_needed("SRC_SYS_GRP_ID", "varchar"))
    .withColumn("SUB_ID", rpad_if_needed("SUB_ID", "varchar"))
    .withColumn("SUB_FIRST_NM", rpad_if_needed("SUB_FIRST_NM", "varchar"))
    .withColumn("SUB_LAST_NM", rpad_if_needed("SUB_LAST_NM", "varchar"))
    .withColumn("GRP_ID", rpad_if_needed("GRP_ID", "varchar"))
    .withColumn("NDC", rpad_if_needed("NDC", "varchar"))
)

# Write to seq_MBR_PDX_DENIED_TRANS_FKey (PxSequentialFile)
write_files(
    df_final_main,
    f"{adls_path}/load/MBR_PDX_DENIED_TRANS.{SrcSysCd}.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue="\""
)

# Now handle the fail links
df_fail_media_type = df_xfm.filter(F.col("SvFKeyLkpCheckMediaTypeCd") == "Y").select(
    F.col("MEDIA_TYP_CD_SK").alias("PRI_SK"),
    F.expr("MEDIA_TYP_CD || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("MEDIA_TYP_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRX' || ';' || 'IDS' || ';' || 'MEDIA TYPE' || ';' || 'MEDIA TYPE' || ';' || MEDIA_TYP_CD").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_rsp_cd = df_xfm.filter(F.col("SvFKeyLkpCheckRspCd") == "Y").select(
    F.col("PDX_RSPN_RSN_CD_SK").alias("PRI_SK"),
    F.expr("RSPN_CD || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("PDX_RSPN_RSN_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'ESI' || ';' || 'IDS' || ';' || 'PHARMACY RESPONSE REASON' || ';' || 'PHARMACY RESPONSE REASON' || ';' || RSPN_CD").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_msg_type = df_xfm.filter(F.col("SvFKeyLkpCheckMsgTypeCd") == "Y").select(
    F.col("PDX_RSPN_TYP_CD_SK").alias("PRI_SK"),
    F.expr("MSG_TYP_CD || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("PDX_RSPN_TYP_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRX' || ';' || 'IDS' || ';' || 'PHARMACY MESSAGE REPONSE TYPE' || ';' || 'PHARMACY MESSAGE REPONSE TYPE' || ';' || MSG_TYP_CD").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_trans_typ = df_xfm.filter(F.col("SvFKeyLkpCheckTransTypeCd") == "Y").select(
    F.col("TRANS_TYP_CD_SK").alias("PRI_SK"),
    F.expr("TRANS_TYP_CD || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("TRANS_TYP_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRX' || ';' || 'IDS' || ';' || 'PHARMACY TRANSACTION TYPE' || ';' || 'PHARMACY TRANSACTION TYPE' || ';' || TRANS_TYP_CD").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_grp = df_xfm.filter(F.col("SvFKeyLkpCheckGrpCd") == "Y").select(
    F.col("GRP_SK").alias("PRI_SK"),
    F.expr("MBR_ID || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("GRP_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKEYLOOKUP").alias("ERROR_TYP"),
    F.lit("GRP").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRX' || ';' || 'IDS' || ';' || 'GRP_ID' || ';' || 'GROUP_TYPE' || ';' || GRP_ID").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_mbr = df_xfm.filter(F.col("SvFKeyLkpCheckMbrCd") == "Y").select(
    F.col("MBR_SK").alias("PRI_SK"),
    F.expr("MBR_ID || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("MBR_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKEYLOOKUP").alias("ERROR_TYP"),
    F.lit("MBR").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRXI' || ';' || 'IDS' || ';' || 'MBR_ID' || ';' || 'MBR_ID' || ';' || MBR_ID").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_ndc = df_xfm.filter(F.col("SvFKeyLkpCheckNDC") == "Y").select(
    F.col("NDC_SK").alias("PRI_SK"),
    F.expr("NDC || SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("NDC_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKEYLOOKUP").alias("ERROR_TYP"),
    F.lit("NDC").alias("PHYSCL_FILE_NM"),
    F.expr("SRC_SYS_CD || ';' || 'OPTUMRX' || ';' || 'IDS' || ';' || 'NDC' || ';' || 'NDC' || ';' || NDC").alias("FRGN_NAT_KEY_STRING"),
    F.expr("StringToTimestamp(DateToString(PRCS_DT, \"%yyyy-%mm-%dd\"), \"%yyyy-%mm-%dd\")").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Funnel them (PxFunnel => union all)
df_fnl_all_fail = df_fail_media_type.unionByName(df_fail_rsp_cd)\
    .unionByName(df_fail_msg_type)\
    .unionByName(df_fail_trans_typ)\
    .unionByName(df_fail_grp)\
    .unionByName(df_fail_mbr)\
    .unionByName(df_fail_ndc)

# Write funnel output to seq_failure_file
write_files(
    df_fnl_all_fail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)