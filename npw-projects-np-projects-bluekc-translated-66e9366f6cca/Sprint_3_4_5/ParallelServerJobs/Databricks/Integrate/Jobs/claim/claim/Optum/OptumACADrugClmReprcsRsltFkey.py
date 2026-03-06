# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :   OptumACAClmLnShadowAdjctCntl
# MAGIC  
# MAGIC DESCRIPTION:  This Job Create the Load File from Sequential File created in Extract Job.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                                                                               Change Description              Development Project      Code Reviewer          Date Reviewed       
# MAGIC                                                                                                                                                                                                                                                                                      
# MAGIC Velmani Kondappan      10-30-2020          6264 - PBM Phase II - Government Programs                                                                               IntegrateDev5               Reddy Sanam             2020-12-16

# MAGIC This job processes ACA Supplemental file (EX-9 file) OPTUMRX.PBM.DrugClm_EX9_ACA* and performs Foreign Key / code mapping lookups to populate DRUG_CLM_ACA_REPRCS_RSLT                                                   This job is called in OptumACAClmLnShadowAdjctCntl sequence job
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
    DecimalType
)
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# --------------------------------------------------------------------------------
# db2_DrugClm (Reference dataset for lkp_DrugClmSk)
# --------------------------------------------------------------------------------
jdbc_url_db2_drugclm, jdbc_props_db2_drugclm = get_db_config(ids_secret_name)
query_db2_drugclm = (
    f"SELECT "
    f"DRUG_CLM.DRUG_CLM_SK, "
    f"DRUG_CLM.CLM_ID AS DRUG_CLM_CLM_ID, "
    f"CD_MPPNG.SRC_CD AS CD_MPPNG_SRC_CD "
    f"FROM {IDSOwner}.DRUG_CLM DRUG_CLM "
    f"JOIN {IDSOwner}.CD_MPPNG CD_MPPNG ON DRUG_CLM.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK"
)
df_db2_DrugClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_drugclm)
    .options(**jdbc_props_db2_drugclm)
    .option("query", query_db2_drugclm)
    .load()
)

# --------------------------------------------------------------------------------
# Seq_DRUG_CLM_ACA_REPRCS_RSLT (Read input file)
# --------------------------------------------------------------------------------
schema_seq_DRUG_CLM_ACA_REPRCS_RSLT = StructType([
    StructField("DRUG_CLM_ACA_REPRCS_RSLT_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_REPRCS_SEQ_NO", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("DRUG_CLM_SK", IntegerType(), nullable=False),
    StructField("QHP_CSR_VRNT_CD", StringType(), nullable=False),
    StructField("CLM_REPRCS_DT_SK", StringType(), nullable=False),
    StructField("CLM_SUBMT_DT_SK", StringType(), nullable=False),
    StructField("CLM_SUBMT_TM", StringType(), nullable=False),
    StructField("ORIG_CLM_RCVD_DT_SK", StringType(), nullable=False),
    StructField("CLM_REPRCS_PAYBL_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_REPRCS_PATN_RESP_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_PAYBL_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_PATN_RESP_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_COINS_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("PBM_CALC_CSR_SBSDY_AMT", DecimalType(38,10), nullable=False),
])
df_seq_DRUG_CLM_ACA_REPRCS_RSLT = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_seq_DRUG_CLM_ACA_REPRCS_RSLT)
    .csv(f"{adls_path}/key/DRUG_CLM_ACA_REPRCS_RSLT.{RunID}.dat")
)

# --------------------------------------------------------------------------------
# Copy (Pass-through)
# --------------------------------------------------------------------------------
df_Copy = df_seq_DRUG_CLM_ACA_REPRCS_RSLT

# --------------------------------------------------------------------------------
# lkp_DrugClmSk (Left join with df_db2_DrugClm)
# --------------------------------------------------------------------------------
df_lkp_DrugClmSk_joined = df_Copy.alias("Lnk_Xfm_logic").join(
    df_db2_DrugClm.alias("Lnk_db2_DrugClm"),
    [
        col("Lnk_Xfm_logic.CLM_ID") == col("Lnk_db2_DrugClm.DRUG_CLM_CLM_ID"),
        col("Lnk_Xfm_logic.SRC_SYS_CD") == col("Lnk_db2_DrugClm.CD_MPPNG_SRC_CD")
    ],
    how="left"
)
df_lkp_DrugClmSk = df_lkp_DrugClmSk_joined.select(
    col("Lnk_Xfm_logic.DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    col("Lnk_Xfm_logic.CLM_ID").alias("CLM_ID"),
    col("Lnk_Xfm_logic.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    col("Lnk_Xfm_logic.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_Xfm_logic.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Xfm_logic.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_db2_DrugClm.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("Lnk_Xfm_logic.QHP_CSR_VRNT_CD").alias("QHP_CSR_VRNT_CD"),
    col("Lnk_Xfm_logic.CLM_REPRCS_DT_SK").alias("CLM_REPRCS_DT_SK"),
    col("Lnk_Xfm_logic.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    col("Lnk_Xfm_logic.CLM_SUBMT_TM").alias("CLM_SUBMT_TM"),
    col("Lnk_Xfm_logic.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    col("Lnk_Xfm_logic.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    col("Lnk_Xfm_logic.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    col("Lnk_Xfm_logic.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    col("Lnk_Xfm_logic.SHADOW_ADJDCT_PATN_RESP_AMT").alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    col("Lnk_Xfm_logic.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    col("Lnk_Xfm_logic.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    col("Lnk_Xfm_logic.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    col("Lnk_Xfm_logic.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

# --------------------------------------------------------------------------------
# Reject_DRUG_CLM_ACA_REPRCS_RSLT (Assume empty, as it's a left join with no error triggers)
# --------------------------------------------------------------------------------
df_Reject_DRUG_CLM_ACA_REPRCS_RSLT = df_lkp_DrugClmSk.filter(lit(False))
df_Reject_out = df_Reject_DRUG_CLM_ACA_REPRCS_RSLT.select(
    col("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    col("CLM_ID"),
    col("CLM_REPRCS_SEQ_NO"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRUG_CLM_SK"),
    col("QHP_CSR_VRNT_CD"),
    col("CLM_REPRCS_DT_SK"),
    col("CLM_SUBMT_DT_SK"),
    col("CLM_SUBMT_TM"),
    col("ORIG_CLM_RCVD_DT_SK"),
    col("CLM_REPRCS_PAYBL_AMT"),
    col("CLM_REPRCS_PATN_RESP_AMT"),
    col("SHADOW_ADJDCT_PAYBL_AMT"),
    col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    col("SHADOW_ADJDCT_COINS_AMT"),
    col("SHADOW_ADJDCT_COPAY_AMT"),
    col("SHADOW_ADJDCT_DEDCT_AMT"),
    col("PBM_CALC_CSR_SBSDY_AMT")
).withColumn(
    "CLM_REPRCS_DT_SK", rpad("CLM_REPRCS_DT_SK", 10, " ")
).withColumn(
    "CLM_SUBMT_DT_SK", rpad("CLM_SUBMT_DT_SK", 10, " ")
).withColumn(
    "ORIG_CLM_RCVD_DT_SK", rpad("ORIG_CLM_RCVD_DT_SK", 10, " ")
)

write_files(
    df_Reject_out,
    f"{adls_path}/load/REJECT_DRUG_CLM_ACA_REPRCS_RSLT.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_SK (Reference dataset for Lkp_CdMppng)
# --------------------------------------------------------------------------------
jdbc_url_cd_mppng_sk, jdbc_props_cd_mppng_sk = get_db_config(ids_secret_name)
query_cd_mppng_sk = (
    f"SELECT "
    f"TRIM(SRC_DRVD_LKUP_VAL) AS SRC_DRVD_LKUP_VAL, "
    f"CD_MPPNG_SK, "
    f"CD_MPPNG.SRC_SYS_CD "
    f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG "
    f"WHERE CD_MPPNG.SRC_CLCTN_CD = '{SrcSysCd}' "
    f"  AND CD_MPPNG.SRC_DOMAIN_NM = 'COST SHARE REDUCTION VARIANT' "
    f"  AND CD_MPPNG.TRGT_CLCTN_CD = 'IDS' "
    f"  AND CD_MPPNG.TRGT_DOMAIN_NM = 'COST SHARE REDUCTION VARIANT'"
)
df_db2_CD_MPPNG_SK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cd_mppng_sk)
    .options(**jdbc_props_cd_mppng_sk)
    .option("query", query_cd_mppng_sk)
    .load()
)

# --------------------------------------------------------------------------------
# Lkp_CdMppng (Left join on SRC_SYS_CD and QHP_CSR_VRNT_CD vs SRC_DRVD_LKUP_VAL)
# --------------------------------------------------------------------------------
df_Lkp_CdMppng_joined = df_lkp_DrugClmSk.alias("LnkDrugClmPrice_Out").join(
    df_db2_CD_MPPNG_SK.alias("lnk_Codes"),
    [
        col("LnkDrugClmPrice_Out.SRC_SYS_CD") == col("lnk_Codes.SRC_SYS_CD"),
        col("LnkDrugClmPrice_Out.QHP_CSR_VRNT_CD") == col("lnk_Codes.SRC_DRVD_LKUP_VAL")
    ],
    how="left"
)
df_Lkp_CdMppng = df_Lkp_CdMppng_joined.select(
    col("LnkDrugClmPrice_Out.DRUG_CLM_ACA_REPRCS_RSLT_SK").alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    col("LnkDrugClmPrice_Out.CLM_ID").alias("CLM_ID"),
    col("LnkDrugClmPrice_Out.CLM_REPRCS_SEQ_NO").alias("CLM_REPRCS_SEQ_NO"),
    col("LnkDrugClmPrice_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LnkDrugClmPrice_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LnkDrugClmPrice_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LnkDrugClmPrice_Out.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    col("lnk_Codes.CD_MPPNG_SK").alias("QHP_CSR_VRNT_CD_SK"),
    col("LnkDrugClmPrice_Out.CLM_REPRcs_DT_SK").alias("CLM_REPRCS_DT_SK"),
    col("LnkDrugClmPrice_Out.CLM_SUBMT_DT_SK").alias("CLM_SUBMT_DT_SK"),
    col("LnkDrugClmPrice_Out.CLM_SUBMT_TM").alias("CLM_SUBMT_TM"),
    col("LnkDrugClmPrice_Out.ORIG_CLM_RCVD_DT_SK").alias("ORIG_CLM_RCVD_DT_SK"),
    col("LnkDrugClmPrice_Out.CLM_REPRCS_PAYBL_AMT").alias("CLM_REPRCS_PAYBL_AMT"),
    col("LnkDrugClmPrice_Out.CLM_REPRCS_PATN_RESP_AMT").alias("CLM_REPRCS_PATN_RESP_AMT"),
    col("LnkDrugClmPrice_Out.SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    col("LnkDrugClmPrice_Out.SHADOW_ADJDCT_PATN_RESP_AMT").alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    col("LnkDrugClmPrice_Out.SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    col("LnkDrugClmPrice_Out.SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    col("LnkDrugClmPrice_Out.SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    col("LnkDrugClmPrice_Out.PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

# --------------------------------------------------------------------------------
# Xfm1 (three output links => normal, UNK, NA)
# --------------------------------------------------------------------------------
w = Window.orderBy(lit(1))
df_with_rnum = df_Lkp_CdMppng.withColumn("rownum", row_number().over(w))

# Normal pass
df_xfm1_normal = df_with_rnum.select(
    col("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    col("CLM_ID"),
    col("CLM_REPRCS_SEQ_NO"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DRUG_CLM_SK"),
    col("QHP_CSR_VRNT_CD_SK"),
    col("CLM_REPRCS_DT_SK"),
    col("CLM_SUBMT_DT_SK"),
    col("CLM_SUBMT_TM"),
    col("ORIG_CLM_RCVD_DT_SK"),
    col("CLM_REPRCS_PAYBL_AMT"),
    col("CLM_REPRCS_PATN_RESP_AMT"),
    col("SHADOW_ADJDCT_PAYBL_AMT"),
    col("SHADOW_ADJDCT_PATN_RESP_AMT"),
    col("SHADOW_ADJDCT_COINS_AMT"),
    col("SHADOW_ADJDCT_COPAY_AMT"),
    col("SHADOW_ADJDCT_DEDCT_AMT"),
    col("PBM_CALC_CSR_SBSDY_AMT")
)

# UNK link
df_unk = df_with_rnum.filter(col("rownum") == 1).select(
    lit(0).alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_REPRCS_SEQ_NO"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("DRUG_CLM_SK"),
    lit(0).alias("QHP_CSR_VRNT_CD_SK"),
    lit("0").alias("CLM_REPRCS_DT_SK"),
    lit("0").alias("CLM_SUBMT_DT_SK"),
    lit("00:00:00").alias("CLM_SUBMT_TM"),
    lit("0").alias("ORIG_CLM_RCVD_DT_SK"),
    lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_PAYBL_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_COINS_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_COPAY_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_DEDCT_AMT"),
    lit(0.00).alias("PBM_CALC_CSR_SBSDY_AMT")
)

# NA link
df_na = df_with_rnum.filter(col("rownum") == 1).select(
    lit(1).alias("DRUG_CLM_ACA_REPRCS_RSLT_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CLM_REPRCS_SEQ_NO"),
    lit("NA").alias("SRC_SYS_CD"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("DRUG_CLM_SK"),
    lit(1).alias("QHP_CSR_VRNT_CD_SK"),
    lit("1").alias("CLM_REPRCS_DT_SK"),
    lit("1").alias("CLM_SUBMT_DT_SK"),
    lit("00:00:00").alias("CLM_SUBMT_TM"),
    lit("1").alias("ORIG_CLM_RCVD_DT_SK"),
    lit(0.00).alias("CLM_REPRCS_PAYBL_AMT"),
    lit(0.00).alias("CLM_REPRCS_PATN_RESP_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_PAYBL_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_PATN_RESP_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_COINS_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_COPAY_AMT"),
    lit(0.00).alias("SHADOW_ADJDCT_DEDCT_AMT"),
    lit(0.00).alias("PBM_CALC_CSR_SBSDY_AMT")
)

# --------------------------------------------------------------------------------
# Funnel_170 (union of normal, UNK, NA)
# --------------------------------------------------------------------------------
df_funnel = df_xfm1_normal.union(df_unk).union(df_na)

# --------------------------------------------------------------------------------
# SeqTgt_DRUG_CLM_ACA_REPRCS_RSLT (final output)
# --------------------------------------------------------------------------------
df_final = df_funnel.withColumn(
    "CLM_REPRCS_DT_SK", rpad("CLM_REPRCS_DT_SK", 10, " ")
).withColumn(
    "CLM_SUBMT_DT_SK", rpad("CLM_SUBMT_DT_SK", 10, " ")
).withColumn(
    "ORIG_CLM_RCVD_DT_SK", rpad("ORIG_CLM_RCVD_DT_SK", 10, " ")
).select(
    "DRUG_CLM_ACA_REPRCS_RSLT_SK",
    "CLM_ID",
    "CLM_REPRCS_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRUG_CLM_SK",
    "QHP_CSR_VRNT_CD_SK",
    "CLM_REPRCS_DT_SK",
    "CLM_SUBMT_DT_SK",
    "CLM_SUBMT_TM",
    "ORIG_CLM_RCVD_DT_SK",
    "CLM_REPRCS_PAYBL_AMT",
    "CLM_REPRCS_PATN_RESP_AMT",
    "SHADOW_ADJDCT_PAYBL_AMT",
    "SHADOW_ADJDCT_PATN_RESP_AMT",
    "SHADOW_ADJDCT_COINS_AMT",
    "SHADOW_ADJDCT_COPAY_AMT",
    "SHADOW_ADJDCT_DEDCT_AMT",
    "PBM_CALC_CSR_SBSDY_AMT"
)

write_files(
    df_final,
    f"{adls_path}/load/DRUG_CLM_ACA_REPRCS_RSLT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="",
    nullValue=None
)