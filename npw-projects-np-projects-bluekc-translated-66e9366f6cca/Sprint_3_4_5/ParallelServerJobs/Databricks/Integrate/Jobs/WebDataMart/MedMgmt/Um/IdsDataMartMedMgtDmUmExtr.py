# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 07/13/06 09:01:21 Batch  14074_32499 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_1 07/13/06 08:56:37 Batch  14074_32207 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_6 07/07/06 09:03:54 Batch  14068_32645 PROMOTE bckcett testIDS30 u10913 OIllie move from devl to TEST
# MAGIC ^1_6 07/07/06 09:02:44 Batch  14068_32572 INIT bckcett devlIDS30 u10913 Ollie move new changes from devl to TEST
# MAGIC 
# MAGIC 
# MAGIC ^1_1 06/01/06 10:52:24 Batch  14032_39155 INIT bckcett devlIDS30 u10913 Ollie moving Med Mgt from Devl to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsDataMartMedMgtDmUmExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                 Pulls data from IDS to create medical management data mart  Extract is based on BeginRunCycle.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - UM
# MAGIC                
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_etrnl_cd_mppng
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                 IsNull
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                 IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                direct update of MED_MGT_DM_UM
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  03/22/2005 -   Originally Programmed
# MAGIC               Tao Luo          06/26/2006 -   Added Run Cycle Logic and join to UM
# MAGIC               Oliver Nielsen  07/06/2006 -  Added new columns -  MBR_FIRST_NM, MBR_MIDINIT, MBR_LAST_NM,MBRGNDR_CD,MBR_GNDR_NM

# MAGIC Use SK's to perform all mappings with any Lookup MappingType.
# MAGIC Transform and output link names are used by job control to get link count.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lit, when, concat, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginRunCycle = get_widget_value('BeginRunCycle','')
CurrRunDt = get_widget_value('CurrRunDt','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_IDSUM = (
    f"SELECT UM.UM_SK as UM_SK,"
    f"UM.SRC_SYS_CD_SK as SRC_SYS_CD_SK,"
    f"UM.UM_REF_ID as UM_REF_ID,"
    f"UM.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,"
    f"UM.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,"
    f"UM.CASE_MGT_SK as CASE_MGT_SK,"
    f"UM.CRT_USER_SK as CRT_USER_SK,"
    f"UM.GRP_SK as GRP_SK,"
    f"UM.MBR_SK as MBR_SK,"
    f"UM.PRI_DIAG_CD_SK as PRI_DIAG_CD_SK,"
    f"UM.PRI_RESP_USER_SK as PRI_RESP_USER_SK,"
    f"UM.PROD_SK as PROD_SK,"
    f"UM.SUBGRP_SK as SUBGRP_SK,"
    f"UM.SUB_SK as SUB_SK,"
    f"UM.QLTY_OF_SVC_LVL_CD_SK as QLTY_OF_SVC_LVL_CD_SK,"
    f"UM.RISK_LVL_CD_SK as RISK_LVL_CD_SK,"
    f"UM.UM_CLS_PLN_PROD_CAT_CD_SK as UM_CLS_PLN_PROD_CAT_CD_SK,"
    f"UM.IP_RCRD_IN as IP_RCRD_IN,"
    f"UM.ATCHMT_SRC_DTM as ATCHMT_SRC_DTM,"
    f"UM.CRT_DT_SK as CRT_DT_SK,"
    f"UM.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM,"
    f"UM.FINL_CARE_DT_SK as FINL_CARE_DT_SK,"
    f"UM.ACTVTY_SEQ_NO as ACTVTY_SEQ_NO "
    f"FROM {IDSOwner}.UM UM "
    f"WHERE UM.UM_SK NOT IN (0,1) "
    f"AND UM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginRunCycle}"
)

df_IDSUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDSUM)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT SUB_SK, SUB_ID FROM {IDSOwner}.SUB")
    .load()
)

df_SUBGRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT SUBGRP_SK, SUBGRP_ID FROM {IDSOwner}.SUBGRP")
    .load()
)

df_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT PROD_SK, PROD_ID FROM {IDSOwner}.PROD")
    .load()
)

df_DIAG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT DIAG_CD_SK, DIAG_CD FROM {IDSOwner}.DIAG_CD")
    .load()
)

df_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT GRP_SK, GRP_ID FROM {IDSOwner}.GRP")
    .load()
)

df_CASE_MGT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CASE_MGT_SK, CASE_MGT_ID FROM {IDSOwner}.CASE_MGT")
    .load()
)

df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT MBR.MBR_SK, MBR.MBR_SFX_NO, SUB.SUB_ID as SUB_ID, "
        f"CD_MPPNG1.TRGT_CD as MBR_GNDR_CD, CD_MPPNG1.TRGT_CD_NM as MBR_GNDR_NM, "
        f"CD_MPPNG2.TRGT_CD as MBR_RELSHP_CD, CD_MPPNG2.TRGT_CD_NM as MBR_RELSHP_NM, "
        f"MBR.FIRST_NM, MBR.MIDINIT, MBR.LAST_NM "
        f"FROM {IDSOwner}.MBR MBR, {IDSOwner}.SUB SUB, {IDSOwner}.CD_MPPNG CD_MPPNG1, {IDSOwner}.CD_MPPNG CD_MPPNG2 "
        f"WHERE MBR.SUB_SK=SUB.SUB_SK "
        f"AND MBR.MBR_GNDR_CD_SK=CD_MPPNG1.CD_MPPNG_SK "
        f"AND MBR.MBR_RELSHP_CD_SK=CD_MPPNG2.CD_MPPNG_SK"
    )
    .load()
)

df_joined = (
    df_IDSUM.alias("um")
    .join(df_SUB.alias("lnkSUB"), col("um.SUB_SK") == col("lnkSUB.SUB_SK"), "left")
    .join(df_SUBGRP.alias("lnkSUBGRP"), col("um.SUBGRP_SK") == col("lnkSUBGRP.SUBGRP_SK"), "left")
    .join(df_PROD.alias("lnkPROD"), col("um.PROD_SK") == col("lnkPROD.PROD_SK"), "left")
    .join(df_DIAG.alias("lnkDIAG"), col("um.PRI_DIAG_CD_SK") == col("lnkDIAG.DIAG_CD_SK"), "left")
    .join(df_MBR.alias("lnkMbr"), col("um.MBR_SK") == col("lnkMbr.MBR_SK"), "left")
    .join(df_GRP.alias("lnkGRP"), col("um.GRP_SK") == col("lnkGRP.GRP_SK"), "left")
    .join(df_CASE_MGT.alias("lnkCASE_MGT"), col("um.CASE_MGT_SK") == col("lnkCASE_MGT.CASE_MGT_SK"), "left")
)

df_tr = (
    df_joined.alias("lnkUm")
    .join(df_hf_etrnl_cd_mppng.alias("refSrcSys"), trim(col("lnkUm.SRC_SYS_CD_SK")) == col("refSrcSys.CD_MPPNG_SK"), "left")
    .join(df_hf_etrnl_cd_mppng.alias("refClsPlnProd"), trim(col("lnkUm.UM_CLS_PLN_PROD_CAT_CD_SK")) == col("refClsPlnProd.CD_MPPNG_SK"), "left")
    .join(df_hf_etrnl_cd_mppng.alias("refQltySvc"), trim(col("lnkUm.QLTY_OF_SVC_LVL_CD_SK")) == col("refQltySvc.CD_MPPNG_SK"), "left")
    .join(df_hf_etrnl_cd_mppng.alias("refRisk"), trim(col("lnkUm.RISK_LVL_CD_SK")) == col("refRisk.CD_MPPNG_SK"), "left")
)

df_filtered = df_tr.filter(
    ~(
        col("lnkSUB.SUB_ID").isNull()
        | col("lnkMbr.MBR_SFX_NO").isNull()
    )
)

df_out_prepared = df_filtered.select(
    rpad(col("refSrcSys.TRGT_CD"), 100, " ").alias("SRC_SYS_CD"),
    rpad(col("lnkUm.UM_REF_ID"), 100, " ").alias("UM_REF_ID"),
    rpad(col("refClsPlnProd.TRGT_CD"), 100, " ").alias("UM_CLS_PLN_PROD_CAT_CD"),
    rpad(col("refClsPlnProd.TRGT_CD_NM"), 100, " ").alias("UM_CLS_PLN_PROD_CAT_NM"),
    rpad(col("refQltySvc.TRGT_CD"), 100, " ").alias("UM_QLTY_OF_SVC_LVL_CD"),
    rpad(col("refQltySvc.TRGT_CD_NM"), 100, " ").alias("UM_QLTY_OF_SVC_LVL_NM"),
    rpad(col("refRisk.TRGT_CD"), 100, " ").alias("UM_RISK_LVL_CD"),
    rpad(col("refRisk.TRGT_CD_NM"), 100, " ").alias("UM_RISK_LVL_NM"),
    rpad(col("lnkUm.IP_RCRD_IN"), 1, " ").alias("UM_IP_RCRD_IN"),
    rpad(lit(CurrRunDt), 100, " ").alias("DM_LAST_UPDT_DT"),
    rpad(FORMAT.DATE(col("lnkUm.MED_MGT_NOTE_DTM"), 'DB2','TIMESTAMP','SYBTIMESTAMP'), 100, " ").alias("MED_MGT_NOTE_DTM"),
    rpad(
        when(
            (trim(col("lnkUm.CRT_DT_SK")) == lit("NA"))
            | (trim(col("lnkUm.CRT_DT_SK")) == lit("UNK")),
            lit(None),
        ).otherwise(col("lnkUm.CRT_DT_SK")),
        100,
        " ",
    ).alias("UM_CRT_DT"),
    rpad(
        when(
            (trim(col("lnkUm.FINL_CARE_DT_SK")) == lit("NA"))
            | (trim(col("lnkUm.FINL_CARE_DT_SK")) == lit("UNK")),
            lit(None),
        ).otherwise(col("lnkUm.FINL_CARE_DT_SK")),
        100,
        " ",
    ).alias("UM_FINL_CARE_DT"),
    rpad(col("lnkUm.ACTVTY_SEQ_NO"), 100, " ").alias("UM_ACTVTY_SEQ_NO"),
    rpad(col("lnkCASE_MGT.CASE_MGT_ID"), 100, " ").alias("CASE_MGT_ID"),
    rpad(coalesce(col("lnkGRP.GRP_ID"), lit(" ")), 100, " ").alias("GRP_ID"),
    rpad(concat(col("lnkMbr.SUB_ID"), col("lnkMbr.MBR_SFX_NO")), 100, " ").alias("MBR_ID"),
    rpad(col("lnkMbr.FIRST_NM"), 100, " ").alias("MBR_FIRST_NM"),
    rpad(col("lnkMbr.MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    rpad(col("lnkMbr.LAST_NM"), 100, " ").alias("MBR_LAST_NM"),
    rpad(col("lnkMbr.MBR_GNDR_CD"), 100, " ").alias("MBR_GNDR_CD"),
    rpad(col("lnkMbr.MBR_GNDR_NM"), 100, " ").alias("MBR_GNDR_NM"),
    rpad(col("lnkMbr.MBR_RELSHP_CD"), 100, " ").alias("MBR_RELSHP_CD"),
    rpad(col("lnkMbr.MBR_RELSHP_NM"), 100, " ").alias("MBR_RELSHP_NM"),
    rpad(col("lnkMbr.MBR_SFX_NO"), 100, " ").alias("MBR_SFX_NO"),
    rpad(col("lnkDIAG.DIAG_CD"), 100, " ").alias("PRI_DIAG_CD"),
    rpad(coalesce(col("lnkPROD.PROD_ID"), lit("UNK")), 100, " ").alias("PROD_ID"),
    rpad(coalesce(col("lnkSUBGRP.SUBGRP_ID"), lit("UNK")), 100, " ").alias("SUBGRP_ID"),
    rpad(coalesce(col("lnkSUB.SUB_ID"), lit("UNK")), 100, " ").alias("SUB_ID"),
    rpad(lit(CurrRunCycle), 100, " ").alias("LAST_UPDT_RUN_CYC_NO"),
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmUmExtr_MED_MGT_DM_UM_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

df_out_prepared.write.format("jdbc")\
    .option("url", jdbc_url_clmmart)\
    .options(**jdbc_props_clmmart)\
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmUmExtr_MED_MGT_DM_UM_temp")\
    .mode("overwrite")\
    .save()

merge_sql = (
    f"MERGE INTO {ClmMartOwner}.MED_MGT_DM_UM AS T "
    f"USING STAGING.IdsDataMartMedMgtDmUmExtr_MED_MGT_DM_UM_temp AS S "
    f"ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.UM_REF_ID = S.UM_REF_ID) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"UM_CLS_PLN_PROD_CAT_CD = S.UM_CLS_PLN_PROD_CAT_CD, "
    f"UM_CLS_PLN_PROD_CAT_NM = S.UM_CLS_PLN_PROD_CAT_NM, "
    f"UM_QLTY_OF_SVC_LVL_CD = S.UM_QLTY_OF_SVC_LVL_CD, "
    f"UM_QLTY_OF_SVC_LVL_NM = S.UM_QLTY_OF_SVC_LVL_NM, "
    f"UM_RISK_LVL_CD = S.UM_RISK_LVL_CD, "
    f"UM_RISK_LVL_NM = S.UM_RISK_LVL_NM, "
    f"UM_IP_RCRD_IN = S.UM_IP_RCRD_IN, "
    f"DM_LAST_UPDT_DT = S.DM_LAST_UPDT_DT, "
    f"MED_MGT_NOTE_DTM = S.MED_MGT_NOTE_DTM, "
    f"UM_CRT_DT = S.UM_CRT_DT, "
    f"UM_FINL_CARE_DT = S.UM_FINL_CARE_DT, "
    f"UM_ACTVTY_SEQ_NO = S.UM_ACTVTY_SEQ_NO, "
    f"CASE_MGT_ID = S.CASE_MGT_ID, "
    f"GRP_ID = S.GRP_ID, "
    f"MBR_ID = S.MBR_ID, "
    f"MBR_FIRST_NM = S.MBR_FIRST_NM, "
    f"MBR_MIDINIT = S.MBR_MIDINIT, "
    f"MBR_LAST_NM = S.MBR_LAST_NM, "
    f"MBR_GNDR_CD = S.MBR_GNDR_CD, "
    f"MBR_GNDR_NM = S.MBR_GNDR_NM, "
    f"MBR_RELSHP_CD = S.MBR_RELSHP_CD, "
    f"MBR_RELSHP_NM = S.MBR_RELSHP_NM, "
    f"MBR_SFX_NO = S.MBR_SFX_NO, "
    f"PRI_DIAG_CD = S.PRI_DIAG_CD, "
    f"PROD_ID = S.PROD_ID, "
    f"SUBGRP_ID = S.SUBGRP_ID, "
    f"SUB_ID = S.SUB_ID, "
    f"LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"SRC_SYS_CD, UM_REF_ID, UM_CLS_PLN_PROD_CAT_CD, UM_CLS_PLN_PROD_CAT_NM, UM_QLTY_OF_SVC_LVL_CD, UM_QLTY_OF_SVC_LVL_NM, "
    f"UM_RISK_LVL_CD, UM_RISK_LVL_NM, UM_IP_RCRD_IN, DM_LAST_UPDT_DT, MED_MGT_NOTE_DTM, UM_CRT_DT, UM_FINL_CARE_DT, "
    f"UM_ACTVTY_SEQ_NO, CASE_MGT_ID, GRP_ID, MBR_ID, MBR_FIRST_NM, MBR_MIDINIT, MBR_LAST_NM, MBR_GNDR_CD, MBR_GNDR_NM, "
    f"MBR_RELSHP_CD, MBR_RELSHP_NM, MBR_SFX_NO, PRI_DIAG_CD, PROD_ID, SUBGRP_ID, SUB_ID, LAST_UPDT_RUN_CYC_NO"
    f") VALUES ("
    f"S.SRC_SYS_CD, S.UM_REF_ID, S.UM_CLS_PLN_PROD_CAT_CD, S.UM_CLS_PLN_PROD_CAT_NM, S.UM_QLTY_OF_SVC_LVL_CD, S.UM_QLTY_OF_SVC_LVL_NM, "
    f"S.UM_RISK_LVL_CD, S.UM_RISK_LVL_NM, S.UM_IP_RCRD_IN, S.DM_LAST_UPDT_DT, S.MED_MGT_NOTE_DTM, S.UM_CRT_DT, S.UM_FINL_CARE_DT, "
    f"S.UM_ACTVTY_SEQ_NO, S.CASE_MGT_ID, S.GRP_ID, S.MBR_ID, S.MBR_FIRST_NM, S.MBR_MIDINIT, S.MBR_LAST_NM, S.MBR_GNDR_CD, S.MBR_GNDR_NM, "
    f"S.MBR_RELSHP_CD, S.MBR_RELSHP_NM, S.MBR_SFX_NO, S.PRI_DIAG_CD, S.PROD_ID, S.SUBGRP_ID, S.SUB_ID, S.LAST_UPDT_RUN_CYC_NO"
    f");"
)

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)