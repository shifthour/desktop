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
# MAGIC ^1_7 06/28/06 09:07:27 Batch  14059_32851 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_6 06/28/06 09:02:32 Batch  14059_32558 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_5 06/20/06 16:37:06 Batch  14051_59831 PROMOTE bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_5 06/20/06 16:35:44 Batch  14051_59749 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_4 06/20/06 16:34:07 Batch  14051_59651 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:16:37 Batch  14050_40618 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:10:31 Batch  14050_40241 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:02:44 Batch  14050_39770 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_2 06/07/06 12:19:56 Batch  14038_44402 PROMOTE bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_2 06/07/06 12:18:53 Batch  14038_44336 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/01/06 10:52:24 Batch  14032_39155 INIT bckcett devlIDS30 u10913 Ollie moving Med Mgt from Devl to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsDataMartMedMgtDmUmSvcExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                 Pulls data from IDS to create medical management data mart  Extract is based on BeginRunCycle.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - UM_SVC
# MAGIC                
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC 
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     direct update of MED_MGT_DM_UM_SVC_D
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  02/23/2006 -   Originally Programmed
# MAGIC               Tao Luo          06/23/2006 -   Added Run Cycle Logic and Join to UM

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
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginRunCycle = get_widget_value('BeginRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDSUmSvc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT UM_SVC.UM_SVC_SK as UM_SVC_SK,UM_SVC.SRC_SYS_CD_SK as SRC_SYS_CD_SK,UM_SVC.UM_REF_ID as UM_REF_ID,UM_SVC.UM_SVC_SEQ_NO as UM_SVC_SEQ_NO,UM_SVC.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,UM_SVC.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,UM_SVC.DSALW_EXCD_SK as DSALW_EXCD_SK,UM_SVC.FCLTY_PROV_SK as FCLTY_PROV_SK,UM_SVC.INPT_USER_SK as INPT_USER_SK,UM_SVC.MBR_SK as MBR_SK,UM_SVC.PCP_PROV_SK as PCP_PROV_SK,UM_SVC.PRI_DIAG_CD_SK as PRI_DIAG_CD_SK,UM_SVC.PROC_CD_SK as PROC_CD_SK,UM_SVC.RQST_PROV_SK as RQST_PROV_SK,UM_SVC.SVC_PROV_SK as SVC_PROV_SK,UM_SVC.UM_SK as UM_SK,UM_SVC.UM_SVC_AUTH_POS_CAT_CD_SK as UM_SVC_AUTH_POS_CAT_CD_SK,UM_SVC.UM_SVC_AUTH_POS_TYP_CD_SK as UM_SVC_AUTH_POS_TYP_CD_SK,UM_SVC.UM_SVC_DENIAL_RSN_CD_SK as UM_SVC_DENIAL_RSN_CD_SK,UM_SVC.UM_SVC_RQST_POS_TYP_CD_SK as UM_SVC_RQST_POS_TYP_CD_SK,UM_SVC.UM_SVC_STTUS_CD_SK as UM_SVC_STTUS_CD_SK,UM_SVC.UM_SVC_TREAT_CAT_CD_SK as UM_SVC_TREAT_CAT_CD_SK,UM_SVC.UM_SVC_TYP_CD_SK as UM_SVC_TYP_CD_SK,UM_SVC.UM_SVC_TOS_CD_SK as UM_SVC_TOS_CD_SK,UM_SVC.PREAUTH_IN as PREAUTH_IN,UM_SVC.RFRL_IN as RFRL_IN,UM_SVC.ATCHMT_SRC_DTM as ATCHMT_SRC_DTM,UM_SVC.AUTH_DT_SK as AUTH_DT_SK,UM_SVC.DENIAL_DT_SK as DENIAL_DT_SK,UM_SVC.INPT_DT_SK as INPT_DT_SK,UM_SVC.NEXT_RVW_DT_SK as NEXT_RVW_DT_SK,UM_SVC.RCVD_DT_SK as RCVD_DT_SK,UM_SVC.SVC_END_DT_SK as SVC_END_DT_SK,UM_SVC.SVC_STRT_DT_SK as SVC_STRT_DT_SK,UM_SVC.ALW_AMT as ALW_AMT,UM_SVC.PD_AMT as PD_AMT,UM_SVC.ALW_UNIT_CT as ALW_UNIT_CT,UM_SVC.AUTH_UNIT_CT as AUTH_UNIT_CT,UM_SVC.MBR_AGE as MBR_AGE,UM_SVC.PD_UNIT_CT as PD_UNIT_CT,UM_SVC.RQST_UNIT_CT as RQST_UNIT_CT,UM_SVC.STTUS_SEQ_NO as STTUS_SEQ_NO,UM_SVC.PROC_CD_MOD_TX as PROC_CD_MOD_TX,UM_SVC.SVC_GRP_ID as SVC_GRP_ID,UM_SVC.SVC_RULE_TYP_TX as SVC_RULE_TYP_TX "
        f"FROM {IDSOwner}.UM_SVC UM_SVC, {IDSOwner}.UM UM "
        f"WHERE UM_SVC.UM_REF_ID = UM.UM_REF_ID "
        f"AND UM_SVC.SRC_SYS_CD_SK = UM.SRC_SYS_CD_SK "
        f"AND UM_SVC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginRunCycle} "
        f"AND UM_SVC.UM_SVC_SK NOT IN (0,1)\n\n{IDSOwner}.UM_SVC UM_SVC, {IDSOwner}.UM UM"
    )
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_refSrcSys = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refDenial = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refSvcTypCd = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refAuthPosTypCd = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refTOS = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refCurTreat = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refRqstPosCat = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")
df_refSttus = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","SRC_CD","SRC_CD_NM")

jdbc_url_ids_diag, jdbc_props_ids_diag = get_db_config(ids_secret_name)
df_lnkProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("query", f"SELECT PROV_SK,PROV_ID FROM {IDSOwner}.PROV")
    .load()
)
df_lnkDiagBuild = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("query", f"SELECT DIAG_CD_SK,DIAG_CD FROM {IDSOwner}.DIAG_CD")
    .load()
)
df_lnkExCdBuild = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("query", f"SELECT EXCD_SK,EXCD_ID FROM {IDSOwner}.EXCD")
    .load()
)
df_lnkGrpBuild = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("query", f"SELECT UM.UM_REF_ID as UM_REF_ID,GRP.GRP_ID as GRP_ID FROM {IDSOwner}.UM UM, {IDSOwner}.GRP GRP WHERE UM.GRP_SK=GRP.GRP_SK")
    .load()
)
df_lnkProcBuild = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("query", f"SELECT PROC_CD_SK,PROC_CD FROM {IDSOwner}.PROC_CD")
    .load()
)
df_lnkMbrBuild = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT MBR.MBR_SK as MBR_SK,MBR.MBR_SFX_NO as MBR_SFX_NO,SUB.SUB_ID as SUB_ID FROM {IDSOwner}.MBR MBR,{IDSOwner}.SUB SUB WHERE MBR.SUB_SK=SUB.SUB_SK"
    )
    .load()
)

tmp_lnkProv = dedup_sort(df_lnkProv, ["PROV_SK"], [])
df_lnkProv2 = tmp_lnkProv
df_lnkProv3 = tmp_lnkProv
df_lnkDiag = dedup_sort(df_lnkDiagBuild, ["DIAG_CD_SK"], [])
df_lnkExCd = dedup_sort(df_lnkExCdBuild, ["EXCD_SK"], [])
df_lnkGrp = dedup_sort(df_lnkGrpBuild, ["UM_REF_ID"], [])
df_lnkProc = dedup_sort(df_lnkProcBuild, ["PROC_CD_SK"], [])
df_lnkMbr = dedup_sort(df_lnkMbrBuild, ["MBR_SK"], [])

df_Trans1 = df_IDSUmSvc.alias("lnkUmSvc") \
    .join(df_refSrcSys.alias("refSrcSys"), trim(col("lnkUmSvc.SRC_SYS_CD_SK")) == col("refSrcSys.CD_MPPNG_SK"), "left") \
    .join(df_refDenial.alias("refDenial"), trim(col("lnkUmSvc.UM_SVC_DENIAL_RSN_CD_SK")) == col("refDenial.CD_MPPNG_SK"), "left") \
    .join(df_refSvcTypCd.alias("refSvcTypCd"), trim(col("lnkUmSvc.UM_SVC_TYP_CD_SK")) == col("refSvcTypCd.CD_MPPNG_SK"), "left") \
    .join(df_refAuthPosTypCd.alias("refAuthPosTypCd"), trim(col("lnkUmSvc.UM_SVC_AUTH_POS_TYP_CD_SK")) == col("refAuthPosTypCd.CD_MPPNG_SK"), "left") \
    .join(df_refTOS.alias("refTOS"), trim(col("lnkUmSvc.UM_SVC_TOS_CD_SK")) == col("refTOS.CD_MPPNG_SK"), "left") \
    .join(df_refCurTreat.alias("refCurTreat"), trim(col("lnkUmSvc.UM_SVC_TREAT_CAT_CD_SK")) == col("refCurTreat.CD_MPPNG_SK"), "left") \
    .join(df_refRqstPosCat.alias("refRqstPosCat"), trim(col("lnkUmSvc.UM_SVC_RQST_POS_TYP_CD_SK")) == col("refRqstPosCat.CD_MPPNG_SK"), "left") \
    .join(df_refSttus.alias("refSttus"), trim(col("lnkUmSvc.UM_SVC_STTUS_CD_SK")) == col("refSttus.CD_MPPNG_SK"), "left") \
    .join(df_lnkDiag.alias("lnkDiag"), col("lnkUmSvc.PRI_DIAG_CD_SK") == col("lnkDiag.DIAG_CD_SK"), "left") \
    .join(df_lnkExCd.alias("lnkExCd"), col("lnkUmSvc.DSALW_EXCD_SK") == col("lnkExCd.EXCD_SK"), "left") \
    .join(df_lnkGrp.alias("lnkGrp"), trim(col("lnkUmSvc.UM_REF_ID")) == col("lnkGrp.UM_REF_ID"), "left") \
    .join(df_lnkProc.alias("lnkProc"), col("lnkUmSvc.PROC_CD_SK") == col("lnkProc.PROC_CD_SK"), "left") \
    .join(df_lnkProv2.alias("lnkProv2"), col("lnkUmSvc.RQST_PROV_SK") == col("lnkProv2.PROV_SK"), "left") \
    .join(df_lnkProv3.alias("lnkProv3"), col("lnkUmSvc.SVC_PROV_SK") == col("lnkProv3.PROV_SK"), "left") \
    .join(df_lnkMbr.alias("lnkMbr"), col("lnkUmSvc.MBR_SK") == col("lnkMbr.MBR_SK"), "left")

df_Trans1 = df_Trans1.filter(
    (col("lnkMbr.SUB_ID").isNotNull()) & (col("lnkMbr.MBR_SFX_NO").isNotNull())
)

df_OutFile = df_Trans1.select(
    rpad(col("refSrcSys.TRGT_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("lnkUmSvc.UM_REF_ID"), <...>, " ").alias("UM_REF_ID"),
    rpad(col("lnkUmSvc.UM_SVC_SEQ_NO"), <...>, " ").alias("UM_SVC_SEQ_NO"),
    rpad(col("refAuthPosTypCd.TRGT_CD"), <...>, " ").alias("UM_SVC_AUTH_POS_TYP_CD"),
    rpad(col("refAuthPosTypCd.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_AUTH_POS_TYP_NM"),
    rpad(col("refDenial.TRGT_CD"), <...>, " ").alias("UM_SVC_DENIAL_RSN_CD"),
    rpad(col("refDenial.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_DENIAL_RSN_NM"),
    rpad(col("refRqstPosCat.TRGT_CD"), <...>, " ").alias("UM_SVC_RQST_POS_TYP_CD"),
    rpad(col("refRqstPosCat.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_RQST_POS_TYP_NM"),
    rpad(col("refSttus.TRGT_CD"), <...>, " ").alias("UM_SVC_STTUS_CD"),
    rpad(col("refSttus.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_STTUS_NM"),
    rpad(col("refCurTreat.TRGT_CD"), <...>, " ").alias("UM_SVC_TREAT_CAT_CD"),
    rpad(col("refCurTreat.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_TREAT_CAT_NM"),
    rpad(col("refSvcTypCd.TRGT_CD"), <...>, " ").alias("UM_SVC_TYP_CD"),
    rpad(col("refSvcTypCd.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_TYP_NM"),
    rpad(col("refTOS.TRGT_CD"), <...>, " ").alias("UM_SVC_TOS_CD"),
    rpad(col("refTOS.TRGT_CD_NM"), <...>, " ").alias("UM_SVC_TOS_NM"),
    rpad(col("lnkUmSvc.PREAUTH_IN"), 1, " ").alias("UM_SVC_PREAUTH_IN"),
    rpad(col("lnkUmSvc.RFRL_IN"), 1, " ").alias("UM_SVC_RFRL_IN"),
    rpad(
        when(
            (trim(col("lnkUmSvc.AUTH_DT_SK")) == "NA")
            | (trim(col("lnkUmSvc.AUTH_DT_SK")) == "UNK"),
            None,
        ).otherwise(col("lnkUmSvc.AUTH_DT_SK")),
        <...>,
        " ",
    ).alias("UM_SVC_AUTH_DT"),
    rpad(
        when(
            (trim(col("lnkUmSvc.DENIAL_DT_SK")) == "NA")
            | (trim(col("lnkUmSvc.DENIAL_DT_SK")) == "UNK"),
            None,
        ).otherwise(col("lnkUmSvc.DENIAL_DT_SK")),
        <...>,
        " ",
    ).alias("UM_SVC_DENIAL_DT"),
    rpad(
        when(
            (trim(col("lnkUmSvc.SVC_END_DT_SK")) == "NA")
            | (trim(col("lnkUmSvc.SVC_END_DT_SK")) == "UNK"),
            None,
        ).otherwise(col("lnkUmSvc.SVC_END_DT_SK")),
        <...>,
        " ",
    ).alias("UM_SVC_END_DT"),
    rpad(
        when(
            (trim(col("lnkUmSvc.NEXT_RVW_DT_SK")) == "NA")
            | (trim(col("lnkUmSvc.NEXT_RVW_DT_SK")) == "UNK"),
            None,
        ).otherwise(col("lnkUmSvc.NEXT_RVW_DT_SK")),
        <...>,
        " ",
    ).alias("UM_SVC_NEXT_RVW_DT"),
    rpad(
        when(
            (trim(col("lnkUmSvc.SVC_STRT_DT_SK")) == "NA")
            | (trim(col("lnkUmSvc.SVC_STRT_DT_SK")) == "UNK"),
            None,
        ).otherwise(col("lnkUmSvc.SVC_STRT_DT_SK")),
        <...>,
        " ",
    ).alias("UM_SVC_STRT_DT"),
    rpad(col("lnkUmSvc.ALW_AMT"), <...>, " ").alias("UM_SVC_ALW_AMT"),
    rpad(col("lnkUmSvc.PD_AMT"), <...>, " ").alias("UM_SVC_PD_AMT"),
    rpad(col("lnkUmSvc.ALW_UNIT_CT"), <...>, " ").alias("UM_SVC_ALW_UNIT_CT"),
    rpad(col("lnkUmSvc.AUTH_UNIT_CT"), <...>, " ").alias("UM_SVC_AUTH_UNIT_CT"),
    rpad(col("lnkUmSvc.PD_UNIT_CT"), <...>, " ").alias("UM_SVC_PD_UNIT_CT"),
    rpad(col("lnkUmSvc.RQST_UNIT_CT"), <...>, " ").alias("UM_SVC_RQST_UNIT_CT"),
    rpad(col("lnkUmSvc.STTUS_SEQ_NO"), <...>, " ").alias("UM_SVC_STTUS_SEQ_NO"),
    rpad(col("lnkExCd.EXCD_ID"), 4, " ").alias("DSALW_EXCD_ID"),
    rpad(
        when(col("lnkGrp.GRP_ID").isNull(), "UNK").otherwise(col("lnkGrp.GRP_ID")),
        <...>,
        " ",
    ).alias("GRP_ID"),
    rpad(
        col("lnkMbr.SUB_ID") + col("lnkMbr.MBR_SFX_NO"),
        <...>,
        " ",
    ).alias("MBR_ID"),
    rpad(col("lnkDiag.DIAG_CD"), <...>, " ").alias("PRI_DIAG_CD"),
    rpad(col("lnkProc.PROC_CD"), 5, " ").alias("PROC_CD"),
    rpad(
        when(col("lnkProv2.PROV_ID").isNull(), "0").otherwise(col("lnkProv2.PROV_ID")),
        <...>,
        " ",
    ).alias("RQST_PROV_ID"),
    rpad(
        when(col("lnkProv3.PROV_ID").isNull(), "0").otherwise(col("lnkProv3.PROV_ID")),
        <...>,
        " ",
    ).alias("SVC_PROV_ID"),
    rpad(col("CurrRunCycle"), <...>, " ").alias("LAST_UPDT_RUN_CYC_NO"),
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmUmSvcExtr_MED_MGT_DM_UM_SVC_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)
df_OutFile.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmUmSvcExtr_MED_MGT_DM_UM_SVC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MED_MGT_DM_UM_SVC AS T
USING STAGING.IdsDataMartMedMgtDmUmSvcExtr_MED_MGT_DM_UM_SVC_temp AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.UM_REF_ID = S.UM_REF_ID
  AND T.UM_SVC_SEQ_NO = S.UM_SVC_SEQ_NO
)
WHEN MATCHED THEN UPDATE SET
  T.UM_SVC_AUTH_POS_TYP_CD = S.UM_SVC_AUTH_POS_TYP_CD,
  T.UM_SVC_AUTH_POS_TYP_NM = S.UM_SVC_AUTH_POS_TYP_NM,
  T.UM_SVC_DENIAL_RSN_CD = S.UM_SVC_DENIAL_RSN_CD,
  T.UM_SVC_DENIAL_RSN_NM = S.UM_SVC_DENIAL_RSN_NM,
  T.UM_SVC_RQST_POS_TYP_CD = S.UM_SVC_RQST_POS_TYP_CD,
  T.UM_SVC_RQST_POS_TYP_NM = S.UM_SVC_RQST_POS_TYP_NM,
  T.UM_SVC_STTUS_CD = S.UM_SVC_STTUS_CD,
  T.UM_SVC_STTUS_NM = S.UM_SVC_STTUS_NM,
  T.UM_SVC_TREAT_CAT_CD = S.UM_SVC_TREAT_CAT_CD,
  T.UM_SVC_TREAT_CAT_NM = S.UM_SVC_TREAT_CAT_NM,
  T.UM_SVC_TYP_CD = S.UM_SVC_TYP_CD,
  T.UM_SVC_TYP_NM = S.UM_SVC_TYP_NM,
  T.UM_SVC_TOS_CD = S.UM_SVC_TOS_CD,
  T.UM_SVC_TOS_NM = S.UM_SVC_TOS_NM,
  T.UM_SVC_PREAUTH_IN = S.UM_SVC_PREAUTH_IN,
  T.UM_SVC_RFRL_IN = S.UM_SVC_RFRL_IN,
  T.UM_SVC_AUTH_DT = S.UM_SVC_AUTH_DT,
  T.UM_SVC_DENIAL_DT = S.UM_SVC_DENIAL_DT,
  T.UM_SVC_END_DT = S.UM_SVC_END_DT,
  T.UM_SVC_NEXT_RVW_DT = S.UM_SVC_NEXT_RVW_DT,
  T.UM_SVC_STRT_DT = S.UM_SVC_STRT_DT,
  T.UM_SVC_ALW_AMT = S.UM_SVC_ALW_AMT,
  T.UM_SVC_PD_AMT = S.UM_SVC_PD_AMT,
  T.UM_SVC_ALW_UNIT_CT = S.UM_SVC_ALW_UNIT_CT,
  T.UM_SVC_AUTH_UNIT_CT = S.UM_SVC_AUTH_UNIT_CT,
  T.UM_SVC_PD_UNIT_CT = S.UM_SVC_PD_UNIT_CT,
  T.UM_SVC_RQST_UNIT_CT = S.UM_SVC_RQST_UNIT_CT,
  T.UM_SVC_STTUS_SEQ_NO = S.UM_SVC_STTUS_SEQ_NO,
  T.DSALW_EXCD_ID = S.DSALW_EXCD_ID,
  T.GRP_ID = S.GRP_ID,
  T.MBR_ID = S.MBR_ID,
  T.PRI_DIAG_CD = S.PRI_DIAG_CD,
  T.PROC_CD = S.PROC_CD,
  T.RQST_PROV_ID = S.RQST_PROV_ID,
  T.SVC_PROV_ID = S.SVC_PROV_ID,
  T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN INSERT
(
  SRC_SYS_CD,
  UM_REF_ID,
  UM_SVC_SEQ_NO,
  UM_SVC_AUTH_POS_TYP_CD,
  UM_SVC_AUTH_POS_TYP_NM,
  UM_SVC_DENIAL_RSN_CD,
  UM_SVC_DENIAL_RSN_NM,
  UM_SVC_RQST_POS_TYP_CD,
  UM_SVC_RQST_POS_TYP_NM,
  UM_SVC_STTUS_CD,
  UM_SVC_STTUS_NM,
  UM_SVC_TREAT_CAT_CD,
  UM_SVC_TREAT_CAT_NM,
  UM_SVC_TYP_CD,
  UM_SVC_TYP_NM,
  UM_SVC_TOS_CD,
  UM_SVC_TOS_NM,
  UM_SVC_PREAUTH_IN,
  UM_SVC_RFRL_IN,
  UM_SVC_AUTH_DT,
  UM_SVC_DENIAL_DT,
  UM_SVC_END_DT,
  UM_SVC_NEXT_RVW_DT,
  UM_SVC_STRT_DT,
  UM_SVC_ALW_AMT,
  UM_SVC_PD_AMT,
  UM_SVC_ALW_UNIT_CT,
  UM_SVC_AUTH_UNIT_CT,
  UM_SVC_PD_UNIT_CT,
  UM_SVC_RQST_UNIT_CT,
  UM_SVC_STTUS_SEQ_NO,
  DSALW_EXCD_ID,
  GRP_ID,
  MBR_ID,
  PRI_DIAG_CD,
  PROC_CD,
  RQST_PROV_ID,
  SVC_PROV_ID,
  LAST_UPDT_RUN_CYC_NO
)
VALUES
(
  S.SRC_SYS_CD,
  S.UM_REF_ID,
  S.UM_SVC_SEQ_NO,
  S.UM_SVC_AUTH_POS_TYP_CD,
  S.UM_SVC_AUTH_POS_TYP_NM,
  S.UM_SVC_DENIAL_RSN_CD,
  S.UM_SVC_DENIAL_RSN_NM,
  S.UM_SVC_RQST_POS_TYP_CD,
  S.UM_SVC_RQST_POS_TYP_NM,
  S.UM_SVC_STTUS_CD,
  S.UM_SVC_STTUS_NM,
  S.UM_SVC_TREAT_CAT_CD,
  S.UM_SVC_TREAT_CAT_NM,
  S.UM_SVC_TYP_CD,
  S.UM_SVC_TYP_NM,
  S.UM_SVC_TOS_CD,
  S.UM_SVC_TOS_NM,
  S.UM_SVC_PREAUTH_IN,
  S.UM_SVC_RFRL_IN,
  S.UM_SVC_AUTH_DT,
  S.UM_SVC_DENIAL_DT,
  S.UM_SVC_END_DT,
  S.UM_SVC_NEXT_RVW_DT,
  S.UM_SVC_STRT_DT,
  S.UM_SVC_ALW_AMT,
  S.UM_SVC_PD_AMT,
  S.UM_SVC_ALW_UNIT_CT,
  S.UM_SVC_AUTH_UNIT_CT,
  S.UM_SVC_PD_UNIT_CT,
  S.UM_SVC_RQST_UNIT_CT,
  S.UM_SVC_STTUS_SEQ_NO,
  S.DSALW_EXCD_ID,
  S.GRP_ID,
  S.MBR_ID,
  S.PRI_DIAG_CD,
  S.PROC_CD,
  S.RQST_PROV_ID,
  S.SVC_PROV_ID,
  S.LAST_UPDT_RUN_CYC_NO
);
"""
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)