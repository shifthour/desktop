# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwMedMgtExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Pulls UM data from IDS to EDW.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   ---------------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tao Luo                          3/1/2006          MedMgmt/                       Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                2/08/2007        MedMgmt/                        Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                    Steph Goddard             01/23/2008  
# MAGIC                                                                                                          rule based upon which we extract records now.  
# MAGIC                                                                                                          Added Hash.Clear and checked null's for lkups
# MAGIC Oliver Nielsen                 8/20/2008         Facets 4.5.1                    Took out the hash clear in this job since hashed files are used in
# MAGIC                                                                                                          the UM_SVC_F extract                                                                              devlEDWnew             Brent Leland                 08-20-2008
# MAGIC Ralph Tucker                 5/11/2012       4896 Edw Remediation     Added new Diag_Cd_Typ fields                                                              IntegrateNewDevl          SAndrew                        2012-05-16
# MAGIC  
# MAGIC Sethuraman R              10/03/2018             5569-                               To add "UM Alternate Ref ID" as part of Oncology UM                     IntegrateDev1                 Kalyan Neelam                2018-10-05
# MAGIC                                                                        Oncology UM

# MAGIC Do NOT clear hashed files at end of job.  They are used in UM_SVC_F extract job
# MAGIC hf_um_d stores processed fields for EdwUmIpFExtr and EdwUmSvcFExtr so the values do not have to be reprocessed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrRunDt = get_widget_value('CurrRunDt', '2006-03-01')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
ExtractRunCycle = get_widget_value('ExtractRunCycle', '100')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
 UM.UM_SK as UM_SK,
 UM.SRC_SYS_CD_SK as SRC_SYS_CD_SK, 
 UM.UM_REF_ID as UM_REF_ID, 
 UM.CASE_MGT_SK as CASE_MGT_SK, 
 UM.CRT_USER_SK as CRT_USER_SK, 
 UM.GRP_SK as GRP_SK, 
 UM.MBR_SK as MBR_SK, 
 UM.PRI_DIAG_CD_SK as PRI_DIAG_CD_SK, 
 UM.PRI_RESP_USER_SK as PRI_RESP_USER_SK, 
 UM.PROD_SK as PROD_SK, 
 UM.SUBGRP_SK as SUBGRP_SK, 
 UM.SUB_SK as SUB_SK, 
 UM.UM_CLS_PLN_PROD_CAT_CD_SK as UM_CLS_PLN_PROD_CAT_CD_SK, 
 UM.QLTY_OF_SVC_LVL_CD_SK as QLTY_OF_SVC_LVL_CD_SK, 
 UM.RISK_LVL_CD_SK as RISK_LVL_CD_SK, 
 UM.IP_RCRD_IN as IP_RCRD_IN, 
 UM.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM, 
 UM.ATCHMT_SRC_DTM as ATCHMT_SRC_DTM, 
 UM.CRT_DT_SK as CRT_DT_SK, 
 UM.FINL_CARE_DT_SK as FINL_CARE_DT_SK, 
 UM.ACTVTY_SEQ_NO as ACTVTY_SEQ_NO, 
 MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY, 
 CASE_MGT.CASE_MGT_ID as CASE_MGT_ID, 
 GRP.GRP_ID as GRP_ID, 
 DIAG_CD.DIAG_CD as DIAG_CD, 
 SUBGRP.SUBGRP_ID as SUBGRP_ID, 
 SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY, 
 UM.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK, 
 UM.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,  
 UM.ALT_UM_REF_ID
FROM {IDSOwner}.UM UM,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.CASE_MGT CASE_MGT,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.DIAG_CD DIAG_CD,
     {IDSOwner}.SUBGRP SUBGRP,
     {IDSOwner}.SUB SUB
WHERE UM.MBR_SK = MBR.MBR_SK
  AND UM.CASE_MGT_SK = CASE_MGT.CASE_MGT_SK
  AND UM.GRP_SK = GRP.GRP_SK
  AND UM.PRI_DIAG_CD_SK = DIAG_CD.DIAG_CD_SK
  AND UM.SUBGRP_SK = SUBGRP.SUBGRP_SK
  AND UM.SUB_SK = SUB.SUB_SK
  AND UM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""
    )
    .load()
)

df_IDS_Extract_App_User = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT APP_USER.USER_SK as USER_SK,
       APP_USER.USER_ID as USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""
    )
    .load()
)

df_IDS_Product = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT UM.UM_SK,
       PROD.PROD_ID
FROM {IDSOwner}.UM UM, 
     {IDSOwner}.PROD PROD
WHERE UM.PROD_SK = PROD.PROD_SK
"""
    )
    .load()
)

df_IDS_lodDiagCdTyp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DIAG_CD_SK,
       DIAG_CD_TYP_CD,
       MAP.TRGT_CD as DIAG_CD_TYP_CD_NM,
       DIAG_CD_TYP_CD_SK as DIAG_CD_TYP_CD_SK
FROM {IDSOwner}.DIAG_CD,
     {IDSOwner}.CD_MPPNG MAP
WHERE DIAG_CD_TYP_CD_SK = MAP.CD_MPPNG_SK
"""
    )
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# Scenario A for hf_um_app_user
df_hf_um_app_user = df_IDS_Extract_App_User.dropDuplicates(["USER_SK"])

# Scenario A for hf_um_prod
df_hf_um_prod = df_IDS_Product.dropDuplicates(["UM_SK"]).withColumnRenamed("UM_SK", "UN_SK")

# Scenario A for hf_um_diag_cd_typ
df_hf_um_diag_cd_typ = df_IDS_lodDiagCdTyp.dropDuplicates(["DIAG_CD_SK"])

df_enriched = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_Sys_Cd_Sk_Lookup"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("Src_Sys_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_CLS_PLN_PROD_CAT_CD_SK")
        == F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup"),
        F.col("Extract.QLTY_OF_SVC_LVL_CD_SK")
        == F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Risk_Lvl_Cd_Sk_Lookup"),
        F.col("Extract.RISK_LVL_CD_SK")
        == F.col("Risk_Lvl_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_um_app_user.alias("Crt_User_Sk_Lookup"),
        F.col("Extract.CRT_USER_SK") == F.col("Crt_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_app_user.alias("Pri_Resp_User_Sk_Lookup"),
        F.col("Extract.PRI_RESP_USER_SK") == F.col("Pri_Resp_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_prod.alias("Prod"),
        F.col("Extract.UM_SK") == F.col("Prod.UN_SK"),
        "left",
    )
    .join(
        df_hf_um_diag_cd_typ.alias("refDiagCdTyp"),
        F.col("Extract.PRI_DIAG_CD_SK") == F.col("refDiagCdTyp.DIAG_CD_SK"),
        "left",
    )
)

# First output (Load) -> UM_D_dat
df_UM_D_dat = (
    df_enriched.withColumn(
        "UM_SK",
        F.col("Extract.UM_SK"),
    )
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.isnull(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD"))
            | (F.length(trim(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD")),
    )
    .withColumn("UM_REF_ID", F.col("Extract.UM_REF_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunDt))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunDt))
    .withColumn("CASE_MGT_SK", F.col("Extract.CASE_MGT_SK"))
    .withColumn("CRT_USER_SK", F.col("Extract.CRT_USER_SK"))
    .withColumn("GRP_SK", F.col("Extract.GRP_SK"))
    .withColumn("MBR_SK", F.col("Extract.MBR_SK"))
    .withColumn("DIAG_CD_SK", F.col("Extract.PRI_DIAG_CD_SK"))
    .withColumn("PRI_RESP_USER_SK", F.col("Extract.PRI_RESP_USER_SK"))
    .withColumn("PROD_SK", F.col("Extract.PROD_SK"))
    .withColumn("SUBGRP_SK", F.col("Extract.SUBGRP_SK"))
    .withColumn("SUB_SK", F.col("Extract.SUB_SK"))
    .withColumn(
        "UM_CLS_PLN_PROD_CAT_CD",
        F.when(
            F.isnull(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD"))
            | (F.length(trim(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD")),
    )
    .withColumn(
        "UM_CLS_PLN_PROD_CAT_NM",
        F.when(
            F.isnull(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))
            | (F.length(trim(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Um_Cls_Pln_Prod_Cat_Cd_Sk_Lookup.TRGT_CD_NM")),
    )
    .withColumn(
        "UM_QLTY_OF_SVC_LVL_CD",
        F.when(
            F.isnull(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD"))
            | (F.length(trim(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD")),
    )
    .withColumn(
        "UM_QLTY_OF_SVC_LVL_NM",
        F.when(
            F.isnull(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD_NM"))
            | (F.length(trim(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Qlty_Of_Svc_Lvl_Cd_Sk_Lookup.TRGT_CD_NM")),
    )
    .withColumn(
        "UM_RISK_LVL_CD",
        F.when(
            F.isnull(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD"))
            | (F.length(trim(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD")),
    )
    .withColumn(
        "UM_RISK_LVL_NM",
        F.when(
            F.isnull(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD_NM"))
            | (F.length(trim(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Risk_Lvl_Cd_Sk_Lookup.TRGT_CD_NM")),
    )
    .withColumn("UM_IP_RCRD_IN", F.col("Extract.IP_RCRD_IN"))
    .withColumn("MED_MGT_NOTE_DTM", F.col("Extract.MED_MGT_NOTE_DTM"))
    .withColumn("UM_ATCHMT_SRC_DTM", F.col("Extract.ATCHMT_SRC_DTM"))
    .withColumn("UM_CRT_DT_SK", F.col("Extract.CRT_DT_SK"))
    .withColumn("UM_FINL_CARE_DT_SK", F.col("Extract.FINL_CARE_DT_SK"))
    .withColumn("UM_ACTVTY_SEQ_NO", F.col("Extract.ACTVTY_SEQ_NO"))
    .withColumn("MBR_UNIQ_KEY", F.col("Extract.MBR_UNIQ_KEY"))
    .withColumn("CASE_MGT_ID", F.col("Extract.CASE_MGT_ID"))
    .withColumn(
        "CRT_USER_ID",
        F.when(
            F.isnull(F.col("Crt_User_Sk_Lookup.USER_ID"))
            | (F.length(trim(F.col("Crt_User_Sk_Lookup.USER_ID"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Crt_User_Sk_Lookup.USER_ID")),
    )
    .withColumn("GRP_ID", F.col("Extract.GRP_ID"))
    .withColumn("PRI_DIAG_CD", F.col("Extract.DIAG_CD"))
    .withColumn(
        "PRI_RESP_USER_ID",
        F.when(
            F.isnull(F.col("Pri_Resp_User_Sk_Lookup.USER_ID"))
            | (F.length(trim(F.col("Pri_Resp_User_Sk_Lookup.USER_ID"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("Pri_Resp_User_Sk_Lookup.USER_ID")),
    )
    .withColumn(
        "PROD_ID",
        F.when(F.isnull(F.col("Prod.PROD_ID")), F.lit("UNK")).otherwise(F.col("Prod.PROD_ID")),
    )
    .withColumn("SUBGRP_ID", F.col("Extract.SUBGRP_ID"))
    .withColumn("SUB_UNIQ_KEY", F.col("Extract.SUB_UNIQ_KEY"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("Extract.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("UM_CLS_PLN_PROD_CAT_CD_SK", F.col("Extract.UM_CLS_PLN_PROD_CAT_CD_SK"))
    .withColumn("UM_QLTY_OF_SVC_LVL_CD_SK", F.col("Extract.QLTY_OF_SVC_LVL_CD_SK"))
    .withColumn("UM_RISK_LVL_CD_SK", F.col("Extract.RISK_LVL_CD_SK"))
    .withColumn(
        "PRI_DIAG_CD_TYP_CD",
        F.when(
            F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD")), F.lit("UNK")
        ).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD")),
    )
    .withColumn(
        "PRI_DIAG_CD_TYP_NM",
        F.when(
            F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_NM")), F.lit("UNK")
        ).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_NM")),
    )
    .withColumn(
        "PRI_DIAG_CD_TYP_CD_SK",
        F.when(
            F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_SK")), F.lit(0)
        ).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_SK")),
    )
    .withColumn(
        "ALT_UM_REF_ID",
        F.when(
            F.length(
                trim(
                    F.when(
                        F.isnull(F.col("Extract.ALT_UM_REF_ID")),
                        F.lit(""),
                    ).otherwise(F.col("Extract.ALT_UM_REF_ID"))
                )
            )
            == 0,
            F.lit("UNK"),
        ).otherwise(F.col("Extract.ALT_UM_REF_ID")),
    )
)

df_UM_D_dat = (
    df_UM_D_dat.withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "),
    )
    .withColumn("UM_IP_RCRD_IN", F.rpad(F.col("UM_IP_RCRD_IN"), 1, " "))
    .withColumn("UM_CRT_DT_SK", F.rpad(F.col("UM_CRT_DT_SK"), 10, " "))
    .withColumn("UM_FINL_CARE_DT_SK", F.rpad(F.col("UM_FINL_CARE_DT_SK"), 10, " "))
)

df_UM_D_dat = df_UM_D_dat.select(
    "UM_SK",
    "SRC_SYS_CD",
    "UM_REF_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CASE_MGT_SK",
    "CRT_USER_SK",
    "GRP_SK",
    "MBR_SK",
    "DIAG_CD_SK",
    "PRI_RESP_USER_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "UM_CLS_PLN_PROD_CAT_CD",
    "UM_CLS_PLN_PROD_CAT_NM",
    "UM_QLTY_OF_SVC_LVL_CD",
    "UM_QLTY_OF_SVC_LVL_NM",
    "UM_RISK_LVL_CD",
    "UM_RISK_LVL_NM",
    "UM_IP_RCRD_IN",
    "MED_MGT_NOTE_DTM",
    "UM_ATCHMT_SRC_DTM",
    "UM_CRT_DT_SK",
    "UM_FINL_CARE_DT_SK",
    "UM_ACTVTY_SEQ_NO",
    "MBR_UNIQ_KEY",
    "CASE_MGT_ID",
    "CRT_USER_ID",
    "GRP_ID",
    "PRI_DIAG_CD",
    "PRI_RESP_USER_ID",
    "PROD_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_CLS_PLN_PROD_CAT_CD_SK",
    "UM_QLTY_OF_SVC_LVL_CD_SK",
    "UM_RISK_LVL_CD_SK",
    "PRI_DIAG_CD_TYP_CD",
    "PRI_DIAG_CD_TYP_NM",
    "PRI_DIAG_CD_TYP_CD_SK",
    "ALT_UM_REF_ID",
)

write_files(
    df_UM_D_dat,
    f"{adls_path}/load/UM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None,
)

# Second output (Load_Hash) -> hf_um_d (Scenario C)
df_hf_um_d = (
    df_enriched.withColumn("UM_SK", F.col("Extract.UM_SK"))
    .withColumn("CASE_MGT_SK", F.col("Extract.CASE_MGT_SK"))
    .withColumn("CRT_USER_SK", F.col("Extract.CRT_USER_SK"))
    .withColumn("GRP_SK", F.col("Extract.GRP_SK"))
    .withColumn("MBR_SK", F.col("Extract.MBR_SK"))
    .withColumn("PRI_RESP_USER_SK", F.col("Extract.PRI_RESP_USER_SK"))
    .withColumn("PROD_SK", F.col("Extract.PROD_SK"))
    .withColumn("SUBGRP_SK", F.col("Extract.SUBGRP_SK"))
    .withColumn("SUB_SK", F.col("Extract.SUB_SK"))
    .withColumn("MED_MGT_NOTE_DTM", F.col("Extract.MED_MGT_NOTE_DTM"))
    .withColumn("CRT_USER_ID", F.col("Crt_User_Sk_Lookup.USER_ID"))
    .withColumn("GRP_ID", F.col("Extract.GRP_ID"))
    .withColumn("MBR_UNIQ_KEY", F.col("Extract.MBR_UNIQ_KEY"))
    .withColumn("PRI_RESP_USER_ID", F.col("Pri_Resp_User_Sk_Lookup.USER_ID"))
    .withColumn(
        "PROD_ID",
        F.when(F.isnull(F.col("Prod.PROD_ID")), F.lit("UNK")).otherwise(F.col("Prod.PROD_ID")),
    )
    .withColumn("SUBGRP_ID", F.col("Extract.SUBGRP_ID"))
    .withColumn("SUB_UNIQ_KEY", F.col("Extract.SUB_UNIQ_KEY"))
    .withColumn(
        "PRI_DIAG_CD_TYP_CD",
        F.when(F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD")), F.lit("UNK")).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD")),
    )
    .withColumn(
        "PRI_DIAG_CD_TYP_NM",
        F.when(F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_NM")), F.lit("UNK")).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_NM")),
    )
    .withColumn(
        "PRI_DIAG_CD_TYP_CD_SK",
        F.when(F.isnull(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_SK")), F.lit(0)).otherwise(F.col("refDiagCdTyp.DIAG_CD_TYP_CD_SK")),
    )
)

df_hf_um_d = df_hf_um_d.select(
    "UM_SK",
    "CASE_MGT_SK",
    "CRT_USER_SK",
    "GRP_SK",
    "MBR_SK",
    "PRI_RESP_USER_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "MED_MGT_NOTE_DTM",
    "CRT_USER_ID",
    "GRP_ID",
    "MBR_UNIQ_KEY",
    "PRI_RESP_USER_ID",
    "PROD_ID",
    "SUBGRP_ID",
    "SUB_UNIQ_KEY",
    "PRI_DIAG_CD_TYP_CD",
    "PRI_DIAG_CD_TYP_NM",
    "PRI_DIAG_CD_TYP_CD_SK",
)

write_files(
    df_hf_um_d,
    f"{adls_path}/hf_um_d.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None,
)