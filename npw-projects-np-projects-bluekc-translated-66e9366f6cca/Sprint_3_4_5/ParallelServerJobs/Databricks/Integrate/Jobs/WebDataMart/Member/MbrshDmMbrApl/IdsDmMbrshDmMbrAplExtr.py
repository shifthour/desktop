# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project          Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------         -------------------------------       -------------------------
# MAGIC Kimberly Doty                 08/17/2009        4113 -  360 Mbr View             Original Programming                                                        devlIDSnew            
# MAGIC Kimberly Doty                 10/08/2009        4113 - 360 Mbr View              Modified to be the daily job                                               devlIDSnew                        Steph Goddard             10/12/2009
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC Bhupinder Kaur               2013-08-06        5114                            Create Load File for  Web Access DM Table                             IntegrateWrhsDevl                Jag Yelavarthi        2013-10-24
# MAGIC                                                                                                        MBRSH_DM_MBR_APL

# MAGIC Write MBRSH_DM_MBR_APL Data into a Sequential file for Load Job IdsDmMbrshDmMbrAplLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC APL_CAT_CD_SK
# MAGIC APL_CUR_DCSN_CD_SK
# MAGIC APL_CUR_STTUS_CD_SK
# MAGIC APL_INITN_METH_CD_SK
# MAGIC APL_SUBTYP_CD_SK
# MAGIC APL_TYP_CD_SK
# MAGIC CUR_APL_LVL_CD_SK
# MAGIC Job Name: IdsDmMbrshDmMbrAplExtr
# MAGIC 
# MAGIC Extracts historical Appeals and daily additions for the datamart table MBRSH_DM_MBR_APL
# MAGIC 
# MAGIC This is the daily job.  It should load any and all appeals changes.  It must run after DSCSD030.
# MAGIC Read from source table APL ,APP_USER and MBR from IDS.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IdsAppealsExtractRunCycle = get_widget_value('IdsAppealsExtractRunCycle','')
CurrDt_Minus30 = get_widget_value('CurrDt_Minus30','')

extract_query_db2_APL_in = f"""
SELECT
apl.APL_ID,
mbr.MBR_UNIQ_KEY,
apl.APL_CAT_CD_SK,
apl.APL_CUR_DCSN_CD_SK,
apl.APL_CUR_STTUS_CD_SK,
apl.APL_INITN_METH_CD_SK,
apl.APL_SUBTYP_CD_SK,
apl.APL_TYP_CD_SK,
apl.CUR_APL_LVL_CD_SK,
apl.CRT_DTM AS APL_CRT_DT,
apl.CUR_STTUS_DTM AS APL_CUR_STTUS_DT,
apl.END_DT_SK AS APL_END_DT,
apl.INITN_DT_SK AS APL_INITN_DT,
apl.APL_DESC,
apl.LAST_UPDT_DTM AS LAST_UPDT_DT,
appU.USER_ID AS LAST_UPDT_USER_ID,
coalesce(CM.TRGT_CD,'UNK') as SRC_SYS_CD
FROM {IDSOwner}.APL apl
JOIN {IDSOwner}.MBR mbr on apl.MBR_SK = mbr.MBR_SK
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CM on apl.SRC_SYS_CD_SK = CM.CD_MPPNG_SK
JOIN {IDSOwner}.APP_USER appU on apl.LAST_UPDT_USER_SK = appU.USER_SK
WHERE apl.SRC_SYS_CD_SK NOT IN (0, 1)
AND apl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsAppealsExtractRunCycle}
"""

jdbc_url_db2_APL_in, jdbc_props_db2_APL_in = get_db_config(ids_secret_name)
df_db2_APL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_APL_in)
    .options(**jdbc_props_db2_APL_in)
    .option("query", extract_query_db2_APL_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""

jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Ref_AplTypCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplInitnMethCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplSubtypCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplCurDcsnCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplCatCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AplCurSttusCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CurAplLvlCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_APL_in.alias("lnk_IdsDmMbrshDmMbrAplExtr_InAbc")
    .join(
        df_Ref_AplCurDcsnCd_Lkp.alias("Ref_AplCurDcsnCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CUR_DCSN_CD_SK")
        == F.col("Ref_AplCurDcsnCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_AplInitnMethCd_Lkp.alias("Ref_AplInitnMethCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_INITN_METH_CD_SK")
        == F.col("Ref_AplInitnMethCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_AplTypCd_Lkp.alias("Ref_AplTypCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_TYP_CD_SK")
        == F.col("Ref_AplTypCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_AplSubtypCd_Lkp.alias("Ref_AplSubtypCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_SUBTYP_CD_SK")
        == F.col("Ref_AplSubtypCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_AplCatCd_Lkp.alias("Ref_AplCatCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CAT_CD_SK")
        == F.col("Ref_AplCatCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_AplCurSttusCd_Lkp.alias("Ref_AplCurSttusCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CUR_STTUS_CD_SK")
        == F.col("Ref_AplCurSttusCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Ref_CurAplLvlCd_Lkp.alias("Ref_CurAplLvlCd_Lkp"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.CUR_APL_LVL_CD_SK")
        == F.col("Ref_CurAplLvlCd_Lkp.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_ID").alias("APL_ID"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Ref_AplCatCd_Lkp.TRGT_CD").alias("APL_CAT_CD"),
        F.col("Ref_AplCatCd_Lkp.TRGT_CD_NM").alias("APL_CAT_NM"),
        F.col("Ref_AplCurDcsnCd_Lkp.TRGT_CD").alias("APL_CUR_DCSN_CD"),
        F.col("Ref_AplCurDcsnCd_Lkp.TRGT_CD_NM").alias("APL_CUR_DCSN_NM"),
        F.col("Ref_AplCurSttusCd_Lkp.TRGT_CD").alias("APL_CUR_STTUS_CD"),
        F.col("Ref_AplCurSttusCd_Lkp.TRGT_CD_NM").alias("APL_CUR_STTUS_NM"),
        F.col("Ref_AplInitnMethCd_Lkp.TRGT_CD").alias("APL_INITN_METH_CD"),
        F.col("Ref_AplInitnMethCd_Lkp.TRGT_CD_NM").alias("APL_INITN_METH_NM"),
        F.col("Ref_AplSubtypCd_Lkp.TRGT_CD").alias("APL_SUBTYP_CD"),
        F.col("Ref_AplSubtypCd_Lkp.TRGT_CD_NM").alias("APL_SUBTYP_NM"),
        F.col("Ref_AplTypCd_Lkp.TRGT_CD").alias("APL_TYP_CD"),
        F.col("Ref_AplTypCd_Lkp.TRGT_CD_NM").alias("APL_TYP_NM"),
        F.col("Ref_CurAplLvlCd_Lkp.TRGT_CD").alias("CUR_APL_LVL_CD"),
        F.col("Ref_CurAplLvlCd_Lkp.TRGT_CD_NM").alias("CUR_APL_LVL_NM"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CRT_DT").alias("APL_CRT_DT"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CUR_STTUS_DT").alias("APL_CUR_STTUS_DT"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_END_DT").alias("APL_END_DT"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_INITN_DT").alias("APL_INITN_DT"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_DESC").alias("APL_DESC"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
        F.col("lnk_IdsDmMbrshDmMbrAplExtr_InAbc.CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
    )
)

df_xfm_BusinessLogic = df_lkp_Codes.select(
    F.col("APL_ID").alias("APL_ID"),
    F.when(
        F.col("SRC_SYS_CD").isNull() | (F.col("SRC_SYS_CD") == ""), "UNK"
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(F.col("APL_CAT_CD_SK").isNull(), "").otherwise(F.col("APL_CAT_CD")).alias("APL_CAT_CD"),
    F.when(F.col("APL_CAT_CD_SK").isNull(), "").otherwise(F.col("APL_CAT_NM")).alias("APL_CAT_NM"),
    F.when(F.col("APL_CUR_DCSN_CD_SK").isNull(), "").otherwise(F.col("APL_CUR_DCSN_CD")).alias("APL_CUR_DCSN_CD"),
    F.when(F.col("APL_CUR_DCSN_CD_SK").isNull(), "").otherwise(F.col("APL_CUR_DCSN_NM")).alias("APL_CUR_DCSN_NM"),
    F.when(F.col("APL_CUR_STTUS_CD_SK").isNull(), "").otherwise(F.col("APL_CUR_STTUS_CD")).alias("APL_CUR_STTUS_CD"),
    F.when(F.col("APL_CUR_STTUS_CD_SK").isNull(), "").otherwise(F.col("APL_CUR_STTUS_NM")).alias("APL_CUR_STTUS_NM"),
    F.when(F.col("APL_INITN_METH_CD_SK").isNull(), "").otherwise(F.col("APL_INITN_METH_CD")).alias("APL_INITN_METH_CD"),
    F.when(F.col("APL_INITN_METH_CD_SK").isNull(), "").otherwise(F.col("APL_INITN_METH_NM")).alias("APL_INITN_METH_NM"),
    F.when(F.col("APL_SUBTYP_CD_SK").isNull(), "").otherwise(F.col("APL_SUBTYP_CD")).alias("APL_SUBTYP_CD"),
    F.when(F.col("APL_SUBTYP_CD_SK").isNull(), "").otherwise(F.col("APL_SUBTYP_NM")).alias("APL_SUBTYP_NM"),
    F.when(F.col("APL_TYP_CD_SK").isNull(), "").otherwise(F.col("APL_TYP_CD")).alias("APL_TYP_CD"),
    F.when(F.col("APL_TYP_CD_SK").isNull(), "").otherwise(F.col("APL_TYP_NM")).alias("APL_TYP_NM"),
    F.when(F.col("CUR_APL_LVL_CD_SK").isNull(), "").otherwise(F.col("CUR_APL_LVL_CD")).alias("CUR_APL_LVL_CD"),
    F.when(F.col("CUR_APL_LVL_CD_SK").isNull(), "").otherwise(F.col("CUR_APL_LVL_NM")).alias("CUR_APL_LVL_NM"),
    F.when(
        F.col("APL_CRT_DT").isNull(),
        FORMAT_DATE_EE("1753-01-01","DB2","TIMESTAMP","SYBTIMESTAMP")
    )
    .otherwise(FORMAT_DATE_EE(F.col("APL_CRT_DT"),"DB2","TIMESTAMP","SYBTIMESTAMP"))
    .alias("APL_CRT_DT"),
    F.when(
        (trim(F.col("APL_CUR_STTUS_DT")) == "UNK")
        | (trim(F.col("APL_CUR_STTUS_DT")) == "NA")
        | (trim(F.col("APL_CUR_STTUS_DT")) == ""),
        FORMAT_DATE_EE("1753-01-01","DB2","TIMESTAMP","SYBTIMESTAMP")
    )
    .otherwise(FORMAT_DATE_EE(F.col("APL_CUR_STTUS_DT"),"DB2","TIMESTAMP","SYBTIMESTAMP"))
    .alias("APL_CUR_STTUS_DT"),
    StringToTimestamp(F.col("APL_END_DT"), "%yyyy-%mm-%dd").alias("APL_END_DT"),
    StringToTimestamp(F.col("APL_INITN_DT"), "%yyyy-%mm-%dd").alias("APL_INITN_DT"),
    F.col("APL_DESC").alias("APL_DESC"),
    F.when(
        (trim(F.col("LAST_UPDT_DT")) == "UNK")
        | (trim(F.col("LAST_UPDT_DT")) == "NA")
        | (trim(F.col("LAST_UPDT_DT")) == ""),
        FORMAT_DATE_EE("1753-01-01","DB2","TIMESTAMP","SYBTIMESTAMP")
    )
    .otherwise(FORMAT_DATE_EE(F.col("LAST_UPDT_DT"),"DB2","TIMESTAMP","SYBTIMESTAMP"))
    .alias("LAST_UPDT_DT"),
    F.col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

df_final = df_xfm_BusinessLogic.select(
    "APL_ID",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "APL_CAT_CD",
    "APL_CAT_NM",
    "APL_CUR_DCSN_CD",
    "APL_CUR_DCSN_NM",
    "APL_CUR_STTUS_CD",
    "APL_CUR_STTUS_NM",
    "APL_INITN_METH_CD",
    "APL_INITN_METH_NM",
    "APL_SUBTYP_CD",
    "APL_SUBTYP_NM",
    "APL_TYP_CD",
    "APL_TYP_NM",
    "CUR_APL_LVL_CD",
    "CUR_APL_LVL_NM",
    "APL_CRT_DT",
    "APL_CUR_STTUS_DT",
    "APL_END_DT",
    "APL_INITN_DT",
    "APL_DESC",
    "LAST_UPDT_DT",
    F.rpad(F.col("LAST_UPDT_USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    "LAST_UPDT_RUN_CYC_NO"
)

write_files(
    df_final,
    f"{adls_path}/load/MBRSH_DM_MBR_APL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)