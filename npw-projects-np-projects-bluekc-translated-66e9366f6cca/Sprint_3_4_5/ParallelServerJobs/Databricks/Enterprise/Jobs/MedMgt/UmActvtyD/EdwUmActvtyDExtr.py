# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/12/09 10:26:07 Batch  15261_37578 PROMOTE bckcetl:31540 edw10 dsadm bls for rt
# MAGIC ^1_2 10/12/09 10:20:46 Batch  15261_37248 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_2 09/28/09 11:45:32 Batch  15247_42360 PROMOTE bckcett:31540 testEDW u150906 TTR583-UMMedMgt_Ralph_testEDW              Maddy
# MAGIC ^1_2 09/28/09 11:36:19 Batch  15247_41826 INIT bckcett:31540 devlEDW u150906 TTR583-UMMedMgt_Ralph_devlEDW              Maddy
# MAGIC ^1_1 05/12/09 14:25:07 Batch  15108_51913 PROMOTE bckcetl edw10 dsadm bls for rt
# MAGIC ^1_1 05/12/09 14:16:44 Batch  15108_51406 INIT bckcett:31540 testEDW dsadm BLS FOR RT
# MAGIC ^1_1 05/01/09 08:54:06 Batch  15097_32051 PROMOTE bckcett testEDW u03651 steph for Ralph
# MAGIC ^1_1 05/01/09 08:51:22 Batch  15097_31885 INIT bckcett devlEDW u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwUmActvtyDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS UM_ACTVTY to flatfile UM_ACTVTY_D.da
# MAGIC       
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: CASE_MGT_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 3/26/2009          BICC/3808                  Original Programming.                                                                               devlEDW                  Steph Goddard            04/03/2009
# MAGIC Ralph Tucker                  09/15/2009     TTR-583                      Added EdwRunCycle to last updt run cyc                                               devlEDW                      Steph Goddard            09/16/2009
# MAGIC Ralph Tucker                  09/08/2010     TTR-885                      Changed hash file clear to correct name                                                 EnterpriseNewDevl      Steph Goddard             09/09/2010

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, length, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','2009-03-27')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','100')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT UM_ACTVTY_SK,SRC_SYS_CD_SK,UM_REF_ID,UM_ACTVTY_SEQ_NO,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,ACTVTY_USER_SK,RTE_USER_SK,UM_SK,ACTVTY_DT_SK,UM_ACTVTY_CMPLXTY_LVL_CD_SK,UM_ACTVTY_RSN_CD_SK,SVC_ROWS_ADD_NO,IP_ROWS_ADD_NO,RVW_ROWS_ADD_NO,NOTE_ROWS_ADD_NO FROM {IDSOwner}.UM_ACTVTY WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= '{ExtractRunCycle}' or LAST_UPDT_RUN_CYC_EXCTN_SK = 0 or LAST_UPDT_RUN_CYC_EXCTN_SK = 1"
df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

user_query = f"SELECT USER_SK as USER_SK,USER_ID as USER_ID FROM {IDSOwner}.APP_USER"
df_IDS_PriCaseMgrUserId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", user_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_hf_um_actvty_user_id = dedup_sort(
    df_IDS_PriCaseMgrUserId,
    ["USER_SK"],
    [("USER_SK","A")]
)

df_hf_cdma_codes_srcsys = df_hf_cdma_codes.alias("refSrcSysCd")
df_hf_cdma_codes_cmplxty = df_hf_cdma_codes.alias("refUmActvtyCmplxtyLvlCdCat")
df_hf_cdma_codes_rsn = df_hf_cdma_codes.alias("refUmActvtyRsnCdCat")

df_Join1 = df_IDS_Extract.alias("Extract").join(
    df_hf_cdma_codes_srcsys,
    col("Extract.SRC_SYS_CD_SK") == col("refSrcSysCd.CD_MPPNG_SK"),
    "left"
)
df_Join2 = df_Join1.alias("Join1").join(
    df_hf_cdma_codes_cmplxty,
    col("Join1.UM_ACTVTY_CMPLXTY_LVL_CD_SK") == col("refUmActvtyCmplxtyLvlCdCat.CD_MPPNG_SK"),
    "left"
)
df_Join3 = df_Join2.alias("Join2").join(
    df_hf_cdma_codes_rsn,
    col("Join2.UM_ACTVTY_RSN_CD_SK") == col("refUmActvtyRsnCdCat.CD_MPPNG_SK"),
    "left"
)
df_Join4 = df_Join3.alias("Join3").join(
    df_hf_um_actvty_user_id.alias("ActvtyUserId"),
    col("Join3.ACTVTY_USER_SK") == col("ActvtyUserId.USER_SK"),
    "left"
)
df_Join5 = df_Join4.alias("Join4").join(
    df_hf_um_actvty_user_id.alias("RteUserId"),
    col("Join4.RTE_USER_SK") == col("RteUserId.USER_SK"),
    "left"
)

df_enriched = df_Join5.select(
    col("Extract.UM_ACTVTY_SK").alias("UM_ACTVTY_SK"),
    when(
        (col("refSrcSysCd.TRGT_CD").isNull()) | (length(trim(col("refSrcSysCd.TRGT_CD"))) == 0),
        lit("NA")
    ).otherwise(col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    col("Extract.UM_REF_ID").alias("UM_REF_ID"),
    col("Extract.UM_ACTVTY_SEQ_NO").alias("UM_ACTVTY_SEQ_NO"),
    rpad(lit(CurrentDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(CurrentDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("Extract.ACTVTY_USER_SK").alias("ACTIVITY_USER_SK"),
    col("Extract.RTE_USER_SK").alias("ROUTE_USER_SK"),
    col("Extract.UM_SK").alias("UM_SK"),
    rpad(col("Extract.ACTVTY_DT_SK"), 10, " ").alias("ACTVTY_DT_SK"),
    rpad(
        when(
            (col("ActvtyUserId.USER_ID").isNull()) | (length(trim(col("ActvtyUserId.USER_ID"))) == 0),
            lit("NA")
        ).otherwise(col("ActvtyUserId.USER_ID")),
        10,
        " "
    ).alias("ACTVTY_USER_ID"),
    rpad(
        when(
            (col("RteUserId.USER_ID").isNull()) | (length(trim(col("RteUserId.USER_ID"))) == 0),
            lit("NA")
        ).otherwise(col("RteUserId.USER_ID")),
        10,
        " "
    ).alias("RTE_USER_ID"),
    rpad(
        when(
            (col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD").isNull()) | (length(trim(col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD")),
        <...>,
        " "
    ).alias("UM_ACTVTY_CMPLXTY_LVL_CD"),
    rpad(
        when(
            (col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD_NM").isNull()) | (length(trim(col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("refUmActvtyCmplxtyLvlCdCat.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_ACTVTY_CMPLXTY_LVL_NM"),
    col("Join4.IP_ROWS_ADD_NO").alias("UM_ACTVTY_IP_ROWS_ADD_NO"),
    col("Join4.NOTE_ROWS_ADD_NO").alias("UM_ACTVTY_NOTE_ROWS_ADD_NO"),
    rpad(
        when(
            (col("refUmActvtyRsnCdCat.TRGT_CD").isNull()) | (length(trim(col("refUmActvtyRsnCdCat.TRGT_CD"))) == 0),
            lit("NA")
        ).otherwise(col("refUmActvtyRsnCdCat.TRGT_CD")),
        <...>,
        " "
    ).alias("UM_ACTVTY_RSN_CD"),
    rpad(
        when(
            (col("refUmActvtyRsnCdCat.TRGT_CD_NM").isNull()) | (length(trim(col("refUmActvtyRsnCdCat.TRGT_CD_NM"))) == 0),
            lit("NA")
        ).otherwise(col("refUmActvtyRsnCdCat.TRGT_CD_NM")),
        <...>,
        " "
    ).alias("UM_ACTVTY_RSN_NM"),
    col("Join4.RVW_ROWS_ADD_NO").alias("UM_ACTVTY_RVW_ROWS_ADD_NO"),
    col("Join4.SVC_ROWS_ADD_NO").alias("UM_ACTVTY_SVC_ROWS_ADD_NO"),
    col("Join4.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Join4.UM_ACTVTY_CMPLXTY_LVL_CD_SK").alias("UM_ACTVTY_CMPLXTY_LVL_CD_SK"),
    col("Join4.UM_ACTVTY_RSN_CD_SK").alias("UM_ACTVTY_RSN_CD_SK")
)

write_files(
    df_enriched,
    f"{adls_path}/load/UM_ACTVTY_D.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)