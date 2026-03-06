# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839- MADOR                 Originally Programmed                            IntegrateDev2

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table K_MBR_MA_DOR.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_K_MBR_MA_DOR_In = f"SELECT GRP_ID, MBR_ID, SUB_ID, SRC_SYS_CD, TAX_YR, CRT_RUN_CYC_EXCTN_SK, MBR_MA_DOR_SK FROM {IDSOwner}.K_MBR_MA_DOR"
df_db2_K_MBR_MA_DOR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_MBR_MA_DOR_In)
    .load()
)

df_Member_DS = spark.read.parquet(f"{adls_path}/ds/MBR_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet")

df_lnkRemDupDataIn = df_Member_DS.select(
    col("MBR_ID").alias("MBR_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_Member_DS.select(
    col("MBR_ID").alias("MBR_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("AS_OF_DTM").alias("AS_OF_DTM"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    col("JAN_COV_IN").alias("JAN_COV_IN"),
    col("FEB_COV_IN").alias("FEB_COV_IN"),
    col("MAR_COV_IN").alias("MAR_COV_IN"),
    col("APR_COV_IN").alias("APR_COV_IN"),
    col("MAY_COV_IN").alias("MAY_COV_IN"),
    col("JUN_COV_IN").alias("JUN_COV_IN"),
    col("JUL_COV_IN").alias("JUL_COV_IN"),
    col("AUG_COV_IN").alias("AUG_COV_IN"),
    col("SEP_COV_IN").alias("SEP_COV_IN"),
    col("OCT_COV_IN").alias("OCT_COV_IN"),
    col("NOV_COV_IN").alias("NOV_COV_IN"),
    col("DEC_COV_IN").alias("DEC_COV_IN"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["MBR_ID","SUB_ID","GRP_ID","TAX_YR","SRC_SYS_CD"],
    []
)

df_jn_IndvBeMaraRslt = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_MBR_MA_DOR_In.alias("Extr"),
        (
            (col("lnkRemDupDataOut.MBR_ID") == col("Extr.MBR_ID")) &
            (col("lnkRemDupDataOut.SUB_ID") == col("Extr.SUB_ID")) &
            (col("lnkRemDupDataOut.GRP_ID") == col("Extr.GRP_ID")) &
            (col("lnkRemDupDataOut.TAX_YR") == col("Extr.TAX_YR")) &
            (col("lnkRemDupDataOut.SRC_SYS_CD") == col("Extr.SRC_SYS_CD"))
        ),
        "left"
    )
    .select(
        col("lnkRemDupDataOut.MBR_ID").alias("MBR_ID"),
        col("lnkRemDupDataOut.SUB_ID").alias("SUB_ID"),
        col("lnkRemDupDataOut.GRP_ID").alias("GRP_ID"),
        col("lnkRemDupDataOut.TAX_YR").alias("TAX_YR"),
        col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Extr.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK")
    )
)

df_xfm_PKEYgen_1 = df_jn_IndvBeMaraRslt.withColumn("orig_MBR_MA_DOR_SK", col("MBR_MA_DOR_SK"))
df_xfm_PKEYgen_1 = df_xfm_PKEYgen_1.withColumn(
    "MBR_MA_DOR_SK",
    when(
        (col("MBR_MA_DOR_SK").isNotNull()) & (col("MBR_MA_DOR_SK") != lit(0)),
        col("MBR_MA_DOR_SK")
    ).otherwise(lit(None))
)

df_enriched = SurrogateKeyGen(df_xfm_PKEYgen_1,<DB sequence name>,'MBR_MA_DOR_SK',<schema>,<secret_name>)

df_New = (
    df_enriched
    .filter((col("orig_MBR_MA_DOR_SK").isNull()) | (col("orig_MBR_MA_DOR_SK") == 0))
    .select(
        col("MBR_ID").alias("MBR_ID"),
        col("SUB_ID").alias("SUB_ID"),
        col("GRP_ID").alias("GRP_ID"),
        col("TAX_YR").alias("TAX_YR"),
        current_timestamp().alias("PRCS_DTM"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        col("MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK")
    )
)

df_lnkPKEYxfmOut = (
    df_enriched
    .select(
        col("MBR_ID").alias("MBR_ID"),
        col("SUB_ID").alias("SUB_ID"),
        col("GRP_ID").alias("GRP_ID"),
        col("TAX_YR").alias("TAX_YR"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        when(
            (col("orig_MBR_MA_DOR_SK").isNull()) | (col("orig_MBR_MA_DOR_SK") == 0),
            lit(IDSRunCycle)
        ).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK")
    )
)

temp_table_name_db2_K_MBR_MA_DOR_Load = "STAGING.ScIdsMadorMbrPkey_db2_K_MBR_MA_DOR_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_db2_K_MBR_MA_DOR_Load}", jdbc_url, jdbc_props)
(
    df_New.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name_db2_K_MBR_MA_DOR_Load)
    .mode("append")
    .save()
)

merge_sql_db2_K_MBR_MA_DOR_Load = f"""
MERGE INTO {IDSOwner}.K_MBR_MA_DOR AS T
USING {temp_table_name_db2_K_MBR_MA_DOR_Load} AS S
ON T.MBR_MA_DOR_SK = S.MBR_MA_DOR_SK
WHEN MATCHED THEN UPDATE SET
  T.MBR_ID=S.MBR_ID,
  T.SUB_ID=S.SUB_ID,
  T.GRP_ID=S.GRP_ID,
  T.TAX_YR=S.TAX_YR,
  T.SRC_SYS_CD=S.SRC_SYS_CD,
  T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
  T.MBR_MA_DOR_SK=S.MBR_MA_DOR_SK,
  T.PRCS_DTM=S.PRCS_DTM
WHEN NOT MATCHED THEN
INSERT (MBR_ID,SUB_ID,GRP_ID,TAX_YR,PRCS_DTM,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,MBR_MA_DOR_SK)
VALUES (S.MBR_ID,S.SUB_ID,S.GRP_ID,S.TAX_YR,S.PRCS_DTM,S.SRC_SYS_CD,S.CRT_RUN_CYC_EXCTN_SK,S.MBR_MA_DOR_SK);
"""
execute_dml(merge_sql_db2_K_MBR_MA_DOR_Load, jdbc_url, jdbc_props)

df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        (
            (col("lnkFullDataJnIn.MBR_ID") == col("lnkPKEYxfmOut.MBR_ID")) &
            (col("lnkFullDataJnIn.SUB_ID") == col("lnkPKEYxfmOut.SUB_ID")) &
            (col("lnkFullDataJnIn.GRP_ID") == col("lnkPKEYxfmOut.GRP_ID")) &
            (col("lnkFullDataJnIn.TAX_YR") == col("lnkPKEYxfmOut.TAX_YR")) &
            (col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"))
        ),
        "inner"
    )
    .select(
        col("lnkFullDataJnIn.MBR_ID").alias("MBR_ID"),
        col("lnkFullDataJnIn.SUB_ID").alias("SUB_ID"),
        col("lnkFullDataJnIn.GRP_ID").alias("GRP_ID"),
        col("lnkFullDataJnIn.TAX_YR").alias("TAX_YR"),
        col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkFullDataJnIn.AS_OF_DTM").alias("AS_OF_DTM"),
        col("lnkFullDataJnIn.GRP_SK").alias("GRP_SK"),
        col("lnkFullDataJnIn.MBR_SK").alias("MBR_SK"),
        col("lnkFullDataJnIn.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
        col("lnkFullDataJnIn.SUB_SK").alias("SUB_SK"),
        col("lnkFullDataJnIn.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        col("lnkFullDataJnIn.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        col("lnkFullDataJnIn.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        col("lnkFullDataJnIn.MBR_MIDINIT").alias("MBR_MIDINIT"),
        col("lnkFullDataJnIn.MBR_LAST_NM").alias("MBR_LAST_NM"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        col("lnkFullDataJnIn.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        col("lnkFullDataJnIn.JAN_COV_IN").alias("JAN_COV_IN"),
        col("lnkFullDataJnIn.FEB_COV_IN").alias("FEB_COV_IN"),
        col("lnkFullDataJnIn.MAR_COV_IN").alias("MAR_COV_IN"),
        col("lnkFullDataJnIn.APR_COV_IN").alias("APR_COV_IN"),
        col("lnkFullDataJnIn.MAY_COV_IN").alias("MAY_COV_IN"),
        col("lnkFullDataJnIn.JUN_COV_IN").alias("JUN_COV_IN"),
        col("lnkFullDataJnIn.JUL_COV_IN").alias("JUL_COV_IN"),
        col("lnkFullDataJnIn.AUG_COV_IN").alias("AUG_COV_IN"),
        col("lnkFullDataJnIn.SEP_COV_IN").alias("SEP_COV_IN"),
        col("lnkFullDataJnIn.OCT_COV_IN").alias("OCT_COV_IN"),
        col("lnkFullDataJnIn.NOV_COV_IN").alias("NOV_COV_IN"),
        col("lnkFullDataJnIn.DEC_COV_IN").alias("DEC_COV_IN"),
        col("lnkFullDataJnIn.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        col("lnkFullDataJnIn.MBR_SFX_NO").alias("MBR_SFX_NO"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK")
    )
)

df_seq_MBR_MA_DOR_Pkey = df_jn_PKEYs.select(
    col("MBR_ID"),
    col("SUB_ID"),
    col("GRP_ID"),
    rpad(col("TAX_YR"),4," ").alias("TAX_YR"),
    col("SRC_SYS_CD"),
    col("AS_OF_DTM"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("SUB_MA_DOR_SK"),
    col("SUB_SK"),
    col("MBR_RELSHP_CD_SK"),
    col("MBR_RELSHP_TYPE_NO"),
    col("MBR_FIRST_NM"),
    rpad(col("MBR_MIDINIT"),1," ").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM"),
    col("MBR_MAIL_ADDR_LN_1"),
    col("MBR_MAIL_ADDR_LN_2"),
    col("MBR_MAIL_ADDR_LN_3"),
    col("MBR_MAIL_ADDR_CITY_NM"),
    col("MBR_MAIL_ADDR_ST_CD"),
    rpad(col("MBR_MAIL_ADDR_ZIP_CD_5"),5," ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    rpad(col("MBR_MAIL_ADDR_ZIP_CD_4"),4," ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    rpad(col("JAN_COV_IN"),1," ").alias("JAN_COV_IN"),
    rpad(col("FEB_COV_IN"),1," ").alias("FEB_COV_IN"),
    rpad(col("MAR_COV_IN"),1," ").alias("MAR_COV_IN"),
    rpad(col("APR_COV_IN"),1," ").alias("APR_COV_IN"),
    rpad(col("MAY_COV_IN"),1," ").alias("MAY_COV_IN"),
    rpad(col("JUN_COV_IN"),1," ").alias("JUN_COV_IN"),
    rpad(col("JUL_COV_IN"),1," ").alias("JUL_COV_IN"),
    rpad(col("AUG_COV_IN"),1," ").alias("AUG_COV_IN"),
    rpad(col("SEP_COV_IN"),1," ").alias("SEP_COV_IN"),
    rpad(col("OCT_COV_IN"),1," ").alias("OCT_COV_IN"),
    rpad(col("NOV_COV_IN"),1," ").alias("NOV_COV_IN"),
    rpad(col("DEC_COV_IN"),1," ").alias("DEC_COV_IN"),
    col("MBR_BRTH_DT"),
    col("MBR_SFX_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_MA_DOR_SK")
)

write_files(
    df_seq_MBR_MA_DOR_Pkey,
    f"{adls_path}/key/MBR_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)