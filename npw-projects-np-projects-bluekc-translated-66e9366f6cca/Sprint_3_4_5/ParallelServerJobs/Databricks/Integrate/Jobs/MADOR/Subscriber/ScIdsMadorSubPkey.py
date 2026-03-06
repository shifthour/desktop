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
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839- MADOR                 Originally Programmed                            IntegrateDev2            Kalyan Neelam              2018-06-14

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table K_SUB_MA_DOR.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad, isnull
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

jdbc_url_db2_K_SUB_MA_DOR_In, jdbc_props_db2_K_SUB_MA_DOR_In = get_db_config(ids_secret_name)
extract_query_db2_K_SUB_MA_DOR_In = f"SELECT GRP_ID, SUB_ID, SRC_SYS_CD, TAX_YR, CRT_RUN_CYC_EXCTN_SK, SUB_MA_DOR_SK FROM {IDSOwner}.K_SUB_MA_DOR"
df_db2_K_SUB_MA_DOR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_SUB_MA_DOR_In)
    .options(**jdbc_props_db2_K_SUB_MA_DOR_In)
    .option("query", extract_query_db2_K_SUB_MA_DOR_In)
    .load()
)

df_Subscriber_DS = spark.read.parquet(f"{adls_path}/ds/SUB_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet")

df_Cp_Sub_lnkRemDupDataIn = df_Subscriber_DS.select(
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Cp_Sub_lnkFullDataJnIn = df_Subscriber_DS.select(
    col("GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("AS_OF_DTM").alias("AS_OF_DTM"),
    col("GRP_SK").alias("GRP_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
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
    col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_rdp_NaturalKeys = dedup_sort(
    df_Cp_Sub_lnkRemDupDataIn,
    ["SUB_ID", "GRP_ID", "TAX_YR", "SRC_SYS_CD"],
    [("SUB_ID", "A"), ("GRP_ID", "A"), ("TAX_YR", "A"), ("SRC_SYS_CD", "A")]
)

df_jn_IndvBeMaraRslt = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_SUB_MA_DOR_In.alias("Extr"),
        [
            col("lnkRemDupDataOut.SUB_ID") == col("Extr.SUB_ID"),
            col("lnkRemDupDataOut.GRP_ID") == col("Extr.GRP_ID"),
            col("lnkRemDupDataOut.TAX_YR") == col("Extr.TAX_YR"),
            col("lnkRemDupDataOut.SRC_SYS_CD") == col("Extr.SRC_SYS_CD")
        ],
        "left"
    )
    .select(
        col("lnkRemDupDataOut.SUB_ID").alias("SUB_ID"),
        col("lnkRemDupDataOut.GRP_ID").alias("GRP_ID"),
        col("lnkRemDupDataOut.TAX_YR").alias("TAX_YR"),
        col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Extr.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK")
    )
)

df_enriched = df_jn_IndvBeMaraRslt.withColumn("orig_SUB_MA_DOR_SK", col("SUB_MA_DOR_SK"))
df_enriched = df_enriched.withColumn(
    "SUB_MA_DOR_SK",
    when((col("SUB_MA_DOR_SK") == 0) | (col("SUB_MA_DOR_SK").isNull()), lit(None)).otherwise(col("SUB_MA_DOR_SK"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SUB_MA_DOR_SK",<schema>,<secret_name>)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_enriched.select(
    col("SUB_ID"),
    col("GRP_ID"),
    col("TAX_YR"),
    col("SRC_SYS_CD"),
    when((col("orig_SUB_MA_DOR_SK").isNull()) | (col("orig_SUB_MA_DOR_SK") == 0), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK")
)

df_xfm_PKEYgen_New = (
    df_enriched
    .filter((col("orig_SUB_MA_DOR_SK").isNull()) | (col("orig_SUB_MA_DOR_SK") == 0))
    .select(
        col("SUB_ID"),
        col("GRP_ID"),
        col("TAX_YR"),
        col("SRC_SYS_CD"),
        current_timestamp().alias("PRCS_DTM"),
        lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK")
    )
)

jdbc_url_db2_K_SUB_MA_DOR_Load, jdbc_props_db2_K_SUB_MA_DOR_Load = get_db_config(ids_secret_name)
tmp_table_db2_K_SUB_MA_DOR_Load = "STAGING.ScIdsMadorSubPkey_db2_K_SUB_MA_DOR_Load_temp"
execute_dml(
    f"DROP TABLE IF EXISTS {tmp_table_db2_K_SUB_MA_DOR_Load}",
    jdbc_url_db2_K_SUB_MA_DOR_Load,
    jdbc_props_db2_K_SUB_MA_DOR_Load
)

df_db2_K_SUB_MA_DOR_Load = df_xfm_PKEYgen_New.select(
    col("SUB_ID"),
    col("GRP_ID"),
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),
    col("SRC_SYS_CD"),
    col("PRCS_DTM"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("SUB_MA_DOR_SK")
)

df_db2_K_SUB_MA_DOR_Load.write.jdbc(
    url=jdbc_url_db2_K_SUB_MA_DOR_Load,
    table=tmp_table_db2_K_SUB_MA_DOR_Load,
    mode="overwrite",
    properties=jdbc_props_db2_K_SUB_MA_DOR_Load
)

merge_sql_db2_K_SUB_MA_DOR_Load = f"""
MERGE {IDSOwner}.K_SUB_MA_DOR AS T
USING {tmp_table_db2_K_SUB_MA_DOR_Load} AS S
ON 
  T.SUB_ID = S.SUB_ID AND
  T.GRP_ID = S.GRP_ID AND
  T.TAX_YR = S.TAX_YR AND
  T.SRC_SYS_CD = S.SRC_SYS_CD AND
  T.PRCS_DTM = S.PRCS_DTM
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.SUB_MA_DOR_SK = S.SUB_MA_DOR_SK
WHEN NOT MATCHED THEN
  INSERT (SUB_ID, GRP_ID, TAX_YR, SRC_SYS_CD, PRCS_DTM, CRT_RUN_CYC_EXCTN_SK, SUB_MA_DOR_SK)
  VALUES (S.SUB_ID, S.GRP_ID, S.TAX_YR, S.SRC_SYS_CD, S.PRCS_DTM, S.CRT_RUN_CYC_EXCTN_SK, S.SUB_MA_DOR_SK);
"""
execute_dml(merge_sql_db2_K_SUB_MA_DOR_Load, jdbc_url_db2_K_SUB_MA_DOR_Load, jdbc_props_db2_K_SUB_MA_DOR_Load)

df_jn_PKEYs = (
    df_Cp_Sub_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        [
            col("lnkFullDataJnIn.SUB_ID") == col("lnkPKEYxfmOut.SUB_ID"),
            col("lnkFullDataJnIn.GRP_ID") == col("lnkPKEYxfmOut.GRP_ID"),
            col("lnkFullDataJnIn.TAX_YR") == col("lnkPKEYxfmOut.TAX_YR"),
            col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
        ],
        "inner"
    )
    .select(
        col("lnkFullDataJnIn.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
        col("lnkFullDataJnIn.SUB_ID").alias("SUB_ID"),
        col("lnkFullDataJnIn.GRP_ID").alias("GRP_ID"),
        col("lnkFullDataJnIn.TAX_YR").alias("TAX_YR"),
        col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkFullDataJnIn.AS_OF_DTM").alias("AS_OF_DTM"),
        col("lnkFullDataJnIn.GRP_SK").alias("GRP_SK"),
        col("lnkFullDataJnIn.SUB_SK").alias("SUB_SK"),
        col("lnkFullDataJnIn.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        col("lnkFullDataJnIn.SUB_MIDINIT").alias("SUB_MIDINIT"),
        col("lnkFullDataJnIn.SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        col("lnkFullDataJnIn.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        col("lnkFullDataJnIn.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
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
        col("lnkFullDataJnIn.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        col("lnkFullDataJnIn.MBR_SFX_NO").alias("MBR_SFX_NO"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK")
    )
)

df_seq_SUB_MA_DOR_Pkey = df_jn_PKEYs.select(
    col("GRP_MA_DOR_SK"),
    col("SUB_ID"),
    col("GRP_ID"),
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),
    col("SRC_SYS_CD"),
    col("AS_OF_DTM"),
    col("GRP_SK"),
    col("SUB_SK"),
    col("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_HOME_ADDR_LN_1"),
    col("SUB_HOME_ADDR_LN_2"),
    col("SUB_HOME_ADDR_LN_3"),
    col("SUB_HOME_ADDR_CITY_NM"),
    col("SUB_HOME_ADDR_ST_CD"),
    rpad(col("SUB_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    rpad(col("SUB_HOME_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    col("SUB_MAIL_ADDR_LN_1"),
    col("SUB_MAIL_ADDR_LN_2"),
    col("SUB_MAIL_ADDR_LN_3"),
    col("SUB_MAIL_ADDR_CITY_NM"),
    col("SUB_MAIL_ADDR_ST_CD"),
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    rpad(col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
    rpad(col("FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
    rpad(col("MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
    rpad(col("APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
    rpad(col("MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
    rpad(col("JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
    rpad(col("JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
    rpad(col("AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
    rpad(col("SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
    rpad(col("OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
    rpad(col("NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
    rpad(col("DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
    col("SUB_BRTH_DT"),
    col("MBR_SFX_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SUB_MA_DOR_SK")
)

write_files(
    df_seq_SUB_MA_DOR_Pkey,
    f"{adls_path}/key/SUB_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)