# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: BcbsIdsPMbrBcbsaSuplmtCntl
# MAGIC 
# MAGIC PROCESSING: Extracts last updated data from BCBS database table MBR_BCBSA_SUPLMT and creates a load file for IDS table P_MBR_BCBSA_SUPLMT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                                                Date                         Change Description                                                       Project #                                                 Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------                                                     ----------------------------     ----------------------------------------------------------------------------            ----------------                                                ------------------------------------       ----------------------------      
# MAGIC Karthik Chintalapani                                                08/20/2014            Originally Programmed                                                       5212                                                         IntegrateNewDevl          Bhoomi Dasari            1/27/2015
# MAGIC 
# MAGIC Manasa Andru                                                          2021-06-28            Added BCBSA_HOME_PLN_CORP_PLN_CD          US - 386505                                          IntegrateDev2                  Jaideep Mankala          06/29/2021
# MAGIC                                                                                                                      and BCBSA_PROD_CAT_CD at the end.
# MAGIC Rojarani Karnati                                                       2021-10-19          Added new column BCBSA_HI_PRFRMNC_NTWK_IN    US378619                                        IntegrateDev2                   Raja Gummadi             2021-10-26
# MAGIC Prabhu ES                                                             2022-03-11               MSSQL ODBC conn params added-redone                        S2S                                                IntegrateDev5                           Kalyan Neelam             2022-06-13

# MAGIC Job name: BcbsIdsPMbrBsbsaSuplmtExtr
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC As this is a P- table and doesnt have any Pkey or Fkey processes the extract and Xfrm steps are done in one single job.
# MAGIC Write P_MBR_BCBSA_SUPLMT
# MAGIC Extract from facets BCBSA_MBR_SUPLMT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')

# Stage: db2_MBR (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT MBR_UNIQ_KEY, MBR_SK FROM {IDSOwner}.MBR WHERE MBR_UNIQ_KEY <> 0 AND MBR_UNIQ_KEY <> 1"
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: BCBSA_MBR_SUPLMT_In (ODBCConnectorPX)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_bcbs = (
    f"SELECT BCBSA_HOME_PLN_ID, "
    f"BCBSA_HOME_PLN_CONSIS_MBR_ID, "
    f"BCBSA_ITS_SUB_ID, "
    f"BCBSA_HOME_PLN_MBR_ID, "
    f"BCBSA_COV_BEG_DT, "
    f"BCBSA_COV_END_DT, "
    f"BCBSA_HOME_PLN_PROD_ID, "
    f"MEME_CK, "
    f"BCBSA_MBR_FIRST_NM, "
    f"BCBSA_MBR_MIDINIT, "
    f"BCBSA_MBR_LAST_NM, "
    f"BCBSA_MBR_GNDR_CD, "
    f"BCBSA_MBR_BRTH_DT, "
    f"BCBSA_MBR_RELSHP_CD, "
    f"BCBSA_HOST_PLN_CD, "
    f"BCBSA_HOME_PLN_CORP_PLN_CD, "
    f"BCBSA_HOST_PLN_CORP_PLN_CD, "
    f"BCBSA_MBR_PARTCPN_CD, "
    f"BCBSA_VOID_IN, "
    f"MBR_ACTVTY_IN, "
    f"BCBSA_MMI_ID, "
    f"BCBSA_PROD_CAT_CD, "
    f"BCBSA_HI_PRFRMNC_NTWK_IN "
    f"FROM {BCBSOwner}.BCBSA_MBR_SUPLMT "
    f"WHERE MBR_ACTVTY_IN = 'Y'"
)
df_BCBSA_MBR_SUPLMT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_bcbs)
    .load()
)

# Stage: StripField (CTransformerStage)
df_StripField = (
    df_BCBSA_MBR_SUPLMT_In
    .withColumn("BCBSA_HOME_PLN_ID", strip_field(F.col("BCBSA_HOME_PLN_ID")))
    .withColumn("BCBSA_CONSIS_MBR_ID", strip_field(F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID")))
    .withColumn("BCBSA_ITS_SUB_ID", strip_field(F.col("BCBSA_ITS_SUB_ID")))
    .withColumn("BCBSA_HOME_PLN_MBR_ID", strip_field(F.col("BCBSA_HOME_PLN_MBR_ID")))
    .withColumn("BCBSA_COV_BEG_DT", TimestampToDate(F.col("BCBSA_COV_BEG_DT")))
    .withColumn("BCBSA_COV_END_DT", TimestampToDate(F.col("BCBSA_COV_END_DT")))
    .withColumn("BCBSA_HOME_PLN_PROD_ID", strip_field(F.col("BCBSA_HOME_PLN_PROD_ID")))
    .withColumn("MBR_UNIQ_KEY", F.when(F.col("MEME_CK").isNull(), F.lit(0)).otherwise(F.col("MEME_CK")))
    .withColumn("BCBSA_MBR_FIRST_NM", strip_field(F.col("BCBSA_MBR_FIRST_NM")))
    .withColumn("BCBSA_MBR_MIDINIT", strip_field(F.col("BCBSA_MBR_MIDINIT")))
    .withColumn("BCBSA_MBR_LAST_NM", strip_field(F.col("BCBSA_MBR_LAST_NM")))
    .withColumn("BCBSA_MBR_GNDR_CD", strip_field(F.col("BCBSA_MBR_GNDR_CD")))
    .withColumn("BCBSA_MBR_BRTH_DT", TimestampToDate(F.col("BCBSA_MBR_BRTH_DT")))
    .withColumn("BCBSA_MBR_RELSHP_CD", strip_field(F.col("BCBSA_MBR_RELSHP_CD")))
    .withColumn("BCBSA_HOME_PLN_CORP_PLN_CD", F.col("BCBSA_HOME_PLN_CORP_PLN_CD"))
    .withColumn("BCBSA_PROD_CAT_CD", F.when(F.col("BCBSA_PROD_CAT_CD").isNull(), F.lit("UNK")).otherwise(F.col("BCBSA_PROD_CAT_CD")))
    .withColumn("BCBSA_HI_PRFRMNC_NTWK_IN", F.col("BCBSA_HI_PRFRMNC_NTWK_IN"))
)

# Stage: Jn_Mbr (PxJoin) - inner join on MBR_UNIQ_KEY
df_Jn_Mbr_pre = df_db2_MBR.alias("lnk_Extr").join(
    df_StripField.alias("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean"),
    F.col("lnk_Extr.MBR_UNIQ_KEY") == F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.MBR_UNIQ_KEY"),
    "inner"
)

df_Jn_Mbr = df_Jn_Mbr_pre.select(
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_ID").alias("BCBSA_HOME_PLN_ID"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_CONSIS_MBR_ID").alias("BCBSA_CONSIS_MBR_ID"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_ITS_SUB_ID").alias("BCBSA_ITS_SUB_ID"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_MBR_ID").alias("BCBSA_HOME_PLN_MBR_ID"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_COV_BEG_DT").alias("BCBSA_COV_BEG_DT"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_COV_END_DT").alias("BCBSA_COV_END_DT"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_PROD_ID").alias("BCBSA_HOME_PLN_PROD_ID"),
    F.col("lnk_Extr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Extr.MBR_SK").alias("MBR_SK"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_FIRST_NM").alias("BCBSA_MBR_FIRST_NM"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_MIDINIT").alias("BCBSA_MBR_MIDINIT"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_LAST_NM").alias("BCBSA_MBR_LAST_NM"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_GNDR_CD").alias("BCBSA_MBR_GNDR_CD"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_BRTH_DT").alias("BCBSA_MBR_BRTH_DT"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_MBR_RELSHP_CD").alias("BCBSA_MBR_RELSHP_CD"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HOME_PLN_CORP_PLN_CD").alias("BCBSA_HOME_PLN_CORP_PLN_CD"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_PROD_CAT_CD").alias("BCBSA_PROD_CAT_CD"),
    F.col("Ink_BcbsIdsPMbrBsbsaSuplmtExtr_Clean.BCBSA_HI_PRFRMNC_NTWK_IN").alias("BCBSA_HI_PRFRMNC_NTWK_IN")
)

# Stage: xfm_BusinessLogic (CTransformerStage)
df_xfm_BusinessLogic_pre = (
    df_Jn_Mbr
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic_pre.select(
    F.col("BCBSA_HOME_PLN_ID"),
    F.col("BCBSA_CONSIS_MBR_ID").alias("BCBSA_HOME_PLN_CONSIS_MBR_ID"),  # Final Column Name
    F.col("BCBSA_ITS_SUB_ID"),
    F.col("BCBSA_HOME_PLN_MBR_ID"),
    F.col("BCBSA_COV_BEG_DT"),
    F.col("BCBSA_COV_END_DT"),
    F.col("BCBSA_HOME_PLN_PROD_ID"),
    F.col("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK"),
    F.col("BCBSA_MBR_FIRST_NM"),
    F.col("BCBSA_MBR_MIDINIT"),
    F.col("BCBSA_MBR_LAST_NM"),
    F.col("BCBSA_MBR_GNDR_CD"),
    F.col("BCBSA_MBR_BRTH_DT"),
    F.col("BCBSA_MBR_RELSHP_CD"),
    F.col("BCBSA_HOME_PLN_CORP_PLN_CD"),
    F.col("BCBSA_PROD_CAT_CD"),
    F.col("BCBSA_HI_PRFRMNC_NTWK_IN")
)

# Stage: Seq_P_MBR_BCBSA_SUPLMT (PxSequentialFile)
# Apply rpad to char/varchar columns before writing
df_final = (
    df_xfm_BusinessLogic
    .withColumn("BCBSA_HOME_PLN_ID", F.rpad(F.col("BCBSA_HOME_PLN_ID"), 3, " "))
    .withColumn("BCBSA_HOME_PLN_CONSIS_MBR_ID", F.rpad(F.col("BCBSA_HOME_PLN_CONSIS_MBR_ID"), 22, " "))
    .withColumn("BCBSA_ITS_SUB_ID", F.rpad(F.col("BCBSA_ITS_SUB_ID"), 17, " "))
    .withColumn("BCBSA_HOME_PLN_MBR_ID", F.rpad(F.col("BCBSA_HOME_PLN_MBR_ID"), 22, " "))
    .withColumn("BCBSA_HOME_PLN_PROD_ID", F.rpad(F.col("BCBSA_HOME_PLN_PROD_ID"), 15, " "))
    .withColumn("BCBSA_MBR_FIRST_NM", F.rpad(F.col("BCBSA_MBR_FIRST_NM"), 70, " "))
    .withColumn("BCBSA_MBR_MIDINIT", F.rpad(F.col("BCBSA_MBR_MIDINIT"), 2, " "))
    .withColumn("BCBSA_MBR_LAST_NM", F.rpad(F.col("BCBSA_MBR_LAST_NM"), 150, " "))
    .withColumn("BCBSA_MBR_GNDR_CD", F.rpad(F.col("BCBSA_MBR_GNDR_CD"), 1, " "))
    .withColumn("BCBSA_HOME_PLN_CORP_PLN_CD", F.rpad(F.col("BCBSA_HOME_PLN_CORP_PLN_CD"), 3, " "))
    .withColumn("BCBSA_HI_PRFRMNC_NTWK_IN", F.rpad(F.col("BCBSA_HI_PRFRMNC_NTWK_IN"), 1, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/P_MBR_BCBSA_SUPLMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)