# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Jag Yelavarthi         2014-07-17             5345                             Original Programming                                                                        IntegrateWrhsDevl   
# MAGIC 
# MAGIC Kiran Mulakalapalli       10/28/2021           US468727                  Added new filed ATND_TEXT from Facets to IDS                         IntegrateDev2              Jeyaprasanna                2021-11-18

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_PROV_ADDR.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC PKEY out file for FKEY job consumption
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','')

# Read the PxDataSet (ds_ProvAddr_Xfm) from .ds -> parquet
df_ds_ProvAddr_Xfm = spark.read.parquet(
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# Select columns in the same order as defined in the dataset stage
df_ds_ProvAddr_Xfm = df_ds_ProvAddr_Xfm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROV_ADDR_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "PROV_ADDR_CNTY_CLS_CD",
    "PROV_ADDR_GEO_ACES_RTRN_CD",
    "PROV_ADDR_METRORURAL_COV_CD",
    "PROV_ADDR_TERM_RSN_CD",
    "HCAP_IN",
    "PDX_24_HR_IN",
    "PRCTC_LOC_IN",
    "PROV_ADDR_DIR_IN",
    "TERM_DT",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "PROV_ADDR_ST_CD",
    "POSTAL_CD",
    "CNTY_NM",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR_TX",
    "LAT_TX",
    "LONG_TX",
    "PRAD_TYPE_MAIL",
    "PROV2_PRAD_EFF_DT",
    "PROV_ADDR_TYP_CD_ORIG",
    "ATND_TEXT"
)

# cpy_MultiStreams: Split into two outputs
df_lnkRemDupDataIn = df_ds_ProvAddr_Xfm.select(
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_ds_ProvAddr_Xfm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    col("PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    col("PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    col("PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    col("HCAP_IN").alias("HCAP_IN"),
    col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    col("TERM_DT").alias("TERM_DT"),
    col("ADDR_LN_1").alias("ADDR_LN_1"),
    col("ADDR_LN_2").alias("ADDR_LN_2"),
    col("ADDR_LN_3").alias("ADDR_LN_3"),
    col("CITY_NM").alias("CITY_NM"),
    col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    col("POSTAL_CD").alias("POSTAL_CD"),
    col("CNTY_NM").alias("CNTY_NM"),
    col("PHN_NO").alias("PHN_NO"),
    col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    col("FAX_NO").alias("FAX_NO"),
    col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    col("LAT_TX").alias("LAT_TX"),
    col("LONG_TX").alias("LONG_TX"),
    col("PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    col("PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT"),
    col("PROV_ADDR_TYP_CD_ORIG").alias("PROV_ADDR_TYP_CD_ORIG"),
    col("ATND_TEXT").alias("ATND_TEXT")
)

# rdp_NaturalKeys (PxRemDup) on lnkRemDupDataIn
df_lnkRemDupDataIn_dedup = dedup_sort(
    df_lnkRemDupDataIn,
    ["PROV_ADDR_ID", "PROV_ADDR_TYP_CD", "PROV_ADDR_EFF_DT", "SRC_SYS_CD"],
    []
)

df_lnkRemDupDataOut = df_lnkRemDupDataIn_dedup.select(
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD"
)

# db2_K_PROV_ADDR_In (DB2ConnectorPX) => reading from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT PROV_ADDR_ID,PROV_ADDR_TYP_CD,PROV_ADDR_EFF_DT_SK AS PROV_ADDR_EFF_DT,"
    "SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PROV_ADDR_SK "
    f"FROM {IDSOwner}.K_PROV_ADDR"
)
df_db2_K_PROV_ADDR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_ProvAddr => Left Join
df_jn_ProvAddr = df_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_K_PROV_ADDR_In.alias("lnkKProvAddr"),
    on=[
        col("lnkRemDupDataOut.PROV_ADDR_ID") == col("lnkKProvAddr.PROV_ADDR_ID"),
        col("lnkRemDupDataOut.PROV_ADDR_TYP_CD") == col("lnkKProvAddr.PROV_ADDR_TYP_CD"),
        col("lnkRemDupDataOut.PROV_ADDR_EFF_DT") == col("lnkKProvAddr.PROV_ADDR_EFF_DT"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKProvAddr.SRC_SYS_CD")
    ],
    how="left"
)

df_lnk_ProvAddr_JoinOut = df_jn_ProvAddr.select(
    col("lnkRemDupDataOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnkRemDupDataOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("lnkRemDupDataOut.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkKProvAddr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkKProvAddr.PROV_ADDR_SK").alias("PROV_ADDR_SK")
)

# xfm_PKEYgen: We handle the stage variable for Surrogate Key
df_stagevar = df_lnk_ProvAddr_JoinOut.withColumn("original_PROV_ADDR_SK", col("PROV_ADDR_SK"))

# We'll store the same column in "PROV_ADDR_SK" for SurrogateKeyGen
df_stagevar = df_stagevar.withColumn("PROV_ADDR_SK", col("PROV_ADDR_SK"))

# Apply SurrogateKeyGen to fill PROV_ADDR_SK where it's null
df_enriched = SurrogateKeyGen(df_stagevar,<DB sequence name>,"PROV_ADDR_SK",<schema>,<secret_name>)

# Output link #1 => lnk_KProvAddr_new => filter rows where original PROV_ADDR_SK was null
df_lnk_KProvAddr_new = df_enriched.filter(
    col("original_PROV_ADDR_SK").isNull()
).select(
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    when(
        col("original_PROV_ADDR_SK").isNull(),
        IDSRunCycle
    ).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("PROV_ADDR_SK").alias("PROV_ADDR_SK")
)

# Output link #2 => lnkPKEYxfmOut => all rows
df_lnkPKEYxfmOut = df_enriched.select(
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    when(
        col("original_PROV_ADDR_SK").isNull(),
        IDSRunCycle
    ).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROV_ADDR_SK").alias("PROV_ADDR_SK")
)

# db2_K_PROV_ADDR_Load => Merge into #$IDSOwner#.K_PROV_ADDR
temp_table_name = "STAGING.IdsProvAddrPkey_db2_K_PROV_ADDR_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

df_lnk_KProvAddr_new.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_PROV_ADDR AS T
USING {temp_table_name} AS S
ON (
  T.PROV_ADDR_ID=S.PROV_ADDR_ID
  AND T.PROV_ADDR_TYP_CD=S.PROV_ADDR_TYP_CD
  AND T.PROV_ADDR_EFF_DT_SK=S.PROV_ADDR_EFF_DT_SK
  AND T.SRC_SYS_CD=S.SRC_SYS_CD
)
WHEN MATCHED THEN 
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PROV_ADDR_SK = S.PROV_ADDR_SK
WHEN NOT MATCHED THEN
  INSERT (
    PROV_ADDR_ID,
    PROV_ADDR_TYP_CD,
    PROV_ADDR_EFF_DT_SK,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    PROV_ADDR_SK
  )
  VALUES (
    S.PROV_ADDR_ID,
    S.PROV_ADDR_TYP_CD,
    S.PROV_ADDR_EFF_DT_SK,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.PROV_ADDR_SK
  )
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs => inner join between lnkFullDataJnIn and lnkPKEYxfmOut
df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.PROV_ADDR_ID") == col("lnkPKEYxfmOut.PROV_ADDR_ID"),
        col("lnkFullDataJnIn.PROV_ADDR_TYP_CD") == col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD"),
        col("lnkFullDataJnIn.PROV_ADDR_EFF_DT") == col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)

df_Lnk_IdsProvAddrPkey_Out = df_jn_PKEYs.select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkFullDataJnIn.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnkFullDataJnIn.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("lnkFullDataJnIn.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    col("lnkFullDataJnIn.PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
    col("lnkFullDataJnIn.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
    col("lnkFullDataJnIn.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    col("lnkFullDataJnIn.HCAP_IN").alias("HCAP_IN"),
    col("lnkFullDataJnIn.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    col("lnkFullDataJnIn.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    col("lnkFullDataJnIn.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    col("lnkFullDataJnIn.TERM_DT").alias("TERM_DT"),
    col("lnkFullDataJnIn.ADDR_LN_1").alias("ADDR_LN_1"),
    col("lnkFullDataJnIn.ADDR_LN_2").alias("ADDR_LN_2"),
    col("lnkFullDataJnIn.ADDR_LN_3").alias("ADDR_LN_3"),
    col("lnkFullDataJnIn.CITY_NM").alias("CITY_NM"),
    col("lnkFullDataJnIn.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    col("lnkFullDataJnIn.POSTAL_CD").alias("POSTAL_CD"),
    col("lnkFullDataJnIn.CNTY_NM").alias("CNTY_NM"),
    col("lnkFullDataJnIn.PHN_NO").alias("PHN_NO"),
    col("lnkFullDataJnIn.PHN_NO_EXT").alias("PHN_NO_EXT"),
    col("lnkFullDataJnIn.FAX_NO").alias("FAX_NO"),
    col("lnkFullDataJnIn.FAX_NO_EXT").alias("FAX_NO_EXT"),
    col("lnkFullDataJnIn.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    col("lnkFullDataJnIn.LAT_TX").alias("LAT_TX"),
    col("lnkFullDataJnIn.LONG_TX").alias("LONG_TX"),
    col("lnkFullDataJnIn.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
    col("lnkFullDataJnIn.PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT"),
    col("lnkFullDataJnIn.PROV_ADDR_TYP_CD_ORIG").alias("PROV_ADDR_TYP_CD_ORIG"),
    col("lnkFullDataJnIn.ATND_TEXT").alias("ATND_TEXT")
)

# seq_PROV_ADDR_PKEY => Write to .dat file with certain properties
# Apply rpad for char columns
df_final = df_Lnk_IdsProvAddrPkey_Out.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("PROV_ADDR_SK"),
    col("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD"),
    rpad("PROV_ADDR_EFF_DT",10," ").alias("PROV_ADDR_EFF_DT"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD"),
    col("PROV_ADDR_CNTY_CLS_CD"),
    col("PROV_ADDR_GEO_ACES_RTRN_CD"),
    col("PROV_ADDR_METRORURAL_COV_CD"),
    col("PROV_ADDR_TERM_RSN_CD"),
    rpad("HCAP_IN",1," ").alias("HCAP_IN"),
    rpad("PDX_24_HR_IN",1," ").alias("PDX_24_HR_IN"),
    rpad("PRCTC_LOC_IN",1," ").alias("PRCTC_LOC_IN"),
    rpad("PROV_ADDR_DIR_IN",1," ").alias("PROV_ADDR_DIR_IN"),
    rpad("TERM_DT",10," ").alias("TERM_DT"),
    col("ADDR_LN_1"),
    col("ADDR_LN_2"),
    col("ADDR_LN_3"),
    col("CITY_NM"),
    col("PROV_ADDR_ST_CD"),
    rpad("POSTAL_CD",11," ").alias("POSTAL_CD"),
    col("CNTY_NM"),
    col("PHN_NO"),
    col("PHN_NO_EXT"),
    col("FAX_NO"),
    col("FAX_NO_EXT"),
    col("EMAIL_ADDR_TX"),
    col("LAT_TX"),
    col("LONG_TX"),
    col("PRAD_TYPE_MAIL"),
    rpad("PROV2_PRAD_EFF_DT",10," ").alias("PROV2_PRAD_EFF_DT"),
    col("PROV_ADDR_TYP_CD_ORIG"),
    col("ATND_TEXT")
)

write_files(
    df_final,
    f"{adls_path}/key/PROV_ADDR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)