# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Jag Yelavarthi         2014-07-30            5345                             Original Programming                                                                             IntegrateWrhsDevl     Kalyan Neelm               2014-12-30
# MAGIC 
# MAGIC Sudhir Bomshetty     2017-09-22            5781                          Modifying job to add BCA Provider params                                               IntegrateDev2           Kalyan Neelam             2017-10-05
# MAGIC 
# MAGIC Madhavan B            2018-03-21            5744                          Modifying job to add EYEMED Provider params                                       IntegrateDev2           Kalyan Neelam             2018-04-05
# MAGIC 
# MAGIC Kiran Mulakalapalli       10/28/2021       US468727                  Added new filed ATND_TEXT from Facets to IDS                                IntegrateDev2            Jeyaprasanna                2021-11-18

# MAGIC FctsIdsProvAddrFkey
# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
SrcClctnCd = get_widget_value('SrcClctnCd','')
SrcSysCdStateCd = get_widget_value('SrcSysCdStateCd','')

# Fixed job name from the JSON
jobName = "IdsProvAddrFkey_EE"

# --------------------------------------------------------------------------------
# Stage: seq_PROV_ADDR_PKEY (PxSequentialFile) - Read PROV_ADDR.#SrcSysCd#.pkey.#RunID#.dat
# --------------------------------------------------------------------------------
schema_seq_PROV_ADDR_PKEY = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", TimestampType(), True),
    StructField("PROV_ADDR_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("PROV_ADDR_ID", StringType(), True),
    StructField("PROV_ADDR_TYP_CD", StringType(), True),
    StructField("PROV_ADDR_EFF_DT", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PROV_ADDR_CNTY_CLS_CD", StringType(), True),
    StructField("PROV_ADDR_GEO_ACES_RTRN_CD", StringType(), True),
    StructField("PROV_ADDR_METRORURAL_COV_CD", StringType(), True),
    StructField("PROV_ADDR_TERM_RSN_CD", StringType(), True),
    StructField("HCAP_IN", StringType(), True),
    StructField("PDX_24_HR_IN", StringType(), True),
    StructField("PRCTC_LOC_IN", StringType(), True),
    StructField("PROV_ADDR_DIR_IN", StringType(), True),
    StructField("TERM_DT", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("ADDR_LN_3", StringType(), True),
    StructField("CITY_NM", StringType(), True),
    StructField("PROV_ADDR_ST_CD", StringType(), True),
    StructField("POSTAL_CD", StringType(), True),
    StructField("CNTY_NM", StringType(), True),
    StructField("PHN_NO", StringType(), True),
    StructField("PHN_NO_EXT", StringType(), True),
    StructField("FAX_NO", StringType(), True),
    StructField("FAX_NO_EXT", StringType(), True),
    StructField("EMAIL_ADDR_TX", StringType(), True),
    StructField("LAT_TX", DoubleType(), True),
    StructField("LONG_TX", DoubleType(), True),
    StructField("PRAD_TYPE_MAIL", StringType(), True),
    StructField("PROV2_PRAD_EFF_DT", StringType(), True),
    StructField("PROV_ADDR_TYP_CD_ORIG", StringType(), True),
    StructField("ATND_TEXT", StringType(), True)
])

df_seq_PROV_ADDR_PKEY = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_PROV_ADDR_PKEY)
    .csv(f"{adls_path}/key/PROV_ADDR.{SrcSysCd}.pkey.{RunID}.dat")
)

# --------------------------------------------------------------------------------
# Stage: db2_Term_Dt_Lkp (DB2ConnectorPX) - Read from IDS
# --------------------------------------------------------------------------------
jdbc_url_db2_Term_Dt_Lkp, jdbc_props_db2_Term_Dt_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Term_Dt_Lkp = f"SELECT DISTINCT CLNDR_DT_SK, CLNDR_DT FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"

df_db2_Term_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Term_Dt_Lkp)
    .options(**jdbc_props_db2_Term_Dt_Lkp)
    .option("query", extract_query_db2_Term_Dt_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_K_PROV_ADDR (DB2ConnectorPX) - Read from IDS
# --------------------------------------------------------------------------------
jdbc_url_db2_K_PROV_ADDR, jdbc_props_db2_K_PROV_ADDR = get_db_config(ids_secret_name)
extract_query_db2_K_PROV_ADDR = f"SELECT PROV_ADDR_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT_SK, SRC_SYS_CD, PROV_ADDR_SK FROM {IDSOwner}.K_PROV_ADDR"

df_db2_K_PROV_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_PROV_ADDR)
    .options(**jdbc_props_db2_K_PROV_ADDR)
    .option("query", extract_query_db2_K_PROV_ADDR)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_Eff_Dt_Lkp (DB2ConnectorPX) - Read from IDS
# --------------------------------------------------------------------------------
jdbc_url_db2_Eff_Dt_Lkp, jdbc_props_db2_Eff_Dt_Lkp = get_db_config(ids_secret_name)
extract_query_db2_Eff_Dt_Lkp = f"SELECT DISTINCT CLNDR_DT_SK, CLNDR_DT FROM {IDSOwner}.CLNDR_DT WHERE CLNDR_DT_SK NOT IN ('INVALID','NA','NULL','UNK')"

df_db2_Eff_Dt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_Eff_Dt_Lkp)
    .options(**jdbc_props_db2_Eff_Dt_Lkp)
    .option("query", extract_query_db2_Eff_Dt_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_CD_MPPNG_LkpData (PxDataSet) - Read from CD_MPPNG.ds -> parquet
# --------------------------------------------------------------------------------
df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

# --------------------------------------------------------------------------------
# Stage: fltr_FilterData (PxFilter)
# --------------------------------------------------------------------------------
df_fltr_FilterData_lnkProvAddrCntyCls = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS COUNTY CLASSIFICATION'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_fltr_FilterData_lnkProvAddrMetroRuralCovCd = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS METRO RURAL COVERAGE'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_fltr_FilterData_lnkProvAddrSt = df_ds_CD_MPPNG_LkpData.filter(
    f"SRC_SYS_CD='{SrcSysCdStateCd}' AND SRC_CLCTN_CD='{SrcClctnCd}' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='STATE' AND TRGT_DOMAIN_NM='STATE'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_fltr_FilterData_lnkProvAddrTermRsnCd = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='PROVIDER ADDRESS TERMINATION REASON' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS TERMINATION REASON'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_fltr_FilterData_lnkProvAddrTypCd = df_ds_CD_MPPNG_LkpData.filter(
    "SRC_SYS_CD='FACETS' AND SRC_CLCTN_CD='FACETS DBO' AND TRGT_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='PROVIDER ADDRESS TYPE' AND TRGT_DOMAIN_NM='PROVIDER ADDRESS TYPE'"
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# --------------------------------------------------------------------------------
# Stage: lkp_ForeignKeys (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_ForeignKeys = (
    df_seq_PROV_ADDR_PKEY.alias("Lnk_ProvAddrFkey_InAbc")
    .join(
        df_db2_Term_Dt_Lkp.alias("Ref_Term_Dt"),
        F.col("Lnk_ProvAddrFkey_InAbc.TERM_DT") == F.col("Ref_Term_Dt.CLNDR_DT_SK"),
        "left"
    )
    .join(
        df_fltr_FilterData_lnkProvAddrTypCd.alias("lnkProvAddrTypCd"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_TYP_CD") == F.col("lnkProvAddrTypCd.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_FilterData_lnkProvAddrCntyCls.alias("lnkProvAddrCntyCls"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_CNTY_CLS_CD") == F.col("lnkProvAddrCntyCls.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_FilterData_lnkProvAddrTermRsnCd.alias("lnkProvAddrTermRsnCd"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_TERM_RSN_CD") == F.col("lnkProvAddrTermRsnCd.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_FilterData_lnkProvAddrMetroRuralCovCd.alias("lnkProvAddrMetroRuralCovCd"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_METRORURAL_COV_CD") == F.col("lnkProvAddrMetroRuralCovCd.SRC_CD"),
        "left"
    )
    .join(
        df_fltr_FilterData_lnkProvAddrSt.alias("lnkProvAddrSt"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_ST_CD") == F.col("lnkProvAddrSt.SRC_CD"),
        "left"
    )
    .join(
        df_db2_K_PROV_ADDR.alias("Ref_Mail_Prov_Addr"),
        (
            (F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_ID") == F.col("Ref_Mail_Prov_Addr.PROV_ADDR_ID")) &
            (F.col("Lnk_ProvAddrFkey_InAbc.PRAD_TYPE_MAIL") == F.col("Ref_Mail_Prov_Addr.PROV_ADDR_TYP_CD")) &
            (F.col("Lnk_ProvAddrFkey_InAbc.PROV2_PRAD_EFF_DT") == F.col("Ref_Mail_Prov_Addr.PROV_ADDR_EFF_DT_SK")) &
            (F.col("Lnk_ProvAddrFkey_InAbc.SRC_SYS_CD") == F.col("Ref_Mail_Prov_Addr.SRC_SYS_CD"))
        ),
        "left"
    )
    .join(
        df_db2_Eff_Dt_Lkp.alias("Ref_Eff_Dt"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_EFF_DT") == F.col("Ref_Eff_Dt.CLNDR_DT_SK"),
        "left"
    )
    .select(
        F.col("Lnk_ProvAddrFkey_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_ProvAddrFkey_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
        F.col("Ref_Eff_Dt.CLNDR_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_CNTY_CLS_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_GEO_ACES_RTRN_CD").alias("PROV_ADDR_GEO_ACES_RTRN_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_METRORURAL_COV_CD").alias("PROV_ADDR_METRORURAL_COV_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_TERM_RSN_CD").alias("PROV_ADDR_TERM_RSN_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.HCAP_IN").alias("HCAP_IN"),
        F.col("Lnk_ProvAddrFkey_InAbc.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        F.col("Lnk_ProvAddrFkey_InAbc.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        F.col("Ref_Term_Dt.CLNDR_DT_SK").alias("TERM_DT_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.TERM_DT").alias("TERM_DT"),
        F.col("Lnk_ProvAddrFkey_InAbc.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("Lnk_ProvAddrFkey_InAbc.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("Lnk_ProvAddrFkey_InAbc.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("Lnk_ProvAddrFkey_InAbc.CITY_NM").alias("CITY_NM"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.POSTAL_CD").alias("POSTAL_CD"),
        F.col("Lnk_ProvAddrFkey_InAbc.CNTY_NM").alias("CNTY_NM"),
        F.col("Lnk_ProvAddrFkey_InAbc.PHN_NO").alias("PHN_NO"),
        F.col("Lnk_ProvAddrFkey_InAbc.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("Lnk_ProvAddrFkey_InAbc.FAX_NO").alias("FAX_NO"),
        F.col("Lnk_ProvAddrFkey_InAbc.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("Lnk_ProvAddrFkey_InAbc.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("Lnk_ProvAddrFkey_InAbc.LAT_TX").alias("LAT_TX"),
        F.col("Lnk_ProvAddrFkey_InAbc.LONG_TX").alias("LONG_TX"),
        F.col("Lnk_ProvAddrFkey_InAbc.PRAD_TYPE_MAIL").alias("PRAD_TYPE_MAIL"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV2_PRAD_EFF_DT").alias("PROV2_PRAD_EFF_DT"),
        F.col("Lnk_ProvAddrFkey_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnkProvAddrTypCd.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
        F.col("lnkProvAddrCntyCls.CD_MPPNG_SK").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
        F.col("lnkProvAddrTermRsnCd.CD_MPPNG_SK").alias("PROV_ADDR_TERM_RSN_CD_SK"),
        F.col("lnkProvAddrMetroRuralCovCd.CD_MPPNG_SK").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
        F.col("lnkProvAddrSt.CD_MPPNG_SK").alias("PROV_ADDR_ST_CD_SK"),
        F.col("Ref_Mail_Prov_Addr.PROV_ADDR_SK").alias("MAIL_PROV_ADDRESS_SK"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
        F.col("Lnk_ProvAddrFkey_InAbc.PROV_ADDR_TYP_CD_ORIG").alias("PROV_ADDR_TYP_CD_ORIG"),
        F.col("Lnk_ProvAddrFkey_InAbc.ATND_TEXT").alias("ATND_TEXT")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfm_CheckLkpResults_sv = (
    df_lkp_ForeignKeys
    .withColumn(
        "svEffDtLkpCheck",
        F.when(
            (F.col("PROV_ADDR_EFF_DT_SK").isNull()) & (F.col("PROV_ADDR_EFF_DT") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svTermDtLkpCheck",
        F.when(
            (F.col("TERM_DT_SK").isNull()) & (F.col("TERM_DT") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMailProvAddrLkpCheck",
        F.lit("N")
    )
    .withColumn(
        "svProvAddrStCdLkpCheck",
        F.when(
            (F.col("PROV_ADDR_ST_CD_SK").isNull()) & (F.trim(F.col("PROV_ADDR_ST_CD")) != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAddrTypCdLkpCheck",
        F.when(
            (F.col("PROV_ADDR_TYP_CD_SK").isNull()) & (F.col("PROV_ADDR_TYP_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAddrCntyClsLkpCheck",
        F.when(
            (F.col("PROV_ADDR_CNTY_CLS_CD_SK").isNull()) & (F.col("PROV_ADDR_CNTY_CLS_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svAddrTermRsnLkpCheck",
        F.when(
            (F.col("PROV_ADDR_TERM_RSN_CD_SK").isNull()) & (F.col("PROV_ADDR_TERM_RSN_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMtroRuralCovLkpCheck",
        F.when(
            (F.col("PROV_ADDR_METRORURAL_COV_CD_SK").isNull()) & (F.col("PROV_ADDR_METRORURAL_COV_CD") != F.lit("NA")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Lnk_Main: no constraint => all rows, with specified output columns/expressions
df_xfm_CheckLkpResults_Lnk_Main = df_xfm_CheckLkpResults_sv.select(
    F.col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.when(F.col("PROV_ADDR_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_ADDR_TYP_CD_SK")).alias("PROV_ADDR_TYP_CD_SK"),
    F.when(F.col("PROV_ADDR_EFF_DT_SK").isNull(), F.lit("1753-01-01")).otherwise(F.col("PROV_ADDR_EFF_DT_SK")).alias("PROV_ADDR_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.col("MAIL_PROV_ADDRESS_SK")
    ).otherwise(
        F.when(
            (F.col("SRC_SYS_CD") == F.lit("NABP")) |
            (F.trim(F.col("PRAD_TYPE_MAIL")) == F.trim(F.col("PROV_ADDR_TYP_CD_ORIG"))),
            F.col("PROV_ADDR_SK")
        ).otherwise(
            F.when(
                (F.col("PROV2_PRAD_EFF_DT") == F.lit("NA")),
                F.lit(1)
            ).otherwise(
                F.when(
                    F.col("MAIL_PROV_ADDRESS_SK").isNull(),
                    F.lit(1)
                ).otherwise(F.col("MAIL_PROV_ADDRESS_SK"))
            )
        )
    ).alias("MAIL_PROV_ADRESS_SK"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.lit(1)
    ).otherwise(
        F.when(F.col("PROV_ADDR_CNTY_CLS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_ADDR_CNTY_CLS_CD_SK"))
    ).alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.lit(1)
    ).otherwise(F.col("PROV_ADDR_GEO_ACES_RTRN_CD")).alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.lit(1)
    ).otherwise(
        F.when(F.col("PROV_ADDR_METRORURAL_COV_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_ADDR_METRORURAL_COV_CD_SK"))
    ).alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.lit(1)
    ).otherwise(
        F.when(F.col("PROV_ADDR_TERM_RSN_CD") == F.lit("NA"), F.lit(1))
        .otherwise(
            F.when(F.col("PROV_ADDR_TERM_RSN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_ADDR_TERM_RSN_CD_SK"))
        )
    ).alias("PROV_ADDR_TERM_RSN_CD_SK"),
    F.col("HCAP_IN").alias("HCAP_IN"),
    F.col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.when(F.col("TERM_DT_SK").isNull(), F.lit("2199-12-31")).otherwise(F.col("TERM_DT_SK")).alias("TERM_DT_SK"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.when(F.col("PROV_ADDR_ST_CD_SK").isNotNull(), F.col("PROV_ADDR_ST_CD_SK"))
        .otherwise(
            F.when(F.col("PROV_ADDR_ST_CD") == F.lit("1"), F.lit(1)).otherwise(F.lit(0))
        )
    ).otherwise(
        F.when(F.col("PROV_ADDR_ST_CD_SK").isNull(), F.lit(1)).otherwise(F.col("PROV_ADDR_ST_CD_SK"))
    ).alias("PROV_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.when(
        (F.col("SRC_SYS_CD").isin("BCA", "EYEMED")),
        F.when(
            F.col("PHN_NO").isNull() | (F.trim(F.col("PHN_NO")) == F.lit("")),
            F.lit(None)
        ).otherwise(F.trim(F.col("PHN_NO")))
    ).otherwise(
        F.when(F.trim(F.coalesce(F.col("PHN_NO"), F.lit(""))) == F.lit(""), F.lit(" ")).otherwise(F.col("PHN_NO"))
    ).alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("LAT_TX").alias("LAT_TX"),
    F.col("LONG_TX").alias("LONG_TX"),
    F.col("ATND_TEXT").alias("ATND_TEXT")
)

# Lnk_UNK: constraint => first row in the dataset
window_unk = Window.orderBy(F.lit(1))
df_with_row_unk = df_xfm_CheckLkpResults_sv.withColumn("row_num_unk", F.row_number().over(window_unk))
df_xfm_CheckLkpResults_Lnk_UNK_all = df_with_row_unk.filter("row_num_unk = 1")
df_xfm_CheckLkpResults_Lnk_UNK = df_xfm_CheckLkpResults_Lnk_UNK_all.select(
    F.lit(0).alias("PROV_ADDR_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("PROV_ADDR_ID"),
    F.lit(0).alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MAIL_PROV_ADRESS_SK"),
    F.lit(0).alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    F.lit("").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.lit(0).alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    F.lit(0).alias("PROV_ADDR_TERM_RSN_CD_SK"),
    F.lit("N").alias("HCAP_IN"),
    F.lit("N").alias("PDX_24_HR_IN"),
    F.lit("N").alias("PRCTC_LOC_IN"),
    F.lit("N").alias("PROV_ADDR_DIR_IN"),
    F.lit("1753-01-01").alias("TERM_DT_SK"),
    F.lit(None).alias("ADDR_LN_1"),
    F.lit(None).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.lit("UNK").alias("CITY_NM"),
    F.lit(0).alias("PROV_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("POSTAL_CD"),
    F.lit("UNK").alias("CNTY_NM"),
    F.lit(None).alias("PHN_NO"),
    F.lit(None).alias("PHN_NO_EXT"),
    F.lit(None).alias("FAX_NO"),
    F.lit(None).alias("FAX_NO_EXT"),
    F.lit(None).alias("EMAIL_ADDR_TX"),
    F.lit(0).alias("LAT_TX"),
    F.lit(0).alias("LONG_TX"),
    F.lit(None).alias("ATND_TEXT")
)

# Lnk_NA: constraint => first row in the dataset
window_na = Window.orderBy(F.lit(1))
df_with_row_na = df_xfm_CheckLkpResults_sv.withColumn("row_num_na", F.row_number().over(window_na))
df_xfm_CheckLkpResults_Lnk_NA_all = df_with_row_na.filter("row_num_na = 1")
df_xfm_CheckLkpResults_Lnk_NA = df_xfm_CheckLkpResults_Lnk_NA_all.select(
    F.lit(1).alias("PROV_ADDR_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PROV_ADDR_ID"),
    F.lit(1).alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MAIL_PROV_ADRESS_SK"),
    F.lit(1).alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    F.lit("").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.lit(1).alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    F.lit(1).alias("PROV_ADDR_TERM_RSN_CD_SK"),
    F.lit("N").alias("HCAP_IN"),
    F.lit("N").alias("PDX_24_HR_IN"),
    F.lit("N").alias("PRCTC_LOC_IN"),
    F.lit("N").alias("PROV_ADDR_DIR_IN"),
    F.lit("1753-01-01").alias("TERM_DT_SK"),
    F.lit(None).alias("ADDR_LN_1"),
    F.lit(None).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.lit("NA").alias("CITY_NM"),
    F.lit(1).alias("PROV_ADDR_ST_CD_SK"),
    F.lit("NA").alias("POSTAL_CD"),
    F.lit("NA").alias("CNTY_NM"),
    F.lit(None).alias("PHN_NO"),
    F.lit(None).alias("PHN_NO_EXT"),
    F.lit(None).alias("FAX_NO"),
    F.lit(None).alias("FAX_NO_EXT"),
    F.lit(None).alias("EMAIL_ADDR_TX"),
    F.lit(0).alias("LAT_TX"),
    F.lit(0).alias("LONG_TX"),
    F.lit(None).alias("ATND_TEXT")
)

# EffDtSkLkpFail: filter svEffDtLkpCheck = 'Y'
df_EffDtSkLkpFail = df_xfm_CheckLkpResults_sv.filter("svEffDtLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("PROV_ADDR_EFF_DT")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# TermDtLkpFail: filter svTermDtLkpCheck = 'Y'
df_TermDtLkpFail = df_xfm_CheckLkpResults_sv.filter("svTermDtLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("TERM_DT")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# MailProvAddrLkupFail: filter svMailProvAddrLkpCheck = 'Y'
df_MailProvAddrLkupFail = df_xfm_CheckLkpResults_sv.filter("svMailProvAddrLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("PROV_ADDR").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("PROV_ADDR_ID"), F.lit(";"), F.col("PRAD_TYPE_MAIL"), F.lit(";"), F.col("PROV2_PRAD_EFF_DT"), F.lit(";"), F.col("SRC_SYS_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ProvAddrStCdLkupFail: filter svProvAddrStCdLkpCheck = 'Y'
df_ProvAddrStCdLkupFail = df_xfm_CheckLkpResults_sv.filter("svProvAddrStCdLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"), F.lit("STATE"), F.lit(";"), F.lit("STATE"), F.lit(";"), F.col("PROV_ADDR_ST_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ProvAddrTypCdSkLkupFail: filter svAddrTypCdLkpCheck = 'Y'
df_ProvAddrTypCdSkLkupFail = df_xfm_CheckLkpResults_sv.filter("svAddrTypCdLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"), F.lit("PROVIDER ADDRESS TYPE"), F.lit(";"), F.lit("PROVIDER ADDRESS TYPE"), F.lit(";"), F.col("PROV_ADDR_TYP_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ProvAddrCntyClsLkpFail: filter svAddrCntyClsLkpCheck = 'Y'
df_ProvAddrCntyClsLkpFail = df_xfm_CheckLkpResults_sv.filter("svAddrCntyClsLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"), F.lit("PROVIDER ADDRESS COUNTY CLASSIFICATION"), F.lit(";"), F.lit("PROVIDER ADDRESS COUNTY CLASSIFICATION"), F.lit(";"), F.col("PROV_ADDR_CNTY_CLS_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ProvAddrTermRsnCdLkpFail: filter svAddrTermRsnLkpCheck = 'Y'
df_ProvAddrTermRsnCdLkpFail = df_xfm_CheckLkpResults_sv.filter("svAddrTermRsnLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"), F.lit("PROVIDER ADDRESS TERMINATION REASON"), F.lit(";"), F.lit("PROVIDER ADDRESS TERMINATION REASON"), F.lit(";"), F.col("PROV_ADDR_TERM_RSN_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ProvMetroRuralCovLkpFail: filter svMtroRuralCovLkpCheck = 'Y'
df_ProvMetroRuralCovLkpFail = df_xfm_CheckLkpResults_sv.filter("svMtroRuralCovLkpCheck = 'Y'").select(
    F.col("PROV_ADDR_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(jobName).alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.lit("FACETS DBO"), F.lit(";"), F.lit("IDS"), F.lit(";"), F.lit("PROVIDER ADDRESS METRO RURAL COVERAGE"), F.lit(";"), F.lit("PROVIDER ADDRESS METRO RURAL COVERAGE"), F.lit(";"), F.col("PROV_ADDR_METRORURAL_COV_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: fnl_NA_UNK_Streams (PxFunnel) => union of Lnk_Main, Lnk_UNK, Lnk_NA
# --------------------------------------------------------------------------------
df_fnl_NA_UNK_Streams = df_xfm_CheckLkpResults_Lnk_Main.unionByName(df_xfm_CheckLkpResults_Lnk_UNK).unionByName(df_xfm_CheckLkpResults_Lnk_NA)

# Output pin: Lnk_IdsProvAddrFkey_Out => direct pass of columns with same name
df_Lnk_IdsProvAddrFkey_Out = df_fnl_NA_UNK_Streams.select(
    "PROV_ADDR_SK",
    "SRC_SYS_CD_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MAIL_PROV_ADRESS_SK",
    "PROV_ADDR_CNTY_CLS_CD_SK",
    "PROV_ADDR_GEO_ACES_RTRN_CD_TX",
    "PROV_ADDR_METRORURAL_COV_CD_SK",
    "PROV_ADDR_TERM_RSN_CD_SK",
    "HCAP_IN",
    "PDX_24_HR_IN",
    "PRCTC_LOC_IN",
    "PROV_ADDR_DIR_IN",
    "TERM_DT_SK",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "PROV_ADDR_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR_TX",
    "LAT_TX",
    "LONG_TX",
    "ATND_TEXT"
)

# --------------------------------------------------------------------------------
# Stage: seq_ProvAddrFKey (PxSequentialFile) - write final file
# --------------------------------------------------------------------------------
# Apply rpad where char length is known
df_seq_ProvAddrFKey_final = df_Lnk_IdsProvAddrFkey_Out \
    .withColumn("PROV_ADDR_EFF_DT_SK", F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " ")) \
    .withColumn("HCAP_IN", F.rpad(F.col("HCAP_IN"), 1, " ")) \
    .withColumn("PDX_24_HR_IN", F.rpad(F.col("PDX_24_HR_IN"), 1, " ")) \
    .withColumn("PRCTC_LOC_IN", F.rpad(F.col("PRCTC_LOC_IN"), 1, " ")) \
    .withColumn("PROV_ADDR_DIR_IN", F.rpad(F.col("PROV_ADDR_DIR_IN"), 1, " ")) \
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " ")) \
    .withColumn("POSTAL_CD", F.rpad(F.col("POSTAL_CD"), 11, " ")) 

df_seq_ProvAddrFKey_final = df_seq_ProvAddrFKey_final.select(
    "PROV_ADDR_SK",
    "SRC_SYS_CD_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MAIL_PROV_ADRESS_SK",
    "PROV_ADDR_CNTY_CLS_CD_SK",
    "PROV_ADDR_GEO_ACES_RTRN_CD_TX",
    "PROV_ADDR_METRORURAL_COV_CD_SK",
    "PROV_ADDR_TERM_RSN_CD_SK",
    "HCAP_IN",
    "PDX_24_HR_IN",
    "PRCTC_LOC_IN",
    "PROV_ADDR_DIR_IN",
    "TERM_DT_SK",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "PROV_ADDR_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR_TX",
    "LAT_TX",
    "LONG_TX",
    "ATND_TEXT"
)

write_files(
    df_seq_ProvAddrFKey_final,
    f"{adls_path}/load/PROV_ADDR.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Fnl_LkpFail (PxFunnel) => union of multiple fail dfs
# --------------------------------------------------------------------------------
df_Fnl_LkpFail = df_ProvAddrTypCdSkLkupFail.unionByName(df_EffDtSkLkpFail)\
    .unionByName(df_MailProvAddrLkupFail)\
    .unionByName(df_ProvAddrStCdLkupFail)\
    .unionByName(df_ProvAddrCntyClsLkpFail)\
    .unionByName(df_ProvAddrTermRsnCdLkpFail)\
    .unionByName(df_ProvMetroRuralCovLkpFail)\
    .unionByName(df_TermDtLkpFail)

df_Fnl_LkpFail_out = df_Fnl_LkpFail.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

# --------------------------------------------------------------------------------
# Stage: Seq_FKeyFailFile (PxSequentialFile) - write final fail file
# --------------------------------------------------------------------------------
df_Seq_FKeyFailFile_final = df_Fnl_LkpFail_out

write_files(
    df_Seq_FKeyFailFile_final,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{jobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)