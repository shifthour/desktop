# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja sunkara          2014-08-07              5345                             Original Programming                                                                         IntegrateWrhsDevl    Kalyan Neelam              2015-01-05

# MAGIC IdsProvLocFkey_EE
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
from pyspark.sql.types import *
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

schema_seq_PROV_LOC_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("PROV_LOC_SK", IntegerType(), False),
    StructField("PROV_ID", StringType(), False),
    StructField("PROV_ADDR_ID", StringType(), False),
    StructField("PROV_ADDR_TYP_CD", StringType(), False),
    StructField("PROV_ADDR_EFF_DT", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("PROV_ADDR_SK", IntegerType(), False),
    StructField("PROV_SK", IntegerType(), False),
    StructField("PRI_ADDR_IN", StringType(), False),
    StructField("REMIT_ADDR_IN", StringType(), False)
])

df_seq_PROV_LOC_Pkey = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seq_PROV_LOC_Pkey)
    .load(f"{adls_path}/key/PROV_LOC.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url_db2_K_PROV_ADDR_In, jdbc_props_db2_K_PROV_ADDR_In = get_db_config(ids_secret_name)
extract_query_db2_K_PROV_ADDR_In = f"SELECT PROV_ADDR_ID,PROV_ADDR_TYP_CD,PROV_ADDR_EFF_DT_SK,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,PROV_ADDR_SK FROM {IDSOwner}.K_PROV_ADDR"
df_db2_K_PROV_ADDR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_PROV_ADDR_In)
    .options(**jdbc_props_db2_K_PROV_ADDR_In)
    .option("query", extract_query_db2_K_PROV_ADDR_In)
    .load()
)

jdbc_url_db2_K_PROV_In, jdbc_props_db2_K_PROV_In = get_db_config(ids_secret_name)
extract_query_db2_K_PROV_In = f"SELECT PROV_ID,SRC_SYS_CD,PROV_SK FROM {IDSOwner}.K_PROV"
df_db2_K_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_PROV_In)
    .options(**jdbc_props_db2_K_PROV_In)
    .option("query", extract_query_db2_K_PROV_In)
    .load()
)

jdbc_url_db2_CLNDR_DT_EffDt_Lkp, jdbc_props_db2_CLNDR_DT_EffDt_Lkp = get_db_config(ids_secret_name)
extract_query_db2_CLNDR_DT_EffDt_Lkp = f"SELECT CLNDR_DT_SK FROM {IDSOwner}.CLNDR_DT"
df_db2_CLNDR_DT_EffDt_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLNDR_DT_EffDt_Lkp)
    .options(**jdbc_props_db2_CLNDR_DT_EffDt_Lkp)
    .option("query", extract_query_db2_CLNDR_DT_EffDt_Lkp)
    .load()
)

df_ds_CD_MPPNG_LkpData_raw = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_ds_CD_MPPNG_LkpData = df_ds_CD_MPPNG_LkpData_raw.select(
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

df_fltr_FilterData_filtered = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == 'FACETS') &
    (F.col("SRC_CLCTN_CD") == 'FACETS DBO') &
    (F.col("TRGT_CLCTN_CD") == 'IDS') &
    (F.col("SRC_DOMAIN_NM") == 'PROVIDER ADDRESS TYPE') &
    (F.col("TRGT_DOMAIN_NM") == 'PROVIDER ADDRESS TYPE')
)
df_fltr_FilterData_out = df_fltr_FilterData_filtered.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

df_main = (
    df_seq_PROV_LOC_Pkey.alias("main")
    .join(
        df_db2_K_PROV_ADDR_In.alias("ref_ProvAddrSk"),
        (F.col("main.PROV_ADDR_ID") == F.col("ref_ProvAddrSk.PROV_ADDR_ID")) &
        (F.col("main.PROV_ADDR_TYP_CD") == F.col("ref_ProvAddrSk.PROV_ADDR_TYP_CD")) &
        (F.col("main.PROV_ADDR_EFF_DT") == F.col("ref_ProvAddrSk.PROV_ADDR_EFF_DT_SK")) &
        (F.col("main.SRC_SYS_CD") == F.col("ref_ProvAddrSk.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_db2_K_PROV_In.alias("ref_ProvSk"),
        (F.col("main.PROV_ID") == F.col("ref_ProvSk.PROV_ID")) &
        (F.col("main.SRC_SYS_CD") == F.col("ref_ProvSk.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_fltr_FilterData_out.alias("ref_ProvAddrTypCdSk"),
        (F.col("main.PROV_ADDR_TYP_CD") == F.col("ref_ProvAddrTypCdSk.SRC_CD")),
        "left"
    )
    .join(
        df_db2_CLNDR_DT_EffDt_Lkp.alias("ref_EffDtSk"),
        (F.col("main.PROV_ADDR_EFF_DT") == F.col("ref_EffDtSk.CLNDR_DT_SK")),
        "left"
    )
)

df_Lkup_Fkey = df_main.select(
    F.col("main.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("main.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("main.PROV_LOC_SK").alias("PROV_LOC_SK"),
    F.col("main.PROV_ID").alias("PROV_ID"),
    F.col("main.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("main.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("main.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("main.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("main.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("main.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("main.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("main.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("main.REMIT_ADDR_IN").alias("REMIT_ADDR_IN"),
    F.col("ref_ProvAddrSk.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("ref_ProvSk.PROV_SK").alias("PROV_SK"),
    F.col("ref_ProvAddrTypCdSk.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("ref_EffDtSk.CLNDR_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

df_xfm = (
    df_Lkup_Fkey
    .withColumn(
        "svProvAddrTypCdSkLkupChk",
        F.when(
            F.col("PROV_ADDR_TYP_CD_SK").isNull() & (F.col("PROV_ADDR_TYP_CD") != 'NA'),
            'Y'
        ).otherwise('N')
    )
    .withColumn(
        "svEffDtSkLkupChk",
        F.when(
            F.col("PROV_ADDR_EFF_DT_SK").isNull() & (F.col("PROV_ADDR_EFF_DT") != 'NA'),
            'Y'
        ).otherwise('N')
    )
    .withColumn(
        "svProvAddrSkLkupChk",
        F.when(
            F.col("PROV_ADDR_SK").isNull() & (F.col("PROV_ADDR_ID") != 'NA'),
            'Y'
        ).otherwise('N')
    )
    .withColumn(
        "svProvSkLkupChk",
        F.when(
            F.col("PROV_SK").isNull() & (F.col("PROV_ID") != 'NA'),
            'Y'
        ).otherwise('N')
    )
    .withColumn(
        "row_num",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )
)

df_Lnk_Main = df_xfm.select(
    F.col("PROV_LOC_SK").alias("PROV_LOC_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.when(
        (F.col("PROV_ADDR_TYP_CD") == 'UNK') | (F.col("PROV_ADDR_TYP_CD_SK").isNull()),
        F.lit(0)
    ).when(
        F.col("PROV_ADDR_TYP_CD") == 'NA',
        F.lit(1)
    ).otherwise(F.col("PROV_ADDR_TYP_CD_SK")).alias("PROV_ADDR_TYP_CD_SK"),
    F.when(
        F.col("PROV_ADDR_EFF_DT_SK").isNull(),
        F.lit('1753-01-01')
    ).otherwise(F.col("PROV_ADDR_EFF_DT_SK")).alias("PROV_ADDR_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("PROV_ADDR_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("PROV_ADDR_SK")).alias("PROV_ADDR_SK"),
    F.when(
        F.col("PROV_SK").isNull(),
        F.lit(0)
    ).otherwise(F.col("PROV_SK")).alias("PROV_SK"),
    F.col("PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("REMIT_ADDR_IN").alias("REMIT_ADDR_IN")
)

df_ProvAddrTypCdSkLkupFail = df_xfm.filter(F.col("svProvAddrTypCdSkLkupChk") == 'Y').select(
    F.col("PROV_LOC_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsProvLocFkey_EE").alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.lit(SrcSysCd), F.lit(';'),
        F.lit("FACETS DBO"), F.lit(';'),
        F.lit("IDS"), F.lit(';'),
        F.lit("PROVIDER ADDRESS TYPE"), F.lit(';'),
        F.lit("PROVIDER ADDRESS TYPE"), F.lit(';'),
        F.col("PROV_ADDR_TYP_CD")
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_ProvAddrSkLkupFail = df_xfm.filter(F.col("svProvAddrSkLkupChk") == 'Y').select(
    F.col("PROV_LOC_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsProvLocFkey_EE").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("PROV_ADDR").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("PROV_ADDR_ID"), F.lit(';'),
        F.col("PROV_ADDR_TYP_CD"), F.lit(';'),
        F.col("PROV_ADDR_EFF_DT"), F.lit(';'),
        F.lit(SrcSysCd)
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_EffDtSkLkupFail = df_xfm.filter(F.col("svEffDtSkLkupChk") == 'Y').select(
    F.col("PROV_LOC_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsProvLocFkey_EE").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("CLNDR_DT").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.lit(SrcSysCd), F.lit(';'),
        F.col("PROV_ADDR_EFF_DT")
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_ProvSkLkupFail = df_xfm.filter(F.col("svProvSkLkupChk") == 'Y').select(
    F.col("PROV_LOC_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsProvLocFkey_EE").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("PROV").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("PROV_ID"), F.lit(';'),
        F.lit(SrcSysCd)
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Lnk_UNK = df_xfm.filter(F.col("row_num") == 1).select(
    F.lit(0).alias("PROV_LOC_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("PROV_ID"),
    F.lit("UNK").alias("PROV_ADDR_ID"),
    F.lit(0).alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PROV_ADDR_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit("N").alias("PRI_ADDR_IN"),
    F.lit("N").alias("REMIT_ADDR_IN")
)

df_Lnk_NA = df_xfm.filter(F.col("row_num") == 1).select(
    F.lit(1).alias("PROV_LOC_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PROV_ID"),
    F.lit("NA").alias("PROV_ADDR_ID"),
    F.lit(1).alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PROV_ADDR_SK"),
    F.lit(1).alias("PROV_SK"),
    F.lit("N").alias("PRI_ADDR_IN"),
    F.lit("N").alias("REMIT_ADDR_IN")
)

df_fnl_NA_UNK_Streams = (
    df_Lnk_Main.unionByName(df_Lnk_UNK)
    .unionByName(df_Lnk_NA)
)

df_fnl_NA_UNK_Streams_out = df_fnl_NA_UNK_Streams.select(
    "PROV_LOC_SK",
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ADDR_SK",
    "PROV_SK",
    "PRI_ADDR_IN",
    "REMIT_ADDR_IN"
)

df_fnl_NA_UNK_Streams_out_rpad = (
    df_fnl_NA_UNK_Streams_out
    .withColumn("PROV_ID", F.rpad(F.col("PROV_ID"), <...>, " "))
    .withColumn("PROV_ADDR_ID", F.rpad(F.col("PROV_ADDR_ID"), <...>, " "))
    .withColumn("PROV_ADDR_EFF_DT_SK", F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " "))
    .withColumn("PRI_ADDR_IN", F.rpad(F.col("PRI_ADDR_IN"), 1, " "))
    .withColumn("REMIT_ADDR_IN", F.rpad(F.col("REMIT_ADDR_IN"), 1, " "))
)

write_files(
    df_fnl_NA_UNK_Streams_out_rpad,
    f"{adls_path}/load/PROV_LOC.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_fnl_lkpfail = (
    df_ProvAddrTypCdSkLkupFail
    .unionByName(df_EffDtSkLkupFail)
    .unionByName(df_ProvAddrSkLkupFail)
    .unionByName(df_ProvSkLkupFail)
)

df_fnl_lkpfail_out = df_fnl_lkpfail.select(
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

df_fnl_lkpfail_out_rpad = (
    df_fnl_lkpfail_out
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " "))
)

write_files(
    df_fnl_lkpfail_out_rpad,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsProvLocFkey_EE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)