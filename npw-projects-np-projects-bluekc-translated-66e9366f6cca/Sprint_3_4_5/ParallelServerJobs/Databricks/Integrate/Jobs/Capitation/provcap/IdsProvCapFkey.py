# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 11/12/08 10:10:08 Batch  14927_36617 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_3 11/12/08 10:04:14 Batch  14927_36261 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 11/06/08 08:57:33 Batch  14921_32258 PROMOTE bckcett testIDS u03651 steph for Brent
# MAGIC ^1_1 11/06/08 08:52:23 Batch  14921_31946 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_2 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_4 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_4 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_3 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 02/15/08 17:33:50 Batch  14656_63234 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 02/15/08 17:32:14 Batch  14656_63137 INIT bckcett testIDS dsadm BLS FOR ON
# MAGIC ^1_1 02/13/08 16:22:03 Batch  14654_58928 PROMOTE bckcett testIDS u03651 steph for Ollie
# MAGIC ^1_1 02/13/08 16:20:24 Batch  14654_58827 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 04/04/07 10:04:56 Batch  14339_36302 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/20/05 07:35:13 Batch  13716_27318 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 07/20/05 07:29:40 Batch  13716_26986 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_6 06/22/05 09:10:14 Batch  13688_33020 PROMOTE bckcett VERSION u06640 Ralph
# MAGIC ^1_6 06/22/05 09:08:48 Batch  13688_32937 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_5 06/20/05 13:59:07 Batch  13686_50351 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_4 06/17/05 10:29:55 Batch  13683_37806 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 06/17/05 10:28:29 Batch  13683_37715 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 06/15/05 08:54:19 Batch  13681_32063 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 06/09/05 13:18:55 Batch  13675_47941 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     IdsProvCapFkey
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker  -  4/27/2005  -  Originally programmed
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                  02/13/2008     Production Support          Changed FKey to use LAST_UPDT                       devlIDS                         Steph Goddard            02/13/2008
# MAGIC                                                                                                         RUN_CYC_EXTN_SK that is passed
# MAGIC                                                                                                         into the job rather than hardcode to 0.
# MAGIC                                                                                                         Fixed for balancing errors.
# MAGIC               NOTE:  Approved 2/13/08 for production fix - but this process needs to be changed to use environment variables.
# MAGIC            
# MAGIC Brent Leland                    06-26-2008     3567 Primary Key            Added output for paymnt sum foriegn key lookup   devlIDS                        Steph Goddard               07/03/2008
# MAGIC 
# MAGIC Parik                               2008-09-08      3567 Primary key           Change in the format of the input file coming in        devlIDS                        Steph Goddard               09/10/2008
# MAGIC                                                                                                       the foreign key lookup 
# MAGIC Ralph Tucker                  2011-05-10      TTR-1058                     Change default on PAYMT_SUM_SK to 'NA'           IntegrateNewDevl        SAndrew                       2011-05-12

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from Primary Key job.
# MAGIC Load working table then join to K_PAYMT_SUM table
# MAGIC Remove duplicate keys
# MAGIC Created in FctsProvCapExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
CurrRunCyc = get_widget_value("CurrRunCyc","100")

schema_IdsProvCapExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("PROV_CAP_SK", IntegerType(), nullable=False),
    StructField("PD_DT_SK", StringType(), nullable=False),
    StructField("CAP_PROV_ID", StringType(), nullable=False),
    StructField("PD_PROV_ID", StringType(), nullable=False),
    StructField("PROV_CAP_PAYMT_LOB_CD", StringType(), nullable=False),
    StructField("PROV_CAP_PAYMT_METH_CD", StringType(), nullable=False),
    StructField("PROV_CAP_PAYMT_CAP_TYP_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), nullable=False),
    StructField("PAYMT_SUM_CD", StringType(), nullable=False),
    StructField("AUTO_ADJ_AMT", DecimalType(38,10), nullable=False),
    StructField("CUR_CAP_AMT", DecimalType(38,10), nullable=False),
    StructField("MNL_ADJ_AMT", DecimalType(38,10), nullable=False),
    StructField("NET_AMT", DecimalType(38,10), nullable=False),
    StructField("AUTO_ADJ_MBR_MO_CT", DecimalType(38,10), nullable=False),
    StructField("CUR_MBR_MO_CT", DecimalType(38,10), nullable=False),
    StructField("MNL_ADJ_MBR_MO_CT", DecimalType(38,10), nullable=False)
])

df_IdsProvCapExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsProvCapExtr)
    .load(f"{adls_path}/key/IdsProvCapExtr.ProvCap.uniq")
)

df_IdsProvCapExtr = df_IdsProvCapExtr.withColumn("SrcSysCdSk", F.lit(SrcSysCdSk).cast(IntegerType()))

schema_WPAYMT_SUM = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("PAYMT_REF_ID", StringType(), nullable=False),
    StructField("PAYMT_SUM_LOB_CD", StringType(), nullable=False)
])

df_WPAYMT_SUM = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_WPAYMT_SUM)
    .load(f"{adls_path}/load/W_PAYMT_SUM.dat")
)

df_hf_pmt_sum_keys = dedup_sort(
    df_WPAYMT_SUM,
    ["SRC_SYS_CD_SK", "PAYMT_REF_ID", "PAYMT_SUM_LOB_CD"],
    [("SRC_SYS_CD_SK","A"), ("PAYMT_REF_ID","A"), ("PAYMT_SUM_LOB_CD","A")]
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_hf_pmt_sum_keys.createOrReplaceTempView("df_hf_pmt_sum_keys_tempview_for_stage_W_PAYMT_SUM_upsert")

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsProvCapFkey_W_PAYMT_SUM_temp", jdbc_url, jdbc_props)

(
    df_hf_pmt_sum_keys
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsProvCapFkey_W_PAYMT_SUM_temp")
    .mode("overwrite")
    .save()
)

merge_sql_W_PAYMT_SUM = f"""
MERGE INTO {IDSOwner}.W_PAYMT_SUM AS T
USING STAGING.IdsProvCapFkey_W_PAYMT_SUM_temp AS S
ON
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.PAYMT_REF_ID = S.PAYMT_REF_ID
    AND T.PAYMT_SUM_LOB_CD = S.PAYMT_SUM_LOB_CD
WHEN MATCHED THEN UPDATE SET
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
    T.PAYMT_REF_ID = S.PAYMT_REF_ID,
    T.PAYMT_SUM_LOB_CD = S.PAYMT_SUM_LOB_CD
WHEN NOT MATCHED THEN
    INSERT (
      SRC_SYS_CD_SK,
      PAYMT_REF_ID,
      PAYMT_SUM_LOB_CD
    )
    VALUES (
      S.SRC_SYS_CD_SK,
      S.PAYMT_REF_ID,
      S.PAYMT_SUM_LOB_CD
    );
"""

execute_dml(merge_sql_W_PAYMT_SUM, jdbc_url, jdbc_props)
execute_dml(f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.W_PAYMT_SUM on key columns with distribution on key columns and detailed indexes all allow write access')", jdbc_url, jdbc_props)

extract_query_keys = f"""
SELECT
  k.SRC_SYS_CD_SK,
  k.PAYMT_REF_ID,
  k.PAYMT_SUM_LOB_CD,
  k.PAYMT_SUM_SK
FROM {IDSOwner}.W_PAYMT_SUM w,
     {IDSOwner}.K_PAYMT_SUM k
WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
  AND w.PAYMT_REF_ID = k.PAYMT_REF_ID
  AND w.PAYMT_SUM_LOB_CD = k.PAYMT_SUM_LOB_CD
"""

df_W_PAYMT_SUM_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_keys)
    .load()
)

df_hf_provcap_paysum = dedup_sort(
    df_W_PAYMT_SUM_out,
    ["SRC_SYS_CD_SK", "PAYMT_REF_ID", "PAYMT_SUM_LOB_CD"],
    [("SRC_SYS_CD_SK","A"), ("PAYMT_REF_ID","A"), ("PAYMT_SUM_LOB_CD","A")]
)

df_purgeTrn_joined = df_IdsProvCapExtr.alias("ProvCapCrfIn").join(
    df_hf_provcap_paysum.alias("PaymtSumLkup"),
    (
        (F.col("ProvCapCrfIn.SrcSysCdSk") == F.col("PaymtSumLkup.SRC_SYS_CD_SK")) &
        (F.col("ProvCapCrfIn.PAYMT_SUM_CD") == F.col("PaymtSumLkup.PAYMT_REF_ID")) &
        (F.col("ProvCapCrfIn.PROV_CAP_PAYMT_LOB_CD") == F.col("PaymtSumLkup.PAYMT_SUM_LOB_CD"))
    ),
    "left"
)

df_purgeTrn_enriched = (
    df_purgeTrn_joined
    .withColumn("PassThru", F.col("ProvCapCrfIn.PASS_THRU_IN"))
    .withColumn("ProvCapPaymtLobSK", GetFkeyCodes(F.col("ProvCapCrfIn.SRC_SYS_CD"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.lit("CLAIM LINE LOB"), F.col("ProvCapCrfIn.PROV_CAP_PAYMT_LOB_CD"), Logging))
    .withColumn("ProvCapPaymtMethSK", GetFkeyCodes(F.col("ProvCapCrfIn.SRC_SYS_CD"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.lit("CAPITATION FUND PAYMENT METHOD"), F.col("ProvCapCrfIn.PROV_CAP_PAYMT_METH_CD"), Logging))
    .withColumn("ProvCapPaymtCapTypSK", GetFkeyCodes(F.col("ProvCapCrfIn.SRC_SYS_CD"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.lit("CAPITATION ADJUSTMENT TYPE"), F.col("ProvCapCrfIn.PROV_CAP_PAYMT_CAP_TYP_CD"), Logging))
    .withColumn("PdDtSK", GetFkeyDate(F.lit("IDS"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.col("ProvCapCrfIn.PD_DT_SK"), Logging))
    .withColumn("ProvSK", GetFkeyProv(F.col("ProvCapCrfIn.SRC_SYS_CD"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.col("ProvCapCrfIn.PD_PROV_ID"), Logging))
    .withColumn("CapProvSK", GetFkeyProv(F.col("ProvCapCrfIn.SRC_SYS_CD"), F.col("ProvCapCrfIn.PROV_CAP_SK"), F.col("ProvCapCrfIn.CAP_PROV_ID"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("ProvCapCrfIn.PROV_CAP_SK")))
)

df_ProvCapFkeyOut = df_purgeTrn_enriched.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_lnkRecycle = df_purgeTrn_enriched.filter(F.col("ErrCount") > 0)

df_ProvCapFkeyOut_sel = df_ProvCapFkeyOut.select(
    F.col("ProvCapCrfIn.PROV_CAP_SK").alias("PROV_CAP_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("PdDtSK").cast(StringType()).alias("PD_DT_SK"),
    F.col("ProvCapCrfIn.CAP_PROV_ID").alias("CAP_PROV_ID"),
    F.col("ProvCapCrfIn.PD_PROV_ID").alias("PD_PROV_ID"),
    F.col("ProvCapPaymtLobSK").alias("PROV_CAP_LOB_CD_SK"),
    F.col("ProvCapPaymtMethSK").alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("ProvCapPaymtCapTypSK").alias("PROV_CAP_TYP_CD_SK"),
    F.col("ProvCapCrfIn.CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ProvCapCrfIn.LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CapProvSK").alias("CAP_PROV_SK"),
    F.col("ProvSK").alias("PD_PROV_SK"),
    F.expr("""
    CASE
      WHEN trim(ProvCapCrfIn.PAYMT_SUM_CD) = 'NA' THEN 1
      WHEN length(trim(ProvCapCrfIn.PAYMT_SUM_CD)) = 0 THEN 1
      WHEN PaymtSumLkup.PAYMT_REF_ID IS NULL THEN 0
      ELSE PaymtSumLkup.PAYMT_SUM_SK
    END
    """).alias("PAYMT_SUM_SK"),
    F.col("ProvCapCrfIn.AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
    F.col("ProvCapCrfIn.CUR_CAP_AMT").alias("CUR_CAP_AMT"),
    F.col("ProvCapCrfIn.MNL_ADJ_AMT").alias("MNL_ADJ_AMT"),
    F.col("ProvCapCrfIn.NET_AMT").alias("NET_AMT"),
    F.col("ProvCapCrfIn.AUTO_ADJ_MBR_MO_CT").alias("AUTO_ADJ_MBR_MO_CT"),
    F.col("ProvCapCrfIn.CUR_MBR_MO_CT").alias("CUR_MBR_MO_CT"),
    F.col("ProvCapCrfIn.MNL_ADJ_MBR_MO_CT").alias("MNL_ADJ_MBR_MO_CT")
)

df_lnkRecycle_sel = df_lnkRecycle.select(
    GetRecycleKey(F.col("ProvCapCrfIn.PROV_CAP_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ProvCapCrfIn.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ProvCapCrfIn.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ProvCapCrfIn.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ProvCapCrfIn.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("ProvCapCrfIn.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("ProvCapCrfIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ProvCapCrfIn.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ProvCapCrfIn.PROV_CAP_SK").alias("PROV_CAP_SK"),
    F.col("ProvCapCrfIn.PD_DT_SK").alias("PD_DT_SK"),
    F.col("ProvCapCrfIn.CAP_PROV_ID").alias("CAP_PROV_ID"),
    F.col("ProvCapCrfIn.PD_PROV_ID").alias("PD_PROV_ID"),
    F.col("ProvCapCrfIn.PROV_CAP_PAYMT_LOB_CD").alias("PROV_CAP_PAYMT_LOB_CD"),
    F.col("ProvCapCrfIn.PROV_CAP_PAYMT_METH_CD").alias("PROV_CAP_PAYMT_METH_CD"),
    F.col("ProvCapCrfIn.PROV_CAP_PAYMT_CAP_TYP_CD").alias("PROV_CAP_PAYMT_CAP_TYP_CD"),
    F.col("ProvCapCrfIn.CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("ProvCapCrfIn.LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("ProvCapCrfIn.PAYMT_SUM_CD").alias("PAYMT_SUM_CD"),
    F.col("ProvCapCrfIn.AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
    F.col("ProvCapCrfIn.CUR_CAP_AMT").alias("CUR_CAP_AMT"),
    F.col("ProvCapCrfIn.MNL_ADJ_AMT").alias("MNL_ADJ_AMT"),
    F.col("ProvCapCrfIn.NET_AMT").alias("NET_AMT"),
    F.col("ProvCapCrfIn.AUTO_ADJ_MBR_MO_CT").alias("AUTO_ADJ_MBR_MO_CT"),
    F.col("ProvCapCrfIn.CUR_MBR_MO_CT").alias("CUR_MBR_MO_CT"),
    F.col("ProvCapCrfIn.MNL_ADJ_MBR_MO_CT").alias("MNL_ADJ_MBR_MO_CT")
)

write_files(
    df_lnkRecycle_sel,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_purgeTrn_limit = df_purgeTrn_enriched.limit(1)

df_DefaultUNK = df_purgeTrn_limit.select(
    F.lit(0).alias("PROV_CAP_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("PD_DT_SK"),
    F.lit("UNK").alias("CAP_PROV_ID"),
    F.lit("UNK").alias("PD_PROV_ID"),
    F.lit(0).alias("PROV_CAP_LOB_CD_SK"),
    F.lit(0).alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.lit(0).alias("PROV_CAP_TYP_CD_SK"),
    F.lit(CurrRunCyc).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CAP_PROV_SK"),
    F.lit(0).alias("PD_PROV_SK"),
    F.lit(0).alias("PAYMT_SUM_SK"),
    F.lit(0.00).alias("AUTO_ADJ_AMT"),
    F.lit(0.00).alias("CUR_CAP_AMT"),
    F.lit(0.00).alias("MNL_ADJ_AMT"),
    F.lit(0.00).alias("NET_AMT"),
    F.lit(0).alias("AUTO_ADJ_MBR_MO_CT"),
    F.lit(0).alias("CUR_MBR_MO_CT"),
    F.lit(0).alias("MNL_ADJ_MBR_MO_CT")
)

df_DefaultNA = df_purgeTrn_limit.select(
    F.lit(1).alias("PROV_CAP_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("PD_DT_SK"),
    F.lit("NA").alias("CAP_PROV_ID"),
    F.lit("NA").alias("PD_PROV_ID"),
    F.lit(1).alias("PROV_CAP_LOB_CD_SK"),
    F.lit(1).alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.lit(1).alias("PROV_CAP_TYP_CD_SK"),
    F.lit(CurrRunCyc).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCyc).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CAP_PROV_SK"),
    F.lit(1).alias("PD_PROV_SK"),
    F.lit(1).alias("PAYMT_SUM_SK"),
    F.lit(0).alias("AUTO_ADJ_AMT"),
    F.lit(0).alias("CUR_CAP_AMT"),
    F.lit(0).alias("MNL_ADJ_AMT"),
    F.lit(0).alias("NET_AMT"),
    F.lit(0).alias("AUTO_ADJ_MBR_MO_CT"),
    F.lit(0).alias("CUR_MBR_MO_CT"),
    F.lit(0).alias("MNL_ADJ_MBR_MO_CT")
)

df_collector = df_ProvCapFkeyOut_sel.union(df_DefaultUNK).union(df_DefaultNA)

df_collector_ordered = df_collector.select(
    "PROV_CAP_SK",
    "SRC_SYS_CD_SK",
    "PD_DT_SK",
    "CAP_PROV_ID",
    "PD_PROV_ID",
    "PROV_CAP_LOB_CD_SK",
    "PROV_CAP_PAYMT_METH_CD_SK",
    "PROV_CAP_TYP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CAP_PROV_SK",
    "PD_PROV_SK",
    "PAYMT_SUM_SK",
    "AUTO_ADJ_AMT",
    "CUR_CAP_AMT",
    "MNL_ADJ_AMT",
    "NET_AMT",
    "AUTO_ADJ_MBR_MO_CT",
    "CUR_MBR_MO_CT",
    "MNL_ADJ_MBR_MO_CT"
).withColumn(
    "PD_DT_SK",
    F.rpad(F.col("PD_DT_SK"), 10, " ")
)

write_files(
    df_collector_ordered,
    f"{adls_path}/load/PROV_CAP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)