# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                            IntegrateNewDevl        Kalyan Neelam            2015-03-06
# MAGIC Raja Gummadi                  03/24/2015      5460                                Changed Primary Key generate process IntegrateNewDevl        Kalyan Neelam            2015-03-25
# MAGIC Venkatesh Munnangi       11-05-2020                                                Added VRSN_ID variable                      IntegrateDev2              Jeyaprasanna              2020-11-12

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC 11-05-2020: Added VRSN_ID from Source to Target
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table K_INDV_BE_MARA_CLNCL_CLS.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
YearMo = get_widget_value('YearMo','')

schema_ds_INDV_BE_MARA_CLNCL_CLS_Xfrm = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", StringType(), True),
    StructField("INDV_BE_MARA_CLNCL_CLS_SK", StringType(), True),
    StructField("INDV_BE_KEY", StringType(), True),
    StructField("CLNCL_CLS_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("CT_UNIQ_INSTS", StringType(), True),
    StructField("LAST_MO_OBSRV", StringType(), True),
    StructField("DX_ADJOR_CONC_PCT", StringType(), True),
    StructField("DX_ADJOR_PROSP_LAG_0_PCT", StringType(), True),
    StructField("DX_ADJOR_PROSP_LAG_3_PCT", StringType(), True),
    StructField("DX_ADJOR_PROSP_LAG_6_PCT", StringType(), True),
    StructField("CX_ADJOR_CONC_PCT", StringType(), True),
    StructField("CX_ADJOR_PROSP_LAG_0_PCT", StringType(), True),
    StructField("CX_ADJOR_PROSP_LAG_3_PCT", StringType(), True),
    StructField("CX_ADJOR_PROSP_LAG_6_PCT", StringType(), True),
    StructField("VRSN_ID", StringType(), True)
])

df_ds_INDV_BE_MARA_CLNCL_CLS_Xfrm = (
    spark.read
    .schema(schema_ds_INDV_BE_MARA_CLNCL_CLS_Xfrm)
    .parquet(f"{adls_path}/ds/INDV_BE_MARA_CLNCL_CLS.{SrcSysCd}.xfrm.{RunID}.parquet")
)

df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_INDV_BE_MARA_CLNCL_CLS_Xfrm.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_INDV_BE_MARA_CLNCL_CLS_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CT_UNIQ_INSTS").alias("CT_UNIQ_INSTS"),
    F.col("LAST_MO_OBSRV").alias("LAST_MO_OBSRV"),
    F.col("DX_ADJOR_CONC_PCT").alias("DX_ADJOR_CONC_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_0_PCT").alias("DX_ADJOR_PROSP_LAG_0_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_3_PCT").alias("DX_ADJOR_PROSP_LAG_3_PCT"),
    F.col("DX_ADJOR_PROSP_LAG_6_PCT").alias("DX_ADJOR_PROSP_LAG_6_PCT"),
    F.col("CX_ADJOR_CONC_PCT").alias("CX_ADJOR_CONC_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_0_PCT").alias("CX_ADJOR_PROSP_LAG_0_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_3_PCT").alias("CX_ADJOR_PROSP_LAG_3_PCT"),
    F.col("CX_ADJOR_PROSP_LAG_6_PCT").alias("CX_ADJOR_PROSP_LAG_6_PCT"),
    F.col("VRSN_ID").alias("VRSN_ID")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_K_INDV_BE_MARA_CLNCL_CLS_SK_In = (
    f"SELECT '{YearMo}' AS PRCS_YR_MO_SK, "
    f"COALESCE(MAX(INDV_BE_MARA_CLNCL_CLS_SK),0) AS MAX_INDV_BE_MARA_CLNCL_CLS_SK "
    f"FROM {IDSOwner}.K_INDV_BE_MARA_CLNCL_CLS"
)
df_K_INDV_BE_MARA_CLNCL_CLS_SK_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_K_INDV_BE_MARA_CLNCL_CLS_SK_In)
    .load()
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    partition_cols=["SRC_SYS_CD", "INDV_BE_KEY", "CLNCL_CLS_ID", "PRCS_YR_MO_SK"],
    sort_cols=[]
)

extract_query_db2_K_INDV_BE_MARA_CLNCL_CLS_In = (
    f"SELECT SRC_SYS_CD, INDV_BE_KEY, CLNCL_CLS_ID, PRCS_YR_MO_SK, "
    f"CRT_RUN_CYC_EXCTN_SK, INDV_BE_MARA_CLNCL_CLS_SK "
    f"FROM {IDSOwner}.K_INDV_BE_MARA_CLNCL_CLS WHERE PRCS_YR_MO_SK = '{YearMo}'"
)
df_db2_K_INDV_BE_MARA_CLNCL_CLS_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_INDV_BE_MARA_CLNCL_CLS_In)
    .load()
)

df_jn_IndvBeClnclCls = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_INDV_BE_MARA_CLNCL_CLS_In.alias("Extr"),
    on=[
        F.col("lnkRemDupDataOut.INDV_BE_KEY") == F.col("Extr.INDV_BE_KEY"),
        F.col("lnkRemDupDataOut.CLNCL_CLS_ID") == F.col("Extr.CLNCL_CLS_ID"),
        F.col("lnkRemDupDataOut.PRCS_YR_MO_SK") == F.col("Extr.PRCS_YR_MO_SK"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD")
    ],
    how="left"
).select(
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnkRemDupDataOut.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("lnkRemDupDataOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extr.INDV_BE_MARA_CLNCL_CLS_SK").alias("INDV_BE_MARA_CLNCL_CLS_SK")
)

df_Lookup_15 = df_jn_IndvBeClnclCls.alias("JoinIn").join(
    df_K_INDV_BE_MARA_CLNCL_CLS_SK_In.alias("MaxSK"),
    on=[F.col("JoinIn.PRCS_YR_MO_SK") == F.col("MaxSK.PRCS_YR_MO_SK")],
    how="left"
).select(
    F.col("JoinIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("JoinIn.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("JoinIn.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("JoinIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("JoinIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("JoinIn.INDV_BE_MARA_CLNCL_CLS_SK").alias("INDV_BE_MARA_CLNCL_CLS_SK"),
    F.col("MaxSK.MAX_INDV_BE_MARA_CLNCL_CLS_SK").alias("MAX_INDV_BE_MARA_CLNCL_CLS_SK")
)

windowSpec_xfm_PKEYgen = Window.orderBy(F.monotonically_increasing_id())
df_xfm_PKEYgen_temp = df_Lookup_15.withColumn(
    "rownum",
    F.row_number().over(windowSpec_xfm_PKEYgen)
)
df_xfm_PKEYgen_temp = df_xfm_PKEYgen_temp.withColumn(
    "svIndvBeMaraClnclCLsSK",
    F.when(
        F.col("INDV_BE_MARA_CLNCL_CLS_SK").isNull(),
        (F.col("rownum") + F.col("MAX_INDV_BE_MARA_CLNCL_CLS_SK"))
    ).otherwise(F.col("INDV_BE_MARA_CLNCL_CLS_SK"))
)
df_xfm_PKEYgen_temp = df_xfm_PKEYgen_temp.withColumn(
    "svRunCyle",
    F.when(F.col("INDV_BE_MARA_CLNCL_CLS_SK").isNull(), F.lit(IDSRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_xfm_PKEYgen_New = df_xfm_PKEYgen_temp.filter(
    F.col("INDV_BE_MARA_CLNCL_CLS_SK").isNull()
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svIndvBeMaraClnclCLsSK").alias("INDV_BE_MARA_CLNCL_CLS_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_xfm_PKEYgen_temp.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svIndvBeMaraClnclCLsSK").alias("INDV_BE_MARA_CLNCL_CLS_SK")
)

jdbc_url, connection_properties = get_db_config(ids_secret_name)
temp_table_name = "STAGING.CblTalnIdsIndvBeMaraClnclClsPkey_db2_K_INDV_BE_MARA_CLNCL_CLS_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, connection_properties)
(
    df_xfm_PKEYgen_New.write
    .jdbc(
        url=jdbc_url,
        table=temp_table_name,
        mode="overwrite",
        properties=connection_properties
    )
)

merge_sql = (
    f"MERGE INTO {IDSOwner}.K_INDV_BE_MARA_CLNCL_CLS AS T "
    f"USING {temp_table_name} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.INDV_BE_KEY = S.INDV_BE_KEY "
    f"AND T.CLNCL_CLS_ID = S.CLNCL_CLS_ID "
    f"AND T.PRCS_YR_MO_SK = S.PRCS_YR_MO_SK "
    f"WHEN MATCHED THEN "
    f"  UPDATE SET "
    f"    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK, "
    f"    T.INDV_BE_MARA_CLNCL_CLS_SK = S.INDV_BE_MARA_CLNCL_CLS_SK "
    f"WHEN NOT MATCHED THEN "
    f"  INSERT (SRC_SYS_CD, INDV_BE_KEY, CLNCL_CLS_ID, PRCS_YR_MO_SK, CRT_RUN_CYC_EXCTN_SK, INDV_BE_MARA_CLNCL_CLS_SK) "
    f"  VALUES (S.SRC_SYS_CD, S.INDV_BE_KEY, S.CLNCL_CLS_ID, S.PRCS_YR_MO_SK, S.CRT_RUN_CYC_EXCTN_SK, S.INDV_BE_MARA_CLNCL_CLS_SK);"
)
execute_dml(merge_sql, jdbc_url, connection_properties)

df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.INDV_BE_KEY") == F.col("lnkPKEYxfmOut.INDV_BE_KEY"),
        F.col("lnkFullDataJnIn.CLNCL_CLS_ID") == F.col("lnkPKEYxfmOut.CLNCL_CLS_ID"),
        F.col("lnkFullDataJnIn.PRCS_YR_MO_SK") == F.col("lnkPKEYxfmOut.PRCS_YR_MO_SK")
    ],
    how="inner"
).select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.INDV_BE_MARA_CLNCL_CLS_SK").alias("INDV_BE_MARA_CLNCL_CLS_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnkFullDataJnIn.CLNCL_CLS_ID").alias("CLNCL_CLS_ID"),
    F.col("lnkFullDataJnIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkFullDataJnIn.CT_UNIQ_INSTS").alias("CT_UNIQ_INSTS"),
    F.col("lnkFullDataJnIn.LAST_MO_OBSRV").alias("LAST_MO_OBSRV"),
    F.col("lnkFullDataJnIn.DX_ADJOR_CONC_PCT").alias("DX_ADJOR_CONC_PCT"),
    F.col("lnkFullDataJnIn.DX_ADJOR_PROSP_LAG_0_PCT").alias("DX_ADJOR_PROSP_LAG_0_PCT"),
    F.col("lnkFullDataJnIn.DX_ADJOR_PROSP_LAG_3_PCT").alias("DX_ADJOR_PROSP_LAG_3_PCT"),
    F.col("lnkFullDataJnIn.DX_ADJOR_PROSP_LAG_6_PCT").alias("DX_ADJOR_PROSP_LAG_6_PCT"),
    F.col("lnkFullDataJnIn.CX_ADJOR_CONC_PCT").alias("CX_ADJOR_CONC_PCT"),
    F.col("lnkFullDataJnIn.CX_ADJOR_PROSP_LAG_0_PCT").alias("CX_ADJOR_PROSP_LAG_0_PCT"),
    F.col("lnkFullDataJnIn.CX_ADJOR_PROSP_LAG_3_PCT").alias("CX_ADJOR_PROSP_LAG_3_PCT"),
    F.col("lnkFullDataJnIn.CX_ADJOR_PROSP_LAG_6_PCT").alias("CX_ADJOR_PROSP_LAG_6_PCT"),
    F.col("lnkFullDataJnIn.VRSN_ID").alias("VRSN_ID")
)

df_seq_INDV_BE_MARA_CLNCL_CLS_Pkey = df_jn_PKEYs.withColumn(
    "PRCS_YR_MO_SK",
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INDV_BE_MARA_CLNCL_CLS_SK",
    "SRC_SYS_CD",
    "INDV_BE_KEY",
    "CLNCL_CLS_ID",
    "PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "CT_UNIQ_INSTS",
    "LAST_MO_OBSRV",
    "DX_ADJOR_CONC_PCT",
    "DX_ADJOR_PROSP_LAG_0_PCT",
    "DX_ADJOR_PROSP_LAG_3_PCT",
    "DX_ADJOR_PROSP_LAG_6_PCT",
    "CX_ADJOR_CONC_PCT",
    "CX_ADJOR_PROSP_LAG_0_PCT",
    "CX_ADJOR_PROSP_LAG_3_PCT",
    "CX_ADJOR_PROSP_LAG_6_PCT",
    "VRSN_ID"
)

write_files(
    df_seq_INDV_BE_MARA_CLNCL_CLS_Pkey,
    f"{adls_path}/key/INDV_BE_MARA_CLNCL_CLS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)