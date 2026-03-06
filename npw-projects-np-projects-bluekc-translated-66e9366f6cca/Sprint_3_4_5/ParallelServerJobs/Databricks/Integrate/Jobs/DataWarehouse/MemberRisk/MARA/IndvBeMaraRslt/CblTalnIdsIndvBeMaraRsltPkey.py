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
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                                                                         Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------                                                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  03/04/2015      5460                                Originally Programmed                                                                      IntegrateNewDevl        Kalyan Neelam             2015-03-06
# MAGIC 
# MAGIC Raja Gummadi                  03/24/2015      5460                                Changed Primary Key generate process                                           IntegrateNewDevl        Kalyan Neelam             2015-03-25
# MAGIC 
# MAGIC Krishnakanth                    03/16/2018      60037                              Added the filter condition                                                                  IntegrateDev2              Kalyan Neelam             2018-03-16     
# MAGIC    Manivannan                                                                                   where INDV_BE_MARA_RSLT_SK < 0
# MAGIC                                                                                                          in the stage K_INDV_BE_MARA_RSLT_SK_In
# MAGIC 
# MAGIC Venkatesh Babu             2020-11-01                                              Added VRSN_ID                                                                                 IntegrateDev2              Jeyaprasanna               2020-11-12
# MAGIC Munnangi

# MAGIC 11-06-2020  : Added VRSN_ID from Source to Target
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table K_INDV_BE_MARA_RSLT.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
YearMo = get_widget_value('YearMo','')

# Read from PxDataSet: ds_INDV_BE_MARA_RSLT_Xfrm (.ds -> parquet)
df_ds_INDV_BE_MARA_RSLT_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/INDV_BE_MARA_RSLT.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# Split into two outputs for cpy_MultiStreams
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_INDV_BE_MARA_RSLT_Xfrm.select(
    F.col("SRC_SYS_CD"),
    F.col("INDV_BE_KEY"),
    F.col("MDL_ID"),
    F.col("PRCS_YR_MO_SK")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_INDV_BE_MARA_RSLT_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("INDV_BE_KEY"),
    F.col("MDL_ID"),
    F.col("SRC_SYS_CD"),
    F.col("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("GNDR"),
    F.col("PRIOR_CST_MED"),
    F.col("PRIOR_CST_RX"),
    F.col("PRIOR_CST_TOT"),
    F.col("CATLCD"),
    F.col("UNCATLCD"),
    F.col("EXPSR_MO"),
    F.col("CATNDC"),
    F.col("UNCATNDC"),
    F.col("AGE"),
    F.col("ER_SCORE"),
    F.col("IP_SCORE"),
    F.col("MED_SCORE"),
    F.col("OTHR"),
    F.col("OP_SCORE"),
    F.col("RX_SCORE"),
    F.col("PHYS_SCORE"),
    F.col("TOT_SCORE"),
    F.col("VRSN_ID")
)

# Read from DB: K_INDV_BE_MARA_RSLT_SK_In
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_k_sk = f"""
SELECT 
'{YearMo}' AS PRCS_YR_MO_SK,
COALESCE(MAX(INDV_BE_MARA_RSLT_SK),0) AS MAX_INDV_BE_MARA_RSLT_SK
FROM {IDSOwner}.K_INDV_BE_MARA_RSLT
WHERE INDV_BE_MARA_RSLT_SK < 0
"""
df_K_INDV_BE_MARA_RSLT_SK_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_k_sk)
    .load()
)

# rdp_NaturalKeys (dedup)
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["SRC_SYS_CD","INDV_BE_KEY","MDL_ID","PRCS_YR_MO_SK"],
    []
)
df_rdp_NaturalKeys_lnkRemDupDataOut = df_rdp_NaturalKeys.select(
    F.col("SRC_SYS_CD"),
    F.col("INDV_BE_KEY"),
    F.col("MDL_ID"),
    F.col("PRCS_YR_MO_SK")
)

# Read from DB: db2_K_INDV_BE_MARA_RSLT_In
extract_query_db2_in = f"""
SELECT 
SRC_SYS_CD,
INDV_BE_KEY,
MDL_ID,
PRCS_YR_MO_SK,
CRT_RUN_CYC_EXCTN_SK,
INDV_BE_MARA_RSLT_SK
FROM {IDSOwner}.K_INDV_BE_MARA_RSLT 
WHERE PRCS_YR_MO_SK = '{YearMo}'
"""
df_db2_K_INDV_BE_MARA_RSLT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_in)
    .load()
)

# jn_IndvBeMaraRslt (left outer join)
df_jn_IndvBeMaraRslt = (
    df_rdp_NaturalKeys_lnkRemDupDataOut.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_INDV_BE_MARA_RSLT_In.alias("Extr"),
        on=[
            "SRC_SYS_CD",
            "INDV_BE_KEY",
            "MDL_ID",
            "PRCS_YR_MO_SK"
        ],
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("lnkRemDupDataOut.MDL_ID").alias("MDL_ID"),
        F.col("lnkRemDupDataOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Extr.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK")
    )
)

# Lookup_15 (left join)
df_Lookup_15 = (
    df_jn_IndvBeMaraRslt.alias("Joinin")
    .join(
        df_K_INDV_BE_MARA_RSLT_SK_In.alias("MaxSK"),
        on=[
            df_jn_IndvBeMaraRslt.PRCS_YR_MO_SK == df_K_INDV_BE_MARA_RSLT_SK_In.PRCS_YR_MO_SK
        ],
        how="left"
    )
    .select(
        F.col("Joinin.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Joinin.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("Joinin.MDL_ID").alias("MDL_ID"),
        F.col("Joinin.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("Joinin.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Joinin.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        F.col("MaxSK.MAX_INDV_BE_MARA_RSLT_SK").alias("MAX_INDV_BE_MARA_RSLT_SK")
    )
)

# xfm_PKEYgen: transformer logic
windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_xfm_PKEYgen_temp = (
    df_Lookup_15
    .withColumn("rownum", F.row_number().over(windowSpec))
    .withColumn(
        "svIndvBeMaraRsltSK",
        F.when(
            F.col("INDV_BE_MARA_RSLT_SK").isNull(),
            (F.col("rownum") - F.lit(1)) + F.col("MAX_INDV_BE_MARA_RSLT_SK") + F.lit(1)
        ).otherwise(F.col("INDV_BE_MARA_RSLT_SK"))
    )
    .withColumn(
        "svRunCycle",
        F.when(
            F.col("INDV_BE_MARA_RSLT_SK").isNull(),
            F.lit(IDSRunCycle)
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
)

# Output link "New" (IsNull(JoinOut.INDV_BE_MARA_RSLT_SK) = @TRUE)
df_xfm_PKEYgen_New = (
    df_xfm_PKEYgen_temp
    .filter(F.col("INDV_BE_MARA_RSLT_SK").isNull())
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("MDL_ID").alias("MDL_ID"),
        F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("svRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svIndvBeMaraRsltSK").alias("INDV_BE_MARA_RSLT_SK")
    )
)

# Output link "lnkPKEYxfmOut"
df_xfm_PKEYgen_lnkPKEYxfmOut = (
    df_xfm_PKEYgen_temp
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("MDL_ID").alias("MDL_ID"),
        F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("svRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svIndvBeMaraRsltSK").alias("INDV_BE_MARA_RSLT_SK")
    )
)

# db2_K_INDV_BE_MARA_RSLT_Load => merge into {IDSOwner}.K_INDV_BE_MARA_RSLT
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
temp_table_name_load = "STAGING.CblTalnIdsIndvBeMaraRsltPkey_db2_K_INDV_BE_MARA_RSLT_Load_temp"

# 1) Drop temp table if exists
drop_sql_load = f"DROP TABLE IF EXISTS {temp_table_name_load}"
execute_dml(drop_sql_load, jdbc_url_ids, jdbc_props_ids)

# 2) Write df_xfm_PKEYgen_New into the temp table
(
    df_xfm_PKEYgen_New.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_name_load)
    .mode("append")
    .save()
)

# 3) Build merge statement
merge_sql_load = f"""
MERGE INTO {IDSOwner}.K_INDV_BE_MARA_RSLT AS T
USING {temp_table_name_load} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.INDV_BE_KEY = S.INDV_BE_KEY AND
    T.MDL_ID = S.MDL_ID AND
    T.PRCS_YR_MO_SK = S.PRCS_YR_MO_SK
WHEN MATCHED THEN 
  UPDATE SET 
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.INDV_BE_MARA_RSLT_SK = S.INDV_BE_MARA_RSLT_SK
WHEN NOT MATCHED THEN 
  INSERT (
    SRC_SYS_CD,
    INDV_BE_KEY,
    MDL_ID,
    PRCS_YR_MO_SK,
    CRT_RUN_CYC_EXCTN_SK,
    INDV_BE_MARA_RSLT_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.INDV_BE_KEY,
    S.MDL_ID,
    S.PRCS_YR_MO_SK,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.INDV_BE_MARA_RSLT_SK
  )
;
"""
execute_dml(merge_sql_load, jdbc_url_ids, jdbc_props_ids)

# jn_PKEYs (inner join)
df_jn_PKEYs = (
    df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        on=[
            "SRC_SYS_CD",
            "INDV_BE_KEY",
            "MDL_ID",
            "PRCS_YR_MO_SK"
        ],
        how="inner"
    )
    .select(
        F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnkPKEYxfmOut.INDV_BE_MARA_RSLT_SK").alias("INDV_BE_MARA_RSLT_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("lnkFullDataJnIn.MDL_ID").alias("MDL_ID"),
        F.col("lnkFullDataJnIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnkFullDataJnIn.GNDR").alias("GNDR"),
        F.col("lnkFullDataJnIn.PRIOR_CST_MED").alias("PRIOR_CST_MED"),
        F.col("lnkFullDataJnIn.PRIOR_CST_RX").alias("PRIOR_CST_RX"),
        F.col("lnkFullDataJnIn.PRIOR_CST_TOT").alias("PRIOR_CST_TOT"),
        F.col("lnkFullDataJnIn.CATLCD").alias("CATLCD"),
        F.col("lnkFullDataJnIn.UNCATLCD").alias("UNCATLCD"),
        F.col("lnkFullDataJnIn.EXPSR_MO").alias("EXPSR_MO"),
        F.col("lnkFullDataJnIn.CATNDC").alias("CATNDC"),
        F.col("lnkFullDataJnIn.UNCATNDC").alias("UNCATNDC"),
        F.col("lnkFullDataJnIn.AGE").alias("AGE"),
        F.col("lnkFullDataJnIn.ER_SCORE").alias("ER_SCORE"),
        F.col("lnkFullDataJnIn.IP_SCORE").alias("IP_SCORE"),
        F.col("lnkFullDataJnIn.MED_SCORE").alias("MED_SCORE"),
        F.col("lnkFullDataJnIn.OTHR").alias("OTHR"),
        F.col("lnkFullDataJnIn.OP_SCORE").alias("OP_SCORE"),
        F.col("lnkFullDataJnIn.RX_SCORE").alias("RX_SCORE"),
        F.col("lnkFullDataJnIn.PHYS_SCORE").alias("PHYS_SCORE"),
        F.col("lnkFullDataJnIn.TOT_SCORE").alias("TOT_SCORE"),
        F.col("lnkFullDataJnIn.VRSN_ID").alias("VRSN_ID")
    )
)

# seq_INDV_BE_MARA_RSLT_Pkey (write to .dat with the specified settings)
# The only known char column is PRCS_YR_MO_SK (length=6). Apply rpad prior to writing.
df_final = df_jn_PKEYs.withColumn(
    "PRCS_YR_MO_SK",
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INDV_BE_MARA_RSLT_SK",
    "SRC_SYS_CD",
    "INDV_BE_KEY",
    "MDL_ID",
    "PRCS_YR_MO_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "GNDR",
    "PRIOR_CST_MED",
    "PRIOR_CST_RX",
    "PRIOR_CST_TOT",
    "CATLCD",
    "UNCATLCD",
    "EXPSR_MO",
    "CATNDC",
    "UNCATNDC",
    "AGE",
    "ER_SCORE",
    "IP_SCORE",
    "MED_SCORE",
    "OTHR",
    "OP_SCORE",
    "RX_SCORE",
    "PHYS_SCORE",
    "TOT_SCORE",
    "VRSN_ID"
)

write_files(
    df_final,
    f"{adls_path}/key/INDV_BE_MARA_RSLT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)