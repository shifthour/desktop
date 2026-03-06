# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsGrpAhyPgmRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2012-11-30           4830                              Initial Programming                                            IntegrateNewDevl       Bhoomi Dasari              12/3/2012

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT
GRP_AHY_PGM.GRP_ID,
GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK,
GRP_AHY_PGM.SRC_SYS_CD_SK,
B_GRP_AHY_PGM.GRP_ID AS GRP_ID_1,
B_GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK AS GRP_AHY_PGM_STRT_DT_SK_1,
B_GRP_AHY_PGM.SRC_SYS_CD_SK AS SRC_SYS_CD_SK_1
FROM {IDSOwner}.GRP_AHY_PGM GRP_AHY_PGM
FULL OUTER JOIN {IDSOwner}.B_GRP_AHY_PGM B_GRP_AHY_PGM
ON GRP_AHY_PGM.GRP_ID = B_GRP_AHY_PGM.GRP_ID
AND GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK = B_GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK
AND GRP_AHY_PGM.SRC_SYS_CD_SK = B_GRP_AHY_PGM.SRC_SYS_CD_SK
WHERE GRP_AHY_PGM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""

df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_SrcTrgtCompRenamed = df_SrcTrgtComp.select(
    F.col("GRP_ID").alias("SRC_GRP_ID"),
    F.col("GRP_AHY_PGM_STRT_DT_SK").alias("SRC_GRP_AHY_PGM_STRT_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("GRP_ID_1").alias("TRGT_GRP_ID"),
    F.col("GRP_AHY_PGM_STRT_DT_SK_1").alias("TRGT_GRP_AHY_PGM_STRT_DT_SK"),
    F.col("SRC_SYS_CD_SK_1").alias("TRGT_SRC_SYS_CD_SK")
)

df_research = df_SrcTrgtCompRenamed.filter(
    F.col("SRC_GRP_AHY_PGM_STRT_DT_SK").isNull()
    | F.col("SRC_GRP_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_GRP_AHY_PGM_STRT_DT_SK").isNull()
    | F.col("TRGT_GRP_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)

w = Window.orderBy(F.lit("1"))
df_notify_temp = df_SrcTrgtCompRenamed.withColumn("row_number", F.row_number().over(w))
df_notify_filtered = df_notify_temp.filter(
    (F.col("row_number") == 1) & (F.lit(ToleranceCd) == "OUT")
)

df_notify_final = df_notify_filtered.select(
    F.rpad(
        F.concat(
            F.lit("ROW COUNT BALANCING "),
            F.lit(SrcSysCd),
            F.lit(" - IDS GRP_AHY_PGM OUT OF TOLERANCE")
        ),
        70,
        " "
    ).alias("NOTIFICATION")
)

df_research_final = df_research.select(
    F.col("TRGT_GRP_ID").alias("TRGT_GRP_ID"),
    F.rpad(F.col("TRGT_GRP_AHY_PGM_STRT_DT_SK"), 10, " ").alias("TRGT_GRP_AHY_PGM_STRT_DT_SK"),
    F.col("TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("SRC_GRP_ID").alias("SRC_GRP_ID"),
    F.rpad(F.col("SRC_GRP_AHY_PGM_STRT_DT_SK"), 10, " ").alias("SRC_GRP_AHY_PGM_STRT_DT_SK"),
    F.col("SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK")
)

write_files(
    df_research_final,
    f"{adls_path}/balancing/research/{SrcSysCd}IdsGrpAhyPgmResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_notify_final,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)