# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   IdsGrpAhyBnfRowCntBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                2014-09-15          TFS 9558                         Initial Programming                                            IntegrateNewDevl        Bhoomi Dasari              12/09/2014

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
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
GRP_AHY_BNF.SRC_SYS_CD_SK AS SRC_SRC_SYS_CD_SK,
GRP_AHY_BNF.GRP_ID AS SRC_GRP_ID,
GRP_AHY_BNF.EFF_DT_SK AS SRC_EFF_DT_SK,
B_GRP_AHY_BNF.SRC_SYS_CD_SK AS TRGT_SRC_SYS_CD_SK,
B_GRP_AHY_BNF.GRP_ID AS TRGT_GRP_ID,
B_GRP_AHY_BNF.EFF_DT_SK AS TRGT_EFF_DT_SK
FROM {IDSOwner}.GRP_AHY_BNF GRP_AHY_BNF
FULL OUTER JOIN {IDSOwner}.B_GRP_AHY_BNF B_GRP_AHY_BNF
ON GRP_AHY_BNF.SRC_SYS_CD_SK = B_GRP_AHY_BNF.SRC_SYS_CD_SK
AND GRP_AHY_BNF.GRP_ID = B_GRP_AHY_BNF.GRP_ID
AND GRP_AHY_BNF.EFF_DT_SK = B_GRP_AHY_BNF.EFF_DT_SK
WHERE
GRP_AHY_BNF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
"""
df_SrcTrgtComp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

w = Window.orderBy(F.lit(1))
df_SrcTrgtComp = df_SrcTrgtComp.withColumn("rownum", F.row_number().over(w))

df_Research = df_SrcTrgtComp.filter(
    F.col("SRC_EFF_DT_SK").isNull()
    | F.col("SRC_GRP_ID").isNull()
    | F.col("SRC_SRC_SYS_CD_SK").isNull()
    | F.col("TRGT_EFF_DT_SK").isNull()
    | F.col("TRGT_GRP_ID").isNull()
    | F.col("TRGT_SRC_SYS_CD_SK").isNull()
)
df_Research = df_Research.withColumn("TRGT_EFF_DT_SK", F.rpad(F.col("TRGT_EFF_DT_SK"), 10, " "))
df_Research = df_Research.withColumn("SRC_EFF_DT_SK", F.rpad(F.col("SRC_EFF_DT_SK"), 10, " "))
df_Research = df_Research.select(
    "TRGT_SRC_SYS_CD_SK",
    "TRGT_GRP_ID",
    "TRGT_EFF_DT_SK",
    "SRC_SRC_SYS_CD_SK",
    "SRC_GRP_ID",
    "SRC_EFF_DT_SK"
)

df_Notify = df_SrcTrgtComp.filter(
    (F.col("rownum") == 1) & (F.lit(ToleranceCd) == F.lit("OUT"))
)
df_Notify = df_Notify.withColumn(
    "NOTIFICATION",
    F.concat(
        F.lit("ROW COUNT BALANCING "),
        F.lit(SrcSysCd),
        F.lit(" - IDS GRP_AHY_BNF OUT OF TOLERANCE")
    )
)
df_Notify = df_Notify.withColumn("NOTIFICATION", F.rpad(F.col("NOTIFICATION"), 70, " "))
df_Notify = df_Notify.select("NOTIFICATION")

write_files(
    df_Research,
    f"{adls_path}/balancing/research/{SrcSysCd}IdsGrpAhyBnfResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/MedMgtBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)