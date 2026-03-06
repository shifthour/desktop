# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmMartMbrDriverHitListLoad
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    ONE TIME Load DM MBR driver table with test members for subsequent member extracts.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	sequential file containing member unique keys 
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: 
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    IDS - W_MBR_DM_DRVR
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC TerriO                    09/04/2009       4113 Member 360                                Originally Programmed                                                                   devlIDSnew                   Steph Goddard         10/17/2009
# MAGIC                                                                                                          
# MAGIC              
# MAGIC Srikanth Mettpalli             06/06/2013          5114                                 Original Programming(Server to Parallel)                                            IntegrateWrhsDevl          Peter Marshall           10/24/2013
# MAGIC Kalyan Neelam         2015-10-22          5212                                            Added condition HOST_MBR_IN <> 'Y' to Mbr extract SQL            IntegrateDev1
# MAGIC 
# MAGIC Michael Harmon      2016-12-010        5541 DirectPay                          Renamed Transforms (to force recompile) and removed extra                Integrate\\Dev1
# MAGIC                                                                                                               comma from end of COALESCE line in MBR lookup

# MAGIC Job Name: IdsDmMbrDmDriverHitListExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Mbrs from hit list and loading in to SQL server table, which is used for the Mbr delete process
# MAGIC SQL Server Table
# MAGIC #$FilePath#/update/MbrDMHitList.dat
# MAGIC IDS Table
# MAGIC File
# MAGIC This table drive the Web Mart Member deletes.
# MAGIC This table drive the select from the IDS Member tables.
# MAGIC #FilePath#/update/MbrDMHitListNotFound.dat
# MAGIC Used to email if hit list entries not found
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartRecordCount = get_widget_value("ClmMartRecordCount","")
ClmMartArraySize = get_widget_value("ClmMartArraySize","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ClmMartOwner = get_widget_value("ClmMartOwner","")
clmmart_secret_name = get_widget_value("clmmart_secret_name","")
CommitPoint = get_widget_value("CommitPoint","0")
RunID = get_widget_value("RunID","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

db2_Mbr_in_query = f"""
SELECT
  MBR.SRC_SYS_CD_SK,
  MBR.MBR_UNIQ_KEY,
  COALESCE(MAP.TRGT_CD,'UNK') AS SRC_SYS_CD
FROM {IDSOwner}.MBR MBR
LEFT JOIN {IDSOwner}.CD_MPPNG MAP
    ON MBR.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
WHERE MBR.HOST_MBR_IN <> 'Y'
"""

df_db2_Mbr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", db2_Mbr_in_query)
    .load()
)

schema_seq_MbrDMHitList_csv_load = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False)
])

df_seq_MbrDMHitList_csv_load = (
    spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_seq_MbrDMHitList_csv_load)
    .load(f"{adls_path}/update/MbrDMHitList.dat")
)

df_xfm_Trim = df_seq_MbrDMHitList_csv_load.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY"),
    trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD")
)

df_lkp_join = df_xfm_Trim.alias("pr").join(
    df_db2_Mbr_in.alias("ref"),
    (
        (F.col("pr.SRC_SYS_CD_SK") == F.col("ref.SRC_SYS_CD_SK")) &
        (F.col("pr.MBR_UNIQ_KEY") == F.col("ref.MBR_UNIQ_KEY")) &
        (F.col("pr.SRC_SYS_CD") == F.col("ref.SRC_SYS_CD"))
    ),
    "left"
)

df_lkp_Mbr_Data_out = df_lkp_join.filter(
    F.col("ref.SRC_SYS_CD_SK").isNotNull()
).select(
    F.col("pr.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("pr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("pr.SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_IdsDmMbrDmDriverHitListExtr3_OutABC = df_lkp_join.filter(
    F.col("ref.SRC_SYS_CD_SK").isNull()
).select(
    F.col("pr.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("pr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("pr.SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_IdsDmMbrDmDriverHitListExtr3_OutABC_out = (
    df_IdsDmMbrDmDriverHitListExtr3_OutABC
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .select("SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "SRC_SYS_CD")
)

write_files(
    df_IdsDmMbrDmDriverHitListExtr3_OutABC_out,
    f"{adls_path}/update/MbrDMHitListNotFound.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

windowSpec = Window.orderBy("SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "SRC_SYS_CD")
df_tmp_var = (
    df_lkp_Mbr_Data_out
    .withColumn("row_number", F.row_number().over(windowSpec))
)
df_tmp_var2 = df_tmp_var.withColumn(
    "CommitCnt",
    F.floor(F.col("row_number") / F.lit(F.when(F.lit(CommitPoint) == F.lit(""), 1).otherwise(CommitPoint).cast("int")))
)

df_IdsDmMbrDmDriverHitListExtr2_OutABC = df_tmp_var2.select(
    F.rpad(F.col("MBR_UNIQ_KEY").cast(StringType()), 20, " ").alias("SUBJ_NM"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("KEY_VAL_INT"),
    F.col("CommitCnt").alias("CMT_GRP_NO")
)

df_IdsDmMbrDmDriverHitListExtr1_OutABC = df_tmp_var2.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrDmDriverHitListExtr_Odbc_W_MBR_DEL_out_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

execute_dml(
    """
    CREATE TABLE STAGING.IdsDmMbrDmDriverHitListExtr_Odbc_W_MBR_DEL_out_temp (
      SRC_SYS_CD VARCHAR(100),
      MBR_UNIQ_KEY INT,
      SRC_SYS_CD_SK INT
    )
    """,
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

df_IdsDmMbrDmDriverHitListExtr1_OutABC.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsDmMbrDmDriverHitListExtr_Odbc_W_MBR_DEL_out_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.W_MBR_DEL AS T
USING STAGING.IdsDmMbrDmDriverHitListExtr_Odbc_W_MBR_DEL_out_temp AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN
  UPDATE SET T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MBR_UNIQ_KEY, SRC_SYS_CD_SK)
  VALUES (S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.SRC_SYS_CD_SK);
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)

df_seq_W_MBR_DM_DRVR_csv_load_out = df_IdsDmMbrDmDriverHitListExtr2_OutABC.select(
    "SUBJ_NM", "SRC_SYS_CD_SK", "KEY_VAL_INT", "CMT_GRP_NO"
)

write_files(
    df_seq_W_MBR_DM_DRVR_csv_load_out,
    f"{adls_path}/load/W_MBR_DM_DRVR_updt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)