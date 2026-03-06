# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsClmMartMbrDriverExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Extract changed data from IDS Member table for driving the Web Datamart member processing.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	IDS - CLM
# MAGIC                 IDS - CD_MPPNG
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: IDS member source has a seperate SQL to extract the records that changed since the last web Datamart processing.  Last run cycle update is used to track record changes.  All parts of the member are extracted when a change is found in the MBR table.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Web Datamart - W_MBR_DEL
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi D               07/13/2009       3500                                                  Originally Programmed                                                                   devlIDSnew                    Steph Goddard          07/15/2009                                                      
# MAGIC              
# MAGIC Srikanth Mettpalli             06/06/2013          5114                                 Original Programming(Server to Parallel)                                            IntegrateWrhsDevl         Peter Marshall            10/24/2013           
# MAGIC Kalyan Neelam       2015-10-21          5212                                                 Added HOST_MBR_IN <> 'Y' condition to the extract sql            IntegrateDev1               Bhoomi Dasari             10/23/2015

# MAGIC This table drive the Web Mart Member deletes.
# MAGIC Pulling changed rows from Mbr and loading in to SQL server table, which is used for the Mbr delete process
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsDmMbrDmDriverExtr
# MAGIC This table drive the select from the IDS Member tables.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, floor, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter Definitions
ClmMartOwner = get_widget_value('ClmMartOwner','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
IDSOwner = get_widget_value('IDSOwner','')
IdsMbrRunCycle = get_widget_value('IdsMbrRunCycle','')
CommitPoint = get_widget_value('CommitPoint','')
RunID = get_widget_value('RunID','')

# Additional required secret name parameters
ids_secret_name = get_widget_value('ids_secret_name','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

# Convert CommitPoint to integer
commitPoint = int(CommitPoint)

# ------------------------------------------------------------------------------
# db2_Mbr_in (DB2ConnectorPX) --> df_db2_Mbr_in
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

query_db2_Mbr_in = (
    f"SELECT MAP1.TRGT_CD, MBR1.MBR_UNIQ_KEY, MBR1.SRC_SYS_CD_SK "
    f"FROM {IDSOwner}.MBR MBR1, {IDSOwner}.CD_MPPNG MAP1 "
    f"WHERE MBR1.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK "
    f"AND MBR1.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsMbrRunCycle} "
    f"AND MBR1.HOST_MBR_IN <> 'Y' "
    f"UNION "
    f"SELECT MAP2.TRGT_CD, MBR2.MBR_UNIQ_KEY, MBR2.SRC_SYS_CD_SK "
    f"FROM {IDSOwner}.MBR MBR2, {IDSOwner}.CD_MPPNG MAP2, {IDSOwner}.SUB SUB2 "
    f"WHERE MBR2.SRC_SYS_CD_SK = MAP2.CD_MPPNG_SK "
    f"AND MBR2.SUB_SK = SUB2.SUB_SK "
    f"AND SUB2.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsMbrRunCycle} "
    f"AND MBR2.HOST_MBR_IN <> 'Y'"
)

df_db2_Mbr_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_Mbr_in)
    .load()
)

# ------------------------------------------------------------------------------
# xfrm_BusinessLogic (CTransformerStage)
w = Window.orderBy("MBR_UNIQ_KEY")
df_xfrm = df_db2_Mbr_in.withColumn("row_num", row_number().over(w)) \
                       .withColumn("CommitCnt", floor(col("row_num") / commitPoint))

# Output link 1 -> lnk_IdsDmMbrDmDriverExtr2_OutABC -> seq_W_MBR_DM_DRVR_csv_load
df_out2 = df_xfrm.select(
    col("MBR_UNIQ_KEY").alias("SUBJ_NM"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY").alias("KEY_VAL_INT"),
    col("CommitCnt").alias("CMT_GRP_NO")
)
df_out2 = df_out2.withColumn("SUBJ_NM", rpad("SUBJ_NM", 20, " "))
df_out2 = df_out2.select("SUBJ_NM", "SRC_SYS_CD_SK", "KEY_VAL_INT", "CMT_GRP_NO")

# Output link 2 -> lnk_IdsDmMbrDmDriverExtr1_OutABC -> Odbc_W_MBR_DEL_out
df_out1 = df_xfrm.select(
    col("TRGT_CD").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_out1 = df_out1.select("SRC_SYS_CD", "MBR_UNIQ_KEY", "SRC_SYS_CD_SK")

# ------------------------------------------------------------------------------
# seq_W_MBR_DM_DRVR_csv_load (PxSequentialFile)
write_files(
    df_out2,
    f"{adls_path}/load/W_MBR_DM_DRVR.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# ------------------------------------------------------------------------------
# Odbc_W_MBR_DEL_out (ODBCConnectorPX)
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDmMbrDmDriverExtr_Odbc_W_MBR_DEL_out_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

df_out1.write.jdbc(
    url=jdbc_url_clmmart,
    table="STAGING.IdsDmMbrDmDriverExtr_Odbc_W_MBR_DEL_out_temp",
    mode="overwrite",
    properties=jdbc_props_clmmart
)

truncate_sql = f"TRUNCATE TABLE {ClmMartOwner}.W_MBR_DEL"
execute_dml(truncate_sql, jdbc_url_clmmart, jdbc_props_clmmart)

insert_sql = (
    f"INSERT INTO {ClmMartOwner}.W_MBR_DEL (SRC_SYS_CD, MBR_UNIQ_KEY, SRC_SYS_CD_SK) "
    f"SELECT SRC_SYS_CD, MBR_UNIQ_KEY, SRC_SYS_CD_SK "
    f"FROM STAGING.IdsDmMbrDmDriverExtr_Odbc_W_MBR_DEL_out_temp"
)
execute_dml(insert_sql, jdbc_url_clmmart, jdbc_props_clmmart)