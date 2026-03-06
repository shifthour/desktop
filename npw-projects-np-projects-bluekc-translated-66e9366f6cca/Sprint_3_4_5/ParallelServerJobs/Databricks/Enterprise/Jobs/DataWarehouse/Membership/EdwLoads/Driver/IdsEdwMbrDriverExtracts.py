# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:      IdsEdwMbrDriverExtracts
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the IDS MBR for all members that have a Last Run Cycle greater than the last run cycle the EDW processed with.
# MAGIC                                Reads the IDS SUB for all subscribers that have a Last Run Cycle greater than the last run cycle the EDW processed with.
# MAGIC                               Loads both groups of SK's into the working table W_MBR_DEL table with an appropriate tag of MEME_CK or SBSB_CK.  
# MAGIC                              W_MBR_DEL  table is used later in the Load sequencers to clear out all mbr or sub records before inserting in new ones.    MBR_COB, MBR_ENRL, MBR_ADDR are some tables for which this occurs
# MAGIC                               
# MAGIC      
# MAGIC 
# MAGIC INPUTS:	        IDS MBR
# MAGIC                         IDS  SUB
# MAGIC 
# MAGIC Modifications:                
# MAGIC Developer                    Date              Project/Altiris #                     Change Description                             Code Reviewer            Date Reviewed        
# MAGIC ================================================================================================================= 
# MAGIC SAndrew                12/15/2005                                                         Initial creation
# MAGIC Brent Leland            04/12/2006                                       Changes SQL logic to >= runcycle from >
# MAGIC SAnderw                 2008-10-21                                        Corrected < 1 to be  =0 or =1 for SK issue in IDS Mbr and IDS Sub extrct                                                                         
# MAGIC 
# MAGIC Srikanth Mettpalli      06/17/2013        5114                              Original Programming     
# MAGIC                                                                                                    (Server to Parallel Conversion)

# MAGIC IDS's  W_MBR_DEL is the extraction "driver" table and is used in all of the extract jobs that are member uniq key or subscriber uniq key dependant.  
# MAGIC 
# MAGIC It serves as a driver table for membership extractions.   It is very similar to the EDW W_MBR_DEL but IDS has SKs and the EDW version doesn't.
# MAGIC Job Name: IdsEdwMbrDriverExtracts
# MAGIC This Job pulls records from IDS's MBR and SUB tables that have a run cycle nbr according to  IDS P_RUN_CYC have a EDW_LOAD_IN = \"N\".  Loads to the \"driver\" tables.
# MAGIC When EDW Membership is processed, it is ran under 2 update modes: daily updates or a full replace.  When a daily update, MonthEnd=N and records are written to the EDW W_MBR_DEL load file.  When a full replace, MonthEnd=Y and records are NOT written to the EDW W_MBR_DEL load file.    W_MBR_DEL is used to delete records for that member that do not correspond to the  IDS RunCycle.  Have had instances where the whole table is wiped out.
# MAGIC Use of EDW W_MBR_DEL is engaged after all of the EDW table loads.  Those mbrs/subs on the EDW tables that are on the EDW W_MBR_DEL table and have records with LAST_UPDT_RUN_CYC_EXCTN_SK <> RunCycle will be deleted.
# MAGIC If MonthEnd = Y then no records will be written to  table EDW W_MBR_DEL.
# MAGIC Union Mbr and Sub Data
# MAGIC Union Mbr and Sub Data
# MAGIC Load File for IDS W_MBR_DRVR
# MAGIC Load File for EDW W_MBR_DEL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
IDSFilePath = get_widget_value('IDSFilePath','')
IDSExtractCycle = get_widget_value('IDSExtractCycle','')
MonthEnd = get_widget_value('MonthEnd','')
ids_secret_name = get_widget_value('ids_secret_name','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sub_sql = f"SELECT SUB.SUB_UNIQ_KEY, SUB.SRC_SYS_CD_SK FROM {IDSOwner}.SUB SUB WHERE SUB.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSExtractCycle} OR SUB.SUB_SK IN (0,1)"
df_db2_SUB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sub_sql)
    .load()
)

df_sub_businesslogic = df_db2_SUB_in.withColumn("MonthEnd", lit(MonthEnd))

df_sub_delData = df_sub_businesslogic.filter(col("MonthEnd") == 'N').select(
    lit("SUB_UNIQ_KEY").alias("SUBJ_NM"),
    lit("FACETS").alias("SRC_SYS_CD"),
    col("SUB_UNIQ_KEY").alias("KEY_VAL_INT"),
    lit(1).alias("CMT_GRP_NO")
)

df_sub_drvrData = df_sub_businesslogic.select(
    lit("SUB_UNIQ_KEY").alias("SUBJ_NM"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("SUB_UNIQ_KEY").alias("KEY_VAL_INT"),
    lit(1).alias("CMT_GRP_NO")
)

mbr_sql = f"SELECT MBR.MBR_UNIQ_KEY, MBR.SRC_SYS_CD_SK FROM {IDSOwner}.MBR MBR WHERE MBR.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSExtractCycle} OR MBR.MBR_SK IN (0,1)"
df_db2_MBR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", mbr_sql)
    .load()
)

df_mbr_businesslogic = df_db2_MBR_in.withColumn("MonthEnd", lit(MonthEnd))

df_mbr_delData = df_mbr_businesslogic.filter(col("MonthEnd") == 'N').select(
    lit("MBR_UNIQ_KEY").alias("SUBJ_NM"),
    lit("FACETS").alias("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY").alias("KEY_VAL_INT"),
    lit(0).alias("CMT_GRP_NO")
)

df_mbr_drvrData = df_mbr_businesslogic.select(
    lit("MBR_UNIQ_KEY").alias("SUBJ_NM"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("MBR_UNIQ_KEY").alias("KEY_VAL_INT"),
    lit(0).alias("CMT_GRP_NO")
)

df_fnl_DrvrData = df_sub_drvrData.select("SUBJ_NM","SRC_SYS_CD_SK","KEY_VAL_INT","CMT_GRP_NO") \
    .unionByName(df_mbr_drvrData.select("SUBJ_NM","SRC_SYS_CD_SK","KEY_VAL_INT","CMT_GRP_NO"))

df_fnl_DrvrData_out = df_fnl_DrvrData.select(
    rpad(col("SUBJ_NM"), 20, " ").alias("SUBJ_NM"),
    col("SRC_SYS_CD_SK"),
    col("KEY_VAL_INT"),
    col("CMT_GRP_NO")
)

write_files(
    df_fnl_DrvrData_out,
    f"{adls_path}/load/W_MBR_DRVR.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_fnl_DelData = df_mbr_delData.select("SUBJ_NM","SRC_SYS_CD","KEY_VAL_INT","CMT_GRP_NO") \
    .unionByName(df_sub_delData.select("SUBJ_NM","SRC_SYS_CD","KEY_VAL_INT","CMT_GRP_NO"))

df_fnl_DelData_out = df_fnl_DelData.select(
    rpad(col("SUBJ_NM"), 20, " ").alias("SUBJ_NM"),
    col("SRC_SYS_CD"),
    col("KEY_VAL_INT"),
    col("CMT_GRP_NO")
)

write_files(
    df_fnl_DelData_out,
    f"{adls_path}/load/W_MBR_DEL.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)