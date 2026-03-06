# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwHooperHolmesCntl
# MAGIC 
# MAGIC PROCESSING:  Extracts data from the UWS table IHM_ELIG_GRP_LIST.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                  Project/Altiris #      \(9)    Change Description                                                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------      ------------------------      \(9)    ------------------------------------------------------------------              --------------------------------       -------------------------------   ----------------------------       
# MAGIC Rishi Reddy             2010-05-25        4283 - Hooper Holmes\(9)    Original Programming\(9)\(9)                    EnterpriseCurDevl            Steph Goddard          05/28/2010
# MAGIC Kalyan Neelam        2011-07-08         4673                                  Added 2 new fields                                                 EnterpriseWrhsDevl         Brent Leland              7-27-2011
# MAGIC                                                                                                      (SUBGRP_ID, SRC_SYS_CD) to the load file.
# MAGIC                                                                                                      Added step to write the various 
# MAGIC                                                                                                     Group/Subgroup/Source combinations to a Seq file
# MAGIC 
# MAGIC Bhoomi Dasari        2011-10-05          4673                                 Changed the 'WHERE' logic in SQL while               EnterpriseWrhsDevl            SAndrew                2011-10-11
# MAGIC                                                                                                     checking dates to '>' instead of '>=' 
# MAGIC                                                                                                     (UWS_IHM_ELIG_GRP_LIST)
# MAGIC 
# MAGIC Bhoomi Dasari        2011-12-08          4673                                  Added new logic to include 'ALL' condition             EnterpriseWrhsDevl              Sandew                2011-12-13
# MAGIC                                                                                                      to include subgrp's for that particular GRP

# MAGIC Extract all the various Group/Subgrp/Source combinations. This file is lated used in EdwScreeningMemberEligibilityExtr
# MAGIC Pull rows with SUBGRP_ID = 'ALL'
# MAGIC Pull rows with SUBGRP_ID <> 'ALL'
# MAGIC Pull all rows for that particular SUBGRP_ID by joining on GRP_ID
# MAGIC P_IHM_ELIG_GRP_LIST Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
UWSOwner = get_widget_value('UWSOwner','')
CurrDate = get_widget_value('CurrDate','2011-12-08')
LastRunDate = get_widget_value('LastRunDate','2011-06-01')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

df_EDW_GrpExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT GRP_D.GRP_ID,\n            SUBGRP_D.SUBGRP_ID,\n            GRP_D.GRP_SK,\n            SUBGRP_D.SUBGRP_SK\nFROM \n          #$EDWOwner#.GRP_D GRP_D,\n          #$EDWOwner#.SUBGRP_D SUBGRP_D\nWHERE\nGRP_D.GRP_SK = SUBGRP_D.GRP_SK"
    )
    .load()
)

df_EDW_SubGrpExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT SUBGRP_D.GRP_ID,\n            SUBGRP_D.SUBGRP_ID\nFROM \n           #$EDWOwner#.SUBGRP_D SUBGRP_D"
    )
    .load()
)

df_hf_pihmgrpeliglist_grplkup = df_EDW_GrpExtract.dropDuplicates(["GRP_ID","SUBGRP_ID"])

df_UWS_Extract_NotAll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, GRP_ID, SUBGRP_ID, USER_ID, LAST_UPDT_DT_SK "
        f"FROM #$UWSOwner#.IHM_ELIG_GRP_LIST "
        f"WHERE LAST_UPDT_DT_SK > '{LastRunDate}' AND LAST_UPDT_DT_SK <= '{CurrDate}' AND SUBGRP_ID <> 'ALL' "
        f"ORDER BY SRC_SYS_CD, GRP_ID, SUBGRP_ID"
    )
    .load()
)

df_UWS_Extract_All = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option(
        "query",
        f"SELECT GRP_ID, SRC_SYS_CD, SUBGRP_ID, USER_ID, LAST_UPDT_DT_SK "
        f"FROM #$UWSOwner#.IHM_ELIG_GRP_LIST "
        f"WHERE LAST_UPDT_DT_SK > '{LastRunDate}' AND LAST_UPDT_DT_SK <= '{CurrDate}' AND SUBGRP_ID = 'ALL' "
        f"ORDER BY GRP_ID, SRC_SYS_CD, SUBGRP_ID"
    )
    .load()
)

df_StripField_NotAll = (
    df_UWS_Extract_NotAll.filter(F.col("SUBGRP_ID") != "ALL")
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
    .withColumn("GRP_ID", strip_field(trim(F.col("GRP_ID"))))
    .withColumn("SUBGRP_ID", strip_field(trim(F.col("SUBGRP_ID"))))
    .withColumn("USER_ID", F.col("USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("LAST_UPDT_DT_SK"))
)

df_hf_pihmgrpeliglist_alllkup = df_StripField_NotAll.dropDuplicates(["SRC_SYS_CD","GRP_ID","SUBGRP_ID"])

df_hf_pihmgrpeliglist_subgrplkup = df_UWS_Extract_All.dropDuplicates(["GRP_ID"])

df_Business_Logic_join = df_EDW_SubGrpExtract.alias("EDW_SubGrpExtract").join(
    df_hf_pihmgrpeliglist_subgrplkup.alias("All_Extract"),
    [
        F.col("EDW_SubGrpExtract.GRP_ID") == F.col("All_Extract.GRP_ID"),
        F.col("EDW_SubGrpExtract.GRP_ID") == F.col("All_Extract.SRC_SYS_CD"),
    ],
    "left",
)

df_Business_Logic = (
    df_Business_Logic_join.filter(F.col("All_Extract.GRP_ID").isNotNull())
    .select(
        F.col("All_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("EDW_SubGrpExtract.GRP_ID").alias("GRP_ID"),
        F.col("EDW_SubGrpExtract.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("All_Extract.USER_ID").alias("USER_ID"),
        F.col("All_Extract.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    )
)

df_hf_pihmgrpeliglist_notalllookup = df_Business_Logic.dropDuplicates(["SRC_SYS_CD","GRP_ID","SUBGRP_ID"])

df_Link_Collector_notall = df_hf_pihmgrpeliglist_notalllookup.select(
    "SRC_SYS_CD","GRP_ID","SUBGRP_ID","USER_ID","LAST_UPDT_DT_SK"
)

df_Link_Collector_all = df_hf_pihmgrpeliglist_alllkup.select(
    "SRC_SYS_CD","GRP_ID","SUBGRP_ID","USER_ID","LAST_UPDT_DT_SK"
)

df_Link_Collector = df_Link_Collector_notall.unionByName(df_Link_Collector_all)

df_hf_pihmgrpeliglist_dups = df_Link_Collector.dropDuplicates(["SRC_SYS_CD","GRP_ID","SUBGRP_ID"])

windowSpec_Transformer = Window.orderBy("SRC_SYS_CD")
df_Transformer_1 = (
    df_hf_pihmgrpeliglist_dups.withColumn("RecInd", F.dense_rank().over(windowSpec_Transformer) - 1)
)

df_Transformer_joined = df_Transformer_1.alias("UWS_Extract").join(
    df_hf_pihmgrpeliglist_grplkup.alias("Grp_lkup"),
    [
        F.col("UWS_Extract.GRP_ID") == F.col("Grp_lkup.GRP_ID"),
        F.col("UWS_Extract.SUBGRP_ID") == F.col("Grp_lkup.SUBGRP_ID"),
    ],
    "left",
)

df_load = df_Transformer_joined.filter(F.col("UWS_Extract.GRP_ID").isNotNull()).select(
    F.col("Grp_lkup.GRP_SK").alias("GRP_SK"),
    F.col("Grp_lkup.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("UWS_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("UWS_Extract.USER_ID").alias("USER_ID"),
    F.col("UWS_Extract.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
)

df_writeToFile = df_Transformer_joined.select(
    F.col("UWS_Extract.GRP_ID").alias("GRP_ID"),
    F.col("UWS_Extract.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("UWS_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("RecInd").alias("RecInd"),
)

df_load_final = df_load.select(
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.rpad(F.col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("USER_ID"), 50, " ").alias("USER_ID"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
)

write_files(
    df_load_final,
    f"{adls_path}/load/P_IHM_ELIG_GRP_LIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_writeToFile_final = df_writeToFile.select(
    F.rpad(F.col("GRP_ID"), 50, " ").alias("GRP_ID"),
    F.rpad(F.col("SUBGRP_ID"), 50, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("SRC_SYS_CD"), 50, " ").alias("SRC_SYS_CD"),
    F.col("RecInd").alias("RecInd"),
)

write_files(
    df_writeToFile_final,
    f"{adls_path_publish}/external/ScrnMbrElig_GrpSubgrpSrcCombinations.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)