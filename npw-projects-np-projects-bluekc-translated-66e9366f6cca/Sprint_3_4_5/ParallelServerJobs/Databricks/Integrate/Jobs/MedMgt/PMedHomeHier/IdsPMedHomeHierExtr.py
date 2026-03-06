# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: TreoIdsMbrPcpAttrbtnCntl
# MAGIC 
# MAGIC PROCESSING:  Extracts data from the UWS tables MED _HOME_HIER and MED_HOME_PCP and loads into P_MED _HOME_HIER
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #      \(9)Change Description                                      Development Project        Code Reviewer          Date Reviewed       
# MAGIC ------------------                     --------------------     ------------------------      \(9)   -----------------------------------------------------------------------              --------------------------------       -------------------------------   ----------------------------       
# MAGIC Santosh Bokka       2013-06-09              4917                                   Original Programming                                             IntegrateNewDevl    Bhoomi Dasari           5/29/2013        
# MAGIC                                                                                                                                                                                          IntegrateNewDevl    SAndrew                 2013-10-25  - final review since a lot of time as elapsed.  no reported changes, reviewed just as part of an overall review        
# MAGIC 
# MAGIC Krishnakanth           2018-03-05              60037                             Added logic to populate the columns                        IntegrateDev2          Jaideep Mankala            03/07/2018
# MAGIC    Manivannan                                                                                PROV_REL_GRP_PROV_ID
# MAGIC                                                                                                       , PROV_REL_GRP_PROV_NM in the
# MAGIC                                                                                                       target IDS table.

# MAGIC P_MED_HOME_HIER Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url_UWS_MED_HOME_HIER, jdbc_props_UWS_MED_HOME_HIER = get_db_config(uws_secret_name)
extract_query_UWS_MED_HOME_HIER = f"""SELECT 
    MHH.MED_HOME_LOC_ID
,   MHH.MED_HOME_LOC_STRT_DT_SK
,   MHH.ENTY_MED_HOME_ID
,   MHH.FCTS_ENTY_MED_HOME_ID
,   MHH.FCTS_ENTY_MED_HOME_DESC
,   MHH.MED_HOME_ID
,   MHH.MED_HOME_DESC
,   MHH.FCTS_MED_HOME_LOC_ID
,   MHH.FCTS_MED_HOME_LOC_DESC
,   MHH.MED_HOME_LOC_END_DT_SK
,   MHP.PROV_ID
,   MHP.PCP_STRT_DT_SK
,   MHP.PROV_NM
,   MHP.PCP_END_DT_SK
,   MHP.USER_ID
,   MHP.LAST_UPDT_DT_SK
,   MHH.PROV_REL_GRP_PROV_ID
,   MHH.PROV_REL_GRP_PROV_NM

FROM {UWSOwner}.MED_HOME_HIER MHH
,    {UWSOwner}.MED_HOME_PCP MHP
WHERE 
MHH.MED_HOME_LOC_ID = MHP.MED_HOME_LOC_ID
"""

df_UWS_MED_HOME_HIER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_UWS_MED_HOME_HIER)
    .options(**jdbc_props_UWS_MED_HOME_HIER)
    .option("query", extract_query_UWS_MED_HOME_HIER)
    .load()
)

df_xfm = df_UWS_MED_HOME_HIER.select(
    F.col("MED_HOME_LOC_ID").alias("MED_HOME_LOC_ID"),
    FORMAT_DATE(F.col("MED_HOME_LOC_STRT_DT_SK"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD').alias("MED_HOME_LOC_STRT_DT_SK"),
    F.col("ENTY_MED_HOME_ID").alias("ENTY_MED_HOME_ID"),
    F.col("FCTS_ENTY_MED_HOME_ID").alias("FCTS_ENTY_MED_HOME_ID"),
    F.col("FCTS_ENTY_MED_HOME_DESC").alias("FCTS_ENTY_MED_HOME_DESC"),
    F.col("MED_HOME_ID").alias("MED_HOME_ID"),
    F.col("MED_HOME_DESC").alias("MED_HOME_DESC"),
    F.col("FCTS_MED_HOME_LOC_ID").alias("FCTS_MED_HOME_LOC_ID"),
    F.col("FCTS_MED_HOME_LOC_DESC").alias("FCTS_MED_HOME_LOC_DESC"),
    FORMAT_DATE(F.col("MED_HOME_LOC_END_DT_SK"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD').alias("MED_HOME_LOC_END_DT_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_NM").alias("PROV_NM"),
    FORMAT_DATE(F.col("PCP_STRT_DT_SK"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD').alias("PCP_STRT_DT_SK"),
    FORMAT_DATE(F.col("PCP_END_DT_SK"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD').alias("PCP_END_DT_SK"),
    F.col("USER_ID").alias("USER_ID"),
    FORMAT_DATE(F.col("LAST_UPDT_DT_SK"), 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD').alias("LAST_UPDT_DT_SK"),
    trim(F.col("PROV_REL_GRP_PROV_ID")).alias("PROV_REL_GRP_PROV_ID"),
    trim(F.col("PROV_REL_GRP_PROV_NM")).alias("PROV_REL_GRP_PROV_NM")
)

df_final = df_xfm.select(
    F.col("MED_HOME_LOC_ID"),
    F.rpad(F.col("MED_HOME_LOC_STRT_DT_SK"), 10, " ").alias("MED_HOME_LOC_STRT_DT_SK"),
    F.col("ENTY_MED_HOME_ID"),
    F.col("FCTS_ENTY_MED_HOME_ID"),
    F.col("FCTS_ENTY_MED_HOME_DESC"),
    F.col("MED_HOME_ID"),
    F.col("MED_HOME_DESC"),
    F.col("FCTS_MED_HOME_LOC_ID"),
    F.col("FCTS_MED_HOME_LOC_DESC"),
    F.rpad(F.col("MED_HOME_LOC_END_DT_SK"), 10, " ").alias("MED_HOME_LOC_END_DT_SK"),
    F.col("PROV_ID"),
    F.col("PROV_NM"),
    F.rpad(F.col("PCP_STRT_DT_SK"), 10, " ").alias("PCP_STRT_DT_SK"),
    F.rpad(F.col("PCP_END_DT_SK"), 10, " ").alias("PCP_END_DT_SK"),
    F.col("USER_ID"),
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM")
)

write_files(
    df_final,
    f"{adls_path}/load/P_MED_HOME_HIER.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)