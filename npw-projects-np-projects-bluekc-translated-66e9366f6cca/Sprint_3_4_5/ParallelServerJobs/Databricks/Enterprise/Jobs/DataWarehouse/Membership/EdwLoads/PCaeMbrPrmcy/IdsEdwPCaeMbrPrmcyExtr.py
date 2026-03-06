# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwPCaeSeq
# MAGIC                
# MAGIC PROCESSING:   This job extracts the records from IDS P_CAE_MBR_PRMCY and IDS P_CAE_MBR_DRVR and writes them to a load file.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Laurel Kindley\(9)2007-01-25      \(9)\(9)\(9)Original Programming.
# MAGIC Laurel Kindley\(9)2008-05-23\(9)3051\(9)\(9)Added extract for P_CAE_MBR_DRVR table\(9)devlEDW                                 Steph Goddard         05/28/2008
# MAGIC Pooja Sunkara         2013-07-29              5114                        Conversion from Server to Parallel version               EnterpriseWrhsDevl

# MAGIC This job extracts the records from IDS P_CAE_MBR_PRMCY and IDS P_CAE_MBR_DRVR and writes them to a load file.
# MAGIC Job name:
# MAGIC IdsEdwPCaeMbrPrmcyExtr
# MAGIC Write P_CAE_MBR_PRMCY Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table P_CAE_MBR_PRMCY
# MAGIC Read data from source table P_CAE_MBR_DRVR
# MAGIC Write P_CAE_MBR_DRVR Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter definitions
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: db2_P_CAE_MBR_PRMCY_in (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_P_CAE_MBR_PRMCY_in = f"""
SELECT
P_CAE_MBR_PRMCY.MBR_SK,
P_CAE_MBR_PRMCY.CAE_UNIQ_KEY,
P_CAE_MBR_PRMCY.INDV_BE_KEY,
P_CAE_MBR_PRMCY.PRI_IN,
P_CAE_MBR_PRMCY.GRP_SK,
P_CAE_MBR_PRMCY.MBR_BRTH_DT,
P_CAE_MBR_PRMCY.MBR_GNDR_CD,
P_CAE_MBR_PRMCY.LAST_UPDT_DT
FROM {IDSOwner}.p_cae_mbr_prmcy P_CAE_MBR_PRMCY
"""
df_db2_P_CAE_MBR_PRMCY_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_P_CAE_MBR_PRMCY_in)
    .load()
)

# Stage: Copy_Mppng (PxCopy)
df_Copy_Mppng = df_db2_P_CAE_MBR_PRMCY_in.select(
    df_db2_P_CAE_MBR_PRMCY_in["MBR_SK"].alias("MBR_SK"),
    df_db2_P_CAE_MBR_PRMCY_in["CAE_UNIQ_KEY"].alias("CAE_UNIQ_KEY"),
    df_db2_P_CAE_MBR_PRMCY_in["INDV_BE_KEY"].alias("INDV_BE_KEY"),
    df_db2_P_CAE_MBR_PRMCY_in["PRI_IN"].alias("PRI_IN"),
    df_db2_P_CAE_MBR_PRMCY_in["GRP_SK"].alias("GRP_SK"),
    df_db2_P_CAE_MBR_PRMCY_in["MBR_BRTH_DT"].alias("MBR_BRTH_DT"),
    df_db2_P_CAE_MBR_PRMCY_in["MBR_GNDR_CD"].alias("MBR_GNDR_CD"),
    df_db2_P_CAE_MBR_PRMCY_in["LAST_UPDT_DT"].alias("LAST_UPDT_DT")
)

# Stage: seq_P_CAE_MBR_PRMCY_csv_load (PxSequentialFile)
df_seq_P_CAE_MBR_PRMCY_csv_load = (
    df_Copy_Mppng
    .withColumn("PRI_IN", rpad("PRI_IN", 1, " "))
    .select(
        "MBR_SK",
        "CAE_UNIQ_KEY",
        "INDV_BE_KEY",
        "PRI_IN",
        "GRP_SK",
        "MBR_BRTH_DT",
        "MBR_GNDR_CD",
        "LAST_UPDT_DT"
    )
)
write_files(
    df_seq_P_CAE_MBR_PRMCY_csv_load,
    f"{adls_path}/load/P_CAE_MBR_PRMCY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Stage: db2_P_CAE_MBR_DRVR_in (DB2ConnectorPX)
extract_query_db2_P_CAE_MBR_DRVR_in = f"""
SELECT
P_CAE_MBR_DRVR.MBR_SK,
P_CAE_MBR_DRVR.CAE_UNIQ_KEY,
P_CAE_MBR_DRVR.INDV_BE_KEY,
P_CAE_MBR_DRVR.PRI_IN,
P_CAE_MBR_DRVR.GRP_SK,
P_CAE_MBR_DRVR.MBR_BRTH_DT,
P_CAE_MBR_DRVR.MBR_GNDR_CD,
P_CAE_MBR_DRVR.LAST_UPDT_DT
FROM {IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR
"""
df_db2_P_CAE_MBR_DRVR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_P_CAE_MBR_DRVR_in)
    .load()
)

# Stage: Cpy_Mppng (PxCopy)
df_Cpy_Mppng = df_db2_P_CAE_MBR_DRVR_in.select(
    df_db2_P_CAE_MBR_DRVR_in["MBR_SK"].alias("MBR_SK"),
    df_db2_P_CAE_MBR_DRVR_in["CAE_UNIQ_KEY"].alias("CAE_UNIQ_KEY"),
    df_db2_P_CAE_MBR_DRVR_in["INDV_BE_KEY"].alias("INDV_BE_KEY"),
    df_db2_P_CAE_MBR_DRVR_in["PRI_IN"].alias("PRI_IN"),
    df_db2_P_CAE_MBR_DRVR_in["GRP_SK"].alias("GRP_SK"),
    df_db2_P_CAE_MBR_DRVR_in["MBR_BRTH_DT"].alias("MBR_BRTH_DT"),
    df_db2_P_CAE_MBR_DRVR_in["MBR_GNDR_CD"].alias("MBR_GNDR_CD"),
    df_db2_P_CAE_MBR_DRVR_in["LAST_UPDT_DT"].alias("LAST_UPDT_DT")
)

# Stage: seq_P_CAE_MBR_DRVR_csv_load (PxSequentialFile)
df_seq_P_CAE_MBR_DRVR_csv_load = (
    df_Cpy_Mppng
    .withColumn("PRI_IN", rpad("PRI_IN", 1, " "))
    .select(
        "MBR_SK",
        "CAE_UNIQ_KEY",
        "INDV_BE_KEY",
        "PRI_IN",
        "GRP_SK",
        "MBR_BRTH_DT",
        "MBR_GNDR_CD",
        "LAST_UPDT_DT"
    )
)
write_files(
    df_seq_P_CAE_MBR_DRVR_csv_load,
    f"{adls_path}/load/P_CAE_MBR_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)