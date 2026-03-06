# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          5/21/2009        Web Realign 3500                             New ETL                                                                                     devlIDSNew                    Steph Goddard           05/28/2009
# MAGIC 
# MAGIC  SAndrew               2009-08-18         Production Support                            Brought down from ids20 to testIDS for emergency prod fix         testIDS                    
# MAGIC                                                                                                                   Changed the SQLServer Update Mode from Clear table to Insert/Update
# MAGIC                                                                                                                     Added tbl to the delete process 
# MAGIC Kalyan Neelam       2010-04-14         4428                                                 Added 19 new fields at the end                                                      IntegrateWrhsDevl         Steph Goddard           04/19/2010
# MAGIC 
# MAGIC Bhoomi Dasari        2011-10-17        4673                                              Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)     IntegrateWrhsDevl           SAndrew                  2011-10-21       
# MAGIC   
# MAGIC Archana Palivela             08/08/2013          5114                                 Original Programming(Server to Parallel)                                              IntegrateWrhsDevl           Jag Yelavarthi           2013-11-30
# MAGIC 
# MAGIC Pooja Sunkara              01/22/2014            5114                                 Changed the Lookup type from Normal to Sparse for                          IntegrateWrhsDevl  
# MAGIC                                                                                                                 Performance efficiency

# MAGIC Write W_WEBDM_ETL_DRVR Data into a Sequential file for Load Job
# MAGIC Lookup condition is set to Drop records as we intend to not to process them if they does not exist in Claim table
# MAGIC Write W_CLM_DEL Data into a Sequential file for Load Job
# MAGIC Reads data from IdsClmMartHitList.dat file.
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmMbrshDmMbrHlthScrnExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import types as T
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
NascoRunCycle = get_widget_value('NascoRunCycle','')
TwoYearsAgo = get_widget_value('TwoYearsAgo','')
FctsRunCycle = get_widget_value('FctsRunCycle','')

schema_Seq_HitList_In = T.StructType([
    T.StructField("CLM_SK", T.IntegerType(), False),
    T.StructField("CLM_ID", T.StringType(), True),
    T.StructField("SRC_SYS_CD_SK", T.IntegerType(), True)
])

df_Seq_HitList_In = (
    spark.read
    .schema(schema_Seq_HitList_In)
    .option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .csv(f"{adls_path}/update/IdsClmMartHitList.dat")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
execute_dml("DROP TABLE IF EXISTS STAGING.IdsDmClmMartHitListExtr_DB2_CD_MPPNG_In_temp", jdbc_url, jdbc_props)

(
    df_Seq_HitList_In.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsDmClmMartHitListExtr_DB2_CD_MPPNG_In_temp")
    .mode("overwrite")
    .save()
)

extract_query_DB2_CD_MPPNG_In = (
    f"select c.CLM_ID, c.LAST_UPDT_RUN_CYC_EXCTN_SK, c.SRC_SYS_CD_SK, m.TRGT_CD as SRC_SYS_CD "
    f"from {IDSOwner}.CLM c "
    f"join {IDSOwner}.CD_MPPNG m on c.SRC_SYS_CD_SK = m.CD_MPPNG_SK "
    f"join STAGING.IdsDmClmMartHitListExtr_DB2_CD_MPPNG_In_temp t on c.CLM_SK = t.CLM_SK"
)

df_DB2_CD_MPPNG_In = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_CD_MPPNG_In)
    .load()
)

df_Lkp_Codes = df_DB2_CD_MPPNG_In

df_xfrm_BusinessLogic = df_Lkp_Codes.filter(F.col("CLM_ID").isNotNull())

df_xfrm_BusinessLogic_output1 = df_xfrm_BusinessLogic.select(
    "CLM_ID",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "SRC_SYS_CD"
)
df_xfrm_BusinessLogic_output1 = (
    df_xfrm_BusinessLogic_output1
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
)

df_xfrm_BusinessLogic_output2 = df_xfrm_BusinessLogic.select(
    "CLM_ID",
    "SRC_SYS_CD_SK",
    "SRC_SYS_CD"
)
df_xfrm_BusinessLogic_output2 = (
    df_xfrm_BusinessLogic_output2
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
)

write_files(
    df_xfrm_BusinessLogic_output1,
    f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_xfrm_BusinessLogic_output2,
    f"{adls_path}/load/W_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)