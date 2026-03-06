# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsBlueKCHstdMbrAttrbtnProcessingExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   EDW -> IDS (Creating Primary Key for the attributed members)
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     EDW Tables                                     Flat Files
# MAGIC                    None                                            PCHM_ATTR_ALL_MBRS_ATTRIBUTED.dat.#RunID# 
# MAGIC                                                                         PCHM_ATTR_ALL_HSTD_MBRS_ATTRIBUTED.dat.#RunID# 
# MAGIC                                                                         
# MAGIC HASH FILES:             
# MAGIC 
# MAGIC 	hf_mbr_pcp_attrbtn_allcol;
# MAGIC                 hf_mbr_pcp_attrbtn_trnsfrm;
# MAGIC                 hf_mbr_pcp_attrbtn
# MAGIC 
# MAGIC TRANSFORMS:   AGGREGATE, LOOK UP
# MAGIC                              
# MAGIC                            
# MAGIC PROCESSING:  Creates the Primary key for all the records with Member and attributed provider information to load into IDS - MBR_PCP_ATTRBTN table.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential files
# MAGIC                       PCHM_ATTR_ALL_HSTD_MBRS_ATTRIBUTED.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ========================================================================================================================================================================
# MAGIC Developer                          Date                          Project/Altiris #                                                    Change Description                               Development Project               Code Reviewer            Date Reviewed
# MAGIC ========================================================================================================================================================================
# MAGIC Manasa Andru              2016-05-31          30001(Data Catalyst-Mbr Attribution)                             Original Programming                                  IntegrateDev2                       Jag Yelavarthi              2016-06-02   
# MAGIC 
# MAGIC Manasa Andru             2016-07-21           30001-Mbr Attribution Phase 1                     Removed the LinkCollector and Hosted Members      IntegrateDev2                        Jag Yelavarthi              2016-07-25
# MAGIC                                                                                                                                          final file as it is now concatenated with the 
# MAGIC                                                                                                                                                 Regular/Home Members file.

# MAGIC This is the ouput file from IdsBlueKCMbrAttrbtnProcessingExtr job which contains members and their attributed provider informnation
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, when, concat, to_date, date_format, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FirstDayNextMonth = get_widget_value('FirstDayNextMonth','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/MbrPcpAttrbtnPK
# COMMAND ----------

schema_FinalFile = StructType([
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("ATTRIBUTED_PROV", StringType(), False),
    StructField("ATTRIBUTED_PROV_GRP", StringType(), False),
    StructField("LAST_EVAL_AND_MNG_SVC_DT_SK", StringType(), False),
    StructField("PROV_SK", IntegerType(), False),
    StructField("REL_GRP_PROV_SK", IntegerType(), False),
    StructField("MBR_PCP_MO_NO", DecimalType(38,10), False),
    StructField("MED_HOME_ID", StringType(), False),
    StructField("MED_HOME_DESC", StringType(), False),
    StructField("FCTS_ENTY_MED_HOME_ID", StringType(), False),
    StructField("FCTS_ENTY_MED_HOME_DESC", StringType(), False),
    StructField("FCTS_MED_HOME_LOC_ID", StringType(), False),
    StructField("FCTS_MED_HOME_LOC_DESC", StringType(), False)
])

df_FinalFile = (
    spark.read
    .option("header", True)
    .option("quote", "\"")
    .option("sep", ",")
    .schema(schema_FinalFile)
    .csv(f"{adls_path_raw}/landing/PCMH_ATTR_ALL_MBRS_ATTRIBUTED.dat.{RunID}")
)

df_step5_input = df_FinalFile

df_step5_allcol = df_step5_input.select(
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    lit("240").alias("ATTRBTN_BCBS_PLN_CD"),
    lit(FirstDayNextMonth).alias("ROW_EFF_DT_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    concat(col("MBR_UNIQ_KEY"), lit(";"), lit(FirstDayNextMonth), lit(";"), lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    lit(0).alias("MBR_PCP_ATTRBTN_SK"),
    lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK").alias("MBR_SK"),
    lit("N").alias("COB_IN"),
    when(col("LAST_EVAL_AND_MNG_SVC_DT_SK").isNull(), lit(None))
     .otherwise(
         date_format(to_date(trim(col("LAST_EVAL_AND_MNG_SVC_DT_SK")), "yyyyMMdd"), "yyyy-MM-dd")
     ).alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    col("MBR_INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("MBR_PCP_MO_NO").alias("MBR_PCP_MO_NO"),
    col("MED_HOME_ID").alias("MED_HOME_ID"),
    col("MED_HOME_DESC").alias("MED_HOME_DESC"),
    col("FCTS_ENTY_MED_HOME_ID").alias("MED_HOME_GRP_ID"),
    col("FCTS_ENTY_MED_HOME_DESC").alias("MED_HOME_GRP_DESC"),
    col("FCTS_MED_HOME_LOC_ID").alias("MED_HOME_LOC_ID"),
    col("FCTS_MED_HOME_LOC_DESC").alias("MED_HOME_LOC_DESC"),
    col("PROV_SK").alias("PROV_SK"),
    col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    lit("240").alias("ATTRBTN_BCBS_PLN_CD_SRC_CD")
)

df_step5_transform = df_step5_input.select(
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    lit("240").alias("ATTRBTN_BCBS_PLN_CD"),
    lit(FirstDayNextMonth).alias("ROW_EFF_DT_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

params = {
    "CurrRunCycle": RunCycle,
    "SrcSysCd": SrcSysCd,
    "$IDSOwner": IDSOwner
}

df_mbrpcpattrbtnpk = MbrPcpAttrbtnPK(df_step5_allcol, df_step5_transform, params)

df_mbrPCPExtr = df_mbrpcpattrbtnpk.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_PCP_ATTRBTN_SK",
    "MBR_UNIQ_KEY",
    "ATTRBTN_BCBS_PLN_CD",
    "ROW_EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_SK",
    "PROV_SK",
    "REL_GRP_PROV_SK",
    "COB_IN",
    "LAST_EVAL_AND_MNG_SVC_DT_SK",
    "INDV_BE_KEY",
    "MBR_PCP_MO_NO",
    "MED_HOME_ID",
    "MED_HOME_DESC",
    "MED_HOME_GRP_ID",
    "MED_HOME_GRP_DESC",
    "MED_HOME_LOC_ID",
    "MED_HOME_LOC_DESC",
    "ATTRBTN_BCBS_PLN_CD_SRC_CD"
)

df_mbrPCPExtr = df_mbrPCPExtr.withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
df_mbrPCPExtr = df_mbrPCPExtr.withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
df_mbrPCPExtr = df_mbrPCPExtr.withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
df_mbrPCPExtr = df_mbrPCPExtr.withColumn("ROW_EFF_DT_SK", rpad(col("ROW_EFF_DT_SK"), 10, " "))
df_mbrPCPExtr = df_mbrPCPExtr.withColumn("COB_IN", rpad(col("COB_IN"), 1, " "))
df_mbrPCPExtr = df_mbrPCPExtr.withColumn("LAST_EVAL_AND_MNG_SVC_DT_SK", rpad(col("LAST_EVAL_AND_MNG_SVC_DT_SK"), 10, " "))

write_files(
    df_mbrPCPExtr,
    f"{adls_path}/key/BlueKCMbrAttrbtnExtr.IdsMbrPCPAttrbtn.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)