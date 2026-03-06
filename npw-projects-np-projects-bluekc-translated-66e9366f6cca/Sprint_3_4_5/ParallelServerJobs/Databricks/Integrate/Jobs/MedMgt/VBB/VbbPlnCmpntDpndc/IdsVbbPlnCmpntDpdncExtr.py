# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Extract job for IDS table VBB_PLN_CMPNT_DPNDC
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                           Date                Project/Altiris #           Change Description                                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                     --------------------     ------------------------             -----------------------------------------------------------------------                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Karthik Chintalapani       2013-05-13          4963 VBB Phase III      Initial Programming                                                                 IntegrateNewDevl          Bhoomi Dasari            5/16/2013
# MAGIC Raja Gummadi               2013-10-16          4963 VBB                     Added snapshot file to the job                                                IntegrateNewDevl           Kalyan Neelam           2013-10-25

# MAGIC IDS VBB_PLN_CMPNT_DPNDC Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/VbbPlnCmpntDpndcPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
VBBOwner = get_widget_value('VBBOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

ids_secret_name = get_widget_value('ids_secret_name','')
vbb_secret_name = get_widget_value('vbb_secret_name','')

jdbc_url, jdbc_props = get_db_config(vbb_secret_name)
extract_query_IHMFCONSTITUENT = (
    f"SELECT TZIM_HIIP_DEPENDENCY.HIPL_ID, "
    f"TZIM_HIIP_DEPENDENCY.INPR_ID, "
    f"TZIM_HIIP_DEPENDENCY.INPR_ID_DEP, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_WAIT_PERIOD, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_CONDITION_IND, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_CREATE_DT, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_CREATE_APP_USER, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_UPDATE_DT, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_UPDATE_DB_USER, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_UPDATE_APP_USER, "
    f"TZIM_HIIP_DEPENDENCY.HIIP_UPDATE_SEQ "
    f"FROM {VBBOwner}.TZIM_HIIP_DEPENDENCY TZIM_HIIP_DEPENDENCY, "
    f"{VBBOwner}.TZIM_HIPL_HEALTH_PLAN TZIM_HIPL_HEALTH_PLAN "
    f"WHERE TZIM_HIIP_DEPENDENCY.HIPL_ID = TZIM_HIPL_HEALTH_PLAN.HIPL_ID "
    f"AND TZIM_HIPL_HEALTH_PLAN.HIPL_ACTIVATED_IND in ('1', '2') "
    f"AND TZIM_HIIP_DEPENDENCY.HIIP_UPDATE_DT > '{LastRunDtm}'"
)
df_IHMFCONSTITUENT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IHMFCONSTITUENT)
    .load()
)

df_trim_stagevars = (
    df_IHMFCONSTITUENT
    .withColumn("_HIPL_ID_stripped", trim(strip_field(col("HIPL_ID"))))
    .withColumn("_INPR_ID_stripped", trim(strip_field(col("INPR_ID"))))
    .withColumn("_INPR_ID_DEP_stripped", trim(strip_field(col("INPR_ID_DEP"))))
    .withColumn("svHiplId", when((col("_HIPL_ID_stripped").isNull()) | (length(col("_HIPL_ID_stripped")) == 0), 'N').otherwise('Y'))
    .withColumn("svInprId", when((col("_INPR_ID_stripped").isNull()) | (length(col("_INPR_ID_stripped")) == 0), 'N').otherwise('Y'))
    .withColumn("svInprIdDep", when((col("_INPR_ID_DEP_stripped").isNull()) | (length(col("_INPR_ID_DEP_stripped")) == 0), 'N').otherwise('Y'))
    .withColumn("svGoodRecord", when((col("svHiplId")=='Y') & (col("svInprId")=='Y') & (col("svInprIdDep")=='Y'), 'Y').otherwise('N'))
)

df_PlnCmpntDpndcData = df_trim_stagevars.filter(col("svGoodRecord")=='Y').select(
    trim(strip_field(col("HIPL_ID"))).alias("HIPL_ID"),
    trim(strip_field(col("INPR_ID"))).alias("INPR_ID"),
    trim(strip_field(col("INPR_ID_DEP"))).alias("INPR_ID_DEP"),
    trim(strip_field(col("HIIP_WAIT_PERIOD"))).alias("HIIP_WAIT_PERIOD"),
    trim(strip_field(col("HIIP_CONDITION_IND"))).alias("HIIP_CONDITION_IND"),
    trim(strip_field(col("HIIP_CREATE_DT"))).alias("HIIP_CREATE_DT"),
    UpCase(trim(strip_field(col("HIIP_CREATE_APP_USER")))).alias("HIIP_CREATE_APP_USER"),
    trim(strip_field(col("HIIP_UPDATE_DT"))).alias("HIIP_UPDATE_DT"),
    UpCase(trim(strip_field(col("HIIP_UPDATE_DB_USER")))).alias("HIIP_UPDATE_DB_USER"),
    UpCase(trim(strip_field(col("HIIP_UPDATE_APP_USER")))).alias("HIIP_UPDATE_APP_USER"),
    trim(strip_field(col("HIIP_UPDATE_SEQ"))).alias("HIIP_UPDATE_SEQ")
)

df_Rejects = df_trim_stagevars.filter(col("svGoodRecord")=='N').select(
    col("HIPL_ID"),
    col("INPR_ID"),
    col("INPR_ID_DEP"),
    col("HIIP_WAIT_PERIOD"),
    col("HIIP_CONDITION_IND"),
    col("HIIP_CREATE_DT"),
    col("HIIP_CREATE_APP_USER"),
    col("HIIP_UPDATE_DT"),
    col("HIIP_UPDATE_DB_USER"),
    col("HIIP_UPDATE_APP_USER"),
    col("HIIP_UPDATE_SEQ")
)

df_Rejects_s = df_Rejects.select(
    "HIPL_ID",
    "INPR_ID",
    "INPR_ID_DEP",
    "HIIP_WAIT_PERIOD",
    "HIIP_CONDITION_IND",
    "HIIP_CREATE_DT",
    "HIIP_CREATE_APP_USER",
    "HIIP_UPDATE_DT",
    "HIIP_UPDATE_DB_USER",
    "HIIP_UPDATE_APP_USER",
    "HIIP_UPDATE_SEQ"
)
write_files(
    df_Rejects_s,
    f"{adls_path_publish}/external/VBB_PLN_CMPNT_DPNDC_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_businessrules_base = (
    df_PlnCmpntDpndcData
    .withColumn("svHiipCreateDt", when((col("HIIP_CREATE_DT").isNull()) | (length(col("HIIP_CREATE_DT")) == 0), '1753-01-01 00:00:00').otherwise(col("HIIP_CREATE_DT")))
    .withColumn("svHiipUpdateDt", when((col("HIIP_UPDATE_DT").isNull()) | (length(col("HIIP_UPDATE_DT")) == 0), '1753-01-01 00:00:00').otherwise(col("HIIP_UPDATE_DT")))
)

df_AllCol = df_businessrules_base.select(
    col("HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    col("INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    col("INPR_ID_DEP").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit('I').alias("INSRT_UPDT_CD"),
    lit('N').alias("DISCARD_IN"),
    lit('Y').alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD_1"),
    (trim(col("HIPL_ID")) + lit(";") + trim(col("INPR_ID")) + lit(";") + lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    lit(0).alias("VBB_PLN_CMPNT_DPNDC_SK"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INPR_ID_DEP").alias("INPR_ID_DEP"),
    col("INPR_ID").alias("INPR_ID"),
    col("HIPL_ID").alias("HIPL_ID"),
    when(trim(col("HIIP_CONDITION_IND"))==lit('0'), lit('Y')).otherwise(lit('N')).alias("MUTLLY_XCLSVE_DPNDC_IN"),
    col("svHiipCreateDt").alias("SRC_SYS_CRT_DTM"),
    col("svHiipUpdateDt").alias("SRC_SYS_UPDT_DTM"),
    when((col("HIIP_WAIT_PERIOD").isNull()) | (length(trim(col("HIIP_WAIT_PERIOD"))) == 0), lit(0)).otherwise(col("HIIP_WAIT_PERIOD")).alias("WAIT_PERD_DAYS_NO")
)

df_Transform = df_businessrules_base.select(
    col("HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    col("INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    col("INPR_ID_DEP").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Snapshot = df_businessrules_base.select(
    col("HIPL_ID").alias("VBB_PLN_UNIQ_KEY"),
    col("INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    col("INPR_ID_DEP").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Snapshot_s = df_Snapshot.select(
    "VBB_PLN_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "RQRD_VBB_CMPNT_UNIQ_KEY",
    "SRC_SYS_CD_SK"
)
write_files(
    df_Snapshot_s,
    f"{adls_path}/load/B_VBB_PLN_CMPNT_DPNDC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

params_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_Key = VbbPlnCmpntDpndcPK(df_Transform, df_AllCol, params_container)

df_TGT_repad = (
    df_Key
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("MUTLLY_XCLSVE_DPNDC_IN", rpad(col("MUTLLY_XCLSVE_DPNDC_IN"), 1, " "))
)

df_TGT_s = df_TGT_repad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "VBB_PLN_CMPNT_DPNDC_SK",
    "VBB_PLN_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "RQRD_VBB_CMPNT_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INPR_ID_DEP",
    "INPR_ID",
    "HIPL_ID",
    "MUTLLY_XCLSVE_DPNDC_IN",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "WAIT_PERD_DAYS_NO"
)
write_files(
    df_TGT_s,
    f"{adls_path}/key/IdsVbbPlnCmpntDpndc.VbbPlnCmpntDpndc.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)