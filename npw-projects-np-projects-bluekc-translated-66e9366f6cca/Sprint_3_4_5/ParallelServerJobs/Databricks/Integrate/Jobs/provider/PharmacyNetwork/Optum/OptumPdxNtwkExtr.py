# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2019 - 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 4;hf_rm_dup_esi_pdx_ntwk;hf_rm_dup_esi_bal_pdx_ntwk;hf_pdx_ntwk_prev_esi_lkup;hf_pdx_ntwk_prov_addr_cnty
# MAGIC 
# MAGIC CALLED BY: OptumProvExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Optum PBM.PROVNTWK file to load Provider 
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram        11/08/2019      6131 PBM Replacement               Initial Programming                       IntegrateDevl                 Kalyan Neelam          2019-11-20
# MAGIC 
# MAGIC Velmani K                04/09/2020      6264-PBM Phase II -                     Added ALWS_90_DAY_RX_IN   IntegrateDev5               Kalyan Neelam           2020-09-18
# MAGIC                                                          Government Programs                  column as part of pahse 2 changes

# MAGIC This container is used in:
# MAGIC NABPPdxNtwkExtr
# MAGIC             OptumPdxNtwkExtr                                        
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Zero Fill Ntwk Code
# MAGIC Apply business logic
# MAGIC Zero Fill Ntwk Code
# MAGIC Read Optum Pharmacy Data
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/PdxNtwkPkey
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, upper, when, length, concat, to_date, date_format, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','NABP')
SrcSysCdSk = get_widget_value('SrcSysCdSk','16754')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
RunID = get_widget_value('RunID','')
InFile = get_widget_value('InFile','')

# UWS (CODBCStage) - reading entire #$UWSOwner#.PDX_DIR_EXCL without parameterized WHERE
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
extract_query_uws = f"SELECT PROV_ID, USER_ID, LAST_UPDT_DT_SK FROM {UWSOwner}.PDX_DIR_EXCL"
df_UWS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_uws)
    .load()
)

# hf_pdx_dir_excd_list (CHashedFileStage) - Scenario C => write to parquet, then read back
write_files(
    df_UWS.select(
        col("PROV_ID"),
        col("USER_ID"),
        col("LAST_UPDT_DT_SK")
    ),
    "hf_pdx_dir_excd_list.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_pdx_dir_excd_list = spark.read.parquet("hf_pdx_dir_excd_list.parquet")

# PullOptum_Data (CSeqFileStage) - read from landing
schema_pulloptum_data = StructType([
    StructField("NABP", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("ADDR", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("CNTY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP_CD", StringType(), True),
    StructField("PHN_NO", StringType(), True),
    StructField("FAX_NO", StringType(), True),
    StructField("HR_IN_24", StringType(), True),
    StructField("EFF_DATE", StringType(), True),
    StructField("PRFRD_IN", StringType(), True),
    StructField("NETWORK_ID", StringType(), True)
])
df_PullOptum_Data = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_pulloptum_data)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# StripField (CTransformerStage)
df_StripField_pre = df_PullOptum_Data.select(
    col("NABP").alias("NABP_in"),
    col("NETWORK_ID").alias("NETWORK_ID_in"),
    col("EFF_DATE").alias("EFF_DATE_in"),
    col("CNTY").alias("CNTY_in"),
    col("ADDR").alias("ADDR_in"),
    col("PRFRD_IN").alias("PRFRD_IN_in")
)
df_StripField = df_StripField_pre.select(
    strip_field(col("NABP_in")).alias("NABP_NO"),
    col("NETWORK_ID_in").alias("NTWK"),
    strip_field(col("EFF_DATE_in")).alias("EFF_DT"),
    upper(trim(strip_field(col("CNTY_in")))).cast("string").alias("c1"),
    trim(strip_field(col("ADDR_in"))).cast("string").alias("c2"),
    upper(trim(col("PRFRD_IN_in"))).alias("PRFRD_IN")
).select(
    col("NABP_NO"),
    col("NTWK"),
    col("EFF_DT"),
    concat(col("c1"), col("c2")).alias("CNTY_ST_CNCT"),
    col("PRFRD_IN")
)

# hf_rm_dup_optum_pdx_ntwk (CHashedFileStage) - Scenario A => deduplicate on [NABP_NO, NTWK]
df_hf_rm_dup_optum_pdx_ntwk = dedup_sort(
    df_StripField,
    partition_cols=["NABP_NO","NTWK"],
    sort_cols=[]
)

# BusinessRules (CTransformerStage) with left join on df_hf_pdx_dir_excd_list
df_BusinessRules_joined = df_hf_rm_dup_optum_pdx_ntwk.alias("Trans").join(
    df_hf_pdx_dir_excd_list.alias("lnkDirExcdListOut"),
    (col("Trans.NABP_NO") == col("lnkDirExcdListOut.PROV_ID")),
    how="left"
)

# Stage Variables
df_BusinessRules_sv = df_BusinessRules_joined.select(
    col("Trans.*"),
    col("lnkDirExcdListOut.PROV_ID").alias("Excl_PROV_ID")
).withColumn(
    "ForeverTermDate",
    lit("9999-12-31")
)

# The output columns
df_BusinessRules = df_BusinessRules_sv.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    col("CurrRunCycleDate").alias("FIRST_RECYC_DT"),  
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    concat(col("SrcSysCd"), lit(";"), col("NABP_NO"), lit(";"), col("NTWK")).alias("PRI_KEY_STRING"),
    lit(0).alias("PDX_NTWK_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(col("NABP_NO")).alias("PROV_ID"),
    when((col("NTWK").isNull()) | (length(trim(col("NTWK"))) == 0), lit("UNK")).otherwise(trim(col("NTWK"))).alias("PDX_NTWK_CD"),
    when(col("Excl_PROV_ID").isNull(), rpad(lit("Y"),1," ")).otherwise(rpad(lit("N"),1," ")).alias("DIR_IN"),
    col("EFF_DT"),
    col("ForeverTermDate").alias("TERM_DT"),
    when(col("PRFRD_IN") == lit("X"), lit("Y")).otherwise(lit("N")).alias("PDX_NTWK_PRFRD_PROV_IN"),
    rpad(lit("N"),1," ").alias("ALWS_90_DAY_RX_IN")
)

# PdxNtwkPkey (CContainerStage)
params_PdxNtwkPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_pdxNtwkPkey = PdxNtwkPkey(df_BusinessRules, params_PdxNtwkPkey)

# OptumPdxNtwkExtr (CSeqFileStage) - write final file
df_OptumPdxNtwkExtr = df_pdxNtwkPkey.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("PDX_NTWK_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROV_ID"),
    col("PDX_NTWK_CD"),
    rpad(col("DIR_IN"),1," ").alias("DIR_IN"),
    rpad(col("EFF_DT"),10," ").alias("EFF_DT"),
    rpad(col("TERM_DT"),10," ").alias("TERM_DT"),
    rpad(col("PDX_NTWK_PRFRD_PROV_IN"),1," ").alias("PDX_NTWK_PRFRD_PROV_IN"),
    rpad(col("ALWS_90_DAY_RX_IN"),1," ").alias("ALWS_90_DAY_RX_IN")
)
write_files(
    df_OptumPdxNtwkExtr,
    f"{adls_path}/key/OptumPdxNtwkExtr.PdxNtwk.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# OptumData_Src (CSeqFileStage)
schema_OptumData_Src = StructType([
    StructField("NABP", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("ADDR", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("CNTY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP_CD", StringType(), True),
    StructField("PHN_NO", StringType(), True),
    StructField("FAX_NO", StringType(), True),
    StructField("HR_IN_24", StringType(), True),
    StructField("EFF_DATE", StringType(), True),
    StructField("PRFRD_IN", StringType(), True),
    StructField("NETWORK_ID", StringType(), True)
])
df_OptumData_Src = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_OptumData_Src)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Transform (CTransformerStage)
df_Transform_pre = df_OptumData_Src.select(
    col("NABP").alias("NABP_in"),
    col("NETWORK_ID").alias("NETWORK_ID_in")
)
df_Transform = df_Transform_pre.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("NABP_in").alias("PROV_ID"),
    # svPdxNtwkCdSk => user-defined function call
    # "GetFkeyCodes("OPTUMRX", 1, "PHARMACY NETWORK", svNtwk, "X")" replaced with a single column
    # storing expression result in "PDX_NTWK_CD_SK"
    lit(-1).alias("PDX_NTWK_CD_SK")
)

# hf_rm_dup_optum_bal_pdx_ntwk (CHashedFileStage) - Scenario A => deduplicate on [SRC_SYS_CD_SK, PROV_ID, PDX_NTWK_CD_SK]
df_hf_rm_dup_optum_bal_pdx_ntwk = dedup_sort(
    df_Transform,
    partition_cols=["SRC_SYS_CD_SK","PROV_ID","PDX_NTWK_CD_SK"],
    sort_cols=[]
)

# Snapshot_File (CSeqFileStage)
df_Snapshot_File = df_hf_rm_dup_optum_bal_pdx_ntwk.select(
    col("SRC_SYS_CD_SK").cast("int"),
    col("PROV_ID").cast("string"),
    col("PDX_NTWK_CD_SK").cast("int")
)
write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_PDX_NTWK.OPTUM.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)