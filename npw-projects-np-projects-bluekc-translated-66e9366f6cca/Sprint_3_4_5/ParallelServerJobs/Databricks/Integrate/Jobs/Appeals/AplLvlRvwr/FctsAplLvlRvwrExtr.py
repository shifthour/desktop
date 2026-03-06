# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  FctsAplLvlRvwrExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_UMUM_UTIL_MGT and CMC_GRGR_GROUP for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the CUST_SVC_TASK table.
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Naren Garapaty      		08/01/2007                    			Initial program                                                                                      	devlIDS30                     Steph Goddard          	8/23/07   
# MAGIC Bhoomi Dasari        		10/18/2007                 	3028                   	Added Balancing Snapshot                                         		devlIDS30                     Steph Goddard          	10/18/2007
# MAGIC Jag Yelavarthi        		2015-01-19                  	TFS#8431		Changed "IDS_SK" to "Table_Name_SK"                       		IntegrateNewDevl  	Kalyan Neelam           	2015-01-27
# MAGIC Manasa Andru       		2016-03-16                	5391                	Updated the field names from APRA_EFF_DT and       	IntegrateDev1         	Kalyan Neelam           2016-03-17
# MAGIC                                                              					APRA_TERM_DT to APRA_EFF_DTM and APRA_TERM_DTM
# MAGIC Prabhu ES             		2022-02-25                	S2S		MSSQL connection parameters added                         		IntegrateDev5	Ken Bradmon	2022-05-17

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets APL_LVL_RVWR data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC MCRE_ID must not be upcased.
# MAGIC This field is the primary key for CMC_MCRE_RELAT_ENT and there are multiple rows where the only difference in the key is the case ( i.e. "PAM" and "pam")
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, when, length, lit, rpad, date_format, upper
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunDate = get_widget_value('RunDate','')
RunID = get_widget_value('RunID','')

# Prepare JDBC connection for Facets
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# Read from FACETS (ODBCConnector) - Extract pin
extract_query_FACETS_1 = f"""
SELECT 
APRA.APAP_ID,
APRA.APLV_SEQ_NO,
APRA.MCRE_ID,
APRA.APRA_EFF_DTM,
APRA.APRA_TERM_DTM,
APRA.APRA_MCTR_TRSN,
APRA.APRA_LAST_UPD_DTM,
APRA.APRA_LAST_UPD_USID
FROM {FacetsOwner}.CMC_APRA_RP_ASSIGN APRA,
     {FacetsOwner}.CMC_APAP_APPEALS APAP,
     {FacetsOwner}.CMC_APLV_APP_LEVEL APLV
WHERE APAP.APAP_ID = APLV.APAP_ID
  AND APLV.APAP_ID=APRA.APAP_ID
  AND APLV.APLV_SEQ_NO=APRA.APLV_SEQ_NO
  AND (
       ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
       OR
       ( APLV.APLV_LAST_UPD_DTM >= '{BeginDate}' AND APLV.APLV_LAST_UPD_DTM <= '{EndDate}' )
      )
"""
df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FACETS_1)
    .load()
)

# Read from FACETS (ODBCConnector) - McreName pin
extract_query_FACETS_2 = f"""
SELECT 
ENT.MCRE_ID,
ENT.MCRE_NAME
FROM {FacetsOwner}.CMC_MCRE_RELAT_ENT ENT
"""
df_FACETS_McreName = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FACETS_2)
    .load()
)

# Scenario A: hf_apl_lvl_rvwr_mcre_name is an intermediate hashed file => deduplicate on key columns (MCRE_ID) and pass through
df_FACETS_McreName = dedup_sort(
    df_FACETS_McreName,
    ["MCRE_ID"],
    []
)

# "Strip" stage: Left join df_FACETS_Extract (primary) with df_FACETS_McreName (lookup) on MCRE_ID
df_Strip_joined = df_FACETS_Extract.alias("Extract").join(
    df_FACETS_McreName.alias("NameLkup"),
    on=[col("Extract.MCRE_ID") == col("NameLkup.MCRE_ID")],
    how="left"
)

df_Strip = df_Strip_joined.select(
    rpad(strip_field(col("Extract.APAP_ID")), 12, ' ').alias("APAP_ID"),
    col("Extract.APLV_SEQ_NO").alias("APLV_SEQ_NO"),
    rpad(strip_field(col("Extract.MCRE_ID")), 9, ' ').alias("MCRE_ID"),
    date_format(col("Extract.APRA_EFF_DTM"), "yyyy-MM-dd").alias("APRA_EFF_DT"),
    date_format(col("Extract.APRA_TERM_DTM"), "yyyy-MM-dd").alias("APRA_TERM_DT"),
    rpad(strip_field(col("Extract.APRA_MCTR_TRSN")), 4, ' ').alias("APRA_MCTR_TRSN"),
    date_format(col("Extract.APRA_LAST_UPD_DTM"), "yyyy-MM-dd HH:mm:ss.SSS").alias("APRA_LAST_UPD_DTM"),
    rpad(strip_field(col("Extract.APRA_LAST_UPD_USID")), 10, ' ').alias("APRA_LAST_UPD_USID"),
    when(
       col("NameLkup.MCRE_NAME").isNull() | (length(col("NameLkup.MCRE_NAME")) == 0),
       rpad(lit(" "), 50, ' ')
    ).otherwise(
       rpad(strip_field(col("NameLkup.MCRE_NAME")), 50, ' ')
    ).alias("MCRE_NAME")
)

# "BusinessRules" stage
df_BusinessRules = df_Strip.select(
    # Stage variables become columns where needed
    # Output columns
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, ' ').alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, ' ').alias("DISCARD_IN"),
    rpad(lit("Y"), 1, ' ').alias("PASS_THRU_IN"),
    col("APRA_EFF_DT").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    rpad(lit("FACETS"), 6, ' ').alias("SRC_SYS_CD"),
    (
      rpad(lit("FACETS"), 6, ' ') + lit(";") +
      rpad(trim(col("APAP_ID")), 12, ' ') + lit(";") +
      col("APLV_SEQ_NO").cast("string") + lit(";") +
      rpad(trim(col("MCRE_ID")), 9, ' ') + lit(";") +
      col("APRA_EFF_DT")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("APL_LVL_RVWR_SK"),         # placeholder until we fix with surrogate
    trim(col("APAP_ID")).alias("APL_ID"),
    col("APLV_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    trim(col("MCRE_ID")).alias("APL_RVWR_ID"),
    rpad(col("APRA_EFF_DT"), 10, ' ').alias("EFF_DT"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(col("APAP_ID")).alias("APL_LVL"),
    trim(col("MCRE_ID")).alias("APL_RVWR"),
    trim(col("APRA_LAST_UPD_USID")).alias("LAST_UPDT_USER"),
    when(
      col("APRA_MCTR_TRSN").isNull() | (length(col("APRA_MCTR_TRSN")) == 0),
      lit("NA")
    ).otherwise(trim(col("APRA_MCTR_TRSN"))).alias("APL_REP_LVL_RVWR_TERM_CD"),
    col("APRA_LAST_UPD_DTM").alias("LAST_UPDT_DTM"),
    rpad(col("APRA_TERM_DT"), 10, ' ').alias("TERM_DT"),
    rpad(trim(col("MCRE_NAME")), 75, ' ').alias("APL_RVWR_NM")
)

# Scenario B: hf_cust_apl_lvl_rvwr read-modify-write
# 1) Read dummy table for "hf_cust_apl_lvl_rvwr"
ids_secret_name = get_widget_value('ids_secret_name','')
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_hf_cust_apl_lvl_rvwr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, APL_ID, APL_LVL_SEQ_NO, APL_RVWR_ID, EFF_DT, CRT_RUN_CYC_EXCTN_SK, APL_LVL_RVWR_SK FROM IDS.dummy_hf_cust_apl_lvl_rvwr")
    .load()
)

# "PrimaryKey" stage: join (left) with dummy table
df_enriched = df_BusinessRules.alias("Transform").join(
    df_hf_cust_apl_lvl_rvwr.alias("lkup"),
    on=[
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.APL_ID") == col("lkup.APL_ID"),
        col("Transform.APL_LVL_SEQ_NO") == col("lkup.APL_LVL_SEQ_NO"),
        col("Transform.APL_RVWR_ID") == col("lkup.APL_RVWR_ID"),
        col("Transform.EFF_DT") == col("lkup.EFF_DT")
    ],
    how="left"
).select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN"),
    col("Transform.PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT"),
    col("Transform.ERR_CT"),
    col("Transform.RECYCLE_CT"),
    col("Transform.SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING"),
    col("lkup.APL_LVL_RVWR_SK").alias("SK"),
    col("Transform.APL_ID"),
    col("Transform.APL_LVL_SEQ_NO"),
    col("Transform.APL_RVWR_ID"),
    col("Transform.EFF_DT"),
    col("Transform.CRT_RUN_CYC_EXCTN_SK"),
    col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.APL_LVL"),
    col("Transform.APL_RVWR"),
    col("Transform.LAST_UPDT_USER"),
    col("Transform.APL_REP_LVL_RVWR_TERM_CD"),
    col("Transform.LAST_UPDT_DTM"),
    col("Transform.TERM_DT"),
    col("Transform.APL_RVWR_NM"),
    col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
)

# Replace KeyMgtGetNextValueConcurrent with SurrogateKeyGen: fill SK if null
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# Now produce the two outputs from "PrimaryKey": updt (constraint: IsNull(lkup.APL_LVL_RVWR_SK)) and Key (all rows)
df_updt = df_enriched.filter(col("SK").isNotNull() & col("lkup_CRT_RUN_CYC_EXCTN_SK").isNull())
df_Key = df_enriched

# "hf_cust_apl_lvl_rvwr_updt" => Merge new records into dummy_hf_cust_apl_lvl_rvwr
# Prepare temporary table
temp_table_updt = "STAGING.FctsAplLvlRvwrExtr_hf_cust_apl_lvl_rvwr_updt_temp"
df_updt_write = df_updt.select(
    col("SRC_SYS_CD"),
    col("APL_ID"),
    col("APL_LVL_SEQ_NO"),
    col("APL_RVWR_ID"),
    col("EFF_DT"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("APL_LVL_RVWR_SK")
)
execute_dml(f"DROP TABLE IF EXISTS {temp_table_updt}", jdbc_url_ids, jdbc_props_ids)
df_updt_write.write.jdbc(jdbc_url_ids, temp_table_updt, mode="overwrite", properties=jdbc_props_ids)

merge_sql_updt = f"""
MERGE IDS.dummy_hf_cust_apl_lvl_rvwr AS T
USING {temp_table_updt} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD AND
    T.APL_ID = S.APL_ID AND
    T.APL_LVL_SEQ_NO = S.APL_LVL_SEQ_NO AND
    T.APL_RVWR_ID = S.APL_RVWR_ID AND
    T.EFF_DT = S.EFF_DT
WHEN MATCHED THEN
    UPDATE SET T.SRC_SYS_CD = T.SRC_SYS_CD
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, APL_ID, APL_LVL_SEQ_NO, APL_RVWR_ID, EFF_DT, CRT_RUN_CYC_EXCTN_SK, APL_LVL_RVWR_SK)
    VALUES (S.SRC_SYS_CD, S.APL_ID, S.APL_LVL_SEQ_NO, S.APL_RVWR_ID, S.EFF_DT, S.CRT_RUN_CYC_EXCTN_SK, S.APL_LVL_RVWR_SK);
"""
execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# "IdsAplLvlRvwrExtr" => write file
df_IdsAplLvlRvwrExtr = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, ' ').alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, ' ').alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, ' ').alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("SK").alias("APL_LVL_RVWR_SK"),
    col("APL_ID"),
    col("APL_LVL_SEQ_NO"),
    col("APL_RVWR_ID"),
    rpad(col("EFF_DT"), 10, ' ').alias("EFF_DT"),
    when(
      col("lkup_CRT_RUN_CYC_EXCTN_SK").isNull(),
      lit(CurrRunCycle)
    ).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_LVL"),
    col("APL_RVWR"),
    col("LAST_UPDT_USER"),
    col("APL_REP_LVL_RVWR_TERM_CD"),
    col("LAST_UPDT_DTM"),
    rpad(col("TERM_DT"), 10, ' ').alias("TERM_DT"),
    upper(col("APL_RVWR_NM")).alias("APL_RVWR_NM")
)

write_files(
    df_IdsAplLvlRvwrExtr,
    f"{adls_path}/key/FctsAplLvlRvwrExtr.AplLvlRvwr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# SnapShot_FACETS - ODBCConnector
extract_query_SnapShot_1 = f"""
SELECT 
APRA.APAP_ID,
APRA.APLV_SEQ_NO,
APRA.MCRE_ID,
APRA.APRA_EFF_DTM,
APRA.APRA_TERM_DTM,
APRA.APRA_LAST_UPD_DTM
FROM {FacetsOwner}.CMC_APRA_RP_ASSIGN APRA,
     {FacetsOwner}.CMC_APAP_APPEALS APAP,
     {FacetsOwner}.CMC_APLV_APP_LEVEL APLV
WHERE APAP.APAP_ID = APLV.APAP_ID
  AND APLV.APAP_ID=APRA.APAP_ID
  AND APLV.APLV_SEQ_NO=APRA.APLV_SEQ_NO
  AND (
       ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
       OR
       ( APLV.APLV_LAST_UPD_DTM >= '{BeginDate}' AND APLV.APLV_LAST_UPD_DTM <= '{EndDate}' )
      )
"""
df_SnapShot_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SnapShot_1)
    .load()
)

extract_query_SnapShot_2 = f"""
SELECT 
ENT.MCRE_ID,
ENT.MCRE_NAME
FROM {FacetsOwner}.CMC_MCRE_RELAT_ENT ENT
"""
df_SnapShot_FACETS_McreName = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_SnapShot_2)
    .load()
)

# Scenario A again for hf_aplbal_lvl_rvwr_mcre_name (intermediate hashed file)
df_SnapShot_FACETS_McreName = dedup_sort(
    df_SnapShot_FACETS_McreName,
    ["MCRE_ID"],
    []
)

# "Rules" stage: left join
df_Rules_joined = df_SnapShot_FACETS_Extract.alias("Extract").join(
    df_SnapShot_FACETS_McreName.alias("NameLkup"),
    on=[col("Extract.MCRE_ID") == col("NameLkup.MCRE_ID")],
    how="left"
)

df_Rules = df_Rules_joined.select(
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X").alias("SRC_SYS_CD_SK"),
    trim(strip_field(col("Extract.APAP_ID"))).alias("APL_ID"),
    col("Extract.APLV_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    trim(strip_field(col("Extract.MCRE_ID"))).alias("APL_RVWR_ID"),
    rpad(date_format(col("Extract.APRA_EFF_DTM"), "yyyy-MM-dd"), 10, ' ').alias("EFF_DT_SK"),
    when(
       col("NameLkup.MCRE_NAME").isNull() | (length(col("NameLkup.MCRE_NAME")) == 0),
       upper(rpad(lit(" "), 1, ' '))
    ).otherwise(
       upper(trim(strip_field(col("NameLkup.MCRE_NAME"))))
    ).alias("APL_RVWR_NM"),
    rpad(date_format(col("Extract.APRA_LAST_UPD_DTM"), "yyyy-MM-dd"), 10, ' ').alias("LAST_UPDT_DT_SK"),
    rpad(date_format(col("Extract.APRA_TERM_DTM"), "yyyy-MM-dd"), 10, ' ').alias("TERM_DT_SK")
)

# "B_APL_LVL_RVWR" => write file
write_files(
    df_Rules,
    f"{adls_path}/load/B_APL_LVL_RVWR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)