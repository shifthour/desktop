# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  FctsAplRvwrExtr
# MAGIC DESCRIPTION:   Pulls data from CMC_APRP_REPS for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Used in the foreign key part of the APL_RVWR table.
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/17/2007                    	3028                   	Initial program                                                                                      	devlIDS30                        
# MAGIC Bhoomi Dasari    		10/18/2007                 	3028                   	Added Balancing Snapshot                                        		devlIDS30
# MAGIC Jag Yelavarthi    		2015-01-19              	TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"                		IntegrateNewDevl      	Kalyan Neelam           2015-01-27
# MAGIC Prabhu ES         		2022-02-25      	S2S 		Remediation-MSSQL conn param added            		IntegrateDev5	Ken Bradmon	2022-05-20

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Rvwr Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, length, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all parameter values
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')  # for reading/writing the dummy hashed-file table in IDS

# --------------------------------------------------------------------------------
# STAGE: FACETS (ODBCConnector)
# --------------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_FACETS = (
    f"SELECT REPS.MCRE_ID, REPS.APRP_MCTR_CAT, REPS.APRP_MCTR_SCAT, REPS.APRP_CRED_SUM, "
    f"REPS.APRP_EFF_DT, REPS.APRP_LAST_UPD_DTM, REPS.APRP_LAST_UPD_USID "
    f"FROM {FacetsOwner}.CMC_APRP_REPS REPS "
    f"WHERE REPS.APRP_LAST_UPD_DTM >= '{BeginDate}' "
    f"AND REPS.APRP_LAST_UPD_DTM <= '{EndDate}'"
)
df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FACETS)
    .load()
)

mcre_query_FACETS = (
    f"SELECT REPS.MCRE_ID, ENT.MCRE_NAME "
    f"FROM {FacetsOwner}.CMC_APRP_REPS REPS, {FacetsOwner}.CMC_MCRE_RELAT_ENT ENT "
    f"WHERE REPS.MCRE_ID = ENT.MCRE_ID"
)
df_FACETS_McreId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", mcre_query_FACETS)
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_mcre_lkup (CHashedFileStage) - SCENARIO A (intermediate hashed file)
#          We replace this hashed file with a deduplicate + direct usage
# --------------------------------------------------------------------------------
# Key columns = [MCRE_ID]
df_FACETS_McreId_dedup = dedup_sort(df_FACETS_McreId, ["MCRE_ID"], [("MCRE_ID", "A")])

# --------------------------------------------------------------------------------
# STAGE: Strip (CTransformerStage)
#   PrimaryLink: df_FACETS_Extract alias -> "Extract"
#   LookupLink: df_FACETS_McreId_dedup alias -> "Mcrelkup", left join on MCRE_ID
#   Output columns with transformations
# --------------------------------------------------------------------------------
df_strip_join = df_FACETS_Extract.alias("Extract").join(
    df_FACETS_McreId_dedup.alias("Mcrelkup"),
    on=[col("Extract.MCRE_ID") == col("Mcrelkup.MCRE_ID")],
    how="left"
)

df_strip = df_strip_join.select(
    # MCRE_ID
    strip_field(F.col("Extract.MCRE_ID")).alias("MCRE_ID"),
    # APRP_MCTR_CAT
    strip_field(F.col("Extract.APRP_MCTR_CAT")).alias("APRP_MCTR_CAT"),
    # APRP_MCTR_SCAT
    strip_field(F.col("Extract.APRP_MCTR_SCAT")).alias("APRP_MCTR_SCAT"),
    # APRP_CRED_SUM
    strip_field(F.col("Extract.APRP_CRED_SUM")).alias("APRP_CRED_SUM"),
    # APRP_EFF_DT
    # Interpreting FORMAT.DATE(...) as a user-defined function call
    F.expr('FORMAT_DATE(Extract.APRP_EFF_DT, "SYBASE", "TIMESTAMP", "CCYY-MM-DD")').alias("APRP_EFF_DT"),
    # APRP_LAST_UPD_DTM
    F.expr('FORMAT_DATE(Extract.APRP_LAST_UPD_DTM, "SYBASE", "TIMESTAMP", "DB2timestamp")').alias("APRP_LAST_UPD_DTM"),
    # APRP_LAST_UPD_USID
    strip_field(F.col("Extract.APRP_LAST_UPD_USID")).alias("APRP_LAST_UPD_USID"),
    # MCRE_NAME
    strip_field(F.col("Mcrelkup.MCRE_NAME")).alias("MCRE_NAME"),
)

# --------------------------------------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
#   Input: df_strip (PrimaryLink)
#   Output columns
# --------------------------------------------------------------------------------
df_businessRules = (
    df_strip
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("svSrcSysCd", lit("FACETS"))
    .withColumn("svAplRvwrId", trim(col("MCRE_ID")))
)

df_businessRules_out = df_businessRules.select(
    # JOB_EXCTN_RCRD_ERR_SK => 0
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # INSRT_UPDT_CD => "I"
    lit("I").alias("INSRT_UPDT_CD"),
    # DISCARD_IN => "N"
    lit("N").alias("DISCARD_IN"),
    # PASS_THRU_IN => RowPassThru => "Y"
    col("RowPassThru").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT => RunDate
    lit(RunDate).alias("FIRST_RECYC_DT"),
    # ERR_CT => 0
    lit(0).alias("ERR_CT"),
    # RECYCLE_CT => 0
    lit(0).alias("RECYCLE_CT"),
    # SRC_SYS_CD => svSrcSysCd => "FACETS"
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    # PRI_KEY_STRING => svSrcSysCd : ";" : svAplRvwrId
    F.concat(col("svSrcSysCd"), F.lit(";"), col("svAplRvwrId")).alias("PRI_KEY_STRING"),
    # APL_RVWR_SK => 0
    lit(0).alias("APL_RVWR_SK"),
    # APL_RVWR_ID => UpCase(svAplRvwrId)
    F.expr("UpCase(svAplRvwrId)").alias("APL_RVWR_ID"),
    # CRT_RUN_CYC_EXCTN_SK => 0
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK => 0
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_USER_SK => If Len(Trim(APRP_LAST_UPD_USID))=0 or IsNull => 'UNK' else Trim(APRP_LAST_UPD_USID)
    when(
        (col("APRP_LAST_UPD_USID").isNull()) | (length(trim(col("APRP_LAST_UPD_USID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("APRP_LAST_UPD_USID"))).alias("LAST_UPDT_USER_SK"),
    # APL_RVWR_CAT_CD_SK => if Len(Trim(APRP_MCTR_CAT))=0 or IsNull => 'NA' else Trim(APRP_MCTR_CAT)
    when(
        (col("APRP_MCTR_CAT").isNull()) | (length(trim(col("APRP_MCTR_CAT"))) == 0),
        lit("NA")
    ).otherwise(trim(col("APRP_MCTR_CAT"))).alias("APL_RVWR_CAT_CD_SK"),
    # APL_RVWR_SUBCAT_CD_SK => similar logic
    when(
        (col("APRP_MCTR_SCAT").isNull()) | (length(trim(col("APRP_MCTR_SCAT"))) == 0),
        lit("NA")
    ).otherwise(trim(col("APRP_MCTR_SCAT"))).alias("APL_RVWR_SUBCAT_CD_SK"),
    # EFF_DT_SK => Trim(APRP_EFF_DT)
    trim(col("APRP_EFF_DT")).alias("EFF_DT_SK"),
    # LAST_UPDT_DTM => Trim(APRP_LAST_UPD_DTM)
    trim(col("APRP_LAST_UPD_DTM")).alias("LAST_UPDT_DTM"),
    # APL_RVWR_NM => Trim(UpCase(MCRE_NAME))
    F.expr("UpCase(MCRE_NAME)").alias("APL_RVWR_NM"),
    # CRDTL_SUM_DESC => Trim(UpCase(APRP_CRED_SUM))
    F.expr("UpCase(APRP_CRED_SUM)").alias("CRDTL_SUM_DESC"),
)

# --------------------------------------------------------------------------------
# STAGE: hf_apl_rvwr (CHashedFileStage) READ-MODIFY-WRITE => SCENARIO B
#         Replace with dummy table in IDS: "dummy_hf_apl_rvwr"
#         We read from it and also write updates/inserts back to it
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_apl_rvwr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, APL_RVWR_ID, CRT_RUN_CYC_EXCTN_SK, APL_RVWR_SK FROM IDS.dummy_hf_apl_rvwr")
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: PrimaryKey (CTransformerStage)
#   InputPins:
#       - PrimaryLink => df_businessRules_out  (alias "Transform")
#       - LookupLink  => df_hf_apl_rvwr       (alias "lkup"), left join
#   OutputPins:
#       - updt => constraint => IsNull(lkup.APL_RVWR_SK)=@TRUE => to hf_apl_rvwr_updt
#       - Key  => => IdsAplRvwrExtr
#   We handle the KeyMgtGetNextValueConcurrent with SurrogateKeyGen on APL_RVWR_SK
# --------------------------------------------------------------------------------
df_primaryKey_join = df_businessRules_out.alias("Transform").join(
    df_hf_apl_rvwr.alias("lkup"),
    (col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD")) &
    (col("Transform.APL_RVWR_ID") == col("lkup.APL_RVWR_ID")),
    how="left"
)

# We incorporate the stage variable logic:
#   SK => if isNull(lkup.APL_RVWR_SK) then KeyMgtGetNextValueConcurrent(...) else lkup.APL_RVWR_SK
#   NewCrtRunCycExtcnSk => if isNull(lkup.APL_RVWR_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
# Then we apply SurrogateKeyGen for APL_RVWR_SK
df_enriched = df_primaryKey_join.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN"),
    col("Transform.PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT"),
    col("Transform.ERR_CT"),
    col("Transform.RECYCLE_CT"),
    col("Transform.SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING"),
    col("Transform.APL_RVWR_SK").alias("TEMP_APL_RVWR_SK"),  # placeholder (0 from upstream)
    col("Transform.APL_RVWR_ID"),
    col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("TEMP_CRT_RUN_CYC_EXCTN_SK"),  # placeholder (0 from upstream)
    col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.LAST_UPDT_USER_SK"),
    col("Transform.APL_RVWR_CAT_CD_SK"),
    col("Transform.APL_RVWR_SUBCAT_CD_SK"),
    col("Transform.EFF_DT_SK"),
    col("Transform.LAST_UPDT_DTM"),
    col("Transform.APL_RVWR_NM"),
    col("Transform.CRDTL_SUM_DESC"),
    col("lkup.APL_RVWR_SK").alias("lkup_APL_RVWR_SK"),
    col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
)

df_enriched = (
    df_enriched
    # Decide the final APL_RVWR_SK
    .withColumn(
        "APL_RVWR_SK",
        when(col("lkup_APL_RVWR_SK").isNull(), lit(None)).otherwise(col("lkup_APL_RVWR_SK"))
    )
    # Decide the final CRT_RUN_CYC_EXCTN_SK
    .withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup_APL_RVWR_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
)

# Now call SurrogateKeyGen on APL_RVWR_SK to fill missing values
# Exactly as required: df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"APL_RVWR_SK",<schema>,<secret_name>)

# Assign the final CRT_RUN_CYC_EXCTN_SK
df_enriched = df_enriched.withColumn("CRT_RUN_CYC_EXCTN_SK", col("NewCrtRunCycExtcnSk"))

# LAST_UPDT_RUN_CYC_EXCTN_SK => always CurrRunCycle from the job
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))

# Create the two output links from PrimaryKey:
#   1) df_updt => constraint => IsNull(lkup_APL_RVWR_SK) = @TRUE
#   2) df_key  => all rows, with the final columns
df_updt = df_enriched.filter(col("lkup_APL_RVWR_SK").isNull()).select(
    col("SRC_SYS_CD"),
    col("APL_RVWR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("APL_RVWR_SK"),
)

df_key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("APL_RVWR_SK"),
    col("APL_RVWR_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_USER_SK"),
    col("APL_RVWR_CAT_CD_SK"),
    col("APL_RVWR_SUBCAT_CD_SK"),
    col("EFF_DT_SK"),
    col("LAST_UPDT_DTM"),
    col("APL_RVWR_NM"),
    col("CRDTL_SUM_DESC"),
)

# --------------------------------------------------------------------------------
# STAGE: IdsAplRvwrExtr (CSeqFileStage) - writing file
#   Input: df_key
#   File: key/FctsAplRvwrExtr.AplRvwr.dat.#RunID#
#   Because "key" is not "landing" or "external", use adls_path
# --------------------------------------------------------------------------------
# Apply rpad for char/varchar columns that have known lengths, or <...> for unknown lengths:
df_key_final = (
    df_key
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("APL_RVWR_NM", rpad(col("APL_RVWR_NM"), 50, " "))
    # For all varchar or char with no explicit length, we place <...> to flag manual fix
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), F.lit(<...>), " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), F.lit(<...>), " "))
    .withColumn("APL_RVWR_ID", rpad(col("APL_RVWR_ID"), F.lit(<...>), " "))
    .withColumn("LAST_UPDT_USER_SK", rpad(col("LAST_UPDT_USER_SK"), F.lit(<...>), " "))
    .withColumn("APL_RVWR_CAT_CD_SK", rpad(col("APL_RVWR_CAT_CD_SK"), F.lit(<...>), " "))
    .withColumn("APL_RVWR_SUBCAT_CD_SK", rpad(col("APL_RVWR_SUBCAT_CD_SK"), F.lit(<...>), " "))
    .withColumn("LAST_UPDT_DTM", rpad(col("LAST_UPDT_DTM"), F.lit(<...>), " "))
    .withColumn("CRDTL_SUM_DESC", rpad(col("CRDTL_SUM_DESC"), F.lit(<...>), " "))
)

write_files(
    df_key_final,
    f"{adls_path}/key/FctsAplRvwrExtr.AplRvwr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# STAGE: hf_apl_rvwr_updt (CHashedFileStage) - scenario B write-back
#   We do a merge (upsert) into the dummy table "IDS.dummy_hf_apl_rvwr"
#   Primary key: SRC_SYS_CD, APL_RVWR_ID
#   Insert/Update columns: SRC_SYS_CD, APL_RVWR_ID, CRT_RUN_CYC_EXCTN_SK, APL_RVWR_SK
# --------------------------------------------------------------------------------
# Write df_updt into a physical STAGING table, then merge
staging_table_updt = "STAGING.FctsAplRvwrExtr_hf_apl_rvwr_updt_temp"

# Drop staging table if exists
execute_dml(f"DROP TABLE IF EXISTS {staging_table_updt}", jdbc_url_ids, jdbc_props_ids)

# Create the staging table by writing df_updt
(
    df_updt
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", staging_table_updt)
    .mode("overwrite")
    .save()
)

# Merge statement
merge_sql_updt = f"""
MERGE INTO IDS.dummy_hf_apl_rvwr AS T
USING {staging_table_updt} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
AND T.APL_RVWR_ID = S.APL_RVWR_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.APL_RVWR_SK = S.APL_RVWR_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, APL_RVWR_ID, CRT_RUN_CYC_EXCTN_SK, APL_RVWR_SK)
  VALUES (S.SRC_SYS_CD, S.APL_RVWR_ID, S.CRT_RUN_CYC_EXCTN_SK, S.APL_RVWR_SK);
"""

execute_dml(merge_sql_updt, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# STAGE: SnapShot_FACETS (ODBCConnector)
# --------------------------------------------------------------------------------
snap_query_Extract = (
    f"SELECT REPS.MCRE_ID, REPS.APRP_EFF_DT "
    f"FROM {FacetsOwner}.CMC_APRP_REPS REPS "
    f"WHERE REPS.APRP_LAST_UPD_DTM >= '{BeginDate}' "
    f"AND REPS.APRP_LAST_UPD_DTM <= '{EndDate}'"
)
df_SnapShot_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", snap_query_Extract)
    .load()
)

snap_query_McreId = (
    f"SELECT REPS.MCRE_ID, ENT.MCRE_NAME "
    f"FROM {FacetsOwner}.CMC_APRP_REPS REPS, {FacetsOwner}.CMC_MCRE_RELAT_ENT ENT "
    f"WHERE REPS.MCRE_ID = ENT.MCRE_ID"
)
df_SnapShot_FACETS_McreId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", snap_query_McreId)
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_aplbalrvwr_mcre_lkup (CHashedFileStage) - SCENARIO A
# --------------------------------------------------------------------------------
# Key columns = [MCRE_ID]
df_SnapShot_FACETS_McreId_dedup = dedup_sort(df_SnapShot_FACETS_McreId, ["MCRE_ID"], [("MCRE_ID", "A")])

# --------------------------------------------------------------------------------
# STAGE: Rules (CTransformerStage)
#   PrimaryLink => df_SnapShot_FACETS_Extract alias "Extract"
#   LookupLink => df_SnapShot_FACETS_McreId_dedup alias "Mcrelkup", left join on MCRE_ID
#   Stage variable: svSrcSysCdSk => GetFkeyCodes("IDS",1,"SOURCE SYSTEM","FACETS","X")
# --------------------------------------------------------------------------------
df_rules_join = df_SnapShot_FACETS_Extract.alias("Extract").join(
    df_SnapShot_FACETS_McreId_dedup.alias("Mcrelkup"),
    on=[col("Extract.MCRE_ID") == col("Mcrelkup.MCRE_ID")],
    how="left"
)
df_rules_stageVar = df_rules_join.withColumn(
    "svSrcSysCdSk",
    F.expr('GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X")')
)

df_rules = df_rules_stageVar.select(
    col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.expr("UpCase(trim(Extract.MCRE_ID))").alias("APL_RVWR_ID"),
    F.expr('FORMAT_DATE(Extract.APRP_EFF_DT, "SYBASE", "TIMESTAMP", "CCYY-MM-DD")').alias("EFF_DT_SK"),
    when(
        (col("Mcrelkup.MCRE_NAME").isNull()) | (length(col("Mcrelkup.MCRE_NAME")) == 0),
        F.lit(" ")
    ).otherwise(
        F.expr("UpCase(trim(strip_field(Mcrelkup.MCRE_NAME)))")
    ).alias("APL_RVWR_NM")
)

# --------------------------------------------------------------------------------
# STAGE: B_APL_RVWR (CSeqFileStage)
#   Input => df_rules
#   Output => load/B_APL_RVWR.dat
# --------------------------------------------------------------------------------
# Because directory is "load", use adls_path
df_b_apl_rvwr_final = (
    df_rules
    # Apply rpad for known or <...> for unknown
    .withColumn("APL_RVWR_ID", rpad(col("APL_RVWR_ID"), 9, " "))       # from original definition char(9)
    .withColumn("EFF_DT_SK", rpad(col("EFF_DT_SK"), 10, " "))
    .withColumn("APL_RVWR_NM", rpad(col("APL_RVWR_NM"), 50, " "))
    # SRC_SYS_CD_SK has no declared length => use <...>
    .withColumn("SRC_SYS_CD_SK", rpad(col("SRC_SYS_CD_SK"), F.lit(<...>), " "))
)

write_files(
    df_b_apl_rvwr_final,
    f"{adls_path}/load/B_APL_RVWR.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)