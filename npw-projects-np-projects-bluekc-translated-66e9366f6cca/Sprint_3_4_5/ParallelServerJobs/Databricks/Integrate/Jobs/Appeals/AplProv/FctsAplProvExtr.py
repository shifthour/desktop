# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name: FctsAplProvExtr
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APAC_ACTIVITY for loading into IDS.
# MAGIC 
# MAGIC PROCESSING:    Creates appeals activity data
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/02/2007                	3028                	Initial program                                                              		devlIDS30                 	Steph Goddard     	8/23/07  
# MAGIC Naren                 		09/24/2007                	3028                	Made Logic Changes on PROV_GRP_PROV_SK      		devlIDS30         
# MAGIC Bhoomi Dasari   		10/18/2007                 	3028		Added Balancing Snapshot                                         		devlIDS30
# MAGIC Jag Yelavarthi    		2015-01-19                  	TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"               		IntegrateNewDevl      	Kalyan Neelam      	2015-01-27
# MAGIC Manasa Andru    		2016-03-16                	5391               	Corrected the field name from APAP_INIT_DT to        		IntegrateDev1            	Kalyan Neelam     	2016-03-17
# MAGIC                                                              					APAP_INIT_DTM in the extract SQL joins in the Facets
# MAGIC                                                                   				Stage.
# MAGIC Prabhu ES           		2022-02-25                	S2S		MSSQL connection parameters added                         		IntegrateDev5	Ken Bradmon	2022-05-17

# MAGIC Balancing Snapshot
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Prov activity Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
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


FacetsOwner = get_widget_value('FacetsOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

df_hf_apl_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT SRC_SYS_CD, APL_ID, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, APL_PROV_SK FROM dummy_hf_apl_prov")
    .load()
)

df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CMC_APAP_APPEALS.APAP_ID,CMC_APAP_APPEALS.APAP_PRPR_ID1,CMC_APAP_APPEALS.APAP_MCTR_PRRE_1,CMC_APAP_APPEALS.APAP_PRPR_ID2,CMC_APAP_APPEALS.APAP_MCTR_PRRE_2,CMC_APAP_APPEALS.APAP_PRPR_ID3,CMC_APAP_APPEALS.APAP_MCTR_PRRE_3,CMC_APAP_APPEALS.APAP_PRPR_ID4,CMC_APAP_APPEALS.APAP_MCTR_PRRE_4 FROM {FacetsOwner}.CMC_APAP_APPEALS CMC_APAP_APPEALS WHERE CMC_APAP_APPEALS.APAP_LAST_UPD_DTM >= '{BeginDate}'   ORDER BY CMC_APAP_APPEALS.APAP_ID"
    )
    .load()
)

df_FACETS_ProvPrprId2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT \nAPPEALS.APAP_PRPR_ID2,\nRELATION.PRER_PRPR_ID\nFROM \n{FacetsOwner}.CMC_APAP_APPEALS APPEALS,\n{FacetsOwner}.CMC_PRPR_PROV PROV,\n{FacetsOwner}.CMC_PRER_RELATION RELATION\nWHERE\nAPPEALS.APAP_PRPR_ID2=PROV.PRPR_ID AND\nPROV.PRPR_ID=RELATION.PRPR_ID AND\nAPPEALS.APAP_INIT_DTM>=RELATION.PRER_EFF_DT AND\nAPPEALS.APAP_INIT_DTM<=RELATION.PRER_TERM_DT AND\nPROV.PRPR_ENTITY='P' AND\nRELATION.PRER_PRPR_ENTITY='G'"
    )
    .load()
)

df_FACETS_ProvPrprId3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT \nAPPEALS.APAP_PRPR_ID3,\nRELATION.PRER_PRPR_ID\nFROM \n{FacetsOwner}.CMC_APAP_APPEALS APPEALS,\n{FacetsOwner}.CMC_PRPR_PROV PROV,\n{FacetsOwner}.CMC_PRER_RELATION RELATION\nWHERE\nAPPEALS.APAP_PRPR_ID3=PROV.PRPR_ID AND\nPROV.PRPR_ID=RELATION.PRPR_ID AND\nAPPEALS.APAP_INIT_DTM>=RELATION.PRER_EFF_DT AND\nAPPEALS.APAP_INIT_DTM<=RELATION.PRER_TERM_DT AND\nPROV.PRPR_ENTITY='P' AND\nRELATION.PRER_PRPR_ENTITY='G'"
    )
    .load()
)

df_FACETS_ProvPrprId1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT \nAPPEALS.APAP_PRPR_ID1,\nRELATION.PRER_PRPR_ID\nFROM \n{FacetsOwner}.CMC_APAP_APPEALS APPEALS,\n{FacetsOwner}.CMC_PRPR_PROV PROV,\n{FacetsOwner}.CMC_PRER_RELATION RELATION\nWHERE\nAPPEALS.APAP_PRPR_ID1=PROV.PRPR_ID AND\nPROV.PRPR_ID=RELATION.PRPR_ID AND\nAPPEALS.APAP_INIT_DTM>=RELATION.PRER_EFF_DT AND\nAPPEALS.APAP_INIT_DTM<=RELATION.PRER_TERM_DT AND\nPROV.PRPR_ENTITY='P' AND\nRELATION.PRER_PRPR_ENTITY='G'"
    )
    .load()
)

df_FACETS_ProvPrprId4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT \nAPPEALS.APAP_PRPR_ID4,\nRELATION.PRER_PRPR_ID\nFROM \n{FacetsOwner}.CMC_APAP_APPEALS APPEALS,\n{FacetsOwner}.CMC_PRPR_PROV PROV,\n{FacetsOwner}.CMC_PRER_RELATION RELATION\nWHERE\nAPPEALS.APAP_PRPR_ID4=PROV.PRPR_ID AND\nPROV.PRPR_ID=RELATION.PRPR_ID AND\nAPPEALS.APAP_INIT_DTM>=RELATION.PRER_EFF_DT AND\nAPPEALS.APAP_INIT_DTM<=RELATION.PRER_TERM_DT AND\nPROV.PRPR_ENTITY='P' AND\nRELATION.PRER_PRPR_ENTITY='G'"
    )
    .load()
)

df_temp_1 = df_FACETS_ProvPrprId1.select(
    F.rpad(F.col("APAP_PRPR_ID1"), 12, " ").alias("APAP_PRPR_ID1"),
    F.rpad(F.col("PRER_PRPR_ID"), 12, " ").alias("PRER_PRPR_ID")
)
write_files(df_temp_1, f"{adls_path}/hf_apl_prov_prpr_id_1.parquet", ",", "overwrite", True, True, "\"", None)
df_hf_apl_prov_prpr_id_1 = spark.read.parquet(f"{adls_path}/hf_apl_prov_prpr_id_1.parquet")

df_temp_2 = df_FACETS_ProvPrprId2.select(
    F.rpad(F.col("APAP_PRPR_ID2"), 12, " ").alias("APAP_PRPR_ID2"),
    F.rpad(F.col("PRER_PRPR_ID"), 12, " ").alias("PRER_PRPR_ID")
)
write_files(df_temp_2, f"{adls_path}/hf_apl_prov_prpr_id_2.parquet", ",", "overwrite", True, True, "\"", None)
df_hf_apl_prov_prpr_id_2 = spark.read.parquet(f"{adls_path}/hf_apl_prov_prpr_id_2.parquet")

df_temp_3 = df_FACETS_ProvPrprId3.select(
    F.rpad(F.col("APAP_PRPR_ID3"), 12, " ").alias("APAP_PRPR_ID3"),
    F.rpad(F.col("PRER_PRPR_ID"), 12, " ").alias("PRER_PRPR_ID")
)
write_files(df_temp_3, f"{adls_path}/hf_apl_prov_prpr_id_3.parquet", ",", "overwrite", True, True, "\"", None)
df_hf_apl_prov_prpr_id_3 = spark.read.parquet(f"{adls_path}/hf_apl_prov_prpr_id_3.parquet")

df_temp_4 = df_FACETS_ProvPrprId4.select(
    F.rpad(F.col("APAP_PRPR_ID4"), 12, " ").alias("APAP_PRPR_ID4"),
    F.rpad(F.col("PRER_PRPR_ID"), 12, " ").alias("PRER_PRPR_ID")
)
write_files(df_temp_4, f"{adls_path}/hf_apl_prov_prpr_id_4.parquet", ",", "overwrite", True, True, "\"", None)
df_hf_apl_prov_prpr_id_4 = spark.read.parquet(f"{adls_path}/hf_apl_prov_prpr_id_4.parquet")

df_Strip_ID1 = (
    df_FACETS_Extract
    .filter(
        F.length(F.trim(F.translate(F.col("APAP_PRPR_ID1"), "\n\r\t", ""))) != 0
    )
    .select(
        F.translate(F.col("APAP_ID"), "\n\r\t", "").alias("APAP_ID"),
        F.translate(F.col("APAP_PRPR_ID1"), "\n\r\t", "").alias("APAP_PRPR_ID"),
        F.translate(F.col("APAP_MCTR_PRRE_1"), "\n\r\t", "").alias("APAP_MCTR_PRRE"),
        F.lit("1").alias("SEQ_NO")
    )
)

df_Strip_ID2 = (
    df_FACETS_Extract
    .filter(
        F.length(F.trim(F.translate(F.col("APAP_PRPR_ID2"), "\n\r\t", ""))) != 0
    )
    .select(
        F.translate(F.col("APAP_ID"), "\n\r\t", "").alias("APAP_ID"),
        F.translate(F.col("APAP_PRPR_ID2"), "\n\r\t", "").alias("APAP_PRPR_ID"),
        F.translate(F.col("APAP_MCTR_PRRE_2"), "\n\r\t", "").alias("APAP_MCTR_PRRE"),
        F.lit("2").alias("SEQ_NO")
    )
)

df_Strip_ID3 = (
    df_FACETS_Extract
    .filter(
        F.length(F.trim(F.translate(F.col("APAP_PRPR_ID3"), "\n\r\t", ""))) != 0
    )
    .select(
        F.translate(F.col("APAP_ID"), "\n\r\t", "").alias("APAP_ID"),
        F.translate(F.col("APAP_PRPR_ID3"), "\n\r\t", "").alias("APAP_PRPR_ID"),
        F.translate(F.col("APAP_MCTR_PRRE_3"), "\n\r\t", "").alias("APAP_MCTR_PRRE"),
        F.lit("3").alias("SEQ_NO")
    )
)

df_Strip_ID4 = (
    df_FACETS_Extract
    .filter(
        F.length(F.trim(F.translate(F.col("APAP_PRPR_ID4"), "\n\r\t", ""))) != 0
    )
    .select(
        F.translate(F.col("APAP_ID"), "\n\r\t", "").alias("APAP_ID"),
        F.translate(F.col("APAP_PRPR_ID4"), "\n\r\t", "").alias("APAP_PRPR_ID"),
        F.translate(F.col("APAP_MCTR_PRRE_4"), "\n\r\t", "").alias("APAP_MCTR_PRRE"),
        F.lit("4").alias("SEQ_NO")
    )
)

df_merge = df_Strip_ID1.unionByName(df_Strip_ID2).unionByName(df_Strip_ID3).unionByName(df_Strip_ID4)

df_BusinessRules_pre = (
    df_merge.alias("Strip")
    .join(df_hf_apl_prov_prpr_id_1.alias("PrprId1Lkup"), F.col("Strip.APAP_PRPR_ID") == F.col("PrprId1Lkup.APAP_PRPR_ID1"), "left")
    .join(df_hf_apl_prov_prpr_id_2.alias("PrprId2Lkup"), F.col("Strip.APAP_PRPR_ID") == F.col("PrprId2Lkup.APAP_PRPR_ID2"), "left")
    .join(df_hf_apl_prov_prpr_id_3.alias("PrprId3Lkup"), F.col("Strip.APAP_PRPR_ID") == F.col("PrprId3Lkup.APAP_PRPR_ID3"), "left")
    .join(df_hf_apl_prov_prpr_id_4.alias("PrprId4Lkup"), F.col("Strip.APAP_PRPR_ID") == F.col("PrprId4Lkup.APAP_PRPR_ID4"), "left")
    .select(
        F.lit("Y").alias("RowPassThru"),
        F.lit("FACETS").alias("svSrcSysCd"),
        F.trim(F.col("Strip.APAP_ID")).alias("svAplId"),
        F.col("Strip.SEQ_NO").alias("svSeqNo"),
        F.trim(F.col("Strip.APAP_PRPR_ID")).alias("svPrPrId"),
        F.trim(F.col("Strip.APAP_MCTR_PRRE")).alias("svMctrId"),
        F.col("PrprId1Lkup.APAP_PRPR_ID1").alias("APAP_PRPR_ID1_lkp"),
        F.col("PrprId1Lkup.PRER_PRPR_ID").alias("PRER_PRPR_ID1"),
        F.col("PrprId2Lkup.APAP_PRPR_ID2").alias("APAP_PRPR_ID2_lkp"),
        F.col("PrprId2Lkup.PRER_PRPR_ID").alias("PRER_PRPR_ID2"),
        F.col("PrprId3Lkup.APAP_PRPR_ID3").alias("APAP_PRPR_ID3_lkp"),
        F.col("PrprId3Lkup.PRER_PRPR_ID").alias("PRER_PRPR_ID3"),
        F.col("PrprId4Lkup.APAP_PRPR_ID4").alias("APAP_PRPR_ID4_lkp"),
        F.col("PrprId4Lkup.PRER_PRPR_ID").alias("PRER_PRPR_ID4")
    )
)

df_BusinessRules = (
    df_BusinessRules_pre
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
        F.col("RunDate").alias("FIRST_RECYC_DT"),  # from parameter
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.concat_ws(";", F.col("svSrcSysCd"), F.col("svAplId"), F.col("svSeqNo")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("APL_PROV_SK"),
        F.col("svAplId").alias("APL_ID"),
        F.col("svSeqNo").alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAplId").alias("APL_SK"),
        F.rpad(
            F.when(F.col("svPrPrId").isNull(), F.lit("UNK")).otherwise(F.col("svPrPrId")),
            12, " "
        ).alias("PROV_ID"),
        F.rpad(
            F.when(F.col("APAP_PRPR_ID1_lkp").isNotNull(), F.col("PRER_PRPR_ID1"))
             .when(F.col("APAP_PRPR_ID2_lkp").isNotNull(), F.col("PRER_PRPR_ID2"))
             .when(F.col("APAP_PRPR_ID3_lkp").isNotNull(), F.col("PRER_PRPR_ID3"))
             .when(F.col("APAP_PRPR_ID4_lkp").isNotNull(), F.col("PRER_PRPR_ID4"))
             .otherwise(F.lit("NA")),
            12, " "
        ).alias("PROV_GRP_PROV_ID"),
        F.when(
            (F.col("svMctrId").isNull()) | (F.length(F.trim(F.col("svMctrId"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("svMctrId")).alias("APL_PROV_RELSHP_RSN_CD_SK")
    )
    .withColumn("FIRST_RECYC_DT", F.col("FIRST_RECYC_DT"))
)

df_Snapshot_Load = df_BusinessRules.select(
    F.col("APL_ID").alias("APL_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("PROV_ID").alias("PROV_ID")
)

df_Snapshot_Transform = df_BusinessRules.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("APL_PROV_SK"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK"),
    F.col("PROV_ID"),
    F.col("PROV_GRP_PROV_ID"),
    F.col("APL_PROV_RELSHP_RSN_CD_SK")
)

df_PrimaryKey_join = (
    df_Snapshot_Transform.alias("Transform")
    .join(
        df_hf_apl_prov.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.APL_ID") == F.col("lkup.APL_ID"),
            F.col("Transform.SEQ_NO") == F.col("lkup.SEQ_NO")
        ],
        "left"
    )
)

df_updt = df_PrimaryKey_join.filter(F.col("lkup.APL_PROV_SK").isNull()).select(
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.expr(
        "CASE WHEN lkup.APL_PROV_SK IS NULL THEN (CASE WHEN 1=1 THEN <DB sequence name> ELSE <DB sequence name> END) ELSE lkup.APL_PROV_SK END"
    ).alias("APL_PROV_SK")  
    # We must still reflect that SK is set by a KeyMgtGetNextValueConcurrent => SurrogateKeyGen call right after df_enriched is defined.
    # But the instructions say to keep the logic. We'll keep the expression to signal the new SK. 
)

df_Key = df_PrimaryKey_join.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT"),
    F.col("Transform.RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING"),
    F.when(F.col("lkup.APL_PROV_SK").isNull(),
           F.expr("CASE WHEN 1=1 THEN <DB sequence name> ELSE <DB sequence name> END")
          ).otherwise(F.col("lkup.APL_PROV_SK")).alias("APL_PROV_SK"),
    F.col("Transform.APL_ID").alias("APL_ID"),
    F.col("Transform.SEQ_NO").alias("SEQ_NO"),
    F.when(F.col("lkup.APL_PROV_SK").isNull(), F.expr("CASE WHEN 1=1 THEN CurrRunCycle ELSE CurrRunCycle END")).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.APL_SK").alias("APL_SK"),
    F.col("Transform.PROV_ID").alias("PROV_ID"),
    F.col("Transform.PROV_GRP_PROV_ID").alias("PROV_GRP_PROV_ID"),
    F.col("Transform.APL_PROV_RELSHP_RSN_CD_SK").alias("APL_PROV_RELSHP_RSN_CD_SK")
)

df_updt_table = "STAGING.FctsAplProvExtr_hf_apl_prov_updt_temp"
spark.sql(f"DROP TABLE IF EXISTS {df_updt_table}")

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", df_updt_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE dummy_hf_apl_prov AS T
USING {df_updt_table} AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.APL_ID = S.APL_ID
    AND T.SEQ_NO = S.SEQ_NO
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.APL_PROV_SK = S.APL_PROV_SK
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, APL_ID, SEQ_NO, CRT_RUN_CYC_EXCTN_SK, APL_PROV_SK)
    VALUES(S.SRC_SYS_CD, S.APL_ID, S.SEQ_NO, S.CRT_RUN_CYC_EXCTN_SK, S.APL_PROV_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_ids_apl_prov_extr_write = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("APL_PROV_SK"),
    F.col("APL_ID"),
    F.col("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_SK"),
    F.rpad(F.col("PROV_ID"), 12, " ").alias("PROV_ID"),
    F.rpad(F.col("PROV_GRP_PROV_ID"), 12, " ").alias("PROV_GRP_PROV_ID"),
    F.col("APL_PROV_RELSHP_RSN_CD_SK")
)

write_files(
    df_ids_apl_prov_extr_write,
    f"{adls_path}/key/FctsAplProvExtr.AplProv.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Transformer = df_Snapshot_Load.withColumn(
    "svSrcSysCdSk",
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X")
).withColumn(
    "svProvSk",
    GetFkeyProv("FACETS", 100, F.trim(F.col("PROV_ID")), "X")
)

df_B_APL_PROV = df_Transformer.select(
    F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("svProvSk").alias("PROV_SK")
)

write_files(
    df_B_APL_PROV,
    f"{adls_path}/load/B_APL_PROV.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)