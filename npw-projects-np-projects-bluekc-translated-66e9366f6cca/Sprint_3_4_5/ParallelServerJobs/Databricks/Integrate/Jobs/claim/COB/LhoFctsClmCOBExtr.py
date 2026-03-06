# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmCOBExtr
# MAGIC CALLED BY:   LhoFctsClmExtr1Seq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the FctsClmCOBExtr file created in FctsClmCOBExtr  job and adds the Recycle fields and values for the SK if possible.
# MAGIC 
# MAGIC     
# MAGIC 
# MAGIC INPUTS:
# MAGIC    #FilePath#/verified #InFile#.Prod #InFile#;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:     hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  File is renamed in job control to final file name before sorting.
# MAGIC                   
# MAGIC                  This is a pulls all records for procedure code table - table is replaced each day - there is no way to determine new records - no audit file
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------      
# MAGIC O. Nielsen                 02/01/2005-   Originally Programmed 
# MAGIC SAndrew                   02/22/2005-   Renamed hf_clsm_cob  to hf_clm_cob_med_sup
# MAGIC                                                         renamed from hf_clcb_cob to hf_clm_cob
# MAGIC Landon H.                03/2005         Added logic to integrate ITS COB from the CLIO table.
# MAGIC Sharon Andrew        05/2005-   added logic to support claims that were on the hit list and therefore their data still existed on the old Facets tables.  
# MAGIC                                                               if claim last activity date was after Nov 6 2004 then all its COB data is on two different facets tables
# MAGIC                                                               if claim last activity date was before  Nov 6 2004 then all its COB data is on one facets tables and it is not product specific
# MAGIC Steph Goddard          02/10/2006  Changed to combine extract, transform, primary key for sequencer
# MAGIC BJ Luce                     03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Steph Goddard          07/31/2006  Added a 'TRIM' to claim number for old claims in transform
# MAGIC Brent Leland              08/23/3006   Changed CLM_COB_TYP field definition from integer/unknown to varchar(20).
# MAGIC                                                            Added delete process to remove historical records with different cob_typ_sk.
# MAGIC SAndrew                   12/08/2006   Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen            08/15/2007   Added Balancing Snapshot File                                                                                     devlIDS30                   Steph Goddard          8/30/07
# MAGIC Ralph Tucker           2008-7-01        3657 Primary Key  Changed primary key from hash file to DB2 table                         devlIDS                           Steph Goddard          changes required
# MAGIC 
# MAGIC Manasa Andru          2013-09-23     Changed the hashed file name from hf_prod_list to hf_clm_cob_prod_list                 IntegrateNewdevl           Kalyan Neelam             2013-09-27
# MAGIC                                                                     and added documentation for the file.
# MAGIC 
# MAGIC Manasa Andru        2015-05-08           Changed the metadata for CLM_COB_TYP_CD to                                              IntegrateNewDevl           Kalyan Neelam             2015-05-11 
# MAGIC                                                                    Varchar 20 in the Transform output link.
# MAGIC 
# MAGIC Manasa Andru        2016-10-14           Changed the datatype of the field CLM_ID from Char18 to Varchar20                 IntegrateDev1                 Kalyan Neelam            2016-10-21
# MAGIC                                                                   in the Snapshot stage and Transform link.
# MAGIC 
# MAGIC Goutham K               2021-07-13                      US-386524                                  Implemented Facets Upgrade REAS_CD changes            IntegrateDev2    Jeyaprasanna       2021-07-14
# MAGIC                                                                                                                 to mke the job compatable to run pointing to SYBATCH        
# MAGIC 	
# MAGIC Prabhu ES               2022-03-29                       S2S                                             MSSQL ODBC conn params added                                   IntegrateDev5	Ken Bradmon	2022-06-08

# MAGIC This hashed file is loaded by FctsClmPrereqSeq. The source data for this hashed file is found in /landing/CLM_COB_PROD_LIST.dat
# MAGIC Delete process is done before loading to remove historical data that no longer exists in the source system.
# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim COB 
# MAGIC Data for a specific Time Period
# MAGIC COB info is in two different places depending on the claims date of entry into Facets.  Claims entered < Nov 2004 have all its COB info on the clcb_cl_cob table.  Clms entered > November 2004 have COB info on CLCB and med_sup.  If claim with service < Nov 2004 is edited, its COB info is moved into the >Nov 2004 tables.   If clm with service < Nov 2004 is just added to hit list, then COB info is still in old tables.
# MAGIC This container is used in:
# MAGIC FctsClmCOBExtr
# MAGIC NascoClmCOBTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmCobPK
# COMMAND ----------

# Retrieve Parameter Values
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstgowner_secret_name = get_widget_value('lhofacetsstgowner_secret_name','')

# --------------------------------------------------------------------------------
# Stage: hf_clm_fcts_reversals (CHashedFileStage) - SCENARIO C
# Read from parquet file, replicate columns in correct order (scenario C)
df_hf_clm_fcts_reversals = spark.read.format("parquet").load(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM")
)

# --------------------------------------------------------------------------------
# Stage: clm_nasco_dup_bypass (CHashedFileStage) - SCENARIO C
df_clm_nasco_dup_bypass = spark.read.format("parquet").load(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select(
    F.col("CLM_ID")
)

# --------------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2Connector) - Database=IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_cd_mppng = (
    f"SELECT TRGT_DOMAIN_NM,SRC_CD,TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM='EXPLANATION CODE PROVIDER ADJUSTMENT'\n\n{IDSOwner}.CD_MPPNG"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppng)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Hashed_File_76 (CHashedFileStage) - SCENARIO A for intermediate hashed file
# Deduplicate df_CD_MPPNG on key columns (TRGT_DOMAIN_NM, SRC_CD), then feed to next stage
df_hashed_file_76 = dedup_sort(
    df_CD_MPPNG,
    partition_cols=["TRGT_DOMAIN_NM","SRC_CD"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: hf_clm_cob_prod_list (CHashedFileStage) - SCENARIO C
df_hf_clm_cob_prod_list = spark.read.format("parquet").load(f"{adls_path}/hf_clm_cob_prod_list.parquet")
df_hf_clm_cob_prod_list = df_hf_clm_cob_prod_list.select(
    F.col("prod_prefix")
)

# --------------------------------------------------------------------------------
# Stage: LhoFACETS_clm (ODBCConnector)
jdbc_url_lhofacets, jdbc_props_lhofacets = get_db_config(lhofacetsstgowner_secret_name)
extract_query_lhofacets_clm = (
    f"SELECT clm.CLCL_ID, clm.PDPD_ID, clm.CLCL_LAST_ACT_DTM \n\n"
    f"FROM  tempdb..{DriverTable}  TMP ,\n"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM  clm\n"
    f"WHERE TMP.CLM_ID = clm.CLCL_ID"
)
df_LhoFACETS_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_lhofacets_clm)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: TrnsStripField (CTransformerStage)
df_TrnsStripField = df_LhoFACETS_clm
df_TrnsStripField = df_TrnsStripField.withColumn("CLCL_ID", trim(F.col("CLCL_ID")))
df_TrnsStripField = df_TrnsStripField.withColumn("PDPD_ID", trim(F.col("PDPD_ID")))
df_TrnsStripField = df_TrnsStripField.withColumn("CLCL_LAST_ACT_DT", F.col("CLCL_LAST_ACT_DTM").substr(1,10))
df_TrnsStripField = df_TrnsStripField.select(
    F.col("CLCL_ID"),
    F.col("PDPD_ID"),
    F.col("CLCL_LAST_ACT_DT")
)

# --------------------------------------------------------------------------------
# Stage: LhoFacets_COB (ODBCConnector) - two output pins
extract_query_cmc_clcb_cl_cob = (
    f"SELECT \n"
    f"COB.CLCL_ID,\n"
    f"COB.MEME_CK,\n"
    f"COB.CLCB_COB_TYPE,\n"
    f"COB.CLCB_COB_AMT,\n"
    f"COB.CLCB_COB_DISALLOW,\n"
    f"COB.CLCB_COB_ALLOW,\n"
    f"COB.CLCB_COB_SANCTION,\n"
    f"COB.CLCB_COB_DED_AMT,\n"
    f"COB.CLCB_COB_COPAY_AMT,\n"
    f"COB.CLCB_COB_COINS_AMT,\n"
    f"C.CLCC_OC_REAS_CODE as CLCB_COB_REAS_CD\n"
    f"FROM \ntempdb..{DriverTable}  TMP  \n"
    f"  INNER JOIN \n{LhoFacetsStgOwner}.CMC_CLCB_CL_COB COB \n"
    f"     ON  TMP.CLM_ID = COB.CLCL_ID\n"
    f"LEFT OUTER  JOIN \n"
    f"(select A.CLCL_ID,A.CLCC_OC_REAS_CODE from {LhoFacetsStgOwner}.CMC_CLCC_CARC_CODE A,(\n"
    f"select CLCL_ID, MIN(CLCC_SEQ_NO) as CLCC_SEQ_NO , CLCC_OC_REAS_CODE from {LhoFacetsStgOwner}.CMC_CLCC_CARC_CODE \n"
    f"group by CLCL_ID ,CLCC_OC_REAS_CODE\n"
    f") as B\n"
    f"where A.CLCL_ID = B.CLCL_ID and A.CLCC_SEQ_NO = B.CLCC_SEQ_NO and A.CLCC_OC_REAS_CODE = B.CLCC_OC_REAS_CODE) C\n"
    f"ON COB.CLCL_ID = C.CLCL_ID"
)
extract_query_cmc_clsm_med_supp = (
    f"SELECT \n"
    f"CLCL_ID,\n"
    f"MEME_CK,\n"
    f"CLSM_OC_ALLOW,\n"
    f"CLSM_OC_DED_AMT,\n"
    f"CLSM_OC_COINS_AMTM,\n"
    f"CLSM_OC_COINS_AMTP,\n"
    f"CLSM_OC_PAID_AMT,\n"
    f"CLSM_OC_REAS_CD,\n"
    f"CLSM_OC_GROUP_CD \n"
    f"FROM {LhoFacetsStgOwner}.CMC_CLSM_MED_SUPP A,  tempdb..{DriverTable}  TMP WHERE TMP.CLM_ID = A.CLCL_ID"
)

df_LhoFacets_COB_CMC_CLCB_CL_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_cmc_clcb_cl_cob)
    .load()
)

df_LhoFacets_COB_CMC_CLSM_MED_SUPP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_cmc_clsm_med_supp)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: from_FctClmExtr (CHashedFileStage) - SCENARIO C for each hashed file
# However, the DataStage job uses this to provide two different outputs: "med_sup" and "clm_cob."
# We will treat these as pass-through data from the loaded DataFrames plus reading from two parquet files.
df_from_FctClmExtr_med_sup_parquet = spark.read.format("parquet").load(f"{adls_path}/hf_clm_cob_med_sup.parquet")
df_from_FctClmExtr_med_sup_parquet = df_from_FctClmExtr_med_sup_parquet.select(
    F.col("CLCL_ID"),
    F.col("MEME_CK"),
    F.col("CLSM_OC_ALLOW"),
    F.col("CLSM_OC_DED_AMT"),
    F.col("CLSM_OC_COINS_AMTM"),
    F.col("CLSM_OC_COINS_AMTP"),
    F.col("CLSM_OC_PAID_AMT"),
    F.col("CLSM_OC_REAS_CD"),
    F.col("CLSM_OC_GROUP_CD")
)

df_from_FctClmExtr_clm_cob_parquet = spark.read.format("parquet").load(f"{adls_path}/hf_clm_cob_info.parquet")
df_from_FctClmExtr_clm_cob_parquet = df_from_FctClmExtr_clm_cob_parquet.select(
    F.col("CLCL_ID"),
    F.col("MEME_CK"),
    F.col("CLCB_COB_TYPE"),
    F.col("CLCB_COB_AMT"),
    F.col("CLCB_COB_DISALLOW"),
    F.col("CLCB_COB_ALLOW"),
    F.col("CLCB_COB_SANCTION"),
    F.col("CLCB_COB_DED_AMT"),
    F.col("CLCB_COB_COPAY_AMT"),
    F.col("CLCB_COB_COINS_AMT"),
    F.col("CLCB_COB_REAS_CD")
)

# Also unify with the DB data for final references (the job has 2 input pins from DB, 2 from parquet).
# We'll simply keep them in separate DataFrames. The next stage "BusinessRules" performs lookups anyway.

df_from_FctClmExtr_med_sup = df_LhoFacets_COB_CMC_CLSM_MED_SUPP.unionByName(df_from_FctClmExtr_med_sup_parquet, allowMissingColumns=True)
df_from_FctClmExtr_clm_cob = df_LhoFacets_COB_CMC_CLCB_CL_COB.unionByName(df_from_FctClmExtr_clm_cob_parquet, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: LhoFACETS2 (ODBCConnector) - two output pins
extract_query_cmc_cdcb_li_cob = (
    f"SELECT CLCL_ID,CDCB_COB_TYPE FROM tempdb..{DriverTable} TMP,  {LhoFacetsStgOwner}.CMC_CDCB_LI_COB A WHERE TMP.CLM_ID = A.CLCL_ID"
)
extract_query_cmc_cddc_dnli_cob = (
    f"SELECT distinct\n"
    f"A.CLCL_ID,\n"
    f"A.CDDC_COB_TYPE \n"
    f"FROM tempdb..{DriverTable} TMP INNER JOIN   {LhoFacetsStgOwner}.CMC_CDDC_DNLI_COB A \n"
    f"   ON TMP.CLM_ID = A.CLCL_ID\n"
    f"--group by A.CLCL_ID"
)

df_LhoFACETS2_cdcb_li_cob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_cmc_cdcb_li_cob)
    .load()
)

df_LhoFACETS2_cddc_dnli_cob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacets)
    .options(**jdbc_props_lhofacets)
    .option("query", extract_query_cmc_cddc_dnli_cob)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_clm_cob_cmc_cdcb_li_cob (CHashedFileStage) - SCENARIO C
df_hf_clm_cob_cmc_cdcb_li_cob = spark.read.format("parquet").load(f"{adls_path}/hf_clm_cob_cmc_cdcb_li_cob.parquet")
df_hf_clm_cob_cmc_cdcb_li_cob = df_hf_clm_cob_cmc_cdcb_li_cob.select(
    F.col("CLCL_ID"),
    F.col("CDCB_COB_TYPE")
)

# Combine with the pinned input from DB:
df_hf_clm_cob_cmc_cdcb_li_cob_input = df_LhoFACETS2_cdcb_li_cob.unionByName(df_hf_clm_cob_cmc_cdcb_li_cob, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: hf_clm_cob_cmc_cddc_dnli_cob (CHashedFileStage) - SCENARIO C
df_hf_clm_cob_cmc_cddc_dnli_cob = spark.read.format("parquet").load(f"{adls_path}/hf_clm_cob_cmc_cddc_dnli_cob.parquet")
df_hf_clm_cob_cmc_cddc_dnli_cob = df_hf_clm_cob_cmc_cddc_dnli_cob.select(
    F.col("CLCL_ID"),
    F.col("CDDC_COB_TYPE")
)

# Combine with the pinned input from DB:
df_hf_clm_cob_cmc_cddc_dnli_cob_input = df_LhoFACETS2_cddc_dnli_cob.unionByName(df_hf_clm_cob_cmc_cddc_dnli_cob, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# We will join multiple lookups:
# Primary link = df_TrnsStripField as "Extract"
# Lookups:
#   product_list_in => df_hf_clm_cob_prod_list
#   clm_cob => df_from_FctClmExtr_clm_cob
#   med_sup => df_from_FctClmExtr_med_sup
#   cdcb_li => df_hf_clm_cob_cmc_cdcb_li_cob_input
#   cddc_dnli => df_hf_clm_cob_cmc_cddc_dnli_cob_input

df_BusinessRules_primary = df_TrnsStripField.alias("Extract")

# Left join with product_list_in on Extract.PDPD_ID[1,2] == product_list_in.prod_prefix
# We'll create a column PDPD_ID_PREFIX = substring(Extract.PDPD_ID, 1, 2) for the join
df_BusinessRules_primary = df_BusinessRules_primary.withColumn("PDPD_ID_PREFIX", F.substring(F.col("PDPD_ID"), 1, 2))
df_hf_clm_cob_prod_list_alias = df_hf_clm_cob_prod_list.alias("product_list_in")

df_join_1 = df_BusinessRules_primary.join(
    df_hf_clm_cob_prod_list_alias,
    df_BusinessRules_primary["PDPD_ID_PREFIX"] == df_hf_clm_cob_prod_list_alias["prod_prefix"],
    how="left"
)

# Left join with clm_cob on Extract.CLCL_ID == clm_cob.CLCL_ID
df_from_FctClmExtr_clm_cob_alias = df_from_FctClmExtr_clm_cob.alias("clm_cob")
df_join_2 = df_join_1.join(
    df_from_FctClmExtr_clm_cob_alias,
    df_join_1["CLCL_ID"] == df_from_FctClmExtr_clm_cob_alias["CLCL_ID"],
    how="left"
)

# Left join with med_sup on Extract.CLCL_ID == med_sup.CLCL_ID
df_from_FctClmExtr_med_sup_alias = df_from_FctClmExtr_med_sup.alias("med_sup")
df_join_3 = df_join_2.join(
    df_from_FctClmExtr_med_sup_alias,
    df_join_2["CLCL_ID"] == df_from_FctClmExtr_med_sup_alias["CLCL_ID"],
    how="left"
)

# Left join with cdcb_li on Extract.CLCL_ID == cdcb_li.CLCL_ID
df_hf_clm_cob_cmc_cdcb_li_cob_input_alias = df_hf_clm_cob_cmc_cdcb_li_cob_input.alias("cdcb_li")
df_join_4 = df_join_3.join(
    df_hf_clm_cob_cmc_cdcb_li_cob_input_alias,
    df_join_3["CLCL_ID"] == df_hf_clm_cob_cmc_cdcb_li_cob_input_alias["CLCL_ID"],
    how="left"
)

# Left join with cddc_dnli on Extract.CLCL_ID == cddc_dnli.CLCL_ID
df_hf_clm_cob_cmc_cddc_dnli_cob_input_alias = df_hf_clm_cob_cmc_cddc_dnli_cob_input.alias("cddc_dnli")
df_BusinessRules = df_join_4.join(
    df_hf_clm_cob_cmc_cddc_dnli_cob_input_alias,
    df_join_4["CLCL_ID"] == df_hf_clm_cob_cmc_cddc_dnli_cob_input_alias["CLCL_ID"],
    how="left"
)

# Create stage variables in Python
# StageVariables:
#   SetPrefix = If IsNull(product_list_in.prod_prefix) = @TRUE then 'CLCB' else 'CLSM'
#   COBType = complicated expression
#   ClmId = trim(Extract.CLCL_ID)

def business_rules_logic(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "SetPrefix",
        F.when(F.col("product_list_in.prod_prefix").isNull(), F.lit("CLCB")).otherwise(F.lit("CLSM"))
    )

    # COBType
    # IF (Extract.CLCL_LAST_ACT_DT <= "2004-11-05") THEN
    #    If Len(trim(clm_cob.CLCB_COB_TYPE))>0 then trim(clm_cob.CLCB_COB_TYPE) Else "NA"
    # ELSE
    #   IF SetPrefix='CLCB' then ...
    #   ELSE ...
    # We must replicate the entire logic exactly, including check for med_sup etc.

    # We'll define a helper for that in-line:
    df = df.withColumn("trim_clm_cob_type", F.trim(F.col("clm_cob.CLCB_COB_TYPE")))
    df = df.withColumn("trim_cdcb_li_type", F.trim(F.col("cdcb_li.CDCB_COB_TYPE")))
    df = df.withColumn("trim_cddc_dnli_type", F.trim(F.col("cddc_dnli.CDDC_COB_TYPE")))
    df = df.withColumn("isNotNull_clm_cob_id", F.col("clm_cob.CLCL_ID").isNotNull())
    df = df.withColumn("isNotNull_cdcb_li_id", F.col("cdcb_li.CLCL_ID").isNotNull())
    df = df.withColumn("isNotNull_cddc_dnli_id", F.col("cddc_dnli.CLCL_ID").isNotNull())
    df = df.withColumn("isNotNull_med_sup_id", F.col("med_sup.CLCL_ID").isNotNull())

    # For legacy date compare, do a substring or cast as date:
    # We'll compare if CLCL_LAST_ACT_DT <= '2004-11-05'
    df = df.withColumn(
        "CLCL_LAST_ACT_DT_cast",
        F.to_date(F.col("CLCL_LAST_ACT_DT"), "yyyy-MM-dd")
    )

    # The big logic in a single when expression:
    df = df.withColumn(
        "COBType",
        F.when(
            (F.col("CLCL_LAST_ACT_DT_cast") <= F.lit("2004-11-05")),
            F.when(
                (F.length(F.trim(F.col("clm_cob.CLCB_COB_TYPE"))) > 0),
                F.trim(F.col("clm_cob.CLCB_COB_TYPE"))
            ).otherwise(F.lit("NA"))
        ).otherwise(
            F.when(
                F.col("SetPrefix") == F.lit("CLCB"),
                F.when(
                    F.col("isNotNull_clm_cob_id"),
                    F.when(
                        (F.length(F.col("trim_clm_cob_type")) > 0),
                        F.col("trim_clm_cob_type")
                    ).otherwise(
                        F.when(
                            F.col("isNotNull_cdcb_li_id") & (F.length(F.col("trim_cdcb_li_type")) > 0),
                            F.col("trim_cdcb_li_type")
                        ).otherwise(
                            F.when(
                                F.col("isNotNull_cddc_dnli_id") & (F.length(F.col("trim_cddc_dnli_type")) > 0),
                                F.col("trim_cddc_dnli_type")
                            ).otherwise(F.lit("UNK"))
                        )
                    )
                ).otherwise(F.lit("NA"))
            ).otherwise(
                F.when(
                    F.col("isNotNull_med_sup_id"),
                    F.lit("M")
                ).otherwise(F.lit("NA"))
            )
        )
    )

    df = df.withColumn("ClmId", trim(F.col("Extract.CLCL_ID")))

    return df

df_BusinessRules = business_rules_logic(df_BusinessRules)

# Next: We produce 2 output links from the transformer: "current" and "old"
# "current": Condition => 
#   (UPCASE(Extract.PDPD_ID[1,2]) in ('TP','TC','MV','MG','MS') AND ISNULL(med_sup.CLCL_ID)=@FALSE) OR
#   ((not in above) AND ISNULL(clm_cob.CLCL_ID)=@FALSE)
# "old": Condition => ISNULL(clm_cob.MEME_CK)=@FALSE and Extract.CLCL_LAST_ACT_DT<='2004-11-05'

def businessrules_split_logic(df: DataFrame) -> (DataFrame, DataFrame):
    # First define a col for PDPD_ID[1,2] uppercase:
    df = df.withColumn("PDPD_prefix_upper", F.upper(F.substring(F.col("PDPD_ID"), 1, 2)))
    df = df.withColumn("isNull_med_sup_id", F.col("med_sup.CLCL_ID").isNull())
    df = df.withColumn("isNull_clm_cob_id", F.col("clm_cob.CLCL_ID").isNull())
    df = df.withColumn("isNull_clm_cob_MEME_CK", F.col("clm_cob.MEME_CK").isNull())
    df = df.withColumn("CLCL_LAST_ACT_DT_cast", F.to_date(F.col("CLCL_LAST_ACT_DT"), "yyyy-MM-dd"))

    cond_current = (
        (
            (
                (F.col("PDPD_prefix_upper").isin("TP","TC","MV","MG","MS"))
                & (F.col("isNull_med_sup_id") == False)
            )
        )
        |
        (
            ~(F.col("PDPD_prefix_upper").isin("TP","TC","MV","MG","MS"))
            & (F.col("isNull_clm_cob_id") == False)
        )
    )

    cond_old = (
        (F.col("isNull_clm_cob_MEME_CK") == False)
        & (F.col("CLCL_LAST_ACT_DT_cast") <= F.lit("2004-11-05"))
    )

    df_current = df.filter(cond_current)
    df_old = df.filter(cond_old)
    return df_current, df_old

df_current, df_old = businessrules_split_logic(df_BusinessRules)

# Now build the columns per link
# "current" => columns named in the DataStage output pin
# "old" => columns named in the DataStage output pin

def build_current_columns(df: DataFrame) -> DataFrame:
    # The output columns must appear in the same order as the job:
    # 1) JOB_EXCTN_RCRD_ERR_SK => "0"
    # 2) INSRT_UPDT_CD => "'I'"
    # 3) DISCARD_IN => "'N'"
    # 4) PASS_THRU_IN => "'Y'"
    # 5) FIRST_RECYC_DT => CurrentDate
    # 6) ERR_CT => "0"
    # 7) RECYCLE_CT => "0"
    # 8) SRC_SYS_CD => "'LUMERIS'"
    # 9) PRI_KEY_STRING => "LUMERIS; + ClmId + ; + COBType"
    # ...
    # Over 27 columns in total. We replicate them exactly.

    df_out = (
        df
        .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
        .withColumn("INSRT_UPDT_CD", F.lit("I"))
        .withColumn("DISCARD_IN", F.lit("N"))
        .withColumn("PASS_THRU_IN", F.lit("Y"))
        .withColumn("FIRST_RECYC_DT", current_date())
        .withColumn("ERR_CT", F.lit(0))
        .withColumn("RECYCLE_CT", F.lit(0))
        .withColumn("SRC_SYS_CD", F.lit("LUMERIS"))
        .withColumn("PRI_KEY_STRING", F.concat_ws("", F.lit("LUMERIS;"), F.col("ClmId"), F.lit(";"), F.col("COBType")))
        .withColumn("CLM_COB_SK", F.lit(0))
        .withColumn("SRC_SYS_CD_SK", F.col("SrcSysCdSk"))  # not spelled out in job, but job uses "SrcSysCdSk" expression
        .withColumn("CLM_ID", F.col("ClmId"))
        .withColumn("CLM_COB_TYP_CD", F.col("COBType"))
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
        .withColumn("CLM_SK", F.lit(0))
        .withColumn("CLM_COB_LIAB_TYP_CD", F.when(
            F.col("SetPrefix")=="CLCB",
            F.lit("NA")
        ).otherwise(
            F.when(
                F.col("med_sup.CLSM_OC_GROUP_CD").isNull() | (F.length(F.trim(F.col("med_sup.CLSM_OC_GROUP_CD")))<1),
                F.lit("NA")
            ).otherwise(F.col("med_sup.CLSM_OC_GROUP_CD"))
        ))
        .withColumn("ALW_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_ALLOW").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_ALLOW")))
        ).otherwise(
            F.when(F.col("med_sup.CLSM_OC_ALLOW").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("med_sup.CLSM_OC_ALLOW")))
        ))
        .withColumn("COPAY_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_COPAY_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_COPAY_AMT")))
        ).otherwise(F.lit(0.0)))
        .withColumn("DEDCT_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_DED_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_DED_AMT")))
        ).otherwise(
            F.when(F.col("med_sup.CLSM_OC_DED_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("med_sup.CLSM_OC_DED_AMT")))
        ))
        .withColumn("DSALW_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_DISALLOW").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_DISALLOW")))
        ).otherwise(F.lit(0.0)))
        .withColumn("MED_COINS_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_COINS_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_COINS_AMT")))
        ).otherwise(
            F.when(F.col("med_sup.CLSM_OC_COINS_AMTM").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("med_sup.CLSM_OC_COINS_AMTM")))
        ))
        .withColumn("MNTL_HLTH_COINS_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.lit(0.0)
        ).otherwise(
            F.when(F.col("med_sup.CLSM_OC_COINS_AMTP").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("med_sup.CLSM_OC_COINS_AMTP")))
        ))
        .withColumn("PD_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_AMT")))
        ).otherwise(
            F.when(F.col("med_sup.CLSM_OC_PAID_AMT").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("med_sup.CLSM_OC_PAID_AMT")))
        ))
        .withColumn("SANC_AMT", F.when(
            F.col("SetPrefix")=="CLCB",
            F.when(F.col("clm_cob.CLCB_COB_SANCTION").isNull(), F.lit(0.0)).otherwise(F.abs(F.col("clm_cob.CLCB_COB_SANCTION")))
        ).otherwise(F.lit(0.0)))
        .withColumn("COB_CAR_RSN_CD_TX", F.when(
            F.col("SetPrefix")=="CLCB",
            F.col("clm_cob.CLCB_COB_REAS_CD")
        ).otherwise(
            F.col("med_sup.CLSM_OC_REAS_CD")
        ))
        .withColumn("COB_CAR_RSN_TX", F.when(
            F.col("SetPrefix")=="CLCB",
            F.col("clm_cob.CLCB_COB_REAS_CD")
        ).otherwise(
            F.col("med_sup.CLSM_OC_REAS_CD")
        ))
    )

    # Apply final select for order
    df_out = df_out.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_COB_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "ALW_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "MED_COINS_AMT",
        "MNTL_HLTH_COINS_AMT",
        "PD_AMT",
        "SANC_AMT",
        "COB_CAR_RSN_CD_TX",
        "COB_CAR_RSN_TX"
    )

    # For char/varchar columns, apply rpad
    # Checking column types from the job, we see INSRT_UPDT_CD(char(10)), DISCARD_IN(char(1)),
    # PASS_THRU_IN(char(1)), etc. We will do rpad to match lengths:
    df_out = df_out.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    df_out = df_out.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    df_out = df_out.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    return df_out

df_current_enriched = build_current_columns(df_current)

def build_old_columns(df: DataFrame) -> DataFrame:
    # replicate the pinned columns
    df_out = (
        df
        .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
        .withColumn("INSRT_UPDT_CD", F.lit("I"))
        .withColumn("DISCARD_IN", F.lit("N"))
        .withColumn("PASS_THRU_IN", F.lit("Y"))
        .withColumn("FIRST_RECYC_DT", current_date())
        .withColumn("ERR_CT", F.lit(0))
        .withColumn("RECYCLE_CT", F.lit(0))
        .withColumn("SRC_SYS_CD", F.lit("FACETS"))
        .withColumn("PRI_KEY_STRING", F.concat_ws("", F.lit("FACETS;"), F.col("ClmId"), F.lit(";"), F.col("COBType")))
        .withColumn("CLM_COB_SK", F.lit(0))
        .withColumn("SRC_SYS_CD_SK", F.col("SrcSysCdSk"))
        .withColumn("CLM_ID", F.col("ClmId"))
        .withColumn("CLM_COB_TYP_CD", F.col("COBType"))
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
        .withColumn("CLM_SK", F.lit(0))
        .withColumn("CLM_COB_LIAB_TYP_CD", F.lit("NA"))
        .withColumn("ALW_AMT", F.abs(F.col("clm_cob.CLCB_COB_ALLOW")))
        .withColumn("COPAY_AMT", F.abs(F.col("clm_cob.CLCB_COB_COPAY_AMT")))
        .withColumn("DEDCT_AMT", F.abs(F.col("clm_cob.CLCB_COB_DED_AMT")))
        .withColumn("DSALW_AMT", F.abs(F.col("clm_cob.CLCB_COB_DISALLOW")))
        .withColumn("MED_COINS_AMT", F.abs(F.col("clm_cob.CLCB_COB_COINS_AMT")))
        .withColumn("MNTL_HLTH_COINS_AMT", F.lit(0.0))
        .withColumn("PD_AMT", F.when(F.col("clm_cob.CLCB_COB_AMT").isNotNull(), F.abs(F.col("clm_cob.CLCB_COB_AMT"))).otherwise(F.lit(0.0)))
        .withColumn("SANC_AMT", F.abs(F.col("clm_cob.CLCB_COB_SANCTION")))
        .withColumn("COB_CAR_RSN_CD_TX", F.col("clm_cob.CLCB_COB_REAS_CD"))
        .withColumn("COB_CAR_RSN_TX", F.lit(None).cast(StringType()))
    )
    df_out = df_out.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_COB_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "ALW_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "MED_COINS_AMT",
        "MNTL_HLTH_COINS_AMT",
        "PD_AMT",
        "SANC_AMT",
        "COB_CAR_RSN_CD_TX",
        "COB_CAR_RSN_TX"
    )
    df_out = df_out.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    df_out = df_out.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    df_out = df_out.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    return df_out

df_old_enriched = build_old_columns(df_old)

# --------------------------------------------------------------------------------
# Stage: PriorNov2004 (CHashedFileStage) - SCENARIO C
# Compare to job: the pinned link is "old" => write to "hf_facets_cob_PriorNov2004CRF"
# We write as parquet in scenario C

df_PriorNov2004 = df_old_enriched
write_files(
    df_PriorNov2004,
    f"{adls_path}/hf_facets_cob_PriorNov2004CRF.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Then it has an output link => "CRF2" => to "Collector". 
# In DataStage, that collector merges with CRF1 from the next stage. 
# We'll just hold it in memory as df_PriorNov2004_out:

df_PriorNov2004_out = df_PriorNov2004

# --------------------------------------------------------------------------------
# Stage: Nov2004PresentCRF (CHashedFileStage) - SCENARIO C
df_Nov2004PresentCRF = df_current_enriched
write_files(
    df_Nov2004PresentCRF,
    f"{adls_path}/hf_facets_cob_Nov2004CRF.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_Nov2004PresentCRF_out = df_Nov2004PresentCRF

# --------------------------------------------------------------------------------
# Stage: Collector (CCollector)
# Merges CRF1 + CRF2 => in code, just do a union
df_Collector_merged = df_Nov2004PresentCRF_out.unionByName(df_PriorNov2004_out, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: hf_fcts_cob_All (CHashedFileStage) - SCENARIO C
# Write to "hf_fcts_cob_All.parquet" then has an output link
write_files(
    df_Collector_merged,
    f"{adls_path}/hf_fcts_cob_All.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_fcts_cob_All = df_Collector_merged

# --------------------------------------------------------------------------------
# Stage: Transformer_74 (CTransformerStage)
# InputPins:
#   "CD_MPPNG" => df_hashed_file_76 (lookup join) on TRGT_DOMAIN_NM & SRC_CD
#   "LhoFctsClmCOB" => df_hf_fcts_cob_All (primary link)
# Join conditions:
#   TRGT_DOMAIN_NM='EXPLANATION CODE PROVIDER ADJUSTMENT'
#   trim(LhoFctsClmCOB.COB_CAR_RSN_CD_TX)==CD_MPPNG.SRC_CD

df_LhoFctsClmCOB_alias = df_hf_fcts_cob_All.alias("LhoFctsClmCOB")
df_CD_MPPNG_alias = df_hashed_file_76.alias("CD_MPPNG")

# We create a trimmed COB_CAR_RSN_CD_TX for join
df_transformer_74_join = df_LhoFctsClmCOB_alias.join(
    df_CD_MPPNG_alias,
    (
        (F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT") == df_CD_MPPNG_alias["TRGT_DOMAIN_NM"])
        &
        (F.trim(df_LhoFctsClmCOB_alias["COB_CAR_RSN_CD_TX"]) == df_CD_MPPNG_alias["SRC_CD"])
    ),
    how="left"
)

# Now produce columns as per the job:
df_Transformer_74_out = df_transformer_74_join.select(
    F.col("LhoFctsClmCOB.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("LhoFctsClmCOB.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("LhoFctsClmCOB.DISCARD_IN").alias("DISCARD_IN"),
    F.col("LhoFctsClmCOB.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("LhoFctsClmCOB.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("LhoFctsClmCOB.ERR_CT").alias("ERR_CT"),
    F.col("LhoFctsClmCOB.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("LhoFctsClmCOB.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LhoFctsClmCOB.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("LhoFctsClmCOB.CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("LhoFctsClmCOB.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.trim(F.col("LhoFctsClmCOB.CLM_ID")).alias("CLM_ID"),
    F.col("LhoFctsClmCOB.CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("LhoFctsClmCOB.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LhoFctsClmCOB.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LhoFctsClmCOB.CLM_SK").alias("CLM_SK"),
    F.col("LhoFctsClmCOB.CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD_SK"),
    F.col("LhoFctsClmCOB.ALW_AMT").alias("ALW_AMT"),
    F.col("LhoFctsClmCOB.COPAY_AMT").alias("COPAY_AMT"),
    F.col("LhoFctsClmCOB.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("LhoFctsClmCOB.DSALW_AMT").alias("DSALW_AMT"),
    F.col("LhoFctsClmCOB.MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("LhoFctsClmCOB.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("LhoFctsClmCOB.PD_AMT").alias("PD_AMT"),
    F.col("LhoFctsClmCOB.SANC_AMT").alias("SANC_AMT"),
    F.col("LhoFctsClmCOB.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.when(F.col("CD_MPPNG.TRGT_CD_NM").isNull(), F.lit(None).cast(StringType())).otherwise(F.trim(F.col("CD_MPPNG.TRGT_CD_NM"))).alias("COB_CAR_RSN_TX")
)

# --------------------------------------------------------------------------------
# Stage: Apply_reversals (CTransformerStage)
df_Apply_reversals_primary = df_Transformer_74_out.alias("CLMCOB")
df_Apply_reversals_fcts_reversals = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_Apply_reversals_nasco_dup_lkup = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

# Join lookups
df_join_reversals = df_Apply_reversals_primary.join(
    df_Apply_reversals_fcts_reversals,
    F.trim(df_Apply_reversals_primary["CLM_ID"]) == df_Apply_reversals_fcts_reversals["CLCL_ID"],
    how="left"
).join(
    df_Apply_reversals_nasco_dup_lkup,
    df_Apply_reversals_primary["CLM_ID"] == df_Apply_reversals_nasco_dup_lkup["CLM_ID"],
    how="left"
)

# Output pin "COB": Constraint => IsNull(nasco_dup_lkup.CLM_ID)= @TRUE
df_COB = df_join_reversals.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())

# Output pin "Reversals": Constraint => 
# ISNULL(fcts_reversals.CLCL_ID)=@FALSE AND (fcts_reversals.CLCL_CUR_STS in ("89","91","99"))
df_Reversals = df_join_reversals.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    &
    (
       (F.col("fcts_reversals.CLCL_CUR_STS")=="89")
       | (F.col("fcts_reversals.CLCL_CUR_STS")=="91")
       | (F.col("fcts_reversals.CLCL_CUR_STS")=="99")
    )
)

# For "Reversals": CLM_ID => CLMCOB.CLM_ID : 'R', PD_AMT => NEG(...), etc:

def build_COB_columns(df: DataFrame) -> DataFrame:
    # columns must match the job pin "COB"
    # same order:
    # [JOB_EXCTN_RCRD_ERR_SK, INSRT_UPDT_CD, DISCARD_IN, PASS_THRU_IN, FIRST_RECYC_DT, ERR_CT, RECYCLE_CT, SRC_SYS_CD,
    #  PRI_KEY_STRING, CLM_COB_SK, SRC_SYS_CD_SK, CLM_ID, CLM_COB_TYP_CD, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK,
    #  CLM_SK, CLM_COB_LIAB_TYP_CD_SK, ALW_AMT, COPAY_AMT, DEDCT_AMT, DSALW_AMT, MED_COINS_AMT, MNTL_HLTH_COINS_AMT, PD_AMT,
    #  SANC_AMT, COB_CAR_RSN_CD_TX, COB_CAR_RSN_TX]
    df_out = df.select(
        F.col("CLMCOB.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("CLMCOB.INSRT_UPDT_CD"),
        F.col("CLMCOB.DISCARD_IN"),
        F.col("CLMCOB.PASS_THRU_IN"),
        F.col("CLMCOB.FIRST_RECYC_DT"),
        F.col("CLMCOB.ERR_CT"),
        F.col("CLMCOB.RECYCLE_CT"),
        F.col("CLMCOB.SRC_SYS_CD"),
        F.col("CLMCOB.PRI_KEY_STRING"),
        F.col("CLMCOB.CLM_COB_SK"),
        F.col("CLMCOB.SRC_SYS_CD_SK"),
        F.col("CLMCOB.CLM_ID"),
        F.col("CLMCOB.CLM_COB_TYP_CD"),
        F.col("CLMCOB.CRT_RUN_CYC_EXCTN_SK"),
        F.col("CLMCOB.LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLMCOB.CLM_SK"),
        F.col("CLMCOB.CLM_COB_LIAB_TYP_CD_SK"),
        F.col("CLMCOB.ALW_AMT"),
        F.col("CLMCOB.COPAY_AMT"),
        F.col("CLMCOB.DEDCT_AMT"),
        F.col("CLMCOB.DSALW_AMT"),
        F.col("CLMCOB.MED_COINS_AMT"),
        F.col("CLMCOB.MNTL_HLTH_COINS_AMT"),
        F.col("CLMCOB.PD_AMT"),
        F.col("CLMCOB.SANC_AMT"),
        F.col("CLMCOB.COB_CAR_RSN_CD_TX"),
        F.col("CLMCOB.COB_CAR_RSN_TX")
    )
    return df_out

df_COB_out = build_COB_columns(df_COB.alias("CLMCOB"))

def build_Reversals_columns(df: DataFrame) -> DataFrame:
    # same columns but do transformations: CLM_ID => CLMCOB.CLM_ID:'R', etc
    df_out = df.select(
        F.col("CLMCOB.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("CLMCOB.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("CLMCOB.DISCARD_IN").alias("DISCARD_IN"),
        F.col("CLMCOB.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("CLMCOB.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("CLMCOB.ERR_CT").alias("ERR_CT"),
        F.col("CLMCOB.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("CLMCOB.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLMCOB.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLMCOB.CLM_COB_SK").alias("CLM_COB_SK"),
        F.col("CLMCOB.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.concat_ws("", F.col("CLMCOB.CLM_ID"), F.lit("R")).alias("CLM_ID"),
        F.col("CLMCOB.CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
        F.col("CLMCOB.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CLMCOB.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.col("CLMCOB.CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK"),
        (-1*F.col("CLMCOB.ALW_AMT")).alias("ALW_AMT"),
        (-1*F.col("CLMCOB.COPAY_AMT")).alias("COPAY_AMT"),
        (-1*F.col("CLMCOB.DEDCT_AMT")).alias("DEDCT_AMT"),
        (-1*F.col("CLMCOB.DSALW_AMT")).alias("DSALW_AMT"),
        (-1*F.col("CLMCOB.MED_COINS_AMT")).alias("MED_COINS_AMT"),
        (-1*F.col("CLMCOB.MNTL_HLTH_COINS_AMT")).alias("MNTL_HLTH_COINS_AMT"),
        (-1*F.col("CLMCOB.PD_AMT")).alias("PD_AMT"),
        (-1*F.col("CLMCOB.SANC_AMT")).alias("SANC_AMT"),
        F.col("CLMCOB.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
        F.col("CLMCOB.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
    )
    return df_out

df_Reversals_out = build_Reversals_columns(df_Reversals.alias("CLMCOB"))

# --------------------------------------------------------------------------------
# Stage: Link_Collector (CCollector)
df_Link_Collector_merged = df_COB_out.unionByName(df_Reversals_out, allowMissingColumns=True)

# --------------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
# This stage has 3 output links: AllCol, RowCount, Transform
# We'll produce them by selecting from df_Link_Collector_merged
df_Snapshot_in = df_Link_Collector_merged.alias("TransformIt")

df_AllCol = df_Snapshot_in.select(
    F.col("TransformIt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    F.col("TransformIt.CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("TransformIt.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("TransformIt.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("TransformIt.DISCARD_IN").alias("DISCARD_IN"),
    F.col("TransformIt.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("TransformIt.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("TransformIt.ERR_CT").alias("ERR_CT"),
    F.col("TransformIt.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("TransformIt.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("TransformIt.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("TransformIt.CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("TransformIt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("TransformIt.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TransformIt.CLM_SK").alias("CLM_SK"),
    F.col("TransformIt.CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD"),
    F.col("TransformIt.ALW_AMT").alias("ALW_AMT"),
    F.col("TransformIt.COPAY_AMT").alias("COPAY_AMT"),
    F.col("TransformIt.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("TransformIt.DSALW_AMT").alias("DSALW_AMT"),
    F.col("TransformIt.MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("TransformIt.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("TransformIt.PD_AMT").alias("PD_AMT"),
    F.col("TransformIt.SANC_AMT").alias("SANC_AMT"),
    F.col("TransformIt.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("TransformIt.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

df_RowCount = df_Snapshot_in.select(
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    F.col("TransformIt.CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("TransformIt.ALW_AMT").alias("ALW_AMT"),
    F.col("TransformIt.PD_AMT").alias("PD_AMT")
)

df_Transform = df_Snapshot_in.select(
    F.col("TransformIt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    # Expression => If Trim(TransformIt.CLM_COB_TYP_CD)='UNK' Then 'UN' Else TransformIt.CLM_COB_TYP_CD
    F.when(
        F.trim(F.col("TransformIt.CLM_COB_TYP_CD"))=="UNK",
        F.lit("UN")
    ).otherwise(F.col("TransformIt.CLM_COB_TYP_CD")).alias("CLM_COB_TYP_CD")
)

# --------------------------------------------------------------------------------
# Stage: Transformer (CTransformerStage) -- Named just "Transformer" in the JSON
# It has stage variables referencing a function GetFkeyCodes(...) which we assume is already in the namespace.
# The logic sets up "ITSHost" = If RowCount.CLM_COB_TYP_CD[1,1]=='*' then 'Y' else 'N'
# Then "ITSCOBType" = If ITSHost=='Y' then RowCount.CLM_COB_TYP_CD[2,1] else 'NA'
# Then "svClmCobTypCdSk" = ...
# Our output pin has columns:
#   SRC_SYS_CD_SK => "SrcSysCdSk"
#   CLM_ID => RowCount.CLM_ID
#   CLM_COB_TYP_CD_SK => "svClmCobTypCdSk"
#   ALW_AMT => RowCount.ALW_AMT
#   PD_AMT => RowCount.PD_AMT

df_Transformer_in = df_RowCount.alias("RowCount")

def transform_stage_logic(df: DataFrame) -> DataFrame:
    # (1) ITSHost
    #   if RowCount.CLM_COB_TYP_CD.substr(0,1) == '*' => 'Y' else 'N'
    df = df.withColumn(
        "ITSHost",
        F.when(F.substring(F.col("CLM_COB_TYP_CD"),1,1)=="*", "Y").otherwise("N")
    )
    # (2) ITSCOBType
    df = df.withColumn(
        "ITSCOBType",
        F.when(F.col("ITSHost")=="Y", F.substring(F.col("CLM_COB_TYP_CD"),2,1)).otherwise(F.lit("NA"))
    )
    # (3) svClmCobTypCdSk => 
    #   If ITSHost=='N' => GetFkeyCodes('FACETS',0,"CLAIM COB",CLM_COB_TYP_CD,'X')
    #   else => GetFkeyCodes("FIT",0,"CLAIM COB",ITSCOBType,'X')
    df = df.withColumn(
        "svClmCobTypCdSk",
        F.when(
            F.col("ITSHost")=="N",
            GetFkeyCodes(F.lit("FACETS"), F.lit(0), F.lit("CLAIM COB"), F.col("CLM_COB_TYP_CD"), F.lit("X"))
        ).otherwise(
            GetFkeyCodes(F.lit("FIT"), F.lit(0), F.lit("CLAIM COB"), F.col("ITSCOBType"), F.lit("X"))
        )
    )
    return df

df_Transformer = transform_stage_logic(df_Transformer_in)

df_Transformer_out = df_Transformer.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # from job "SrcSysCdSk" (we assume it's also a col or param?)
    F.col("CLM_ID"),
    F.col("svClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
    F.col("ALW_AMT"),
    F.col("PD_AMT")
)

# --------------------------------------------------------------------------------
# Stage: B_CLM_COB (CSeqFileStage)
# We write a .dat file. Must define a schema to avoid inferschema. Then use write_files.
# The file is "B_CLM_COB.LhoFACETS.dat.#RunID#", path "load" => => f"{adls_path}/load/B_CLM_COB.LhoFACETS.dat.{RunID}"

schema_b_clm_cob = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_COB_TYP_CD_SK", StringType(), True),
    StructField("ALW_AMT", DecimalType(38,10), True),
    StructField("PD_AMT", DecimalType(38,10), True)
])

df_B_CLM_COB = df_Transformer_out.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD_SK"),
    F.col("ALW_AMT"),
    F.col("PD_AMT")
)

write_files(
    df_B_CLM_COB,
    f"{adls_path}/load/B_CLM_COB.LhoFACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ClmCobPK (CContainerStage)
# The shared container has 2 inputs: "Transform" and "AllCol"
# from the snapshot stage, we pass df_Transform, df_AllCol with param dictionary
params_ClmCobPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "CurrentDate": get_widget_value('CurrentDate',''),
    "IDSOwner": IDSOwner
}
output_Key = ClmCobPK(df_Transform.alias("Keys"), df_AllCol.alias("AllColOut"), params_ClmCobPK)
df_ClmCobPK_out = output_Key  # This container returns one output link named "Key"

# --------------------------------------------------------------------------------
# Stage: IdsClmCOB (CSeqFileStage)
# We write out "LhoFctsClmCOBExtr.LhoFctsClmCOB.dat.#RunID#" in directory "key"
# Must define schema, then write.

schema_idsclmcob = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38,10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("CLM_COB_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_COB_TYP_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_SK", IntegerType(), True),
    StructField("CLM_COB_LIAB_TYP_CD", StringType(), True),
    StructField("ALW_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("MED_COINS_AMT", DecimalType(38,10), True),
    StructField("MNTL_HLTH_COINS_AMT", DecimalType(38,10), True),
    StructField("PD_AMT", DecimalType(38,10), True),
    StructField("SANC_AMT", DecimalType(38,10), True),
    StructField("COB_CAR_RSN_CD_TX", StringType(), True),
    StructField("COB_CAR_RSN_TX", StringType(), True)
])

write_files(
    df_ClmCobPK_out,
    f"{adls_path}/key/LhoFctsClmCOBExtr.LhoFctsClmCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Select_CLM_COB (CODBCStage)
# Query (output pin) => "SELECT CLM_COB_SK ... FROM #$IDSOwner#.CLM_COB c, #$IDSOwner#.W_FCTS_RCRD_DEL d
#   where d.SUBJ_NM='CLM_ID' and c.SRC_SYS_CD_SK=#SrcSysCdSk# and d.KEY_VAL=c.CLM_ID
#   and c.LAST_UPDT_RUN_CYC_EXCTN_SK <> #CurrRunCycle#"
#
# Since database=unknown, but references #$IDSOwner#. We'll treat it as "IDS" again => use the ids_secret_name, same logic.

delete_query_select = (
    f"SELECT CLM_COB_SK\n"
    f"  FROM {IDSOwner}.CLM_COB c,\n"
    f"             {IDSOwner}.W_FCTS_RCRD_DEL d\n"
    f"WHERE d.SUBJ_NM = 'CLM_ID'\n"
    f"      AND c.SRC_SYS_CD_SK = {SrcSysCdSk}\n"
    f"      AND d.KEY_VAL = c.CLM_ID \n"
    f"      AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {CurrRunCycle}"
)
df_select_clm_cob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", delete_query_select)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CLM_COB (CODBCStage) - the input pin has "SqlInsert" => "DELETE FROM #$IDSOwner#.CLM_COB c WHERE CLM_COB_SK=?"
# We'll merge or do a direct delete for each row in df_select_clm_cob

# Implementation: iterate each row => but we do not define a function. We'll run a DML statement using "execute_dml"
# Because we must do a parameter ? => we can do something like:

rows_to_delete = df_select_clm_cob.select("CLM_COB_SK").collect()
for row in rows_to_delete:
    clm_cob_sk_val = row["CLM_COB_SK"]
    delete_sql = f"DELETE FROM {IDSOwner}.CLM_COB WHERE CLM_COB_SK = {clm_cob_sk_val}"
    execute_dml(delete_sql, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# End of Job (no spark.stop())  
# The script ends here.