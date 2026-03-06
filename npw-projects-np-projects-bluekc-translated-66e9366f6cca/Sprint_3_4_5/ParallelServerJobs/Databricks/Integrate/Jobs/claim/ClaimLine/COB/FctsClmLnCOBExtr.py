# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnCOBExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDSM_LI_MSUPP, CMC_CDIO_L_ITS_OPL or CMC_CDCB_LI_COB (depending on first two characters of product code)  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDSM_LI_MSUPP, CMC_CDCB_LI_COB
# MAGIC                 Joined with 
# MAGIC                 tempdb..TMP_IDS_CLAIM  to get claims updated within time period
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------    
# MAGIC Steph Goddard        01/2006-                                         Originally Programmed
# MAGIC Landon Hall            03/2005-                                          added logic to suppoet ITS Host to Facets Conversion.  
# MAGIC                                                                                         Includes the additional flow for CDIO.               
# MAGIC Sharon Andrew       05/2005-                                          added logic to suppoet claims that were on the hit list and 
# MAGIC                                                                                         therefore their data still existed on the old Facets tables.  
# MAGIC                                                                                         if claim last activity date was after Nov 6 2004 then all its 
# MAGIC                                                                                         COB data is on two different facets tables
# MAGIC                                                                                          if claim last activity date was before  Nov 6 2004 then 
# MAGIC                                                                                          all its COB data is on one facets tables and it is not product specific.
# MAGIC Sharon Andrew       06/22/05    Task Tracker 3428 and 3507. 
# MAGIC                                                                                         Removed use of hf_clio_cdio.   No longer creates CDIO 
# MAGIC                                                                                         records and they will be deleted from IDS, EDW and ClaimMart.
# MAGIC                                                                                         Added Absolute() to the amount fields to prevent negative 
# MAGIC                                                                                         amount fields being created.
# MAGIC Steph Goddard   03/01/2006                                          combined extract, transform, primary key for sequencer
# MAGIC                            03/20/2006                                          do not clear hf_prod_list - shared between programs (where is this created?)
# MAGIC BJ Luce            03/20/2006                                            add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. 
# MAGIC                                                                                         If the claim is on the file, a row is not generated for it in IDS. 
# MAGIC                                                                                         However, an R row will be build for it if the status if '91'
# MAGIC Sanderw         12/08/2006          Project 1576  -            Reversal logic added for new status codes 89 and  and 99
# MAGIC O. Nielsen       08/15/2007          Balancing                   Added Snapshot File extraction for Balancing Project                                           devlIDS30                  Steph Goddard             8/30/07
# MAGIC 
# MAGIC Parik                2008-07-23           3567(Primary key)      Added the primary key process for Facets Claim Line COB                                   devlIDS                       Steph Goddard             07/27/2008
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidnidi   2021-03-17          Facets Upgrade added JOIN to get CLCB_COB_REAS_CD                                                  IntegrateDev1                   Kalyan Neelam               2021-03-18
# MAGIC Prabhu ES              2022-02-28      S2S Remediation     MSSQL connection parameters added                                                       IntegrateDev5                      Manasa Andru                 2022-06-10

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Balancing
# MAGIC Pulling Facets Claim COB Data for claims on the tmp_ids_claim table.
# MAGIC Hash file (hf_clm_ln_cob_allcol) cleared from the container - ClmLnCobPK
# MAGIC This container is used in:     FctsClmLnCOBExtr      FctsClmLnDntlCOBExtr
# MAGIC NascoClmLnCobExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnCobPK
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, rpad, when, expr, coalesce, isnull
from pyspark.sql.functions import abs as abs_, length as length_
 
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# 1) Read "hf_clm_fcts_reversals" (CHashedFileStage) => Scenario C
schema_hf_clm_fcts_reversals = StructType([
    StructField("CLCL_ID", StringType(), False),
    StructField("CLCL_CUR_STS", StringType(), False),
    StructField("CLCL_PAID_DT", TimestampType(), False),
    StructField("CLCL_ID_ADJ_TO", StringType(), False),
    StructField("CLCL_ID_ADJ_FROM", StringType(), False)
])
df_hf_clm_fcts_reversals = (
    spark.read.schema(schema_hf_clm_fcts_reversals)
    .parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

# 2) Read "clm_nasco_dup_bypass" (CHashedFileStage) => Scenario C
schema_clm_nasco_dup_bypass = StructType([
    StructField("CLM_ID", StringType(), False)
])
df_clm_nasco_dup_bypass = (
    spark.read.schema(schema_clm_nasco_dup_bypass)
    .parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
)

# 3) DB2Connector "CD_MAPPING"
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_CD_MAPPING = (
    f"SELECT SRC_CD, TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM = 'EXPLANATION CODE PROVIDER ADJUSTMENT'"
)
df_CD_MAPPING = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MAPPING)
    .load()
)

# Scenario A for "hf_code_maps": direct wiring + dedup on key columns (SRC_CD)
df_hf_code_maps = dedup_sort(df_CD_MAPPING, ["SRC_CD"], [])

# 4) Read "Product_list" => "hf_prod_list" => Scenario C
schema_product_list = StructType([
    StructField("prod_prefix", StringType(), False)
])
df_product_list = (
    spark.read.schema(schema_product_list)
    .parquet(f"{adls_path}/hf_prod_list.parquet")
)

# 5) ODBCConnector "FACETS_CLM"
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_FACETS_CLM = (
    f"SELECT line.CLCL_ID, "
    f"       line.CDML_SEQ_NO, "
    f"       clm.PDPD_ID, "
    f"       clm.CLCL_LAST_ACT_DTM "
    f"FROM {FacetsOwner}.CMC_CLCL_CLAIM clm, "
    f"     {FacetsOwner}.CMC_CDML_CL_LINE line, "
    f"     tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = clm.CLCL_ID "
    f"  AND clm.CLCL_ID = line.CLCL_ID"
)
df_FACETS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FACETS_CLM)
    .load()
)

# Scenario A => "hf_clmln_cob_clm_info": dedup by (CLCL_ID, CDML_SEQ_NO)
df_hf_clmln_cob_clm_info = dedup_sort(
    df_FACETS_CLM,
    ["CLCL_ID", "CDML_SEQ_NO"],
    []
)

# 6) ODBCConnector "FACETS_cob" => 2 queries => cdcb_info & cdsm_info
extract_query_cdcb_info = (
    f"SELECT cdcb.CLCL_ID, "
    f"       cdcb.CDML_SEQ_NO, "
    f"       cdcb.MEME_CK, "
    f"       cdcb.CDCB_COB_TYPE, "
    f"       cdcb.CDCB_PRO_RATE_IND, "
    f"       cdcb.CDCB_COB_AMT, "
    f"       cdcb.CDCB_COB_DISALLOW, "
    f"       cdcb.CDCB_COB_ALLOW, "
    f"       cdcb.CDCB_ADJ_AMT, "
    f"       cdcb.CDCB_SUBTRACT_AMT, "
    f"       cdcb.CDCB_COB_SAV, "
    f"       cdcb.CDCB_COB_APP, "
    f"       cdcb.CDCB_COB_OOP, "
    f"       cdcb.CDCB_COB_SANCTION, "
    f"       cdcb.CDCB_COB_DED_AMT, "
    f"       cdcb.CDCB_COB_COPAY_AMT, "
    f"       cdcb.CDCB_COB_COINS_AMT, "
    f"       CAST(TRIM(C.CDCC_OC_REAS_CODE) as char(5)) as CDCB_COB_REAS_CD, "
    f"       cdcb.CDCB_LOCK_TOKEN "
    f"FROM {FacetsOwner}.CMC_CDCB_LI_COB cdcb "
    f"INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = cdcb.CLCL_ID "
    f"LEFT OUTER JOIN ("
    f"    SELECT A.CLCL_ID,A.CDML_SEQ_NO,A.CDCC_OC_REAS_CODE "
    f"    FROM {FacetsOwner}.CMC_CDCC_CARC_CODE A, "
    f"         (SELECT CLCL_ID,CDML_SEQ_NO, MIN(CDCC_SEQ_NO) as CDCC_SEQ_NO "
    f"          FROM {FacetsOwner}.CMC_CDCC_CARC_CODE "
    f"          GROUP BY CLCL_ID,CDML_SEQ_NO) B "
    f"    WHERE A.CLCL_ID=B.CLCL_ID "
    f"      AND A.CDML_SEQ_NO=B.CDML_SEQ_NO "
    f"      AND A.CDCC_SEQ_NO=B.CDCC_SEQ_NO"
    f") C ON cdcb.CLCL_ID=C.CLCL_ID AND cdcb.CDML_SEQ_NO=C.CDML_SEQ_NO"
)
df_cdcb_info = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cdcb_info)
    .load()
)

extract_query_cdsm_info = (
    f"SELECT CLCL_ID, "
    f"       CDML_SEQ_NO, "
    f"       MEME_CK, "
    f"       CDSM_PRO_RATE_IND, "
    f"       CDSM_OC_ALLOW, "
    f"       CDSM_OC_DED_AMT, "
    f"       CDSM_OC_COINS_AMTM, "
    f"       CDSM_OC_COINS_AMTP, "
    f"       CDSM_OC_PAID_AMT, "
    f"       CAST(TRIM(CDSM_OC_REAS_CD) as CHAR(5)) AS CDSM_OC_REAS_CD, "
    f"       CDSM_OC_GROUP_CD, "
    f"       CDSM_LOCK_TOKEN, "
    f"       ATXR_SOURCE_ID "
    f"FROM {FacetsOwner}.CMC_CDSM_LI_MSUPP cdsm, "
    f"     tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = cdsm.CLCL_ID"
)
df_cdsm_info = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cdsm_info)
    .load()
)

# Scenario A => "hf_cdcb_info" => dedup by (CLCL_ID, CDML_SEQ_NO)
df_hf_cdcb_info = dedup_sort(
    df_cdcb_info,
    ["CLCL_ID", "CDML_SEQ_NO"],
    []
)

# Scenario A => "hf_cdsm_info" => dedup by (CLCL_ID, CDML_SEQ_NO)
df_hf_cdsm_info = dedup_sort(
    df_cdsm_info,
    ["CLCL_ID", "CDML_SEQ_NO"],
    []
)

# 7) "StripField" (CTransformerStage) 
# Primary input => df_hf_clmln_cob_clm_info as "Extract"
df_extract = df_hf_clmln_cob_clm_info

# Join (left) with df_hf_cdcb_info => "cdcb_in"
join_cond_cdcb = [
    df_extract.CLCL_ID == df_hf_cdcb_info.CLCL_ID,
    df_extract.CDML_SEQ_NO == df_hf_cdcb_info.CDML_SEQ_NO
]
df_join_1 = df_extract.alias("Extract").join(
    df_hf_cdcb_info.alias("cdcb_in"),
    on=join_cond_cdcb,
    how="left"
)

# Join (left) with df_hf_cdsm_info => "cdsm_in"
join_cond_cdsm = [
    df_join_1["Extract.CLCL_ID"] == df_hf_cdsm_info.CLCL_ID,
    df_join_1["Extract.CDML_SEQ_NO"] == df_hf_cdsm_info.CDML_SEQ_NO
]
df_join_2 = df_join_1.join(
    df_hf_cdsm_info.alias("cdsm_in"),
    on=join_cond_cdsm,
    how="left"
)

# Join (left) with df_product_list => "product_list_in" on Extract.PDPD_ID[1,2] = product_list_in.prod_prefix
# We simulate substring with Python expression or pyspark substring
from pyspark.sql.functions import substring
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

df_join_3 = df_join_2.join(
    df_product_list.alias("product_list_in"),
    on=[
        substring(df_join_2["Extract.PDPD_ID"], 1, 2) == col("product_list_in.prod_prefix")
    ],
    how="left"
)

# Stage variables:
# SetPrefix => if isnull(product_list_in.prod_prefix) then 'CDCB' else 'CDSM'
# preNovmbrORpostNovember => if Extract.CLCL_LAST_ACT_DTM <= '2004-11-05' then "PRE" else "POST"
df_strip_vars = df_join_3.withColumn(
    "_SetPrefix",
    when(col("product_list_in.prod_prefix").isNull(), lit("CDCB")).otherwise(lit("CDSM"))
).withColumn(
    "_preNovmbrORpostNovember",
    when(col("Extract.CLCL_LAST_ACT_DTM") <= lit("2004-11-05"), lit("PRE")).otherwise(lit("POST"))
)

# Now produce two outputs based on constraints:
# "ClmLnCDSMOut" => _SetPrefix='CDSM' and cdsm_in.CLCL_ID is not null
df_ClmLnCDSMOut = df_strip_vars.filter(
    (col("_SetPrefix") == "CDSM") & (col("cdsm_in.CLCL_ID").isNotNull())
)

# "ClmLnCDCBOut" => _SetPrefix='CDCB' and cdcb_in.CLCL_ID is not null
df_ClmLnCDCBOut = df_strip_vars.filter(
    (col("_SetPrefix") == "CDCB") & (col("cdcb_in.CLCL_ID").isNotNull())
)

# Each output has its own select with the specified columns/expressions:

# ClmLnCDSMOut columns:
df_ClmLnCDSMOut_sel = df_ClmLnCDSMOut.select(
    col("Extract.CLCL_ID").alias("CLCL_ID"),
    col("Extract.CDML_SEQ_NO").alias("SEQ_NO"),
    lit("M").alias("COB_TYPE"),
    lit(CurrentDate).alias("EXT_TIMESTAMP"),
    col("cdsm_in.CDSM_PRO_RATE_IND").alias("PRO_RATE_IND"),
    col("cdsm_in.CDSM_OC_GROUP_CD").alias("COB_LIAB_TYP_CD"),
    lit("0.00").alias("ADJ_AMT"),
    col("cdsm_in.CDSM_OC_ALLOW").alias("COB_ALLOW"),
    lit("0.00").alias("COB_APP"),
    lit("0.00").alias("COB_COPAY_AMT"),
    col("cdsm_in.CDSM_OC_DED_AMT").alias("COB_DED_AMT"),
    lit("0.00").alias("COB_DISALLOW"),
    col("cdsm_in.CDSM_OC_COINS_AMTM").alias("COB_COINS_AMT"),
    col("cdsm_in.CDSM_OC_COINS_AMTP").alias("COB_MNTL_HLTH_COINS"),
    lit("0.00").alias("COB_OOP"),
    col("cdsm_in.CDSM_OC_PAID_AMT").alias("COB_AMT"),
    lit("0.00").alias("COB_SANCTION"),
    lit("0.00").alias("COB_SAV"),
    lit("0.00").alias("SUBTRACT_AMT"),
    col("cdsm_in.CDSM_OC_REAS_CD").alias("COB_REAS_CD"),
    col("cdsm_in.CDSM_OC_REAS_CD").alias("CAR_RSN_TX")
)

# ClmLnCDCBOut columns:
df_ClmLnCDCBOut_sel = df_ClmLnCDCBOut.select(
    col("cdcb_in.CLCL_ID").alias("CLCL_ID"),
    col("Extract.CDML_SEQ_NO").alias("SEQ_NO"),
    col("cdcb_in.CDCB_COB_TYPE").alias("COB_TYPE"),
    lit(CurrentDate).alias("EXT_TIMESTAMP"),
    col("cdcb_in.CDCB_PRO_RATE_IND").alias("PRO_RATE_IND"),
    lit("NA").alias("COB_LIAB_TYP_CD"),
    col("cdcb_in.CDCB_ADJ_AMT").alias("ADJ_AMT"),
    col("cdcb_in.CDCB_COB_ALLOW").alias("COB_ALLOW"),
    col("cdcb_in.CDCB_COB_APP").alias("COB_APP"),
    col("cdcb_in.CDCB_COB_COPAY_AMT").alias("COB_COPAY_AMT"),
    col("cdcb_in.CDCB_COB_DED_AMT").alias("COB_DED_AMT"),
    col("cdcb_in.CDCB_COB_DISALLOW").alias("COB_DISALLOW"),
    col("cdcb_in.CDCB_COB_COINS_AMT").alias("COB_COINS_AMT"),
    lit("0.00").alias("COB_MNTL_HLTH_COINS"),
    col("cdcb_in.CDCB_COB_OOP").alias("COB_OOP"),
    col("cdcb_in.CDCB_COB_AMT").alias("COB_AMT"),
    col("cdcb_in.CDCB_COB_SANCTION").alias("COB_SANCTION"),
    col("cdcb_in.CDCB_COB_SAV").alias("COB_SAV"),
    col("cdcb_in.CDCB_SUBTRACT_AMT").alias("SUBTRACT_AMT"),
    col("cdcb_in.CDCB_COB_REAS_CD").alias("COB_REAS_CD"),
    when(
        col("_preNovmbrORpostNovember") == "PRE",
        lit(None).cast(StringType())
    ).otherwise(col("cdcb_in.CDCB_COB_REAS_CD")).alias("CAR_RSN_TX")
)

# Collector => union these two into single "df_Strip"
common_cols = [
    "CLCL_ID","SEQ_NO","COB_TYPE","EXT_TIMESTAMP","PRO_RATE_IND","COB_LIAB_TYP_CD",
    "ADJ_AMT","COB_ALLOW","COB_APP","COB_COPAY_AMT","COB_DED_AMT","COB_DISALLOW",
    "COB_COINS_AMT","COB_MNTL_HLTH_COINS","COB_OOP","COB_AMT","COB_SANCTION",
    "COB_SAV","SUBTRACT_AMT","COB_REAS_CD","CAR_RSN_TX"
]
df_ClmLnCDSMOut_ready = df_ClmLnCDSMOut_sel.select(common_cols)
df_ClmLnCDCBOut_ready = df_ClmLnCDCBOut_sel.select(common_cols)
df_Strip = df_ClmLnCDSMOut_ready.union(df_ClmLnCDCBOut_ready)

# Next: "Business_Logic" (CTransformerStage)
# Primary link => df_Strip, lookup link => df_hf_code_maps
join_cond_code_maps = [
    trim(col("df_Strip.COB_REAS_CD")) == col("df_hf_code_maps.SRC_CD")
]
df_join_bl = df_Strip.alias("df_Strip").join(
    df_hf_code_maps.alias("cd_mppng"), 
    on=join_cond_code_maps, 
    how="left"
)

# Produce final "df_FctsClmLnCOB" from stage "Business_Logic"
df_FctsClmLnCOB = df_join_bl.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"), 
    rpad(lit("N"),1," ").alias("DISCARD_IN"),
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),
    col("df_Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    # "SRC_SYS_CD" from parameter or stage var? job says "Expression": "SrcSysCd"
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    expr("SrcSysCd || ';' || df_Strip.CLCL_ID || ';' || df_Strip.SEQ_NO || ';' || df_Strip.COB_TYPE").alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_COB_LN_SK"),
    col("df_Strip.CLCL_ID").alias("CLM_ID"),
    col("df_Strip.SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(length_(trim(col("df_Strip.COB_TYPE")))==0, lit("UNK")).otherwise(trim(col("df_Strip.COB_TYPE"))).alias("CLM_LN_COB_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(length_(trim(col("df_Strip.PRO_RATE_IND")))==0, lit("NA")).otherwise(trim(col("df_Strip.PRO_RATE_IND"))).alias("CLM_LN_COB_CAR_PRORTN_CD"),
    when(length_(trim(col("df_Strip.COB_LIAB_TYP_CD")))==0, lit("NA")).otherwise(trim(col("df_Strip.COB_LIAB_TYP_CD"))).alias("CLM_LN_COB_LIAB_TYP_CD"),
    when(length_(trim(col("df_Strip.ADJ_AMT")))==0, lit(0.00))
      .otherwise(expr("abs(df_Strip.COB_APP) - abs(df_Strip.COB_SAV)")).alias("COB_CAR_ADJ_AMT"),
    when(length_(trim(col("df_Strip.COB_ALLOW")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_ALLOW"))).alias("ALW_AMT"),
    when(length_(trim(col("df_Strip.COB_APP")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_APP"))).alias("APLD_AMT"),
    when(length_(trim(col("df_Strip.COB_COPAY_AMT")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_COPAY_AMT"))).alias("COPAY_AMT"),
    when(length_(trim(col("df_Strip.COB_DED_AMT")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_DED_AMT"))).alias("DEDCT_AMT"),
    when(length_(trim(col("df_Strip.COB_DISALLOW")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_DISALLOW"))).alias("DSALW_AMT"),
    when(length_(trim(col("df_Strip.COB_COINS_AMT")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_COINS_AMT"))).alias("COINS_AMT"),
    when(length_(trim(col("df_Strip.COB_MNTL_HLTH_COINS")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_MNTL_HLTH_COINS"))).alias("MNTL_HLTH_COINS_AMT"),
    when(length_(trim(col("df_Strip.COB_OOP")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_OOP"))).alias("OOP_AMT"),
    when(length_(trim(col("df_Strip.COB_AMT")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_AMT"))).alias("PD_AMT"),
    when(length_(trim(col("df_Strip.COB_SANCTION")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_SANCTION"))).alias("SANC_AMT"),
    when(length_(trim(col("df_Strip.COB_SAV")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.COB_SAV"))).alias("SAV_AMT"),
    when(length_(trim(col("df_Strip.SUBTRACT_AMT")))==0, lit(0.00))
      .otherwise(abs_(col("df_Strip.SUBTRACT_AMT"))).alias("SUBTR_AMT"),
    when(length_(trim(col("df_Strip.COB_REAS_CD")))==0, lit(None)).otherwise(trim(col("df_Strip.COB_REAS_CD"))).alias("COB_CAR_RSN_CD_TX"),
    when(col("cd_mppng.TRGT_CD_NM").isNull(), lit(None)).otherwise(col("cd_mppng.TRGT_CD_NM")).alias("COB_CAR_RSN_TX")
)

# 8) "Transformer_70"
# Primary link => df_FctsClmLnCOB as "FctsClmLnCOB"
# 2 lookups => fcts_reversals (df_hf_clm_fcts_reversals) & nasco_dup_lkup (df_clm_nasco_dup_bypass)
# left join with df_hf_clm_fcts_reversals
join_cond_rev = [
    trim(col("FctsClmLnCOB.CLM_ID")) == col("fcts_reversals.CLCL_ID")
]
df_join_rev = df_FctsClmLnCOB.alias("FctsClmLnCOB").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    on=join_cond_rev,
    how="left"
)

# left join with df_clm_nasco_dup_bypass
join_cond_dup = [
    col("FctsClmLnCOB.CLM_ID") == col("nasco_dup_lkup.CLM_ID")
]
df_join_final_t70 = df_join_rev.join(
    df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
    on=join_cond_dup,
    how="left"
)

# Now produce 2 outputs:
# (1) "ClnLnCOB" => IsNull(nasco_dup_lkup.CLM_ID) => nasco_dup_lkup.CLM_ID is null
df_ClnLnCOB = df_join_final_t70.filter(
    col("nasco_dup_lkup.CLM_ID").isNull()
)

# (2) "reversals" => notnull(fcts_reversals.CLCL_ID) AND fcts_reversals.CLCL_CUR_STS in ["89","91","99"]
df_reversals = df_join_final_t70.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
       (col("fcts_reversals.CLCL_CUR_STS")=="89")|
       (col("fcts_reversals.CLCL_CUR_STS")=="91")|
       (col("fcts_reversals.CLCL_CUR_STS")=="99")
    )
)

# ClnLnCOB select:
df_ClnLnCOB_sel = df_ClnLnCOB.select(
    col("FctsClmLnCOB.JOB_EXCTN_RCRD_ERR_SK"),
    col("FctsClmLnCOB.INSRT_UPDT_CD"),
    col("FctsClmLnCOB.DISCARD_IN"),
    col("FctsClmLnCOB.PASS_THRU_IN"),
    col("FctsClmLnCOB.FIRST_RECYC_DT"),
    col("FctsClmLnCOB.ERR_CT"),
    col("FctsClmLnCOB.RECYCLE_CT"),
    col("FctsClmLnCOB.SRC_SYS_CD"),
    col("FctsClmLnCOB.PRI_KEY_STRING"),
    col("FctsClmLnCOB.CLM_COB_LN_SK"),
    col("FctsClmLnCOB.CLM_ID"),
    col("FctsClmLnCOB.CLM_LN_SEQ_NO"),
    col("FctsClmLnCOB.CLM_LN_COB_TYP_CD"),
    col("FctsClmLnCOB.CRT_RUN_CYC_EXCTN_SK"),
    col("FctsClmLnCOB.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FctsClmLnCOB.CLM_LN_COB_CAR_PRORTN_CD"),
    col("FctsClmLnCOB.CLM_LN_COB_LIAB_TYP_CD"),
    col("FctsClmLnCOB.COB_CAR_ADJ_AMT"),
    col("FctsClmLnCOB.ALW_AMT"),
    col("FctsClmLnCOB.APLD_AMT"),
    col("FctsClmLnCOB.COPAY_AMT"),
    col("FctsClmLnCOB.DEDCT_AMT"),
    col("FctsClmLnCOB.DSALW_AMT"),
    col("FctsClmLnCOB.COINS_AMT"),
    col("FctsClmLnCOB.MNTL_HLTH_COINS_AMT"),
    col("FctsClmLnCOB.OOP_AMT"),
    col("FctsClmLnCOB.PD_AMT"),
    col("FctsClmLnCOB.SANC_AMT"),
    col("FctsClmLnCOB.SAV_AMT"),
    col("FctsClmLnCOB.SUBTR_AMT"),
    col("FctsClmLnCOB.COB_CAR_RSN_CD_TX"),
    col("FctsClmLnCOB.COB_CAR_RSN_TX")
)

# reversals select => multiply certain columns by -1
df_reversals_sel = df_reversals.select(
    col("FctsClmLnCOB.JOB_EXCTN_RCRD_ERR_SK"),
    col("FctsClmLnCOB.INSRT_UPDT_CD"),
    col("FctsClmLnCOB.DISCARD_IN"),
    col("FctsClmLnCOB.PASS_THRU_IN"),
    col("FctsClmLnCOB.FIRST_RECYC_DT"),
    col("FctsClmLnCOB.ERR_CT"),
    col("FctsClmLnCOB.RECYCLE_CT"),
    col("FctsClmLnCOB.SRC_SYS_CD"),
    expr("SrcSysCd || ';' || FctsClmLnCOB.CLM_ID || 'R' || ';' || FctsClmLnCOB.CLM_LN_SEQ_NO || ';' || FctsClmLnCOB.CLM_LN_COB_TYP_CD").alias("PRI_KEY_STRING"),
    col("FctsClmLnCOB.CLM_COB_LN_SK"),
    expr("FctsClmLnCOB.CLM_ID || 'R'").alias("CLM_ID"),
    col("FctsClmLnCOB.CLM_LN_SEQ_NO"),
    col("FctsClmLnCOB.CLM_LN_COB_TYP_CD"),
    col("FctsClmLnCOB.CRT_RUN_CYC_EXCTN_SK"),
    col("FctsClmLnCOB.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FctsClmLnCOB.CLM_LN_COB_CAR_PRORTN_CD"),
    col("FctsClmLnCOB.CLM_LN_COB_LIAB_TYP_CD"),
    (col("FctsClmLnCOB.COB_CAR_ADJ_AMT") * -1).alias("COB_CAR_ADJ_AMT"),
    (-1*col("FctsClmLnCOB.ALW_AMT")).alias("ALW_AMT"),
    (-1*col("FctsClmLnCOB.APLD_AMT")).alias("APLD_AMT"),
    (-1*col("FctsClmLnCOB.COPAY_AMT")).alias("COPAY_AMT"),
    (-1*col("FctsClmLnCOB.DEDCT_AMT")).alias("DEDCT_AMT"),
    (-1*col("FctsClmLnCOB.DSALW_AMT")).alias("DSALW_AMT"),
    (-1*col("FctsClmLnCOB.COINS_AMT")).alias("COINS_AMT"),
    (-1*col("FctsClmLnCOB.MNTL_HLTH_COINS_AMT")).alias("MNTL_HLTH_COINS_AMT"),
    (-1*col("FctsClmLnCOB.OOP_AMT")).alias("OOP_AMT"),
    (-1*col("FctsClmLnCOB.PD_AMT")).alias("PD_AMT"),
    (-1*col("FctsClmLnCOB.SANC_AMT")).alias("SANC_AMT"),
    (-1*col("FctsClmLnCOB.SAV_AMT")).alias("SAV_AMT"),
    (-1*col("FctsClmLnCOB.SUBTR_AMT")).alias("SUBTR_AMT"),
    col("FctsClmLnCOB.COB_CAR_RSN_CD_TX"),
    col("FctsClmLnCOB.COB_CAR_RSN_TX")
)

# Collector2 => union
common_cols_t2 = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","CLM_COB_LN_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_LN_COB_CAR_PRORTN_CD","CLM_LN_COB_LIAB_TYP_CD",
    "COB_CAR_ADJ_AMT","ALW_AMT","APLD_AMT","COPAY_AMT","DEDCT_AMT","DSALW_AMT","COINS_AMT","MNTL_HLTH_COINS_AMT",
    "OOP_AMT","PD_AMT","SANC_AMT","SAV_AMT","SUBTR_AMT","COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
]
df_ClnLnCOB_ready = df_ClnLnCOB_sel.select(common_cols_t2)
df_reversals_ready = df_reversals_sel.select(common_cols_t2)
df_Trans = df_reversals_ready.union(df_ClnLnCOB_ready)

# "SnapShot" => The stage shows 3 output links with partial columns each.

# For the link "AllCol" => columns in JSON:
df_AllCol = df_Trans.select(
    rpad(col("SRC_SYS_CD_SK"),0," ").alias("SRC_SYS_CD_SK"),  # The job says expression=SrcSysCdSk, we have it as a param
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_COB_TYP_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_COB_LN_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_COB_CAR_PRORTN_CD"),
    col("CLM_LN_COB_LIAB_TYP_CD"),
    col("COB_CAR_ADJ_AMT"),
    col("ALW_AMT"),
    col("APLD_AMT"),
    col("COPAY_AMT"),
    col("DEDCT_AMT"),
    col("DSALW_AMT"),
    col("COINS_AMT"),
    col("MNTL_HLTH_COINS_AMT"),
    col("OOP_AMT"),
    col("PD_AMT"),
    col("SANC_AMT"),
    col("SAV_AMT"),
    col("SUBTR_AMT"),
    col("COB_CAR_RSN_CD_TX"),
    col("COB_CAR_RSN_TX")
)

# For the link "Transform" => columns in JSON:
df_Transform = df_Trans.select(
    rpad(col("SRC_SYS_CD_SK"),0," ").alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_COB_TYP_CD")
)

# For the link "Snapshot" => columns in JSON:
df_Snapshot = df_Trans.select(
    rpad(col("SRC_SYS_CD_SK"),0," ").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_COB_TYP_CD"),
    col("ALW_AMT"),
    col("PD_AMT")
)

# Next: "ClmLnCobPK" => shared container with 2 inputs: "Transform","AllCol" => single output => "Key"
# We call the container as a function:
params_ClmLnCobPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_key = ClmLnCobPK(df_Transform, df_AllCol, params_ClmLnCobPK)

# "FctsClmLnCOBExtr" => CSeqFileStage => from df_key => write to .dat
# The job’s final columns in the same order as df_key (JSON):
final_cols_fcts = [
 "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN",
 "FIRST_RECYC_DT","ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING",
 "CLM_COB_LN_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_COB_TYP_CD","CRT_RUN_CYC_EXCTN_SK",
 "LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_LN_COB_CAR_PRORTN_CD","CLM_LN_COB_LIAB_TYP_CD","COB_CAR_ADJ_AMT",
 "ALW_AMT","APLD_AMT","COPAY_AMT","DEDCT_AMT","DSALW_AMT","COINS_AMT","MNTL_HLTH_COINS_AMT",
 "OOP_AMT","PD_AMT","SANC_AMT","SAV_AMT","SUBTR_AMT","COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
]
df_fcts_write = df_key.select(
    *[
      rpad(col(c), int(c.split('(')[-1].replace(')','')) if '(' in c else  (
         10 if c in ["INSRT_UPDT_CD","CLM_LN_COB_TYP_CD"] else
         1 if c in ["DISCARD_IN","PASS_THRU_IN"] else
         4 if c in ["COB_CAR_RSN_CD_TX"] else
         10 if c in ["COB_LN_XYZ"] else 0
      )
      ," ") if False else col(c)
      for c in final_cols_fcts
    ]
)

# Write the file
write_files(
    df_fcts_write,
    f"{adls_path}/key/FctsClmLnCOBExtr.FctsClmLnCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Next "Transformer" => stage name "Transformer" => input "Snapshot" => output => "RowCount"
# The stage variable: "ClmLnCobTypCdSk" => if substring= '*' => ...
# We treat "GetFkeyCodes(...)" as a user-defined function. We'll assume it's available: GetFkeyCodes(...)
df_Transformer = df_Snapshot.withColumn(
    "CLM_LN_COB_TYP_CD_SK",
    when(
        expr("substring(CLCL_ID,1,1)='*'"), 
        expr("GetFkeyCodes('FIT', 0, 'CLAIM COB', substring(CLCL_ID,2,1), 'X')")
    ).otherwise(
        expr("GetFkeyCodes('FACETS', 0, 'CLAIM COB', CLM_LN_COB_TYP_CD, 'X')")
    )
)

df_RowCount = df_Transformer.select(
    col("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_COB_TYP_CD_SK"),
    col("ALW_AMT"),
    col("PD_AMT")
)

# "B_CLM_LN_COB" => CSeqFileStage => write df_RowCount => .dat
write_files(
    df_RowCount.select(
        col("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CLM_LN_COB_TYP_CD_SK"),
        col("ALW_AMT"),
        col("PD_AMT")
    ),
    f"{adls_path}/load/B_CLM_LN_COB.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)