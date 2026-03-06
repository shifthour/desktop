# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     LhofFctsClmLnCOBExtr
# MAGIC CALLED BY:  LhoFctsClmExtr1Seq
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
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 	Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263731                                         Original Programming                                              IntegrateDev2                   Jaideep Mankala       10/09/2020
# MAGIC 
# MAGIC Goutham K               2021-07-13                      US-386524                                  Implemented Facets Upgrade REAS_CD changes            IntegrateDev2                    Jeyaprasanna            2021-07-14
# MAGIC                                                                                                                 to mke the job compatable to run pointing to SYBATCH        
# MAGIC 	
# MAGIC Prabhu ES              2022-02-26       S2S Remediation      MSSQL connection parameters added                                                           IntegrateDev5                  Manasa Andru                2022-06-09

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnCobPK
# COMMAND ----------

# Retrieve job parameters
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')

# Read from hashed file "hf_clm_fcts_reversals" (Scenario C: read parquet)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

# Read from hashed file "clm_nasco_dup_bypass" (Scenario C: read parquet)
df_clm_nasco_dup_bypass = (
    spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
)

# Connect to IDS database for "CD_MAPPING"
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_CD_MAPPING = (
    f"SELECT SRC_CD as SRC_CD, TRGT_CD_NM as TRGT_CD_NM "
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

# Scenario A intermediate hashed file "hf_code_maps" between "CD_MAPPING" and "Business_Logic"
# Deduplicate by primary key SRC_CD
df_hf_code_maps = dedup_sort(df_CD_MAPPING, ["SRC_CD"], [])

# Read from hashed file "Product_list" (Scenario C: read parquet)
df_Product_list = (
    spark.read.parquet(f"{adls_path}/hf_prod_list.parquet")
)

# Connect to LhoFacetsStg for "FACETS_cob" (two outputs: cdcb_info, cdsm_info)
jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstg_secret_name)

extract_query_cdcb_info = (
    f"SELECT cdcb.CLCL_ID,cdcb.CDML_SEQ_NO,cdcb.MEME_CK,cdcb.CDCB_COB_TYPE,cdcb.CDCB_PRO_RATE_IND,"
    f"cdcb.CDCB_COB_AMT,cdcb.CDCB_COB_DISALLOW,cdcb.CDCB_COB_ALLOW,cdcb.CDCB_ADJ_AMT,cdcb.CDCB_SUBTRACT_AMT,"
    f"cdcb.CDCB_COB_SAV,cdcb.CDCB_COB_APP,cdcb.CDCB_COB_OOP,cdcb.CDCB_COB_SANCTION,cdcb.CDCB_COB_DED_AMT,"
    f"cdcb.CDCB_COB_COPAY_AMT,cdcb.CDCB_COB_COINS_AMT,C.CDCC_OC_REAS_CODE AS CDCB_COB_REAS_CD,cdcb.CDCB_LOCK_TOKEN "
    f"FROM {lhofacetsstg_secret_name}.CMC_CDCB_LI_COB cdcb "
    f"INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = cdcb.CLCL_ID "
    f"LEFT OUTER JOIN (SELECT A.CLCL_ID,A.CDML_SEQ_NO, A.CDCC_OC_REAS_CODE "
    f"                 FROM {lhofacetsstg_secret_name}.CMC_CDCC_CARC_CODE A , "
    f"                      (SELECT CLCL_ID,CDML_SEQ_NO, MIN(CDCC_SEQ_NO) as CDCC_SEQ_NO "
    f"                       FROM {lhofacetsstg_secret_name}.CMC_CDCC_CARC_CODE "
    f"                       GROUP BY CLCL_ID,CDML_SEQ_NO) B "
    f"                 WHERE A.CLCL_ID = B.CLCL_ID AND A.CDML_SEQ_NO=B.CDML_SEQ_NO AND A.CDCC_SEQ_NO=B.CDCC_SEQ_NO) C "
    f"ON  cdcb.CLCL_ID=C.CLCL_ID AND cdcb.CDML_SEQ_NO = C.CDML_SEQ_NO"
)
df_FACETS_cob_cdcb_info = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_cdcb_info)
    .load()
)

extract_query_cdsm_info = (
    f"SELECT CLCL_ID,CDML_SEQ_NO,MEME_CK,CDSM_PRO_RATE_IND,CDSM_OC_ALLOW,CDSM_OC_DED_AMT,"
    f"CDSM_OC_COINS_AMTM,CDSM_OC_COINS_AMTP,CDSM_OC_PAID_AMT,CDSM_OC_REAS_CD,CDSM_OC_GROUP_CD,"
    f"CDSM_LOCK_TOKEN,ATXR_SOURCE_ID "
    f"FROM {lhofacetsstg_secret_name}.CMC_CDSM_LI_MSUPP cdsm,  tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = cdsm.CLCL_ID"
)
df_FACETS_cob_cdsm_info = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_cdsm_info)
    .load()
)

# Scenario A intermediate hashed file "hf_cdcb_info" with input from df_FACETS_cob_cdcb_info, dedupe on (CLCL_ID, CDML_SEQ_NO)
df_hf_cdcb_info = dedup_sort(df_FACETS_cob_cdcb_info, ["CLCL_ID", "CDML_SEQ_NO"], [])

# Scenario A intermediate hashed file "hf_cdsm_info" with input from df_FACETS_cob_cdsm_info, dedupe on (CLCL_ID, CDML_SEQ_NO)
df_hf_cdsm_info = dedup_sort(df_FACETS_cob_cdsm_info, ["CLCL_ID", "CDML_SEQ_NO"], [])

# "FACETS_CLM" stage
extract_query_facets_clm = (
    f"SELECT line.CLCL_ID, "
    f"       line.CDML_SEQ_NO, "
    f"       clm.PDPD_ID, "
    f"       clm.CLCL_LAST_ACT_DTM "
    f"FROM {lhofacetsstg_secret_name}.CMC_CLCL_CLAIM clm, "
    f"     {lhofacetsstg_secret_name}.CMC_CDML_CL_LINE line, "
    f"     tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = clm.CLCL_ID "
    f"  AND clm.CLCL_ID = line.CLCL_ID"
)
df_FACETS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_facets_clm)
    .load()
)

# Scenario A intermediate hashed file "hf_clmln_cob_clm_info" with input from df_FACETS_CLM, dedupe on (CLCL_ID, CDML_SEQ_NO)
df_hf_clmln_cob_clm_info = dedup_sort(df_FACETS_CLM, ["CLCL_ID", "CDML_SEQ_NO"], [])

# Now implement the "StripField" Transformer stage logic
# Primary link: df_hf_clmln_cob_clm_info (alias "Extract")
# Lookup links: df_hf_cdcb_info (alias "cdcb_in"), df_hf_cdsm_info (alias "cdsm_in"), df_Product_list (alias "product_list_in")
df_StripField_merged = (
    df_hf_clmln_cob_clm_info.alias("Extract")
    .join(
        df_hf_cdcb_info.alias("cdcb_in"),
        [
            F.col("Extract.CLCL_ID") == F.col("cdcb_in.CLCL_ID"),
            F.col("Extract.CDML_SEQ_NO") == F.col("cdcb_in.CDML_SEQ_NO"),
        ],
        "left",
    )
    .join(
        df_hf_cdsm_info.alias("cdsm_in"),
        [
            F.col("Extract.CLCL_ID") == F.col("cdsm_in.CLCL_ID"),
            F.col("Extract.CDML_SEQ_NO") == F.col("cdsm_in.CDML_SEQ_NO"),
        ],
        "left",
    )
    .join(
        df_Product_list.alias("product_list_in"),
        F.col("Extract.PDPD_ID").substr(F.lit(1), F.lit(2)) == F.col("product_list_in.prod_prefix"),
        "left",
    )
    .withColumn(
        "SetPrefix",
        F.when(F.col("product_list_in.prod_prefix").isNull(), F.lit("CDCB")).otherwise(F.lit("CDSM")),
    )
    .withColumn(
        "preNovmbrORpostNovember",
        F.when(F.col("Extract.CLCL_LAST_ACT_DTM") <= F.lit("2004-11-05"), F.lit("PRE")).otherwise(F.lit("POST")),
    )
)

# Build df_ClmLnCDSMOut (constraint: SetPrefix='CDSM' AND cdsm_in.CLCL_ID not null)
df_ClmLnCDSMOut = df_StripField_merged.filter(
    (F.col("SetPrefix") == "CDSM") & (F.col("cdsm_in.CLCL_ID").isNotNull())
).select(
    F.col("Extract.CLCL_ID").alias("CLCL_ID"),
    F.col("Extract.CDML_SEQ_NO").alias("SEQ_NO"),
    F.lit("M").alias("COB_TYPE"),
    F.lit(CurrentDate).alias("EXT_TIMESTAMP"),
    F.col("cdsm_in.CDSM_PRO_RATE_IND").alias("PRO_RATE_IND"),
    F.col("cdsm_in.CDSM_OC_GROUP_CD").alias("COB_LIAB_TYP_CD"),
    F.lit("0.00").alias("ADJ_AMT"),
    F.col("cdsm_in.CDSM_OC_ALLOW").alias("COB_ALLOW"),
    F.lit("0.00").alias("COB_APP"),
    F.lit("0.00").alias("COB_COPAY_AMT"),
    F.col("cdsm_in.CDSM_OC_DED_AMT").alias("COB_DED_AMT"),
    F.lit("0.00").alias("COB_DISALLOW"),
    F.col("cdsm_in.CDSM_OC_COINS_AMTM").alias("COB_COINS_AMT"),
    F.col("cdsm_in.CDSM_OC_COINS_AMTP").alias("COB_MNTL_HLTH_COINS"),
    F.lit("0.00").alias("COB_OOP"),
    F.col("cdsm_in.CDSM_OC_PAID_AMT").alias("COB_AMT"),
    F.lit("0.00").alias("COB_SANCTION"),
    F.lit("0.00").alias("COB_SAV"),
    F.lit("0.00").alias("SUBTRACT_AMT"),
    F.col("cdsm_in.CDSM_OC_REAS_CD").alias("COB_REAS_CD"),
    F.col("cdsm_in.CDSM_OC_REAS_CD").alias("CAR_RSN_TX"),
)

# Build df_ClmLnCDCBOut (constraint: SetPrefix='CDCB' AND cdcb_in.CLCL_ID not null)
df_ClmLnCDCBOut = df_StripField_merged.filter(
    (F.col("SetPrefix") == "CDCB") & (F.col("cdcb_in.CLCL_ID").isNotNull())
).select(
    F.col("cdcb_in.CLCL_ID").alias("CLCL_ID"),
    F.col("Extract.CDML_SEQ_NO").alias("SEQ_NO"),
    F.col("cdcb_in.CDCB_COB_TYPE").alias("COB_TYPE"),
    F.lit(CurrentDate).alias("EXT_TIMESTAMP"),
    F.col("cdcb_in.CDCB_PRO_RATE_IND").alias("PRO_RATE_IND"),
    F.lit("NA").alias("COB_LIAB_TYP_CD"),
    F.col("cdcb_in.CDCB_ADJ_AMT").alias("ADJ_AMT"),
    F.col("cdcb_in.CDCB_COB_ALLOW").alias("COB_ALLOW"),
    F.col("cdcb_in.CDCB_COB_APP").alias("COB_APP"),
    F.col("cdcb_in.CDCB_COB_COPAY_AMT").alias("COB_COPAY_AMT"),
    F.col("cdcb_in.CDCB_COB_DED_AMT").alias("COB_DED_AMT"),
    F.col("cdcb_in.CDCB_COB_DISALLOW").alias("COB_DISALLOW"),
    F.col("cdcb_in.CDCB_COB_COINS_AMT").alias("COB_COINS_AMT"),
    F.lit("0.00").alias("COB_MNTL_HLTH_COINS"),
    F.col("cdcb_in.CDCB_COB_OOP").alias("COB_OOP"),
    F.col("cdcb_in.CDCB_COB_AMT").alias("COB_AMT"),
    F.col("cdcb_in.CDCB_COB_SANCTION").alias("COB_SANCTION"),
    F.col("cdcb_in.CDCB_COB_SAV").alias("COB_SAV"),
    F.col("cdcb_in.CDCB_SUBTRACT_AMT").alias("SUBTRACT_AMT"),
    F.col("cdcb_in.CDCB_COB_REAS_CD").alias("COB_REAS_CD"),
    F.when(
        F.col("preNovmbrORpostNovember") == "PRE", F.lit(None)
    ).otherwise(F.col("cdcb_in.CDCB_COB_REAS_CD")).alias("CAR_RSN_TX"),
)

# Collector stage to combine df_ClmLnCDSMOut and df_ClmLnCDCBOut
df_Strip = df_ClmLnCDSMOut.unionByName(df_ClmLnCDCBOut)

# "Business_Logic" transformer
# Primary link: df_Strip
# Lookup link: df_hf_code_maps => join on trim(Strip.COB_REAS_CD) = cd_mppng.SRC_CD
df_BusinessLogic = (
    df_Strip.alias("Strip")
    .join(
        df_hf_code_maps.alias("cd_mppng"),
        F.trim(F.col("Strip.COB_REAS_CD")) == F.col("cd_mppng.SRC_CD"),
        "left",
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("Strip.CLCL_ID"), F.lit(";"), F.col("Strip.SEQ_NO"), F.lit(";"), F.col("Strip.COB_TYPE")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_COB_LN_SK"),
        F.col("Strip.CLCL_ID").alias("CLM_ID"),
        F.col("Strip.SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.when(F.length(F.trim(F.col("Strip.COB_TYPE"))) == 0, F.lit("UNK")).otherwise(F.trim(F.col("Strip.COB_TYPE"))).alias("CLM_LN_COB_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.length(F.trim(F.col("Strip.PRO_RATE_IND"))) == 0, F.lit("NA")).otherwise(F.trim(F.col("Strip.PRO_RATE_IND"))).alias("CLM_LN_COB_CAR_PRORTN_CD"),
        F.when(F.length(F.trim(F.col("Strip.COB_LIAB_TYP_CD"))) == 0, F.lit("NA")).otherwise(F.trim(F.col("Strip.COB_LIAB_TYP_CD"))).alias("CLM_LN_COB_LIAB_TYP_CD"),
        F.when(F.length(F.trim(F.col("Strip.ADJ_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_APP")) - F.abs(F.col("Strip.COB_SAV"))).alias("COB_CAR_ADJ_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_ALLOW"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_ALLOW"))).alias("ALW_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_APP"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_APP"))).alias("APLD_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_COPAY_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_COPAY_AMT"))).alias("COPAY_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_DED_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_DED_AMT"))).alias("DEDCT_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_DISALLOW"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_DISALLOW"))).alias("DSALW_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_COINS_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_COINS_AMT"))).alias("COINS_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_MNTL_HLTH_COINS"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_MNTL_HLTH_COINS"))).alias("MNTL_HLTH_COINS_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_OOP"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_OOP"))).alias("OOP_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_AMT"))).alias("PD_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_SANCTION"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_SANCTION"))).alias("SANC_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_SAV"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.COB_SAV"))).alias("SAV_AMT"),
        F.when(F.length(F.trim(F.col("Strip.SUBTRACT_AMT"))) == 0, F.lit(0.00)).otherwise(F.abs(F.col("Strip.SUBTRACT_AMT"))).alias("SUBTR_AMT"),
        F.when(F.length(F.trim(F.col("Strip.COB_REAS_CD"))) == 0, F.lit(None)).otherwise(F.trim(F.col("Strip.COB_REAS_CD"))).alias("COB_CAR_RSN_CD_TX"),
        F.when(F.col("cd_mppng.TRGT_CD_NM").isNull(), F.lit(None)).otherwise(F.col("cd_mppng.TRGT_CD_NM")).alias("COB_CAR_RSN_TX"),
    )
)

# "Transformer_70" stage
# Primary link: df_BusinessLogic (alias FctsClmLnCOB)
# Lookup link 1: df_hf_clm_fcts_reversals (alias fcts_reversals), 
#    join condition trim(FctsClmLnCOB.CLM_ID) = fcts_reversals.CLCL_ID
# Lookup link 2: df_clm_nasco_dup_bypass (alias nasco_dup_lkup),
#    join condition FctsClmLnCOB.CLM_ID = nasco_dup_lkup.CLM_ID
df_T70_joined = (
    df_BusinessLogic.alias("FctsClmLnCOB")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.trim(F.col("FctsClmLnCOB.CLM_ID")) == F.col("fcts_reversals.CLCL_ID"),
        "left",
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("FctsClmLnCOB.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left",
    )
)

df_ClnLnCOB = df_T70_joined.filter(F.col("nasco_dup_lkup.CLM_ID").isNull()).select(
    F.col("FctsClmLnCOB.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("FctsClmLnCOB.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("FctsClmLnCOB.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("FctsClmLnCOB.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FctsClmLnCOB.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("FctsClmLnCOB.ERR_CT").alias("ERR_CT"),
    F.col("FctsClmLnCOB.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("FctsClmLnCOB.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FctsClmLnCOB.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("FctsClmLnCOB.CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("FctsClmLnCOB.CLM_ID").alias("CLM_ID"),
    F.col("FctsClmLnCOB.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_TYP_CD"), 10, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("FctsClmLnCOB.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("FctsClmLnCOB.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_CAR_PRORTN_CD"), 10, " ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_LIAB_TYP_CD"), 10, " ").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("FctsClmLnCOB.COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.col("FctsClmLnCOB.ALW_AMT").alias("ALW_AMT"),
    F.col("FctsClmLnCOB.APLD_AMT").alias("APLD_AMT"),
    F.col("FctsClmLnCOB.COPAY_AMT").alias("COPAY_AMT"),
    F.col("FctsClmLnCOB.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("FctsClmLnCOB.DSALW_AMT").alias("DSALW_AMT"),
    F.col("FctsClmLnCOB.COINS_AMT").alias("COINS_AMT"),
    F.col("FctsClmLnCOB.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("FctsClmLnCOB.OOP_AMT").alias("OOP_AMT"),
    F.col("FctsClmLnCOB.PD_AMT").alias("PD_AMT"),
    F.col("FctsClmLnCOB.SANC_AMT").alias("SANC_AMT"),
    F.col("FctsClmLnCOB.SAV_AMT").alias("SAV_AMT"),
    F.col("FctsClmLnCOB.SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("FctsClmLnCOB.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("FctsClmLnCOB.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
)

df_Reversals = df_T70_joined.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
).select(
    F.col("FctsClmLnCOB.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("FctsClmLnCOB.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("FctsClmLnCOB.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("FctsClmLnCOB.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FctsClmLnCOB.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("FctsClmLnCOB.ERR_CT").alias("ERR_CT"),
    F.col("FctsClmLnCOB.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("FctsClmLnCOB.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(
        F.col("SrcSysCd"),
        F.lit(";"),
        F.col("FctsClmLnCOB.CLM_ID"),
        F.lit("R"),
        F.lit(";"),
        F.col("FctsClmLnCOB.CLM_LN_SEQ_NO"),
        F.lit(";"),
        F.col("FctsClmLnCOB.CLM_LN_COB_TYP_CD"),
    ).alias("PRI_KEY_STRING"),
    F.col("FctsClmLnCOB.CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.concat(F.col("FctsClmLnCOB.CLM_ID"), F.lit("R")).alias("CLM_ID"),
    F.col("FctsClmLnCOB.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_TYP_CD"), 10, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("FctsClmLnCOB.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("FctsClmLnCOB.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_CAR_PRORTN_CD"), 10, " ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.rpad(F.col("FctsClmLnCOB.CLM_LN_COB_LIAB_TYP_CD"), 10, " ").alias("CLM_LN_COB_LIAB_TYP_CD"),
    (-1 * F.col("FctsClmLnCOB.COB_CAR_ADJ_AMT")).alias("COB_CAR_ADJ_AMT"),
    (-1 * F.col("FctsClmLnCOB.ALW_AMT")).alias("ALW_AMT"),
    (-1 * F.col("FctsClmLnCOB.APLD_AMT")).alias("APLD_AMT"),
    (-1 * F.col("FctsClmLnCOB.COPAY_AMT")).alias("COPAY_AMT"),
    (-1 * F.col("FctsClmLnCOB.DEDCT_AMT")).alias("DEDCT_AMT"),
    (-1 * F.col("FctsClmLnCOB.DSALW_AMT")).alias("DSALW_AMT"),
    (-1 * F.col("FctsClmLnCOB.COINS_AMT")).alias("COINS_AMT"),
    (-1 * F.col("FctsClmLnCOB.MNTL_HLTH_COINS_AMT")).alias("MNTL_HLTH_COINS_AMT"),
    (-1 * F.col("FctsClmLnCOB.OOP_AMT")).alias("OOP_AMT"),
    (-1 * F.col("FctsClmLnCOB.PD_AMT")).alias("PD_AMT"),
    (-1 * F.col("FctsClmLnCOB.SANC_AMT")).alias("SANC_AMT"),
    (-1 * F.col("FctsClmLnCOB.SAV_AMT")).alias("SAV_AMT"),
    (-1 * F.col("FctsClmLnCOB.SUBTR_AMT")).alias("SUBTR_AMT"),
    F.col("FctsClmLnCOB.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("FctsClmLnCOB.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
)

# Collector2 stage to unify df_Reversals and df_ClnLnCOB into df_Trans
df_Trans = df_Reversals.unionByName(df_ClnLnCOB)

# SnapShot stage: from df_Trans produce three outputs: AllCol, Transform, Snapshot
df_SnapShot_AllCol = df_Trans.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),  # Will fill below with expression
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_COB_CAR_PRORTN_CD").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.col("CLM_LN_COB_LIAB_TYP_CD").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("APLD_AMT").alias("APLD_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("OOP_AMT").alias("OOP_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("SANC_AMT").alias("SANC_AMT"),
    F.col("SAV_AMT").alias("SAV_AMT"),
    F.col("SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
)

df_SnapShot_Transform = df_Trans.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
)

df_SnapShot_Snapshot = df_Trans.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_COB_TYP_CD"), 10, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
)

# Because the SnapShot stage expressions must fill certain columns, we refine them now:
df_SnapShot_AllCol = df_SnapShot_AllCol.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
df_SnapShot_Transform = df_SnapShot_Transform.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
df_SnapShot_Snapshot = df_SnapShot_Snapshot.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))

# Next stage "ClmLnCobPK" shared container
# Two inputs: df_SnapShot_Transform (alias "Transform"), df_SnapShot_AllCol (alias "AllCol")
params_ClmLnCobPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_Key = ClmLnCobPK(df_SnapShot_Transform, df_SnapShot_AllCol, params_ClmLnCobPK)

# Write the resulting df_Key to "FctsClmLnCOBExtr" (CSeqFileStage)
# Column order as in the job
df_FctsClmLnCOBExtr = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_COB_TYP_CD"), 10, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CLM_LN_COB_CAR_PRORTN_CD"), 10, " ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.rpad(F.col("CLM_LN_COB_LIAB_TYP_CD"), 10, " ").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("COB_CAR_ADJ_AMT"),
    F.col("ALW_AMT"),
    F.col("APLD_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT"),
    F.col("OOP_AMT"),
    F.col("PD_AMT"),
    F.col("SANC_AMT"),
    F.col("SAV_AMT"),
    F.col("SUBTR_AMT"),
    F.col("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX"),
)

write_files(
    df_FctsClmLnCOBExtr,
    f"{adls_path}/key/LhoFctsClmLnCOBExtr.LhoFctsClmLnCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# "Transformer" stage after SnapShot_Snapshot => output to "B_CLM_LN_COB"
# Stage variable: ClmLnCobTypCdSk = If Snapshot.CLM_LN_COB_TYP_CD[1,1] = '*' then GetFkeyCodes("FIT",0,"CLAIM COB",...) else ...
# We'll simply call it in-line.
df_Transformer = df_SnapShot_Snapshot.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # The stage var expression for ClmLnCobTypCdSk calls user-defined function GetFkeyCodes
    #  If first char = '*', use one call, else another
    F.when(
        F.col("CLM_LN_COB_TYP_CD").substr(F.lit(1), F.lit(1)) == "*",
        GetFkeyCodes("FIT", F.lit(0), "CLAIM COB", F.col("CLM_LN_COB_TYP_CD").substr(F.lit(2), F.lit(1)), 'X')
    ).otherwise(
        GetFkeyCodes("FACETS", F.lit(0), "CLAIM COB", F.col("CLM_LN_COB_TYP_CD"), 'X')
    ).alias("CLM_LN_COB_TYP_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
)

# Write to "B_CLM_LN_COB" (CSeqFileStage)
df_B_CLM_LN_COB = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD_SK"),
    F.col("ALW_AMT"),
    F.col("PD_AMT"),
)

write_files(
    df_B_CLM_LN_COB,
    f"{adls_path}/load/B_CLM_LN_COB.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)