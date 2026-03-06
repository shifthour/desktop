# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2020, 2021, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmLnDntlCOBExtr
# MAGIC CALLED BY:  LhoFctsClmPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDDC_DNLI_COB  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDDC_DNLI_COB
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
# MAGIC OUTPUTS:  .../key/FctsClmLnDntlCOBExtr.FctsClmLnCOB.dat.#RunID#
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 	Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263731                                        Original Programming                                              IntegrateDev2  
# MAGIC 
# MAGIC 
# MAGIC Goutham K               2021-07-13                      US-386524                                  Implemented Facets Upgrade REAS_CD changes            IntegrateDev2       Jeyaprasanna               2021-07-14
# MAGIC                                                                                                                 to mke the job compatable to run pointing to SYBATCH             
# MAGIC Prabhu ES               2022-03-29                       S2S                                             MSSQL ODBC conn params added                                   IntegrateDev5      	Ken Bradmon	2022-06-11

# MAGIC This container is used in:
# MAGIC FctsClmLnCOBExtr FctsClmLnDntlCOBExtr
# MAGIC NascoClmLnCobExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_ln_cob_allcol) cleared from the container - ClmLnCobPK
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Facets Claim Line Dental Data
# MAGIC Apply business logic
# MAGIC Read the Facets Claim Line Dental COB Extract file created from FctsClmLnDntlCOBExtr.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, LongType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnCobPK
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')

DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrDate = get_widget_value('CurrDate','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

# -- Read from hf_clm_fcts_reversals (Scenario C: hashed file -> read parquet, deduplicate)
df_hf_clm_fcts_reversals_raw = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = dedup_sort(
    df_hf_clm_fcts_reversals_raw,
    ["CLCL_ID"],
    [("CLCL_ID", "A")]
)

# -- Read from clm_nasco_dup_bypass (Scenario C: hashed file -> read parquet, deduplicate)
df_clm_nasco_dup_bypass_raw = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = dedup_sort(
    df_clm_nasco_dup_bypass_raw,
    ["CLM_ID"],
    [("CLM_ID", "A")]
)

# -- Stage: CMC_CDDC_DNLI_COB (ODBC Connector) => df_CMC_CDDC_DNLI_COB
query_cmc_cob = f"""
SELECT cob.CLCL_ID,cob.CDDL_SEQ_NO,cob.MEME_CK,cob.CDDC_PRO_RATE_IND,cob.CDDC_ADJ_AMT,cob.CDDC_SUBTRACT_AMT,cob.CDDC_COB_TYPE,C.CCDC_OC_REAS_CODE AS CDDC_COB_REAS_CD,cob.CDDC_COB_AMT,cob.CDDC_COB_DISALLOW,cob.CDDC_COB_ALLOW,cob.CDDC_COB_SAV,CDDC_COB_APP,cob.CDDC_COB_OOP,cob.CDDC_COB_SANCTION,cob.CDDC_COB_DED_AMT,cob.CDDC_COB_COPAY_AMT,cob.CDDC_COB_COINS_AMT 
FROM {LhoFacetsStgOwner}.CMC_CDDC_DNLI_COB  cob
INNER JOIN tempdb..{DriverTable}  TMP  ON TMP.CLM_ID = cob.CLCL_ID
INNER JOIN (
  SELECT A.CLCL_ID, A.CDDL_SEQ_NO, A.CCDC_OC_REAS_CODE 
  FROM {LhoFacetsStgOwner}.CMC_CCDC_CARC_CODE A ,
       (
        select CLCL_ID,CDDL_SEQ_NO, MIN(CCDC_SEQ_NO) as CCDC_SEQ_NO 
        from {LhoFacetsStgOwner}.CMC_CCDC_CARC_CODE
        group by CLCL_ID,CDDL_SEQ_NO
       ) B
  WHERE A.CLCL_ID = B.CLCL_ID 
    AND A.CDDL_SEQ_NO=B.CDDL_SEQ_NO 
    AND A.CCDC_SEQ_NO=B.CCDC_SEQ_NO
) C
ON cob.CLCL_ID = C.CLCL_ID 
AND cob.CDDL_SEQ_NO = C.CDDL_SEQ_NO
"""
df_CMC_CDDC_DNLI_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_cmc_cob)
    .load()
)

# -- Stage: cdma_mapping (DB2 Connector, input from IDS)
query_cdma_mapping = f"""
SELECT SRC_SYS_CD as SRC_SYS_CD,
       SRC_CD as SRC_CD,
       TRGT_DOMAIN_NM as TRGT_DOMAIN_NM,
       TRGT_CD_NM as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM  = 'EXPLANATION CODE PROVIDER ADJUSTMENT'

{IDSOwner}.CD_MPPNG
"""
df_cdma_mapping_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_cdma_mapping)
    .load()
)

# -- cdma_lookup (Scenario A: CHashedFileStage as intermediate) => deduplicate by PK [SRC_SYS_CD, SRC_CD]
df_cdma_mapping = dedup_sort(
    df_cdma_mapping_raw,
    ["SRC_SYS_CD","SRC_CD"],
    [("SRC_SYS_CD","A"),("SRC_CD","A")]
)

# -- Stage: StripFields => primary link from df_CMC_CDDC_DNLI_COB, lookup from df_cdma_mapping
df_stripfields_join = (
    df_CMC_CDDC_DNLI_COB.alias("Extract")
    .join(
        df_cdma_mapping.alias("cd_mp"),
        [
            (F.lit(SrcSysCd) == F.col("cd_mp.SRC_SYS_CD")),
            (F.trim(F.col("Extract.CDDC_COB_REAS_CD")) == F.col("cd_mp.SRC_CD")),
            (F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT") == F.col("cd_mp.TRGT_DOMAIN_NM"))
        ],
        "left"
    )
)

df_stripfields = (
    df_stripfields_join
    .withColumn("CLM_ID", strip_field(F.col("Extract.CLCL_ID")))
    .withColumn("CLM_LN_SEQ_NO", F.col("Extract.CDDL_SEQ_NO"))
    .withColumn("CLM_LN_COB_TYPE", strip_field(F.col("Extract.CDDC_COB_TYPE")))
    .withColumn("CLM_LN_COB_CAR_PRORTN_CD", strip_field(F.col("Extract.CDDC_PRO_RATE_IND")))
    .withColumn("CLM_LN_COB_LIAB_TYP_CD", F.lit("NA"))
    .withColumn("COB_CAR_ADJ_AMT", strip_field(F.col("Extract.CDDC_ADJ_AMT")))
    .withColumn("ALW_AMT", strip_field(F.col("Extract.CDDC_COB_ALLOW")))
    .withColumn("APLD_AMT", strip_field(F.col("Extract.CDDC_COB_APP")))
    .withColumn("COPAY_AMT", strip_field(F.col("Extract.CDDC_COB_COPAY_AMT")))
    .withColumn("DEDCT_AMT", strip_field(F.col("Extract.CDDC_COB_DED_AMT")))
    .withColumn("DSALW_AMT", strip_field(F.col("Extract.CDDC_COB_DISALLOW")))
    .withColumn("COINS_AMT", strip_field(F.col("Extract.CDDC_COB_COINS_AMT")))
    .withColumn("MNTL_HLTH_COINS_AMT", F.lit("0.00"))
    .withColumn("OPP_AMT", strip_field(F.col("Extract.CDDC_COB_OOP")))
    .withColumn("PD_AMT", strip_field(F.col("Extract.CDDC_COB_AMT")))
    .withColumn("SANC_AMT", strip_field(F.col("Extract.CDDC_COB_SANCTION")))
    .withColumn("SAV_AMT", strip_field(F.col("Extract.CDDC_COB_SAV")))
    .withColumn("SBTR_AMT", strip_field(F.col("Extract.CDDC_SUBTRACT_AMT")))
    .withColumn("CDDC_COB_REAS_CD", strip_field(F.col("Extract.CDDC_COB_REAS_CD")))
    .withColumn(
        "CDDC_COB_REAS_TX",
        F.when(F.col("cd_mp.TRGT_CD_NM").isNull(), F.lit(None))
         .otherwise(F.trim(F.col("cd_mp.TRGT_CD_NM")))
    )
)

# -- Stage: ids_excd (DB2 Connector) => read
query_ids_excd = f"""
SELECT 
              EXCD_ID,  
              SRC_SYS_CD_SK,  
              TRGT_CD 
FROM {IDSOwner}.EXCD , 
     {IDSOwner}.CD_MPPNG
WHERE   EXCD_HIPAA_PROV_ADJ_CD_SK = CD_MPPNG_SK

{IDSOwner}.EXCD
"""
df_ids_excd_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_ids_excd)
    .load()
)

# -- Trans1 => simple transform
df_trans1 = (
    df_ids_excd_raw
    .withColumn("EXCD_ID", F.col("EXCD_ID"))
    .withColumn("TRGT_CD_NM", F.when(F.trim(F.col("TRGT_CD")) == F.lit("UNK"), F.lit(None))
                               .otherwise(F.trim(F.col("TRGT_CD"))))
)

# -- hf_clmlnDntl_cob_excd (Scenario A: CHashedFileStage as intermediate) => deduplicate by PK [EXCD_ID]
df_excd_cdma_in_raw = df_trans1.select(
    F.trim(F.col("EXCD_ID")).alias("EXCD_ID"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_excd_cdma_in = dedup_sort(
    df_excd_cdma_in_raw,
    ["EXCD_ID"],
    [("EXCD_ID","A")]
)

# -- Stage: Business_Logic => primary link: df_stripfields(alias: Strip), lookup link: df_excd_cdma_in(alias: excd_cdma_in)
df_business_logic_join = (
    df_stripfields.alias("Strip")
    .join(
        df_excd_cdma_in.alias("excd_cdma_in"),
        F.trim(F.col("Strip.CDDC_COB_REAS_CD")) == F.col("excd_cdma_in.EXCD_ID"),
        "left"
    )
)

df_business_logic = (
    df_business_logic_join
    .withColumn(
        "CobCarRsnTx",
        F.when(F.length(F.trim(F.col("Strip.CDDC_COB_REAS_CD"))) == 0, F.lit(None))
         .when((F.length(F.trim(F.col("excd_cdma_in.EXCD_ID"))) == 0) | F.col("excd_cdma_in.EXCD_ID").isNull(), F.lit(None))
         .when(F.trim(F.col("excd_cdma_in.TRGT_CD_NM")) == F.lit("NA"), F.lit(None))
         .otherwise(F.trim(F.col("excd_cdma_in.TRGT_CD_NM")))
    )
    .withColumn("ClmLnCobLiabTypCd", F.lit("NA"))
    .withColumn("MntlHlthCoinsAmt", F.lit(0.0))
)

df_business_logic_out = df_business_logic.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("U").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),  # from parameter or col? The original says "CurrDate". We'll use an expression with F.lit(CurrDate).
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),F.trim(F.col("Strip.CLM_ID")),F.lit(";"),F.col("Strip.CLM_LN_SEQ_NO"),F.lit(";"),F.trim(F.col("Strip.CLM_LN_COB_TYPE"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_COB_LN_SK"),
    F.trim(F.col("Strip.CLM_ID")).alias("CLM_ID"),
    F.col("Strip.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(
        F.col("Strip.CLM_LN_COB_TYPE").isNull() | (F.length(F.trim(F.col("Strip.CLM_LN_COB_TYPE"))) == 0),
        F.lit("UNK")
    ).otherwise(F.upper(F.trim(F.col("Strip.CLM_LN_COB_TYPE")))).alias("CLM_LN_COB_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("Strip.CLM_LN_COB_CAR_PRORTN_CD").isNull() | (F.length(F.trim(F.col("Strip.CLM_LN_COB_CAR_PRORTN_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.trim(F.col("Strip.CLM_LN_COB_CAR_PRORTN_CD")))).alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.col("ClmLnCobLiabTypCd").alias("CLM_LN_COB_LIAB_TYP_CD"),
    (F.abs(F.col("APLD_AMT")) - F.abs(F.col("SAV_AMT"))).alias("COB_CAR_ADJ_AMT"),
    F.abs(F.col("ALW_AMT")).alias("ALW_AMT"),
    F.abs(F.col("APLD_AMT")).alias("APLD_AMT"),
    F.abs(F.col("COPAY_AMT")).alias("COPAY_AMT"),
    F.abs(F.col("DEDCT_AMT")).alias("DEDCT_AMT"),
    F.abs(F.col("DSALW_AMT")).alias("DSALW_AMT"),
    F.abs(F.col("COINS_AMT")).alias("MED_COINS_AMT"),
    F.abs(F.col("MntlHlthCoinsAmt")).alias("MNTL_HLTH_COINS_AMT"),
    F.abs(F.col("OPP_AMT")).alias("OOP_AMT"),
    F.abs(F.col("PD_AMT")).alias("PD_AMT"),
    F.abs(F.col("SANC_AMT")).alias("SANC_AMT"),
    F.abs(F.col("SAV_AMT")).alias("SAV_AMT"),
    F.abs(F.col("SBTR_AMT")).alias("SUBTR_AMT"),
    F.col("Strip.CDDC_COB_REAS_CD").alias("COB_CAR_RSN_CD_TX"),
    F.col("CobCarRsnTx").alias("COB_CAR_RSN_TX")
).alias("Transform")

# -- Stage: Trans2 => joins with hf_clm_fcts_reversals (fcts_reversals, left) and clm_nasco_dup_bypass (nasco_dup_lkup, left)
df_trans2_join = (
    df_business_logic_out.alias("Transform")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        (F.trim(F.col("Transform.CLM_ID")) == F.col("fcts_reversals.CLCL_ID")),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        (F.col("Transform.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID")),
        "left"
    )
)

# -- Split into two outputs
df_trans2_ClnLnDnt = df_trans2_join.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_trans2_reversals = df_trans2_join.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
)

# -- Select columns for each output link in Trans2
cols_ClnLnDnt = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","CLM_COB_LN_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_COB_TYP_CD","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_LN_COB_CAR_PRORTN_CD","CLM_LN_COB_LIAB_TYP_CD","COB_CAR_ADJ_AMT","ALW_AMT",
    "APLD_AMT","COPAY_AMT","DEDCT_AMT","DSALW_AMT","MED_COINS_AMT","MNTL_HLTH_COINS_AMT","OOP_AMT","PD_AMT",
    "SANC_AMT","SAV_AMT","SUBTR_AMT","COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
]
df_ClnLnDnt = df_trans2_ClnLnDnt.select(*cols_ClnLnDnt)

cols_reversals = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","CLM_COB_LN_SK"
    ,"CLM_ID","CLM_LN_SEQ_NO","CLM_LN_COB_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_COB_CAR_PRORTN_CD","CLM_LN_COB_LIAB_TYP_CD",
    # The expressions are negation replacements:
    F.expr("-1 * COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.expr("Neg(ALW_AMT)").alias("ALW_AMT"),
    F.expr("Neg(APLD_AMT)").alias("APLD_AMT"),
    F.expr("Neg(COPAY_AMT)").alias("COPAY_AMT"),
    F.expr("Neg(DEDCT_AMT)").alias("DEDCT_AMT"),
    F.expr("Neg(DSALW_AMT)").alias("DSALW_AMT"),
    F.expr("Neg(MED_COINS_AMT)").alias("MED_COINS_AMT"),
    F.expr("Neg(MNTL_HLTH_COINS_AMT)").alias("MNTL_HLTH_COINS_AMT"),
    F.expr("Neg(OOP_AMT)").alias("OOP_AMT"),
    F.expr("Neg(PD_AMT)").alias("PD_AMT"),
    F.expr("Neg(SANC_AMT)").alias("SANC_AMT"),
    F.expr("Neg(SAV_AMT)").alias("SAV_AMT"),
    F.expr("Neg(SUBTR_AMT)").alias("SUBTR_AMT"),
    "COB_CAR_RSN_CD_TX","COB_CAR_RSN_TX"
]
df_reversals = df_trans2_reversals.select(*cols_reversals)

# -- Collector => union these two
df_collector = df_ClnLnDnt.unionByName(df_reversals.select(cols_ClnLnDnt))

# -- SnapShot => we produce 3 outputs: AllCol, Transform, Snapshot
df_snapshot_in = df_collector

# 1) AllCol output
df_snapshot_allcol = df_snapshot_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CLM_LN_COB_CAR_PRORTN_CD"),10," ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.rpad(F.col("CLM_LN_COB_LIAB_TYP_CD"),10," ").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("APLD_AMT").alias("APLD_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("MED_COINS_AMT").alias("COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("OOP_AMT").alias("OOP_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("SANC_AMT").alias("SANC_AMT"),
    F.col("SAV_AMT").alias("SAV_AMT"),
    F.col("SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

# 2) Transform output
df_snapshot_transform = df_snapshot_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD")
)

# 3) Snapshot output
df_snapshot_snapshot = df_snapshot_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),12," ").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_COB_TYP_CD"),10," ").alias("CLM_LN_COB_TYP_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

# -- Shared Container: ClmLnCobPK -> takes df_snapshot_transform (C199P1) and df_snapshot_allcol (C199P2), returns 1 output (C199P5)
params_ClmLnCobPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_container_out = ClmLnCobPK(df_snapshot_transform, df_snapshot_allcol, params_ClmLnCobPK)

# -- Stage: FctsClmLnDntlCOBExtr => CSeqFileStage => write
df_FctsClmLnDntlCOBExtr = df_container_out.select(
    F.rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()),1," ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_COB_TYP_CD"),10," ").alias("CLM_LN_COB_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CLM_LN_COB_CAR_PRORTN_CD"),10," ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.rpad(F.col("CLM_LN_COB_LIAB_TYP_CD"),10," ").alias("CLM_LN_COB_LIAB_TYP_CD"),
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
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

write_files(
    df_FctsClmLnDntlCOBExtr,
    f"{adls_path}/key/LhoFctsClmLnDntlCOBExtr.LhoFctsClmLnCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -- Stage: Transformer => input df_snapshot_snapshot => produce "RowCount" => B_CLM_LN_COB
df_transformer = df_snapshot_snapshot.withColumn(
    "ClmLnCobTypCdSk",
    F.when(
        F.substring(F.col("CLM_LN_COB_TYP_CD"),1,1) == F.lit("*"),
        GetFkeyCodes("FIT",F.lit(0),"CLAIM COB",F.substring(F.col("CLM_LN_COB_TYP_CD"),2,1),"X")
    ).otherwise(
        GetFkeyCodes("FACETS",F.lit(0),"CLAIM COB",F.col("CLM_LN_COB_TYP_CD"),"X")
    )
)

df_transformer_out = df_transformer.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ClmLnCobTypCdSk").alias("CLM_LN_COB_TYP_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

# -- Stage: B_CLM_LN_COB => output to seq file
df_B_CLM_LN_COB = df_transformer_out.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_COB_TYP_CD_SK"),
    F.col("ALW_AMT"),
    F.col("PD_AMT")
)

write_files(
    df_B_CLM_LN_COB,
    f"{adls_path}/load/B_CLM_LN_COB.DENTAL.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)