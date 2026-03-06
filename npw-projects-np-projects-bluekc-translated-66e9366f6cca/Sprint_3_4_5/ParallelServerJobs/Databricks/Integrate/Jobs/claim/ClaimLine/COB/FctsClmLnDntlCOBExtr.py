# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnCOBExtr
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
# MAGIC                  SAndrew   01/15/2004-   Originally Programmed
# MAGIC           Brent Leland    2006-03-06    -  Changed to use Sequencer Processing
# MAGIC          BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------    
# MAGIC 
# MAGIC Parik                        2008-07-25       3567(Primary Key)  Added primary keying process for Claim Line Dental COB                                devlIDS                           Steph Goddard          07/28/2008
# MAGIC 
# MAGIC Manasa Andru         2016-10-30       TFS - 10697          Added snapshot file to address the balancing issue for the                              IntegrateDev2                   Kalyan Neelam          2016-10-31
# MAGIC                                                                                           CLM_LN_COB table.
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidnidi   2021-03-17                                  Facets Upgrade added JOIN to get CLCB_COB_REAS_CD                                IntegrateDev1                   Kalyan Neelam          2021-03-18
# MAGIC Prabhu ES              2022-02-26       S2S Remediation      MSSQL connection parameters added                                                           IntegrateDev5	Ken Bradmon	2022-06-10

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
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnCobPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrDate = get_widget_value('CurrDate','')

# Read hashed file hf_clm_fcts_reversals (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read hashed file clm_nasco_dup_bypass (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Connect to Facets (ODBCConnector) for CMC_CDDC_DNLI_COB
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cmc_cddc = f"""
SELECT cob.CLCL_ID,
       cob.CDDL_SEQ_NO,
       cob.MEME_CK,
       cob.CDDC_PRO_RATE_IND,
       cob.CDDC_ADJ_AMT,
       cob.CDDC_SUBTRACT_AMT,
       cob.CDDC_COB_TYPE,
       C.CCDC_OC_REAS_CODE AS CDDC_COB_REAS_CD,
       cob.CDDC_COB_AMT,
       cob.CDDC_COB_DISALLOW,
       cob.CDDC_COB_ALLOW,
       cob.CDDC_COB_SAV,
       cob.CDDC_COB_APP,
       cob.CDDC_COB_OOP,
       cob.CDDC_COB_SANCTION,
       cob.CDDC_COB_DED_AMT,
       cob.CDDC_COB_COPAY_AMT,
       cob.CDDC_COB_COINS_AMT
FROM {FacetsOwner}.CMC_CDDC_DNLI_COB cob
INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = cob.CLCL_ID
INNER JOIN (
  SELECT A.CLCL_ID,
         A.CDDL_SEQ_NO,
         A.CCDC_OC_REAS_CODE
  FROM {FacetsOwner}.CMC_CCDC_CARC_CODE A,
       (
         SELECT CLCL_ID,
                CDDL_SEQ_NO,
                MIN(CCDC_SEQ_NO) as CCDC_SEQ_NO
         FROM {FacetsOwner}.CMC_CCDC_CARC_CODE
         GROUP BY CLCL_ID, CDDL_SEQ_NO
       ) B
  WHERE A.CLCL_ID = B.CLCL_ID
    AND A.CDDL_SEQ_NO = B.CDDL_SEQ_NO
    AND A.CCDC_SEQ_NO = B.CCDC_SEQ_NO
) C
ON cob.CLCL_ID = C.CLCL_ID
AND cob.CDDL_SEQ_NO = C.CDDL_SEQ_NO
"""
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc_cddc)
    .load()
)

# Connect to IDS (DB2Connector) for cdma_mapping
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_cdma_mapping = f"""
SELECT SRC_SYS_CD as SRC_SYS_CD,
       SRC_CD as SRC_CD,
       TRGT_DOMAIN_NM as TRGT_DOMAIN_NM,
       TRGT_CD_NM as TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'EXPLANATION CODE PROVIDER ADJUSTMENT'

{IDSOwner}.CD_MPPNG
"""
df_cdma_mapping = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cdma_mapping)
    .load()
)

# Deduplicate the cdma_mapping -> cdma_lookup (Scenario A for hashed file "cdma_lookup")
df_cdma_mapping_dedup = dedup_sort(
    df_cdma_mapping,
    partition_cols=["SRC_SYS_CD", "SRC_CD"],
    sort_cols=[]
)

# Emulate expressions in cdma_lookup
df_cd_mp = (
    df_cdma_mapping_dedup
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("SRC_CD", trim(F.col("SRC_CD")))
    .withColumn("TRGT_DOMAIN_NM", F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT"))
    .withColumn("TRGT_CD_NM", F.col("TRGT_CD_NM"))
)

# StripFields transformer: left join with df_cd_mp
df_StripFields_join = (
    df_Extract.alias("Extract")
    .join(
        df_cd_mp.alias("cd_mp"),
        (
            (F.trim(F.col("Extract.CDDC_COB_REAS_CD")) == F.col("cd_mp.SRC_CD")) &
            (F.lit("FACETS") == F.col("cd_mp.SRC_SYS_CD")) &
            (F.lit("EXPLANATION CODE PROVIDER ADJUSTMENT") == F.col("cd_mp.TRGT_DOMAIN_NM"))
        ),
        "left"
    )
)

df_StripFields = (
    df_StripFields_join
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
        .otherwise(trim(F.col("cd_mp.TRGT_CD_NM")))
    )
)

# Connect to IDS (DB2Connector) for ids_excd
extract_query_ids_excd = f"""
SELECT
    EXCD_ID,
    SRC_SYS_CD_SK,
    TRGT_CD
FROM {IDSOwner}.EXCD ,
     {IDSOwner}.CD_MPPNG
WHERE EXCD_HIPAA_PROV_ADJ_CD_SK = CD_MPPNG_SK

{IDSOwner}.EXCD
"""
df_ids_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_excd)
    .load()
)

# Trans1
df_excd_cdma = (
    df_ids_excd.alias("excd")
    .withColumn("EXCD_ID", F.col("excd.EXCD_ID"))
    .withColumn(
        "TRGT_CD_NM",
        F.when(F.trim(F.col("excd.TRGT_CD")) == F.lit("UNK"), F.lit(None))
        .otherwise(trim(F.col("excd.TRGT_CD")))
    )
    .select("EXCD_ID", "TRGT_CD_NM")
)

# hf_clmlnDntl_cob_excd => Scenario A or C? 
# Checking usage: "Trans1 => hf_clmlnDntl_cob_excd => Business_Logic."
# This is a straight pass. No transformer writes back. We treat as Scenario A, deduplicate on key "EXCD_ID".
df_excd_cdma_dedup = dedup_sort(
    df_excd_cdma,
    partition_cols=["EXCD_ID"],
    sort_cols=[]
)

# Emulate expressions in hf_clmlnDntl_cob_excd (the stage columns specify new expressions, but they reference unknown input).
# We must not skip logic, but the references are unclear. We'll pass through the columns as best as we can.
df_excd_cdma_in = (
    df_excd_cdma_dedup
    .withColumn("EXCD_ID", trim(F.col("EXCD_ID")))  # The stage expression references something else, but we have no matching col. Mimic trim.
    .withColumn("TRGT_CD_NM", F.col("TRGT_CD_NM"))
    .select("EXCD_ID", "TRGT_CD_NM")
)

# Business_Logic transformer: left join with df_excd_cdma_in, primary link df_StripFields
df_Business_Logic_join = (
    df_StripFields.alias("Strip")
    .join(
        df_excd_cdma_in.alias("excd_cdma_in"),
        F.trim(F.col("Strip.CDDC_COB_REAS_CD")) == F.col("excd_cdma_in.EXCD_ID"),
        "left"
    )
)

df_Business_Logic = (
    df_Business_Logic_join
    .withColumn("ClmLnCobLiabTypCd", F.lit("NA"))
    .withColumn("MntlHlthCoinsAmt", F.lit(0.0))
    .withColumn(
        "CobCarRsnTx",
        F.when(
            (F.length(F.trim(F.col("Strip.CDDC_COB_REAS_CD"))) == 0) |
            (F.trim(F.col("Strip.CDDC_COB_REAS_CD")).isNull()),
            F.lit(None)
        ).otherwise(
            F.when(
                (F.length(F.trim(F.col("excd_cdma_in.EXCD_ID"))) == 0) |
                (F.trim(F.col("excd_cdma_in.EXCD_ID")).isNull()),
                F.lit(None)
            ).otherwise(
                F.when(
                    (F.trim(F.col("excd_cdma_in.TRGT_CD_NM")) == F.lit("NA")),
                    F.lit(None)
                ).otherwise(trim(F.col("excd_cdma_in.TRGT_CD_NM")))
            )
        )
    )
    .select("Strip.*", "ClmLnCobLiabTypCd", "MntlHlthCoinsAmt", "CobCarRsnTx")
)

df_Transform = (
    df_Business_Logic
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("U"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.col("CurrDate"))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("SrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(";", 
                    F.col("SrcSysCd"), 
                    F.trim(F.col("CLM_ID")), 
                    F.col("CLM_LN_SEQ_NO").cast(StringType()), 
                    F.trim(F.col("CLM_LN_COB_TYPE"))
                   )
    )
    .withColumn("CLM_COB_LN_SK", F.lit(0))
    .withColumn("CLM_ID", F.trim(F.col("CLM_ID")))
    .withColumn("CLM_LN_SEQ_NO", F.col("CLM_LN_SEQ_NO"))
    .withColumn(
        "CLM_LN_COB_TYP_CD",
        F.when(
            (F.col("CLM_LN_COB_TYPE").isNull()) | 
            (F.length(F.trim(F.col("CLM_LN_COB_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("CLM_LN_COB_TYPE"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "CLM_LN_COB_CAR_PRORTN_CD",
        F.when(
            (F.col("CLM_LN_COB_CAR_PRORTN_CD").isNull()) | 
            (F.length(F.trim(F.col("CLM_LN_COB_CAR_PRORTN_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("CLM_LN_COB_CAR_PRORTN_CD"))))
    )
    .withColumn("CLM_LN_COB_LIAB_TYP_CD", F.col("ClmLnCobLiabTypCd"))
    .withColumn("COB_CAR_ADJ_AMT", F.abs(F.col("APLD_AMT")) - F.abs(F.col("SAV_AMT")))
    .withColumn("ALW_AMT", F.abs(F.col("ALW_AMT")))
    .withColumn("APLD_AMT", F.abs(F.col("APLD_AMT")))
    .withColumn("COPAY_AMT", F.abs(F.col("COPAY_AMT")))
    .withColumn("DEDCT_AMT", F.abs(F.col("DEDCT_AMT")))
    .withColumn("DSALW_AMT", F.abs(F.col("DSALW_AMT")))
    .withColumn("MED_COINS_AMT", F.abs(F.col("COINS_AMT")))
    .withColumn("MNTL_HLTH_COINS_AMT", F.abs(F.col("MntlHlthCoinsAmt")))
    .withColumn("OOP_AMT", F.abs(F.col("OPP_AMT")))
    .withColumn("PD_AMT", F.abs(F.col("PD_AMT")))
    .withColumn("SANC_AMT", F.abs(F.col("SANC_AMT")))
    .withColumn("SAV_AMT", F.abs(F.col("SAV_AMT")))
    .withColumn("SUBTR_AMT", F.abs(F.col("SBTR_AMT")))
    .withColumn("COB_CAR_RSN_CD_TX", F.col("CDDC_COB_REAS_CD"))
    .withColumn("COB_CAR_RSN_TX", F.col("CobCarRsnTx"))
)

# Trans2, reference lookups: fcts_reversals (left join) on CLCL_ID
df_Trans2_join1 = (
    df_Transform.alias("Transform")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        (F.trim(F.col("Transform.CLM_ID")) == F.col("fcts_reversals.CLCL_ID")),
        "left"
    )
)

# Then another left join: df_clm_nasco_dup_bypass on CLM_ID
df_Trans2_join = (
    df_Trans2_join1.alias("Transform")
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        (F.col("Transform.CLM_ID") == F.col("nasco_dup_lkup.CLM_ID")),
        "left"
    )
)

df_Trans2 = df_Trans2_join

# Output links from Trans2:
# 1) ClnLnDnt (constraint: IsNull(nasco_dup_lkup.CLM_ID) = true)
df_ClnLnDnt = df_Trans2.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_ClnLnDnt = (
    df_ClnLnDnt
    .withColumn("PRI_KEY_STRING", F.col("PRI_KEY_STRING"))
)

# 2) reversals (constraint: fcts_reversals.CLCL_ID is not null and fcts_reversals.CLCL_CUR_STS='91')
df_reversals = df_Trans2.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
)
df_reversals = (
    df_reversals
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.col("SrcSysCd"),
            F.trim(F.col("CLM_ID")),
            F.lit("R"),
            F.trim(F.col("CLM_LN_SEQ_NO").cast(StringType())),
            F.trim(F.col("CLM_LN_COB_TYP_CD"))
        )
    )
    .withColumn("CLM_ID", F.concat(F.trim(F.col("CLM_ID")), F.lit("R")))
    .withColumn("COB_CAR_ADJ_AMT", -1 * F.col("COB_CAR_ADJ_AMT"))
    .withColumn("ALW_AMT", -1 * F.col("ALW_AMT"))
    .withColumn("APLD_AMT", -1 * F.col("APLD_AMT"))
    .withColumn("COPAY_AMT", -1 * F.col("COPAY_AMT"))
    .withColumn("DEDCT_AMT", -1 * F.col("DEDCT_AMT"))
    .withColumn("DSALW_AMT", -1 * F.col("DSALW_AMT"))
    .withColumn("MED_COINS_AMT", -1 * F.col("MED_COINS_AMT"))
    .withColumn("MNTL_HLTH_COINS_AMT", -1 * F.col("MNTL_HLTH_COINS_AMT"))
    .withColumn("OOP_AMT", -1 * F.col("OOP_AMT"))
    .withColumn("PD_AMT", -1 * F.col("PD_AMT"))
    .withColumn("SANC_AMT", -1 * F.col("SANC_AMT"))
    .withColumn("SAV_AMT", -1 * F.col("SAV_AMT"))
    .withColumn("SUBTR_AMT", -1 * F.col("SUBTR_AMT"))
)

# Collector: union the two outputs
common_cols = [
    "JOB_EXCTN_RCRD_ERR_SK", "INSRT_UPDT_CD", "DISCARD_IN", "PASS_THRU_IN",
    "FIRST_RECYC_DT", "ERR_CT", "RECYCLE_CT", "SRC_SYS_CD", "PRI_KEY_STRING",
    "CLM_COB_LN_SK", "CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK", "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_COB_CAR_PRORTN_CD", "CLM_LN_COB_LIAB_TYP_CD", "COB_CAR_ADJ_AMT",
    "ALW_AMT", "APLD_AMT", "COPAY_AMT", "DEDCT_AMT", "DSALW_AMT", 
    "MED_COINS_AMT", "MNTL_HLTH_COINS_AMT", "OOP_AMT", "PD_AMT", 
    "SANC_AMT", "SAV_AMT", "SUBTR_AMT", "COB_CAR_RSN_CD_TX", "COB_CAR_RSN_TX"
]

df_ClnLnDnt_sel = (
    df_ClnLnDnt.select(
        *[F.col(c) for c in common_cols if c in df_ClnLnDnt.columns]  # ensure in both
    )
    .withColumn("COINS_AMT", F.col("MED_COINS_AMT"))
)

df_reversals_sel = (
    df_reversals.select(
        *[F.col(c) for c in common_cols if c in df_reversals.columns]
    )
    .withColumn("COINS_AMT", F.col("MED_COINS_AMT"))
)

df_Collector = df_ClnLnDnt_sel.unionByName(df_reversals_sel)

# SnapShot
df_SnapShot = df_Collector.select(
    F.lit(None).alias("SRC_SYS_CD_SK"),
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_COB_TYP_CD",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_COB_LN_SK",
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "CLM_LN_COB_CAR_PRORTN_CD",
    "CLM_LN_COB_LIAB_TYP_CD",
    "COB_CAR_ADJ_AMT",
    "ALW_AMT",
    "APLD_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    F.col("COINS_AMT").alias("COINS_AMT"),
    "MNTL_HLTH_COINS_AMT",
    "OOP_AMT",
    "PD_AMT",
    "SANC_AMT",
    "SAV_AMT",
    "SUBTR_AMT",
    "COB_CAR_RSN_CD_TX",
    "COB_CAR_RSN_TX"
).alias("Trans")

df_SnapShot_AllCol = df_SnapShot.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLM_ID"),
    F.col("Trans.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_COB_LN_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_COB_CAR_PRORTN_CD",
    "CLM_LN_COB_LIAB_TYP_CD",
    "COB_CAR_ADJ_AMT",
    "ALW_AMT",
    "APLD_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "COINS_AMT",
    "MNTL_HLTH_COINS_AMT",
    "OOP_AMT",
    "PD_AMT",
    "SANC_AMT",
    "SAV_AMT",
    "SUBTR_AMT",
    "COB_CAR_RSN_CD_TX",
    "COB_CAR_RSN_TX"
)

df_SnapShot_Transform = df_SnapShot.select(
    F.lit(None).alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLM_ID"),
    F.col("Trans.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD")
)

df_SnapShot_Transformer = df_SnapShot.select(
    F.lit(None).alias("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Trans.CLM_ID").alias("CLCL_ID"),
    F.col("Trans.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Trans.CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
    "ALW_AMT",
    "PD_AMT"
)

# Simulate the Shared Container "ClmLnCobPK"
# Two inputs: 
#   1) "Transform" => df_SnapShot_Transform
#   2) "AllCol" => df_SnapShot_AllCol
# One output => "Key"
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_Key, = ClmLnCobPK(df_SnapShot_Transform, df_SnapShot_AllCol, params)

# FctsClmLnDntlCOBExtr (CSeqFileStage) => write to .dat with the specified path
# The stage columns specify final ordering and rpad for char/varchar

df_FctsClmLnDntlCOBExtr_pre = df_Key.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    rpad(F.col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_COB_LN_SK"),
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    rpad(F.col("CLM_LN_COB_TYP_CD"), 10, " ").alias("CLM_LN_COB_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("CLM_LN_COB_CAR_PRORTN_CD"), 10, " ").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    rpad(F.col("CLM_LN_COB_LIAB_TYP_CD"), 10, " ").alias("CLM_LN_COB_LIAB_TYP_CD"),
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
    F.col("COB_CAR_RSN_TX")
)

write_files(
    df_FctsClmLnDntlCOBExtr_pre,
    f"{adls_path}/key/FctsClmLnDntlCOBExtr.FctsClmLnCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer => B_CLM_LN_COB
df_Transformer = (
    df_SnapShot_Transformer.alias("Snapshot")
    .withColumn(
        "ClmLnCobTypCdSk",
        F.when(
            F.substring(F.col("Snapshot.CLM_LN_COB_TYP_CD"), 1, 1) == F.lit("*"),
            F.expr("GetFkeyCodes('FIT', 0, 'CLAIM COB', substring(Snapshot.CLM_LN_COB_TYP_CD,2,1), 'X')")
        )
        .otherwise(
            F.expr("GetFkeyCodes('FACETS', 0, 'CLAIM COB', Snapshot.CLM_LN_COB_TYP_CD, 'X')")
        )
    )
    .select(
        F.col("Snapshot.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
        F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Snapshot.CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
        F.col("ALW_AMT"),
        F.col("PD_AMT"),
        F.col("ClmLnCobTypCdSk").alias("CLM_LN_COB_TYP_CD_SK")
    )
)

# B_CLM_LN_COB => write to .dat
df_B_CLM_LN_COB_pre = (
    df_Transformer
    .select(
        rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 1, " ").alias("SRC_SYS_CD_SK"),
        rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        rpad(F.col("CLM_LN_COB_TYP_CD_SK").cast(StringType()), 1, " ").alias("CLM_LN_COB_TYP_CD_SK"),
        F.col("ALW_AMT"),
        F.col("PD_AMT")
    )
)

write_files(
    df_B_CLM_LN_COB_pre,
    f"{adls_path}/load/B_CLM_LN_COB.DENTAL.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)  

params = {
  "EnvProjectPath": f"dap/<...>/<project>",
  "File_Path": "<file_path>",
  "File_Name": "<file_name>"
}
dbutils.notebook.run("../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params)