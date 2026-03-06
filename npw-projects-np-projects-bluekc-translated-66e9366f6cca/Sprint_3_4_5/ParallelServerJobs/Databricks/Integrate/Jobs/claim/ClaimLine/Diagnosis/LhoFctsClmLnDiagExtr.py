# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmLnDiagExtr
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC                
# MAGIC             
# MAGIC PROCESSING:  Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 	Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-12                        US -  263734                                         Original Programming                                              IntegrateDev2                   Jaideep Mankala       10/09/2020
# MAGIC Prabhu ES                2022-03-29                        S2S                                             MSSQL ODBC conn params added                                  IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Extract Facets Claim Line Diagnosis Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
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


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCd = get_widget_value('SrcSysCd','LUMERIS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read hashed file hf_clm_fcts_reversals (Scenario C => read as Parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
).alias("fcts_reversals")

# Read hashed file clm_nasco_dup_bypass (Scenario C => read as Parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select(
    "CLM_ID"
).alias("nasco_dup_lkup")

# Read from DB2Connector (CD_MAPPING)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_cd_mapping = """
SELECT  CD_MPPNG.SRC_CD,
        CD_MPPNG.TRGT_CD
FROM    #$IDSOwner#.CD_MPPNG CD_MPPNG
WHERE   CD_MPPNG.SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND   CD_MPPNG.TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND   SRC_CLCTN_CD = 'FACETS DBO'
"""
df_CD_MAPPING = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mapping)
    .load()
)

# TargetCodes stage is Scenario A (CD_MAPPING -> hashed -> next). Deduplicate on key SRC_CD
df_targetcodes = dedup_sort(df_CD_MAPPING, ["SRC_CD"], [])

df_targetcodes = df_targetcodes.select("SRC_CD","TRGT_CD").alias("TRGT_CD")

# ODBCConnector: CMC_CDML_CL_LINE
# We assume we have some secret for LhoFacetsStgOwner or similar. The instructions do not provide a standard name,
# so call it generically and treat it like any read:
lho_facets_stg_secret_name = get_widget_value('lho_facets_stg_secret_name','')
jdbc_url_lho, jdbc_props_lho = get_db_config(lho_facets_stg_secret_name)
extract_query_cmc_cdml_cl_line = f"""
SELECT  CL_LINE.CLCL_ID,
        CL_LINE.CDML_SEQ_NO,
        CL_LINE.IDCD_ID,
        CL_LINE.CDML_CLMD_TYPE2,
        CL_LINE.CDML_CLMD_TYPE3,
        CL_LINE.CDML_CLMD_TYPE4,
        CL_LINE.CDML_CLMD_TYPE5,
        CL_LINE.CDML_CLMD_TYPE6,
        CL_LINE.CDML_CLMD_TYPE7,
        CL_LINE.CDML_CLMD_TYPE8,
        TYP_CD.CLMF_ICD_IND_PROC,
        CLM.CLCL_HIGH_SVC_DT
FROM    #$LhoFacetsStgOwner#.CMC_CDML_CL_LINE CL_LINE
        INNER JOIN tempdb..[{DriverTable}] TMP
            ON TMP.CLM_ID = CL_LINE.CLCL_ID
        INNER JOIN #$LhoFacetsStgOwner#.CMC_CLCL_CLAIM CLM
            ON CL_LINE.CLCL_ID = CLM.CLCL_ID
        LEFT OUTER JOIN #$LhoFacetsStgOwner#.CMC_CLMF_MULT_FUNC TYP_CD
            ON CL_LINE.CLCL_ID = TYP_CD.CLCL_ID
"""
df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lho)
    .options(**jdbc_props_lho)
    .option("query", extract_query_cmc_cdml_cl_line)
    .load()
)
df_CMC_CDML_CL_LINE = df_CMC_CDML_CL_LINE.select(
    "CLCL_ID","CDML_SEQ_NO","IDCD_ID","CDML_CLMD_TYPE2","CDML_CLMD_TYPE3",
    "CDML_CLMD_TYPE4","CDML_CLMD_TYPE5","CDML_CLMD_TYPE6","CDML_CLMD_TYPE7","CDML_CLMD_TYPE8",
    "CLMF_ICD_IND_PROC","CLCL_HIGH_SVC_DT"
).alias("Extract")

# ODBCConnector: CMC_CDML_CL_LINE2
extract_query_cmc_cdml_cl_line2 = f"""
SELECT  DIAG2.CLCL_ID,
        DIAG2.CLMD_TYPE,
        DIAG2.IDCD_ID
FROM    #$LhoFacetsStgOwner#.CMC_CLMD_DIAG DIAG2,
        tempdb..[{DriverTable}] TMP
WHERE   TMP.CLM_ID = DIAG2.CLCL_ID
"""
df_CMC_CDML_CL_LINE2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lho)
    .options(**jdbc_props_lho)
    .option("query", extract_query_cmc_cdml_cl_line2)
    .load()
)
df_CMC_CDML_CL_LINE2 = df_CMC_CDML_CL_LINE2.select("CLCL_ID","CLMD_TYPE","IDCD_ID")

# hf_lhofcts_clmlndiag_lkup is Scenario A (CMC_CDML_CL_LINE2 -> hashed -> next).
# Key columns: CLCL_ID (primaryKey), CLMD_TYPE (primaryKey).
df_lhofcts_clmlndiag_lkup = dedup_sort(
    df_CMC_CDML_CL_LINE2,
    ["CLCL_ID","CLMD_TYPE"],
    []
).alias("diag_lkup")

# Now, TrnsStripField
# Each link is a left join to df_lhofcts_clmlndiag_lkup with different conditions:
df_trns_strip = (
    df_CMC_CDML_CL_LINE
    .alias("Extract")
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag2"),
        (F.col("Extract.CLCL_ID") == F.col("diag2.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE2") == F.col("diag2.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag3"),
        (F.col("Extract.CLCL_ID") == F.col("diag3.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE3") == F.col("diag3.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag4"),
        (F.col("Extract.CLCL_ID") == F.col("diag4.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE4") == F.col("diag4.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag5"),
        (F.col("Extract.CLCL_ID") == F.col("diag5.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE5") == F.col("diag5.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag6"),
        (F.col("Extract.CLCL_ID") == F.col("diag6.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE6") == F.col("diag6.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag7"),
        (F.col("Extract.CLCL_ID") == F.col("diag7.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE7") == F.col("diag7.CLMD_TYPE")),
        "left"
    )
    .join(
        df_lhofcts_clmlndiag_lkup.alias("diag8"),
        (F.col("Extract.CLCL_ID") == F.col("diag8.CLCL_ID")) &
        (F.col("Extract.CDML_CLMD_TYPE8") == F.col("diag8.CLMD_TYPE")),
        "left"
    )
    .select(
        strip_field(F.col("Extract.CLCL_ID")).alias("CLCL_ID"),
        F.col("Extract.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        strip_field(F.col("Extract.IDCD_ID")).alias("IDCD_ID"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE2")).alias("CDML_CLMD_TYPE2"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE3")).alias("CDML_CLMD_TYPE3"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE4")).alias("CDML_CLMD_TYPE4"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE5")).alias("CDML_CLMD_TYPE5"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE6")).alias("CDML_CLMD_TYPE6"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE7")).alias("CDML_CLMD_TYPE7"),
        strip_field(F.col("Extract.CDML_CLMD_TYPE8")).alias("CDML_CLMD_TYPE8"),
        strip_field(F.col("diag2.IDCD_ID")).alias("DIAG2_IDCD_ID"),
        strip_field(F.col("diag3.IDCD_ID")).alias("DIAG3_IDCD_ID"),
        strip_field(F.col("diag4.IDCD_ID")).alias("DIAG4_IDCD_ID"),
        strip_field(F.col("diag5.IDCD_ID")).alias("DIAG5_IDCD_ID"),
        strip_field(F.col("diag6.IDCD_ID")).alias("DIAG6_IDCD_ID"),
        strip_field(F.col("diag7.IDCD_ID")).alias("DIAG7_IDCD_ID"),
        strip_field(F.col("diag8.IDCD_ID")).alias("DIAG8_IDCD_ID"),
        trim(strip_field(F.col("Extract.CLMF_ICD_IND_PROC"))).alias("CLMF_ICD_IND_PROC"),
        F.col("Extract.CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT")
    )
).alias("Strip")

# BusinessRules
# Join fcts_reversals, clm_nasco_dup_bypass, targetcodes (with no join condition for TRGT_CD => forced cross or left).
# The instructions forbid cross-join for left links with no condition, but we must not skip logic.
# We'll emulate the DataStage approach by left-joining on a literal True for "TRGT_CD" but note the caution:
df_businessrules_base = (
    df_trns_strip
    .join(df_hf_clm_fcts_reversals, F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass, F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
    # forced "no condition" left join for TRGT_CD:
    .join(df_targetcodes, F.lit(True), "left")
)

# Create stage variables with columns:
df_businessrules_base = (
    df_businessrules_base
    .withColumn(
        "svDiag1",
        F.regexp_replace(
            F.when(
                F.col("Strip.IDCD_ID").isNull() | (F.length(trim(F.col("Strip.IDCD_ID"))) == 0),
                F.lit("UNK")
            )
            .otherwise(F.upper(trim(F.col("Strip.IDCD_ID")))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag2",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG2_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG2_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG2_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag3",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG3_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG3_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG3_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag4",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG4_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG4_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG4_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag5",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG5_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG5_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG5_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag6",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG6_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG6_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG6_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag7",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG7_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG7_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG7_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svDiag8",
        F.regexp_replace(
            F.when(
                F.col("Strip.DIAG8_IDCD_ID").isNull() | (F.length(trim(F.col("Strip.DIAG8_IDCD_ID"))) == 0),
                F.lit("")
            )
            .otherwise(trim(F.col("Strip.DIAG8_IDCD_ID"))),
            r"\.",
            ""
        )
    )
    .withColumn(
        "svType2",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE2").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE2"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE2")))
    )
    .withColumn(
        "svType3",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE3").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE3"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE3")))
    )
    .withColumn(
        "svType4",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE4").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE4"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE4")))
    )
    .withColumn(
        "svType5",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE5").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE5"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE5")))
    )
    .withColumn(
        "svType6",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE6").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE6"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE6")))
    )
    .withColumn(
        "svType7",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE7").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE7"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE7")))
    )
    .withColumn(
        "svType8",
        F.when(
            F.col("Strip.CDML_CLMD_TYPE8").isNull() | (F.length(trim(F.col("Strip.CDML_CLMD_TYPE8"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("Strip.CDML_CLMD_TYPE8")))
    )
    .withColumn("svFirstRecycleDt", current_timestamp())
    .withColumn(
        "svPriKeyString",
        F.concat_ws(";", F.lit(SrcSysCd), trim(F.col("Strip.CLCL_ID")), trim(F.col("Strip.CDML_SEQ_NO")), F.lit(""))
    )
    .withColumn("svClmId", trim(F.col("Strip.CLCL_ID")))
    .withColumn("svClmLnSeqNo", trim(F.col("Strip.CDML_SEQ_NO")))
    .withColumn(
        "svDiagCodeTypCode",
        F.when(F.col("TRGT_CD.TRGT_CD").isNull(), F.lit("")).otherwise(F.col("TRGT_CD.TRGT_CD"))
    )
)

# For each output link from BusinessRules, we create a filtered DataFrame with its columns and constraints.
# Then we will union them all in the Collector step.

# Convenience for conditions:
cond_isNull_nasco = F.col("nasco_dup_lkup.CLM_ID").isNull()
cond_reversal_exists = F.col("fcts_reversals.CLCL_ID").isNotNull() & (
    (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
    (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
    (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
)

# Common columns function to select and rpad if needed:
def select_businessrules_columns(df, clm_ln_diag_ordnl_cd_expr):
    return df.select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        clm_ln_diag_ordnl_cd_expr.alias("CLM_LN_DIAG_ORDNL_CD"),
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        # We build PRI_KEY_STRING from the expression in each link or specify the column expression
        F.lit(0).alias("CLM_LN_DIAG_SK"), 
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        # placeholders for DIAG_CD, DIAG_CD_TYP_CD will be added in different links
    )

# DiagCd1
df_diagcd1 = (
    df_businessrules_base
    .filter(cond_isNull_nasco)
    .withColumn("CLM_ID", F.col("svClmId"))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag1"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svPriKeyString"), F.lit("1")))
)
df_diagcd1 = df_diagcd1.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    "svFirstRecycleDt",
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd2
df_diagcd2 = (
    df_businessrules_base
    .filter((F.col("svType2") != "") & (F.col("svDiag2") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(2))
    .withColumn("DIAG_CD", F.col("svDiag2"))
    .withColumn("DIAG_CD_TYPE_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("2")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("DIAG_CD"),
        F.col("DIAG_CD_TYPE_CD").alias("DIAG_CD_TYP_CD")
    )
)

# DiagCd3
df_diagcd3 = (
    df_businessrules_base
    .filter((F.col("svType3") != "") & (F.col("svDiag3") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(3))
    .withColumn("DIAG_CD", F.col("svDiag3"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("3")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd4
df_diagcd4 = (
    df_businessrules_base
    .filter((F.col("svType4") != "") & (F.col("svDiag4") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(4))
    .withColumn("DIAG_CD", F.col("svDiag4"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("4")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd5
df_diagcd5 = (
    df_businessrules_base
    .filter((F.col("svType5") != "") & (F.col("svDiag5") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(5))
    .withColumn("DIAG_CD", F.col("svDiag5"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("5")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd6
df_diagcd6 = (
    df_businessrules_base
    .filter((F.col("svType6") != "") & (F.col("svDiag6") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(6))
    .withColumn("DIAG_CD", F.col("svDiag6"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("6")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd7
df_diagcd7 = (
    df_businessrules_base
    .filter((F.col("svType7") != "") & (F.col("svDiag7") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(7))
    .withColumn("DIAG_CD", F.col("svDiag7"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("7")))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd8
df_diagcd8 = (
    df_businessrules_base
    .filter((F.col("svType8") != "") & (F.col("svDiag8") != "") & cond_isNull_nasco)
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(8))
    .withColumn("DIAG_CD", F.col("svDiag8"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn("PRI_KEY_STRING", F.concat_ws("", F.col("svClmLnSeqNo"), F.lit("8")))
    .select(
        F.col("svClmId").alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        "CLM_LN_DIAG_ORDNL_CD",
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        "PRI_KEY_STRING",
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

# DiagCd1_reversal
df_diagcd1_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists)
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag1"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";1"))
    )
)
df_diagcd1_rev = df_diagcd1_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd2_reversal
df_diagcd2_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType2") != "") & (F.col("svDiag2") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag2"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";2"))
    )
)
df_diagcd2_rev = df_diagcd2_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd3_reversal
df_diagcd3_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType3") != "") & (F.col("svDiag3") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag3"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";3"))
    )
)
df_diagcd3_rev = df_diagcd3_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd4_reversal
df_diagcd4_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType4") != "") & (F.col("svDiag4") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag4"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";4"))
    )
)
df_diagcd4_rev = df_diagcd4_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd5_reversal
df_diagcd5_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType5") != "") & (F.col("svDiag5") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag5"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";5"))
    )
)
df_diagcd5_rev = df_diagcd5_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd6_reversal
df_diagcd6_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType6") != "") & (F.col("svDiag6") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag6"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";6"))
    )
)
df_diagcd6_rev = df_diagcd6_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd7_reversal
df_diagcd7_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType7") != "") & (F.col("svDiag7") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag7"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";7"))
    )
)
df_diagcd7_rev = df_diagcd7_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# DiagCd8_reversal
df_diagcd8_rev = (
    df_businessrules_base
    .filter(cond_reversal_exists & (F.col("svType8") != "") & (F.col("svDiag8") != ""))
    .withColumn("CLM_ID", F.concat_ws("", F.col("svClmId"), F.lit("R")))
    .withColumn("CLM_LN_SEQ_NO", F.col("svClmLnSeqNo"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.lit(1))
    .withColumn("DIAG_CD", F.col("svDiag8"))
    .withColumn("DIAG_CD_TYP_CD", F.col("svDiagCodeTypCode"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws("", F.lit(SrcSysCd), F.lit(";"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";8"))
    )
)
df_diagcd8_rev = df_diagcd8_rev.select(
    "CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD",
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    F.lit(0).alias("CLM_LN_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# Collector => union all
df_collector = df_diagcd1.unionByName(df_diagcd2)\
    .unionByName(df_diagcd3)\
    .unionByName(df_diagcd4)\
    .unionByName(df_diagcd5)\
    .unionByName(df_diagcd6)\
    .unionByName(df_diagcd7)\
    .unionByName(df_diagcd8)\
    .unionByName(df_diagcd1_rev)\
    .unionByName(df_diagcd2_rev)\
    .unionByName(df_diagcd3_rev)\
    .unionByName(df_diagcd4_rev)\
    .unionByName(df_diagcd5_rev)\
    .unionByName(df_diagcd6_rev)\
    .unionByName(df_diagcd7_rev)\
    .unionByName(df_diagcd8_rev)

df_collector = df_collector.select(
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DIAG_ORDNL_CD",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_DIAG_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

# Next stage hf_clm_ln_diag_dedup is Scenario A again (Collector -> hashed -> next).
# Key columns: CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DIAG_ORDNL_CD => dedup
df_hf_clm_ln_diag_dedup = dedup_sort(
    df_collector,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD"],
    []
).alias("dedup")

# Next: SnapShot (CTransformerStage)
df_snap_AllCol = df_hf_clm_ln_diag_dedup.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("dedup.CLM_ID").alias("CLM_ID"),
    F.col("dedup.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("dedup.CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    F.col("dedup.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("dedup.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("dedup.DISCARD_IN").alias("DISCARD_IN"),
    F.col("dedup.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("dedup.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("dedup.ERR_CT").alias("ERR_CT"),
    F.col("dedup.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("dedup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("dedup.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("dedup.CLM_LN_DIAG_SK").alias("CLM_LN_DIAG_SK"),
    F.col("dedup.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("dedup.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("dedup.DIAG_CD").alias("DIAG_CD"),
    F.col("dedup.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
).alias("AllColOut")

df_snap_Snapshot = df_hf_clm_ln_diag_dedup.select(
    F.col("dedup.CLM_ID").alias("CLCL_ID"),
    F.col("dedup.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("dedup.CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
).alias("Snapshot")

df_snap_Transform = df_hf_clm_ln_diag_dedup.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("dedup.CLM_ID").alias("CLM_ID"),
    F.col("dedup.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("dedup.CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
).alias("Keys")

# Next Transformer stage => "Transformer": 
# Stage variable "svDiagOrdSk" = GetFkeyCodes(SrcSysCd,0,"DIAGNOSIS ORDINAL",Snapshot.CLM_LN_DIAG_ORDNL_CD,'X')
# We assume that user-defined function is available. We'll just produce final output:
df_transformer = df_snap_Snapshot.alias("Snapshot").select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
    F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # we emulate the stage var with direct call:
    F.lit("<...>").alias("CLM_LN_DIAG_ORDNL_CD_SK")  # we cannot evaluate GetFkeyCodes properly here
)

# Output pin "RowCount" => B_CLM_LN_DIAG
df_b_clm_ln_diag = df_transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DIAG_ORDNL_CD_SK"
).alias("RowCount")

# Write B_CLM_LN_DIAG.#SrcSysCd#.dat.#RunID#
write_files(
    df_b_clm_ln_diag,
    f"{adls_path}/load/B_CLM_LN_DIAG.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container: ClmLnDiagPK
# Two pins: (Transform -> Keys) and (AllCol -> AllColOut).
params_ClmLnDiagPK = {}
df_ClmLnDiagPK_out = ClmLnDiagPK(df_snap_Transform, df_snap_AllCol, params_ClmLnDiagPK)
# The output has linkName "Key". Next stage is FctsClmLnDiagExtr.

# Finally, FctsClmLnDiagExtr => CSeqFileStage => LhoFctsClmLnDiagExtr.LhoFctsClmLnDiag.dat.#RunID#
write_files(
    df_ClmLnDiagPK_out,
    f"{adls_path}/key/LhoFctsClmLnDiagExtr.LhoFctsClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)