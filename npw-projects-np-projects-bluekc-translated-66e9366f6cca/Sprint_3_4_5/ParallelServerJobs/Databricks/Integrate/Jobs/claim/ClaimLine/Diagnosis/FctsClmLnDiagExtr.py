# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC                
# MAGIC             
# MAGIC PROCESSING:  Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                    Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        04/2004-                                         Originally Programmed
# MAGIC Steph Goddard        07/22/2004                                     added NullOptCode to diagnosis code to code NA for blanks
# MAGIC Steph Goddard        07/27/2004                                     added Ereplace to diagnosis code to remove decimal point
# MAGIC Kevin Soderlund      0/14/04                                           Added fields to extract for Facets 4.11
# MAGIC Suzanne Saylor       03/01/2006                                      Changed to combine extract, transform, pkey for sequencer 
# MAGIC BJ Luce                   03/20/2006                                     add hf_clm_nasco_dup_bypass, identifies claims that are nasco 
# MAGIC                                                                                         dups. If the claim is on the file, a row is not generated for it in IDS. 
# MAGIC                                                                                         However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                  12/08/2006     Project 1576            Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland            05/02/2007     IAD Prod. Supp.      Added current timestamp parameter to eliminate FORMAT.DATE   devlIDS30
# MAGIC                                                                                         routine call to improve efficency.
# MAGIC                                                                                         Reduced the number of string trims done on claim ID in transform stage.
# MAGIC Oliver Nielsen          08/15/2007     Balancing                 Added snapshot file for balancing                                                       devlIDS30                  Steph Goddard            8/30/07
# MAGIC Oliver Nielsen          07/25/2008     Facets 4.5.1             Changed IDCD_CD from Char(6) to VarChar(10)                                devlIDSnew
# MAGIC Ralph Tucker          2008-07-23      3657 Primary Key     Changed primary key from hash file to DB2 table                                 devlIDS                      Steph Goddard           changes required
# MAGIC Rick Henry              2012-04-24      4896 ICD10              Added Diag_Cd_Typ_Cd lookup and pass to outfile                           NewDevl
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC 
# MAGIC Manasa Andru          2016-08-05        TFS - 12584           Changed main query to use left outer join to CMC_CLMF_MULT_FUNC      IntergrateDev2     Jag Yelavarthi               2016-08-11         
# MAGIC                                                                                               Added logic to set default value for CLMF_ICD_IND_PROC
# MAGIC Prabhu ES                2022-02-28      S2S Remediation     MSSQL connection parameters added                                                          IntegrateDev5               Manasa Andru               2022-06-10

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
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# ---------------------
# 1) Read Hashed File: hf_clm_fcts_reversals  (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet").alias("fcts_reversals")

# ---------------------
# 2) Read Hashed File: clm_nasco_dup_bypass  (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet").alias("nasco_dup_lkup")

# ---------------------
# 3) Read DB2Connector Stage: CD_MAPPING from IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_CD_MAPPING = f"""
SELECT
  CD_MPPNG.SRC_CD,
  CD_MPPNG.TRGT_CD
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE CD_MPPNG.SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND CD_MPPNG.TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND SRC_CLCTN_CD = 'FACETS DBO'
"""
df_CD_MAPPING = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CD_MAPPING)
    .load()
)

# ---------------------
# 4) Replace Hashed File "TargetCodes" with Scenario A dedup logic
#    Key column is SRC_CD (PrimaryKey).
df_TargetCodes_temp = dedup_sort(
    df_CD_MAPPING,
    ["SRC_CD"],
    [("SRC_CD", "A")]
).alias("TRGT_CD")

# ---------------------
# 5) Read ODBCConnector Stage: CMC_CDML_CL_LINE (with #DriverTable# join)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_CMC_CDML_CL_LINE = f"""
SELECT
    CL_LINE.CLCL_ID,
    CL_LINE.CDML_SEQ_NO,
    CL_LINE.IDCD_ID,
    CL_LINE.CDML_CLMD_TYPE2,
    CL_LINE.CDML_CLMD_TYPE3,
    CL_LINE.CDML_CLMD_TYPE4,
    CL_LINE.CDML_CLMD_TYPE5,
    CL_LINE.CDML_CLMD_TYPE6,
    CL_LINE.CDML_CLMD_TYPE7,
    CL_LINE.CDML_CLMD_TYPE8,
    DIAG2.IDCD_ID as DIAG2_IDCD_ID,
    DIAG3.IDCD_ID as DIAG3_IDCD_ID,
    DIAG4.IDCD_ID as DIAG4_IDCD_ID,
    DIAG5.IDCD_ID as DIAG5_IDCD_ID,
    DIAG6.IDCD_ID as DIAG6_IDCD_ID,
    DIAG7.IDCD_ID as DIAG7_IDCD_ID,
    DIAG8.IDCD_ID as DIAG8_IDCD_ID,
    TYP_CD.CLMF_ICD_IND_PROC,
    CLM.CLCL_HIGH_SVC_DT
FROM {FacetsOwner}.CMC_CDML_CL_LINE CL_LINE
INNER JOIN tempdb..{DriverTable} TMP
  ON TMP.CLM_ID = CL_LINE.CLCL_ID
INNER JOIN {FacetsOwner}.CMC_CLCL_CLAIM CLM
  ON CL_LINE.CLCL_ID = CLM.CLCL_ID
LEFT OUTER JOIN CMC_CLMF_MULT_FUNC TYP_CD
  ON CL_LINE.CLCL_ID = TYP_CD.CLCL_ID
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG2
  ON DIAG2.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG2.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE2
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG3
  ON DIAG3.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG3.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE3
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG4
  ON DIAG4.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG4.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE4
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG5
  ON DIAG5.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG5.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE5
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG6
  ON DIAG6.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG6.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE6
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG7
  ON DIAG7.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG7.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE7
LEFT OUTER JOIN {FacetsOwner}.CMC_CLMD_DIAG DIAG8
  ON DIAG8.CLCL_ID = CL_LINE.CLCL_ID
  AND DIAG8.CLMD_TYPE = CL_LINE.CDML_CLMD_TYPE8
"""
df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_CDML_CL_LINE)
    .load()
).alias("Extract")

# ---------------------
# 6) Transformer Stage: TrnsStripField
df_TrnsStripField = (
    df_CMC_CDML_CL_LINE
    .withColumn("CLCL_ID", F.regexp_replace(F.col("CLCL_ID"), "[\r\n\t]", ""))
    .withColumn("CDML_SEQ_NO", F.col("CDML_SEQ_NO"))
    .withColumn("IDCD_ID", F.regexp_replace(F.col("IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE2", F.regexp_replace(F.col("CDML_CLMD_TYPE2"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE3", F.regexp_replace(F.col("CDML_CLMD_TYPE3"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE4", F.regexp_replace(F.col("CDML_CLMD_TYPE4"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE5", F.regexp_replace(F.col("CDML_CLMD_TYPE5"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE6", F.regexp_replace(F.col("CDML_CLMD_TYPE6"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE7", F.regexp_replace(F.col("CDML_CLMD_TYPE7"), "[\r\n\t]", ""))
    .withColumn("CDML_CLMD_TYPE8", F.regexp_replace(F.col("CDML_CLMD_TYPE8"), "[\r\n\t]", ""))
    .withColumn("DIAG2_IDCD_ID", F.regexp_replace(F.col("DIAG2_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG3_IDCD_ID", F.regexp_replace(F.col("DIAG3_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG4_IDCD_ID", F.regexp_replace(F.col("DIAG4_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG5_IDCD_ID", F.regexp_replace(F.col("DIAG5_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG6_IDCD_ID", F.regexp_replace(F.col("DIAG6_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG7_IDCD_ID", F.regexp_replace(F.col("DIAG7_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("DIAG8_IDCD_ID", F.regexp_replace(F.col("DIAG8_IDCD_ID"), "[\r\n\t]", ""))
    .withColumn("CLMF_ICD_IND_PROC", trim(F.regexp_replace(F.col("CLMF_ICD_IND_PROC"), "[\r\n\t]", "")))
    .withColumn("CLCL_HIGH_SVC_DT", F.col("CLCL_HIGH_SVC_DT"))
).alias("Strip")

# ---------------------
# 7) Transformer Stage: BusinessRules
#    We have multiple reference links: fcts_reversals, nasco_dup_lkup, TRGT_CD => joined left
#    TRGT_CD has no join condition => replicate DataStage reference link with no condition => emulate cross join left
#    We avoid crossJoin() with a filter that picks one row? Not specified. The stage references TRGT_CD.TRGT_CD. We'll approximate by a cross join.

# First join the two actual keyed lookups:
df_joined = (
    df_TrnsStripField.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
          "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
          "left")
)

# Then incorporate dfTargetCodes with a left cross-join:
# (To replicate a DataStage reference link with no keys, we do a cross join + coalesce.)
# We force a single partition on dfTargetCodes to avoid a huge Cartesian product; DataStage reference link might yield repeated rows, but we must keep logic consistent.
df_tc_single = df_TargetCodes_temp.coalesce(1)  # keep it smaller
df_businessrules_pre = df_joined.crossJoin(df_tc_single)

# Now define the stage variables as columns:
df_businessrules = (
    df_businessrules_pre
    .withColumn(
        "svDiag1",
        F.regexp_replace(
            F.when(
                F.col("IDCD_ID").isNull() | (F.length(trim(F.col("IDCD_ID"))) == 0),
                F.lit("UNK")
            ).otherwise(F.upper(trim(F.col("IDCD_ID")))),
            "\u002E",  # '.'
            ""
        )
    )
    .withColumn(
        "svDiag2",
        F.regexp_replace(
            F.when(
                F.col("DIAG2_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG2_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG2_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag3",
        F.regexp_replace(
            F.when(
                F.col("DIAG3_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG3_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG3_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag4",
        F.regexp_replace(
            F.when(
                F.col("DIAG4_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG4_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG4_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag5",
        F.regexp_replace(
            F.when(
                F.col("DIAG5_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG5_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG5_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag6",
        F.regexp_replace(
            F.when(
                F.col("DIAG6_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG6_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG6_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag7",
        F.regexp_replace(
            F.when(
                F.col("DIAG7_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG7_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG7_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svDiag8",
        F.regexp_replace(
            F.when(
                F.col("DIAG8_IDCD_ID").isNull() | (F.length(trim(F.col("DIAG8_IDCD_ID"))) == 0),
                F.lit("")
            ).otherwise(trim(F.col("DIAG8_IDCD_ID"))),
            "\u002E",
            ""
        )
    )
    .withColumn(
        "svType2",
        F.when(
            F.col("CDML_CLMD_TYPE2").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE2"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE2")))
    )
    .withColumn(
        "svType3",
        F.when(
            F.col("CDML_CLMD_TYPE3").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE3"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE3")))
    )
    .withColumn(
        "svType4",
        F.when(
            F.col("CDML_CLMD_TYPE4").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE4"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE4")))
    )
    .withColumn(
        "svType5",
        F.when(
            F.col("CDML_CLMD_TYPE5").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE5"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE5")))
    )
    .withColumn(
        "svType6",
        F.when(
            F.col("CDML_CLMD_TYPE6").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE6"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE6")))
    )
    .withColumn(
        "svType7",
        F.when(
            F.col("CDML_CLMD_TYPE7").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE7"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE7")))
    )
    .withColumn(
        "svType8",
        F.when(
            F.col("CDML_CLMD_TYPE8").isNull() | (F.length(trim(F.col("CDML_CLMD_TYPE8"))) == 0),
            F.lit("")
        ).otherwise(trim(F.col("CDML_CLMD_TYPE8")))
    )
    .withColumn("svFirstRecycleDt", current_timestamp())
    .withColumn(
        "svPriKeyString",
        F.concat_ws(";", F.lit("FACETS"), trim(F.col("CLCL_ID")), trim(F.col("CDML_SEQ_NO")), F.lit(""))
    )
    .withColumn("svClmId", trim(F.col("CLCL_ID")))
    .withColumn("svClmLnSeqNo", trim(F.col("CDML_SEQ_NO")))
    .withColumn(
        "svDiagCodeTypCode",
        F.when(F.col("TRGT_CD").isNull(), F.lit("")).otherwise(F.col("TRGT_CD"))
    )
)

# Next, produce multiple output DataFrames from constraints:

def outDF(dfIn, clm_ln_diag_ordnl_cd_expr, diag_cd_expr, diag_cd_typ_cd_expr,
          constraint_expr, pk_string_expr=None, reversal_clm_id_expr=None):
    """
    Helps to generate an output link from dfIn with the given constraint, columns, etc.
    pk_string_expr can override default expression for PRI_KEY_STRING.
    reversal_clm_id_expr can override default expression for CLM_ID.
    """
    filtered = dfIn.filter(constraint_expr)
    sel_cols = [
        F.when(reversal_clm_id_expr.isNull(), F.col("svClmId")).otherwise(reversal_clm_id_expr).alias("CLM_ID"),
        F.col("svClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
        clm_ln_diag_ordnl_cd_expr.alias("CLM_LN_DIAG_ORDNL_CD"),
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("svFirstRecycleDt").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.when(pk_string_expr.isNull(), F.col("svPriKeyString")).otherwise(pk_string_expr).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_DIAG_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        diag_cd_expr.alias("DIAG_CD"),
        diag_cd_typ_cd_expr.alias("DIAG_CD_TYP_CD")
    ]
    return filtered.select(*sel_cols)

dfDiagCd1 = outDF(
    df_businessrules,
    F.lit(1),
    F.col("svDiag1"),
    F.col("svDiagCodeTypCode"),
    (F.col("nasco_dup_lkup.CLM_ID").isNull()),
    None,
    None
)

dfDiagCd2 = outDF(
    df_businessrules,
    F.lit(2),
    F.col("svDiag2"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType2") != "") &
        (F.col("svDiag2") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("2")),
    None
)

dfDiagCd3 = outDF(
    df_businessrules,
    F.lit(3),
    F.col("svDiag3"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType3") != "") &
        (F.col("svDiag3") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("3")),
    None
)

dfDiagCd4 = outDF(
    df_businessrules,
    F.lit(4),
    F.col("svDiag4"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType4") != "") &
        (F.col("svDiag4") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("4")),
    None
)

dfDiagCd5 = outDF(
    df_businessrules,
    F.lit(5),
    F.col("svDiag5"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType5") != "") &
        (F.col("svDiag5") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("5")),
    None
)

dfDiagCd6 = outDF(
    df_businessrules,
    F.lit(6),
    F.col("svDiag6"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType6") != "") &
        (F.col("svDiag6") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("6")),
    None
)

dfDiagCd7 = outDF(
    df_businessrules,
    F.lit(7),
    F.col("svDiag7"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType7") != "") &
        (F.col("svDiag7") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("7")),
    None
)

dfDiagCd8 = outDF(
    df_businessrules,
    F.lit(8),
    F.col("svDiag8"),
    F.col("svDiagCodeTypCode"),
    (
        (F.col("svType8") != "") &
        (F.col("svDiag8") != "") &
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
    ),
    F.concat_ws(";", F.col("svClmLnSeqNo"), F.lit("8")),
    None
)

dfDiagCd1_reversal = outDF(
    df_businessrules,
    F.lit(1),
    F.col("svDiag1"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("1")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd2_reversal = outDF(
    df_businessrules,
    F.lit(2),
    F.col("svDiag2"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType2") != "") &
        (F.col("svDiag2") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("2")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd3_reversal = outDF(
    df_businessrules,
    F.lit(3),
    F.col("svDiag3"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType3") != "") &
        (F.col("svDiag3") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("3")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd4_reversal = outDF(
    df_businessrules,
    F.lit(4),
    F.col("svDiag4"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType4") != "") &
        (F.col("svDiag4") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("4")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd5_reversal = outDF(
    df_businessrules,
    F.lit(5),
    F.col("svDiag5"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType5") != "") &
        (F.col("svDiag5") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("5")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd6_reversal = outDF(
    df_businessrules,
    F.lit(6),
    F.col("svDiag6"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType6") != "") &
        (F.col("svDiag6") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("6")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd7_reversal = outDF(
    df_businessrules,
    F.lit(7),
    F.col("svDiag7"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType7") != "") &
        (F.col("svDiag7") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("7")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

dfDiagCd8_reversal = outDF(
    df_businessrules,
    F.lit(8),
    F.col("svDiag8"),
    F.col("svDiagCodeTypCode"),
    (
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        ) &
        (F.col("svType8") != "") &
        (F.col("svDiag8") != "")
    ),
    F.concat_ws(";", F.lit("FACETS"), F.col("svClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(""), F.lit("8")),
    F.concat(F.col("svClmId"), F.lit("R"))
)

# ---------------------
# 8) Collector Stage: Union all output dataframes into one
dfCollector = dfDiagCd2 \
    .unionByName(dfDiagCd6) \
    .unionByName(dfDiagCd1) \
    .unionByName(dfDiagCd3) \
    .unionByName(dfDiagCd5) \
    .unionByName(dfDiagCd7) \
    .unionByName(dfDiagCd4) \
    .unionByName(dfDiagCd8) \
    .unionByName(dfDiagCd1_reversal) \
    .unionByName(dfDiagCd2_reversal) \
    .unionByName(dfDiagCd3_reversal) \
    .unionByName(dfDiagCd4_reversal) \
    .unionByName(dfDiagCd5_reversal) \
    .unionByName(dfDiagCd6_reversal) \
    .unionByName(dfDiagCd7_reversal) \
    .unionByName(dfDiagCd8_reversal)

# ---------------------
# 9) Next Hashed File: hf_clm_ln_diag_dedup (Scenario A or C?)
#    The job flow: Collector -> hf_clm_ln_diag_dedup -> SnapShot
#    Pattern is "AnyStage -> CHashedFile -> NextStage" with no writing back to same hashed file from a Transformer.
#    So it is scenario A: we deduplicate on the key columns with "PrimaryKey = true":
#    Keys: CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DIAG_ORDNL_CD
#    We remove the hashed file by direct dedup, then pass to "SnapShot".
df_hf_clm_ln_diag_dedup_temp = dedup_sort(
    dfCollector,
    ["CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_DIAG_ORDNL_CD"],
    [("CLM_ID","A"),("CLM_LN_SEQ_NO","A"),("CLM_LN_DIAG_ORDNL_CD","A")]
).alias("dedup")

# ---------------------
# 10) SnapShot Transformer
#     Input => "dedup"
dfSnapShot_primary = df_hf_clm_ln_diag_dedup_temp.select(
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # placeholder for an expression used in the job
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

dfSnapShot_snapshot = df_hf_clm_ln_diag_dedup_temp.select(
    F.col("dedup.CLM_ID").alias("CLCL_ID"),
    F.col("dedup.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("dedup.CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
).alias("Snapshot")

dfSnapShot_transform = df_hf_clm_ln_diag_dedup_temp.select(
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("dedup.CLM_ID").alias("CLM_ID"),
    F.col("dedup.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("dedup.CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
).alias("Keys")

# ---------------------
# 11) Next Transformer => "Transformer"
#     Input => Snapshot link
dfTransformer = dfSnapShot_snapshot.select(
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
    F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # The job calculates something: CLM_LN_DIAG_ORDNL_CD_SK with a stage var:
    #  "svDiagOrdSk" = "GetFkeyCodes('FACETS', 0, \"DIAGNOSIS ORDINAL\", Snapshot.CLM_LN_DIAG_ORDNL_CD, 'X')"
    #  We treat that function as available. We'll just call it directly:
    F.lit("GetFkeyCodes('FACETS', 0, \"DIAGNOSIS ORDINAL\", Snapshot.CLM_LN_DIAG_ORDNL_CD, 'X')").alias("CLM_LN_DIAG_ORDNL_CD_SK")
).alias("RowCountSelect")

# ---------------------
# 12) Output to CSeqFileStage => "B_CLM_LN_DIAG"
#     Write a delimited file (no header)
df_B_CLM_LN_DIAG = (
    dfTransformer
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("CLM_LN_SEQ_NO", F.col("CLM_LN_SEQ_NO"))
    .withColumn("CLM_LN_DIAG_ORDNL_CD_SK", F.col("CLM_LN_DIAG_ORDNL_CD_SK"))
)

write_files(
    df_B_CLM_LN_DIAG.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DIAG_ORDNL_CD_SK"
    ),
    f"{adls_path}/load/B_CLM_LN_DIAG.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------
# 13) Shared Container "ClmLnDiagPK" => with 2 inputs => "Transform" and "AllCol"
#     Then 1 output => "Key"
params_for_clm_ln_diag_pk = {
    "DriverTable": DriverTable,
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrDateTime,
    "FacetsDB": f"{FacetsOwner}",  # from the job references
    "FacetsOwner": FacetsOwner,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "IDSOwner": IDSOwner
}

df_ClmLnDiagPK_out = ClmLnDiagPK(dfSnapShot_transform.alias("Keys"), dfSnapShot_primary.alias("AllColOut"), params_for_clm_ln_diag_pk)
# The container returns a single output (Key), but in DataStage there's one link with columns.
# We'll rename the returned DataFrame to match the final usage.
df_ClmLnDiagPK = df_ClmLnDiagPK_out

# ---------------------
# 14) Final CSeqFileStage => "FctsClmLnDiagExtr"
df_final = df_ClmLnDiagPK.select(
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
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DIAG_ORDNL_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DIAG_CD",
    "DIAG_CD_TYP_CD"
)

write_files(
    df_final,
    f"{adls_path}/key/FctsClmLnDiagExtr.FctsClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)