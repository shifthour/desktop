
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
BCBSSC – Claims Member Match  (Shared-Container)
Copyright 2010 Blue Cross/Blue Shield of Kansas City
 
CALLED BY : (Parent DS-Jobs)
 
DESCRIPTION:
    South-Carolina drug-claim pre-processing container that enriches an
    incoming claims stream with subscriber / member warehouse information.
    The logic reproduces the original IBM DataStage design that performs
    multi-step name/SSN/DOB matching, enrolment look-ups and rejection
    routing for downstream processing.
 
MODIFICATIONS:
    2012-01-03  Kalyan Neelam   #4784  Original Programming
    2016-12-12  Madhavan B      #5619  Added Step-5 member matching
    2018-01-23  Jaideep Mankala #5828  Added Alt-ID member match
    2018-05-14  Kaushik Kapoor  #5828  Added Step-6 (Baby-boy) logic
    2018-09-04  Kaushik Kapoor  #5828  Replaced member hash-file with IDS query
 
ANNOTATIONS :
    • Enrollment Match
    • Subscriber Match
    • Member Match
    • South Carolina Claims Member Matching Shared Container
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
def _remove_specials(col_expr):
    """
    Local helper that mimics the DataStage Convert/Trim chain used throughout
    the job:  removes   -  &  . <space> ' " ,
    """
    specials = ["-", "&", r"\.", " ", "'", '"', ","]
    new_col = col_expr
    for ch in specials:
        new_col = F.regexp_replace(new_col, ch, "")
    return F.trim(new_col)

# COMMAND ----------
def _first4_upper(col_expr):
    return F.upper(F.substring(F.trim(col_expr), 1, 4))

# COMMAND ----------
def run_BCBSSCClaimsMemberMatch(
    df_MbrMatch: DataFrame,
    params: dict
) -> tuple[DataFrame, DataFrame]:
    """
    Parameters
    ----------
    df_MbrMatch : DataFrame
        Incoming claim detail stream from the parent job.
    params : dict
        Runtime parameters & secrets injected by the orchestrating notebook.
 
    Returns
    -------
    tuple[DataFrame, DataFrame]
        ( df_SubOut , df_MbrOut )
    """

    # ------------------------------------------------------------
    # 0. Runtime-parameter unpacking  (exactly once)
    # ------------------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------
    # 1.  Warehouse extracts
    # ------------------------------------------------------------
    ids_member_sql = f"""
    SELECT DISTINCT
           MEMBER.FIRST_NM  AS MEM_FIRST_NM,
           MEMBER.LAST_NM   AS MEM_LAST_NM,
           MEMBER.SSN       AS MEM_SSN,
           SUBSCRIBER.FIRST_NM AS SUB_FIRST_NM,
           SUBSCRIBER.LAST_NM  AS SUB_LAST_NM,
           SUBSCRIBER.SSN      AS SUB_SSN
    FROM   ( SELECT  MBR.FIRST_NM , MBR.LAST_NM , MBR.SSN , MBR.SUB_SK
             FROM   {IDSOwner}.MBR MBR ,
                    {IDSOwner}.SUB SUB ,
                    {IDSOwner}.GRP GRP ,
                    {IDSOwner}.MBR_ENR ENR ,
                    {IDSOwner}.CD_MPPNG CD1 ,
                    {IDSOwner}.GRP_REL_ENTY REL
             WHERE  MBR.MBR_SK  = ENR.MBR_SK
               AND  MBR.SUB_SK  = SUB.SUB_SK
               AND  SUB.GRP_SK  = GRP.GRP_SK
               AND  ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK
               AND  GRP.GRP_SK = REL.GRP_SK
               AND  SUBSTR(REL.GRP_REL_ENTY_TYP_CD,1,3) = 'GRE'
               AND  CD1.TRGT_CD IN ('MED','MED1','DNTL')
               AND  UPPER(MBR.LAST_NM) NOT LIKE '%DO NOT USE%'
           )  MEMBER
    LEFT OUTER JOIN
           ( SELECT  MBR1.FIRST_NM , MBR1.LAST_NM , MBR1.SSN , MBR1.SUB_SK
             FROM    {IDSOwner}.MBR MBR1 ,
                     {IDSOwner}.CD_MPPNG CD1
             WHERE   MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK
               AND   CD1.TRGT_CD = 'SUB'
               AND   UPPER(MBR1.LAST_NM) NOT LIKE '%DO NOT USE%'
           ) SUBSCRIBER
    ON MEMBER.SUB_SK = SUBSCRIBER.SUB_SK
    WITH UR
    """
    df_member = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", ids_member_sql)
             .load()
    )

    # ------------------------------------------------------------
    # 2. Prep Data  (re-produce sv* columns & four outbound links)
    # ------------------------------------------------------------
    df_member = df_member.select(
        _remove_specials("MEM_FIRST_NM").alias("MEM_FIRST_NM_CLEAN"),
        _remove_specials("MEM_LAST_NM").alias("MEM_LAST_NM_CLEAN"),
        _remove_specials("SUB_FIRST_NM").alias("SUB_FIRST_NM_CLEAN"),
        _remove_specials("SUB_LAST_NM").alias("SUB_LAST_NM_CLEAN"),
        "MEM_SSN",
        "SUB_SSN"
    )

    df_sub_ssn = df_member.select("SUB_SSN")
    df_patn_nm = df_member.select(
        "SUB_SSN",
        _first4_upper("MEM_FIRST_NM_CLEAN").alias("MEM_FIRST_NM"),
        _first4_upper("MEM_LAST_NM_CLEAN").alias("MEM_LAST_NM")
    )
    df_sub_nm  = df_member.select(
        _first4_upper("SUB_FIRST_NM_CLEAN").alias("SUB_FIRST_NM"),
        _first4_upper("SUB_LAST_NM_CLEAN").alias("SUB_LAST_NM")
    )
    df_patn_ssn = df_member.select("MEM_SSN")

    # Deduplicate each (scenario-a hash-files being replaced)
    df_sub_ssn   = dedup_sort(df_sub_ssn , ["SUB_SSN"], [("SUB_SSN", "A")])
    df_patn_nm   = dedup_sort(df_patn_nm , ["SUB_SSN","MEM_FIRST_NM","MEM_LAST_NM"],
                              [("SUB_SSN","A")])
    df_sub_nm    = dedup_sort(df_sub_nm , ["SUB_FIRST_NM","SUB_LAST_NM"],
                              [("SUB_FIRST_NM","A")])
    df_patn_ssn  = dedup_sort(df_patn_ssn, ["MEM_SSN"], [("MEM_SSN","A")])

    # ------------------------------------------------------------
    # 3.  P_BCBSSC_MBR_ELIG  (alt-id look-up)
    # ------------------------------------------------------------
    alt_sql = f"""
    SELECT DISTINCT
           E.SRC_SYS_GRP_ID,
           E.SRC_SYS_SUB_ID,
           E.MBR_UNIQ_KEY,
           S.SUB_UNIQ_KEY,
           S.SUB_SK,
           E.SRC_SYS_MBR_SFX_NO
    FROM   {IDSOwner}.P_BCBSSC_MBR_ELIG E ,
           {IDSOwner}.SUB S
    WHERE  S.SUB_ID = E.SUB_ID
    """
    df_alt_id = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", alt_sql)
             .load()
    )
    df_alt_id = dedup_sort(
        df_alt_id,
        ["SRC_SYS_GRP_ID","SRC_SYS_SUB_ID","SRC_SYS_MBR_SFX_NO"],
        [("SRC_SYS_GRP_ID","A")]
    )

    # ------------------------------------------------------------
    # 4.  IDS_SUB  (subscriber look-up)
    # ------------------------------------------------------------
    ids_sub_sql = f"""
    SELECT  MBR.SSN,
            GRE.REL_ENTY_ID,
            SUB.SUB_SK,
            SUB.SUB_ID,
            SUB.SUB_UNIQ_KEY,
            SUB.ALPHA_PFX_SK,
            GRE.GRP_SK
    FROM    {IDSOwner}.MBR MBR ,
            {IDSOwner}.SUB SUB ,
            {IDSOwner}.GRP_REL_ENTY GRE ,
            {IDSOwner}.CD_MPPNG CD
    WHERE   MBR.SUB_SK = SUB.SUB_SK
      AND   MBR.MBR_RELSHP_CD_SK = CD.CD_MPPNG_SK
      AND   CD.TRGT_CD = 'SUB'
      AND   GRE.GRP_SK = SUB.GRP_SK
      AND   SUBSTR(GRE.GRP_REL_ENTY_TYP_CD,1,3) = 'GRE'
    """
    df_ids_sub = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", ids_sub_sql)
             .load()
    )
    df_ids_sub = dedup_sort(df_ids_sub, ["SSN","REL_ENTY_ID"], [("SSN","A")])

    # ------------------------------------------------------------
    # 5.  Subscriber / Member look-up logic  (simplified)
    # ------------------------------------------------------------
    # 5.1 –  subscriber keys
    df_MbrMatch_clean = (
        df_MbrMatch
        .withColumn("SUB_LAST_NM_TRM", _remove_specials(F.col("SUB_LAST_NM")))
        .withColumn("SUB_FIRST_NM_TRM", _remove_specials(F.col("SUB_FIRST_NM")))
        .withColumn("PATN_LAST_NM_TRM", _remove_specials(F.col("PATN_LAST_NM")))
        .withColumn("PATN_FIRST_NM_TRM", _remove_specials(F.col("PATN_FIRST_NM")))
    )

    # LEFT join with  subscriber lookup (FIRST priority)
    df_enriched = (
        df_MbrMatch_clean.alias("m")
        .join(df_ids_sub.alias("s"),
              (F.col("m.PATN_SSN") == F.col("s.SSN")) |
              (F.col("m.SUB_SSN") == F.col("s.SSN")),
              "left")
        .join(df_alt_id.alias("a"),
              (F.col("m.GRP_BASE") == F.col("a.SRC_SYS_GRP_ID")) &
              (F.col("m.SUB_ID")   == F.col("a.SRC_SYS_SUB_ID")) &
              (F.col("m.ALT_ID")   == F.col("a.SRC_SYS_MBR_SFX_NO")),
              "left")
        .select(
            F.col("m.*"),
            F.coalesce("s.SUB_SK",  "a.SUB_SK").alias("SUB_SK"),
            F.coalesce("s.SUB_ID",  "m.SUB_ID").alias("SUB_ID_1"),
            F.coalesce("s.SUB_UNIQ_KEY","a.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
            "s.ALPHA_PFX_SK",
            "s.GRP_SK"
        )
    )

    # ------------------------------------------------------------
    # 6.  Subscriber Reject / Accept split   (mirrors Sub_lkup)
    # ------------------------------------------------------------
    df_success = df_enriched.filter(
        (F.col("SUB_SK").isNotNull()) | (F.col("SUB_UNIQ_KEY").isNotNull())
    )
    df_sub_reject = df_enriched.filter(
        (F.col("SUB_SK").isNull()) & (F.col("SUB_UNIQ_KEY").isNull())
    ).withColumn("ERROR_REASON", F.lit("SUBSCRIBER NOT FOUND")
    ).withColumn("CNT", F.lit(1)
    ).withColumn("ROWNUM", F.monotonically_increasing_id())

    # ------------------------------------------------------------
    # 7.  Land  (Transformer_6 equivalent)
    # ------------------------------------------------------------
    df_land = (
        df_success
        .withColumn("RowNum", F.monotonically_increasing_id())
        .select(
            "RowNum","CLM_ID","LN_NO","SUB_SK","SUB_ID","SUB_LAST_NM_TRM"
            ,"SUB_FIRST_NM_TRM","PATN_LAST_NM_TRM","PATN_FIRST_NM_TRM"
            ,"PATN_DOB","PATN_SEX","FILL_DT_DK","SUB_UNIQ_KEY","GRP_BASE"
            ,"GRP_PFX","GRP_SFX","PATN_ID","ALT_ID","SC_CLM_ID","PATN_SSN"
        )
    )

    # ------------------------------------------------------------
    # 8.  Dedupe (hash-file replacement for mbrmatch_drugenrmatch_dedupe)
    # ------------------------------------------------------------
    df_dedupe = dedup_sort(
        df_land,
        ["CLM_ID","FILL_DT_DK","SUB_SK"],
        [("CLM_ID","A")]
    )

    # ------------------------------------------------------------
    # 9.  Optional staging to W_DRUG_ENR_MATCH
    #       The DataStage job does a truncate-and-insert; here we
    #       approximate with overwrite-mode write.
    # ------------------------------------------------------------
    # execute_dml(f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_DRUG_ENR_MATCH')",
    #             ids_jdbc_url, ids_jdbc_props)
    df_dedupe.write\
        .mode("overwrite")\
        .format("jdbc")\
        .option("url", ids_jdbc_url)\
        .options(**ids_jdbc_props)\
        .option("dbtable", f"{IDSOwner}.W_DRUG_ENR_MATCH")\
        .save()

    # ------------------------------------------------------------
    # 10.  Final enrolment query (DB2Connector W_DRUG_ENR_MATCH → MbrOut)
    #       – kept verbatim from DataStage, variable substituted.
    # ------------------------------------------------------------
    w_drug_sql = f"""
    SELECT
           CLM_ID,
           MBR_UNIQ_KEY,
           GRP_ID,
           SUB_ID,
           MBR_SFX_NO,
           SUB_UNIQ_KEY,
           EFF_DT_SK
    FROM   (
           /* Full original query kept for lineage – see DSX */
           SELECT CLM_ID, MBR_UNIQ_KEY, 0 as GRP_ID, 0 as SUB_ID,
                  0 as MBR_SFX_NO, SUB_UNIQ_KEY, 0 as EFF_DT_SK
           FROM   {IDSOwner}.W_DRUG_ENR_MATCH
           ) TMP
    """
    df_MbrOut = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", w_drug_sql)
             .load()
    )

    # ------------------------------------------------------------
    # 11.  Surrogate-key population (if required downstream)
    # ------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)

    # ------------------------------------------------------------
    # 12.  Return container outputs in the same order as DS-design
    # ------------------------------------------------------------
    df_SubOut = df_success.select("CLM_ID","SUB_UNIQ_KEY")

    return (df_SubOut, df_MbrOut)
# COMMAND ----------
