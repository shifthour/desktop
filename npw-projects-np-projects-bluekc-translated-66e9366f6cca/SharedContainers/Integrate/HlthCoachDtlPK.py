# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ---------- 

"""
Shared Container: HlthCoachDtlPK
Folder Path     : Shared Containers/PrimaryKey
Job Type        : Server Job
Job Category    : DS_Integrate

Description:
Takes the sorted file and applies the primary key. Preps the file for the foreign key lookup process that follows.
*****************************************************************************************************************************************************************************
COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME:     IdsClmDiagPkey

MODIFICATIONS:
  Steph Goddard 03/22/2004      - Originally Programmed.
  Steph Goddard 02/07/2007      - Changed documentation, added PCTAClmDiagExtr
  Kalyan Neelam  2010-07-01      - Took out IDS_SK and put in HLTH_COACH_DTL_SK to get the PK SK.
                                  Assigned job parm CurrRunCycle to the CRT_RUN_CYC_EXCTN_SK in the updt link
  Kalyan Neelam  2011-05-17 4673 - Added HLTH_COACH_DTL_CLSD_RSN_CD, HLTH_COACH_DTL_STTUS_CD, CLSD_DT, INIT_ENR_DT
  Kalyan Neelam  2012-10-31 4830 - Added HLTH_COACH_SESS_TYP_CD, HLTH_COACH_TYP_CD, HLTH_COACH_SESS_FOCUS_TX
  Kalyan Neelam  2013-07-30 5063 - Added HLTH_COACH_PRI_INTNTN_CD, HLTH_COACH_NM, HLTH_COACH_PHN_NO
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_HlthCoachDtlPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the HlthCoachDtlPK shared-container logic.

    Parameters
    ----------
    df_Transform : DataFrame
        Input DataFrame coming in on link "Transform".
    params : dict
        Runtime parameters passed from the parent job / notebook.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to link "Key".
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # Read dummy table that replaces hashed file hf_hlth_coach_dtl
    # ------------------------------------------------------------------
    extract_query = """
        SELECT
            SRC_SYS_CD                              AS SRC_SYS_CD_lkp,
            MBR_UNIQ_KEY                            AS MBR_UNIQ_KEY_lkp,
            HLTH_COACH_ID                           AS HLTH_COACH_ID_lkp,
            CNTCT_DT_SK                             AS CNTCT_DT_SK_lkp,
            CNTCT_TM                                AS CNTCT_TM_lkp,
            HLTH_COACH_DTL_INTERACTN_CD             AS HLTH_COACH_DTL_INTERACTN_CD_lkp,
            CRT_RUN_CYC_EXCTN_SK                    AS CRT_RUN_CYC_EXCTN_SK_lkp,
            HLTH_COACH_DTL_SK                       AS HLTH_COACH_DTL_SK_lkp
        FROM <dummy_hf_hlth_coach_dtl>
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # Join input with lookup table
    # ------------------------------------------------------------------
    t  = df_Transform.alias("t")
    lk = df_lkup.alias("lk")

    join_expr = (
        (t.SRC_SYS_CD                == lk.SRC_SYS_CD_lkp) &
        (t.MBR_UNIQ_KEY              == lk.MBR_UNIQ_KEY_lkp) &
        (t.HLTH_COACH_ID             == lk.HLTH_COACH_ID_lkp) &
        (t.CNTCT_DT                  == lk.CNTCT_DT_SK_lkp) &
        (t.CNTCT_TM                  == lk.CNTCT_TM_lkp) &
        (t.HLTH_COACH_DTL_INTERACTN_CD == lk.HLTH_COACH_DTL_INTERACTN_CD_lkp)
    )

    df_join = t.join(lk, join_expr, "left")

    # ------------------------------------------------------------------
    # Flag to replicate lkup.NOTFOUND
    # ------------------------------------------------------------------
    df_join = df_join.withColumn(
        "lkup_notfound",
        F.col("HLTH_COACH_DTL_SK_lkp").isNull()
    ).withColumn(
        "CurrRunCycle",
        F.lit(CurrRunCycle)
    )

    # ------------------------------------------------------------------
    # Prepare surrogate-key column for SurrogateKeyGen
    # ------------------------------------------------------------------
    df_join = df_join.withColumn(
        "HLTH_COACH_DTL_SK",
        F.when(F.col("lkup_notfound"), F.lit(None)).otherwise(F.col("HLTH_COACH_DTL_SK_lkp"))
    )

    # ------------------------------------------------------------------
    # Call SurrogateKeyGen (mandatory replacement for KeyMgtGetNextValueConcurrent)
    # ------------------------------------------------------------------
    df_enriched = df_join
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"HLTH_COACH_DTL_SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Post-generation aliases & additional derived columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_enriched
        .withColumn("SK", F.col("HLTH_COACH_DTL_SK"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lkup_notfound"), F.col("CurrRunCycle"))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # ------------------------------------------------------------------
    # Build DataFrame for 'Key' output link
    # ------------------------------------------------------------------
    df_key = (
        df_enriched.select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            F.col("SK").alias("HLTH_COACH_DTL_SK"),
            "MBR_UNIQ_KEY",
            "HLTH_COACH_ID",
            F.col("CNTCT_DT").alias("CNTCT_DT_SK"),
            "CNTCT_TM",
            "HLTH_COACH_DTL_INTERACTN_CD",
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "GRP_ID",
            "HLTH_COACH_DTL_ACTN_CD",
            "HLTH_COACH_DTL_RSLT_CD",
            "CNTCT_MIN_NO",
            "HLTH_COACH_DTL_CLSD_RSN_CD",
            "HLTH_COACH_DTL_STTUS_CD",
            "CLSD_DT",
            "INIT_ENR_DT",
            "HLTH_COACH_SESS_TYP_CD",
            "HLTH_COACH_TYP_CD",
            "HLTH_COACH_SESS_FOCUS_TX",
            "HLTH_COACH_NM",
            "HLTH_COACH_PHN_NO",
            "HLTH_COACH_PRI_INTNTN_CD"
        )
    )

    # ------------------------------------------------------------------
    # Build DataFrame for 'updt' link (rows to insert into dummy table)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(F.col("lkup_notfound") == F.lit(True))
        .select(
            "SRC_SYS_CD",
            "MBR_UNIQ_KEY",
            "HLTH_COACH_ID",
            F.col("CNTCT_DT").alias("CNTCT_DT_SK"),
            "CNTCT_TM",
            "HLTH_COACH_DTL_INTERACTN_CD",
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("HLTH_COACH_SK")
        )
    )

    # ------------------------------------------------------------------
    # Write back to dummy table that replaces hashed file
    # ------------------------------------------------------------------
    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", "<dummy_hf_hlth_coach_dtl>")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_key