# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY:   FctsClmDiagExtr and NascoClmDiagExtr

DESCRIPTION:   Shared container used for Primary Keying of Claim Diagnosis


PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Ralph Tucker     2008-07-23               Initial program                                                                    3657 Primary Key    devlIDS                              Steph Goddard          07/27/2008


Parik                  2008-08-13               Was using the wrong generated key value as compared  3657 Primary Key    devlIDS                               Steph Goddard         08/19/2008
                                                             to production, changed that value

SAndrew          2008-12-22               Added new field CLM_DIAG_POA_CD to input file                DRG                       devlIDS                            Brent Leland             12-29-2008
Rick Henry        2012-05-01               added field DIAG_CD_TYP_CD to end of input / output file   4896 ICD10                                                  SAndrew                    2012-05-17
"""

# Temp table is tuncated before load and runstat done after load
# Assign primary surrogate key
# Load IDS temp. table
# join primary key info with table info
# update primary key table (K_CLM_DIAG) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# IDS Primary Key Container for Claim Diagnosis
# This container is used in:
# FctsClmDiagExtr
# NascoClmDiagExtr
#
# These programs need to be re-compiled when logic changes
# Hashfile cleared in calling program
# Hash file (hf_clm_diag_allcol) cleared in the calling program


def run_ClmDiagPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    # ------------------ Parameter Unpacking ------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ---------------------------------------------------------

    # ------------------ Stage: hf_clm_diag_allcol (Scenario a – intermediate) ------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_DIAG_ORDNL_CD"],
        [("SRC_SYS_CD_SK", "A")]
    )
    # ------------------------------------------------------------------------------------------

    # ------------------ Stage: K_CLM_DIAG_TEMP (DB write) ------------------
    execute_dml(f"TRUNCATE TABLE {IDSOwner}.K_CLM_DIAG_TEMP", ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_DIAG_TEMP")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CLM_DIAG_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # -----------------------------------------------------------------------

    # ------------------ Stage: K_CLM_DIAG_TEMP (DB read) --------------------
    extract_query = f"""
        SELECT k.CLM_DIAG_SK,
               w.SRC_SYS_CD_SK,
               w.CLM_ID,
               w.CLM_DIAG_ORDNL_CD,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CLM_DIAG_TEMP w,
             {IDSOwner}.K_CLM_DIAG k
        WHERE w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
          AND w.CLM_ID          = k.CLM_ID
          AND w.CLM_DIAG_ORDNL_CD = k.CLM_DIAG_ORDNL_CD
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.CLM_ID,
               w2.CLM_DIAG_ORDNL_CD,
               {CurrRunCycle}
        FROM {IDSOwner}.K_CLM_DIAG_TEMP w2
        WHERE NOT EXISTS (
            SELECT k2.CLM_DIAG_SK
            FROM {IDSOwner}.K_CLM_DIAG k2
            WHERE w2.SRC_SYS_CD_SK   = k2.SRC_SYS_CD_SK
              AND w2.CLM_ID          = k2.CLM_ID
              AND w2.CLM_DIAG_ORDNL_CD = k2.CLM_DIAG_ORDNL_CD
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # -----------------------------------------------------------------------

    # ------------------ Stage: PrimaryKey (Transformer) --------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_DIAG_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "CLM_DIAG_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("CLM_DIAG_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CLM_DIAG_SK',<schema>,<secret_name>)
    # -----------------------------------------------------------------------

    # ------------------ Output Link: updt --------------------
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_DIAG_ORDNL_CD",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        "CLM_DIAG_SK"
    )

    write_files(
        df_updt,
        f"{adls_path}/ClmDiagPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------------

    # ------------------ Output Link: NewKeys -----------------
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_DIAG_ORDNL_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CLM_DIAG_SK"
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CLM_DIAG.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------------

    # ------------------ Output Link: Keys --------------------
    df_Keys = df_enriched.select(
        "CLM_DIAG_SK",
        "SRC_SYS_CD_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_DIAG_ORDNL_CD",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ---------------------------------------------------------

    # ------------------ Stage: Merge -------------------------
    join_expr = (
        (F.col("a.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("a.CLM_ID") == F.col("k.CLM_ID")) &
        (F.col("a.CLM_DIAG_ORDNL_CD") == F.col("k.CLM_DIAG_ORDNL_CD"))
    )

    df_Key = (
        df_AllCol_dedup.alias("a")
        .join(df_Keys.alias("k"), join_expr, "left")
        .select(
            F.col("a.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("a.DISCARD_IN"),
            F.col("a.PASS_THRU_IN"),
            F.col("a.FIRST_RECYC_DT"),
            F.col("a.ERR_CT"),
            F.col("a.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("a.PRI_KEY_STRING"),
            F.col("k.CLM_DIAG_SK"),
            F.col("a.SRC_SYS_CD_SK"),
            F.col("a.CLM_ID"),
            F.col("a.CLM_DIAG_ORDNL_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
            F.col("a.DIAG_CD"),
            F.col("a.CLM_DIAG_POA_CD"),
            F.col("a.DIAG_CD_TYP_CD")
        )
    )
    # ---------------------------------------------------------

    return df_Key