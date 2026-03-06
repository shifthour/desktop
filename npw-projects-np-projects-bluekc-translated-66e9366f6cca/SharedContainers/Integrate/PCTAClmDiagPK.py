# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
PCTAClmDiagPK – IDS Primary Key Container for Claim Diagnosis

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY:   PCTAClmDiagExtr

DESCRIPTION: Shared container used for Primary Keying of PCTA Claim Diagnosis

PROCESSING:                                                                                                            

MODIFICATIONS:
Developer           Date          Change Description                                   Project/Altius #          Development Project      Code Reviewer    Date Reviewed
------------------  ------------  ---------------------------------------------------  -------------------------  -----------------------  ---------------  -------------
Parik               2008-08-13    Initial program                                      3567 Primary Key           devlIDS                  Steph Goddard    08/19/2008
SAndrew             2008-12-22    Added CLM_DIAG_POA_CD to primary key, default NA     DRG 3494                  devlIDS                  Brent Leland     12-29-2008
Manasa Andru        2012-06-28    Added DIAG_CD_TYP_CD                                 4896 EdwRemediation        IDSNewDevl               Brent Leland     07-02-2012
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_PCTAClmDiagPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the primary-key logic for the Claim Diagnosis shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming DataFrame for link “AllCol”.
    df_Transform : DataFrame
        Incoming DataFrame for link “Transform”.
    params : dict
        Dictionary containing all runtime parameters.

    Returns
    -------
    DataFrame
        DataFrame for output link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (once only)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Hashed-file hf_clm_diag_allcol  ➜  Scenario a  (intermediate)
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_DIAG_ORDNL_CD"],
        []
    )

    # ------------------------------------------------------------------
    # DB2  – K_CLM_DIAG_TEMP  (truncate → load → runstats)
    # ------------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_CLM_DIAG_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_DIAG_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CLM_DIAG_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Extract W_Extract
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.CLM_DIAG_SK,
                w.SRC_SYS_CD_SK,
                w.CLM_ID,
                w.CLM_DIAG_ORDNL_CD,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_CLM_DIAG_TEMP w,
                {IDSOwner}.K_CLM_DIAG      k
        WHERE   w.SRC_SYS_CD_SK         = k.SRC_SYS_CD_SK
          AND   w.CLM_ID                = k.CLM_ID
          AND   w.CLM_DIAG_ORDNL_CD     = k.CLM_DIAG_ORDNL_CD

        UNION

        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.CLM_ID,
                w2.CLM_DIAG_ORDNL_CD,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_CLM_DIAG_TEMP w2
        WHERE   NOT EXISTS (
                    SELECT  k2.CLM_DIAG_SK
                    FROM    {IDSOwner}.K_CLM_DIAG k2
                    WHERE   w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
                      AND   w2.CLM_ID            = k2.CLM_ID
                      AND   w2.CLM_DIAG_ORDNL_CD = k2.CLM_DIAG_ORDNL_CD
                )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    join_cols = ["SRC_SYS_CD_SK", "CLM_ID", "CLM_DIAG_ORDNL_CD"]

    df_join = (
        df_W_Extract.alias("w")
        .join(df_allcol_dedup.select(join_cols + ["SRC_SYS_CD"]).alias("s"),
              on=join_cols,
              how="left")
    )

    df_enriched = (
        df_join
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("w.CLM_DIAG_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == F.lit("I"),
                   F.lit(CurrRunCycle)).otherwise(F.col("w.CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("CLM_DIAG_SK", F.col("w.CLM_DIAG_SK"))
        .withColumn("SRC_SYS_CD", F.col("s.SRC_SYS_CD"))
    )

    # ------------------------------------------------------------------
    # Surrogate-key generation
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'CLM_DIAG_SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Derive intermediary links
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched.select(
            "SRC_SYS_CD",
            "CLM_ID",
            "CLM_DIAG_ORDNL_CD",
            "CRT_RUN_CYC_EXCTN_SK",
            "CLM_DIAG_SK"
        )
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_DIAG_ORDNL_CD",
            "CRT_RUN_CYC_EXCTN_SK",
            "CLM_DIAG_SK"
        )
    )

    df_Keys = (
        df_enriched.select(
            "CLM_DIAG_SK",
            "SRC_SYS_CD_SK",
            "SRC_SYS_CD",
            "CLM_ID",
            "CLM_DIAG_ORDNL_CD",
            "INSRT_UPDT_CD",
            "CRT_RUN_CYC_EXCTN_SK"
        )
    )

    # ------------------------------------------------------------------
    # Write hf_clm_diag (parquet)   – Scenario c
    # ------------------------------------------------------------------
    parquet_path_hf_clm_diag = f"{adls_path}/PCTAClmDiagPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_hf_clm_diag,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Write K_CLM_DIAG sequential file
    # ------------------------------------------------------------------
    seq_path_k_clm_diag = f"{adls_path}/load/K_CLM_DIAG.dat"
    write_files(
        df_NewKeys,
        seq_path_k_clm_diag,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Merge transformer logic  (AllColOut  ⟕ Keys)
    # ------------------------------------------------------------------
    df_Key = (
        df_allcol_dedup.alias("a")
        .join(df_Keys.alias("k"), on=join_cols, how="left")
        .select(
            "a.JOB_EXCTN_RCRD_ERR_SK",
            "k.INSRT_UPDT_CD",
            "a.DISCARD_IN",
            "a.PASS_THRU_IN",
            "a.FIRST_RECYC_DT",
            "a.ERR_CT",
            "a.RECYCLE_CT",
            "k.SRC_SYS_CD",
            "a.PRI_KEY_STRING",
            "k.CLM_DIAG_SK",
            "a.SRC_SYS_CD_SK",
            "a.CLM_ID",
            "a.CLM_DIAG_ORDNL_CD",
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
            "a.DIAG_CD",
            "a.CLM_DIAG_POA_CD",
            "a.DIAG_CD_TYP_CD"
        )
    )

    return df_Key