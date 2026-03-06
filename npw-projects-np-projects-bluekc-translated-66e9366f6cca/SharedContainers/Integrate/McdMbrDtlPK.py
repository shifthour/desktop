# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: McdMbrDtlPK
----------------------------------------------------------------------
Assign primary surrogate key
update primary key table (K_MCAID_CLM_DTL) with new keys created today
join primary key info with table info
IDS Primary Key Container for
Customer Service Task Link
Hash file cleared in calling job
Load IDS temp. table
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
primary key hash file only contains current run keys

VC LOGS
*****************************************************************************************************************************************************************************
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY:

PROCESSING:


MODIFICATIONS:
Developer                          Date               Project/Altiris #                    Change Description                                       Development Project    Code Reviewer            Date Reviewed
----------------------------------      -------------------   --------------------------------              ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
Ralph Tucker                  11/24/2009   3659 - Mcd Cap                     Originally Programmed                                    devlIDSnew                  Steph Goddard             01/11/2010
Ralph Tucker                  02/14/2011   TTR-1018                              Removed K table clear                                   IntegrateNewDevl        Steph Goddard             02/15/2011
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# COMMAND ----------
def run_McdMbrDtlPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the DataStage shared container 'McdMbrDtlPK'.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Dictionary of runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to container link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters
    # ------------------------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    CurrDate              = params.get("CurrDate", "")
    RunID                 = params.get("RunID", "")
    SrcSysCdSk            = params.get("SrcSysCdSk", "")
    SrcSysCd              = params["SrcSysCd"]

    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]

    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]

    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_mcaid_mbr_dtl_allcol  (Scenario a – intermediate hash)
    # ------------------------------------------------------------------
    dedup_part_cols = [
        "SRC_SYS_CD_SK",
        "MCAID_RECPNT_NO",
        "MCAID_MBR_PAYMT_TYP_CD",
        "MCAID_ST_REF_ID"
    ]
    dedup_sort_cols = []  # No explicit sort columns defined in original job
    df_AllCol_dedup = dedup_sort(df_AllCol, dedup_part_cols, dedup_sort_cols)

    # ------------------------------------------------------------------
    # Stage: K_MCAID_MBR_DTL_TEMP  (DB2/Azure-SQL)
    # ------------------------------------------------------------------
    # 1) Truncate temporary table
    truncate_query = f"TRUNCATE TABLE {IDSOwner}.K_MCAID_MBR_DTL_TEMP"
    execute_dml(truncate_query, ids_jdbc_url, ids_jdbc_props)

    # 2) Load Transform link data into temp table
    (
        df_Transform
        .select(
            col("SRC_SYS_CD_SK"),
            col("MCAID_RECPNT_NO"),
            col("MCAID_MBR_PAYMT_TYP_CD"),
            col("MCAID_ST_REF_ID")
        )
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MCAID_MBR_DTL_TEMP")
        .mode("append")
        .save()
    )

    # 3) Extract W_Extract link
    extract_query = f"""
        SELECT k.MCAID_MBR_DTL_SK,
               w.SRC_SYS_CD_SK,
               w.MCAID_RECPNT_NO,
               w.MCAID_MBR_PAYMT_TYP_CD,
               w.MCAID_ST_REF_ID,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MCAID_MBR_DTL_TEMP w
        JOIN {IDSOwner}.K_MCAID_MBR_DTL k
          ON w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
         AND w.MCAID_RECPNT_NO        = k.MCAID_RECPNT_NO
         AND w.MCAID_MBR_PAYMT_TYP_CD = k.MCAID_MBR_PAYMT_TYP_CD
         AND w.MCAID_ST_REF_ID        = k.MCAID_ST_REF_ID
        UNION ALL
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.MCAID_RECPNT_NO,
               w2.MCAID_MBR_PAYMT_TYP_CD,
               w2.MCAID_ST_REF_ID,
               {CurrRunCycle}
        FROM {IDSOwner}.K_MCAID_MBR_DTL_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_MCAID_MBR_DTL k2
            WHERE w2.SRC_SYS_CD_SK          = k2.SRC_SYS_CD_SK
              AND w2.MCAID_RECPNT_NO        = k2.MCAID_RECPNT_NO
              AND w2.MCAID_MBR_PAYMT_TYP_CD = k2.MCAID_MBR_PAYMT_TYP_CD
              AND w2.MCAID_ST_REF_ID        = k2.MCAID_ST_REF_ID
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
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            when(col("MCAID_MBR_DTL_SK") == -1, lit("I")).otherwise(lit("U"))
        )
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("INSRT_UPDT_CD") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'MCAID_MBR_DTL_SK',<schema>,<secret_name>)

    # NewKeys link (constraint: INSRT_UPDT_CD == 'I')
    df_newkeys = (
        df_enriched
        .filter(col("INSRT_UPDT_CD") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "MCAID_RECPNT_NO",
            "MCAID_MBR_PAYMT_TYP_CD",
            "MCAID_ST_REF_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "MCAID_MBR_DTL_SK"
        )
    )

    # Keys link (all rows)
    df_keys = (
        df_enriched
        .select(
            "MCAID_MBR_DTL_SK",
            "SRC_SYS_CD_SK",
            "SRC_SYS_CD",
            "MCAID_RECPNT_NO",
            "MCAID_MBR_PAYMT_TYP_CD",
            "MCAID_ST_REF_ID",
            "INSRT_UPDT_CD",
            "CRT_RUN_CYC_EXCTN_SK"
        )
    )

    # updt link (all rows)
    df_updt = (
        df_enriched
        .select(
            col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("MCAID_RECPNT_NO"),
            col("MCAID_MBR_PAYMT_TYP_CD"),
            col("MCAID_ST_REF_ID").alias("MCAID_ICN_NBR"),
            "CRT_RUN_CYC_EXCTN_SK",
            "MCAID_MBR_DTL_SK"
        )
    )

    # ------------------------------------------------------------------
    # Stage: K_MCAID_MBR_DTL (Sequential File)
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MCAID_MBR_DTL.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: hf_mcaid_mbr_dtl  (Scenario c – write as Parquet)
    # ------------------------------------------------------------------
    parquet_path_updt = f"{adls_path}/McdMbrDtlPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------------------
    join_expr = [
        col("all.SRC_SYS_CD_SK")          == col("k.SRC_SYS_CD_SK"),
        col("all.MCAID_RECPNT_NO")        == col("k.MCAID_RECPNT_NO"),
        col("all.MCAID_MBR_PAYMT_TYP_CD") == col("k.MCAID_MBR_PAYMT_TYP_CD"),
        col("all.MCAID_ST_REF_ID")        == col("k.MCAID_ST_REF_ID")
    ]
    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
    )

    df_Key = (
        df_merge
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.MCAID_MBR_DTL_SK").alias("MCAID_MBR_DTL_SK"),
            col("all.MCAID_RECPNT_NO").alias("MCAID_RECPNT_NO"),
            col("all.MCAID_MBR_PAYMT_TYP_CD").alias("MCAID_MBR_PAYMT_TYP_CD"),
            col("all.MCAID_ST_REF_ID").alias("MCAID_ST_REF_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.MBR_SK").alias("MBR_SK"),
            col("all.FIRST_SVC_DT_SK").alias("FIRST_SVC_DT_SK"),
            col("all.LAST_SVC_DT_SK").alias("LAST_SVC_DT_SK"),
            col("all.MCAID_RECPNT_LAST_NM").alias("MCAID_RECPNT_LAST_NM"),
            col("all.MCAID_RECPNT_FIRST_NM").alias("MCAID_RECPNT_FIRST_NM"),
            col("all.MBR_SRC_SYS_CD_SK").alias("MBR_SRC_SYS_CD_SK"),
            col("all.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            col("all.PRCS_DT_SK").alias("PRCS_DT_SK"),
            col("all.TRANS_AMT").alias("TRANS_AMT")
        )
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------