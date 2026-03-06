# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: McdPaymtSumPK

* VC LOGS *
*****************************************************************************************************************************************************************************
COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


CALLED BY:

PROCESSING:


MODIFICATIONS:
Developer                          Date               Project/Altiris #                    Change Description                                       Development Project    Code Reviewer            Date Reviewed
----------------------------------      -------------------   --------------------------------              ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
Ralph Tucker                  11/23/2009   3659 - Mcd Cap                     Originally Programmed                                    IntegrateCurDevl           Steph Goddard             01/20/2010
Ralph Tucker                  02/14/2011   TTR-1018                              Removed K table clear                                   IntegrateNewDevl         Steph Goddard             02/15/2011

Annotations:
Assign primary surrogate key
update primary key table (K_MCAID_PAYMT_SUM) with new keys created today
join primary key info with table info
IDS Primary Key Container for
Customer Service Task Link
Hash file cleared in calling job
Load IDS temp. table
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
primary key hash file only contains current run keys
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_McdPaymtSumPK(df_AllCol: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters
    # ------------------------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    SrcSysCd          = params["SrcSysCd"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Replace intermediate hash-file: hf_mcaid_pymt_sum_allcol
    # ------------------------------------------------------------------
    df_all_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MCAID_PAYMT_TYP_CD", "TRANS_DT"],
        [("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # Truncate and load IDS temp table K_MCAID_PAYMT_SUM_TEMP
    # ------------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_MCAID_PAYMT_SUM_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    df_Transform_sel = df_Transform.select(
        "SRC_SYS_CD_SK",
        "MCAID_PAYMT_TYP_CD",
        "TRANS_DT_SK"
    )
    (
        df_Transform_sel.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MCAID_PAYMT_SUM_TEMP")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Extract W_Extract dataset
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.MCAID_PAYMT_SUM_SK,
           w.SRC_SYS_CD_SK,
           w.MCAID_PAYMT_TYP_CD,
           w.TRANS_DT_SK,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_MCAID_PAYMT_SUM_TEMP w
           JOIN {IDSOwner}.K_MCAID_PAYMT_SUM k
             ON w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
            AND w.MCAID_PAYMT_TYP_CD = k.MCAID_PAYMT_TYP_CD
            AND w.TRANS_DT_SK        = k.TRANS_DT_SK
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MCAID_PAYMT_TYP_CD,
           w2.TRANS_DT_SK,
           {CurrRunCycle}
      FROM {IDSOwner}.K_MCAID_PAYMT_SUM_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_MCAID_PAYMT_SUM k2
            WHERE w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
              AND w2.MCAID_PAYMT_TYP_CD = k2.MCAID_PAYMT_TYP_CD
              AND w2.TRANS_DT_SK        = k2.TRANS_DT_SK)
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Primary key logic and surrogate-key generation
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("MCAID_PAYMT_SUM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn("MCAID_CLM_PAYMT_TYP_CD", F.col("MCAID_PAYMT_TYP_CD"))
        .withColumn(
            "MCAID_CLM_DTL_SK",
            F.when(F.col("MCAID_PAYMT_SUM_SK") == F.lit(-1), F.lit(None)).otherwise(F.col("MCAID_PAYMT_SUM_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("MCAID_PAYMT_SUM_SK") == F.lit(-1), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MCAID_CLM_DTL_SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Prepare outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MCAID_CLM_PAYMT_TYP_CD",
            "TRANS_DT_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "MCAID_CLM_DTL_SK"
        )
    )
    df_keys = df_enriched.select(
        "MCAID_CLM_DTL_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "MCAID_CLM_PAYMT_TYP_CD",
        "TRANS_DT_SK",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "MCAID_CLM_PAYMT_TYP_CD",
        "TRANS_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "MCAID_CLM_DTL_SK"
    )

    # ------------------------------------------------------------------
    # Write Sequential File: K_MCAID_PAYMT_SUM.dat
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MCAID_PAYMT_SUM.dat"
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
    # Write Parquet for hf_mcaid_paymt_sum
    # ------------------------------------------------------------------
    parquet_path = f"{adls_path}/McdPaymtSumPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Merge AllColOut with Keys
    # ------------------------------------------------------------------
    df_key = (
        df_all_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.MCAID_PAYMT_TYP_CD") == F.col("k.MCAID_CLM_PAYMT_TYP_CD")),
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.MCAID_CLM_DTL_SK").alias("MCAID_PAYMT_SUM_SK"),
            F.col("k.MCAID_CLM_PAYMT_TYP_CD").alias("MCAID_CLM_PAYMT_TYP_CD"),
            F.col("k.TRANS_DT_SK").alias("TRANS_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MCD_ST_FISCAL_YR_END_DT").alias("MCD_ST_FISCAL_YR_END_DT"),
            F.col("all.MCD_ST_PAYMT_RSN_CD").alias("MCD_ST_PAYMT_RSN_CD"),
            F.col("all.MCD_ST_PAYMT_REF_ID").alias("MCD_ST_PAYMT_REF_ID"),
            F.col("all.PD_AMT").alias("PD_AMT")
        )
    )

    return df_key