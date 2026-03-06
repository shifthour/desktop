# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName   : MbrAccumPK
JobType   : Server Job
Folder    : Shared Containers/PrimaryKey

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Member Accumulator job

CALLED BY : FctsMbrAccumExtr

PROCESSING:    



MODIFICATIONS:
Developer                     Date              Change Description                                          Project/Altius #    Development Project   Code Reviewer       Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                      2008-08-27           Initial program                                               3567(Primary Key)        devlIDS              Steph Goddard       09/02/2008
Karthik Chintalapani       2016-11-11           5634 Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT       IntegrateDev2           Kalyan Neelam        2016-11-28
"""

from pyspark.sql import DataFrame, functions as F, types as T

def run_MbrAccumPK(
        df_AllCol: DataFrame,
        df_Transform: DataFrame,
        params: dict
    ) -> DataFrame:
    """
    Executes the MbrAccumPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        DataFrame for link 'AllCol'.
    df_Transform : DataFrame
        DataFrame for link 'Transform'.
    params : dict
        Dictionary containing runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        DataFrame for link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (once only)
    # ------------------------------------------------------------------
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]

    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]

    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Step-1  :  dedup for intermediate hash file hf_mbr_accum_allcol
    # ------------------------------------------------------------------
    partition_cols_allcol = [
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "MBR_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO"
    ]
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols_allcol,
        []                       # no specific sort order defined
    )

    # ------------------------------------------------------------------
    # Step-2  :  Write df_Transform into {IDSOwner}.K_MBR_ACCUM_TEMP
    # ------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_MBR_ACCUM_TEMP"
    execute_dml(
        f"DELETE FROM {temp_table}",
        ids_jdbc_url,
        ids_jdbc_props
    )

    transform_cols = [
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "MBR_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO"
    ]
    (
        df_Transform
        .select(*transform_cols)
        .write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .option("dbtable", temp_table)
        .options(**ids_jdbc_props)
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {temp_table} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Step-3  :  Extract W_Extract (link 'W_Extract')
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.MBR_ACCUM_SK,
           w.SRC_SYS_CD_SK,
           w.MBR_UNIQ_KEY,
           w.PROD_ACCUM_ID,
           w.MBR_ACCUM_TYP_CD,
           w.ACCUM_NO,
           w.YR_NO,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_ACCUM_TEMP w
    JOIN {IDSOwner}.K_MBR_ACCUM k
         ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
        AND w.MBR_UNIQ_KEY = k.MBR_UNIQ_KEY
        AND w.PROD_ACCUM_ID = k.PROD_ACCUM_ID
        AND w.MBR_ACCUM_TYP_CD = k.MBR_ACCUM_TYP_CD
        AND w.ACCUM_NO = k.ACCUM_NO
        AND w.YR_NO = k.YR_NO
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MBR_UNIQ_KEY,
           w2.PROD_ACCUM_ID,
           w2.MBR_ACCUM_TYP_CD,
           w2.ACCUM_NO,
           w2.YR_NO,
           {CurrRunCycle}
    FROM {IDSOwner}.K_MBR_ACCUM_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_ACCUM k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
          AND w2.PROD_ACCUM_ID = k2.PROD_ACCUM_ID
          AND w2.MBR_ACCUM_TYP_CD = k2.MBR_ACCUM_TYP_CD
          AND w2.ACCUM_NO = k2.ACCUM_NO
          AND w2.YR_NO    = k2.YR_NO
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
    # Step-4  :  PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("MBR_ACCUM_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "MBR_ACCUM_SK",
            F.when(F.col("MBR_ACCUM_SK") == -1, F.lit(None).cast(T.LongType()))
             .otherwise(F.col("MBR_ACCUM_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("MBR_ACCUM_SK").isNull(), F.lit(CurrRunCycle).cast(T.IntegerType()))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key population (special placeholder call)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        key_col="MBR_ACCUM_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Step-5  :  Prepare link-specific DataFrames
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "MBR_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "MBR_ACCUM_SK"
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "MBR_ACCUM_TYP_CD",
            "ACCUM_NO",
            "YR_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "MBR_ACCUM_SK"
        )
    )

    df_Keys = df_enriched.select(
        "MBR_ACCUM_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "MBR_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # ------------------------------------------------------------------
    # Step-6  :  Merge transformer logic
    # ------------------------------------------------------------------
    join_cond = (
        (F.col("k.SRC_SYS_CD_SK")    == F.col("a.SRC_SYS_CD_SK")) &
        (F.col("k.MBR_UNIQ_KEY")     == F.col("a.MBR_UNIQ_KEY")) &
        (F.col("k.PROD_ACCUM_ID")    == F.col("a.PROD_ACCUM_ID")) &
        (F.col("k.MBR_ACCUM_TYP_CD") == F.col("a.MBR_ACCUM_TYP_CD")) &
        (F.col("k.ACCUM_NO")         == F.col("a.ACCUM_NO")) &
        (F.col("k.YR_NO")            == F.col("a.YR_NO"))
    )

    df_Key = (
        df_AllColOut.alias("a")
        .join(df_Keys.alias("k"), join_cond, "left")
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
            F.col("k.MBR_ACCUM_SK"),
            F.col("a.MBR_UNIQ_KEY"),
            F.col("a.PROD_ACCUM_ID"),
            F.col("a.MBR_ACCUM_TYP_CD"),
            F.col("a.ACCUM_NO"),
            F.col("a.YR_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("a.GRGR_ID"),
            F.col("a.MBR_SK"),
            F.col("a.ACCUM_AMT"),
            F.col("a.CAROVR_AMT"),
            F.col("a.COB_OOP_AMT"),
            F.col("a.PLN_YR_EFF_DT"),
            F.col("a.PLN_YR_END_DT")
        )
    )

    # ------------------------------------------------------------------
    # Step-7  :  Write outputs (files / parquet)
    # ------------------------------------------------------------------
    # Sequential file: K_MBR_ACCUM.dat
    file_path_newkeys = f"{adls_path}/load/K_MBR_ACCUM.dat"
    write_files(
        df_NewKeys,
        file_path_newkeys,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # Parquet file: hf_mbr_accum (link 'updt')
    file_path_updt = f"{adls_path}/MbrAccumPK_updt.parquet"
    write_files(
        df_updt,
        file_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key