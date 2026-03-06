# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName       : FcltyClmValPK
JobType       : Server Job
JobCategory   : DS_Integrate
FolderPath    : Shared Containers/PrimaryKey

Description
-----------
* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Facility Claim Val

CALLED BY: FctsClmFcltyValExtr
           NascoClmFcltyValExtr

PROCESSING:    


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------

Bhoomi Dasari    2008-08-01               Initial program                                                               3567 Primary Key    devlIDS                                Steph Goddard            08/07/2008
"""

# IDS Primary Key Container for FctsClmFcltyValExtr and NascoClmFcltyValExtr
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_FCLTY_CLM_VAL) with new keys created today
# Hash file (hf_fclty_val_allcol) cleared in the job FctsClmFcltyValExtr and NascoClmFcltyValExtr
# Assign primary surrogate key
# join primary key info with table info
# Load IDS temp. table
# Temp table is truncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys

from pyspark.sql import DataFrame, functions as F
from functools import reduce

def run_FcltyClmValPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the shared-container logic of FcltyClmValPK.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Input stream corresponding to link AllCol.
    df_Transform: DataFrame
        Input stream corresponding to link Transform.
    params      : dict
        Runtime parameters and environment information.

    Returns
    -------
    DataFrame
        Output stream corresponding to link Key.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_fclty_val_allcol  (Scenario-a hashed file removal)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_VAL_ORD"],
        sort_cols=[("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # Stage: K_FCLTY_CLM_VAL_TEMP  (DB2/Azure-SQL load & extract)
    # ------------------------------------------------------------------
    drop_query = f"DROP TABLE IF EXISTS {IDSOwner}.K_FCLTY_CLM_VAL_TEMP"
    execute_dml(drop_query, ids_jdbc_url, ids_jdbc_props)

    create_query = f"""
    CREATE TABLE {IDSOwner}.K_FCLTY_CLM_VAL_TEMP (
        SRC_SYS_CD_SK INT          NOT NULL,
        CLM_ID        VARCHAR(20)  NOT NULL,
        FCLTY_CLM_VAL_SEQ_NO SMALLINT NOT NULL,
        PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID, FCLTY_CLM_VAL_SEQ_NO)
    )
    """
    execute_dml(create_query, ids_jdbc_url, ids_jdbc_props)

    # Append rows coming from df_Transform
    (
        df_Transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_FCLTY_CLM_VAL_TEMP")
        .mode("append")
        .save()
    )

    runstats_query = f"""CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_FCLTY_CLM_VAL_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')"""
    execute_dml(runstats_query, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
    SELECT  k.FCLTY_CLM_VAL_SK,
            w.SRC_SYS_CD_SK,
            w.CLM_ID,
            w.FCLTY_CLM_VAL_SEQ_NO,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_FCLTY_CLM_VAL_TEMP w
    JOIN {IDSOwner}.K_FCLTY_CLM_VAL k
      ON w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
     AND w.CLM_ID              = k.CLM_ID
     AND w.FCLTY_CLM_VAL_SEQ_NO= k.FCLTY_CLM_VAL_SEQ_NO
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD_SK,
            w2.CLM_ID,
            w2.FCLTY_CLM_VAL_SEQ_NO,
            {CurrRunCycle}
    FROM {IDSOwner}.K_FCLTY_CLM_VAL_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_FCLTY_CLM_VAL k2
        WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID               = k2.CLM_ID
          AND w2.FCLTY_CLM_VAL_SEQ_NO = k2.FCLTY_CLM_VAL_SEQ_NO
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
            "svInstUpdt",
            F.when(F.col("FCLTY_CLM_VAL_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("FCLTY_CLM_VAL_SK") == F.lit(-1), F.lit(None)).otherwise(F.col("FCLTY_CLM_VAL_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("FCLTY_CLM_VAL_SK") == F.lit(-1), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svClmId",
            F.trim(F.col("CLM_ID"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # updt link
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("FCLTY_CLM_VAL_SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("FCLTY_CLM_VAL_SK")
        )
    )

    # NewKeys link
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("FCLTY_CLM_VAL_SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("FCLTY_CLM_VAL_SK")
        )
    )

    # Keys link
    df_Keys = (
        df_enriched
        .select(
            F.col("svSK").alias("FCLTY_CLM_VAL_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            F.col("FCLTY_CLM_VAL_SEQ_NO"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    join_expr = reduce(
        lambda a, b: a & b,
        [
            F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
            F.col("all.CLM_ID") == F.col("k.CLM_ID"),
            F.col("all.FCLTY_CLM_VAL_ORD") == F.col("k.FCLTY_CLM_VAL_SEQ_NO")
        ]
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
    )

    df_Key = (
        df_merge
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
            F.col("k.FCLTY_CLM_VAL_SK").alias("FCLTY_CLM_VAL_SK"),
            F.col("k.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("all.CLM_ID").alias("CLM_ID"),
            F.col("all.FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_ORD"),
            F.col("all.CLVC_NUMBER").alias("CLVC_NUMBER"),
            F.col("all.CLVC_LETTER").alias("CLVC_LETTER"),
            F.col("all.FCLTY_CLM_VAL_CD").alias("FCLTY_CLM_VAL_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
            F.col("all.VAL_AMT").alias("VAL_AMT"),
            F.col("all.VAL_UNIT_CT").alias("VAL_UNIT_CT")
        )
    )

    # ------------------------------------------------------------------
    # Stage: K_FCLTY_CLM_VAL (Sequential file write)
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_FCLTY_CLM_VAL.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: hf_fclty_val (Parquet write for hashed file)
    # ------------------------------------------------------------------
    parquet_file_path = f"{adls_path}/FcltyClmValPK_updt.parquet"
    write_files(
        df_updt,
        parquet_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return the container's output link(s)
    # ------------------------------------------------------------------
    return df_Key