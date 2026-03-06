# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName      : MbrEpsdMesrPK
JobType      : Server Job
JobCategory  : DS_Integrate
FolderPath   : Shared Containers/PrimaryKey

* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:
    Shared container used for Primary Keying of MbrEpsdMesr job

CALLED BY :
    ImpProMbrEpsdMesrExtr

PROCESSING:
    • primary key hash file only contains current run keys and is cleared before writing
    • update primary key table (K_MBR_EPSD_MESR) with new keys created today
    • Assign primary surrogate key
    • Hash file (hf_mbr_epsd_mesr_allcol) cleared in calling job
    • join primary key info with table info
    • Load IDS temp. table
    • Temp table is truncated before load and runstat done after load
    • SQL joins temp table with key table to assign known keys

MODIFICATIONS:
Developer         Date          Change Description                                  Project/Altius #     Code Reviewer   Date Reviewed
Bhoomi Dasari     2008-08-26    Initial program                                     3567 Primary Key     Steph Goddard   09/02/2008
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_MbrEpsdMesrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the MbrEpsdMesrPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Container input corresponding to link 'Transform'.
    params : dict
        Runtime parameters and JDBC configuration.

    Returns
    -------
    DataFrame
        Container output corresponding to link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]

    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage : hf_mbr_epsd_mesr_allcol  (Scenario-a intermediate hash file)
    #         Deduplicate on key columns and forward to Merge
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "EPSD_TREAT_GRP_CD",
        "PRCS_YR_MO_SK"
    ]
    df_AllColOut = df_AllCol.dropDuplicates(key_cols_allcol)

    # ------------------------------------------------------------------
    # Stage : K_MBR_EPSD_MESR_TEMP  (DB2Connector via JDBC)
    #         Write to temp table then extract W_Extract
    # ------------------------------------------------------------------
    # 1) Prepare data for insert
    df_temp_insert = df_Transform.select(
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "EPSD_TREAT_GRP_CD",
        "PRCS_YR_MO_SK"
    )

    # 2) Truncate temp table
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_MBR_EPSD_MESR_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # 3) Append data
    (
        df_temp_insert.write
            .format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", f"{IDSOwner}.K_MBR_EPSD_MESR_TEMP")
            .mode("append")
            .save()
    )

    # 4) Extract W_Extract
    extract_query = f"""
    SELECT  k.MBR_EPSD_MESR_SK,
            w.SRC_SYS_CD_SK,
            w.MBR_UNIQ_KEY,
            w.EPSD_TREAT_GRP_CD,
            w.PRCS_YR_MO_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_EPSD_MESR_TEMP w,
         {IDSOwner}.K_MBR_EPSD_MESR       k
    WHERE w.SRC_SYS_CD_SK     = k.SRC_SYS_CD_SK
      AND w.MBR_UNIQ_KEY      = k.MBR_UNIQ_KEY
      AND w.PRCS_YR_MO_SK     = k.PRCS_YR_MO_SK
      AND w.EPSD_TREAT_GRP_CD = k.EPSD_TREAT_GRP_CD

    UNION

    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MBR_UNIQ_KEY,
           w2.EPSD_TREAT_GRP_CD,
           w2.PRCS_YR_MO_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_MBR_EPSD_MESR_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_EPSD_MESR k2
        WHERE w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
          AND w2.MBR_UNIQ_KEY      = k2.MBR_UNIQ_KEY
          AND w2.PRCS_YR_MO_SK     = k2.PRCS_YR_MO_SK
          AND w2.EPSD_TREAT_GRP_CD = k2.EPSD_TREAT_GRP_CD
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
    # Stage : PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("MBR_EPSD_MESR_SK") == -1, F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("svSK", F.col("MBR_EPSD_MESR_SK"))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("MBR_EPSD_MESR_SK") == -1,
                           F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svEpsdTrtGrpCd", F.trim(F.col("EPSD_TREAT_GRP_CD")))
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ----------------------------- Outputs -----------------------------
    # updt link (to hf_mbr_epsd_mesr parquet)
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
        "PRCS_YR_MO_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("MBR_EPSD_MESR_SK")
    )

    # NewKeys link (to K_MBR_EPSD_MESR sequential file)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
                   .select(
                       "SRC_SYS_CD_SK",
                       "MBR_UNIQ_KEY",
                       F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
                       "PRCS_YR_MO_SK",
                       F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
                       F.col("svSK").alias("MBR_EPSD_MESR_SK")
                   )
    )

    # Keys link (to Merge)
    df_Keys = df_enriched.select(
        F.col("svSK").alias("MBR_EPSD_MESR_SK"),
        "SRC_SYS_CD_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        F.col("svEpsdTrtGrpCd").alias("EPSD_TREAT_GRP_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Stage : hf_mbr_epsd_mesr (Scenario-c — write as Parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/MbrEpsdMesrPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage : K_MBR_EPSD_MESR (Sequential file)
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_MBR_EPSD_MESR.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage : Merge  (Transformer)
    # ------------------------------------------------------------------
    join_expr = (
        (df_AllColOut["SRC_SYS_CD_SK"]     == df_Keys["SRC_SYS_CD_SK"]) &
        (df_AllColOut["MBR_UNIQ_KEY"]      == df_Keys["MBR_UNIQ_KEY"]) &
        (df_AllColOut["EPSD_TREAT_GRP_CD"] == df_Keys["EPSD_TREAT_GRP_CD"]) &
        (df_AllColOut["PRCS_YR_MO_SK"]     == df_Keys["PRCS_YR_MO_SK"])
    )

    df_Key = (
        df_AllColOut.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_expr, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT"),
            F.col("AllColOut.RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING"),
            F.col("Keys.MBR_EPSD_MESR_SK"),
            F.col("AllColOut.MBR_UNIQ_KEY"),
            F.col("AllColOut.EPSD_TREAT_GRP_CD"),
            F.col("AllColOut.PRCS_YR_MO_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.EPSD_TREAT_GRP_SK"),
            F.col("AllColOut.MBR_SK"),
            F.col("AllColOut.MBR_MED_MESRS_SK"),
            F.col("AllColOut.ER_DT_SK"),
            F.col("AllColOut.IP_DT_SK")
        )
    )

    # ------------------------------------------------------------------
    # Return container output(s) in defined order
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------