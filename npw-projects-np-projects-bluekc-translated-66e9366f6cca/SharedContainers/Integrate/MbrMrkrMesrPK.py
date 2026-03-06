
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

"""
Shared Container: MbrMrkrMesrPK
Description:
* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Mbr Mrkr Mesr job

CALLED BY : ImpProMbrMrkrMesrExtr

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-26               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard         09/02/2008

Annotations:
Used by  ImpProMbrMrkrMesrExtr
IDS Primary Key Container for Mbr Mrkr Mesr
primary key hash file only contains current run keys and is cleared before writing
update primary key table (MBR_MRKR_MESR) with new keys created today
Assign primary surrogate key
Hash file (hf_mbr_mrkr_mesr_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
"""

# COMMAND ----------

def run_MbrMrkrMesrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the MbrMrkrMesrPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming DataFrame for the link "AllCol".
    df_Transform : DataFrame
        Incoming DataFrame for the link "Transform".
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        The DataFrame corresponding to the output link "Key".
    """

    # ------------------------------------------------------------------
    # Unpack required parameters
    # ------------------------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # 1. Deduplicate intermediate hash file (scenario a replacement)
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "MRKR_ID", "PRCS_YR_MO_SK"],
        [("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # 2. Load df_Transform into temporary table K_MBR_MRKR_MESR_TEMP
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_MBR_MRKR_MESR_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MBR_MRKR_MESR_TEMP")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # 3. Extract W_Extract dataset from the database
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.MBR_MRKR_MESR_SK,
            w.SRC_SYS_CD_SK,
            w.MBR_UNIQ_KEY,
            w.MRKR_ID,
            w.PRCS_YR_MO_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_MRKR_MESR_TEMP w
    JOIN {IDSOwner}.K_MBR_MRKR_MESR k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
     AND w.PRCS_YR_MO_SK = k.PRCS_YR_MO_SK
     AND w.MRKR_ID       = k.MRKR_ID
    UNION
    SELECT -1 AS MBR_MRKR_MESR_SK,
           w2.SRC_SYS_CD_SK,
           w2.MBR_UNIQ_KEY,
           w2.MRKR_ID,
           w2.PRCS_YR_MO_SK,
           {CurrRunCycle} AS CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_MRKR_MESR_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_MRKR_MESR k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
          AND w2.PRCS_YR_MO_SK = k2.PRCS_YR_MO_SK
          AND w2.MRKR_ID       = k2.MRKR_ID
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
    # 4. Transformer logic (PrimaryKey stage)
    # ------------------------------------------------------------------
    df_enriched = df_W_Extract

    df_enriched = (
        df_enriched
        .withColumn(
            "svInstUpdt",
            F.when(F.col("MBR_MRKR_MESR_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("MBR_MRKR_MESR_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svMrkrId", F.trim(F.col("MRKR_ID")))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # 5. Build DataFrames for subsequent stages
    # ------------------------------------------------------------------
    # updt link (hf_mbr_mrkr)
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            F.col("svMrkrId").alias("MRKR_ID"),
            "PRCS_YR_MO_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MBR_MRKR_MESR_SK")
        )
    )

    # NewKeys link (K_MBR_MRKR_MESR sequential file)
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            F.col("svMrkrId").alias("MRKR_ID"),
            "PRCS_YR_MO_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MBR_MRKR_MESR_SK")
        )
    )

    # Keys link (to Merge stage)
    df_Keys = (
        df_enriched
        .select(
            F.col("svSK").alias("MBR_MRKR_MESR_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            F.col("svMrkrId").alias("MRKR_ID"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ------------------------------------------------------------------
    # 6. Merge logic
    # ------------------------------------------------------------------
    join_expr = [
        df_allcol_dedup["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"],
        df_allcol_dedup["MBR_UNIQ_KEY"]  == df_Keys["MBR_UNIQ_KEY"],
        df_allcol_dedup["PRCS_YR_MO_SK"] == df_Keys["PRCS_YR_MO_SK"],
        df_allcol_dedup["MRKR_ID"]       == df_Keys["MRKR_ID"],
    ]

    df_Key = (
        df_allcol_dedup.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_expr, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.MBR_MRKR_MESR_SK").alias("MBR_MRKR_MESR_SK"),
            F.col("AllColOut.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("AllColOut.MRKR_ID").alias("MRKR_ID"),
            F.col("AllColOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.MRKR_SK").alias("MRKR_SK"),
            F.col("AllColOut.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
            F.col("AllColOut.MBR_SK").alias("MBR_SK"),
            F.col("AllColOut.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK")
        )
    )

    # ------------------------------------------------------------------
    # 7. Write sequential file (K_MBR_MRKR_MESR.dat)
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MBR_MRKR_MESR.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # 8. Write parquet hash file replacement (hf_mbr_mrkr)
    # ------------------------------------------------------------------
    parquet_path = f"{adls_path}/MbrMrkrMesrPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # 9. Return the container output link
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------
