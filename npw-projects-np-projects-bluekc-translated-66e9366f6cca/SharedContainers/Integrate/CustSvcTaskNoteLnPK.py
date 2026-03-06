# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: CustSvcTaskNoteLnPK
Job Type       : Server Job
Category       : DS_Integrate
Folder         : Shared Containers/PrimaryKey

Description:
* VC LOGS *
^1_2 03/18/09 13:13:22 Batch  15053_47621 PROMOTE bckcetl ids20 dsadm bls for sg
^1_2 03/18/09 13:10:47 Batch  15053_47449 INIT bckcett testIDS dsadm bls for sg
^1_1 10/21/08 10:37:16 Batch  14905_38245 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 10/21/08 10:33:47 Batch  14905_38033 INIT bckcett testIDS dsadm rc for brent
^1_1 10/20/08 12:44:46 Batch  14904_45890 PROMOTE bckcett testIDS u08717 Brent
^1_1 10/20/08 12:41:40 Batch  14904_45702 INIT bckcett devlIDS u08717 Brent
^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcetl devlIDS u08717 Brent
^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent

CALLED BY:
FctsCustSvcTaskNoteLnExtr

PROCESSING:
Assign primary surrogate key to input records

MODIFICATIONS:
Developer          Date         Project/Altiris #   Change Description
------------------ ------------ ------------------  --------------------------------------------------
Brent Leland       2008-03-04   3567 Primary Key    Original Programming. Changed counter IDS_SK to
                                                   CUST_SVC_TASK_NOTE_LN_SK
Steph Goddard      03/18/2009   Prod Supp           Changed hash file name to note_ln instead of note
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def run_CustSvcTaskNoteLnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the CustSvcTaskNoteLnPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Stream corresponding to link “AllCol”.
    df_Transform : DataFrame
        Stream corresponding to link “Transform”.
    params : dict
        Runtime parameters and JDBC configs.

    Returns
    -------
    DataFrame
        Output stream for link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each only once)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_cust_svc_task_note_ln_allcol  (Scenario-a intermediate)
    # ------------------------------------------------------------------
    # Replace intermediate hash-file with de-duplication
    key_cols_allcol = [
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "CUST_SVC_TASK_NOTE_LOC_CD",
        "NOTE_SEQ_NO",
        "LAST_UPDT_DTM",
        "LN_SEQ_NO",
    ]
    sort_cols_allcol = [("LAST_UPDT_DTM", "D")]
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        key_cols_allcol,
        sort_cols_allcol
    )

    # ------------------------------------------------------------------
    # Stage: K_CUST_SVC_TASK_NOTE_LN_TEMP  (write to temp table)
    # ------------------------------------------------------------------
    temp_table_name = f"{IDSOwner}.K_CUST_SVC_TASK_NOTE_LN_TEMP"
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table_name)
        .mode("append")
        .save()
    )

    # Optional runstats
    runstats_stmt = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CUST_SVC_TASK_NOTE_LN_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_stmt, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Stage: K_CUST_SVC_TASK_NOTE_LN_TEMP  (read W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.CUST_SVC_TASK_NOTE_LN_SK,
                w.SRC_SYS_CD_SK,
                w.CUST_SVC_ID,
                w.TASK_SEQ_NO,
                w.CUST_SVC_TASK_NOTE_LOC_CD,
                w.NOTE_SEQ_NO,
                w.LAST_UPDT_DTM,
                w.LN_SEQ_NO,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_CUST_SVC_TASK_NOTE_LN_TEMP w
        JOIN    {IDSOwner}.K_CUST_SVC_TASK_NOTE_LN k
              ON w.SRC_SYS_CD_SK              = k.SRC_SYS_CD_SK
             AND w.CUST_SVC_ID               = k.CUST_SVC_ID
             AND w.TASK_SEQ_NO               = k.TASK_SEQ_NO
             AND w.CUST_SVC_TASK_NOTE_LOC_CD = k.CUST_SVC_TASK_NOTE_LOC_CD
             AND w.NOTE_SEQ_NO               = k.NOTE_SEQ_NO
             AND w.LAST_UPDT_DTM             = k.LAST_UPDT_DTM
             AND w.LN_SEQ_NO                 = k.LN_SEQ_NO
        UNION ALL
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.CUST_SVC_ID,
                w2.TASK_SEQ_NO,
                w2.CUST_SVC_TASK_NOTE_LOC_CD,
                w2.NOTE_SEQ_NO,
                w2.LAST_UPDT_DTM,
                w2.LN_SEQ_NO,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_CUST_SVC_TASK_NOTE_LN_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM   {IDSOwner}.K_CUST_SVC_TASK_NOTE_LN k2
            WHERE  w2.SRC_SYS_CD_SK              = k2.SRC_SYS_CD_SK
              AND  w2.CUST_SVC_ID               = k2.CUST_SVC_ID
              AND  w2.TASK_SEQ_NO               = k2.TASK_SEQ_NO
              AND  w2.CUST_SVC_TASK_NOTE_LOC_CD = k2.CUST_SVC_TASK_NOTE_LOC_CD
              AND  w2.NOTE_SEQ_NO               = k2.NOTE_SEQ_NO
              AND  w2.LAST_UPDT_DTM             = k2.LAST_UPDT_DTM
              AND  w2.LN_SEQ_NO                 = k2.LN_SEQ_NO
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
    # Stage: PrimaryKey (Transformer logic)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CUST_SVC_TASK_NOTE_LN_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "CUST_SVC_TASK_NOTE_LN_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("CUST_SVC_TASK_NOTE_LN_SK"))
        )
    )

    # Surrogate key population for new records
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CUST_SVC_TASK_NOTE_LN_SK",
        <schema>,
        <secret_name>
    )

    # After key generation, refresh useful aliases
    df_enriched = df_enriched.withColumn("INSRT_UPDT_CD", F.col("svInstUpdt"))

    # ------------------------------------------------------------------
    # Split Streams from PrimaryKey transformer
    # ------------------------------------------------------------------
    # NewKeys (only inserts)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "LN_SEQ_NO",
            "svCrtRunCycExctnSk"
                .alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_NOTE_LN_SK",
        )
    )

    # Keys (all records)
    df_Keys = (
        df_enriched.select(
            "CUST_SVC_TASK_NOTE_LN_SK",
            "SRC_SYS_CD_SK",
            "svSrcSysCd".alias("SRC_SYS_CD"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "LN_SEQ_NO",
            "INSRT_UPDT_CD",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # updt (hash-file replacement)
    df_updt = (
        df_enriched.select(
            "svSrcSysCd".alias("SRC_SYS_CD"),
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "CUST_SVC_TASK_NOTE_LOC_CD",
            "NOTE_SEQ_NO",
            "LAST_UPDT_DTM",
            "LN_SEQ_NO",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "CUST_SVC_TASK_NOTE_LN_SK",
        )
    )

    # ------------------------------------------------------------------
    # Write sequential file: K_CUST_SVC_TASK_NOTE_LN.dat
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_CUST_SVC_TASK_NOTE_LN.dat"
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
    # Write parquet replacement for hash file hf_cust_svc_task_note_ln
    # ------------------------------------------------------------------
    parquet_path = f"{adls_path}/CustSvcTaskNoteLnPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------------------
    join_cond = (
        (df_allcol_dedup["SRC_SYS_CD_SK"]              == df_Keys["SRC_SYS_CD_SK"]) &
        (df_allcol_dedup["CUST_SVC_ID"]                == df_Keys["CUST_SVC_ID"]) &
        (df_allcol_dedup["TASK_SEQ_NO"]                == df_Keys["TASK_SEQ_NO"]) &
        (df_allcol_dedup["CUST_SVC_TASK_NOTE_LOC_CD"]  == df_Keys["CUST_SVC_TASK_NOTE_LOC_CD"]) &
        (df_allcol_dedup["NOTE_SEQ_NO"]                == df_Keys["NOTE_SEQ_NO"]) &
        (df_allcol_dedup["LAST_UPDT_DTM"]              == df_Keys["LAST_UPDT_DTM"]) &
        (df_allcol_dedup["LN_SEQ_NO"]                  == df_Keys["LN_SEQ_NO"])
    )

    df_Key = (
        df_allcol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_cond, "left")
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
            F.col("k.CUST_SVC_TASK_NOTE_LN_SK").alias("CUST_SVC_TASK_NOTE_LN_SK"),
            F.col("all.CUST_SVC_ID").alias("CUST_SVC_ID"),
            F.col("all.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
            F.col("all.CUST_SVC_TASK_NOTE_LOC_CD").alias("CUST_SVC_TASK_NOTE_LOC_CD"),
            F.col("all.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
            F.col("all.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            F.col("all.LN_SEQ_NO").alias("LN_SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
            F.col("all.LN_TX").alias("LN_TX"),
        )
    )

    # ------------------------------------------------------------------
    # Final container output
    # ------------------------------------------------------------------
    return df_Key