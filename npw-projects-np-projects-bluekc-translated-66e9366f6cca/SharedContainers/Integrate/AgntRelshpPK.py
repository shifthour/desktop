"""AgntRelshpPK – Shared Container Conversion
-------------------------------------------------------------------------------------------------
IDS Primary Key Container for Commission Agent Relshp
Used by

FctsAgntRelshpExtr
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_AGNT_RELSHP) with new keys created today
Assign primary surrogate key
Hash file (hf_agnt_relshp_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys

* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of Comm Agnt Relshp job
CALLED BY : FctsAgntRelshpExtr
"""

# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_AgntRelshpPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Parameter unpacking
    CurrRunCycle            = params["CurrRunCycle"]
    IDSOwner                = params["IDSOwner"]
    ids_secret_name         = params["ids_secret_name"]
    ids_jdbc_url            = params["ids_jdbc_url"]
    ids_jdbc_props          = params["ids_jdbc_props"]
    adls_path               = params["adls_path"]
    adls_path_raw           = params["adls_path_raw"]
    adls_path_publish       = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage hf_agnt_relshp_allcol  (Scenario a – intermediate hash file)
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "AGNT_ID", "REL_AGNT_ID", "EFF_DT"],
        []
    )

    # ------------------------------------------------------------------
    # Stage K_AGNT_RELSHP_TEMP  – load temp table
    temp_table = f"{IDSOwner}.K_AGNT_RELSHP_TEMP"
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {temp_table} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Stage K_AGNT_RELSHP_TEMP  – extract (W_Extract)
    extract_query = f"""
    SELECT  k.AGNT_RELSHP_SK,
            w.SRC_SYS_CD_SK,
            w.AGNT_ID,
            w.REL_AGNT_ID,
            w.EFF_DT_SK,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_AGNT_RELSHP_TEMP w
    JOIN {IDSOwner}.K_AGNT_RELSHP k
         ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
        AND w.AGNT_ID       = k.AGNT_ID
        AND w.REL_AGNT_ID   = k.REL_AGNT_ID
        AND w.EFF_DT_SK     = k.EFF_DT_SK
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD_SK,
            w2.AGNT_ID,
            w2.REL_AGNT_ID,
            w2.EFF_DT_SK,
            {CurrRunCycle}
    FROM {IDSOwner}.K_AGNT_RELSHP_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_AGNT_RELSHP k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.AGNT_ID       = k2.AGNT_ID
          AND w2.REL_AGNT_ID   = k2.REL_AGNT_ID
          AND w2.EFF_DT_SK     = k2.EFF_DT_SK
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
    # Stage PrimaryKey – transformation
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("AGNT_RELSHP_SK") == F.lit(-1), F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("SrcSysCd", F.lit("FACETS"))
        .withColumn("svAgntId", F.trim(F.col("AGNT_ID")))
        .withColumn("svRelAgntId", F.trim(F.col("REL_AGNT_ID")))
        .withColumn(
            "svSK",
            F.when(F.col("AGNT_RELSHP_SK") == F.lit(-1), F.lit(None).cast("long"))
             .otherwise(F.col("AGNT_RELSHP_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("AGNT_RELSHP_SK") == F.lit(-1), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svSK',
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Stage PrimaryKey – updt link (to hf_agnt_relshp, scenario c → parquet)
    df_updt = df_enriched.select(
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svAgntId").alias("AGNT_ID"),
        F.col("svRelAgntId").alias("REL_AGNT_ID"),
        F.col("EFF_DT_SK"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("AGNT_RELSHP_SK")
    )

    write_files(
        df_updt,
        f"{adls_path}/AgntRelshpPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage PrimaryKey – NewKeys link (sequential file)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svAgntId").alias("AGNT_ID"),
            F.col("svRelAgntId").alias("REL_AGNT_ID"),
            F.col("EFF_DT_SK"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("AGNT_RELSHP_SK")
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_AGNT_RELSHP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage PrimaryKey – Keys link
    df_Keys = df_enriched.select(
        F.col("svSK").alias("AGNT_RELSHP_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svAgntId").alias("AGNT_ID"),
        F.col("svRelAgntId").alias("REL_AGNT_ID"),
        F.col("EFF_DT_SK"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Stage Merge – join AllColOut with Keys
    join_expr = (
        (F.col("all.AGNT_ID")      == F.col("k.AGNT_ID")) &
        (F.col("all.REL_AGNT_ID") == F.col("k.REL_AGNT_ID"))
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN").alias("DISCARD_IN"),
        F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("all.ERR_CT").alias("ERR_CT"),
        F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("k.AGNT_RELSHP_SK").alias("AGNT_RELSHP_SK"),
        F.col("all.AGNT_ID").alias("AGNT_ID"),
        F.col("all.REL_AGNT_ID").alias("REL_AGNT_ID"),
        F.col("all.EFF_DT").alias("EFF_DT"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.AGNT").alias("AGNT"),
        F.col("all.REL_AGNT").alias("REL_AGNT"),
        F.col("all.AGNT_RELSHP_TERM_RSN_CD").alias("AGNT_RELSHP_TERM_RSN_CD"),
        F.col("all.TERM_DT").alias("TERM_DT")
    )

    # ------------------------------------------------------------------
    # return container output
    return df_Key