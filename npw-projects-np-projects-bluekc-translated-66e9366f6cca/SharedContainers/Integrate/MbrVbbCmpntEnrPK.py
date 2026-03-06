# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Mbr Vbb Cmpnt Enr

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Raja Gummadi    2013-05-22              Initial Programming                                                      4963 VBB Phase III     IntegrateNewDevl              Bhoomi Dasari           7/8/2013
"""

from functools import reduce
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def run_MbrVbbCmpntEnrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    join primary key info with table info
    update primary key table (K_VBB_PLN) with new keys created today
    Assign primary surrogate key
    primary key hash file only contains current run keys and is cleared before writing
    Temp table is tuncated before load and runstat done after load
    SQL joins temp table with key table to assign known keys
    Load IDS temp. table
    Hash file (hf_vbb_cmpnt_allcol) cleared in the calling program - IdsMbrVbbCmpntEnrExtr
    Hashfile cleared in calling program
    This container is used in:
        IdsMbrVbbCmpntEnrExtr
    These programs need to be re-compiled when logic changes
    """

    # ------------------------------------------------------------------
    # Unpack parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Handle hashed-file scenario a – replace with dedup logic
    # ------------------------------------------------------------------
    key_columns = ["MBR_UNIQ_KEY", "VBB_CMPNT_UNIQ_KEY", "SRC_SYS_CD_SK"]

    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=key_columns,
        sort_cols=[("MBR_UNIQ_KEY", "A")],
    )

    df_Transform_dedup = dedup_sort(
        df_Transform,
        partition_cols=key_columns,
        sort_cols=[("MBR_UNIQ_KEY", "A")],
    )

    # ------------------------------------------------------------------
    # Load temp table K_MBR_VBB_CMPNT_ENR_TEMP
    # ------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_MBR_VBB_CMPNT_ENR_TEMP"

    # Overwrite temp table with new deduped data
    (
        df_Transform_dedup.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("overwrite")
        .save()
    )

    # ------------------------------------------------------------------
    # Extract W_Extract set from DB
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT
        k.MBR_VBB_CMPNT_ENR_SK,
        w.MBR_UNIQ_KEY,
        w.VBB_CMPNT_UNIQ_KEY,
        w.SRC_SYS_CD_SK,
        k.CRT_RUN_CYC_EXCTN_SK
    FROM {temp_table} w
    JOIN {IDSOwner}.K_MBR_VBB_CMPNT_ENR k
         ON w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
        AND w.VBB_CMPNT_UNIQ_KEY  = k.VBB_CMPNT_UNIQ_KEY
        AND w.MBR_UNIQ_KEY        = k.MBR_UNIQ_KEY
    UNION
    SELECT
        -1,
        w2.MBR_UNIQ_KEY,
        w2.VBB_CMPNT_UNIQ_KEY,
        w2.SRC_SYS_CD_SK,
        {CurrRunCycle}
    FROM {temp_table} w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_MBR_VBB_CMPNT_ENR k2
        WHERE w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
          AND w2.VBB_CMPNT_UNIQ_KEY = k2.VBB_CMPNT_UNIQ_KEY
          AND w2.MBR_UNIQ_KEY       = k2.MBR_UNIQ_KEY
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
    # PrimaryKey Transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(F.col("MBR_VBB_CMPNT_ENR_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("MBR_VBB_CMPNT_ENR_SK") == -1, F.lit(None)).otherwise(
                F.col("MBR_VBB_CMPNT_ENR_SK")
            ),
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>,
    )

    # ------------------------------------------------------------------
    # Derive output links from PrimaryKey Transformer
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("MBR_VBB_CMPNT_ENR_SK"),
    )

    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I").select(
            "MBR_UNIQ_KEY",
            "VBB_CMPNT_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MBR_VBB_CMPNT_ENR_SK"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        )
    )

    df_keys = df_enriched.select(
        F.col("svSK").alias("MBR_VBB_CMPNT_ENR_SK"),
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )

    # ------------------------------------------------------------------
    # Write hashed-file target (scenario c) as parquet
    # ------------------------------------------------------------------
    parquet_path_updt = f"{adls_path}/MbrVbbCmpntEnrPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Write sequential file for new keys
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MBR_VBB_CMPNT_ENR.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Merge Transformer – build final Key output
    # ------------------------------------------------------------------
    join_expr = [
        df_AllCol_dedup["MBR_UNIQ_KEY"] == df_keys["MBR_UNIQ_KEY"],
        df_AllCol_dedup["VBB_CMPNT_UNIQ_KEY"] == df_keys["VBB_CMPNT_UNIQ_KEY"],
        df_AllCol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"],
    ]

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), reduce(lambda x, y: x & y, join_expr), "left")
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
            F.col("k.MBR_VBB_CMPNT_ENR_SK").alias("MBR_VBB_CMPNT_ENR_SK"),
            F.col("all.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("all.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
            F.col("all.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
            F.col("all.HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
            F.col("all.HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
            F.col("all.MEIP_ENROLLED_BY").alias("MEIP_ENROLLED_BY"),
            F.col("all.MEIP_STATUS").alias("MEIP_STATUS"),
            F.col("all.MEIP_TERMED_BY").alias("MEIP_TERMED_BY"),
            F.col("all.MEIP_TERM_REASON").alias("MEIP_TERM_REASON"),
            F.col("all.MBR_CNTCT_IN").alias("MBR_CNTCT_IN"),
            F.col("all.MBR_CNTCT_LAST_UPDT_DT_SK").alias("MBR_CNTCT_LAST_UPDT_DT_SK"),
            F.col("all.MBR_VBB_CMPNT_CMPLTN_DT_SK").alias("MBR_VBB_CMPNT_CMPLTN_DT_SK"),
            F.col("all.MBR_VBB_CMPNT_ENR_DT_SK").alias("MBR_VBB_CMPNT_ENR_DT_SK"),
            F.col("all.MBR_VBB_CMPNT_TERM_DT_SK").alias("MBR_VBB_CMPNT_TERM_DT_SK"),
            F.col("all.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
            F.col("all.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
            F.col("all.VBB_VNDR_UNIQ_KEY").alias("VBB_VNDR_UNIQ_KEY"),
            F.col("all.VNDR_PGM_SEQ_NO").alias("VNDR_PGM_SEQ_NO"),
            F.col("all.MBR_CMPLD_ACHV_LVL_CT").alias("MBR_CMPLD_ACHV_LVL_CT"),
            F.col("all.TRZ_MBR_UNVRS_ID").alias("TRZ_MBR_UNVRS_ID"),
            F.col("all.HIPL_ID").alias("HIPL_ID"),
        )
    )

    # ------------------------------------------------------------------
    # Return the single output stream
    # ------------------------------------------------------------------
    return df_merge