
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Container  : CapPK
Job Type   : Server Job
Folder     : Shared Containers/PrimaryKey

COPYRIGHT 2008, 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
    Shared container used for Primary Keying of Capitation job

CALLED BY : FctsCapExtr

MODIFICATIONS:
Developer          Date         Ticket            Description
-----------------  -----------  ----------------  -------------------------------------------------------------------
Parik              2008-09-03   3567              Initial program
Rishi Reddy        2011-06-20   4663              Added GL_CAT_CD_SK column
SAndrew            2011-09-09   TTR-1212          Converted hf_cap_alloc to distributed hash file
Tejaswi Gogineni   2018-10-23   INC0473770        Changed datatype for SEQ_NO

ANNOTATIONS:
This container is used in: FctsCapExtr – programs need recompilation when logic changes
Hash file (hf_cap_allcol_dist) not cleared in calling program. Distributed and needs special job to clear.
join primary key info with table info
update primary key table (K_CAP) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
Load IDS temp. table
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from typing import Dict, Tuple, List

# COMMAND ----------
def run_CapPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: Dict
) -> DataFrame:
    """
    Executes the CapPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link “AllCol”.
    df_Transform : DataFrame
        Container input link “Transform”.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Container output link “Key”.
    """

    # ------------------------------------------------------------------
    # Un-pack required parameters (once only)
    # ------------------------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage  : K_CAP_TEMP  (DB2Connector – write, truncate, runstats)
    # ------------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_CAP_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .write
        .mode("overwrite")
        .jdbc(url=ids_jdbc_url, table=f"{IDSOwner}.K_CAP_TEMP", properties=ids_jdbc_props)
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CAP_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
        SELECT k.CAP_SK,
               w.SRC_SYS_CD_SK,
               w.MBR_UNIQ_KEY,
               w.CAP_PROV_ID,
               w.ERN_DT_SK,
               w.PD_DT_SK,
               w.CAP_FUND_ID,
               w.CAP_POOL_CD,
               w.SEQ_NO,
               k.CRT_RUN_CYC_EXCTN_SK
          FROM {IDSOwner}.K_CAP_TEMP w,
               {IDSOwner}.K_CAP      k
         WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
           AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
           AND w.CAP_PROV_ID   = k.CAP_PROV_ID
           AND w.ERN_DT_SK     = k.ERN_DT_SK
           AND w.PD_DT_SK      = k.PD_DT_SK
           AND w.CAP_FUND_ID   = k.CAP_FUND_ID
           AND w.CAP_POOL_CD   = k.CAP_POOL_CD
           AND w.SEQ_NO        = k.SEQ_NO
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.MBR_UNIQ_KEY,
               w2.CAP_PROV_ID,
               w2.ERN_DT_SK,
               w2.PD_DT_SK,
               w2.CAP_FUND_ID,
               w2.CAP_POOL_CD,
               w2.SEQ_NO,
               {CurrRunCycle}
          FROM {IDSOwner}.K_CAP_TEMP w2
         WHERE NOT EXISTS (
                SELECT 1
                  FROM {IDSOwner}.K_CAP k2
                 WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
                   AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
                   AND w2.CAP_PROV_ID   = k2.CAP_PROV_ID
                   AND w2.ERN_DT_SK     = k2.ERN_DT_SK
                   AND w2.PD_DT_SK      = k2.PD_DT_SK
                   AND w2.CAP_FUND_ID   = k2.CAP_FUND_ID
                   AND w2.CAP_POOL_CD   = k2.CAP_POOL_CD
                   AND w2.SEQ_NO        = k2.SEQ_NO
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
    # Stage  : PrimaryKey  (Transformer)
    # ------------------------------------------------------------------
    df_enriched = df_W_Extract

    # ----- Surrogate-key generation (must follow exact-signature rule)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CAP_SK",
        <schema>,
        <secret_name>
    )

    df_enriched = (
        df_enriched
        .withColumn(
            "svInstUpdt",
            when(col("CAP_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svSK", col("CAP_SK"))
    )

    # ----- Output link : updt  (to hf_cap – treated as parquet)
    df_updt = (
        df_enriched
        .select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "CAP_PROV_ID",
            "ERN_DT_SK",
            "PD_DT_SK",
            "CAP_FUND_ID",
            "CAP_POOL_CD",
            "SEQ_NO",
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CAP_SK")
        )
    )

    write_files(
        df_updt,
        f"{adls_path}/CapPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True
    )

    # ----- Output link : NewKeys  (to sequential file)
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "CAP_PROV_ID",
            "ERN_DT_SK",
            "PD_DT_SK",
            "CAP_FUND_ID",
            "CAP_POOL_CD",
            "SEQ_NO",
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CAP_SK")
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CAP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    # ----- Output link : Keys  (internal join input for Merge stage)
    df_keys = (
        df_enriched
        .select(
            col("svSK").alias("CAP_SK"),
            "SRC_SYS_CD_SK",
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "CAP_PROV_ID",
            "ERN_DT_SK",
            "PD_DT_SK",
            "CAP_FUND_ID",
            "CAP_POOL_CD",
            "SEQ_NO",
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage  : hf_cap_allcol_dist  (hash file – scenario a, dedup only)
    # ------------------------------------------------------------------
    partition_cols: List[str] = [
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "CAP_PROV_ID",
        "ERN_DT_SK",
        "PD_DT_SK",
        "CAP_FUND_ID",
        "CAP_POOL_CD",
        "SEQ_NO"
    ]
    df_allcol_dedup = dedup_sort(df_AllCol, partition_cols, [])

    # ------------------------------------------------------------------
    # Stage  : Merge  (Transformer)
    # ------------------------------------------------------------------
    join_cond = (
        (df_allcol_dedup.SRC_SYS_CD_SK == df_keys.SRC_SYS_CD_SK) &
        (df_allcol_dedup.MBR_UNIQ_KEY == df_keys.MBR_UNIQ_KEY) &
        (df_allcol_dedup.CAP_PROV_ID  == df_keys.CAP_PROV_ID ) &
        (df_allcol_dedup.ERN_DT_SK    == df_keys.ERN_DT_SK   ) &
        (df_allcol_dedup.PD_DT_SK     == df_keys.PD_DT_SK    ) &
        (df_allcol_dedup.CAP_FUND_ID  == df_keys.CAP_FUND_ID ) &
        (df_allcol_dedup.CAP_POOL_CD  == df_keys.CAP_POOL_CD ) &
        (df_allcol_dedup.SEQ_NO       == df_keys.SEQ_NO      )
    )

    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
    )

    df_Key = df_merge.select(
        col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("k.INSRT_UPDT_CD"          ).alias("INSRT_UPDT_CD"),
        col("all.DISCARD_IN"           ).alias("DISCARD_IN"),
        col("all.PASS_THRU_IN"         ).alias("PASS_THRU_IN"),
        col("all.FIRST_RECYC_DT"       ).alias("FIRST_RECYC_DT"),
        col("all.ERR_CT"               ).alias("ERR_CT"),
        col("all.RECYCLE_CT"           ).alias("RECYCLE_CT"),
        col("k.SRC_SYS_CD"             ).alias("SRC_SYS_CD"),
        col("all.PRI_KEY_STRING"       ).alias("PRI_KEY_STRING"),
        col("k.CAP_SK"                 ).alias("CAP_SK"),
        col("all.MBR_UNIQ_KEY"         ).alias("MBR_UNIQ_KEY"),
        col("all.CAP_PROV_ID"          ).alias("CAP_PROV_ID"),
        col("all.ERN_DT_SK"            ).alias("ERN_DT_SK"),
        col("all.PD_DT_SK"             ).alias("PD_DT_SK"),
        col("all.CAP_FUND_ID"          ).alias("CAP_FUND_ID"),
        col("all.CAP_POOL_CD"          ).alias("CAP_POOL_CD"),
        col("all.SEQ_NO"               ).alias("SEQ_NO"),
        col("k.CRT_RUN_CYC_EXCTN_SK"   ).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle               ).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        col("all.CAP_FUND_SK"          ).alias("CAP_FUND_SK"),
        col("all.CAP_PROV_SK"          ).alias("CAP_PROV_SK"),
        col("all.CLS_ID"               ).alias("CLS_ID"),
        col("all.CLS_PLN_ID"           ).alias("CLS_PLN_ID"),
        col("all.FNCL_LOB_ID"          ).alias("FNCL_LOB_ID"),
        col("all.GRP_CK"               ).alias("GRP_CK"),
        col("all.GRGR_ID"              ).alias("GRGR_ID"),
        col("all.MBR_CK"               ).alias("MBR_CK"),
        col("all.NTWK_ID"              ).alias("NTWK_ID"),
        col("all.PD_PROV_ID"           ).alias("PD_PROV_ID"),
        col("all.PCP_PROV_ID"          ).alias("PCP_PROV_ID"),
        col("all.PROD_ID"              ).alias("PROD_ID"),
        col("all.SUBGRP_ID"            ).alias("SUBGRP_ID"),
        col("all.SUB_ID"               ).alias("SUB_ID"),
        col("all.CAP_ADJ_RSN_CD"       ).alias("CAP_ADJ_RSN_CD"),
        col("all.CAP_ADJ_STTUS_CD"     ).alias("CAP_ADJ_STTUS_CD"),
        col("all.CAP_ADJ_TYP_CD"       ).alias("CAP_ADJ_TYP_CD"),
        col("all.CAP_CAT_CD"           ).alias("CAP_CAT_CD"),
        col("all.CAP_COPAY_TYP_CD"     ).alias("CAP_COPAY_TYP_CD"),
        col("all.CAP_LOB_CD"           ).alias("CAP_LOB_CD"),
        col("all.CAP_PERD_CD"          ).alias("CAP_PERD_CD"),
        col("all.CAP_SCHD_CD"          ).alias("CAP_SCHD_CD"),
        col("all.CAP_TYP_CD"           ).alias("CAP_TYP_CD"),
        col("all.ADJ_AMT"              ).alias("ADJ_AMT"),
        col("all.CAP_AMT"              ).alias("CAP_AMT"),
        col("all.COPAY_AMT"            ).alias("COPAY_AMT"),
        col("all.FUND_RATE_AMT"        ).alias("FUND_RATE_AMT"),
        col("all.MBR_AGE"              ).alias("MBR_AGE"),
        col("all.MBR_MO_CT"            ).alias("MBR_MO_CT"),
        col("all.SBSB_CK"              ).alias("SBSB_CK"),
        col("all.CRFD_ACCT_CAT"        ).alias("CRFD_ACCT_CAT")
    )

    return df_Key
