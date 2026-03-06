# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name      : IncmInvcSubPK
Job Type      : Server Job
Category      : DS_Integrate
Folder Path   : Shared Containers/PrimaryKey

DESCRIPTION:
    Shared container used for Primary Keying of Income Invoice Sub job
    IDS Primary Key Container for Income Invoice Sub
    Used by FctsIncomeInvcSubExtr
    Hash file (hf_invc_sub_allcol) cleared in calling job
    SQL joins temp table with key table to assign known keys
    Temp table is truncated before load and runstat done after load
    Load IDS temp. table
    join primary key info with table info
    update primary key table (K_INVC_SUB) with new keys created today
    primary key hash file only contains current run keys and is cleared before writing
    Assign primary surrogate key

MODIFICATIONS:
    2008-09-30  Bhoomi Dasari        Initial program
    2021-03-24  Goutham Kalidindi    Changed datatype of BLIV_ID
    2022-07-15  Reddy Sanam          Transformer “Merge” change
"""
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

# COMMAND ----------
def run_IncmInvcSubPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the shared-container logic for IncmInvcSubPK.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link “AllCol”.
    df_Transform : DataFrame
        Input stream corresponding to link “Transform”.
    params : dict
        Runtime parameter dictionary.

    Returns
    -------
    DataFrame
        Output stream corresponding to link “Key”.
    """

    # --------------------------------------------------
    # Unpack run-time parameters (exactly once)
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage: hf_invc_sub_allcol  (Scenario a – intermediate hash file)
    partition_cols = [
        "SRC_SYS_CD_SK", "BILL_INVC_ID", "SUB_UNIQ_KEY", "CLS_PLN_ID",
        "PROD_ID", "PROD_BILL_CMPNT_ID", "COV_DUE_DT", "COV_STRT_DT_SK",
        "INVC_SUB_PRM_TYP_CD", "CRT_DT", "INVC_SUB_BILL_DISP_CD"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=partition_cols,
        sort_cols=<…>
    )

    # --------------------------------------------------
    # Stage: K_INVC_SUB_TEMP  (load temp table, then extract W_Extract)
    temp_table = f"{IDSOwner}.K_INVC_SUB_TEMP"
    df_Transform.write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_INVC_SUB_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
SELECT  k.INVC_SUB_SK,
        w.SRC_SYS_CD_SK,
        w.BILL_INVC_ID,
        w.SUB_UNIQ_KEY,
        w.CLS_PLN_ID,
        w.PROD_ID,
        w.PROD_BILL_CMPNT_ID,
        w.COV_DUE_DT_SK,
        w.COV_STRT_DT_SK,
        w.INVC_SUB_PRM_TYP_CD,
        w.CRT_TS,
        w.INVC_SUB_BILL_DISP_CD,
        k.CRT_RUN_CYC_EXCTN_SK
FROM   {IDSOwner}.K_INVC_SUB_TEMP w
INNER  JOIN {IDSOwner}.K_INVC_SUB k
       ON  w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
       AND w.BILL_INVC_ID           = k.BILL_INVC_ID
       AND w.SUB_UNIQ_KEY           = k.SUB_UNIQ_KEY
       AND w.CLS_PLN_ID             = k.CLS_PLN_ID
       AND w.PROD_ID                = k.PROD_ID
       AND w.PROD_BILL_CMPNT_ID     = k.PROD_BILL_CMPNT_ID
       AND w.COV_DUE_DT_SK          = k.COV_DUE_DT_SK
       AND w.COV_STRT_DT_SK         = k.COV_STRT_DT_SK
       AND w.INVC_SUB_PRM_TYP_CD    = k.INVC_SUB_PRM_TYP_CD
       AND w.CRT_TS                 = k.CRT_TS
       AND w.INVC_SUB_BILL_DISP_CD  = k.INVC_SUB_BILL_DISP_CD
UNION
SELECT -1,
        w2.SRC_SYS_CD_SK,
        w2.BILL_INVC_ID,
        w2.SUB_UNIQ_KEY,
        w2.CLS_PLN_ID,
        w2.PROD_ID,
        w2.PROD_BILL_CMPNT_ID,
        w2.COV_DUE_DT_SK,
        w2.COV_STRT_DT_SK,
        w2.INVC_SUB_PRM_TYP_CD,
        w2.CRT_TS,
        w2.INVC_SUB_BILL_DISP_CD,
        {CurrRunCycle}
FROM   {IDSOwner}.K_INVC_SUB_TEMP w2
WHERE  NOT EXISTS (
          SELECT 1
          FROM   {IDSOwner}.K_INVC_SUB k2
          WHERE  w2.SRC_SYS_CD_SK          = k2.SRC_SYS_CD_SK
          AND    w2.BILL_INVC_ID           = k2.BILL_INVC_ID
          AND    w2.SUB_UNIQ_KEY           = k2.SUB_UNIQ_KEY
          AND    w2.CLS_PLN_ID             = k2.CLS_PLN_ID
          AND    w2.PROD_ID                = k2.PROD_ID
          AND    w2.PROD_BILL_CMPNT_ID     = k2.PROD_BILL_CMPNT_ID
          AND    w2.COV_DUE_DT_SK          = k2.COV_DUE_DT_SK
          AND    w2.COV_STRT_DT_SK         = k2.COV_STRT_DT_SK
          AND    w2.INVC_SUB_PRM_TYP_CD    = k2.INVC_SUB_PRM_TYP_CD
          AND    w2.CRT_TS                 = k2.CRT_TS
          AND    w2.INVC_SUB_BILL_DISP_CD  = k2.INVC_SUB_BILL_DISP_CD
)
"""
    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # --------------------------------------------------
    # Stage: PrimaryKey  (transform & surrogate key)
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("INVC_SUB_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svBillInvcId", F.trim(F.col("BILL_INVC_ID")))
        .withColumn("svClsPlnId",  F.trim(F.col("CLS_PLN_ID")))
        .withColumn("svProdId",    F.trim(F.col("PROD_ID")))
        .withColumn("svprodBillCmpntId", F.trim(F.col("PROD_BILL_CMPNT_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'INVC_SUB_SK',<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("svSK", F.col("INVC_SUB_SK")) \
                             .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))

    # Output link: updt
    df_updt = (
        df_enriched.select(
            "SRC_SYS_CD",                # SrcSysCd param
            "svBillInvcId",              # BILL_INVC_ID
            "SUB_UNIQ_KEY",
            "svClsPlnId",                # CLS_PLN_ID
            "svProdId",                  # PROD_ID
            "svprodBillCmpntId",         # PROD_BILL_CMPNT_ID
            "COV_DUE_DT_SK",
            "COV_STRT_DT_SK",
            "INVC_SUB_PRM_TYP_CD",
            "CRT_TS",
            "INVC_SUB_BILL_DISP_CD",
            "svCrtRunCycExctnSk",        # CRT_RUN_CYC_EXCTN_SK
            "svSK"                       # INVC_SUB_SK
        )
    )

    # Output link: NewKeys (constraint svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
                   .select(
                       "SRC_SYS_CD_SK",
                       "svBillInvcId",
                       "SUB_UNIQ_KEY",
                       "svClsPlnId",
                       "svProdId",
                       "svprodBillCmpntId",
                       "COV_DUE_DT_SK",
                       "COV_STRT_DT_SK",
                       "INVC_SUB_PRM_TYP_CD",
                       "CRT_TS",
                       "INVC_SUB_BILL_DISP_CD",
                       "svCrtRunCycExctnSk",
                       "svSK"
                   )
    )

    # Output link: Keys
    df_Keys = (
        df_enriched.select(
            "svSK",
            "SRC_SYS_CD_SK",
            "svBillInvcId",
            "SUB_UNIQ_KEY",
            "svClsPlnId",
            "svProdId",
            "svprodBillCmpntId",
            "COV_DUE_DT_SK",
            "COV_STRT_DT_SK",
            "INVC_SUB_PRM_TYP_CD",
            "CRT_TS",
            "INVC_SUB_BILL_DISP_CD",
            "SRC_SYS_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            "svCrtRunCycExctnSk"
        )
    )

    # --------------------------------------------------
    # Persist parquet & sequential outputs
    write_files(
        df_updt,
        f"{adls_path}/IncmInvcSubPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_INVC_SUB.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: Merge
    join_keys = [
        "SRC_SYS_CD_SK", "BILL_INVC_ID", "SUB_UNIQ_KEY", "CLS_PLN_ID",
        "PROD_ID", "PROD_BILL_CMPNT_ID", "COV_DUE_DT_SK", "COV_STRT_DT_SK",
        "INVC_SUB_PRM_TYP_CD", "CRT_TS", "INVC_SUB_BILL_DISP_CD"
    ]
    join_cond = reduce(
        lambda a, b: a & b,
        [F.col(f"all.{c}") == F.col(f"k.{c}") for c in join_keys]
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
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
            F.col("k.svSK").alias("INVC_SK"),
            F.col("k.svBillInvcId").alias("BILL_INVC_ID"),
            F.col("k.svClsPlnId").alias("CLS_PLN_ID"),
            F.col("k.svProdId").alias("PROD_ID"),
            F.col("k.svprodBillCmpntId").alias("PROD_BILL_CMPNT_ID"),
            F.col("k.COV_DUE_DT_SK").alias("COV_DUE_DT"),
            F.col("k.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
            F.col("all.COV_END_DT_SK").alias("COV_END_DT_SK"),
            F.col("k.INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
            F.col("k.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
            F.col("k.CRT_TS").alias("CRT_DT"),
            F.col("k.INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
            F.col("all.CLS_ID").alias("CLS_ID"),
            F.col("all.CSPI_ID").alias("CSPI_ID"),
            F.col("all.GRP_ID").alias("GRP_ID"),
            F.col("all.INVC_ID").alias("INVC_ID"),
            F.col("all.SUBGRP_ID").alias("SUBGRP_ID"),
            F.col("all.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
            F.col("all.DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
            F.col("all.SUB_PRM_AMT").alias("SUB_PRM_AMT"),
            F.col("all.DPNDT_CT").alias("DPNDT_CT"),
            F.col("all.SUB_CT").alias("SUB_CT"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.VOL_COV_AMT").alias("VOL_COV_AMT")
        )
    )

    # --------------------------------------------------
    return df_merge