# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

"""
***************************************************************************************************************************************************************
COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Income Invoice Sub subsidy job

CALLED BY : FctsIncomeInvcSubSbsdyExtr

PROCESSING:    
      



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Kalyan Neelam       2014-06-20              Initial programming                                                      5235                          IntegrateNewDevl          Bhoomi Dasari             6/21/2014

Goutham Kalidindi       2021-03-25          Changed billing invoice ID from Char(12)                                                      IntegrateDev2                Jeyaprasanna             2021-03-31
                                                                to Varchar(15)

Reddy Sanam           2022-07-15           Changed the shared container for a column mapping  Keys.CRT_TS [1,23] to Keys.CRT_TS
"""

# COMMAND ----------
def run_IncmInvcSubSubsdyPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    IDS Primary Key Container for Income Invoice Sub
    Used by FctsIncomeInvcSubExtr
    """

    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle            = params["CurrRunCycle"]
    SrcSysCd                = params["SrcSysCd"]
    IDSOwner                = params["IDSOwner"]
    ids_secret_name         = params["ids_secret_name"]
    ids_jdbc_url            = params["ids_jdbc_url"]
    ids_jdbc_props          = params["ids_jdbc_props"]
    adls_path               = params["adls_path"]
    adls_path_raw           = params["adls_path_raw"]
    adls_path_publish       = params["adls_path_publish"]
    FilePath                = params.get("FilePath","")

    # --------------------------------------------------
    # Stage: hf_invc_sub_sbsdy_allcol  (scenario a – intermediate hashed file)
    partition_cols_allcol = [
        "SRC_SYS_CD_SK",
        "BILL_INVC_ID",
        "SUB_UNIQ_KEY",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "COV_DUE_DT",
        "COV_STRT_DT_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD",
        "CRT_DT",
        "INVC_SUB_SBSDY_BILL_DISP_CD"
    ]
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols_allcol,
        []
    )

    # --------------------------------------------------
    # Stage: K_INVC_SUB_SUBSDY_TEMP  (load temp table, then extract)
    truncate_sql = f"TRUNCATE TABLE {IDSOwner}.K_INVC_SUB_SBSDY_TEMP"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .option("dbtable", f"{IDSOwner}.K_INVC_SUB_SBSDY_TEMP")
        .options(**ids_jdbc_props)
        .mode("append")
        .save()
    )

    runstat_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_INVC_SUB_SBSDY_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstat_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
SELECT  k.INVC_SUB_SBSDY_SK,
        w.BILL_INVC_ID,
        w.SUB_UNIQ_KEY,
        w.CLS_PLN_ID,
        w.PROD_ID,
        w.PROD_BILL_CMPNT_ID,
        w.COV_DUE_DT_SK,
        w.COV_STRT_DT_SK,
        w.INVC_SUB_SBSDY_PRM_TYP_CD,
        w.CRT_TS,
        w.INVC_SUB_SBSDY_BILL_DISP_CD,
        w.SRC_SYS_CD_SK,
        k.CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.K_INVC_SUB_SBSDY_TEMP w,
     {IDSOwner}.K_INVC_SUB_SBSDY k
WHERE w.SRC_SYS_CD_SK               = k.SRC_SYS_CD_SK
  AND w.BILL_INVC_ID                = k.BILL_INVC_ID
  AND w.SUB_UNIQ_KEY                = k.SUB_UNIQ_KEY
  AND w.CLS_PLN_ID                  = k.CLS_PLN_ID
  AND w.PROD_ID                     = k.PROD_ID
  AND w.PROD_BILL_CMPNT_ID          = k.PROD_BILL_CMPNT_ID
  AND w.COV_DUE_DT_SK               = k.COV_DUE_DT_SK 
  AND w.COV_STRT_DT_SK              = k.COV_STRT_DT_SK 
  AND w.INVC_SUB_SBSDY_PRM_TYP_CD   = k.INVC_SUB_SBSDY_PRM_TYP_CD
  AND w.CRT_TS                      = k.CRT_TS
  AND w.INVC_SUB_SBSDY_BILL_DISP_CD = k.INVC_SUB_SBSDY_BILL_DISP_CD
UNION
SELECT -1,
       w2.BILL_INVC_ID,
       w2.SUB_UNIQ_KEY,
       w2.CLS_PLN_ID,
       w2.PROD_ID,
       w2.PROD_BILL_CMPNT_ID,
       w2.COV_DUE_DT_SK,
       w2.COV_STRT_DT_SK,
       w2.INVC_SUB_SBSDY_PRM_TYP_CD,
       w2.CRT_TS,
       w2.INVC_SUB_SBSDY_BILL_DISP_CD,
       w2.SRC_SYS_CD_SK,
       {CurrRunCycle}
FROM {IDSOwner}.K_INVC_SUB_SBSDY_TEMP w2
WHERE NOT EXISTS (
    SELECT 1
    FROM {IDSOwner}.K_INVC_SUB_SBSDY k2
    WHERE w2.SRC_SYS_CD_SK             = k2.SRC_SYS_CD_SK
      AND w2.BILL_INVC_ID              = k2.BILL_INVC_ID
      AND w2.SUB_UNIQ_KEY              = k2.SUB_UNIQ_KEY
      AND w2.CLS_PLN_ID                = k2.CLS_PLN_ID
      AND w2.PROD_ID                   = k2.PROD_ID
      AND w2.PROD_BILL_CMPNT_ID        = k2.PROD_BILL_CMPNT_ID
      AND w2.COV_DUE_DT_SK             = k2.COV_DUE_DT_SK 
      AND w2.COV_STRT_DT_SK            = k2.COV_STRT_DT_SK 
      AND w2.INVC_SUB_SBSDY_PRM_TYP_CD = k2.INVC_SUB_SBSDY_PRM_TYP_CD
      AND w2.CRT_TS                    = k2.CRT_TS
      AND w2.INVC_SUB_SBSDY_BILL_DISP_CD = k2.INVC_SUB_SBSDY_BILL_DISP_CD
)
"""
    df_w_extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # Transformer: PrimaryKey
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("INVC_SUB_SBSDY_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svBillInvcId", F.trim(F.col("BILL_INVC_ID")))
        .withColumn("svClsPlnId",  F.trim(F.col("CLS_PLN_ID")))
        .withColumn("svProdId",    F.trim(F.col("PROD_ID")))
        .withColumn("svprodBillCmpntId", F.trim(F.col("PROD_BILL_CMPNT_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svSK", F.col("INVC_SUB_SBSDY_SK"))
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # Output link: updt (to hf_invc_sub_subsdy)
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("svBillInvcId").alias("BILL_INVC_ID"),
        "SUB_UNIQ_KEY",
        F.col("svClsPlnId").alias("CLS_PLN_ID"),
        F.col("svProdId").alias("PROD_ID"),
        F.col("svprodBillCmpntId").alias("PROD_BILL_CMPNT_ID"),
        "COV_DUE_DT_SK",
        "COV_STRT_DT_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD",
        "CRT_TS",
        "INVC_SUB_SBSDY_BILL_DISP_CD",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("INVC_SUB_SBSDY_SK")
    )

    # Output link: NewKeys (sequential file) – only inserts
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "BILL_INVC_ID",
            "SUB_UNIQ_KEY",
            "CLS_PLN_ID",
            "PROD_ID",
            "PROD_BILL_CMPNT_ID",
            "COV_DUE_DT_SK",
            "COV_STRT_DT_SK",
            "INVC_SUB_SBSDY_PRM_TYP_CD",
            "CRT_TS",
            "INVC_SUB_SBSDY_BILL_DISP_CD",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("INVC_SUB_SBSDY_SK")
        )
    )

    # Output link: Keys (to Merge)
    df_keys = df_enriched.select(
        F.col("svSK").alias("INVC_SUB_SBSDY_SK"),
        "SRC_SYS_CD_SK",
        F.col("svBillInvcId").alias("BILL_INVC_ID"),
        "SUB_UNIQ_KEY",
        F.col("svClsPlnId").alias("CLS_PLN_ID"),
        F.col("svProdId").alias("PROD_ID"),
        F.col("svprodBillCmpntId").alias("PROD_BILL_CMPNT_ID"),
        "COV_DUE_DT_SK",
        "COV_STRT_DT_SK",
        "INVC_SUB_SBSDY_PRM_TYP_CD",
        "CRT_TS",
        "INVC_SUB_SBSDY_BILL_DISP_CD",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # Write hashed file replacement as parquet
    write_files(
        df_updt,
        f"{adls_path}/IncmInvcSubSubsdyPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # Write Sequential file for new keys
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INVC_SUB_SBSDY.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: Merge
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.BILL_INVC_ID") == F.col("k.BILL_INVC_ID")) &
        (F.col("all.SUB_UNIQ_KEY") == F.col("k.SUB_UNIQ_KEY")) &
        (F.col("all.CLS_PLN_ID") == F.col("k.CLS_PLN_ID")) &
        (F.col("all.PROD_ID") == F.col("k.PROD_ID")) &
        (F.col("all.PROD_BILL_CMPNT_ID") == F.col("k.PROD_BILL_CMPNT_ID")) &
        (F.col("all.COV_DUE_DT") == F.col("k.COV_DUE_DT_SK")) &
        (F.col("all.COV_STRT_DT_SK") == F.col("k.COV_STRT_DT_SK")) &
        (F.col("all.INVC_SUB_SBSDY_PRM_TYP_CD") == F.col("k.INVC_SUB_SBSDY_PRM_TYP_CD")) &
        (F.col("all.CRT_DT") == F.col("k.CRT_TS")) &
        (F.col("all.INVC_SUB_SBSDY_BILL_DISP_CD") == F.col("k.INVC_SUB_SBSDY_BILL_DISP_CD"))
    )

    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
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
            F.col("k.INVC_SUB_SBSDY_SK").alias("INVC_SUB_SBSDY_SK"),
            F.col("k.BILL_INVC_ID").alias("BILL_INVC_ID"),
            F.col("k.CLS_PLN_ID").alias("CLS_PLN_ID"),
            F.col("k.PROD_ID").alias("PROD_ID"),
            F.col("k.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
            F.col("k.COV_DUE_DT_SK").alias("COV_DUE_DT"),
            F.col("k.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
            F.col("k.INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
            F.col("k.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
            F.col("k.CRT_TS").alias("CRT_DT"),
            F.col("k.INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.COV_END_DT_SK").alias("COV_END_DT_SK"),
            F.col("all.CLS_ID").alias("CLS_ID"),
            F.col("all.CSPI_ID").alias("CSPI_ID"),
            F.col("all.GRP_ID").alias("GRP_ID"),
            F.col("all.INVC_ID").alias("INVC_ID"),
            F.col("all.SUBGRP_ID").alias("SUBGRP_ID"),
            F.col("all.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
            F.col("all.SUB_SBSDY_AMT").alias("SUB_SBSDY_AMT")
        )
    )

    # --------------------------------------------------
    # Return container output
    return df_merge

# COMMAND ----------