
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Claim Prov

PROCESSING:    


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Ralph Tucker     2008-07-29               Initial program                                                                    3657 Primary Key           devlIDS                    Steph Goddard             08/01/2008


Parik                     2008-08-14          Made changes to the container                                          3567 Primary Key            devlIDS                    Steph Goddard             08/19/2008
Kalyan Neelam     2009-12-30           Added documentation for Medicaid                                    4110                              IntegrateCurDevl       Steph Goddard             01/11/2010
Kalyan Neelam    2010-12-24            Updated documentation with new source Medtrak            4616                              IntegrateNewDevl      Steph Goddard             12/28/2010
Madhavan B       2017-06-20           Added new column SVC_FCLTY_LOC_NTNL_PROV_ID  5788 - BHI Updates       IntegrateDev2             Kalyan Neelam         2017-07-07
Sudhir Bomshetty  2018-04-16           Added NTNL_PROV_ID column                                        5781 HEDIS                 IntegrateDev2             Kalyan Neelam         2018-04-19
"""

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_ClmProvPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PySpark translation of the DataStage shared container `ClmProvPK`.
    Consumes:
        df_AllCol   – input stream that previously landed in hash file `hf_clm_prov_allcol`
        df_Transform – input stream used to populate IDS temp table `K_CLM_PROV_TEMP`
    Produces:
        DataFrame for the container output link `Key`
    """

    # --------------------------------------------------
    # Unpack runtime parameters (exactly once)
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Stage: hf_clm_prov_allcol  (scenario a – intermediate hash file)
    df_allcol_clean = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_PROV_ROLE_TYP_CD"],
        []
    )
    # --------------------------------------------------

    # --------------------------------------------------
    # Stage: K_CLM_PROV_TEMP – load to IDS (write) then extract (read)
    #   1. Truncate / reload the temp table
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_CLM_PROV_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_PROV_TEMP")
        .mode("overwrite")
        .save()
    )

    #   2. Extract combined dataset mirroring DataStage SQL
    extract_query = f"""
    SELECT k.CLM_PROV_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_PROV_ROLE_TYP_CD,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_PROV_TEMP w
    JOIN {IDSOwner}.K_CLM_PROV k
      ON w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
     AND w.CLM_ID               = k.CLM_ID
     AND w.CLM_PROV_ROLE_TYP_CD = k.CLM_PROV_ROLE_TYP_CD

    UNION

    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_PROV_ROLE_TYP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_PROV_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_PROV k2
        WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID               = k2.CLM_ID
          AND w2.CLM_PROV_ROLE_TYP_CD = k2.CLM_PROV_ROLE_TYP_CD
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

    # --------------------------------------------------
    # Stage: PrimaryKey (CTransformer)
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_PROV_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("CLM_PROV_SK") == -1, F.lit(None)).otherwise(F.col("CLM_PROV_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Replace KeyMgtGetNextValueConcurrent with SurrogateKeyGen
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svSK',
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------

    # --------------------------------------------------
    # Prepare downstream DataFrames from PrimaryKey outputs
    # updt link
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_PROV_SK")
    )

    # NewKeys link  (constraint svInstUpdt = 'I')
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_PROV_ROLE_TYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_PROV_SK")
        )
    )

    # Keys link
    df_keys = df_enriched.select(
        F.col("svSK").alias("CLM_PROV_SK"),
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------

    # --------------------------------------------------
    # Stage: hf_clm_prov  (scenario c – write parquet)
    write_files(
        df_updt,
        f"{adls_path}/ClmProvPK_updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True
    )

    # Stage: K_CLM_PROV  (sequential file)
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_PROV.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False
    )
    # --------------------------------------------------

    # --------------------------------------------------
    # Stage: Merge  (CTransformer)
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.CLM_ID") == F.col("k.CLM_ID")) &
        (F.col("all.CLM_PROV_ROLE_TYP_CD") == F.col("k.CLM_PROV_ROLE_TYP_CD"))
    )

    df_merge = (
        df_allcol_clean.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.CLM_PROV_SK"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("all.CLM_ID"),
            F.col("all.CLM_PROV_ROLE_TYP_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.PROV_ID"),
            F.col("all.TAX_ID"),
            F.col("all.SVC_FCLTY_LOC_NTNL_PROV_ID"),
            F.col("all.NTNL_PROV_ID")
        )
    )
    # --------------------------------------------------

    return df_merge
# COMMAND ----------
