# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container  : RiskCatPK
Folder Path       : Shared Containers/PrimaryKey
Job Description   :
* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of RiskCat job

CALLED BY : ImpProRiskCatExtr
            McSourceRiskCatExtr
            UwsRiskCatExtr

PROCESSING:    

MODIFICATIONS:
Developer            Date          Change Description                                                               Project/Altius #      Development Project   Code Reviewer       Date Reviewed
-------------------  ------------  -------------------------------------------------------------------------------  --------------------  --------------------  ------------------  -------------
Bhoomi Dasari        2008-08-25    Initial program                                                                  3567 Primary Key       devlIDS              Steph Goddard        09/02/2008
Kalyan Neelam        2010-08-11    Updated the Output SQL in K_RISK_CAT_TEMP - join to the CD_MPPNG table           4297                  RebuildIntNewDevl

DataStage Annotations:
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_RISK_CAT) with new keys created today
- Assign primary surrogate key
- Hash file (hf_risk_cat_allcol) cleared in calling job
- Used by  ImpProRiskCatExtr, McSourceRiskCatExtr
- IDS Primary Key Container for Risk Cat
- join primary key info with table info
- Load IDS temp. table
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_RiskCatPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the RiskCatPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Runtime parameters and JDBC configs.

    Returns
    -------
    DataFrame
        Output stream corresponding to link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]

    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Stage : hf_risk_cat_allcol  (scenario a – intermediate hash file)
    #        Replace with dedup logic on keys SRC_SYS_CD_SK, RISK_CAT_ID
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "RISK_CAT_ID"],
        sort_cols=[]
    )

    # ------------------------------------------------------------------
    # Stage : K_RISK_CAT_TEMP  (DB2 Connector)
    #          1) Load temp table
    #          2) After-SQL runstats
    #          3) Extract W_Extract
    # ------------------------------------------------------------------
    # 1. Truncate / overwrite the temp table
    execute_dml(
        query=f"TRUNCATE TABLE {IDSOwner}.K_RISK_CAT_TEMP IMMEDIATE",
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    (
        df_Transform
        .select("SRC_SYS_CD_SK", "RISK_CAT_ID")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_RISK_CAT_TEMP")
        .mode("overwrite")
        .save()
    )

    # 2. After-SQL : runstats
    execute_dml(
        query=f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_RISK_CAT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    # 3. Extract W_Extract
    extract_query = f"""
        SELECT  k.RISK_CAT_SK,
                w.SRC_SYS_CD_SK,
                w.RISK_CAT_ID,
                k.CRT_RUN_CYC_EXCTN_SK,
                CD.TRGT_CD
        FROM {IDSOwner}.K_RISK_CAT_TEMP w
        JOIN {IDSOwner}.K_RISK_CAT k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.RISK_CAT_ID   = k.RISK_CAT_ID
        JOIN {IDSOwner}.CD_MPPNG CD
          ON w.SRC_SYS_CD_SK = CD.CD_MPPNG_SK

        UNION

        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.RISK_CAT_ID,
                {CurrRunCycle},
                CD1.TRGT_CD
        FROM {IDSOwner}.K_RISK_CAT_TEMP w2
        JOIN {IDSOwner}.CD_MPPNG CD1
          ON w2.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK
        WHERE NOT EXISTS (
              SELECT 1
              FROM {IDSOwner}.K_RISK_CAT k2
              WHERE k2.SRC_SYS_CD_SK = w2.SRC_SYS_CD_SK
                AND k2.RISK_CAT_ID   = w2.RISK_CAT_ID
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
        .withColumn(
            "svInstUpdt",
            F.when(F.col("RISK_CAT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svRiskCatId",
            F.trim(F.col("RISK_CAT_ID"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("RISK_CAT_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # SurrogateKeyGen to populate svSK where needed
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Links from PrimaryKey
    # ------------------------------------------------------------------
    # updt link
    df_updt = (
        df_enriched.select(
            F.col("TRGT_CD").alias("SRC_SYS_CD"),
            F.col("svRiskCatId").alias("RISK_CAT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("RISK_CAT_SK")
        )
    )

    # NewKeys link (svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svRiskCatId").alias("RISK_CAT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("RISK_CAT_SK")
        )
    )

    # Keys link
    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("RISK_CAT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("TRGT_CD").alias("SRC_SYS_CD"),
            F.col("svRiskCatId").alias("RISK_CAT_ID"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage : Merge (Transformer)
    # ------------------------------------------------------------------
    join_expr = [
        df_AllColOut["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"],
        df_AllColOut["RISK_CAT_ID"]   == df_Keys["RISK_CAT_ID"]
    ]

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
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
            F.col("k.RISK_CAT_SK"),
            F.col("all.RISK_CAT_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MAJ_PRCTC_CAT_CD"),
            F.col("all.RISK_CAT_DESC"),
            F.col("all.RISK_CAT_LABEL"),
            F.col("all.RISK_CAT_LONG_DESC"),
            F.col("all.RISK_CAT_NM")
        )
    )

    # ------------------------------------------------------------------
    # Stage : K_RISK_CAT  (Sequential File write)
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_RISK_CAT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage : hf_risk_cat  (Parquet write, scenario c)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/RiskCatPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return the container output(s)
    # ------------------------------------------------------------------
    return df_Key