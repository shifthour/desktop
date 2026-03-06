# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container  : AplMcasPkey
DataStage JobType : Server Job
Folder Path       : Shared Containers

* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_1 12/20/07 09:28:41 Batch  14599_34126 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 12/20/07 09:25:27 Batch  14599_33930 INIT bckcett testIDS30 dsadm bls for sa
^1_1 12/17/07 19:49:17 Batch  14596_71364 PROMOTE bckcett testIDS30 u03651 Steph for Sharon
^1_1 12/17/07 19:43:25 Batch  14596_71010 INIT bckcett devlIDS30 u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   HlthCoachSumPkey
PROCESSING:    Used in NewDirHlthCoachSumExtr

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
------------------- ---------------------------- --------------------------------------------------------------------  -----------------  ---------------------------- ---------------------- ----------------
Bhoomi Dasari      12/07/2007                   Initial program                                                        3036               devlIDS30                   
Kalyan Neelam      2010-07-01                   Took out IDS_SK and Added HLTH_COACH_SK to get the PK SK.              4487               RebuildIntNewDevl
                                                Assigned job parm CurrRunCycle to the CRT_RUN_CYC_EXCTN_SK in the updt link

Annotation:
This container is used in:
Evicore,NDBH and Telligen Appeals
......Extr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

# COMMAND ----------
def run_AplMcasPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic contained within the shared container AplMcasPkey.

    Parameters
    ----------
    df_Transform : pyspark.sql.DataFrame
        Input stream corresponding to the DataStage link "Transform".
    params : dict
        Dictionary of runtime parameters already supplied by the calling job.

    Returns
    -------
    pyspark.sql.DataFrame
        Output stream corresponding to the DataStage link "Key".
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Read dummy table replacing hashed file hf_cust_apl
    # ------------------------------------------------------------------
    dummy_table_name = "dummy_hf_cust_apl"

    extract_query = f"""
        SELECT
            SRC_SYS_CD               AS SRC_SYS_CD_lkp,
            APL_ID                   AS APL_ID_lkp,
            CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
            APL_SK                   AS APL_SK_lkp
        FROM {IDSOwner}.{dummy_table_name}
    """

    df_hf_cust_apl = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # ------------------------------------------------------------------
    # Join input stream with lookup data
    # ------------------------------------------------------------------
    join_expr = (
        (col("tr.SRC_SYS_CD") == col("lk.SRC_SYS_CD_lkp")) &
        (col("tr.APL_ID")     == col("lk.APL_ID_lkp"))
    )

    df_join = (
        df_Transform.alias("tr")
        .join(df_hf_cust_apl.alias("lk"), join_expr, "left")
    )

    # ------------------------------------------------------------------
    # Derive transformation columns SK and NewCrtRunCycExtcnSk
    # ------------------------------------------------------------------
    df_enriched = (
        df_join
        .withColumn("SK", col("APL_SK_lkp"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("APL_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # Surrogate key generation (mandatory call signature)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Prepare DataFrame for updating dummy table (formerly hashed-file updt link)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("APL_SK_lkp").isNull())
        .select(
            col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("APL_ID").alias("APL_ID"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("APL_SK")
        )
    )

    (
        df_updt.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.{dummy_table_name}")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Compose final Key link output
    # ------------------------------------------------------------------
    df_Key = (
        df_enriched
        .select(
            col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("DISCARD_IN").alias("DISCARD_IN"),
            col("PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("ERR_CT").alias("ERR_CT"),
            col("RECYCLE_CT").alias("RECYCLE_CT"),
            col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("SK").alias("APL_SK"),
            col("APL_ID").alias("APL_ID"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("CRT_USER_SK").alias("CRT_USER_SK"),
            col("CUR_PRI_USER_SK").alias("CUR_PRI_USER_SK"),
            col("CUR_SEC_USER_SK").alias("CUR_SEC_USER_SK"),
            col("CUR_TRTY_USER_SK").alias("CUR_TRTY_USER_SK"),
            col("GRP_SK").alias("GRP_SK"),
            col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
            col("MBR_SK").alias("MBR_SK"),
            col("PROD_SK").alias("PROD_SK"),
            col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
            col("SUBGRP_SK").alias("SUBGRP_SK"),
            col("SUB_SK").alias("SUB_SK"),
            col("APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
            col("APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
            col("APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
            col("APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
            col("APL_MBR_HOME_ADDR_ST_CD_SK").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
            col("APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
            col("APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
            col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
            col("CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
            col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
            col("CRT_DTM").alias("CRT_DTM"),
            col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
            col("END_DT_SK").alias("END_DT_SK"),
            col("INITN_DT_SK").alias("INITN_DT_SK"),
            col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
            col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
            col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
            col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
            col("APL_DESC").alias("APL_DESC"),
            col("APL_SUM_DESC").alias("APL_SUM_DESC")
        )
    )

    return df_Key
# COMMAND ----------