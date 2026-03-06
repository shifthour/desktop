# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
Shared Container: UserGrpPartcpnPkey
* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_2 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_2 03/21/07 14:59:13 Batch  14325_53956 INIT bckcetl ids20 dsadm dsadm
^1_1 03/07/07 15:57:28 Batch  14311_57454 INIT bckcetl ids20 dsadm dsadm
^1_1 10/24/05 15:34:29 Batch  13812_56076 PROMOTE bckcetl ids20 dsadm GIna Parr
^1_1 10/24/05 15:29:11 Batch  13812_55756 INIT bckcett testIDS30 dsadm Gina Parr
^1_1 10/18/05 08:21:55 Batch  13806_30120 PROMOTE bckcett testIDS30 u03651 steffy
^1_1 10/18/05 08:16:03 Batch  13806_29765 INIT bckcett devlIDS30 u03651 steffy

Annotation:
This container is used in:
FctsEmpUserGrpPartcpnExtr
NpsEmpUserGrpPartcpnExtr

These programs need to be re-compiled when logic changes
"""

def run_UserGrpPartcpnPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    CurrRunCycle      = params["CurrRunCycle"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    extract_query = """
        SELECT
            GRP_USER_ID        AS GRP_USER_ID_lkp,
            INDV_USER_ID       AS INDV_USER_ID_lkp,
            SRC_SYS_CD         AS SRC_SYS_CD_lkp,
            CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK_lkp,
            USER_GRP_PARTCPN_SK AS USER_GRP_PARTCPN_SK_lkp
        FROM dummy_hf_usr_grp_partcpn
    """

    df_hf_usr_grp_partcpn_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    df_joined = (
        df_Transform.alias("Transform")
        .join(
            df_hf_usr_grp_partcpn_lkup.alias("lkup"),
            (F.col("Transform.USGR_USUS_ID") == F.col("lkup.GRP_USER_ID_lkp")) &
            (F.col("Transform.USUS_ID")      == F.col("lkup.INDV_USER_ID_lkp")) &
            (F.col("Transform.SRC_SYS_CD")   == F.col("lkup.SRC_SYS_CD_lkp")),
            "left"
        )
    )

    df_enriched = (
        df_joined
        .withColumn("SK", F.col("lkup.USER_GRP_PARTCPN_SK_lkp"))
        .withColumn(
            "CrtRunCcyExctnSK",
            F.when(
                F.col("lkup.USER_GRP_PARTCPN_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("LastUpdtRunCycExctnSK", F.lit(CurrRunCycle))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    df_updt = (
        df_enriched
        .filter(F.col("lkup.USER_GRP_PARTCPN_SK_lkp").isNull())
        .select(
            F.col("USGR_USUS_ID").alias("GRP_USER_ID"),
            F.col("USUS_ID").alias("INDV_USER_ID"),
            F.col("SRC_SYS_CD"),
            F.col("CrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("USER_GRP_PARTCPN_SK")
        )
    )

    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", "dummy_hf_usr_grp_partcpn")
        .mode("append")
        .save()
    )

    df_output = (
        df_enriched
        .select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            F.col("SK").alias("USER_GRP_PARTCPN_SK"),
            "USGR_USUS_ID",
            F.col("CrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LastUpdtRunCycExctnSK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "USUS_ID",
            "USGU_DESC"
        )
    )

    return df_output