
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
SubPcaPkey – Shared container for primary keying of SUB_PCA

* VC LOGS *
^1_1 09/30/08 12:58:03 Batch  14884_46688 PROMOTE bckcetl ids20 dsadm bls for hs
^1_1 09/30/08 12:56:15 Batch  14884_46578 INIT bckcett testIDS dsadm bls for hs
^1_1 09/30/08 07:44:46 Batch  14884_27893 PROMOTE bckcett testIDS u03651 steph for Hugh
^1_1 09/30/08 07:27:40 Batch  14884_26863 INIT bckcett devlIDSnew u03651 steffy
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_2 12/26/07 15:33:58 Batch  14605_56043 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_2 11/23/07 15:42:37 Batch  14572_56566 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_2 03/29/07 15:10:50 Batch  14333_54658 PROMOTE bckcetl ids20 dsadm BLS for h sisson
^1_2 03/29/07 14:57:46 Batch  14333_53870 INIT bckcett testIDS30 dsadm bls for h sisson
^1_10 03/06/07 19:41:19 Batch  14310_70881 PROMOTE bckcett testIDS30 u10157 sa
^1_10 03/06/07 19:18:09 Batch  14310_69492 PROMOTE bckcett devlIDS30 u10157 sa
^1_10 02/02/07 09:11:17 Batch  14278_33085 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
^1_10 02/02/07 09:06:50 Batch  14278_32880 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_9 01/29/07 14:16:26 Batch  14274_51398 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_8 01/29/07 14:09:25 Batch  14274_50971 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_7 01/25/07 15:43:59 Batch  14270_56676 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_6 01/17/07 10:04:53 Batch  14262_36297 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_5 01/16/07 14:30:51 Batch  14261_52255 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_4 01/16/07 14:03:58 Batch  14261_50642 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_3 01/16/07 14:01:30 Batch  14261_50494 INIT bckcett devlIDS30 u11141 Hugh Sisson
^1_2 11/17/06 17:15:46 Batch  14201_62175 INIT bckcett devlIDS30 u11141 Hugh Sisson - GRGR_ID fix
^1_1 11/17/06 11:12:19 Batch  14201_40344 INIT bckcett devlIDS30 u06640 Ralph

**********************************************************************************************************************************************
 Copyright 2006, 2008 Blue Cross and Blue Shield of Kansas City

Called by:
                    IdsSubPcaFkey

Processing:
                    Shared container for primary keying of SUB_PCA

Control Job Rerun Information: 
                    Previous Run Successful:    Restart, no other steps necessary
                    Previous Run Aborted:         Restart, no other steps necessary

Modifications:                        
                                              Project/                                                                                                 Code                  Date
Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
Hugh Sisson    2008-09-24    TTR-374        Added 2 new columns, including 1 in the natural key     Steph Goddard   09/29/2008

This container is used in:
FctsSubPcaExtr
InitalLoadFctsSubPcaExtr
These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

def run_SubPcaPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    CurrRunCycle       = params["CurrRunCycle"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    jdbc_url           = params["jdbc_url"]
    jdbc_props         = params["jdbc_props"]

    extract_query = f"""
        SELECT
            SRC_SYS_CD            AS SRC_SYS_CD_lkp,
            SUB_UNIQ_KEY          AS SUB_UNIQ_KEY_lkp,
            SUB_PCA_ACCUM_PFX_ID  AS SUB_PCA_ACCUM_PFX_ID_lkp,
            PLN_YR_BEG_DT         AS PLN_YR_BEG_DT_lkp,
            EFF_DT                AS EFF_DT_lkp,
            CRT_RUN_CYC_EXCTN_SK  AS CRT_RUN_CYC_EXCTN_SK_lkp,
            SUB_PCA_SK            AS SUB_PCA_SK_lkp
        FROM {IDSOwner}.dummy_hf_sub_pca
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    join_expr = (
        (col("Transform.SRC_SYS_CD")           == col("SRC_SYS_CD_lkp")) &
        (col("Transform.SUB_UNIQ_KEY")         == col("SUB_UNIQ_KEY_lkp")) &
        (col("Transform.SUB_PCA_ACCUM_PFX_ID") == col("SUB_PCA_ACCUM_PFX_ID_lkp")) &
        (col("Transform.PLN_YR_BEG_DT")        == col("PLN_YR_BEG_DT_lkp")) &
        (col("Transform.EFF_DT")               == col("EFF_DT_lkp"))
    )

    df_enriched = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_expr, "left")
        .withColumn("SK", col("SUB_PCA_SK_lkp"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("SUB_PCA_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    df_updt = (
        df_enriched
        .filter(col("SUB_PCA_SK_lkp").isNull())
        .select(
            col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
            col("Transform.SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
            col("Transform.PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
            col("Transform.EFF_DT").alias("EFF_DT"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("SUB_PCA_SK")
        )
    )

    (
        df_updt.write.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("dbtable", f"{IDSOwner}.dummy_hf_sub_pca")
            .mode("append")
            .save()
    )

    df_Key = df_enriched.select(
        col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("Transform.ERR_CT").alias("ERR_CT"),
        col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("SK").alias("SUB_PCA_SK"),
        col("Transform.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("Transform.SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
        col("Transform.PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
        col("Transform.EFF_DT").alias("EFF_DT"),
        col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Transform.GRP").alias("GRP"),
        col("Transform.SUB").alias("SUB"),
        col("Transform.SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
        col("Transform.TERM_DT").alias("TERM_DT"),
        col("Transform.ALLOC_AMT").alias("ALLOC_AMT"),
        col("Transform.BAL_AMT").alias("BAL_AMT"),
        col("Transform.CAROVR_AMT").alias("CAROVR_AMT"),
        col("Transform.MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
        col("Transform.PD_AMT").alias("PD_AMT")
    )

    return df_Key
