# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 4;hf_dm_mbr_life_evt_clm_stts_not;hf_dm_mbr_life_evt_mbr_proc;hf_dm_mbr_life_evt_mbr_svrc_dt;hf_dm_mbr_life_evt_mbr_svrc_max
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari        9/9/2009           4113                                                   New ETL                                                                                     devlIDSNew                   Steph Goddard          09/22/2009
# MAGIC Sandrew                 2009-10-27        4113 Mbr 360                           re-wrote with production volume considerations and stripped                devlIDSNew                   Steph Goddard          10/28/2009
# MAGIC                                                                                                         cdma lookups to hash files when possible and not in join criteria
# MAGIC 
# MAGIC 
# MAGIC Shiva Devagiri           07/22/2013        5114                            Create Load File for EDW Table MBRSHP_DM_MBR_LIF_EVT             EnterpriseWrhseDevl           Peter Marshall            10/21/2013

# MAGIC Read from source tables CLM AND CLM_LN AND PROC_CD AND MBR AND SUB AND CD_MPPNG from IDS.
# MAGIC Job Name: IdsDmMbrshDmMbrLifEvntExtr
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD,
# MAGIC MBR_RELSHP_CD_SK,
# MAGIC CLM_STTUS_CD_SK,
# MAGIC CLM_SUBTYP_CD_SK,
# MAGIC CLM_TYP_CD_SK,
# MAGIC SUB_UNIQ_KEY
# MAGIC Add Defaults and Null Handling.
# MAGIC Write MBRSHP_DM_MBR_LIF_EVT Data into a Sequential file for Load Job IdsDmMbrshpDmMbrLifEvtLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrRunCycle = get_widget_value("CurrRunCycle","")

# ----------------------------------------------------------------------------------------------------
# db2_MBRSHP_DM_MBR_LIF_EVT_in
# ----------------------------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_MBRSHP_DM_MBR_LIF_EVT_in = f"""
SELECT 
MBR.SRC_SYS_CD_SK,
COALESCE(MAP.TRGT_CD, 'UNK') SRC_SYS_CD,
MBR.MBR_UNIQ_KEY,
SUB.SUB_UNIQ_KEY,
PROC.ALT_KEY_WORD_2,
CLM.CLM_ID,
LN.SVC_STRT_DT_SK,
CLM.CLM_STTUS_CD_SK,
MBR.MBR_RELSHP_CD_SK,
CLM.CLM_SUBTYP_CD_SK,
CLM.CLM_TYP_CD_SK
FROM {IDSOwner}.CLM CLM
INNER JOIN {IDSOwner}.CLM_LN LN ON CLM.CLM_SK = LN.CLM_SK
INNER JOIN {IDSOwner}.MBR MBR ON CLM.MBR_SK = MBR.MBR_SK
INNER JOIN {IDSOwner}.PROC_CD PROC ON LN.PROC_CD_SK = PROC.PROC_CD_SK
INNER JOIN {IDSOwner}.SUB SUB ON MBR.SUB_SK = SUB.SUB_SK
INNER JOIN {IDSOwner}.CD_MPPNG MAP ON LN.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
AND MAP.TRGT_CD = 'FACETS'
AND LN.SVC_STRT_DT_SK >= '2009-01-01'
AND (PROC.ALT_KEY_WORD_2 = 'LIVE BIRTH' OR PROC.ALT_KEY_WORD_2 = 'LOST PREG')
AND MBR.MBR_UNIQ_KEY <> 0
AND MBR.MBR_UNIQ_KEY <> 1
"""
df_db2_MBRSHP_DM_MBR_LIF_EVT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBRSHP_DM_MBR_LIF_EVT_in)
    .load()
)
df_lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc = df_db2_MBRSHP_DM_MBR_LIF_EVT_in

# ----------------------------------------------------------------------------------------------------
# db2_CD_MAPPING_In
# ----------------------------------------------------------------------------------------------------
extract_query_db2_CD_MAPPING_In = f"""
SELECT 
CD_MPPNG_SK,
SRC_CD_SK,
TRGT_CD,
TRGT_CD_NM,
TRGT_DOMAIN_NM
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'CLAIM STATUS'
AND TRGT_CD_NM IN ('REVERSAL','ADJUSTED')
"""
df_db2_CD_MAPPING_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MAPPING_In)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# db2__In_SUB_In
# ----------------------------------------------------------------------------------------------------
extract_query_db2__In_SUB_In = f"""
SELECT 
SUB_UNIQ_KEY,
SUB_ID
FROM {IDSOwner}.SUB SUB
WHERE SUB_ID like 'PRXY%'
"""
df_db2__In_SUB_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2__In_SUB_In)
    .load()
)
df_Ref_SubUniqKey_lkp = df_db2__In_SUB_In

# ----------------------------------------------------------------------------------------------------
# db2_CD_MPPNG_In
# ----------------------------------------------------------------------------------------------------
extract_query_db2_CD_MPPNG_In_2 = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_In_2)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# Cpy_Cd_Mapping (PxCopy)
# ----------------------------------------------------------------------------------------------------
df_clm_typ_cd = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_mbr_relatnshp = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_clm_sub_typ_cd = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# ----------------------------------------------------------------------------------------------------
# lkp_Codes (PxLookup)
# ----------------------------------------------------------------------------------------------------
df_lkp_Codes_temp = (
    df_lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.alias("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc")
    .join(
        df_Ref_SubUniqKey_lkp.alias("Ref_SubUniqKey_lkp"),
        on=[
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.SUB_UNIQ_KEY")
            == F.col("Ref_SubUniqKey_lkp.SUB_UNIQ_KEY")
        ],
        how="left",
    )
    .join(
        df_clm_sub_typ_cd.alias("clm_sub_typ_cd"),
        on=[
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.CLM_SUBTYP_CD_SK")
            == F.col("clm_sub_typ_cd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_mbr_relatnshp.alias("mbr_relatnshp"),
        on=[
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.MBR_RELSHP_CD_SK")
            == F.col("mbr_relatnshp.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_clm_typ_cd.alias("clm_typ_cd"),
        on=[
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.CLM_TYP_CD_SK")
            == F.col("clm_typ_cd.CD_MPPNG_SK")
        ],
        how="left",
    )
    .join(
        df_db2_CD_MAPPING_In.alias("Ref_CdMpping_lkp"),
        on=[
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.CLM_STTUS_CD_SK")
            == F.col("Ref_CdMpping_lkp.CD_MPPNG_SK"),
            F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.SRC_SYS_CD_SK")
            == F.col("Ref_CdMpping_lkp.SRC_CD_SK"),
        ],
        how="left",
    )
)

df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.ALT_KEY_WORD_2").alias("ALT_KEY_WORD_2"),
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("lnk_IdsDmMbrshpDmMbrLifEvtExtr_InAbc.CLM_ID").alias("CLM_ID"),
    F.col("Ref_CdMpping_lkp.CD_MPPNG_SK").alias("TESTIF_RVRSAL_CLM_CDMPNG"),
    F.col("Ref_SubUniqKey_lkp.SUB_UNIQ_KEY").alias("TESTIF_PRXYCLM"),
    F.col("clm_typ_cd.TRGT_CD").alias("TESTIF_MEDICL_TGTCD"),
    F.col("mbr_relatnshp.TRGT_CD").alias("TESTIF_SUBSCRBR_TGTCD"),
    F.col("clm_sub_typ_cd.TRGT_CD").alias("TESTIF_PRCDRECLM"),
)

# ----------------------------------------------------------------------------------------------------
# xfm_BusinessLogic (CTransformerStage)
# ----------------------------------------------------------------------------------------------------
df_xfm_temp = (
    df_lkp_Codes
    .withColumn(
        "TestIfReversalClaim",
        F.when(F.col("TESTIF_RVRSAL_CLM_CDMPNG").isNull(), F.lit("N")).otherwise(F.lit("Y")),
    )
    .withColumn(
        "TestIfSubscriberOrSpouse",
        F.when(F.col("TESTIF_SUBSCRBR_TGTCD").isNull(), F.lit("N"))
        .otherwise(
            F.when(F.trim(F.col("TESTIF_SUBSCRBR_TGTCD")) == F.lit("SUB"), F.lit("Y"))
            .otherwise(
                F.when(F.trim(F.col("TESTIF_SUBSCRBR_TGTCD")) == F.lit("SPOUSE"), F.lit("Y"))
                .otherwise(F.lit("N"))
            )
        ),
    )
    .withColumn(
        "TestIfMedical",
        F.when(
            (F.col("TESTIF_MEDICL_TGTCD").isNotNull())
            & (F.trim(F.col("TESTIF_MEDICL_TGTCD")) == F.lit("MED")),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "TestIfProcedureClm",
        F.when(
            (F.col("TESTIF_PRCDRECLM").isNotNull())
            & (F.trim(F.col("TESTIF_PRCDRECLM")) == F.lit("PR")),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "TestIfProxyClaim",
        F.when(F.col("TESTIF_PRXYCLM").isNull(), F.lit("N")).otherwise(F.lit("Y")),
    )
)

df_xfm = df_xfm_temp.withColumn(
    "TestIfWriteOutClaim",
    F.when(
        (F.col("TestIfReversalClaim") == F.lit("N"))
        & (F.col("TestIfSubscriberOrSpouse") == F.lit("Y"))
        & (F.col("TestIfMedical") == F.lit("Y"))
        & (F.col("TestIfProcedureClm") == F.lit("Y"))
        & (F.col("TestIfProxyClaim") == F.lit("N")),
        F.lit("Y"),
    ).otherwise(F.lit("N")),
)

df_lnk_IdsDmMbrshpDmIndBeLtrExtr_OutAbc = df_xfm.filter(
    F.col("TestIfWriteOutClaim") == F.lit("Y")
).select(
    F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK"))
    .otherwise(F.trim(F.col("SRC_SYS_CD")))
    .alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
)

# ----------------------------------------------------------------------------------------------------
# Cpy_Clms (PxCopy)
# ----------------------------------------------------------------------------------------------------
df_all_claims = df_lnk_IdsDmMbrshpDmIndBeLtrExtr_OutAbc.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("PROC_ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
)

df_get_max_dt = df_lnk_IdsDmMbrshpDmIndBeLtrExtr_OutAbc.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("PROC_ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
)

# ----------------------------------------------------------------------------------------------------
# Aggregator_SrvcDt (PxAggregator)
# ----------------------------------------------------------------------------------------------------
df_Mbr_Last_Srv_Dt = (
    df_get_max_dt.groupBy(
        "SRC_SYS_CD", "MBR_UNIQ_KEY", "SUB_UNIQ_KEY", "PROC_ALT_KEY_WORD_2"
    )
    .agg(F.max("SVC_STRT_DT_SK").alias("MAX_SVC_STRT_DT"))
)

# ----------------------------------------------------------------------------------------------------
# Trnsf_Srv_Dt (CTransformerStage)
# ----------------------------------------------------------------------------------------------------
df_Trnsf_Srv_Dt_temp = df_Mbr_Last_Srv_Dt.filter(F.col("MBR_UNIQ_KEY").isNotNull())

df_Srv_Dt_Lkp = df_Trnsf_Srv_Dt_temp.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("PROC_ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("MAX_SVC_STRT_DT").alias("MAX_SVC_STRT_DT"),
)

# ----------------------------------------------------------------------------------------------------
# Lkp_Max_Srv_Dt (PxLookup)
# ----------------------------------------------------------------------------------------------------
df_Lkp_Max_Srv_Dt_temp = df_all_claims.alias("All_Claims").join(
    df_Srv_Dt_Lkp.alias("Srv_Dt_Lkp"),
    on=[
        F.col("All_Claims.SRC_SYS_CD") == F.col("Srv_Dt_Lkp.SRC_SYS_CD"),
        F.col("All_Claims.MBR_UNIQ_KEY") == F.col("Srv_Dt_Lkp.MBR_UNIQ_KEY"),
        F.col("All_Claims.SUB_UNIQ_KEY") == F.col("Srv_Dt_Lkp.SUB_UNIQ_KEY"),
        F.col("All_Claims.PROC_ALT_KEY_WORD_2") == F.col("Srv_Dt_Lkp.PROC_ALT_KEY_WORD_2"),
        F.col("All_Claims.SVC_STRT_DT_SK") == F.col("Srv_Dt_Lkp.MAX_SVC_STRT_DT"),
    ],
    how="left",
)

df_Get_Max_Clm = df_Lkp_Max_Srv_Dt_temp.select(
    F.col("All_Claims.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("All_Claims.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("All_Claims.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("All_Claims.PROC_ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("All_Claims.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("All_Claims.CLM_ID").alias("CLM_ID"),
)

# ----------------------------------------------------------------------------------------------------
# Aggregator_Clm (PxAggregator)
# ----------------------------------------------------------------------------------------------------
df_Mbr_Last_Clm_Id = (
    df_Get_Max_Clm.groupBy(
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "SUB_UNIQ_KEY",
        "PROC_ALT_KEY_WORD_2",
        "SVC_STRT_DT_SK",
    )
    .agg(F.max("CLM_ID").alias("MAX_CLM_ID"))
)

# ----------------------------------------------------------------------------------------------------
# Trnsf_Clm_Dt (CTransformerStage)
# ----------------------------------------------------------------------------------------------------
df_Trnsf_Clm_Dt_temp = (
    df_Mbr_Last_Clm_Id
    .withColumn(
        "svTypeCode",
        F.when(
            F.trim(F.col("PROC_ALT_KEY_WORD_2")) == F.lit("LIVE BIRTH"), F.lit("LIVEBRTH")
        ).otherwise(F.lit("LOSTPREGNCY")),
    )
    .withColumn(
        "svTypeName",
        F.when(
            F.trim(F.col("PROC_ALT_KEY_WORD_2")) == F.lit("LIVE BIRTH"), F.lit("LIVE BIRTH")
        ).otherwise(F.lit("LOST PREGNANCY")),
    )
)

df_lnk_IdsDmMbrshpDmMbrLifEvtExtr_OutAbc_final = df_Trnsf_Clm_Dt_temp.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svTypeCode").alias("MBR_LIFE_EVT_TYP_CD"),
    F.to_timestamp(F.col("SVC_STRT_DT_SK"), "yyyy-MM-dd").alias("CLM_SVC_STRT_DT"),
    F.col("MAX_CLM_ID").alias("CLM_ID"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("svTypeName").alias("MBR_LIFE_EVT_TYP_NM"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
)

# ----------------------------------------------------------------------------------------------------
# Seq_MBR_LIF_EVT_csv (PxSequentialFile)
# ----------------------------------------------------------------------------------------------------
write_files(
    df_lnk_IdsDmMbrshpDmMbrLifEvtExtr_OutAbc_final,
    f"{adls_path}/load/MBRSHP_DM_MBRLF_EVNT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)