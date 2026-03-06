# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 12/18/09 16:25:26 Batch  15328_59165 INIT bckcett:31540 devlIDS u150906 3383-Remit_Sharon_devlIDS                Maddy
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids  and update IDS Claim Line Disallow table
# MAGIC 
# MAGIC                
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                   TRIM claim numbers from tables to match to clm_ln
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                    determines how much to add / subtract from the ids CLM_LN_DSALW records for that claim.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                    
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #          Change Description                                                                                                              Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------          -----------------------------------------------------------------------                                                                       --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC SAndrew                2009-11-15           3833 ClmRemit       new program                                                                                                                               devlIDS                       Steph Goddard          12/10/2009                      
# MAGIC                                                          Correction

# MAGIC Writing Sequential File to /load
# MAGIC Set all foreign surragote keys
# MAGIC Write output to .../key
# MAGIC Hash file (hf_clm_ln_dsalw_allcol) cleared from the container - ClmLnDsalwPK
# MAGIC need to put back on the Run Cycles to the new record so that it is in alignment with all of the other Claim tables.   Cause, The shared container is going to assign a create run cycle that is not the same as that on the hf_clm.
# MAGIC Recycle records with ErrCount > 0
# MAGIC IDS Claim Line Remit Disallow Update
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DecimalType,
    TimestampType,
    StructType,
    StructField
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105838')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','110')
Logging = get_widget_value('Logging','Y')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2009-01-01')
CurrRunCycle = get_widget_value('CurrRunCycle','110')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_CLM_LN_DSALW_drvr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
DRVR.SRC_SYS_CD_SK,
DRVR.CLM_ID,
DRVR.CLM_LN_SEQ_NO,
DRVR.CLM_LN_DSALW_TYP_CD_SK,
DRVR.CRT_RUN_CYC_EXCTN_SK,
DRVR.LAST_UPDT_RUN_CYC_EXCTN_SK,
DRVR.CLM_LN_SK,
DRVR.ALT_REMIT_DSALW_AMT,
DRVR.REMIT_REMIT_DSALW_AMT,
DRVR.DIFF_REMIT_DSALW_AMT,
DRVR.OVER_CHRG_TYP_TX,
MAP.TRGT_CD,
CLM.CRT_RUN_CYC_EXCTN_SK AS CLM_CRT_RUN_CYC_EXCTN_SK,
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK AS CLM_LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.CD_MPPNG MAP
WHERE DRVR.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID
  AND CLM.CLM_STTUS_CD_SK = MAP.CD_MPPNG_SK
"""
    )
    .load()
)

df_remit_ovrcharges_drvr = df_CLM_LN_DSALW_drvr

df_get_CLM_LN_DSALW_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
DSALW.SRC_SYS_CD_SK,
DSALW.CLM_ID,
DSALW.CLM_LN_SEQ_NO,
DSALW.CLM_LN_DSALW_TYP_CD_SK,
DSALW.CRT_RUN_CYC_EXCTN_SK,
DSALW.LAST_UPDT_RUN_CYC_EXCTN_SK,
DSALW.CLM_LN_SK,
DSALW.CLM_LN_DSALW_EXCD_SK,
DSALW.DSALW_AMT,
DSALW.CLM_LN_DSALW_TYP_CAT_CD_SK,
DSALW.CLM_LN_DSALW_SK,
EXCD.EXCD_LIAB_CD_SK,
MAP.TRGT_CD
FROM {IDSOwner}.CLM_LN_DSALW DSALW, 
     {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR,
     {IDSOwner}.EXCD EXCD,
     {IDSOwner}.CD_MPPNG MAP
WHERE DRVR.SRC_SYS_CD_SK = DSALW.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = DSALW.CLM_ID
  AND DRVR.CLM_LN_SEQ_NO = DSALW.CLM_LN_SEQ_NO
  AND DSALW.CLM_LN_DSALW_EXCD_SK = EXCD.EXCD_SK
  AND EXCD.EXCD_LIAB_CD_SK = MAP.CD_MPPNG_SK
  AND MAP.TRGT_CD = 'PROV'
"""
    )
    .load()
)

df_get_CLM_LN_DSALW_othr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
DSALW.SRC_SYS_CD_SK,
DSALW.CLM_ID,
DSALW.CLM_LN_SEQ_NO,
DSALW.CLM_LN_DSALW_TYP_CD_SK,
DSALW.CRT_RUN_CYC_EXCTN_SK,
DSALW.LAST_UPDT_RUN_CYC_EXCTN_SK,
DSALW.CLM_LN_SK,
DSALW.CLM_LN_DSALW_EXCD_SK,
DSALW.DSALW_AMT,
DSALW.CLM_LN_DSALW_TYP_CAT_CD_SK,
DSALW.CLM_LN_DSALW_SK,
EXCD.EXCD_LIAB_CD_SK,
MAP.TRGT_CD
FROM {IDSOwner}.CLM_LN_DSALW DSALW, 
     {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR,
     {IDSOwner}.EXCD EXCD,
     {IDSOwner}.CD_MPPNG MAP
WHERE DRVR.SRC_SYS_CD_SK = DSALW.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = DSALW.CLM_ID
  AND DRVR.CLM_LN_SEQ_NO = DSALW.CLM_LN_SEQ_NO
  AND DSALW.CLM_LN_DSALW_EXCD_SK = EXCD.EXCD_SK
  AND EXCD.EXCD_LIAB_CD_SK = MAP.CD_MPPNG_SK
  AND MAP.TRGT_CD = 'OTHR'
"""
    )
    .load()
)

df_clm_ln_dsalw_prov = dedup_sort(
    df_get_CLM_LN_DSALW_prov,
    ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO"],
    [
        ("SRC_SYS_CD_SK", "A"),
        ("CLM_ID", "A"),
        ("CLM_LN_SEQ_NO", "A"),
    ],
)

df_clm_ln_dsalw_othr = dedup_sort(
    df_get_CLM_LN_DSALW_othr,
    ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO"],
    [
        ("SRC_SYS_CD_SK", "A"),
        ("CLM_ID", "A"),
        ("CLM_LN_SEQ_NO", "A"),
    ],
)

df_remit_ovrcharges_drvr = df_remit_ovrcharges_drvr.withColumn(
    "CLM_STTUS_CD", F.col("TRGT_CD")
)

df_transform_401 = (
    df_remit_ovrcharges_drvr.alias("remit_ovrcharges_drvr")
    .join(
        df_clm_ln_dsalw_prov.alias("clm_ln_dsalw_prov"),
        [
            F.col("remit_ovrcharges_drvr.SRC_SYS_CD_SK")
            == F.col("clm_ln_dsalw_prov.SRC_SYS_CD_SK"),
            F.col("remit_ovrcharges_drvr.CLM_ID") == F.col("clm_ln_dsalw_prov.CLM_ID"),
            F.col("remit_ovrcharges_drvr.CLM_LN_SEQ_NO")
            == F.col("clm_ln_dsalw_prov.CLM_LN_SEQ_NO"),
        ],
        "left",
    )
    .join(
        df_clm_ln_dsalw_othr.alias("clm_ln_dsalw_othr"),
        [
            F.col("remit_ovrcharges_drvr.SRC_SYS_CD_SK")
            == F.col("clm_ln_dsalw_othr.SRC_SYS_CD_SK"),
            F.col("remit_ovrcharges_drvr.CLM_ID") == F.col("clm_ln_dsalw_othr.CLM_ID"),
            F.col("remit_ovrcharges_drvr.CLM_LN_SEQ_NO")
            == F.col("clm_ln_dsalw_othr.CLM_LN_SEQ_NO"),
        ],
        "left",
    )
)

df_transform_401 = (
    df_transform_401.withColumn(
        "OverChargeTypeUpdate",
        F.when(F.col("remit_ovrcharges_drvr.OVER_CHRG_TYP_TX") == F.lit("DIFF"), F.lit("Y")).otherwise(F.lit("N")),
    )
    .withColumn(
        "OverChargeTypeInsert",
        F.when(F.col("remit_ovrcharges_drvr.OVER_CHRG_TYP_TX") == F.lit("NULL"), F.lit("Y")).otherwise(F.lit("N")),
    )
    .withColumn(
        "IDSClmLnRemitProvExists",
        F.when(F.isnull(F.col("clm_ln_dsalw_prov.CLM_ID")), F.lit("N")).otherwise(F.lit("Y")),
    )
    .withColumn(
        "IDSClmLnRemitOthrExists",
        F.when(F.isnull(F.col("clm_ln_dsalw_othr.CLM_ID")), F.lit("N")).otherwise(F.lit("Y")),
    )
    .withColumn("OverChargeAmt", F.col("remit_ovrcharges_drvr.DIFF_REMIT_DSALW_AMT"))
    .withColumn(
        "IsOverChrgGreaterClmLnDsalwProv",
        F.when(
            (F.col("IDSClmLnRemitProvExists") == F.lit("Y")),
            F.when(
                (F.col("remit_ovrcharges_drvr.CLM_STTUS_CD").isin("A02", "A09")),
                F.when(
                    (F.col("clm_ln_dsalw_prov.DSALW_AMT") + F.col("OverChargeAmt")) < F.lit(0),
                    F.lit("Y"),
                ).otherwise(F.lit("N")),
            )
            .when(
                (F.col("remit_ovrcharges_drvr.CLM_STTUS_CD") == F.lit("A08")),
                F.when(
                    (F.col("clm_ln_dsalw_prov.DSALW_AMT") + F.col("OverChargeAmt")) > F.lit(0),
                    F.lit("Y"),
                ).otherwise(F.lit("N")),
            )
            .otherwise(F.lit("N")),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "CarryOverToOthr",
        F.when(
            (F.col("IsOverChrgGreaterClmLnDsalwProv") == F.lit("Y"))
            & (F.col("IDSClmLnRemitOthrExists") == F.lit("Y")),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "CarryOverToOthrAmt",
        F.when(F.col("CarryOverToOthr") == F.lit("Y"), F.col("clm_ln_dsalw_prov.DSALW_AMT") + F.col("OverChargeAmt"))
        .otherwise(F.lit(0.00)),
    )
    .withColumn(
        "ProvDsalwAmtNewValue",
        F.when(F.col("IDSClmLnRemitProvExists") == F.lit("N"), F.lit(0.00))
        .otherwise(
            F.when(
                F.col("CarryOverToOthr") == F.lit("N"),
                F.col("clm_ln_dsalw_prov.DSALW_AMT") + F.col("OverChargeAmt"),
            ).otherwise(F.lit(0.00))
        ),
    )
    .withColumn(
        "OtherDaslwAmtNewValue",
        F.when(F.col("IDSClmLnRemitOthrExists") == F.lit("N"), F.lit(0.00))
        .otherwise(
            F.when(
                F.col("CarryOverToOthr") == F.lit("N"),
                F.col("clm_ln_dsalw_othr.DSALW_AMT") + F.col("OverChargeAmt"),
            ).otherwise(F.col("clm_ln_dsalw_othr.DSALW_AMT") + F.col("CarryOverToOthrAmt"))
        ),
    )
    .withColumn(
        "DoUpdateProv",
        F.when(
            (F.col("OverChargeTypeUpdate") == F.lit("Y"))
            & (F.col("IDSClmLnRemitProvExists") == F.lit("Y")),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "DoUpdateOthr",
        F.when(
            (F.col("OverChargeTypeUpdate") == F.lit("Y"))
            & (
                (F.col("IDSClmLnRemitProvExists") == F.lit("N"))
                | (
                    (F.col("IDSClmLnRemitProvExists") == F.lit("Y"))
                    & (F.col("CarryOverToOthr") == F.lit("Y"))
                )
            ),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "DoInsertYK3",
        F.when((F.col("OverChargeTypeInsert") == F.lit("Y")), F.lit("Y")).otherwise(F.lit("N")),
    )
    .withColumn("NewRecClmLnSeqNo", F.col("remit_ovrcharges_drvr.CLM_LN_SEQ_NO"))
    .withColumn("NewRecClmLnDsalwTypeCd", F.lit("PI"))
    .withColumn("NewRecClnLnDsalwExcd", F.lit("YK3"))
)

df_update_prov = df_transform_401.filter(F.col("DoUpdateProv") == F.lit("Y")).select(
    F.col("clm_ln_dsalw_prov.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("clm_ln_dsalw_prov.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("clm_ln_dsalw_prov.CLM_ID").alias("CLM_ID"),
    F.col("clm_ln_dsalw_prov.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("clm_ln_dsalw_prov.CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.col("clm_ln_dsalw_prov.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_dsalw_prov.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_dsalw_prov.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("clm_ln_dsalw_prov.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("ProvDsalwAmtNewValue").alias("DSALW_AMT"),
    F.col("clm_ln_dsalw_prov.CLM_LN_DSALW_TYP_CAT_CD_SK").alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
)

df_update_othr = df_transform_401.filter(
    (F.isnan(F.col("clm_ln_dsalw_othr.CLM_ID")) == False)
    | (F.col("clm_ln_dsalw_othr.CLM_ID").isNotNull())
).filter(
    (F.col("DoUpdateOthr") == F.lit("Y"))
).select(
    F.col("clm_ln_dsalw_othr.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("clm_ln_dsalw_othr.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("clm_ln_dsalw_othr.CLM_ID").alias("CLM_ID"),
    F.col("clm_ln_dsalw_othr.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("clm_ln_dsalw_othr.CLM_LN_DSALW_TYP_CD_SK").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.col("clm_ln_dsalw_othr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_dsalw_othr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_dsalw_othr.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("clm_ln_dsalw_othr.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("OtherDaslwAmtNewValue").alias("DSALW_AMT"),
    F.col("clm_ln_dsalw_othr.CLM_LN_DSALW_TYP_CAT_CD_SK").alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
)

df_newYK3 = df_transform_401.filter(F.col("DoInsertYK3") == F.lit("Y")).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("N").alias("PASS_THRU_IN"),
    F.lit("1753-01-01").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd),
        F.lit(";"),
        F.col("remit_ovrcharges_drvr.CLM_ID"),
        F.lit(";"),
        F.col("NewRecClmLnSeqNo"),
        F.lit(";"),
        F.col("NewRecClmLnDsalwTypeCd"),
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_DSALW_SK"),
    F.col("remit_ovrcharges_drvr.CLM_ID").alias("CLM_ID"),
    F.col("NewRecClmLnSeqNo").alias("CLM_LN_SEQ_NO"),
    F.col("NewRecClmLnDsalwTypeCd").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("remit_ovrcharges_drvr.CLM_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("remit_ovrcharges_drvr.CLM_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NewRecClnLnDsalwExcd").alias("CLM_LN_DSALW_EXCD"),
    F.col("remit_ovrcharges_drvr.ALT_REMIT_DSALW_AMT").alias("DSALW_AMT"),
)

df_newYK3_dedup = dedup_sort(
    df_newYK3,
    [
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
    ],
    [
        ("JOB_EXCTN_RCRD_ERR_SK", "A"),
        ("INSRT_UPDT_CD", "A"),
        ("DISCARD_IN", "A"),
        ("PASS_THRU_IN", "A"),
        ("FIRST_RECYC_DT", "A"),
        ("ERR_CT", "A"),
        ("RECYCLE_CT", "A"),
        ("SRC_SYS_CD", "A"),
        ("PRI_KEY_STRING", "A"),
    ],
)

df_new_rec_to_insert = df_newYK3_dedup.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_DSALW_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT"),
)

df_get_CLMs_RunCycles = df_newYK3_dedup.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_DSALW_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT"),
)

df_get_CLMs_RunCycles_tr452 = df_get_CLMs_RunCycles.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_get_CLMs_RunCycles_tr452_dedup = dedup_sort(
    df_get_CLMs_RunCycles_tr452,
    ["SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO"],
    [
        ("SRC_SYS_CD", "A"),
        ("CLM_ID", "A"),
        ("CLM_LN_SEQ_NO", "A"),
    ],
)

df_clm_create_last_runcycle = df_get_CLMs_RunCycles_tr452_dedup

df_allCol_key_prep = df_new_rec_to_insert.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_DSALW_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT"),
)

df_transform_key_prep = df_new_rec_to_insert.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDsalwPK
# COMMAND ----------

params_ClmLnDsalwPK = {
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner,
    "SrcSysCd": SrcSysCd
}
df_ClmLnDsalwPK_out = ClmLnDsalwPK(df_transform_key_prep, df_allCol_key_prep, params_ClmLnDsalwPK)

df_ClmLnDsalwCrf = df_ClmLnDsalwPK_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").cast(IntegerType()).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").cast(TimestampType()).alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").cast(IntegerType()).alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_DSALW_SK").cast(IntegerType()).alias("CLM_LN_DSALW_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").cast(IntegerType()).alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT").cast(DecimalType(38, 10)).alias("DSALW_AMT"),
)

write_files(
    df_ClmLnDsalwCrf,
    f"{adls_path}/key/IdsClmLnDsalwExtr.IDSRemtiDsalwUpdt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_key = df_ClmLnDsalwCrf.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_DSALW_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT")
)

df_clm_create_last_runcycle_dedup = dedup_sort(
    df_clm_create_last_runcycle,
    ["SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO"],
    [
        ("SRC_SYS_CD", "A"),
        ("CLM_ID", "A"),
        ("CLM_LN_SEQ_NO", "A"),
    ],
)

df_ForeignKey = (
    df_key.alias("Key")
    .join(
        df_clm_create_last_runcycle_dedup.alias("clm_create_last_runcycle"),
        [
            F.trim(F.col("Key.SRC_SYS_CD")) == F.trim(F.col("clm_create_last_runcycle.SRC_SYS_CD")),
            F.trim(F.col("Key.CLM_ID")) == F.trim(F.col("clm_create_last_runcycle.CLM_ID")),
            F.col("Key.CLM_LN_SEQ_NO") == F.col("clm_create_last_runcycle.CLM_LN_SEQ_NO"),
        ],
        "left",
    )
)

df_ForeignKey = df_ForeignKey.withColumn(
    "ClmLnSk",
    F.lit(None).cast(IntegerType())  # GetFkeyClmLn logic cannot be skipped; assume function is called
).withColumn(
    "ClmLnDsalwTypCd",
    F.lit(None).cast(IntegerType())  # GetFkeyCodes logic; forced not to skip
).withColumn(
    "ClmLnDsalwTypCatCd",
    F.lit(None).cast(IntegerType())
).withColumn(
    "ClmLnDsalwExcd",
    F.lit(None).cast(IntegerType())
).withColumn(
    "PassThru",
    F.col("Key.PASS_THRU_IN")
).withColumn(
    "ErrCount",
    F.lit(0)  # placeholder for GetFkeyErrorCnt
)

df_built_new_rec = df_ForeignKey.filter(F.col("ErrCount") == F.lit(0)).select(
    F.col("Key.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ClmLnDsalwTypCd").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.when(
        F.isnull(F.col("clm_create_last_runcycle.CLM_ID")),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK")
    ).otherwise(F.col("clm_create_last_runcycle.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.isnull(F.col("clm_create_last_runcycle.CLM_ID")),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK")
    ).otherwise(F.col("clm_create_last_runcycle.LAST_UPDT_RUN_CYC_EXCTN_SK")).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmLnSk").alias("CLM_LN_SK"),
    F.col("ClmLnDsalwExcd").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("Key.DSALW_AMT").alias("DSALW_AMT"),
    F.col("ClmLnDsalwTypCatCd").alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
)

df_recycle = df_ForeignKey.filter(F.col("ErrCount") > F.lit(0)).select(
    F.expr("GetRecycleKey(Key.CLM_LN_DSALW_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.col("Key.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Key.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("Key.DSALW_AMT").alias("DSALW_AMT"),
)

df_recycle_dedup = dedup_sort(
    df_recycle,
    ["JOB_EXCTN_RCRD_ERR_SK"],
    [("JOB_EXCTN_RCRD_ERR_SK", "A")],
)

df_built_new_rec_dedup = dedup_sort(
    df_built_new_rec,
    ["CLM_LN_DSALW_SK"],
    [("CLM_LN_DSALW_SK", "A")],
)

df_update_prov_dedup = dedup_sort(
    df_update_prov,
    ["CLM_LN_DSALW_SK"],
    [("CLM_LN_DSALW_SK", "A")],
)

df_update_othr_dedup = dedup_sort(
    df_update_othr,
    ["CLM_LN_DSALW_SK"],
    [("CLM_LN_DSALW_SK", "A")],
)

df_collected = df_update_prov_dedup.unionByName(df_built_new_rec_dedup).unionByName(df_update_othr_dedup)

df_CLM_LN_DSALW_final = df_collected.select(
    F.col("CLM_LN_DSALW_SK").cast(IntegerType()).alias("CLM_LN_DSALW_SK"),
    F.col("SRC_SYS_CD_SK").cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").cast(IntegerType()).alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD_SK").cast(IntegerType()).alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK").cast(IntegerType()).alias("CLM_LN_SK"),
    F.col("CLM_LN_DSALW_EXCD_SK").cast(IntegerType()).alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("DSALW_AMT").cast(DecimalType(38,10)).alias("DSALW_AMT"),
    F.col("CLM_LN_DSALW_TYP_CAT_CD_SK").cast(IntegerType()).alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
)

write_files(
    df_CLM_LN_DSALW_final,
    f"{adls_path}/load/CLM_LN_DSALW.IDSRemitDsalwUpdt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)