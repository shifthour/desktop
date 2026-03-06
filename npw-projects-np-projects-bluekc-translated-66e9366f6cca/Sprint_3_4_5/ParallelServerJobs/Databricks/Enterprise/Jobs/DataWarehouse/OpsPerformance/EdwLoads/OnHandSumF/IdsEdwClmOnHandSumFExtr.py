# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     EdwClmInvtryExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extracts from IDS CLM_INVTRY table, groups by the 11-column natural key and counts rows.  Output rows are given an SK.
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Change CurrRunDate value, if necessary, and run
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    2008-01-02    3531              Original program                                                              Steph Goddard   01/17/2008
# MAGIC 
# MAGIC Pooja Sunkara  2014-02-12   5114             Rewrite in Parallel                                                           Bhoomi Dasari     2/24/2014

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC EDW Claim Inventory Summary Fact extract from IDS
# MAGIC Code SK lookups
# MAGIC Job name:
# MAGIC IdsEdwClmOnHandSumFExtr
# MAGIC Write CLM_ON_HAND_SUM_F
# MAGIC  Data into a dataset for Pkey job.
# MAGIC Read data from source table CLM
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
LastRunDate = get_widget_value("LastRunDate","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_CD_MPPNG_In
extract_query_db2_CD_MPPNG_In = (
    f"SELECT \n"
    f"CD_MPPNG_SK,\n"
    f"COALESCE(TRGT_CD,'UNK') TRGT_CD,\n"
    f"COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM \n"
    f"FROM\n"
    f"{IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_In)
    .load()
)

# Stage: Copy_CdMppng
df_Copy_CdMppng_ref_ClmInptSrcCd = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_CdMppng_ref_ClmSubTypCd = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_CdMppng_ref_srcsys = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_CdMppng_ref_ClmTypCd = df_db2_CD_MPPNG_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: db2_OPS_WORK_UNIT_In
extract_query_db2_OPS_WORK_UNIT_In = (
    f"SELECT\n"
    f"OPS_WORK_UNIT.OPS_WORK_UNIT_SK,\n"
    f"OPS_WORK_UNIT.OPS_WORK_UNIT_ID,\n"
    f"OPS_WORK_UNIT.OPS_WORK_UNIT_DESC\n"
    f"FROM \n"
    f"{IDSOwner}.OPS_WORK_UNIT  OPS_WORK_UNIT"
)
df_db2_OPS_WORK_UNIT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_OPS_WORK_UNIT_In)
    .load()
)

# Stage: Copy_OpsWorkUnit
df_Copy_OpsWorkUnit_ref_ops_work_unit_id = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_ba = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_its = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_g25 = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_bc = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_tip = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_ppo = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_bm = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_unid = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_dent = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)
df_Copy_OpsWorkUnit_ref_owud_nasco = df_db2_OPS_WORK_UNIT_In.select(
    F.col("OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC")
)

# Stage: db2_CD_MPPNG_SRCCD_NM_In
extract_query_db2_CD_MPPNG_SRCCD_NM_In = (
    f"SELECT \n"
    f"CD_MPPNG.CD_MPPNG_SK,\n"
    f"CD_MPPNG.TRGT_CD,\n"
    f"CD_MPPNG.TRGT_CD_NM,\n"
    f"CD_MPPNG.SRC_CD_NM\n"
    f"FROM {IDSOwner}.CD_MPPNG  CD_MPPNG \n"
    f"WHERE \n"
    f"CD_MPPNG.TRGT_DOMAIN_NM = 'CLAIM INVENTORY CLAIM SUBTYPE' AND CD_MPPNG.SRC_SYS_CD = 'FACETS'"
)
df_db2_CD_MPPNG_SRCCD_NM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_SRCCD_NM_In)
    .load()
)

# Stage: Cpy_SrcCdNm
df_Cpy_SrcCdNm_ref_CiscH = df_db2_CD_MPPNG_SRCCD_NM_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)
df_Cpy_SrcCdNm_ref_CiscM = df_db2_CD_MPPNG_SRCCD_NM_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)
df_Cpy_SrcCdNm_ref_CiscD = df_db2_CD_MPPNG_SRCCD_NM_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM")
)

# Stage: db2_CLM_In
extract_query_db2_CLM_In = (
    f"SELECT \n"
    f"CLM.SRC_SYS_CD_SK,\n"
    f"CLM.CLM_SUBTYP_CD_SK,\n"
    f"CLM.CLM_TYP_CD_SK,\n"
    f"CLM.FIRST_PASS_IN,\n"
    f"CLM.INPT_DT_SK,\n"
    f"CLM.CLM_INPT_SRC_CD_SK,\n"
    f"OPS.OPS_WORK_UNIT_ID,\n"
    f"PROD.PROD_ID,\n"
    f"CLM.CRT_RUN_CYC_EXCTN_SK \n"
    f"FROM \n"
    f"( {IDSOwner}.CLM CLM  left outer join {IDSOwner}.PROD PROD on CLM.PROD_SK = PROD.PROD_SK ) "
    f"left outer join {IDSOwner}.P_OPS_GRP_WORK_UNIT_XREF OPS on CLM.GRP_SK = OPS.GRP_SK\n"
    f"WHERE\n"
    f"CLM.INPT_DT_SK>='{LastRunDate}'  AND\n"
    f"CLM.SRC_SYS_CD_SK in ( SELECT CD_MPPNG_SK from {IDSOwner}.CD_MPPNG "
    f"WHERE ( TRGT_CD = 'FACETS' OR TRGT_CD = 'NPS'  )AND SRC_DOMAIN_NM = 'SOURCE SYSTEM' AND SRC_SYS_CD = 'IDS' )"
)
df_db2_CLM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_In)
    .load()
)

# Stage: Xfrm_OpsWrkUnitDesc
df_Xfrm_OpsWrkUnitDesc = df_db2_CLM_In.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
    F.col("CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    F.col("FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("CLM_INPT_SRC_CD_SK").alias("CLM_INPT_SRC_CD_SK"),
    F.col("INPT_DT_SK").alias("INPT_DT_SK"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.lit("ITS Home").alias("ITSHOME"),
    F.lit("TIP").alias("TIP"),
    F.lit("PPO").alias("PPO"),
    F.lit("BA").alias("BA"),
    F.lit("BC").alias("BC"),
    F.lit("DENTAL").alias("DENTAL"),
    F.lit("Unidentified").alias("UNIDENTIFIED"),
    F.lit("MEDICAL").alias("MEDICAL"),
    F.lit("Dental").alias("Dental_1"),
    F.lit("HOSPITAL").alias("HOSPITAL"),
    F.lit("Group 25").alias("GROUP25"),
    F.lit("BA+").alias("BA_PLUS"),
    F.lit("NASCO").alias("NASCO"),
    F.lit(EDWRunCycleDate).alias("EXTR_DT_SK")
)

# Stage: cdMppng_Lkup (PxLookup)
df_cdMppng_Lkup = (
    df_Xfrm_OpsWrkUnitDesc.alias("Ink_IdsEdwClmOnHandSumFExtr_LkupABC")
    .join(
        df_Copy_CdMppng_ref_ClmInptSrcCd.alias("ref_ClmInptSrcCd"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_INPT_SRC_CD_SK")
        == F.col("ref_ClmInptSrcCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Copy_CdMppng_ref_ClmSubTypCd.alias("ref_ClmSubTypCd"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_SUBTYP_CD_SK")
        == F.col("ref_ClmSubTypCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Copy_CdMppng_ref_srcsys.alias("ref_srcsys"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.SRC_SYS_CD_SK")
        == F.col("ref_srcsys.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Copy_CdMppng_ref_ClmTypCd.alias("ref_ClmTypCd"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_TYP_CD_SK")
        == F.col("ref_ClmTypCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_ops_work_unit_id.alias("ref_ops_work_unit_id"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.OPS_WORK_UNIT_ID")
        == F.col("ref_ops_work_unit_id.OPS_WORK_UNIT_ID"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_ba.alias("ref_owud_ba"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.BA")
        == F.col("ref_owud_ba.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_its.alias("ref_owud_its"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.ITSHOME")
        == F.col("ref_owud_its.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_g25.alias("ref_owud_g25"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.GROUP25")
        == F.col("ref_owud_g25.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_bc.alias("ref_owud_bc"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.BC")
        == F.col("ref_owud_bc.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_tip.alias("ref_owud_tip"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.TIP")
        == F.col("ref_owud_tip.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_ppo.alias("ref_owud_ppo"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.PPO")
        == F.col("ref_owud_ppo.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_bm.alias("ref_owud_bm"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.BA_PLUS")
        == F.col("ref_owud_bm.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_unid.alias("ref_owud_unid"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.UNIDENTIFIED")
        == F.col("ref_owud_unid.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_dent.alias("ref_owud_dent"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.Dental_1")
        == F.col("ref_owud_dent.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Copy_OpsWorkUnit_ref_owud_nasco.alias("ref_owud_nasco"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.NASCO")
        == F.col("ref_owud_nasco.OPS_WORK_UNIT_DESC"),
        "left",
    )
    .join(
        df_Cpy_SrcCdNm_ref_CiscM.alias("ref_CiscM"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.MEDICAL")
        == F.col("ref_CiscM.SRC_CD_NM"),
        "left",
    )
    .join(
        df_Cpy_SrcCdNm_ref_CiscD.alias("ref_CiscD"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.DENTAL")
        == F.col("ref_CiscD.SRC_CD_NM"),
        "left",
    )
    .join(
        df_Cpy_SrcCdNm_ref_CiscH.alias("ref_CiscH"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.HOSPITAL")
        == F.col("ref_CiscH.SRC_CD_NM"),
        "left",
    )
    .select(
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.CLM_INPT_SRC_CD_SK").alias("CLM_INPT_SRC_CD_SK"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.INPT_DT_SK").alias("INPT_DT_SK"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        F.col("Ink_IdsEdwClmOnHandSumFExtr_LkupABC.PROD_ID").alias("PROD_ID"),
        F.col("ref_ClmInptSrcCd.TRGT_CD").alias("ClmInptSrcCd_TRGT_CD"),
        F.col("ref_ClmSubTypCd.TRGT_CD").alias("ClmSubTypCd_TRGT_CD"),
        F.col("ref_srcsys.TRGT_CD").alias("srcsys_TRGT_CD"),
        F.col("ref_ClmTypCd.TRGT_CD").alias("ClmTypCd_TRGT_CD"),
        F.col("ref_ops_work_unit_id.OPS_WORK_UNIT_SK").alias("ops_work_unit_id_OPS_WORK_UNIT_SK"),
        F.col("ref_ops_work_unit_id.OPS_WORK_UNIT_DESC").alias("ops_work_unit_id_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_ba.OPS_WORK_UNIT_SK").alias("owud_ba_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_ba.OPS_WORK_UNIT_ID").alias("owud_ba_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_ba.OPS_WORK_UNIT_DESC").alias("owud_ba_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_its.OPS_WORK_UNIT_SK").alias("owud_its_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_its.OPS_WORK_UNIT_ID").alias("owud_its_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_its.OPS_WORK_UNIT_DESC").alias("owud_its_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_g25.OPS_WORK_UNIT_SK").alias("owud_g_25_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_g25.OPS_WORK_UNIT_ID").alias("owud_g_25_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_g25.OPS_WORK_UNIT_DESC").alias("owud_g_25_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_bc.OPS_WORK_UNIT_SK").alias("owud_bc_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_bc.OPS_WORK_UNIT_ID").alias("owud_bc_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_bc.OPS_WORK_UNIT_DESC").alias("owud_bc_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_tip.OPS_WORK_UNIT_SK").alias("owud_tip_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_tip.OPS_WORK_UNIT_ID").alias("owud_tip_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_tip.OPS_WORK_UNIT_DESC").alias("owud_tip_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_ppo.OPS_WORK_UNIT_SK").alias("owud_ppo_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_ppo.OPS_WORK_UNIT_ID").alias("owud_ppo_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_ppo.OPS_WORK_UNIT_DESC").alias("owud_ppo_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_bm.OPS_WORK_UNIT_SK").alias("owud_bm_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_bm.OPS_WORK_UNIT_ID").alias("owud_bm_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_bm.OPS_WORK_UNIT_DESC").alias("owud_bm_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_unid.OPS_WORK_UNIT_SK").alias("owud_unid_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_unid.OPS_WORK_UNIT_ID").alias("owud_unid_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_unid.OPS_WORK_UNIT_DESC").alias("owud_unid_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_dent.OPS_WORK_UNIT_SK").alias("owud_dent_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_dent.OPS_WORK_UNIT_ID").alias("owud_dent_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_dent.OPS_WORK_UNIT_DESC").alias("owud_dent_OPS_WORK_UNIT_DESC"),
        F.col("ref_owud_nasco.OPS_WORK_UNIT_SK").alias("owud_nasco_OPS_WORK_UNIT_SK"),
        F.col("ref_owud_nasco.OPS_WORK_UNIT_ID").alias("owud_nasco_OPS_WORK_UNIT_ID"),
        F.col("ref_owud_nasco.OPS_WORK_UNIT_DESC").alias("owud_nasco_OPS_WORK_UNIT_DESC"),
        F.col("ref_CiscM.CD_MPPNG_SK").alias("ciscM_CD_MPPNG_SK"),
        F.col("ref_CiscM.TRGT_CD").alias("ciscM_TRGT_CD"),
        F.col("ref_CiscM.TRGT_CD_NM").alias("ciscM_TRGT_CD_NM"),
        F.col("ref_CiscD.CD_MPPNG_SK").alias("ciscD_CD_MPPNG_SK"),
        F.col("ref_CiscD.TRGT_CD").alias("ciscD_TRGT_CD"),
        F.col("ref_CiscD.TRGT_CD_NM").alias("ciscD_TRGT_CD_NM"),
        F.col("ref_CiscH.CD_MPPNG_SK").alias("ciscH_CD_MPPNG_SK"),
        F.col("ref_CiscH.TRGT_CD").alias("ciscH_TRGT_CD"),
        F.col("ref_CiscH.TRGT_CD_NM").alias("ciscH_TRGT_CD_NM"),
    )
)

# Stage: xmf_businessLogic (Transformer)
df_xmf_businessLogic_vars = (
    df_cdMppng_Lkup
    .withColumn("svProdId1", F.col("PROD_ID").substr(F.lit(1), F.lit(1)))
    .withColumn("svProdId2", F.col("PROD_ID").substr(F.lit(1), F.lit(2)))
    .withColumn("svProdId3", F.col("PROD_ID").substr(F.lit(1), F.lit(3)))
    .withColumn("svHasNascoSrcSysCd", F.when(F.col("srcsys_TRGT_CD")=="NPS", True).otherwise(False))
    .withColumn("svHasGroup25Id", F.when(F.col("OPS_WORK_UNIT_ID")=="Group25", True).otherwise(False))
    .withColumn("svHasItsHomeSrcCd", F.when(
        (F.col("ClmInptSrcCd_TRGT_CD").isin("H","K","RH")), True
    ).otherwise(False))
    .withColumn("svHasOpsWorkUnitId", F.when(
        (F.col("OPS_WORK_UNIT_ID").isNotNull()) | (F.length(F.col("OPS_WORK_UNIT_ID"))>0),
        True
    ).otherwise(False))
    .withColumn(
        "svHasTipProdId",
        F.when(
            F.col("svProdId2")=="MG", True
        ).when(
            F.col("svProdId3").isin("MSK","MSM","MSX","MVM","MVC","TCK","TCM","TCX","TPK","TPM","TPX"),
            True
        ).otherwise(False)
    )
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svOpsWorkUnitId",
    F.when(F.col("svHasNascoSrcSysCd"), F.col("owud_nasco_OPS_WORK_UNIT_ID"))
    .when(F.col("svHasGroup25Id"), F.col("owud_g_25_OPS_WORK_UNIT_ID"))
    .when(F.col("svHasItsHomeSrcCd"), F.col("owud_its_OPS_WORK_UNIT_ID"))
    .when(F.col("svHasOpsWorkUnitId"), F.col("OPS_WORK_UNIT_ID"))
    .when(F.col("svHasTipProdId"), F.col("owud_tip_OPS_WORK_UNIT_ID"))
    .when(
        (F.col("svProdId2").isin("PB","PC","PF")),
        F.col("owud_ppo_OPS_WORK_UNIT_ID")
    )
    .when(F.col("svProdId2")=="BA+", F.col("owud_bm_OPS_WORK_UNIT_ID"))
    .when(F.col("svProdId2")=="BA", F.col("owud_ba_OPS_WORK_UNIT_ID"))
    .when(F.col("svProdId2")=="BC", F.col("owud_bc_OPS_WORK_UNIT_ID"))
    .when(F.col("svProdId1")=="D", F.col("owud_dent_OPS_WORK_UNIT_ID"))
    .when(F.col("owud_unid_OPS_WORK_UNIT_ID").isNotNull(), F.col("owud_unid_OPS_WORK_UNIT_ID"))
    .otherwise(F.lit("UNK"))
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svOpsWorkUnitSk",
    F.when(F.col("svHasNascoSrcSysCd"), F.col("owud_nasco_OPS_WORK_UNIT_SK"))
    .when(F.col("svHasGroup25Id"), F.col("owud_g_25_OPS_WORK_UNIT_SK"))
    .when(F.col("svHasItsHomeSrcCd"), F.col("owud_its_OPS_WORK_UNIT_SK"))
    .when(F.col("svHasOpsWorkUnitId"), F.col("ops_work_unit_id_OPS_WORK_UNIT_SK"))
    .when(F.col("svHasTipProdId"), F.col("owud_tip_OPS_WORK_UNIT_SK"))
    .when(
        (F.col("svProdId2").isin("PB","PC","PF")),
        F.col("owud_ppo_OPS_WORK_UNIT_SK")
    )
    .when(F.col("svProdId2")=="BA+", F.col("owud_bm_OPS_WORK_UNIT_SK"))
    .when(F.col("svProdId2")=="BA", F.col("owud_ba_OPS_WORK_UNIT_SK"))
    .when(F.col("svProdId2")=="BC", F.col("owud_bc_OPS_WORK_UNIT_SK"))
    .when(F.col("svProdId1")=="D", F.col("owud_dent_OPS_WORK_UNIT_SK"))
    .when(F.col("owud_unid_OPS_WORK_UNIT_SK").isNotNull(), F.col("owud_unid_OPS_WORK_UNIT_SK"))
    .otherwise(F.lit(0))
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svOpsWorkUnitNm",
    F.when(F.col("svHasNascoSrcSysCd"), F.col("owud_nasco_OPS_WORK_UNIT_DESC"))
    .when(F.col("svHasGroup25Id"), F.col("owud_g_25_OPS_WORK_UNIT_DESC"))
    .when(F.col("svHasItsHomeSrcCd"), F.col("owud_its_OPS_WORK_UNIT_DESC"))
    .when(F.col("svHasOpsWorkUnitId"), F.col("ops_work_unit_id_OPS_WORK_UNIT_DESC"))
    .when(F.col("svHasTipProdId"), F.col("owud_tip_OPS_WORK_UNIT_DESC"))
    .when(
        (F.col("svProdId2").isin("PB","PC","PF")),
        F.col("owud_ppo_OPS_WORK_UNIT_DESC")
    )
    .when(F.col("svProdId2")=="BA+", F.col("owud_bm_OPS_WORK_UNIT_DESC"))
    .when(F.col("svProdId2")=="BA", F.col("owud_ba_OPS_WORK_UNIT_DESC"))
    .when(F.col("svProdId2")=="BC", F.col("owud_bc_OPS_WORK_UNIT_DESC"))
    .when(F.col("svProdId1")=="D", F.col("owud_dent_OPS_WORK_UNIT_DESC"))
    .when(F.col("owud_unid_OPS_WORK_UNIT_DESC").isNotNull(), F.col("owud_unid_OPS_WORK_UNIT_DESC"))
    .otherwise(F.lit("UNK"))
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svClmInvtrySubtypCd",
    F.when(F.col("ClmTypCd_TRGT_CD")=="MED",
        F.when(
            F.col("ClmSubTypCd_TRGT_CD").isin("IP","OP"),
            F.col("ciscH_TRGT_CD")
        ).when(
            F.col("ClmSubTypCd_TRGT_CD")=="PR",
            F.col("ciscM_TRGT_CD")
        ).otherwise("UNK")
    ).when(
        F.col("ClmTypCd_TRGT_CD")=="DNTL",
        F.col("ciscD_TRGT_CD")
    ).otherwise("UNK")
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svClmInvtrySubtypNm",
    F.when(F.col("ClmTypCd_TRGT_CD")=="MED",
        F.when(
            F.col("ClmSubTypCd_TRGT_CD").isin("IP","OP"),
            F.col("ciscH_TRGT_CD_NM")
        ).when(
            F.col("ClmSubTypCd_TRGT_CD")=="PR",
            F.col("ciscM_TRGT_CD_NM")
        ).otherwise("UNK")
    ).when(
        F.col("ClmTypCd_TRGT_CD")=="DNTL",
        F.col("ciscD_TRGT_CD_NM")
    ).otherwise("UNK")
)

df_xmf_businessLogic_vars = df_xmf_businessLogic_vars.withColumn(
    "svClmInvtrySubtypSk",
    F.when(F.col("ClmTypCd_TRGT_CD")=="MED",
        F.when(
            F.col("ClmSubTypCd_TRGT_CD").isin("IP","OP"),
            F.col("ciscH_CD_MPPNG_SK")
        ).when(
            F.col("ClmSubTypCd_TRGT_CD")=="PR",
            F.col("ciscM_CD_MPPNG_SK")
        ).otherwise("UNK")
    ).when(
        F.col("ClmTypCd_TRGT_CD")=="DNTL",
        F.col("ciscD_CD_MPPNG_SK")
    ).otherwise("UNK")
)

df_xmf_businessLogic = df_xmf_businessLogic_vars.select(
    F.lit(0).alias("CLM_ON_HAND_SUM_SK"),
    F.when(F.col("srcsys_TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("srcsys_TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("svOpsWorkUnitId").alias("OPS_WORK_UNIT_ID"),
    F.col("svClmInvtrySubtypCd").alias("CLM_INVTRY_SUBTYP_CD"),
    F.lit(EDWRunCycleDate).alias("EXTR_DT_SK"),
    F.col("INPT_DT_SK").alias("INPT_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svOpsWorkUnitSk").alias("OPS_WORK_UNIT_SK"),
    F.when(F.trim(F.col("FIRST_PASS_IN"))=="Y", F.lit(0)).otherwise(F.lit(1)).alias("NON_FIRST_PASS_SUM_CT"),
    F.lit(1).alias("SUM_CT"),
    F.col("svClmInvtrySubtypNm").alias("CLM_INVTRY_SUBTYP_NM"),
    F.col("svOpsWorkUnitNm").alias("OPS_WORK_UNIT_DESC"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svClmInvtrySubtypSk").alias("CLM_INVTRY_SUBTYP_CD_SK")
)

# Stage: Agg_SUM (PxAggregator)
group_cols = [
    "CLM_ON_HAND_SUM_SK","SRC_SYS_CD","OPS_WORK_UNIT_ID","CLM_INVTRY_SUBTYP_CD",
    "EXTR_DT_SK","INPT_DT_SK","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "OPS_WORK_UNIT_SK","CLM_INVTRY_SUBTYP_NM","OPS_WORK_UNIT_DESC","CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_INVTRY_SUBTYP_CD_SK"
]
df_Agg_SUM = df_xmf_businessLogic.groupBy(group_cols).agg(
    F.sum("NON_FIRST_PASS_SUM_CT").alias("SUM_NON_FIRST_PASS_SUM_CT"),
    F.sum("SUM_CT").alias("SUM_CT_Sum")
).select(
    F.col("CLM_ON_HAND_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("OPS_WORK_UNIT_ID"),
    F.col("CLM_INVTRY_SUBTYP_CD"),
    F.col("EXTR_DT_SK"),
    F.col("INPT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("OPS_WORK_UNIT_SK"),
    F.col("CLM_INVTRY_SUBTYP_NM"),
    F.col("OPS_WORK_UNIT_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_INVTRY_SUBTYP_CD_SK"),
    F.col("SUM_NON_FIRST_PASS_SUM_CT"),
    F.col("SUM_CT_Sum")
)

# Stage: Xfrm_BusinessLogic
# We create row_number so we can filter the first row for UNKRow and NARow constraints.
w = Window.orderBy(F.lit(1))
df_Agg_SUM_rn = df_Agg_SUM.withColumn("row_num", F.row_number().over(w))

df_UNKRow = df_Agg_SUM_rn.filter(F.col("row_num")==1).select(
    F.lit(0).alias("CLM_ON_HAND_SUM_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("OPS_WORK_UNIT_ID"),
    F.lit("UNK").alias("CLM_INVTRY_SUBTYP_CD"),
    F.lit("1753-01-01").alias("EXTR_DT_SK"),
    F.lit("1753-01-01").alias("INPT_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("OPS_WORK_UNIT_SK"),
    F.lit("1753-01-01").alias("EXTR_DT"),
    F.lit("1753-01-01").alias("INPT_DT"),
    F.lit(0).alias("NON_FIRST_PASS_SUM_CT"),
    F.lit(0).alias("SUM_CT"),
    F.lit("UNK").alias("CLM_INVTRY_SUBTYP_NM"),
    F.lit(None).alias("OPS_WORK_UNIT_DESC"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_INVTRY_SUBTYP_CD_SK")
)

df_NARow = df_Agg_SUM_rn.filter(F.col("row_num")==1).select(
    F.lit(1).alias("CLM_ON_HAND_SUM_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("OPS_WORK_UNIT_ID"),
    F.lit("NA").alias("CLM_INVTRY_SUBTYP_CD"),
    F.lit("1753-01-01").alias("EXTR_DT_SK"),
    F.lit("1753-01-01").alias("INPT_DT_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("OPS_WORK_UNIT_SK"),
    F.lit("1753-01-01").alias("EXTR_DT"),
    F.lit("1753-01-01").alias("INPT_DT"),
    F.lit(0).alias("NON_FIRST_PASS_SUM_CT"),
    F.lit(0).alias("SUM_CT"),
    F.lit("NA").alias("CLM_INVTRY_SUBTYP_NM"),
    F.lit(None).alias("OPS_WORK_UNIT_DESC"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_INVTRY_SUBTYP_CD_SK")
)

df_main = df_Agg_SUM_rn.filter((F.col("SRC_SYS_CD")!="NA") & (F.col("SRC_SYS_CD")!="UNK")).select(
    F.col("CLM_ON_HAND_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("OPS_WORK_UNIT_ID"),
    F.col("CLM_INVTRY_SUBTYP_CD"),
    F.col("EXTR_DT_SK"),
    F.col("INPT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("OPS_WORK_UNIT_SK"),
    F.expr('FORMAT.DATE.EE(EXTR_DT_SK, "DATE", "DATE", "DB2TIMESTAMP")[1, 10]').alias("EXTR_DT"),
    F.expr('FORMAT.DATE.EE(INPT_DT_SK, "DATE", "DATE", "DB2TIMESTAMP")[1, 10]').alias("INPT_DT"),
    F.col("SUM_NON_FIRST_PASS_SUM_CT").alias("NON_FIRST_PASS_SUM_CT"),
    F.col("SUM_CT_Sum").alias("SUM_CT"),
    F.col("CLM_INVTRY_SUBTYP_NM"),
    F.col("OPS_WORK_UNIT_DESC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_INVTRY_SUBTYP_CD_SK")
)

# Stage: fnl_dataLinks (PxFunnel)
df_funnel = df_main.unionByName(df_UNKRow).unionByName(df_NARow)

# Stage: ds_CLM_ON_HAND_SUM_F_Load (PxDataSet -> Parquet)
# Final select to maintain column order
df_final = df_funnel.select(
    "CLM_ON_HAND_SUM_SK",
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "EXTR_DT_SK",
    "INPT_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "OPS_WORK_UNIT_SK",
    "EXTR_DT",
    "INPT_DT",
    "NON_FIRST_PASS_SUM_CT",
    "SUM_CT",
    "CLM_INVTRY_SUBTYP_NM",
    "OPS_WORK_UNIT_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_INVTRY_SUBTYP_CD_SK"
)

df_final = df_final.withColumn(
    "EXTR_DT_SK", F.rpad(F.col("EXTR_DT_SK"), 10, " ")
).withColumn(
    "INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/ds/CLM_ON_HAND_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)