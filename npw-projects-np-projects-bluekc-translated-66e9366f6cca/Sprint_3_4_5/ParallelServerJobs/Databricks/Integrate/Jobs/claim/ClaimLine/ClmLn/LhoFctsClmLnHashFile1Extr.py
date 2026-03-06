# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME: FctsClmLnHashFile1Extr
# MAGIC CALLED BY:  LhoFctsClmOnDmdPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  One of three hash file extracts that must run in order (1,2,3)
# MAGIC 
# MAGIC 
# MAGIC \(9)Extract Claim Line Disallow Data extract from Facets, and load to hash files to be used in FctsClmLnTrns, FctsClmLnDntlTrns and FctsClmLnDsalwExtr
# MAGIC                 Extract rows from the disallow tables for W_CLM_LN_DSALW - pull all rows for claims on TMP_DRIVER
# MAGIC 
# MAGIC  SOURCE:
# MAGIC 
# MAGIC \(9)CMC_CDML_CL_LINE\(9) 
# MAGIC                 CMC_CDDL_CL_LINE
# MAGIC                 CMC_CDMD_LI_DISALL
# MAGIC                 CMC_CDDD_DNLI_DIS
# MAGIC                 CMC_CDOR_LI_OVR
# MAGIC                 CMC_CDDO_DNLI_OVR
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CMC_CDCB_LI_COB
# MAGIC                 TMP_DRIVER
# MAGIC                  IDS CD_MPPNG
# MAGIC                  IDS DSALW_EXCD
# MAGIC 
# MAGIC 
# MAGIC  Hash Files
# MAGIC               used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC                    hf_clm_ln_dsalw_a_dup
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 
# MAGIC \(9)W_CLM_LN_DSALW.dat to be loaded in IDS, must go through the delete process
# MAGIC 
# MAGIC  
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 \(9)Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263702                                         Original Programming                                              IntegrateDev2                     Jaideep Mankala      10/09/2020
# MAGIC \(9)
# MAGIC Venkatesh Munnangi 2020-10-12                                                                             Removed Data Elements Populated in the job
# MAGIC Prabhu ES                 2022-03-17                        S2S                                               MSSQL ODBC conn params added                               IntegrateDev5     \(9)Ken Bradmon\(9)2022-06-11

# MAGIC Facets Claim Line Hash File Extract
# MAGIC These hash files are used in FctsClmLnHashFile2Extr and FctsClmLnHashFile3Extr
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Load all rows on CDDD and CDMD to P_CLM_LN_DSALW for the claims being processed.   This table will have to be part of the delete process.
# MAGIC 
# MAGIC The P_CLM_LN_DSALW table will be used for reprocessing disallows rather than repulling from Facets archive.  
# MAGIC 
# MAGIC Loaded in IdsFctsClmLoad4Seq.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
DriverTable = get_widget_value('DriverTable','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Read from cddd_cdmd_all (ODBCConnector, not IDS/EDW/BCBS => generic DB config)
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
cddd_cdmd_all_query = (
    f"SELECT CLCL_ID,CDML_SEQ_NO,CDMD_TYPE,CDMD_DISALL_AMT,EXCD_ID,MEME_CK "
    f"FROM {LhoFacetsStgOwner}.CMC_CDMD_LI_DISALL DIS, tempdb..{DriverTable} TMP "
    f"WHERE DIS.CLCL_ID = TMP.CLM_ID "
    f"union "
    f"SELECT CLCL_ID,CDDL_SEQ_NO,CDDD_TYPE,CDDD_DISALL_AMT,EXCD_ID,MEME_CK "
    f"FROM {LhoFacetsStgOwner}.CMC_CDDD_DNLI_DIS DIS, tempdb..{DriverTable} TMP "
    f"WHERE DIS.CLCL_ID = TMP.CLM_ID"
)
df_cddd_cdmd_all = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", cddd_cdmd_all_query)
    .load()
)

# Trans1
df_Trans1_intermediate = df_cddd_cdmd_all.withColumn(
    "svFctsSrcSysCdSk",
    GetFkeyCodes("IDS", F.lit(0), F.lit("SOURCE SYSTEM"), F.lit(SrcSysCd), F.lit("X"))
).withColumn("SRC_SYS_CD_SK", F.col("svFctsSrcSysCdSk")) \
 .withColumn("CLM_ID", F.col("CLCL_ID")) \
 .withColumn("CLM_LN_SEQ_NO", F.col("CDML_SEQ_NO")) \
 .withColumn("CLM_LN_DSALW_TYP_CD", F.col("CDMD_TYPE")) \
 .withColumn("CLM_LN_DSALW_EXCD", F.col("EXCD_ID")) \
 .withColumn("DSALW_AMT", F.col("CDMD_DISALL_AMT")) \
 .withColumn("MBR_UNIQ_KEY", F.col("MEME_CK")) \
 .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle))

df_Trans1 = df_Trans1_intermediate.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("DSALW_AMT"),
    F.col("MBR_UNIQ_KEY"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

p_clm_ln_dsalw_filepath = f"{adls_path}/load/P_CLM_LN_DSALW.{Source}.dat"
write_files(
    df_Trans1,
    p_clm_ln_dsalw_filepath,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# dsalw_excd (DB2Connector with Database=IDS => read entire table for lookups)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
dsalw_excd_query = (
    f"SELECT DSALW_EXCD.EXCD_ID, DSALW_EXCD.EFF_DT_SK, DSALW_EXCD.TERM_DT_SK, CD_MPPNG.SRC_CD, DSALW_EXCD.BYPS_IN "
    f"FROM {IDSOwner}.DSALW_EXCD DSALW_EXCD "
    f"JOIN {IDSOwner}.CD_MPPNG CD_MPPNG ON DSALW_EXCD.EXCD_RESP_CD_SK=CD_MPPNG.CD_MPPNG_SK"
)
df_dsalw_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", dsalw_excd_query)
    .load()
)

# FacetsOverrides (ODBCConnector, also from LhoFacetsStgOwner)
facets_overrides_query_cdor_lkup = (
    f"SELECT OVR.CLCL_ID,OVR.CDML_SEQ_NO,OVR.CDOR_OR_ID,CDOR_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT,"
    f"LINE.CDML_CONSIDER_CHG,LINE.CDML_DISALL_AMT,LINE.CDML_DISALL_EXCD "
    f"FROM {LhoFacetsStgOwner}.CMC_CDOR_LI_OVR OVR,"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,"
    f"{LhoFacetsStgOwner}.CMC_CDML_CL_LINE LINE "
    f"WHERE  OVR.CLCL_ID = TMP.CLM_ID "
    f"and OVR.CLCL_ID = CLAIM.CLCL_ID "
    f"and OVR.CDOR_OR_ID in ('AA','AU','AT') "
    f"and OVR.CLCL_ID = LINE.CLCL_ID "
    f"and OVR.CDML_SEQ_NO  = LINE.CDML_SEQ_NO "
    f"and LINE.CDML_DISALL_AMT <> 0 "
    f"union "
    f"SELECT OVR.CLCL_ID,OVR.CDDL_SEQ_NO,OVR.CDDO_OR_ID,CDDO_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT,"
    f"LINE.CDDL_CONSIDER_CHG,LINE.CDDL_DISALL_AMT,LINE.CDDL_DISALL_EXCD "
    f"FROM {LhoFacetsStgOwner}.CMC_CDDO_DNLI_OVR OVR,"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,"
    f"{LhoFacetsStgOwner}.CMC_CDDL_CL_LINE LINE "
    f"WHERE  OVR.CLCL_ID = TMP.CLM_ID "
    f"and OVR.CLCL_ID = CLAIM.CLCL_ID "
    f"and OVR.CDDO_OR_ID in ('DA','DU','DT') "
    f"and OVR.CLCL_ID = LINE.CLCL_ID "
    f"and OVR.CDDL_SEQ_NO  = LINE.CDDL_SEQ_NO "
    f"and LINE.CDDL_DISALL_AMT <> 0"
)

facets_overrides_query_its_claim = (
    f"SELECT MISC.CLCL_ID "
    f"FROM {LhoFacetsStgOwner}.CMC_CDOR_LI_OVR OVR,"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,"
    f"{LhoFacetsStgOwner}.CMC_CLMI_MISC MISC "
    f"WHERE  OVR.CLCL_ID = TMP.CLM_ID "
    f"and OVR.CLCL_ID = CLAIM.CLCL_ID "
    f"and OVR.CDOR_OR_ID in ('AU','AT','DU','DT') "
    f"and OVR.CLCL_ID = MISC.CLCL_ID "
    f"and MISC.CLMI_ITS_SCCF_NO > ' '"
)

facets_overrides_query_cdor_x_extr = (
    f"SELECT OVR.CLCL_ID,OVR.CDML_SEQ_NO,OVR.CDOR_OR_ID,OVR.CDOR_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT, "
    f"LINE.CDML_DISALL_AMT "
    f"FROM {LhoFacetsStgOwner}.CMC_CDOR_LI_OVR OVR,"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,"
    f"{LhoFacetsStgOwner}.CMC_CDML_CL_LINE LINE "
    f"WHERE  OVR.CLCL_ID = TMP.CLM_ID "
    f"and OVR.CLCL_ID = CLAIM.CLCL_ID "
    f"and OVR.CDOR_OR_ID = 'AX' "
    f"and OVR.CLCL_ID = LINE.CLCL_ID "
    f"and OVR.CDML_SEQ_NO  = LINE.CDML_SEQ_NO "
    f"and OVR.CDOR_OR_AMT > 0 "
    f"and LINE.CDML_DISALL_AMT <> 0 "
    f"union "
    f"SELECT OVR.CLCL_ID,OVR.CDDL_SEQ_NO,OVR.CDDO_OR_ID,OVR.CDDO_OR_AMT,OVR.EXCD_ID,CLAIM.CLCL_PAID_DT, "
    f"LINE.CDDL_DISALL_AMT "
    f"FROM {LhoFacetsStgOwner}.CMC_CDDO_DNLI_OVR OVR,"
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM,"
    f"tempdb..{DriverTable} TMP,"
    f"{LhoFacetsStgOwner}.CMC_CDDL_CL_LINE LINE "
    f"WHERE  OVR.CLCL_ID = TMP.CLM_ID "
    f"and OVR.CLCL_ID = CLAIM.CLCL_ID "
    f"and OVR.CDDO_OR_ID = 'DX' "
    f"and OVR.CLCL_ID = LINE.CLCL_ID "
    f"and OVR.CDDL_SEQ_NO  = LINE.CDDL_SEQ_NO "
    f"and OVR.CDDO_OR_AMT > 0 "
    f"and LINE.CDDL_DISALL_AMT <> 0"
)

df_FacetsOverrides_cdor_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", facets_overrides_query_cdor_lkup)
    .load()
)

df_FacetsOverrides_its_claim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", facets_overrides_query_its_claim)
    .load()
)

df_FacetsOverrides_cdor_x_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", facets_overrides_query_cdor_x_extr)
    .load()
)

# Trans3: primary link = cdor_lkup, join with dsalw_excd on EXCD_ID, substring( CLCL_PAID_DT ,1,10 ) => EFF_DT_SK, TERM_DT_SK
df_Trans3_join = (
    df_FacetsOverrides_cdor_lkup.alias("cdor_lkup")
    .join(
        df_dsalw_excd.alias("DsalwExcdRespCdAAAT"),
        (
            (F.col("cdor_lkup.EXCD_ID") == F.col("DsalwExcdRespCdAAAT.EXCD_ID")) &
            (F.substring(F.col("cdor_lkup.CLCL_PAID_DT"), 1, 10) == F.col("DsalwExcdRespCdAAAT.EFF_DT_SK")) &
            (F.substring(F.col("cdor_lkup.CLCL_PAID_DT"), 1, 10) == F.col("DsalwExcdRespCdAAAT.TERM_DT_SK"))
        ),
        "left"
    )
)

df_cdor_a_t_inter = df_Trans3_join.filter(
    (F.col("cdor_lkup.CDOR_OR_ID").isin(["AA","AT","DA","DT"]))
).withColumn("CLCL_ID", F.col("cdor_lkup.CLCL_ID")) \
 .withColumn("CDML_SEQ_NO", F.col("cdor_lkup.CDML_SEQ_NO")) \
 .withColumn("CDOR_OR_ID", F.col("cdor_lkup.CDOR_OR_ID")) \
 .withColumn("CDOR_OR_AMT", F.col("cdor_lkup.CDOR_OR_AMT")) \
 .withColumn("EXCD_ID", F.col("cdor_lkup.EXCD_ID")) \
 .withColumn("CLCL_PAID_DT", F.col("cdor_lkup.CLCL_PAID_DT")) \
 .withColumn("CDML_CONSIDER_CHG", F.col("cdor_lkup.CDML_CONSIDER_CHG")) \
 .withColumn("CDML_DISALL_AMT", F.col("cdor_lkup.CDML_DISALL_AMT")) \
 .withColumn("CDML_DISALL_EXCD", F.col("cdor_lkup.CDML_DISALL_EXCD")) \
 .withColumn("SRC_CD", F.when(F.col("DsalwExcdRespCdAAAT.SRC_CD").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdRespCdAAAT.SRC_CD"))) \
 .withColumn("BYPS_IN", F.when(F.col("DsalwExcdRespCdAAAT.BYPS_IN").isNull(), F.lit("N")).otherwise(F.col("DsalwExcdRespCdAAAT.BYPS_IN")))

df_cdor_a_t = df_cdor_a_t_inter.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.rpad(F.col("CDOR_OR_ID"),2," ").alias("CDOR_OR_ID"),
    F.col("CDOR_OR_AMT"),
    F.rpad(F.col("EXCD_ID"),3," ").alias("EXCD_ID"),
    F.col("CLCL_PAID_DT"),
    F.col("CDML_CONSIDER_CHG"),
    F.col("CDML_DISALL_AMT"),
    F.rpad(F.col("CDML_DISALL_EXCD"),3," ").alias("CDML_DISALL_EXCD"),
    F.col("SRC_CD"),
    F.rpad(F.col("BYPS_IN"),1," ").alias("BYPS_IN")
)

df_cdor_u_inter = df_Trans3_join.filter(
    (F.col("cdor_lkup.CDOR_OR_ID").isin(["AU","DU"]))
).withColumn("CLCL_ID", F.col("cdor_lkup.CLCL_ID")) \
 .withColumn("CDML_SEQ_NO", F.col("cdor_lkup.CDML_SEQ_NO")) \
 .withColumn("CDOR_OR_ID", F.col("cdor_lkup.CDOR_OR_ID")) \
 .withColumn("CDOR_OR_AMT", F.col("cdor_lkup.CDOR_OR_AMT")) \
 .withColumn("EXCD_ID", F.col("cdor_lkup.EXCD_ID")) \
 .withColumn("CLCL_PAID_DT", F.col("cdor_lkup.CLCL_PAID_DT")) \
 .withColumn("CDML_CONSIDER_CHG", F.col("cdor_lkup.CDML_CONSIDER_CHG")) \
 .withColumn("CDML_DISALL_AMT", F.col("cdor_lkup.CDML_DISALL_AMT")) \
 .withColumn("CDML_DISALL_EXCD", F.col("cdor_lkup.CDML_DISALL_EXCD")) \
 .withColumn("SRC_CD", F.when(F.col("DsalwExcdRespCdAAAT.SRC_CD").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdRespCdAAAT.SRC_CD"))) \
 .withColumn("BYPS_IN", F.when(F.col("DsalwExcdRespCdAAAT.BYPS_IN").isNull(), F.lit("N")).otherwise(F.col("DsalwExcdRespCdAAAT.BYPS_IN")))

df_cdor_u = df_cdor_u_inter.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.rpad(F.col("CDOR_OR_ID"),2," ").alias("CDOR_OR_ID"),
    F.col("CDOR_OR_AMT"),
    F.rpad(F.col("EXCD_ID"),3," ").alias("EXCD_ID"),
    F.col("CLCL_PAID_DT"),
    F.col("CDML_CONSIDER_CHG"),
    F.col("CDML_DISALL_AMT"),
    F.rpad(F.col("CDML_DISALL_EXCD"),3," ").alias("CDML_DISALL_EXCD"),
    F.col("SRC_CD"),
    F.rpad(F.col("BYPS_IN"),1," ").alias("BYPS_IN")
)

# cdor_a_t: Scenario A => deduplicate by keys [CLCL_ID,CDML_SEQ_NO,CDOR_OR_ID]
df_cdor_a_t_dedup = dedup_sort(df_cdor_a_t, ["CLCL_ID","CDML_SEQ_NO","CDOR_OR_ID"], [])
# This hashed file has 3 output pins: "t_lkup","a_lkup","a_t_extr_t". 
# No constraints => the same data is sent out each pin.
df_t_lkup = df_cdor_a_t_dedup
df_a_lkup = df_cdor_a_t_dedup
df_a_t_extr_t = df_cdor_a_t_dedup

# cdor_u: Scenario A => deduplicate by keys [CLCL_ID,CDML_SEQ_NO,CDOR_OR_ID], single output pin "au_lkup"
df_cdor_u_dedup = dedup_sort(df_cdor_u, ["CLCL_ID","CDML_SEQ_NO","CDOR_OR_ID"], [])
df_au_lkup = df_cdor_u_dedup

# its_clm_lkup: read from "its_claim" => scenario A => dedup by key [CLCL_ID]
df_its_claim = df_FacetsOverrides_its_claim.withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"),12," "))
df_its_claim_dedup = dedup_sort(df_its_claim, ["CLCL_ID"], [])
df_its_lkup = df_its_claim_dedup

# cdor_x_extr => Trans2
# Trans2: primary link = cdor_x_extr, left join t_lkup, a_lkup, dsalw_excd
df_cdor_x_extr = df_FacetsOverrides_cdor_x_extr.alias("cdor_x_extr")

df_trans2_join1 = df_cdor_x_extr.join(
    df_t_lkup.alias("t_lkup"),
    (
        (F.col("cdor_x_extr.CLCL_ID") == F.col("t_lkup.CLCL_ID")) &
        (F.col("cdor_x_extr.CDML_SEQ_NO") == F.col("t_lkup.CDML_SEQ_NO")) &
        (F.lit("AT") == F.col("t_lkup.CDOR_OR_ID"))
    ),
    "left"
)
df_trans2_join2 = df_trans2_join1.join(
    df_a_lkup.alias("a_lkup"),
    (
        (F.col("cdor_x_extr.CLCL_ID") == F.col("a_lkup.CLCL_ID")) &
        (F.col("cdor_x_extr.CDML_SEQ_NO") == F.col("a_lkup.CDML_SEQ_NO")) &
        (F.lit("AA") == F.col("a_lkup.CDOR_OR_ID"))
    ),
    "left"
)
df_trans2_join3 = df_trans2_join2.join(
    df_dsalw_excd.alias("DsalwExcdRespCdX"),
    (
        (F.col("cdor_x_extr.EXCD_ID") == F.col("DsalwExcdRespCdX.EXCD_ID")) &
        (F.substring(F.col("cdor_x_extr.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdX.EFF_DT_SK")) &
        (F.substring(F.col("cdor_x_extr.CLCL_PAID_DT"),1,10) == F.col("DsalwExcdRespCdX.TERM_DT_SK"))
    ),
    "left"
)

df_dsalw_x_inter = df_trans2_join3.filter(
    (F.col("t_lkup.CDML_SEQ_NO").isNull()) &
    (F.col("a_lkup.CDML_SEQ_NO").isNull()) &
    (F.trim(F.col("cdor_x_extr.EXCD_ID")) != F.lit("319"))
).withColumn("CLCL_ID", F.rpad(F.col("cdor_x_extr.CLCL_ID"),12," ")) \
 .withColumn("CDML_SEQ_NO", F.col("cdor_x_extr.CDML_SEQ_NO")) \
 .withColumn("DSALW_TYP", F.col("cdor_x_extr.CDOR_OR_ID")) \
 .withColumn("DSALW_AMT", F.col("cdor_x_extr.CDOR_OR_AMT")) \
 .withColumn("DSALW_EXCD", F.rpad(F.col("cdor_x_extr.EXCD_ID"),3," ")) \
 .withColumn("EXCD_RESP_CD", F.when(F.col("DsalwExcdRespCdX.EXCD_ID").isNull(), F.lit("NA")).otherwise(F.col("DsalwExcdRespCdX.SRC_CD"))) \
 .withColumn("CDML_DISALL_AMT", F.col("cdor_x_extr.CDML_DISALL_AMT")) \
 .withColumn("BYPS_IN", F.when(F.col("DsalwExcdRespCdX.SRC_CD").isNull(),F.lit("N")).otherwise(F.col("DsalwExcdRespCdX.BYPS_IN")))

df_dsalw_x = df_dsalw_x_inter.select(
    "CLCL_ID","CDML_SEQ_NO","DSALW_TYP","DSALW_AMT","DSALW_EXCD","EXCD_RESP_CD","CDML_DISALL_AMT","BYPS_IN"
)

# hash_x_tmp => scenario C (no next stage) => write to parquet
df_dsalw_x_dedup = dedup_sort(df_dsalw_x, ["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"], [])
write_files(
    df_dsalw_x_dedup,
    "hf_clm_ln_dsalw_x_tmp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# trns_a_t: primary link = a_t_extr_t, left link = au_lkup, left link = its_lkup
df_a_t_extr_t = df_a_t_extr_t.alias("a_t_extr_t")
df_au_lkup = df_au_lkup.alias("au_lkup")
df_its_lkup = df_its_lkup.alias("its_lkup")

df_trns_a_t_join1 = df_a_t_extr_t.join(
    df_au_lkup,
    (
        (F.col("a_t_extr_t.CLCL_ID") == F.col("au_lkup.CLCL_ID")) &
        (F.col("a_t_extr_t.CDML_SEQ_NO") == F.col("au_lkup.CDML_SEQ_NO")) &
        ((F.col("au_lkup.CDOR_OR_ID") == F.lit("AU")) | (F.col("au_lkup.CDOR_OR_ID") == F.lit("DU")))
    ),
    "left"
)
df_trns_a_t_join2 = df_trns_a_t_join1.join(
    df_its_lkup,
    (
        F.col("a_t_extr_t.CLCL_ID") == F.col("its_lkup.CLCL_ID")
    ),
    "left"
)

df_trns_a_t_stagevars = df_trns_a_t_join2.withColumn(
    "svAUamt",
    F.when(F.col("au_lkup.CDML_SEQ_NO").isNull(), F.lit(0)).otherwise(F.col("au_lkup.CDOR_OR_AMT"))
).withColumn(
    "svATAUamt",
    (F.col("a_t_extr_t.CDML_CONSIDER_CHG") - (F.col("a_t_extr_t.CDOR_OR_AMT") * F.col("svAUamt")))
).withColumn(
    "svATAUamtwoconsider",
    (F.col("a_t_extr_t.CDOR_OR_AMT") * F.col("svAUamt"))
).withColumn(
    "svAAamt",
    (F.col("a_t_extr_t.CDML_CONSIDER_CHG") - F.col("a_t_extr_t.CDOR_OR_AMT"))
)

df_disalw_a_dup_inter = df_trns_a_t_stagevars.filter(
    (
        ((F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("AA")) | (F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("DA"))) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")).isNotNull()) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("")) &
        (F.col("svAAamt") > 0) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("319")))
    )
).withColumn("CLCL_ID", F.rpad(F.col("a_t_extr_t.CLCL_ID"),12," ")) \
 .withColumn("CDML_SEQ_NO", F.col("a_t_extr_t.CDML_SEQ_NO")) \
 .withColumn("DSALW_TYP", F.col("a_t_extr_t.CDOR_OR_ID")) \
 .withColumn("DSALW_AMT", F.col("svAAamt")) \
 .withColumn("DSALW_EXCD", F.rpad(F.col("a_t_extr_t.EXCD_ID"),3," ")) \
 .withColumn("EXCD_RESP_CD", F.col("a_t_extr_t.SRC_CD")) \
 .withColumn("CDML_DISALL_AMT", F.col("a_t_extr_t.CDML_DISALL_AMT")) \
 .withColumn("BYPS_IN", F.rpad(F.col("a_t_extr_t.BYPS_IN"),1," "))

df_disalw_a_dup = df_disalw_a_dup_inter.select(
    "CLCL_ID","CDML_SEQ_NO","DSALW_TYP","DSALW_AMT","DSALW_EXCD","EXCD_RESP_CD","CDML_DISALL_AMT","BYPS_IN"
)

df_dsalw_tu_inter = df_trns_a_t_stagevars.filter(
    (
        ((F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("AT")) | (F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("DT"))) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")).isNotNull()) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("")) &
        (F.col("a_t_extr_t.CDML_DISALL_AMT") != 0) &
        (
          (F.col("its_lkup.CLCL_ID").isNull()) |
          ( (F.col("its_lkup.CLCL_ID").isNotNull()) & (F.col("svATAUamtwoconsider") != 0) )
        ) &
        (F.col("svAUamt") > 0) &
        (F.trim(F.col("a_t_extr_t.EXCD_ID")) != F.lit("319"))
    )
).withColumn("CLCL_ID", F.rpad(F.col("a_t_extr_t.CLCL_ID"),12," ")) \
 .withColumn("CDML_SEQ_NO", F.col("a_t_extr_t.CDML_SEQ_NO")) \
 .withColumn("DSALW_TYP",
    F.when(F.col("a_t_extr_t.CDOR_OR_ID") == F.lit("DT"), F.lit("DTDU")).otherwise(F.lit("ATAU"))
 ) \
 .withColumn("DSALW_AMT", F.col("svATAUamt")) \
 .withColumn("DSALW_EXCD", F.rpad(F.col("a_t_extr_t.EXCD_ID"),3," ")) \
 .withColumn("EXCD_RESP_CD", F.col("a_t_extr_t.SRC_CD")) \
 .withColumn("CDML_DISALL_AMT", F.col("a_t_extr_t.CDML_DISALL_AMT")) \
 .withColumn("BYPS_IN", F.rpad(F.col("a_t_extr_t.BYPS_IN"),1," "))

df_dsalw_tu = df_dsalw_tu_inter.select(
    "CLCL_ID","CDML_SEQ_NO","DSALW_TYP","DSALW_AMT","DSALW_EXCD","EXCD_RESP_CD","CDML_DISALL_AMT","BYPS_IN"
)

# hash_tu_tmp => scenario A => deduplicate => has output "a_dup_lkup => atau_check"
df_dsalw_tu_dedup = dedup_sort(df_dsalw_tu, ["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"], [])
df_a_dup_lkup = df_dsalw_tu_dedup

# aa_processing => scenario A => deduplicate => has output "aa_check => atau_check"
df_disalw_a_dup_dedup = dedup_sort(df_disalw_a_dup, ["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"], [])
df_aa_check = df_disalw_a_dup_dedup

# atau_check => primary link = aa_check, left link = a_dup_lkup
df_aa_check = df_aa_check.alias("aa_check")
df_a_dup_lkup = df_a_dup_lkup.alias("a_dup_lkup")

df_atau_check_join = df_aa_check.join(
    df_a_dup_lkup,
    (
        (F.col("aa_check.CLCL_ID") == F.col("a_dup_lkup.CLCL_ID")) &
        (F.col("aa_check.CDML_SEQ_NO") == F.col("a_dup_lkup.CDML_SEQ_NO")) &
        (F.lit("ATAU") == F.col("a_dup_lkup.DSALW_TYP"))
    ),
    "left"
)

df_disalw_a_inter = df_atau_check_join.filter(
    F.col("a_dup_lkup.CLCL_ID").isNull()
).withColumn("CLCL_ID", F.col("aa_check.CLCL_ID")) \
 .withColumn("CDML_SEQ_NO", F.col("aa_check.CDML_SEQ_NO")) \
 .withColumn("DSALW_TYP", F.col("aa_check.DSALW_TYP")) \
 .withColumn("DSALW_AMT", F.col("aa_check.DSALW_AMT")) \
 .withColumn("DSALW_EXCD", F.col("aa_check.DSALW_EXCD")) \
 .withColumn("EXCD_RESP_CD", F.col("aa_check.EXCD_RESP_CD")) \
 .withColumn("CDML_DISALL_AMT", F.col("aa_check.CDML_DISALL_AMT")) \
 .withColumn("BYPS_IN", F.col("aa_check.BYPS_IN"))

df_disalw_a = df_disalw_a_inter.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    "CDML_SEQ_NO",
    F.rpad(F.col("DSALW_TYP"),2," ").alias("DSALW_TYP"),
    "DSALW_AMT",
    F.rpad(F.col("DSALW_EXCD"),3," ").alias("DSALW_EXCD"),
    "EXCD_RESP_CD",
    "CDML_DISALL_AMT",
    F.rpad(F.col("BYPS_IN"),1," ").alias("BYPS_IN")
)

# hash_a_tmp => scenario C => final output
df_disalw_a_dedup = dedup_sort(df_disalw_a, ["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"], [])
write_files(
    df_disalw_a_dedup,
    "hf_clm_ln_dsalw_a_tmp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)