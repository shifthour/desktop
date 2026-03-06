# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS and codes hash file to load into the EDW
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS: Changed code to reflect new updated ETL Mapping document
# MAGIC 
# MAGIC Developer                                     Date                 Project               Description                                                          Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------         --------------------------------   ---------------       ------------------------------------                                         -----------------------------        ------------------------------------       -----------------------------------
# MAGIC Santosh Bokka                     06/10/2013                                   - Originally Program                                                   - EdwNewDevl                   Kalyan Neelam                   2013-06-27
# MAGIC Santosh Bokka                    08/12/2013                                      Changed code to reflect 
# MAGIC                                                                                                       new updated ETL Mapping document                     EdwNewDevl                   Bhoomi Dasari                     2013-08-13
# MAGIC Santosh Bokka                    08/22/2013                                       Changed code to reflect                                                                                      SAndrew                             2013-08-29
# MAGIC                                                                                                        new updated ETL Mapping 
# MAGIC                                                                                                          document                                                               EdwNewDevl                  Sandrew                             2013-10-25
# MAGIC Santosh Bokka                    11/12/2013                                       Updated Server name and Database
# MAGIC                                                                                                        Owner name for Lookup components                      EdwNewDevl                   Kalyan Neelam                   2013-11-12                                                                                                                                                                   
# MAGIC Pooja Sunkara                      03/31/2014        5114                           Rewrite in Parallel                                               EnterpriseWrhsDevl         Jag Yelavarthi                     2014-04-21    
# MAGIC 
# MAGIC Santosh Bokka                     2014-06-11         4917                Added BCBSKC Runcyle to load TREO Historical file.    EnterpriseNewDevl       Bhoomi Dasari                    6/23/2014 
# MAGIC 
# MAGIC Karthik Chintalapani               2015-06-12         TFS 10700           Modified the code in the Xfrm_PSN and                  EnterpriseNewDevl           Kalyan Neelam                   2015-06-15
# MAGIC                                                                                                    Filter_PSN stages to include 'BLUE-SELECT' 
# MAGIC                                                                                                    and 'BLUESELCET+' 
# MAGIC 
# MAGIC Hugh Sisson                       2017-08-17     TFS-19835      Change the SQL query in the db2_PROV_D_in stage         EnterpriseDev3                 Jag Yelavarthi                   2017-08-23     
# MAGIC                                                                                           to extract PROV_SK, PROV_ID, and PROV_NM.   
# MAGIC                                                                                           Lkup_Codes stage  changed to use the new lookup
# MAGIC                                                                                           column names.
# MAGIC 
# MAGIC Manasa Andru                   2018-07-02       Project- 60037      Added BCBSKC source system in the extract SQL          EnterpriseDev1               Kalyan Neelam                    2018-07-03
# MAGIC                                                                                                 in the db2_PROD_SH_NM_In stage.

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwPcmhProvMbrRmbrmtEligFExtr
# MAGIC PCMH_PGM_D extract from IDS to EDW
# MAGIC Write PCMH_PROV_MBR_RMBRMT_ELIG_F Data into a Sequential file for Load Ready Job.
# MAGIC Pull data from PROV_MBR_RMBRMT_ELIG
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSBCBSKCRunCycle = get_widget_value('IDSBCBSKCRunCycle','')
FirstOfMonth = get_widget_value('FirstOfMonth','')
LastOfMonth = get_widget_value('LastOfMonth','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_PROV_MBR_RMBRMT_ELIG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT "
        "PPMRE.PCMH_PROV_MBR_RMBRMT_ELIG_SK,"
        "PPMRE.PROV_ID,"
        "PPMRE.MBR_UNIQ_KEY,"
        "PPMRE.AS_OF_YR_MO_SK,"
        "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,"
        "PPMRE.CRT_RUN_CYC_EXCTN_SK,"
        "PPMRE.LAST_UPDT_RUN_CYC_EXCTN_SK,"
        "PPMRE.CLM_SK,"
        "PPMRE.GRP_SK,"
        "PPMRE.MBR_SK,"
        "PPMRE.PROV_GRP_PROV_SK,"
        "PPMRE.PROV_SK,"
        "PPMRE.PROD_SH_NM_DLVRY_METH_CD_SK,"
        "PPMRE.ELIG_FOR_RMBRMT_IN,"
        "PPMRE.GRP_ELIG_IN,"
        "PPMRE.MBR_MED_COV_ACTV_IN,"
        "PPMRE.MBR_MED_COV_PRI_IN,"
        "PPMRE.MBR_1_ACTV_PCMH_PROV_IN,"
        "PPMRE.MBR_SEL_PCP_IN,"
        "PPMRE.PCMH_AUTO_ASG_PCP_IN,"
        "PPMRE.PROV_GRP_CLM_RQRMT_IN,"
        "PPMRE.MBR_UNIQ_KEY_ORIG_EFF_DT_SK,"
        "PPMRE.CLM_SVC_DT_SK,"
        "PPMRE.INDV_BE_KEY,"
        "PPMRE.MBR_ELIG_STRT_DT_SK,"
        "PPMRE.MBR_ELIG_END_DT_SK,"
        "PPMRE.CAP_SK,"
        "PPMRE.PAYMT_CMPL_DT_SK,"
        "PPMRE.PAYMT_EXCL_RSN_DESC "
        "FROM "
        + IDSOwner
        + ".PCMH_PROV_MBR_RMBRMT_ELIG PPMRE "
        "LEFT JOIN "
        + IDSOwner
        + ".CD_MPPNG CD "
        "ON  PPMRE.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
        "WHERE  ((CD.TRGT_CD = 'TREO' and PPMRE.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
        + IDSRunCycle
        + ") or (CD.TRGT_CD = 'BCBSKC' and PPMRE.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
        + IDSBCBSKCRunCycle
        + "))"
    )
    .load()
)

df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT "
        "CD_MPPNG_SK,"
        "COALESCE(TRGT_CD,'UNK') TRGT_CD,"
        "COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
        "FROM "
        + IDSOwner
        + ".CD_MPPNG"
    )
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_db2_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT "
        "GRP_SK,"
        "GRP_ID,"
        "GRP_NM "
        "FROM "
        + EDWOwner
        + ".GRP_D"
    )
    .load()
)

df_db2_PROV_D_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT DISTINCT "
        "PROV_D.PROV_SK,"
        "PROV_D.PROV_ID,"
        "PROV_D.PROV_NM "
        "FROM "
        + EDWOwner
        + ".PROV_D PROV_D ;"
    )
    .load()
)

df_db2_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT DISTINCT "
        "PROV.PROV_SK,"
        "PROV.PROV_NM "
        "FROM "
        + IDSOwner
        + ".PROV PROV;"
    )
    .load()
)

df_db2_PROD_SH_NM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT pmre.MBR_SK, "
        "       psn.PROD_SH_NM, "
        "       me.TERM_DT_SK "
        "FROM "
        + IDSOwner
        + ".CD_MPPNG map, "
        + IDSOwner
        + ".PCMH_PROV_MBR_RMBRMT_ELIG pmre, "
        + IDSOwner
        + ".MBR_ENR me, "
        + IDSOwner
        + ".PROD prod, "
        + IDSOwner
        + ".PROD_SH_NM psn "
        "Where "
        "pmre.MBR_SK = me.MBR_SK "
        "and me.PROD_SK=prod.PROD_SK "
        "and map.SRC_CD in ('BCBSKC', 'TREO') "
        "and map.CD_MPPNG_SK = pmre.SRC_SYS_CD_SK "
        "and prod.PROD_SH_NM_SK = psn.PROD_SH_NM_SK "
        "and me.TERM_DT_SK >= "
        + FirstOfMonth
        + " "
        "and me.EFF_DT_SK <= "
        + LastOfMonth
        + " "
        "and me.ELIG_IN = 'Y'"
    )
    .load()
)

df_Xfrm_PSN_intermediate = df_db2_PROD_SH_NM_In.withColumn(
    "svProdShNm",
    F.when(
        (F.col("PROD_SH_NM").isin("PC", "PCB", "BCARE", "BLUE-ACCESS", "BLUE-SELECT", "BLUESELECT+")),
        F.lit("1")
    ).otherwise(F.lit("2"))
)
df_xfrm_PSN = df_Xfrm_PSN_intermediate.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("svProdShNm").alias("DUMMY_COL")
)

df_filter_psn_in = df_xfrm_PSN.filter(
    (F.col("PROD_SH_NM") == "PC")
    | (F.col("PROD_SH_NM") == "PCB")
    | (F.col("PROD_SH_NM") == "BCARE")
    | (F.col("PROD_SH_NM") == "BLUE-ACCESS")
    | (F.col("PROD_SH_NM") == "BLUE-SELECT")
    | (F.col("PROD_SH_NM") == "BLUESELECT+")
)
df_filter_psn_not_in = df_xfrm_PSN.filter(
    ~(
        (F.col("PROD_SH_NM") == "PC")
        | (F.col("PROD_SH_NM") == "PCB")
        | (F.col("PROD_SH_NM") == "BCARE")
        | (F.col("PROD_SH_NM") == "BLUE-ACCESS")
        | (F.col("PROD_SH_NM") == "BLUE-SELECT")
        | (F.col("PROD_SH_NM") == "BLUESELECT+")
    )
)

df_dedup_psn1 = dedup_sort(
    df_filter_psn_in,
    partition_cols=["MBR_SK"],
    sort_cols=[("MBR_SK", "A"), ("TERM_DT_SK", "D"), ("PROD_SH_NM", "A")]
)
df_dedup_psn2 = dedup_sort(
    df_filter_psn_not_in,
    partition_cols=["MBR_SK"],
    sort_cols=[("MBR_SK", "A"), ("TERM_DT_SK", "D"), ("PROD_SH_NM", "A")]
)

df_dedup_psn1_sel = df_dedup_psn1.select(
    F.col("MBR_SK"),
    F.col("TERM_DT_SK"),
    F.col("PROD_SH_NM"),
    F.col("DUMMY_COL")
)
df_dedup_psn2_sel = df_dedup_psn2.select(
    F.col("MBR_SK"),
    F.col("TERM_DT_SK"),
    F.col("PROD_SH_NM"),
    F.col("DUMMY_COL")
)

df_fnl_Psn = df_dedup_psn1_sel.unionByName(df_dedup_psn2_sel)

df_dedup_pick_psn1_pre = dedup_sort(
    df_fnl_Psn,
    partition_cols=["MBR_SK"],
    sort_cols=[("MBR_SK", "A"), ("DUMMY_COL", "A")]
)
df_dedup_pick_psn1 = df_dedup_pick_psn1_pre.select(
    F.col("MBR_SK"),
    F.col("PROD_SH_NM")
)

df_lkup_codes_joined = (
    df_db2_PROV_MBR_RMBRMT_ELIG_in.alias("main")
    .join(
        df_db2_CD_MPPNG_In.alias("ref_cdma_codes"),
        F.col("main.PROD_SH_NM_DLVRY_METH_CD_SK") == F.col("ref_cdma_codes.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_GRP_In.alias("ref_grp_id"),
        F.col("main.GRP_SK") == F.col("ref_grp_id.GRP_SK"),
        "left"
    )
    .join(
        df_db2_PROV_D_In.alias("ref_Prov_grp_prov"),
        F.col("main.PROV_GRP_PROV_SK") == F.col("ref_Prov_grp_prov.PROV_SK"),
        "left"
    )
    .join(
        df_db2_PROV_In.alias("ref_Prov_nm"),
        F.col("main.PROV_SK") == F.col("ref_Prov_nm.PROV_SK"),
        "left"
    )
    .join(
        df_dedup_pick_psn1.alias("ref_ProdShNm"),
        F.col("main.MBR_SK") == F.col("ref_ProdShNm.MBR_SK"),
        "left"
    )
)

df_lkup_codes = df_lkup_codes_joined.select(
    F.col("main.PCMH_PROV_MBR_RMBRMT_ELIG_SK").alias("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
    F.col("main.PROV_ID").alias("PROV_ID"),
    F.col("main.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("main.AS_OF_YR_MO_SK").alias("AS_OF_YR_MO_SK"),
    F.col("main.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("main.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("main.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("main.CLM_SK").alias("CLM_SK"),
    F.col("main.GRP_SK").alias("GRP_SK"),
    F.col("main.MBR_SK").alias("MBR_SK"),
    F.col("main.PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
    F.col("main.PROV_SK").alias("PROV_SK"),
    F.col("main.PROD_SH_NM_DLVRY_METH_CD_SK").alias("PROD_SH_NM_DLVRY_METH_CD_SK"),
    F.col("main.ELIG_FOR_RMBRMT_IN").alias("ELIG_FOR_RMBRMT_IN"),
    F.col("main.GRP_ELIG_IN").alias("GRP_ELIG_IN"),
    F.col("main.MBR_MED_COV_ACTV_IN").alias("MBR_MED_COV_ACTV_IN"),
    F.col("main.MBR_MED_COV_PRI_IN").alias("MBR_MED_COV_PRI_IN"),
    F.col("main.MBR_1_ACTV_PCMH_PROV_IN").alias("MBR_1_ACTV_PCMH_PROV_IN"),
    F.col("main.MBR_SEL_PCP_IN").alias("MBR_SEL_PCP_IN"),
    F.col("main.PCMH_AUTO_ASG_PCP_IN").alias("PCMH_AUTO_ASG_PCP_IN"),
    F.col("main.PROV_GRP_CLM_RQRMT_IN").alias("PROV_GRP_CLM_RQRMT_IN"),
    F.col("main.MBR_UNIQ_KEY_ORIG_EFF_DT_SK").alias("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
    F.col("main.CLM_SVC_DT_SK").alias("CLM_SVC_DT_SK"),
    F.col("main.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("main.MBR_ELIG_STRT_DT_SK").alias("MBR_ELIG_STRT_DT_SK"),
    F.col("main.MBR_ELIG_END_DT_SK").alias("MBR_ELIG_END_DT_SK"),
    F.col("main.CAP_SK").alias("CAP_SK"),
    F.col("main.PAYMT_CMPL_DT_SK").alias("PAYMT_CMPL_DT_SK"),
    F.col("main.PAYMT_EXCL_RSN_DESC").alias("PAYMT_EXCL_RSN_DESC"),
    F.col("ref_cdma_codes.TRGT_CD").alias("PROD_SH_NM_DLVRY_METH_CD"),
    F.col("ref_cdma_codes.TRGT_CD_NM").alias("PROD_SH_NM_DLVRY_METH_NM"),
    F.col("ref_grp_id.GRP_ID").alias("GRP_ID"),
    F.col("ref_grp_id.GRP_NM").alias("GRP_NM"),
    F.col("ref_ProdShNm.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("ref_Prov_grp_prov.PROV_ID").alias("PROV_GRP_PROV_ID"),
    F.col("ref_Prov_grp_prov.PROV_NM").alias("PROV_GRP_PROV_NM"),
    F.col("ref_Prov_nm.PROV_NM").alias("PROV_NM")
)

df_xmb_main_in_pre = df_lkup_codes.filter(
    (F.col("PCMH_PROV_MBR_RMBRMT_ELIG_SK") != 0)
    & (F.col("PCMH_PROV_MBR_RMBRMT_ELIG_SK") != 1)
)

df_xmb_main_in = (
    df_xmb_main_in_pre
    .withColumn("SRC_SYS_CD", F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("PROD_SH_NM_DLVRY_METH_CD",
                F.when(F.col("PROD_SH_NM_DLVRY_METH_CD").isNull(), F.lit("UNK")).otherwise(F.col("PROD_SH_NM_DLVRY_METH_CD")))
    .withColumn("PROD_SH_NM_DLVRY_METH_NM",
                F.when(F.col("PROD_SH_NM_DLVRY_METH_NM").isNull(), F.lit("UNK")).otherwise(F.col("PROD_SH_NM_DLVRY_METH_NM")))
    .withColumn("GRP_ID", F.when((F.col("GRP_ID").isNull()) | (F.length(trim(F.col("GRP_ID"))) == 0), F.lit("UNK")).otherwise(F.col("GRP_ID")))
    .withColumn("GRP_NM", F.when((F.col("GRP_NM").isNull()) | (F.length(trim(F.col("GRP_NM"))) == 0), F.lit("UNK")).otherwise(F.col("GRP_NM")))
    .withColumn("PROD_SH_NM",
                F.when((F.col("PROD_SH_NM").isNull()) | (F.length(trim(F.col("PROD_SH_NM"))) == 0), F.lit("UNK ")).otherwise(F.col("PROD_SH_NM")))
    .withColumn("PROV_GRP_PROV_ID", F.when((F.col("PROV_GRP_PROV_ID").isNull()) | (F.length(trim(F.col("PROV_GRP_PROV_ID"))) == 0),
                                           F.lit("UNK")).otherwise(F.col("PROV_GRP_PROV_ID")))
    .withColumn("PROV_GRP_PROV_NM", F.when((F.col("PROV_GRP_PROV_NM").isNull()) | (F.length(trim(F.col("PROV_GRP_PROV_NM"))) == 0),
                                           F.lit("UNK")).otherwise(F.col("PROV_GRP_PROV_NM")))
    .withColumn("PROV_NM", F.when((F.col("PROV_NM").isNull()) | (F.length(trim(F.col("PROV_NM"))) == 0), F.lit("UNK")).otherwise(F.col("PROV_NM")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("CLM_ID", F.lit("NA"))
    .select(
        F.col("PCMH_PROV_MBR_RMBRMT_ELIG_SK"),
        F.col("PROV_ID"),
        F.col("MBR_UNIQ_KEY"),
        F.col("AS_OF_YR_MO_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLM_SK"),
        F.col("GRP_SK"),
        F.col("MBR_SK"),
        F.col("PROV_GRP_PROV_SK"),
        F.col("PROV_SK"),
        F.col("PROD_SH_NM_DLVRY_METH_CD"),
        F.col("PROD_SH_NM_DLVRY_METH_NM"),
        F.col("ELIG_FOR_RMBRMT_IN"),
        F.col("GRP_ELIG_IN"),
        F.col("MBR_MED_COV_ACTV_IN"),
        F.col("MBR_MED_COV_PRI_IN"),
        F.col("MBR_1_ACTV_PCMH_PROV_IN"),
        F.col("MBR_SEL_PCP_IN"),
        F.col("PCMH_AUTO_ASG_PCP_IN"),
        F.col("PROV_GRP_CLM_RQRMT_IN"),
        F.col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"),
        F.col("CLM_SVC_DT_SK"),
        F.col("INDV_BE_KEY"),
        F.col("CLM_ID"),
        F.col("GRP_ID"),
        F.col("GRP_NM"),
        F.col("PROD_SH_NM"),
        F.col("PROV_GRP_PROV_ID"),
        F.col("PROV_GRP_PROV_NM"),
        F.col("PROV_NM"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROD_SH_NM_DLVRY_METH_CD_SK"),
        F.col("MBR_ELIG_STRT_DT_SK"),
        F.col("MBR_ELIG_END_DT_SK"),
        F.col("CAP_SK"),
        F.col("PAYMT_CMPL_DT_SK"),
        F.col("PAYMT_EXCL_RSN_DESC")
    )
)

df_xmb_NALink_single = spark.createDataFrame(
    [
        (
            1, "NA", 1, "175301", "NA", "1753-01-01", "1753-01-01",
            1, 1, 1, 1, 1, "NA", "NA", "N", "N", "N", "N", "N", "N",
            "N", "N", "1753-01-01", "1753-01-01", 1, "NA", "NA", "NA",
            "NA", "NA", "NA", "NA", 100, 100, 100, 1, "1753-01-01", "1753-01-01",
            1, "1753-01-01", "NA"
        )
    ],
    [
        "PCMH_PROV_MBR_RMBRMT_ELIG_SK",
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "AS_OF_YR_MO_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "GRP_SK",
        "MBR_SK",
        "PROV_GRP_PROV_SK",
        "PROV_SK",
        "PROD_SH_NM_DLVRY_METH_CD",
        "PROD_SH_NM_DLVRY_METH_NM",
        "ELIG_FOR_RMBRMT_IN",
        "GRP_ELIG_IN",
        "MBR_MED_COV_ACTV_IN",
        "MBR_MED_COV_PRI_IN",
        "MBR_1_ACTV_PCMH_PROV_IN",
        "MBR_SEL_PCP_IN",
        "PCMH_AUTO_ASG_PCP_IN",
        "PROV_GRP_CLM_RQRMT_IN",
        "MBR_UNIQ_KEY_ORIG_EFF_DT_SK",
        "CLM_SVC_DT_SK",
        "INDV_BE_KEY",
        "CLM_ID",
        "GRP_ID",
        "GRP_NM",
        "PROD_SH_NM",
        "PROV_GRP_PROV_ID",
        "PROV_GRP_PROV_NM",
        "PROV_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROD_SH_NM_DLVRY_METH_CD_SK",
        "MBR_ELIG_STRT_DT_SK",
        "MBR_ELIG_END_DT_SK",
        "CAP_SK",
        "PAYMT_CMPL_DT_SK",
        "PAYMT_EXCL_RSN_DESC"
    ]
)
df_xmb_UNKLink_single = spark.createDataFrame(
    [
        (
            0, "UNK", 0, "175301", "UNK", "1753-01-01", "1753-01-01",
            0, 0, 0, 0, 0, "UNK", "UNK", "N", "N", "N", "N", "N", "N",
            "N", "N", "1753-01-01", "1753-01-01", 0, "UNK", "UNK", "UNK",
            "UNK", "UNK", "UNK", "UNK", 100, 100, 100, 0, "1753-01-01", "1753-01-01",
            0, "1753-01-01", "UNK"
        )
    ],
    [
        "PCMH_PROV_MBR_RMBRMT_ELIG_SK",
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "AS_OF_YR_MO_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "GRP_SK",
        "MBR_SK",
        "PROV_GRP_PROV_SK",
        "PROV_SK",
        "PROD_SH_NM_DLVRY_METH_CD",
        "PROD_SH_NM_DLVRY_METH_NM",
        "ELIG_FOR_RMBRMT_IN",
        "GRP_ELIG_IN",
        "MBR_MED_COV_ACTV_IN",
        "MBR_MED_COV_PRI_IN",
        "MBR_1_ACTV_PCMH_PROV_IN",
        "MBR_SEL_PCP_IN",
        "PCMH_AUTO_ASG_PCP_IN",
        "PROV_GRP_CLM_RQRMT_IN",
        "MBR_UNIQ_KEY_ORIG_EFF_DT_SK",
        "CLM_SVC_DT_SK",
        "INDV_BE_KEY",
        "CLM_ID",
        "GRP_ID",
        "GRP_NM",
        "PROD_SH_NM",
        "PROV_GRP_PROV_ID",
        "PROV_GRP_PROV_NM",
        "PROV_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROD_SH_NM_DLVRY_METH_CD_SK",
        "MBR_ELIG_STRT_DT_SK",
        "MBR_ELIG_END_DT_SK",
        "CAP_SK",
        "PAYMT_CMPL_DT_SK",
        "PAYMT_EXCL_RSN_DESC"
    ]
)

df_Fnl_All = (
    df_xmb_main_in.select(df_xmb_main_in.columns)
    .unionByName(df_xmb_NALink_single.select(df_xmb_main_in.columns))
    .unionByName(df_xmb_UNKLink_single.select(df_xmb_main_in.columns))
)

df_seq_PCMH_PROV_MBR_RMBRMT_ELIG_F_csv_load = df_Fnl_All.select(
    "PCMH_PROV_MBR_RMBRMT_ELIG_SK",
    "PROV_ID",
    "MBR_UNIQ_KEY",
    "AS_OF_YR_MO_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "GRP_SK",
    "MBR_SK",
    "PROV_GRP_PROV_SK",
    "PROV_SK",
    "PROD_SH_NM_DLVRY_METH_CD",
    "PROD_SH_NM_DLVRY_METH_NM",
    "ELIG_FOR_RMBRMT_IN",
    "GRP_ELIG_IN",
    "MBR_MED_COV_ACTV_IN",
    "MBR_MED_COV_PRI_IN",
    "MBR_1_ACTV_PCMH_PROV_IN",
    "MBR_SEL_PCP_IN",
    "PCMH_AUTO_ASG_PCP_IN",
    "PROV_GRP_CLM_RQRMT_IN",
    "MBR_UNIQ_KEY_ORIG_EFF_DT_SK",
    "CLM_SVC_DT_SK",
    "INDV_BE_KEY",
    "CLM_ID",
    "GRP_ID",
    "GRP_NM",
    "PROD_SH_NM",
    "PROV_GRP_PROV_ID",
    "PROV_GRP_PROV_NM",
    "PROV_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROD_SH_NM_DLVRY_METH_CD_SK",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "CAP_SK",
    "PAYMT_CMPL_DT_SK",
    "PAYMT_EXCL_RSN_DESC"
)

df_seq_PCMH_PROV_MBR_RMBRMT_ELIG_F_csv_load = df_seq_PCMH_PROV_MBR_RMBRMT_ELIG_F_csv_load\
.withColumn("AS_OF_YR_MO_SK", F.rpad(F.col("AS_OF_YR_MO_SK"), 6, " "))\
.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))\
.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))\
.withColumn("ELIG_FOR_RMBRMT_IN", F.rpad(F.col("ELIG_FOR_RMBRMT_IN"), 1, " "))\
.withColumn("GRP_ELIG_IN", F.rpad(F.col("GRP_ELIG_IN"), 1, " "))\
.withColumn("MBR_MED_COV_ACTV_IN", F.rpad(F.col("MBR_MED_COV_ACTV_IN"), 1, " "))\
.withColumn("MBR_MED_COV_PRI_IN", F.rpad(F.col("MBR_MED_COV_PRI_IN"), 1, " "))\
.withColumn("MBR_1_ACTV_PCMH_PROV_IN", F.rpad(F.col("MBR_1_ACTV_PCMH_PROV_IN"), 1, " "))\
.withColumn("MBR_SEL_PCP_IN", F.rpad(F.col("MBR_SEL_PCP_IN"), 1, " "))\
.withColumn("PCMH_AUTO_ASG_PCP_IN", F.rpad(F.col("PCMH_AUTO_ASG_PCP_IN"), 1, " "))\
.withColumn("PROV_GRP_CLM_RQRMT_IN", F.rpad(F.col("PROV_GRP_CLM_RQRMT_IN"), 1, " "))\
.withColumn("MBR_UNIQ_KEY_ORIG_EFF_DT_SK", F.rpad(F.col("MBR_UNIQ_KEY_ORIG_EFF_DT_SK"), 10, " "))\
.withColumn("CLM_SVC_DT_SK", F.rpad(F.col("CLM_SVC_DT_SK"), 10, " "))\
.withColumn("MBR_ELIG_STRT_DT_SK", F.rpad(F.col("MBR_ELIG_STRT_DT_SK"), 10, " "))\
.withColumn("MBR_ELIG_END_DT_SK", F.rpad(F.col("MBR_ELIG_END_DT_SK"), 10, " "))\
.withColumn("PAYMT_CMPL_DT_SK", F.rpad(F.col("PAYMT_CMPL_DT_SK"), 10, " "))

write_files(
    df_seq_PCMH_PROV_MBR_RMBRMT_ELIG_F_csv_load,
    f"{adls_path}/load/PCMH_PROV_MBR_RMBRMT_ELIG_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)