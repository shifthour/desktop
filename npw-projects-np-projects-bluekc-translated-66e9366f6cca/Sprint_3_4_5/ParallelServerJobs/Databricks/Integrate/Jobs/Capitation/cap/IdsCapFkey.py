# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                        Date               UserStory/Ticket                Change Description                                                                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    --------------------------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------   
# MAGIC Ralph Tucker                  5/04/2005                                             Originally programmed                                                                                                                                                                         
# MAGIC Parik                                2008-09-02     3567(Primary Key)           Added Source System Code SK as parameter                                          devlIDS                        Steph Goddard            09/10/2008
# MAGIC Ralph Tucker                  2011-03-09     TTR-1058                       Chaged FKey on Sched and Pool codes                                                  IntegrateNewDevl        SAndrew                      2011-04-11         
# MAGIC Rishi Reddy                     2011-06-20     4663                               Added GL_CAT_CD_SK Column                                                              IntegrateCurDevl          Brent Leland                07-12-2011
# MAGIC Santosh Bokka               2014-05-30      4917                               Added svSrcSysCd and changed logic in                                                 IntegrateNewDevl        Kalyan Neelam            2014-07-01
# MAGIC                                                                                                       svCapCatCdSk and  svCapLobCdSk                                                                                                                                     
# MAGIC Karthik Chintalapani        2014-09-09      4917                               Reverted the logic updated by Santosh                                                    IntegrateNewDevl        Kalyan Neelam           2014-09-11
# MAGIC                                                                                                       since there is a new entry for 'UNK' in CD_MPPNG table                                                                                                                                                      
# MAGIC Karthik Chintalapani        2015-10-19      5212                               Changed the logic for BCBSA to retrieve MBR_SK for BCBSA                 IntegrateDev1              Kalyan Neelam           2015-10-21
# MAGIC Manasa Andru                2018-08-20      60037                             Added LFSTYL_RATE_FCTR_SK at the end of the file.                          IntegrateDev2              Abhiram Dasarathy     2018-08-24
# MAGIC                                                                Attrbtn/Capitn Support                                                                                                                                                                                                     
# MAGIC Tejaswi Gogineni            2018-10-23      INC0473770                   Changed the Datatype for  SEQ_NO across the job to match it                IntegrateDev1              Hugh Sisson               2018-12-26
# MAGIC                                                                                                       with the table datatype to get rid of warnings.            
# MAGIC 
# MAGIC Sharon Andrew        2021-02-01       LHO Medicare Advantage      Changed source system code used in foreign key and                          IntegrateDev2               Kalyan Neelam          2021-02-25
# MAGIC                                                                                                       code lookups to account for LHO

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from Primary Key job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


TmpOutFile = get_widget_value("TmpOutFile","")
InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
CurrRunCyc = get_widget_value("CurrRunCyc","")

schema_CapCrfIn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CAP_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("CAP_PROV_ID", StringType(), False),
    StructField("ERN_DT_SK", StringType(), False),
    StructField("PD_DT_SK", StringType(), False),
    StructField("CAP_FUND_ID", StringType(), False),
    StructField("CAP_POOL_CD", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXTCN_SK", IntegerType(), False),
    StructField("CAP_FUND_SK", IntegerType(), False),
    StructField("CAP_PROV_SK", IntegerType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("FNCL_LOB_ID", StringType(), False),
    StructField("GRP_CK", StringType(), False),
    StructField("GRGR_ID", StringType(), False),
    StructField("MBR_CK", StringType(), False),
    StructField("NTWK_ID", StringType(), False),
    StructField("PD_PROV_ID", StringType(), False),
    StructField("PCP_PROV_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("CAP_ADJ_RSN_CD", StringType(), False),
    StructField("CAP_ADJ_STTUS_CD", StringType(), False),
    StructField("CAP_ADJ_TYP_CD", StringType(), False),
    StructField("CAP_CAT_CD", StringType(), False),
    StructField("CAP_COPAY_TYP_CD", StringType(), False),
    StructField("CAP_LOB_CD", StringType(), False),
    StructField("CAP_PERD_CD", StringType(), False),
    StructField("CAP_SCHD_CD", StringType(), False),
    StructField("CAP_TYP_CD", StringType(), False),
    StructField("ADJ_AMT", DecimalType(38,10), False),
    StructField("CAP_AMT", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("FUND_RATE_AMT", DecimalType(38,10), False),
    StructField("MBR_AGE", DecimalType(38,10), False),
    StructField("MBR_MO_CT", DecimalType(38,10), False),
    StructField("SBSB_CK", StringType(), False),
    StructField("CRFD_ACCT_CAT", StringType(), False)
])

full_path_in = f"{adls_path}/key/{InFile}"
df_CapCrfIn = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_CapCrfIn)
    .load(full_path_in)
)

df_ss = (
    df_CapCrfIn
    .withColumn(
        "svSrcSysCd",
        F.when(
            F.trim(F.col("SRC_SYS_CD")).isin("BCBSKC","BCBSA","LUMERIS","FACETS"),
            F.lit("FACETS")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svErnDtSk", GetFkeyDate(F.lit("IDS"), F.col("CAP_SK"), F.col("ERN_DT_SK"), F.lit(Logging)))
    .withColumn("svPdDtSk", GetFkeyDate(F.lit("IDS"), F.col("CAP_SK"), F.col("PD_DT_SK"), F.lit(Logging)))
    .withColumn(
        "svCapFundSk",
        F.when(
            F.col("SRC_SYS_CD") == F.lit("BCBSA"),
            F.col("CAP_FUND_SK")
        ).when(
            F.col("SRC_SYS_CD") == F.lit("LUMERIS"),
            GetFkeyCapFund(F.lit("LUMERIS"), F.col("CAP_SK"), F.trim(F.col("CAP_FUND_ID")), F.lit(Logging))
        ).otherwise(
            GetFkeyCapFund(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("CAP_FUND_ID")), F.lit(Logging))
        )
    )
    .withColumn("svGrpSk", GetFkeyGrp(F.col("svSrcSysCd"), F.col("CAP_SK"), F.col("GRGR_ID"), F.lit(Logging)))
    .withColumn(
        "svCapPoolCdSk",
        F.when(
            F.trim(F.col("SRC_SYS_CD")) == F.lit("LUMERIS"),
            GetFkeyCapPool(F.lit("LUMERIS"), F.col("CAP_SK"), F.col("CAP_POOL_CD"), F.lit(Logging))
        ).otherwise(
            GetFkeyCapPool(F.lit("FACETS"), F.col("CAP_SK"), F.col("CAP_POOL_CD"), F.lit(Logging))
        )
    )
    .withColumn(
        "svCapProvSk",
        F.when(
            F.col("SRC_SYS_CD") == F.lit("LUMERIS"),
            F.when(
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("CAP_PROV_ID")), F.lit(Logging)) != F.lit(0),
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("CAP_PROV_ID")), F.lit(Logging))
            ).otherwise(
                GetFkeyProv(F.lit("LUMERIS"), F.col("CAP_SK"), F.trim(F.col("CAP_PROV_ID")), F.lit(Logging))
            )
        ).otherwise(
            GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("CAP_PROV_ID")), F.lit(Logging))
        )
    )
    .withColumn("svClsSk", GetFkeyCls(F.col("svSrcSysCd"), F.col("CAP_SK"), F.col("GRGR_ID"), F.col("CLS_ID"), F.lit(Logging)))
    .withColumn("svClsPlnSk", GetFkeyClsPln(F.col("svSrcSysCd"), F.col("CAP_SK"), F.col("CLS_PLN_ID"), F.lit(Logging)))
    .withColumn("svFnclLobSk", GetFkeyFnclLob(F.lit("PSI"), F.col("CAP_SK"), F.col("FNCL_LOB_ID"), F.lit(Logging)))
    .withColumn(
        "svMbrSk",
        F.when(
            F.col("MBR_CK") == F.lit("1"),
            F.lit("1")
        ).otherwise(
            GetFkeyMbr(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("MBR_CK")), F.lit(Logging))
        )
    )
    .withColumn("svNtwkSk", GetFkeyNtwk(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("NTWK_ID")), F.lit(Logging)))
    .withColumn(
        "svPdProvSk",
        F.when(
            F.col("SRC_SYS_CD") == F.lit("LUMERIS"),
            F.when(
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PD_PROV_ID")), F.lit(Logging)) != F.lit(0),
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PD_PROV_ID")), F.lit(Logging))
            ).otherwise(
                GetFkeyProv(F.lit("LUMERIS"), F.col("CAP_SK"), F.trim(F.col("PD_PROV_ID")), F.lit(Logging))
            )
        ).otherwise(
            GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PD_PROV_ID")), F.lit(Logging))
        )
    )
    .withColumn(
        "svPcpProvSk",
        F.when(
            F.col("SRC_SYS_CD") == F.lit("LUMERIS"),
            F.when(
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PCP_PROV_ID")), F.lit(Logging)) != F.lit(0),
                GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PCP_PROV_ID")), F.lit(Logging))
            ).otherwise(
                GetFkeyProv(F.lit("LUMERIS"), F.col("CAP_SK"), F.trim(F.col("PCP_PROV_ID")), F.lit(Logging))
            )
        ).otherwise(
            GetFkeyProv(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("PCP_PROV_ID")), F.lit(Logging))
        )
    )
    .withColumn("svProdSk", GetFkeyProd(F.lit("FACETS"), F.col("CAP_SK"), F.trim(F.col("PROD_ID")), F.lit(Logging)))
    .withColumn("svSubGrpSk", GetFkeySubgrp(F.col("svSrcSysCd"), F.col("CAP_SK"), F.trim(F.col("GRGR_ID")), F.trim(F.col("SUBGRP_ID")), F.lit(Logging)))
    .withColumn("svSubSk", GetFkeySub(F.lit("FACETS"), F.col("CAP_SK"), F.trim(F.col("SBSB_CK")), F.lit(Logging)))
    .withColumn("svCapAdjRsnCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CAP_SK"), F.lit("CAPITATION ADJUSTMENT REASON"), F.trim(F.col("CAP_ADJ_RSN_CD")), F.lit(Logging)))
    .withColumn("svCapAdjSttusCdSk", GetFkeyCodes(F.lit("FACETS"), F.col("CAP_SK"), F.lit("CAPITATION ADJUSTMENT STATUS"), F.trim(F.col("CAP_ADJ_STTUS_CD")), F.lit(Logging)))
    .withColumn("svCapAdjTypCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CAP_SK"), F.lit("CAPITATION ADJUSTMENT METHOD"), F.trim(F.col("CAP_ADJ_TYP_CD")), F.lit(Logging)))
    .withColumn(
        "svCapCatCdSk",
        F.when(
            F.trim(F.col("SRC_SYS_CD")) == F.lit("BCBSKC"),
            F.col("SrcSysCdSk")
        ).when(
            F.trim(F.col("SRC_SYS_CD")) == F.lit("LUMERIS"),
            GetFkeyCodes(F.lit("LUMERIS"), F.col("CAP_SK"), F.lit("CAPITATION CATEGORY"), F.trim(F.col("CAP_CAT_CD")), F.lit(Logging))
        ).otherwise(
            GetFkeyCodes(F.col("svSrcSysCd"), F.col("CAP_SK"), F.lit("CAPITATION CATEGORY"), F.trim(F.col("CAP_CAT_CD")), F.lit(Logging))
        )
    )
    .withColumn("svCopayTypCdSk", GetFkeyCodes(F.col("svSrcSysCd"), F.col("CAP_SK"), F.lit("CAPITATION COPAYMENT TYPE"), F.trim(F.col("CAP_COPAY_TYP_CD")), F.lit(Logging)))
    .withColumn("svCapLobCdSk", GetFkeyCodes(F.lit("FACETS"), F.col("CAP_SK"), F.lit("CLAIM LINE LOB"), F.trim(F.col("CAP_LOB_CD")), F.lit(Logging)))
    .withColumn("svCapPerdCdSk", GetFkeyCodes(F.lit("FACETS"), F.col("CAP_SK"), F.lit("CAPITATION PERIOD"), F.trim(F.col("CAP_PERD_CD")), F.lit(Logging)))
    .withColumn("svCapSchdCdSk", GetFkeyCapSched(F.lit("FACETS"), F.col("CAP_SK"), F.col("CAP_SCHD_CD"), F.lit(Logging)))
    .withColumn("svCapTypCd", GetFkeyCodes(F.lit("FACETS"), F.col("CAP_SK"), F.lit("CAPITATION ADJUSTMENT TYPE"), F.trim(F.col("CAP_TYP_CD")), F.lit(Logging)))
    .withColumn(
        "svGlCatCdSk",
        F.when(
            F.col("SRC_SYS_CD") == F.lit("LUMERIS"),
            GetFkeyCodes(F.lit("LUMERIS"), F.col("CAP_SK"), F.lit("GENERAL LEDGER CATEGORY"), F.trim(F.col("CRFD_ACCT_CAT")), F.lit(Logging))
        ).otherwise(
            GetFkeyCodes(F.col("svSrcSysCd"), F.col("CAP_SK"), F.lit("GENERAL LEDGER CATEGORY"), F.trim(F.col("CRFD_ACCT_CAT")), F.lit(Logging))
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CAP_SK")))
)

df_CapFkeyOut = (
    df_ss
    .select(
        F.col("CAP_SK").alias("CAP_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("CAP_PROV_ID").alias("CAP_PROV_ID"),
        F.col("svErnDtSk").alias("ERN_DT_SK"),
        F.col("svPdDtSk").alias("PD_DT_SK"),
        F.col("CAP_FUND_ID").alias("CAP_FUND_ID"),
        F.col("svCapPoolCdSk").alias("CAP_POOL_CD_SK"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svCapFundSk").alias("CAP_FUND_SK"),
        F.col("svCapProvSk").alias("CAP_PROV_SK"),
        F.col("svClsSk").alias("CLS_SK"),
        F.col("svClsPlnSk").alias("CLS_PLN_SK"),
        F.col("svFnclLobSk").alias("FNCL_LOB_SK"),
        F.col("svGrpSk").alias("GRP_SK"),
        F.when(F.trim(F.col("SRC_SYS_CD")) == F.lit("BCBSA"), F.col("MBR_CK")).otherwise(F.col("svMbrSk")).alias("MBR_SK"),
        F.col("svNtwkSk").alias("NTWK_SK"),
        F.col("svPdProvSk").alias("PD_PROV_SK"),
        F.col("svPcpProvSk").alias("PCP_PROV_SK"),
        F.col("svProdSk").alias("PROD_SK"),
        F.col("svSubGrpSk").alias("SUBGRP_SK"),
        F.col("svSubSk").alias("SUB_SK"),
        F.col("svCapAdjRsnCdSk").alias("CAP_ADJ_RSN_CD_SK"),
        F.col("svCapAdjSttusCdSk").alias("CAP_ADJ_STTUS_CD_SK"),
        F.col("svCapAdjTypCdSk").alias("CAP_ADJ_TYP_CD_SK"),
        F.col("svCapCatCdSk").alias("CAP_CAT_CD_SK"),
        F.col("svCopayTypCdSk").alias("CAP_COPAY_TYP_CD_SK"),
        F.col("svCapLobCdSk").alias("CAP_LOB_CD_SK"),
        F.col("svCapPerdCdSk").alias("CAP_PERD_CD_SK"),
        F.col("svCapSchdCdSk").alias("CAP_SCHD_CD_SK"),
        F.col("svCapTypCd").alias("CAP_TYP_CD_SK"),
        F.col("ADJ_AMT").alias("ADJ_AMT"),
        F.col("CAP_AMT").alias("CAP_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("MBR_AGE").alias("MBR_AGE"),
        F.col("MBR_MO_CT").alias("MBR_MO_CT"),
        F.col("svGlCatCdSk").alias("GL_CAT_CD_SK"),
        F.lit("").alias("LFSTYL_RATE_FCTR_SK")
    )
)

df_lnkRecycle_unfiltered = (
    df_ss
    .select(
        F.when(True, GetRecycleKey(F.col("CAP_SK"))).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CAP_SK").alias("CAP_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("CAP_PROV_ID").alias("CAP_PROV_ID"),
        F.col("ERN_DT_SK").alias("ERN_DT_SK"),
        F.col("PD_DT_SK").alias("PD_DT_SK"),
        F.col("CAP_FUND_ID").alias("CAP_FUND_ID"),
        F.col("CAP_POOL_CD").alias("CAP_POOL_CD"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("PD_PROV_ID").alias("CAP_FUND_SK"),
        F.col("CAP_PROV_SK").alias("CAP_PROV_SK"),
        F.col("CLS_ID").alias("CLS_ID"),
        F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("FNCL_LOB_ID").alias("FNCL_LOB_ID"),
        F.col("GRP_CK").alias("GRP_CK"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("MBR_CK").alias("MBR_CK"),
        F.col("NTWK_ID").alias("NTWK_ID"),
        F.col("PD_PROV_ID").alias("PD_PROV_ID"),
        F.col("PCP_PROV_ID").alias("PCP_PROV_ID"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("CAP_ADJ_RSN_CD").alias("CAP_ADJ_RSN_CD"),
        F.col("CAP_ADJ_STTUS_CD").alias("CAP_ADJ_STTUS_CD"),
        F.col("CAP_ADJ_TYP_CD").alias("CAP_ADJ_TYP_CD"),
        F.col("CAP_CAT_CD").alias("CAP_CAT_CD"),
        F.col("CAP_COPAY_TYP_CD").alias("CAP_COPAY_TYP_CD"),
        F.col("CAP_LOB_CD").alias("CAP_LOB_CD"),
        F.col("CAP_PERD_CD").alias("CAP_PERD_CD"),
        F.col("CAP_SCHD_CD").alias("CAP_SCHD_CD"),
        F.col("CAP_TYP_CD").alias("CAP_TYP_CD"),
        F.col("ADJ_AMT").alias("ADJ_AMT"),
        F.col("CAP_AMT").alias("CAP_AMT"),
        F.col("COPAY_AMT").alias("COPAY_AMT"),
        F.col("FUND_RATE_AMT").alias("FUND_RATE_AMT"),
        F.col("MBR_AGE").alias("MBR_AGE"),
        F.col("MBR_MO_CT").alias("MBR_MO_CT"),
        F.col("CRFD_ACCT_CAT").alias("GL_CAT_CD_SK"),
        F.lit("").alias("LFSTYL_RATE_FCTR_SK")
    )
)

df_lnkRecycle = df_lnkRecycle_unfiltered.filter(F.col("ErrCount") > 0)

df_DefaultUNK_base = df_ss.limit(1)
df_DefaultNA_base = df_ss.limit(1)

df_DefaultUNK = (
    df_DefaultUNK_base
    .select(
        F.lit("0").alias("CAP_SK"),
        F.lit("0").alias("SRC_SYS_CD_SK"),
        F.lit("0").alias("MBR_UNIQ_KEY"),
        F.lit("UNK").alias("CAP_PROV_ID"),
        F.lit("UNK").alias("ERN_DT_SK"),
        F.lit("UNK").alias("PD_DT_SK"),
        F.lit("UNK").alias("CAP_FUND_ID"),
        F.lit("0").alias("CAP_POOL_CD_SK"),
        F.lit("1").alias("SEQ_NO"),
        F.col("CurrRunCyc").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CurrRunCyc").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("0").alias("CAP_FUND_SK"),
        F.lit("0").alias("CAP_PROV_SK"),
        F.lit("0").alias("CLS_SK"),
        F.lit("0").alias("CLS_PLN_SK"),
        F.lit("0").alias("FNCL_LOB_SK"),
        F.lit("0").alias("GRP_SK"),
        F.lit("0").alias("MBR_SK"),
        F.lit("0").alias("NTWK_SK"),
        F.lit("0").alias("PD_PROV_SK"),
        F.lit("0").alias("PCP_PROV_SK"),
        F.lit("0").alias("PROD_SK"),
        F.lit("0").alias("SUBGRP_SK"),
        F.lit("0").alias("SUB_SK"),
        F.lit("0").alias("CAP_ADJ_RSN_CD_SK"),
        F.lit("0").alias("CAP_ADJ_STTUS_CD_SK"),
        F.lit("0").alias("CAP_ADJ_TYP_CD_SK"),
        F.lit("0").alias("CAP_CAT_CD_SK"),
        F.lit("0").alias("CAP_COPAY_TYP_CD_SK"),
        F.lit("0").alias("CAP_LOB_CD_SK"),
        F.lit("0").alias("CAP_PERD_CD_SK"),
        F.lit("0").alias("CAP_SCHD_CD_SK"),
        F.lit("0").alias("CAP_TYP_CD_SK"),
        F.lit("0").alias("ADJ_AMT"),
        F.lit("0").alias("CAP_AMT"),
        F.lit("0").alias("COPAY_AMT"),
        F.lit("0").alias("FUND_RATE_AMT"),
        F.lit("0").alias("MBR_AGE"),
        F.lit("0").alias("MBR_MO_CT"),
        F.lit("0").alias("GL_CAT_CD_SK"),
        F.lit("0").alias("LFSTYL_RATE_FCTR_SK")
    )
)

df_DefaultNA = (
    df_DefaultNA_base
    .select(
        F.lit("1").alias("CAP_SK"),
        F.lit("1").alias("SRC_SYS_CD_SK"),
        F.lit("1").alias("MBR_UNIQ_KEY"),
        F.lit("NA").alias("CAP_PROV_ID"),
        F.lit("NA").alias("ERN_DT_SK"),
        F.lit("NA").alias("PD_DT_SK"),
        F.lit("NA").alias("CAP_FUND_ID"),
        F.lit("1").alias("CAP_POOL_CD_SK"),
        F.lit("1").alias("SEQ_NO"),
        F.col("CurrRunCyc").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CurrRunCyc").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("1").alias("CAP_FUND_SK"),
        F.lit("1").alias("CAP_PROV_SK"),
        F.lit("1").alias("CLS_SK"),
        F.lit("1").alias("CLS_PLN_SK"),
        F.lit("1").alias("FNCL_LOB_SK"),
        F.lit("1").alias("GRP_SK"),
        F.lit("1").alias("MBR_SK"),
        F.lit("1").alias("NTWK_SK"),
        F.lit("1").alias("PD_PROV_SK"),
        F.lit("1").alias("PCP_PROV_SK"),
        F.lit("1").alias("PROD_SK"),
        F.lit("1").alias("SUBGRP_SK"),
        F.lit("1").alias("SUB_SK"),
        F.lit("1").alias("CAP_ADJ_RSN_CD_SK"),
        F.lit("1").alias("CAP_ADJ_STTUS_CD_SK"),
        F.lit("1").alias("CAP_ADJ_TYP_CD_SK"),
        F.lit("1").alias("CAP_CAT_CD_SK"),
        F.lit("1").alias("CAP_COPAY_TYP_CD_SK"),
        F.lit("1").alias("CAP_LOB_CD_SK"),
        F.lit("1").alias("CAP_PERD_CD_SK"),
        F.lit("1").alias("CAP_SCHD_CD_SK"),
        F.lit("1").alias("CAP_TYP_CD_SK"),
        F.lit("0").alias("ADJ_AMT"),
        F.lit("0").alias("CAP_AMT"),
        F.lit("0").alias("COPAY_AMT"),
        F.lit("0").alias("FUND_RATE_AMT"),
        F.lit("0").alias("MBR_AGE"),
        F.lit("0").alias("MBR_MO_CT"),
        F.lit("1").alias("GL_CAT_CD_SK"),
        F.lit("1").alias("LFSTYL_RATE_FCTR_SK")
    )
)

df_Collector = df_CapFkeyOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_lnkRecycle_char_map = [
    ("INSRT_UPDT_CD", 10), ("DISCARD_IN", 1), ("PASS_THRU_IN", 1),
    ("CAP_PROV_ID", 10), ("ERN_DT_SK", 10), ("PD_DT_SK", 10), ("CAP_FUND_ID", 10),
    ("CLS_ID", 10), ("CLS_PLN_ID", 10), ("FNCL_LOB_ID", 10), ("GRP_CK", 10), ("GRGR_ID", 10),
    ("MBR_CK", 10), ("NTWK_ID", 10), ("PD_PROV_ID", 12), ("PCP_PROV_ID", 12), ("PROD_ID", 12),
    ("SUBGRP_ID", 12), ("SUB_ID", 20), ("CAP_ADJ_RSN_CD", 10), ("CAP_ADJ_STTUS_CD", 10),
    ("CAP_ADJ_TYP_CD", 10), ("CAP_CAT_CD", 10), ("CAP_COPAY_TYP_CD", 10), ("CAP_LOB_CD", 10),
    ("CAP_PERD_CD", 10), ("CAP_SCHD_CD", 10), ("CAP_TYP_CD", 10), ("GL_CAT_CD_SK", 4)
]
df_lnkRecycle_order = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","CAP_SK","MBR_UNIQ_KEY","CAP_PROV_ID","ERN_DT_SK","PD_DT_SK","CAP_FUND_ID",
    "CAP_POOL_CD","SEQ_NO","CRT_RUN_CYC_EXTCN_SK","LAST_UPDT_RUN_CYC_EXTCN_SK","CAP_FUND_SK","CAP_PROV_SK","CLS_ID",
    "CLS_PLN_ID","FNCL_LOB_ID","GRP_CK","GRGR_ID","MBR_CK","NTWK_ID","PD_PROV_ID","PCP_PROV_ID","PROD_ID",
    "SUBGRP_ID","SUB_ID","CAP_ADJ_RSN_CD","CAP_ADJ_STTUS_CD","CAP_ADJ_TYP_CD","CAP_CAT_CD","CAP_COPAY_TYP_CD",
    "CAP_LOB_CD","CAP_PERD_CD","CAP_SCHD_CD","CAP_TYP_CD","ADJ_AMT","CAP_AMT","COPAY_AMT","FUND_RATE_AMT","MBR_AGE",
    "MBR_MO_CT","GL_CAT_CD_SK","LFSTYL_RATE_FCTR_SK"
]
df_lnkRecycle_padded = df_lnkRecycle
for col_name, length_val in df_lnkRecycle_char_map:
    df_lnkRecycle_padded = df_lnkRecycle_padded.withColumn(col_name, F.rpad(F.col(col_name), length_val, " "))
df_lnkRecycle_final = df_lnkRecycle_padded.select(df_lnkRecycle_order)

write_files(
    df_lnkRecycle_final,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

char_map_final = [
    ("CAP_PROV_ID", 10), ("ERN_DT_SK", 10), ("PD_DT_SK", 10),
    ("CAP_FUND_ID", 10), ("CAP_CAT_CD_SK", 10), ("CAP_COPAY_TYP_CD_SK", 10),
    ("CAP_LOB_CD_SK", 10), ("CAP_POOL_CD_SK", 10), ("CLS_SK", 10), ("CLS_PLN_SK", 10),
    ("FNCL_LOB_SK", 10), ("GRP_SK", 10), ("MBR_SK", 10), ("NTWK_SK", 10),
    ("PD_PROV_SK", 10), ("PCP_PROV_SK", 10), ("PROD_SK", 10), ("SUBGRP_SK", 10),
    ("SUB_SK", 10), ("LFSTYL_RATE_FCTR_SK", 10), ("SRC_SYS_CD_SK", 10)
]
collector_order = [
    "CAP_SK","SRC_SYS_CD_SK","MBR_UNIQ_KEY","CAP_PROV_ID","ERN_DT_SK","PD_DT_SK","CAP_FUND_ID",
    "CAP_POOL_CD_SK","SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CAP_FUND_SK",
    "CAP_PROV_SK","CLS_SK","CLS_PLN_SK","FNCL_LOB_SK","GRP_SK","MBR_SK","NTWK_SK","PD_PROV_SK",
    "PCP_PROV_SK","PROD_SK","SUBGRP_SK","SUB_SK","CAP_ADJ_RSN_CD_SK","CAP_ADJ_STTUS_CD_SK",
    "CAP_ADJ_TYP_CD_SK","CAP_CAT_CD_SK","CAP_COPAY_TYP_CD_SK","CAP_LOB_CD_SK","CAP_PERD_CD_SK",
    "CAP_SCHD_CD_SK","CAP_TYP_CD_SK","ADJ_AMT","CAP_AMT","COPAY_AMT","FUND_RATE_AMT","MBR_AGE",
    "MBR_MO_CT","GL_CAT_CD_SK","LFSTYL_RATE_FCTR_SK"
]
df_collector_padded = df_Collector
for c_name, c_len in char_map_final:
    df_collector_padded = df_collector_padded.withColumn(c_name, F.rpad(F.col(c_name), c_len, " "))
df_collector_final = df_collector_padded.select(collector_order)

write_files(
    df_collector_final,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)