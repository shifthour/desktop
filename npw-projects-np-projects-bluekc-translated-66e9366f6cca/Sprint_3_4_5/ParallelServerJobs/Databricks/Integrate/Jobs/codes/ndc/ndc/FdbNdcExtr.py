# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_2 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 04/11/07 11:18:19 Batch  14346_40703 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/22/07 14:08:49 Batch  14326_50941 INIT bckcett devlIDS30 u10157 Task Tracker 69 and 4580
# MAGIC ^1_1 05/23/06 11:51:34 Batch  14023_42698 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_11 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 12/05/05 10:59:01 Batch  13854_39546 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_10 11/30/05 16:50:19 Batch  13849_60624 INIT bckcett devlIDS30 u05779 bj
# MAGIC 
# MAGIC  7;hf_ndc_gcn_link;hf_ndc_ahfs_link;hf_ndc_tcc_link;hf_ndc_lblr_link;hf_ndc_dsm_drug_typ;hf_ndc_gnn;hf_ndc_route_link
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    FdbNdcExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from files provided by First Data Bank.  NDC (National Drug Code) is a standard code.  grams will probably need to be rewritten when the new source
# MAGIC   is found. Transforms the data in common record format and runs primary key
# MAGIC 
# MAGIC    
# MAGIC INPUTS:
# MAGIC 	 #FilePath#/landing
# MAGIC                        gcnlink.dat
# MAGIC                        ahfslink.dat
# MAGIC                        tcclink.dat
# MAGIC                        ndc.dat
# MAGIC                        lblrlink.dat
# MAGIC                        gnn.dat
# MAGIC                 IDS NDC
# MAGIC                
# MAGIC   
# MAGIC HASH FILES:   6;hf_ndc_gcn_link;hf_ndc_ahfs_link;hf_ndc_tcc_link;hf_ndc_lblr_link;hf_ndc_dsm_drug_typ;hf_ndc_gnn
# MAGIC                         primary key hash file hf_ndc
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                   FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                    Convert NDC data provided by First Data Bank and process through primary key
# MAGIC 
# MAGIC                 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( TmpOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard    08/2004-   Originally Programmed
# MAGIC              BJ Luce               06/2005 - rewritting of CdsNdcExtr to use new data source from First Data Bank
# MAGIC              BJ Luce               10/31/2005 - add new fields
# MAGIC              BJ Luce               04/25/2006    trim mgf name on lblr input so that it is null instead of space
# MAGIC                                                                 change to environment parameters
# MAGIC              Bhoomi              03/12/2007 - Keeping Date format intact and checking "If date is blank, null or zero's using the Sk for 1753-01-01"
# MAGIC                                                                (AVG_WHLSL_PRICE_CHG_DT_SK, GNRC_PRICE_INCHG_DT_SK, SRC_NDC_CRT_DT_SK, SRC_NDC_UPDT_DT_SK)
# MAGIC              Bhoomi              03/12/2007 - Keeping Date format intact and checking "If date is blank, null or zero's using the Sk for 2199-12-31"
# MAGIC                                                                (OBSLT_DT_SK)
# MAGIC              Bhoomi              03/28/2007 - Adding a new field NDC_RTE_TYP_CD_SK             code walkthru by Steph Goddard  4/5/07
# MAGIC             
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              4/17/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard            09/27/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                 
# MAGIC 
# MAGIC Hari Krishna Raoi Yadav  05/18/2021     US- 382322                  updated the derivaion logic in Stage BusinessRules  IntegrateSITF           Jeyaprasanna             2021-05-23
# MAGIC                                                                                                       for Column GCN_CD

# MAGIC Pulling NDC information from First Data Bank
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC NEVER DROP THE DATA , AS THERE WOULD BE CDS UPDATES
# MAGIC Writing Sequential File to /key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

# Read from dummy table for hf_ndc_lkup (Scenario B)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_hf_ndc_lkup = f"SELECT NDC_CD, CRT_RUN_CYC_EXCTN_SK, NDC_CD_SK FROM {IDSOwner}.dummy_hf_ndc"
df_hf_ndc_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_hf_ndc_lkup)
    .load()
)

# --------------------------------------------------------------------------------
# link_data stage (CSeqFileStage)
# --------------------------------------------------------------------------------
# gcn_link
schema_gcn_link = StructType([
    StructField("GCN_SEQNO", StringType(), nullable=False),
    StructField("GCN", StringType(), nullable=False)
])
df_gcn_link = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_gcn_link)
    .csv(f"{adls_path_raw}/landing/gcnlink.dat")
)

# ahfs_link
schema_ahfs_link = StructType([
    StructField("GCN_SEQNO", StringType(), nullable=False),
    StructField("AHFS", StringType(), nullable=False),
    StructField("AHFS_REL", StringType(), nullable=False)
])
df_ahfs_link = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ahfs_link)
    .csv(f"{adls_path_raw}/landing/ahfslink.dat")
)

# tcclink
schema_tcc_link = StructType([
    StructField("GCN_SEQNO", StringType(), nullable=False),
    StructField("HIC3", StringType(), nullable=False),
    StructField("HICL_SEQQNO", StringType(), nullable=False),
    StructField("GCDF", StringType(), nullable=False),
    StructField("GCRT", StringType(), nullable=False),
    StructField("STR", StringType(), nullable=False),
    StructField("GTC", StringType(), nullable=False),
    StructField("TC", StringType(), nullable=False),
    StructField("DCC", StringType(), nullable=False),
    StructField("GCNSEQ_GI", StringType(), nullable=False),
    StructField("GENDER", StringType(), nullable=False),
    StructField("HIC3_SEQN", StringType(), nullable=False),
    StructField("STR60", StringType(), nullable=False)
])
df_tcc_link = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_tcc_link)
    .csv(f"{adls_path_raw}/landing/tcclink.dat")
)

# lblr_link
# The stage declared MFG as varchar and LBLRID, LBLRIND as char
schema_lblr_link = StructType([
    StructField("LBLRID", StringType(), nullable=False),
    StructField("MFG", StringType(), nullable=False),
    StructField("LBLRIND", StringType(), nullable=False)
])
df_lblr_link = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_lblr_link)
    .csv(f"{adls_path_raw}/landing/lblrlink.dat")
)

# gnn
schema_gnn = StructType([
    StructField("HICL_SEQNO", StringType(), nullable=False),
    StructField("GNN", StringType(), nullable=False),
    StructField("GNN60", StringType(), nullable=False)
])
df_gnn = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_gnn)
    .csv(f"{adls_path_raw}/landing/gnn.dat")
)

# route_link
schema_route_link = StructType([
    StructField("GCRT", StringType(), nullable=True),
    StructField("RT", StringType(), nullable=True),
    StructField("GCRT2", StringType(), nullable=True),
    StructField("GCRT_DESC", StringType(), nullable=True),
    StructField("SYSTEMATIC", StringType(), nullable=True)
])
df_route_link = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_route_link)
    .csv(f"{adls_path_raw}/landing/route3link.dat")
)

# --------------------------------------------------------------------------------
# NDC stage (CSeqFileStage)
# --------------------------------------------------------------------------------
# Large schema for ndc.dat
schema_ndc = StructType([
    StructField("NDC", StringType(), nullable=False),
    StructField("LBLRID", StringType(), nullable=False),
    StructField("GCN_SEQNO", StringType(), nullable=False),
    StructField("PS", StringType(), nullable=False),
    StructField("DF", StringType(), nullable=False),
    StructField("AD", StringType(), nullable=False),
    StructField("LN", StringType(), nullable=False),
    StructField("BN", StringType(), nullable=False),
    StructField("PNDC", StringType(), nullable=False),
    StructField("REPNDC", StringType(), nullable=False),
    StructField("NDCFI", StringType(), nullable=False),
    StructField("DADDNC", StringType(), nullable=False),
    StructField("DUPDC", StringType(), nullable=False),
    StructField("DESI", StringType(), nullable=False),
    StructField("DESDTEC", StringType(), nullable=False),
    StructField("DESI2", StringType(), nullable=False),
    StructField("DES2DTEC", StringType(), nullable=False),
    StructField("DEA", StringType(), nullable=False),
    StructField("CL", StringType(), nullable=False),
    StructField("GPI", StringType(), nullable=False),
    StructField("HOSP", StringType(), nullable=False),
    StructField("INNOV", StringType(), nullable=False),
    StructField("IPI", StringType(), nullable=False),
    StructField("MINI", StringType(), nullable=False),
    StructField("MAINT", StringType(), nullable=False),
    StructField("OBC", StringType(), nullable=False),
    StructField("OBSDTEC", StringType(), nullable=False),
    StructField("PPI", StringType(), nullable=False),
    StructField("STPK", StringType(), nullable=False),
    StructField("REPACK", StringType(), nullable=False),
    StructField("TOP200", StringType(), nullable=False),
    StructField("UD", StringType(), nullable=False),
    StructField("CSP", StringType(), nullable=False),
    StructField("NDL_GDGE", StringType(), nullable=False),
    StructField("NDL_LNGTH", StringType(), nullable=False),
    StructField("SVR_CPCTY", StringType(), nullable=False),
    StructField("SHLF_PCK", StringType(), nullable=False),
    StructField("SHIPPER", StringType(), nullable=False),
    StructField("HCFA_FDA", StringType(), nullable=False),
    StructField("HCFA_UNIT", StringType(), nullable=False),
    StructField("HCFA_PS", StringType(), nullable=False),
    StructField("HCFA_APPC", StringType(), nullable=False),
    StructField("HCFA_MRKC", StringType(), nullable=False),
    StructField("HCFA_TRMC", StringType(), nullable=False),
    StructField("HCFA_TYP", StringType(), nullable=False),
    StructField("HCFA_DESC1", StringType(), nullable=False),
    StructField("HCFA_DESI1", StringType(), nullable=False),
    StructField("UU", StringType(), nullable=False),
    StructField("PD", StringType(), nullable=False),
    StructField("LN25", StringType(), nullable=False),
    StructField("LN25I", StringType(), nullable=False),
    StructField("GPIDC", StringType(), nullable=False),
    StructField("BBDC", StringType(), nullable=False),
    StructField("HOME", StringType(), nullable=False),
    StructField("INPCKI", StringType(), nullable=False),
    StructField("OUTPCKI", StringType(), nullable=False),
    StructField("OBC_EXP", StringType(), nullable=False),
    StructField("PS_EQUIV", StringType(), nullable=False),
    StructField("PLBLR", StringType(), nullable=False),
    StructField("TOP50GEN", StringType(), nullable=False),
    StructField("OBC3", StringType(), nullable=False),
    StructField("GMI", StringType(), nullable=False),
    StructField("GNI", StringType(), nullable=False),
    StructField("GSI", StringType(), nullable=False),
    StructField("GTI", StringType(), nullable=False),
    StructField("NDCGI1", StringType(), nullable=False),
    StructField("HCFA_DC", StringType(), nullable=False),
    StructField("LN60", StringType(), nullable=False)
])
df_NDC = (
    spark.read.option("header", False)
    .option("quote", "'")
    .option("delimiter", ",")
    .schema(schema_ndc)
    .csv(f"{adls_path_raw}/landing/ndc.dat")
)

# --------------------------------------------------------------------------------
# NDC_ids stage (DB2Connector, reading from IDS)
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_NDC_ids = f"SELECT NDC as NDC, TRGT_CD as TRGT_CD FROM {IDSOwner}.NDC, {IDSOwner}.CD_MPPNG WHERE NDC_DSM_DRUG_TYP_CD_SK = CD_MPPNG_SK"
df_NDC_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_NDC_ids)
    .load()
)

# --------------------------------------------------------------------------------
# hf_ndc_dsm_drug_typ (Scenario A: remove hashed file, deduplicate on PK NDC)
# --------------------------------------------------------------------------------
df_ndc_lkup_tmp = dedup_sort(df_NDC_ids, ["NDC"], [])
df_ndc_lkup = df_ndc_lkup_tmp

# --------------------------------------------------------------------------------
# hf_ndc_gnn (Scenario A: remove hashed file, deduplicate on PK HICL_SEQNO)
# --------------------------------------------------------------------------------
df_gnn_lkup_tmp = dedup_sort(df_gnn, ["HICL_SEQNO"], [])
df_gnn_lkup = df_gnn_lkup_tmp

# --------------------------------------------------------------------------------
# Transformer_39 (tcclink primary + gnn_lkup lookup, left join on HICL_SEQQNO)
# --------------------------------------------------------------------------------
df_tcc_link_out_pre = df_tcc_link.alias("tcclink").join(
    df_gnn_lkup.alias("gnn_lkup"),
    F.col("tcclink.HICL_SEQQNO") == F.col("gnn_lkup.HICL_SEQNO"),
    "left"
)
df_tcc_link_out = df_tcc_link_out_pre.select(
    F.col("tcclink.GCN_SEQNO").alias("GCN_SEQNO"),
    F.col("tcclink.HIC3").alias("HIC3"),
    F.col("tcclink.HICL_SEQQNO").alias("HICL_SEQQNO"),
    F.col("tcclink.GCDF").alias("GCDF"),
    F.col("tcclink.GCRT").alias("GCRT"),
    F.col("tcclink.STR").alias("STR"),
    F.col("tcclink.GTC").alias("GTC"),
    F.col("tcclink.TC").alias("TC"),
    F.col("tcclink.DCC").alias("DCC"),
    F.col("tcclink.GCNSEQ_GI").alias("GCNSEQ_GI"),
    F.col("tcclink.GENDER").alias("GENDER"),
    F.col("tcclink.HIC3_SEQN").alias("HIC3_SEQN"),
    F.col("tcclink.STR60").alias("STR60"),
    F.when(F.col("gnn_lkup.HICL_SEQNO").isNull(), F.lit("UNK"))
     .otherwise(F.col("gnn_lkup.GNN")).alias("GNN")
)

# --------------------------------------------------------------------------------
# link_hash stage (Scenario A for each input->output)
#    gcn_link -> gcn_lkup
#    ahfs_link -> ahfs_lkup
#    lblr_link -> lblr_lkup
#    tcc_link_out (from Transformer_39) -> tcc_lkup
#    route_link -> route_lkup
# --------------------------------------------------------------------------------
df_gcn_lkup_tmp = dedup_sort(df_gcn_link, ["GCN_SEQNO"], [])
df_gcn_lkup_final = df_gcn_lkup_tmp

df_ahfs_lkup_tmp = dedup_sort(df_ahfs_link, ["GCN_SEQNO"], [])
df_ahfs_lkup_final = df_ahfs_lkup_tmp

df_lblr_lkup_tmp = dedup_sort(df_lblr_link, ["LBLRID"], [])
df_lblr_lkup_final = df_lblr_lkup_tmp

df_tcc_lkup_tmp = dedup_sort(df_tcc_link_out, ["GCN_SEQNO"], [])
df_tcc_lkup_final = df_tcc_lkup_tmp

df_route_lkup_tmp = dedup_sort(df_route_link, ["GCRT"], [])
df_route_lkup_final = df_route_lkup_tmp.select(
    F.col("GCRT"),
    F.col("RT").cast(StringType()).alias("RT"),
    F.col("GCRT2"),
    F.col("GCRT_DESC").cast(StringType()).alias("GCRT_DESC")
)

# --------------------------------------------------------------------------------
# BusinessRules stage (CTransformerStage) - multiple lookups
# --------------------------------------------------------------------------------
# Primary link: df_NDC (alias Extract)
# Left joins: 
#   ndc_lkup (alias ndc_lkup) on Extract.NDC = ndc_lkup.NDC
#   gcn_lkup (alias gcn_lkup_final) on Extract.GCN_SEQNO = gcn_lkup.GCN_SEQNO
#   ahfs_lkup (alias ahfs_lkup_final) on Extract.GCN_SEQNO = ahfs_lkup.GCN_SEQNO
#   tcc_lkup (alias tcc_lkup_final) on Extract.GCN_SEQNO = tcc_lkup.GCN_SEQNO
#   lblr_lkup (alias lblr_lkup_final) on trim(Extract.LBLRID) = lblr_lkup.LBLRID
df_BusinessRules_0 = df_NDC.alias("Extract").join(
    df_ndc_lkup.alias("ndc_lkup"),
    F.col("Extract.NDC") == F.col("ndc_lkup.NDC"),
    "left"
).join(
    df_gcn_lkup_final.alias("gcn_lkup"),
    F.col("Extract.GCN_SEQNO") == F.col("gcn_lkup.GCN_SEQNO"),
    "left"
).join(
    df_ahfs_lkup_final.alias("ahfs_lkup"),
    F.col("Extract.GCN_SEQNO") == F.col("ahfs_lkup.GCN_SEQNO"),
    "left"
).join(
    df_tcc_lkup_final.alias("tcc_lkup"),
    F.col("Extract.GCN_SEQNO") == F.col("tcc_lkup.GCN_SEQNO"),
    "left"
).join(
    df_lblr_lkup_final.alias("lblr_lkup"),
    trim(F.col("Extract.LBLRID")) == F.col("lblr_lkup.LBLRID"),
    "left"
)

df_BusinessRules = df_BusinessRules_0.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FDB").alias("SRC_SYS_CD"),
    (F.lit("FDB") + F.lit(",") + F.col("Extract.NDC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("NDC_SK"),
    F.col("Extract.NDC").alias("NDC"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("tcc_lkup.GCN_SEQNO").isNull(), F.lit("UNK")).otherwise(F.col("tcc_lkup.HIC3")).alias("TCC_CD"),
    F.when(F.col("ahfs_lkup.GCN_SEQNO").isNull(), F.lit("UNK")).otherwise(F.col("ahfs_lkup.AHFS")).alias("AHFS_TCC_CD"),
    F.when(F.col("gcn_lkup.GCN_SEQNO").isNull(), F.expr('""')).otherwise(
        F.regexp_replace(F.col("gcn_lkup.GCN"), "[\\x0D\\x0A\\x09]", "")
    ).alias("GCN_CD"),
    F.when(F.col("ndc_lkup.NDC").isNull(), F.lit("NA")).otherwise(F.col("ndc_lkup.TRGT_CD")).alias("DSM_DRUG_TYP_CD"),
    F.lit("N").alias("CLM_TRANS_ADD_IN"),
    F.when(F.col("Extract.MAINT") == "1", F.lit("Y")).otherwise(F.lit("N")).alias("DRUG_MNTN_IN"),
    F.expr("substring(Extract.NDC,1,9)").alias("CORE_NINE_NO"),
    trim(F.col("Extract.LN")).alias("DRUG_LABEL_NM"),
    F.expr("substring(Extract.NDC,1,5)").alias("LABELER_NO"),
    F.when(F.col("lblr_lkup.LBLRID").isNull(), F.lit(None)).otherwise(trim(F.col("lblr_lkup.MFG"))).alias("LABELER_NM"),
    F.expr("substring(Extract.NDC,6,4)").alias("PROD_NO"),
    F.col("Extract.DF").alias("DF"),
    trim(F.col("Extract.BN")).alias("BN"),
    F.col("Extract.PS").alias("PS"),
    trim(F.col("Extract.NDCFI")).alias("NDCFI"),
    F.when(
        (F.length(trim(F.col("Extract.DADDNC"))) == 0)|
        (trim(F.col("Extract.DADDNC")) == "00000000")|
        (F.col("Extract.DADDNC").isNull()), 
        F.lit("1753-01-01")
    ).otherwise(
        trim(F.col("Extract.DADDNC")).substr(1,8).alias("DADDNC")
    ).alias("DADDNC"),
    F.when(
        (F.length(trim(F.col("Extract.DUPDC"))) == 0)|
        (trim(F.col("Extract.DUPDC")) == "00000000")|
        (F.col("Extract.DUPDC").isNull()), 
        F.lit("1753-01-01")
    ).otherwise(
        trim(F.col("Extract.DUPDC")).substr(1,8)
    ).alias("DUPDC"),
    F.when(F.col("Extract.DESI")=="1", "Y").otherwise(
        F.when(F.col("Extract.DESI")=="0","N").otherwise("X")
    ).alias("DESI"),
    trim(F.col("Extract.DEA")).alias("DEA"),
    trim(F.col("Extract.CL")).alias("CL"),
    trim(F.col("Extract.GPI")).alias("GPI"),
    F.when(F.col("Extract.INNOV")=="1","Y").otherwise("N").alias("INNOV"),
    F.when(F.col("Extract.IPI")=="1","Y").otherwise("N").alias("IPI"),
    trim(F.col("Extract.OBC")).alias("OBC"),
    F.when(
        (F.length(trim(F.col("Extract.OBSDTEC"))) == 0)|
        (trim(F.col("Extract.OBSDTEC"))=="00000000")|
        (F.col("Extract.OBSDTEC").isNull()), 
        F.lit("2199-12-31")
    ).otherwise(
        trim(F.col("Extract.OBSDTEC")).substr(1,8)
    ).alias("OBSDTEC"),
    F.when(F.col("Extract.UD")=="1","Y").otherwise("N").alias("UD"),
    trim(F.col("Extract.NDL_GDGE")).alias("NDL_GDGE"),
    trim(F.col("Extract.NDL_LNGTH")).alias("NDL_LNGTH"),
    trim(F.col("Extract.SVR_CPCTY")).alias("SVR_CPCTY"),
    F.when(F.col("Extract.UU")=="1","Y").otherwise("N").alias("UU"),
    F.col("Extract.PD").alias("PD"),
    F.when(
        (F.length(trim(F.col("Extract.GPIDC"))) == 0)|
        (trim(F.col("Extract.GPIDC"))=="00000000")|
        (F.col("Extract.GPIDC").isNull()),
        F.lit("1753-01-01")
    ).otherwise(
        trim(F.col("Extract.GPIDC")).substr(1,8)
    ).alias("GPIDC"),
    F.when(
        (F.length(trim(F.col("Extract.BBDC"))) == 0)|
        (trim(F.col("Extract.BBDC"))=="00000000")|
        (F.col("Extract.BBDC").isNull()),
        F.lit("1753-01-01")
    ).otherwise(
        trim(F.col("Extract.BBDC")).substr(1,8)
    ).alias("BBDC"),
    trim(F.col("Extract.PS_EQUIV")).alias("PS_EQUIV"),
    F.when(F.col("Extract.PLBLR")=="1","Y").otherwise("N").alias("PLBLR"),
    F.col("Extract.GMI").alias("GMI"),
    F.col("Extract.GNI").alias("GNI"),
    F.col("Extract.GSI").alias("GSI"),
    F.when(F.col("Extract.NDCGI1")=="2","Y").otherwise("N").alias("NDCGI1"),
    F.when(F.col("tcc_lkup.GCN_SEQNO").isNull(), F.lit("UNK")).otherwise(F.col("tcc_lkup.GCDF")).alias("GCDF"),
    F.when(F.col("tcc_lkup.GCN_SEQNO").isNull(), F.lit("UNK")).otherwise(F.col("tcc_lkup.STR")).alias("STR"),
    F.when(F.col("tcc_lkup.GCN_SEQNO").isNull(), F.lit("UNK")).otherwise(F.col("tcc_lkup.GNN")).alias("GNN"),
    F.when(F.col("tcc_lkup.GCRT").isNull(), F.lit("UNK")).otherwise(F.col("tcc_lkup.GCRT")).alias("GCRT")
)

# --------------------------------------------------------------------------------
# Transformer_49 (PrimaryLink = df_BusinessRules, LookupLink = df_route_lkup_final)
# join on GCRT
# --------------------------------------------------------------------------------
df_Transformer49_pre = df_BusinessRules.alias("Transform1").join(
    df_route_lkup_final.alias("route_lkup"),
    F.col("Transform1.GCRT") == F.col("route_lkup.GCRT"),
    "left"
)
df_Transformer49 = df_Transformer49_pre.select(
    F.col("Transform1.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform1.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform1.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform1.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform1.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform1.ERR_CT").alias("ERR_CT"),
    F.col("Transform1.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform1.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform1.NDC_SK").alias("NDC_SK"),
    F.col("Transform1.NDC").alias("NDC"),
    F.col("Transform1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform1.TCC_CD").alias("TCC_CD"),
    F.col("Transform1.AHFS_TCC_CD").alias("AHFS_TCC_CD"),
    F.col("Transform1.GCN_CD").alias("GCN_CD"),
    F.col("Transform1.DSM_DRUG_TYP_CD").alias("DSM_DRUG_TYP_CD"),
    F.col("Transform1.CLM_TRANS_ADD_IN").alias("CLM_TRANS_ADD_IN"),
    F.col("Transform1.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("Transform1.CORE_NINE_NO").alias("CORE_NINE_NO"),
    F.col("Transform1.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("Transform1.LABELER_NO").alias("LABELER_NO"),
    F.col("Transform1.LABELER_NM").alias("LABELER_NM"),
    F.col("Transform1.PROD_NO").alias("PROD_NO"),
    F.col("Transform1.DF").alias("DF"),
    F.col("Transform1.BN").alias("BN"),
    F.col("Transform1.PS").alias("PS"),
    F.col("Transform1.NDCFI").alias("NDCFI"),
    F.col("Transform1.DADDNC").alias("DADDNC"),
    F.col("Transform1.DUPDC").alias("DUPDC"),
    F.col("Transform1.DESI").alias("DESI"),
    F.col("Transform1.DEA").alias("DEA"),
    F.col("Transform1.CL").alias("CL"),
    F.col("Transform1.GPI").alias("GPI"),
    F.col("Transform1.INNOV").alias("INNOV"),
    F.col("Transform1.IPI").alias("IPI"),
    F.col("Transform1.OBC").alias("OBC"),
    F.col("Transform1.OBSDTEC").alias("OBSDTEC"),
    F.col("Transform1.UD").alias("UD"),
    F.col("Transform1.NDL_GDGE").alias("NDL_GDGE"),
    F.col("Transform1.NDL_LNGTH").alias("NDL_LNGTH"),
    F.col("Transform1.SVR_CPCTY").alias("SVR_CPCTY"),
    F.col("Transform1.UU").alias("UU"),
    F.col("Transform1.PD").alias("PD"),
    F.col("Transform1.GPIDC").alias("GPIDC"),
    F.col("Transform1.BBDC").alias("BBDC"),
    F.col("Transform1.PS_EQUIV").alias("PS_EQUIV"),
    F.col("Transform1.PLBLR").alias("PLBLR"),
    F.col("Transform1.GMI").alias("GMI"),
    F.col("Transform1.GNI").alias("GNI"),
    F.col("Transform1.GSI").alias("GSI"),
    F.col("Transform1.NDCGI1").alias("NDCGI1"),
    F.col("Transform1.GCDF").alias("GCDF"),
    F.col("Transform1.STR").alias("STR"),
    F.col("Transform1.GNN").alias("GNN"),
    F.col("route_lkup.GCRT2").alias("GCRT2")
)

# --------------------------------------------------------------------------------
# PkeyTrn stage
# left-join with df_hf_ndc_lkup (lkup),
# stage variables SK, NewCrtRunCycExtcnSk
# --------------------------------------------------------------------------------
df_pkeyTrn_pre = df_Transformer49.alias("Transform").join(
    df_hf_ndc_lkup.alias("lkup"),
    F.col("Transform.NDC") == F.col("lkup.NDC_CD"),
    "left"
)

df_pkeyTrn_withvars = df_pkeyTrn_pre.select(
    F.col("Transform.*"),
    F.col("lkup.*")
)

# Build columns for SK and NewCrtRunCycExtcnSk prior to SurrogateKeyGen
df_pkeyTrn_vars = df_pkeyTrn_withvars.withColumn(
    "ExistingSK",
    F.col("lkup.NDC_CD_SK")
).withColumn(
    "ExistingCrtRun",
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK")
).withColumn(
    "NDC_SK",
    F.when(F.col("lkup.NDC_CD_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.NDC_CD_SK"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK_new",
    F.when(F.col("lkup.NDC_CD_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# Now apply SurrogateKeyGen to fill any null NDC_SK
df_enriched = SurrogateKeyGen(df_pkeyTrn_vars, <DB sequence name>, "NDC_SK", <schema>, <secret_name>)

# Create df_Key for NDCCrfPkey (all rows)
df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("Transform.NDC").alias("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.AHFS_TCC_CD").alias("AHFS_TCC_CD"),
    F.col("Transform.GCDF").alias("DOSE_FORM"),
    F.col("Transform.TCC_CD").alias("TCC_CD"),
    F.col("Transform.DSM_DRUG_TYP_CD").alias("DSM_DRUG_TYP_CD"),
    F.col("Transform.DEA").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.col("Transform.CL").alias("NDC_DRUG_CLS_CD"),
    F.col("Transform.DF").alias("NDC_DRUG_FORM_CD"),
    F.col("Transform.NDCFI").alias("NDC_FMT_CD"),
    F.col("Transform.GMI").alias("NDC_GNRC_MNFCTR_CD"),
    F.col("Transform.GNI").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.col("Transform.GPI").alias("NDC_GNRC_PRICE_CD"),
    F.col("Transform.GSI").alias("NDC_GNRC_PRICE_SPREAD_CD"),
    F.col("Transform.OBC").alias("NDC_ORANGE_BOOK_CD"),
    F.col("Transform.CLM_TRANS_ADD_IN").alias("CLM_TRANS_ADD_IN"),
    F.col("Transform.DESI").alias("DESI_DRUG_IN"),
    F.col("Transform.DRUG_MNTN_IN").alias("DRUG_MNTN_IN"),
    F.col("Transform.INNOV").alias("INNVTR_IN"),
    F.col("Transform.IPI").alias("INSTUT_PROD_IN"),
    F.col("Transform.PLBLR").alias("PRIV_LBLR_IN"),
    F.col("Transform.NDCGI1").alias("SNGL_SRC_IN"),
    F.col("Transform.UD").alias("UNIT_DOSE_IN"),
    F.col("Transform.UU").alias("UNIT_OF_USE_IN"),
    F.col("Transform.BBDC").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.col("Transform.GPIDC").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    F.col("Transform.OBSDTEC").alias("OBSLT_DT_SK"),
    F.col("Transform.DADDNC").alias("SRC_NDC_CRT_DT_SK"),
    F.col("Transform.DUPDC").alias("SRC_NDC_UPDT_DT_SK"),
    F.col("Transform.BN").alias("BRND_NM"),
    F.col("Transform.CORE_NINE_NO").alias("CORE_NINE_NO"),
    F.col("Transform.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("Transform.STR").alias("DRUG_STRG_DESC"),
    F.col("Transform.GCN_CD").alias("GCN_CD_TX"),
    F.col("Transform.GNN").alias("GNRC_NM_SH_DESC"),
    F.col("Transform.LABELER_NM").alias("LBLR_NM"),
    F.col("Transform.LABELER_NO").alias("LBLR_NO"),
    F.col("Transform.NDL_GDGE").alias("NEEDLE_GAUGE_VAL"),
    F.col("Transform.NDL_LNGTH").alias("NEEDLE_LGTH_VAL"),
    F.col("Transform.PD").alias("PCKG_DESC"),
    F.col("Transform.PS_EQUIV").alias("PCKG_SIZE_EQVLNT_NO"),
    F.col("Transform.PS").alias("PCKG_SIZE_NO"),
    F.col("Transform.PROD_NO").alias("PROD_NO"),
    F.col("Transform.SVR_CPCTY").alias("SYRNG_CPCT_VAL"),
    F.col("GCRT2").alias("NDC_RTE_TYP_CD")
)

# Rows for df_updt (IsNull(lkup.NDC_CD_SK) = @TRUE)
df_updt = df_enriched.filter(F.col("ExistingSK").isNull()).select(
    F.col("Transform.NDC").alias("NDC_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("NDC_SK").alias("NDC_CD_SK")
)

# --------------------------------------------------------------------------------
# hf_ndc (Scenario B: write back to dummy table with merge)
# --------------------------------------------------------------------------------
# Create staging table
temp_table_name_hf_ndc = f"STAGING.FdbNdcExtr_hf_ndc_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_hf_ndc}", jdbc_url, jdbc_props)

(df_updt.write.format("jdbc")
  .option("url", jdbc_url)
  .options(**jdbc_props)
  .option("dbtable", temp_table_name_hf_ndc)
  .mode("overwrite")
  .save()
)

# Merge into {IDSOwner}.dummy_hf_ndc
merge_sql_hf_ndc = f"""
MERGE INTO {IDSOwner}.dummy_hf_ndc as T
USING {temp_table_name_hf_ndc} as S
ON T.NDC_CD = S.NDC_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.NDC_CD_SK = S.NDC_CD_SK
WHEN NOT MATCHED THEN
  INSERT (NDC_CD, CRT_RUN_CYC_EXCTN_SK, NDC_CD_SK)
  VALUES (S.NDC_CD, S.CRT_RUN_CYC_EXCTN_SK, S.NDC_CD_SK);
"""
execute_dml(merge_sql_hf_ndc, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# NDCCrfPkey (CSeqFileStage) writing #TmpOutFile#
# Before writing, apply rpad for char/varchar columns
df_Key_for_write = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("NDC_SK"),
    F.col("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("AHFS_TCC_CD"), 10, " ").alias("AHFS_TCC_CD"),
    F.rpad(F.col("DOSE_FORM"), 2, " ").alias("DOSE_FORM"),
    F.rpad(F.col("TCC_CD"), 10, " ").alias("TCC_CD"),
    F.rpad(F.col("DSM_DRUG_TYP_CD"), 10, " ").alias("DSM_DRUG_TYP_CD"),
    F.rpad(F.col("NDC_DRUG_ABUSE_CTL_CD"), 1, " ").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.rpad(F.col("NDC_DRUG_CLS_CD"), 1, " ").alias("NDC_DRUG_CLS_CD"),
    F.rpad(F.col("NDC_DRUG_FORM_CD"), 1, " ").alias("NDC_DRUG_FORM_CD"),
    F.rpad(F.col("NDC_FMT_CD"), 1, " ").alias("NDC_FMT_CD"),
    F.rpad(F.col("NDC_GNRC_MNFCTR_CD"), 1, " ").alias("NDC_GNRC_MNFCTR_CD"),
    F.rpad(F.col("NDC_GNRC_NMD_DRUG_CD"), 1, " ").alias("NDC_GNRC_NMD_DRUG_CD"),
    F.rpad(F.col("NDC_GNRC_PRICE_CD"), 1, " ").alias("NDC_GNRC_PRICE_CD"),
    F.rpad(F.col("NDC_GNRC_PRICE_SPREAD_CD"), 1, " ").alias("NDC_GNRC_PRICE_SPREAD_CD"),
    F.rpad(F.col("NDC_ORANGE_BOOK_CD"), 1, " ").alias("NDC_ORANGE_BOOK_CD"),
    F.rpad(F.col("CLM_TRANS_ADD_IN"), 1, " ").alias("CLM_TRANS_ADD_IN"),
    F.rpad(F.col("DESI_DRUG_IN"), 1, " ").alias("DESI_DRUG_IN"),
    F.rpad(F.col("DRUG_MNTN_IN"), 1, " ").alias("DRUG_MNTN_IN"),
    F.rpad(F.col("INNVTR_IN"), 1, " ").alias("INNVTR_IN"),
    F.rpad(F.col("INSTUT_PROD_IN"), 1, " ").alias("INSTUT_PROD_IN"),
    F.rpad(F.col("PRIV_LBLR_IN"), 1, " ").alias("PRIV_LBLR_IN"),
    F.rpad(F.col("SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
    F.rpad(F.col("UNIT_DOSE_IN"), 1, " ").alias("UNIT_DOSE_IN"),
    F.rpad(F.col("UNIT_OF_USE_IN"), 1, " ").alias("UNIT_OF_USE_IN"),
    F.rpad(F.col("AVG_WHLSL_PRICE_CHG_DT_SK"), 10, " ").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    F.rpad(F.col("GNRC_PRICE_IN_CHG_DT_SK"), 10, " ").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    F.rpad(F.col("OBSLT_DT_SK"), 10, " ").alias("OBSLT_DT_SK"),
    F.rpad(F.col("SRC_NDC_CRT_DT_SK"), 10, " ").alias("SRC_NDC_CRT_DT_SK"),
    F.rpad(F.col("SRC_NDC_UPDT_DT_SK"), 10, " ").alias("SRC_NDC_UPDT_DT_SK"),
    F.col("BRND_NM"),
    F.col("CORE_NINE_NO"),
    F.col("DRUG_LABEL_NM"),
    F.col("DRUG_STRG_DESC"),
    F.col("GCN_CD_TX"),
    F.col("GNRC_NM_SH_DESC"),
    F.col("LBLR_NM"),
    F.col("LBLR_NO"),
    F.col("NEEDLE_GAUGE_VAL"),
    F.col("NEEDLE_LGTH_VAL"),
    F.col("PCKG_DESC"),
    F.col("PCKG_SIZE_EQVLNT_NO"),
    F.col("PCKG_SIZE_NO"),
    F.col("PROD_NO"),
    F.col("SYRNG_CPCT_VAL"),
    F.rpad(F.col("NDC_RTE_TYP_CD"), 2, " ").alias("NDC_RTE_TYP_CD")
)
write_files(
    df_Key_for_write,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# FDB_Source stage (CSeqFileStage)
# --------------------------------------------------------------------------------
schema_fdb_source = StructType([
    StructField("NDC", StringType(), nullable=False)
])
df_FDB_Source = (
    spark.read.option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_fdb_source)
    .csv(f"{adls_path_raw}/landing/ndc.dat")
)

# --------------------------------------------------------------------------------
# Transform
# --------------------------------------------------------------------------------
df_Transform_fdb = df_FDB_Source.select(
    trim(F.regexp_replace(F.col("NDC"), "[\\x0D\\x0A\\x09]", "")).alias("NDC")
)

# --------------------------------------------------------------------------------
# Snapshot_File (CSeqFileStage) => "load/B_NDC.dat"
# --------------------------------------------------------------------------------
df_Snapshot_File_for_write = df_Transform_fdb.select(
    F.rpad(F.col("NDC"), 11, " ").alias("NDC")
)
write_files(
    df_Snapshot_File_for_write,
    f"{adls_path}/load/B_NDC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)