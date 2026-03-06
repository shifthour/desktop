# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008, 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after FctsClmDriverBuild
# MAGIC          *  UNIX file K_CLM_LN.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm_ln is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN
# MAGIC              CLM_LN_PCA
# MAGIC              CLM_LN_REMIT
# MAGIC              CLM_LN_CLNCL_EDIT
# MAGIC 
# MAGIC 2;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                  Date               Project/Altiris #      change Description\(9)\(9)\(9)\(9)Development Project    Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------   ------------------------      --------------------------------------------------------------------------------------   ----------------------------------   -------------------------------   ----------------------------       
# MAGIC Brent Leland\(9)  2008-07-30     3567 Primary Key   Original Programming                                                        devlIDS                        Steph Goddard          10/03/2008
# MAGIC  
# MAGIC K Chintalapani           2015-07-02     TFS-9987               Used the Trim function in the Facets source stage  
# MAGIC                                                                                        for Claim line extract to remove the spaces in CLCL_ID   IntegrateDev1               Kalyan Neelam          2015-07-02      
# MAGIC                                                                                        to  accountfor missing claimlines.
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu  2018-02-23     TFS-21100              Used the Trim function in Trans2 stage  
# MAGIC                                                                                         for Claim line extract to remove the spaces in CLCL_ID   IntegrateDev2             Hugh Sisson              2018-02-27        
# MAGIC                                                                                         to accountfor missing claimlines.
# MAGIC Prabhu ES                2022-02-26     S2S Remediation     MSSQL connection parameters added                            IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Get SK for primary key on input record
# MAGIC hf_clm_ln hash file used to key tables
# MAGIC CLM_LN
# MAGIC CLM_LN_PCA
# MAGIC CLM_LN_REMIT
# MAGIC CLM_LN_CLNCL_EDIT
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Dental Lines
# MAGIC Medical Lines
# MAGIC Key adj-to and adj-from claim IDs on claims above
# MAGIC Check is adjusted from or to claim was keyed above
# MAGIC Get SK for records with out keys
# MAGIC Eliminate dups from input sources
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnLoadPK

# Obtain parameter values
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
AdjFromTable = get_widget_value('AdjFromTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

# Read from CMC_CDDL_CL_LINE (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
query_cmc_cddl_cl_line = (
    f"SELECT \n"
    f"RTrim(TMP.CLM_ID) AS CLM_ID,\n"
    f"CL.CDDL_SEQ_NO,\n"
    f"TMP.CLM_STS,\n"
    f"TMP.CLCL_PAID_DT,\n"
    f"TMP.CLCL_ID_ADJ_TO,\n"
    f"TMP.CLCL_ID_ADJ_FROM\n"
    f"FROM {FacetsOwner}.CMC_CDDL_CL_LINE CL,\n"
    f"     tempdb..{DriverTable} TMP\n"
    f"WHERE TMP.CLM_ID = CL.CLCL_ID"
)
df_CMC_CDDL_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_cmc_cddl_cl_line)
    .load()
)

df_DntlClms = df_CMC_CDDL_CL_LINE

# Trans1 logic
df_RDntl = df_DntlClms.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLM_STS")) == True).select(
    F.concat(trim(F.col("CLM_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)
df_Dntl = df_DntlClms.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLM_STS")) != True).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# hf_clm_ln_pk_dntl_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_dntl_hold = dedup_sort(
    df_Dntl,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# hf_clm_ln_pk_rdntl_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_rdntl_hold = dedup_sort(
    df_RDntl,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# Read from CMC_CDML_CL_LINE (ODBCConnector)
query_cmc_cdml_cl_line = (
    f"SELECT \n"
    f"RTrim(TMP.CLM_ID) as CLM_ID,\n"
    f"CL.CDML_SEQ_NO CDDL_SEQ_NO,\n"
    f"TMP.CLM_STS,\n"
    f"TMP.CLCL_PAID_DT,\n"
    f"TMP.CLCL_ID_ADJ_TO,\n"
    f"TMP.CLCL_ID_ADJ_FROM\n"
    f"FROM {FacetsOwner}.CMC_CDML_CL_LINE CL,\n"
    f"     tempdb..{DriverTable} TMP\n"
    f"WHERE TMP.CLM_ID = CL.CLCL_ID"
)
df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_cmc_cdml_cl_line)
    .load()
)

df_MedClms = df_CMC_CDML_CL_LINE

# Trans5 logic
df_RMed = df_MedClms.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLM_STS")) == True).select(
    F.concat(trim(F.col("CLM_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)
df_Med = df_MedClms.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLM_STS")) != True).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# hf_clm_ln_pk_med_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_med_hold = dedup_sort(
    df_Med,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# hf_clm_ln_pk_rmed_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_rmed_hold = dedup_sort(
    df_RMed,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# Collector - merges the four inputs
df_Collector = df_lnk_clm_ln_pk_dntl_hold.unionByName(df_lnk_clm_ln_pk_rdntl_hold) \
    .unionByName(df_lnk_clm_ln_pk_med_hold) \
    .unionByName(df_lnk_clm_ln_pk_rmed_hold)

df_All_Clms = df_Collector

# Trans2
df_Transform = df_All_Clms.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    trim(F.col("CLM_ID")).alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# Container: ClmLnLoadPK
params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_PK_Clms = ClmLnLoadPK(df_Transform, params_ClmLnLoadPK)

# hf_clm_ln_pk_lkup (Scenario A) - remove hashed file, deduplicate on primary keys
df_lkup = dedup_sort(
    df_PK_Clms,
    ["SRC_SYS_CD","CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# Read from DNTL_ADJ_CLM (ODBCConnector)
query_dntl_adj_clm = (
    f"SELECT \n"
    f"RTrim(CLM.CLCL_ID) as CLCL_ID,\n"
    f"CL.CDDL_SEQ_NO,\n"
    f"CLM.CLCL_CUR_STS,\n"
    f"CLM.CLCL_PAID_DT,\n"
    f"CLM.CLCL_LAST_ACT_DTM\n"
    f"FROM \n"
    f"     tempdb..{AdjFromTable} DRVR,\n"
    f"     {FacetsOwner}.CMC_CLCL_CLAIM CLM,\n"
    f"     {FacetsOwner}.CMC_CDDL_CL_LINE CL\n"
    f"WHERE CLM.CLCL_ID   = DRVR.CLM_ID\n"
    f"  AND DRVR.CLM_ID   = CL.CLCL_ID"
)
df_DNTL_ADJ_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_dntl_adj_clm)
    .load()
)

# hf_clm_ln_pk_denadj_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_denadj_hold = dedup_sort(
    df_DNTL_ADJ_CLM.select(
        F.col("CLCL_ID"),
        F.col("CDDL_SEQ_NO"),
        F.col("CLCL_CUR_STS"),
        F.col("CLCL_PAID_DT"),
        F.col("CLCL_LAST_ACT_DTM")
    ),
    ["CLCL_ID","CDDL_SEQ_NO","CLCL_CUR_STS","CLCL_PAID_DT","CLCL_LAST_ACT_DTM"],
    []
)

# Read from MED_ADJ_CLM (ODBCConnector)
query_med_adj_clm = (
    f"SELECT \n"
    f"RTrim(CLM.CLCL_ID) AS CLCL_ID,\n"
    f"CL.CDDL_SEQ_NO,\n"
    f"CLM.CLCL_CUR_STS,\n"
    f"CLM.CLCL_PAID_DT,\n"
    f"CLM.CLCL_LAST_ACT_DTM\n"
    f"FROM \n"
    f"     tempdb..{AdjFromTable} DRVR,\n"
    f"     {FacetsOwner}.CMC_CLCL_CLAIM CLM,\n"
    f"     {FacetsOwner}.CMC_CDDL_CL_LINE CL\n"
    f"WHERE CLM.CLCL_ID = DRVR.CLM_ID\n"
    f"  AND DRVR.CLM_ID = CL.CLCL_ID"
)
df_MED_ADJ_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_med_adj_clm)
    .load()
)

# hf_clm_ln_pk_medadj_hold (Scenario A) - remove hashed file, deduplicate
df_lnk_clm_ln_pk_medadj_hold = dedup_sort(
    df_MED_ADJ_CLM.select(
        F.col("CLCL_ID"),
        F.col("CDDL_SEQ_NO"),
        F.col("CLCL_CUR_STS"),
        F.col("CLCL_PAID_DT"),
        F.col("CLCL_LAST_ACT_DTM")
    ),
    ["CLCL_ID","CDDL_SEQ_NO","CLCL_CUR_STS","CLCL_PAID_DT","CLCL_LAST_ACT_DTM"],
    []
)

# Collector2
df_Collector2 = df_lnk_clm_ln_pk_denadj_hold.unionByName(df_lnk_clm_ln_pk_medadj_hold)
df_Adj = df_Collector2

# Trans3
df_Radj = df_Adj.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLCL_CUR_STS")) == True).select(
    F.concat(trim(F.col("CLCL_ID")), F.lit("R")).alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)
df_Oadj = df_Adj.filter(IS.CLM.REVERSAL(SrcSysCd, F.col("CLCL_CUR_STS")) != True).select(
    F.concat(trim(F.col("CLCL_ID"))).alias("CLM_ID"),
    F.col("CDDL_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# hf_clm_ln_pk_radj_hold (Scenario A)
df_lnk_clm_ln_pk_radj_hold = dedup_sort(
    df_Radj,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# hf_clm_ln_pk_oadj_hold (Scenario A)
df_lnk_clm_ln_pk_oadj_hold = dedup_sort(
    df_Oadj,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# Collector3
df_Collector3 = df_lnk_clm_ln_pk_radj_hold.unionByName(df_lnk_clm_ln_pk_oadj_hold)
df_Colllection = df_Collector3

# hf_clm_pk_adj_tofrom (Scenario A)
df_Adjust = dedup_sort(
    df_Colllection,
    ["CLM_ID","CLM_LN_SEQ_NO"],
    []
)

# Trans4
# We perform a left join of df_Adjust (primary link) with df_lkup (lookup link).
# The join conditions:
#   df_Adjust["CLM_ID"] == df_lkup["CLM_ID"]
#   df_Adjust["CLM_LN_SEQ_NO"] == df_lkup["CLM_LN_SEQ_NO"]
#   and also we have to match SrcSysCd => df_lkup["SRC_SYS_CD"] (we treat SrcSysCd as a literal column in the primary link).
df_Adjust_with_src = df_Adjust.withColumn("SrcSysCd", F.lit(SrcSysCd))
df_join_Trans4 = df_Adjust_with_src.join(
    df_lkup.alias("Lkup"),
    (
        (df_Adjust_with_src["SrcSysCd"] == F.col("Lkup.SRC_SYS_CD")) &
        (df_Adjust_with_src["CLM_ID"] == F.col("Lkup.CLM_ID")) &
        (df_Adjust_with_src["CLM_LN_SEQ_NO"] == F.col("Lkup.CLM_LN_SEQ_NO"))
    ),
    how="left"
)

# Constraint for NewAdjClms => "IsNull(Lkup.CLM_LN_SK) = @TRUE"
df_NewAdjClms = df_join_Trans4.filter(F.col("Lkup.CLM_LN_SK").isNull()).select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

# Container: ClmLnLoadPK2 (same shared container name as ClmLnLoadPK)
params_ClmLnLoadPK2 = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_Not_Used = ClmLnLoadPK(df_NewAdjClms, params_ClmLnLoadPK2)

# hf_clm_ln_pk_lkup2 (Scenario C) => writing to the same file name "hf_clm_ln_pk_lkup", which must become parquet
# The stage columns are: 
#   SRC_SYS_CD (varchar),
#   CLM_ID (varchar),
#   CLM_LN_SEQ_NO (int),
#   CRT_RUN_CYC_EXCTN_SK (int),
#   CLM_LN_SK (int)
# Because we do not have exact lengths for the varchar columns, use <...> as placeholder for rpad.
df_hf_clm_ln_pk_lkup2_final = df_Not_Used.select(
    rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK")
)

write_files(
    df_hf_clm_ln_pk_lkup2_final,
    "hf_clm_ln_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)