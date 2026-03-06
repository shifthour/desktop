# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLMD_DIAG to a landing file for the IDS
# MAGIC       
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                        --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        04/01/2004                                    Originally Programmed
# MAGIC SharonAndrew        06/21/2004                                    added source to primary key
# MAGIC Steph Goddard       07/27/2004                                     took decimal point out of diagnosis code using Ereplace
# MAGIC Steph Goddard       02/09/2006                                     Changed to combine extract, transform, pkey for sequencer 
# MAGIC BJ Luce                  03/20/2006                                     add hf_clm_nasco_dup_bypass built in ClmDriverBuild, identifies 
# MAGIC                                                                                        claims that are nasco dups. If the claim is on the file, a row is 
# MAGIC                                                                                        not generated for it in IDS. However, an R row will be build for 
# MAGIC                                                                                        it if the status if '91'
# MAGIC Sanderw                 12/08/2006     Project 1756           Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland           05/02/2007     IAD Prod. Supp.     Added current date/time parameter so FORMAT.DATE routine                             devlIDS30
# MAGIC                                                                                       call could be removed from transform stage for efficentcy. 
# MAGIC Oliver Nielsen         08/13/2007                                    Added Balancing Snapshot File                                                                            devIDS30                       Steph Goddard            8/30/07
# MAGIC Ralph Tucker           2008-07-23       3657 Primary Key  Changed primary key from hash file to DB2 table                                                 devlIDS                           Steph Goddard            7/27/2008
# MAGIC Oliver Nielsen         07/25/2008        Facets 4.5.1        Changed IDCD_ID from char(6) to Varchar(10) when querying                             devlIDSnew                    Steph Goddard            08/20/2008
# MAGIC                                                                                      from Facets.  Also changed CLMD_TYPE to Char(2)
# MAGIC                                                                                        Added logic for ordinal code
# MAGIC Parik                      2008-08-13        3567 Primary Key    Changes within the shared container, so validated this job                                  devlIDS                          Steph Goddard            08/14/2008
# MAGIC                                                                                        Removed constraint logic for specific reversal claim status
# MAGIC SAndrew               2008-12-22         DRG                      Retrive feild CLMD_POA_IN from Facets for new field CLM_DIAG_POA_CD_SK devlIDS                        Brent Leland                12-29-2008
# MAGIC Rick Henry            2012-04-24         4896                      Added Diag_Cd_Typ_cd for SK lookup                                                                  NewDevl                      Sandrew                    2012-05-17
# MAGIC Hugh Sisson          2016-05-26        TFS12539              Changed main query to use left outer join to CMC_CLMF_MULT_FUNC              IntergrateDev2              
# MAGIC                                                                                       Added logic to det sefault value for CLMF_ICD_IND_PROC
# MAGIC Kaushik Kapoor     2017-11-16         TFS - 20365         Added Upcase logic on CLMD_POA_IND field in Staging variable in                    IntegrateDev2               
# MAGIC                                                                                       transformer
# MAGIC Prabhu ES             2022-02-28      S2S Remediation     MSSQL connection parameters added                                                                  IntegrateDev5                  Kalyan Neelam            2022-06-10

# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDIagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Facets Claim Diagnosis Data
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Hash files built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

# 1) Read from hashed file: hf_clm_fcts_reversals (Scenario C)
df_hf_clm_fcts_reversals_raw = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals_raw.select(
    rpad("CLCL_ID",12," "),
    rpad("CLCL_CUR_STS",2," "),
    F.col("CLCL_PAID_DT"),
    rpad("CLCL_ID_ADJ_TO",12," "),
    rpad("CLCL_ID_ADJ_FROM",12," ")
).alias("hf_clm_fcts_reversals")

# 2) Read from hashed file: clm_nasco_dup_bypass (Scenario C)
df_clm_nasco_dup_bypass_raw = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass_raw.select(
    F.col("CLM_ID")
).alias("clm_nasco_dup_bypass")

# 3) CD_MAPPING (DB2Connector → read from IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_CD_MAPPING = (
    "SELECT CD_MPPNG.SRC_CD,\n"
    "       CD_MPPNG.TRGT_CD\n"
    f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG\n"
    "WHERE CD_MPPNG.SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'\n"
    "  AND CD_MPPNG.TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'\n"
    "  AND SRC_CLCTN_CD = 'FACETS DBO'"
)
df_CD_MAPPING_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MAPPING)
    .load()
)
# 3a) Because the data goes to a hashed file with no read-modify-write pattern, do Scenario A dedup on PK = SRC_CD
df_CD_MAPPING = dedup_sort(df_CD_MAPPING_raw, ["SRC_CD"], [("SRC_CD","A")])

# 4) hf_ClmDiagExtr_DiagCdTypCd is Scenario A (intermediate hashed file). We skip reading/writing that file.
#    So df_ClmDiagExtr_DiagCdTypCd = df_CD_MAPPING (deduplicated) directly
df_ClmDiagExtr_DiagCdTypCd = df_CD_MAPPING.select(
    F.col("SRC_CD"),
    F.col("TRGT_CD")
).alias("hf_ClmDiagExtr_DiagCdTypCd")

# 5) CMC_CLMD_DIAG (ODBCConnector → read from Facets)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CLMD_DIAG = (
    "SELECT A.CLCL_ID,\n"
    "       A.CLMD_TYPE,\n"
    "       A.MEME_CK,\n"
    "       A.IDCD_ID,\n"
    "       A.CLMD_LOCK_TOKEN,\n"
    "       A.CLMD_POA_IND,\n"
    "       B.CLMF_ICD_IND_PROC,\n"
    "       C.CLCL_HIGH_SVC_DT\n"
    f"FROM tempdb..{DriverTable} TMP,\n"
    f"     {FacetsOwner}.CMC_CLCL_CLAIM C,\n"
    f"     {FacetsOwner}.CMC_CLMD_DIAG A LEFT OUTER JOIN {FacetsOwner}.CMC_CLMF_MULT_FUNC B\n"
    "     ON A.CLCL_ID = B.CLCL_ID\n"
    "WHERE A.CLCL_ID = TMP.CLM_ID\n"
    "  AND C.CLCL_ID = A.CLCL_ID"
)
df_CMC_CLMD_DIAG_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLMD_DIAG)
    .load()
)
df_CMC_CLMD_DIAG = df_CMC_CLMD_DIAG_raw.alias("CMC_CLMD_DIAG")

# 6) TrnsStripField
df_TrnsStripField = df_CMC_CLMD_DIAG.select(
    strip_field(F.col("CMC_CLMD_DIAG.CLCL_ID")).alias("CLCL_ID"),
    strip_field(F.col("CMC_CLMD_DIAG.CLMD_TYPE")).alias("CLMD_TYPE"),
    F.lit(current_timestamp()).alias("EXT_TIMESTAMP"),
    F.col("CMC_CLMD_DIAG.MEME_CK").alias("MEME_CK"),
    strip_field(F.col("CMC_CLMD_DIAG.IDCD_ID")).alias("IDCD_ID"),
    F.col("CMC_CLMD_DIAG.CLMD_LOCK_TOKEN").alias("CLMD_LOCK_TOKEN"),
    strip_field(F.col("CMC_CLMD_DIAG.CLMD_POA_IND")).alias("CLMD_POA_IND"),
    trim(strip_field(F.col("CMC_CLMD_DIAG.CLMF_ICD_IND_PROC"))).alias("CLMF_ICD_IND_PROC"),
    F.col("CMC_CLMD_DIAG.CLCL_HIGH_SVC_DT").alias("CLCL_HIGH_SVC_DT")
).alias("Strip")

# 7) BusinessRules (CTransformerStage)
# Joining: 
#   - fcts_reversals (left) on (Strip.CLCL_ID == fcts_reversals.CLCL_ID)
#   - nasco_dup_lkup (left) on (Strip.CLCL_ID == nasco_dup_lkup.CLM_ID)
#   - TRGT_CD (left) but no join condition given → forced cross join approach. 
df_BusinessRules_Joined = (
    df_TrnsStripField.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("Strip.CLCL_ID")==F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("Strip.CLCL_ID")==F.col("nasco_dup_lkup.CLM_ID"), "left")
    .join(df_ClmDiagExtr_DiagCdTypCd.alias("TRGT_CD"), F.lit(True), "left")
)

df_BusinessRules = df_BusinessRules_Joined.select(
    F.col("Strip.CLCL_ID").alias("Strip_CLCL_ID"),
    F.col("Strip.CLMD_TYPE").alias("Strip_CLMD_TYPE"),
    F.col("Strip.EXT_TIMESTAMP").alias("Strip_EXT_TIMESTAMP"),
    F.col("Strip.MEME_CK").alias("Strip_MEME_CK"),
    F.col("Strip.IDCD_ID").alias("Strip_IDCD_ID"),
    F.col("Strip.CLMD_LOCK_TOKEN").alias("Strip_CLMD_LOCK_TOKEN"),
    F.col("Strip.CLMD_POA_IND").alias("Strip_CLMD_POA_IND"),
    F.col("Strip.CLMF_ICD_IND_PROC").alias("Strip_CLMF_ICD_IND_PROC"),
    F.col("Strip.CLCL_HIGH_SVC_DT").alias("Strip_CLCL_HIGH_SVC_DT"),
    F.col("fcts_reversals.CLCL_ID").alias("fcts_reversals_CLCL_ID"),
    F.col("nasco_dup_lkup.CLM_ID").alias("nasco_dup_lkup_CLM_ID"),
    F.col("TRGT_CD.SRC_CD").alias("hfDiagExtr_SRC_CD"),
    F.col("TRGT_CD.TRGT_CD").alias("hfDiagExtr_TRGT_CD")
)

df_BusinessRules_vars = df_BusinessRules.withColumn(
    "svClmId",
    trim(F.col("Strip_CLCL_ID"))
).withColumn(
    "svOrdnlCd",
    F.when(F.col("Strip_CLMD_TYPE").substr(F.lit(1),F.lit(1))=="0", F.col("Strip_CLMD_TYPE").substr(F.lit(2),F.lit(1)))
     .otherwise(
       F.when(F.col("Strip_CLMD_TYPE")=="AD","A")
        .otherwise(
          F.when(F.col("Strip_CLMD_TYPE")=="E1","E")
           .otherwise(trim(F.col("Strip_CLMD_TYPE")))
        )
     )
).withColumn(
    "svClmDiagPOA",
    F.when(F.length(trim(F.col("Strip_CLMD_POA_IND")))==0, F.lit("NA"))
     .otherwise(trim(F.upper(F.col("Strip_CLMD_POA_IND"))))
).withColumn(
    "svDiagCdTypCd",
    F.when(F.col("hfDiagExtr_TRGT_CD").isNull(), F.lit(" "))
     .otherwise(F.col("hfDiagExtr_TRGT_CD"))
)

# Output link "FctsClmDiagCd" => constraint: IsNull(nasco_dup_lkup.CLM_ID) = @TRUE
df_FctsClmDiagCd = df_BusinessRules_vars.filter(F.col("nasco_dup_lkup_CLM_ID").isNull())
df_FctsClmDiagCd_out = df_FctsClmDiagCd.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("Strip_EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), strip_field(F.col("svClmId")), F.lit(" ;"), F.col("svOrdnlCd")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_DIAG_SK"),
    F.col("svClmId").alias("CLM_ID"),
    rpad(F.col("svOrdnlCd"),2," ").alias("CLM_DIAG_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.regexp_replace(trim(F.col("Strip_IDCD_ID")), "\\.", "").alias("DIAG_CD"),
    rpad(F.col("svClmDiagPOA"),2," ").alias("CLM_DIAG_POA_CD"),
    F.col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD")
)

# Output link "reversals" => constraint: IsNull(fcts_reversals.CLCL_ID) = @FALSE
df_reversals = df_BusinessRules_vars.filter(~F.col("fcts_reversals_CLCL_ID").isNull())
df_reversals_out = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("Strip_EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), strip_field(F.col("svClmId")), F.lit("R;"), F.col("svOrdnlCd")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_DIAG_SK"),
    F.concat(F.col("svClmId"), F.lit("R")).alias("CLM_ID"),
    rpad(F.col("svOrdnlCd"),2," ").alias("CLM_DIAG_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.regexp_replace(trim(F.col("Strip_IDCD_ID")), "\\.", "").alias("DIAG_CD"),
    rpad(F.col("svClmDiagPOA"),2," ").alias("CLM_DIAG_POA_CD"),
    F.col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD")
)

# 8) Collector => union both outputs
collector_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_DIAG_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "DIAG_CD",
    "CLM_DIAG_POA_CD",
    "DIAG_CD_TYP_CD"
]
df_collector = df_reversals_out.select(collector_cols).unionByName(df_FctsClmDiagCd_out.select(collector_cols))

# 9) Snapshot => pass data to two different output links + one more.
# Columns are the same but reorganized for each link.
# Output: "TransformIt" => V0S15P3
df_Snapshot_TransformIt = df_collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_DIAG_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "DIAG_CD",
    "CLM_DIAG_POA_CD",
    "DIAG_CD_TYP_CD"
).alias("TransformIt")

# Output: "AllCol" => V0S15P2
df_Snapshot_AllCol = df_Snapshot_TransformIt.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    rpad(F.col("TransformIt.CLM_DIAG_ORDNL_CD"),2," ").alias("CLM_DIAG_ORDNL_CD"),
    F.col("TransformIt.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("TransformIt.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.col("TransformIt.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(F.col("TransformIt.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("TransformIt.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("TransformIt.ERR_CT").alias("ERR_CT"),
    F.col("TransformIt.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("TransformIt.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("TransformIt.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("TransformIt.CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("TransformIt.CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("TransformIt.LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("TransformIt.DIAG_CD").alias("DIAG_CD"),
    rpad(F.col("TransformIt.CLM_DIAG_POA_CD"),2," ").alias("CLM_DIAG_POA_CD"),
    F.col("TransformIt.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
).alias("AllColOut")

# Output: "Load" => V0S15P3
df_Snapshot_Load = df_Snapshot_TransformIt.select(
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    rpad(F.col("TransformIt.CLM_DIAG_ORDNL_CD"),2," ").alias("CLM_DIAG_ORDNL_CD")
).alias("Load")

# Output: "Transform" => V0S15P7
df_Snapshot_Transform = df_Snapshot_TransformIt.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("TransformIt.CLM_ID").alias("CLM_ID"),
    rpad(F.col("TransformIt.CLM_DIAG_ORDNL_CD"),2," ").alias("CLM_DIAG_ORDNL_CD")
).alias("Transform")

# 10) Next Transformer => "Transformer" stage
df_Transformer = df_Snapshot_Load.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Load.CLM_ID").alias("CLM_ID"),
    F.expr("GetFkeyCodes('FACETS', 0, 'DIAGNOSIS ORDINAL', Load.CLM_DIAG_ORDNL_CD, 'X')").alias("CLM_DIAG_ORDNL_CD_SK")
).alias("RowCount")

# 11) B_CLM_DIAG => CSeqFileStage => write to .dat
df_B_CLM_DIAG_out = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD_SK"
)
write_files(
    df_B_CLM_DIAG_out,
    f"{adls_path}/load/B_CLM_DIAG.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 12) ClmDiagPK => Shared Container with 2 inputs, 1 output
params_ClmDiagPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_ClmDiagPK_out = ClmDiagPK(df_Snapshot_Transform, df_Snapshot_AllCol, params_ClmDiagPK).alias("Key")

# 13) IdsClmDiag => CSeqFileStage => write final
df_IdsClmDiag_out = df_ClmDiagPK_out.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),  # The container stage mapping set it to 0
    rpad(F.col("Key.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.col("Key.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(F.col("Key.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Key.ERR_CT").alias("ERR_CT"),
    F.col("Key.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    rpad(F.col("Key.CLM_DIAG_ORDNL_CD"),2," ").alias("CLM_DIAG_ORDNL_CD"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("Key.CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("Key.DIAG_CD").alias("DIAG_CD"),
    rpad(F.col("Key.CLM_DIAG_POA_CD"),2," ").alias("CLM_DIAG_POA_CD"),
    F.col("Key.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)
write_files(
    df_IdsClmDiag_out,
    f"{adls_path}/key/FctsClmDiagExtr.FctsClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)