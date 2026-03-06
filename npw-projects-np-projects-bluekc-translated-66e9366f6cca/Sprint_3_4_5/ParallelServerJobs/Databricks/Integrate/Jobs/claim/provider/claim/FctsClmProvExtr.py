# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC 
# MAGIC   
# MAGIC PROCESSING:   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC                             Claim status values are hard coded in transform stage "BusinessRules"
# MAGIC                               (Strip.CLCL_CUR_STS = '11') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '15') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '81') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '82') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '93') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '97') OR 
# MAGIC                               (Strip.CLCL_CUR_STS = '99')
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tom Harrocks         03/09/2004                                    Originally Programmed
# MAGIC BJ Luce                  08/2004                                          Release 2.0 - make common record format standard
# MAGIC Tao Luo                  12/20/2005                                    Added Claim Status Code check condition
# MAGIC Steph Goddard       02/14/2006                                     Added transform and primary key steps for sequencer
# MAGIC BJ Luce                  03/20/2006                                     add hf_clm_nasco_dup_bypass, identifies claims that are 
# MAGIC                                                                                        nasco dups. If the claim is on the file, a row is not generated 
# MAGIC                                                                                        for it in IDS. However, an R row will be build for it if the 
# MAGIC                                                                                       status if '91'
# MAGIC Steph Goddard       03/31/2006                                    changed rule for PCP provider to check if an HMO member 
# MAGIC                                                                                       or not by looking at product
# MAGIC Steph Goddard       05/08/2006                                    checked for denied/rejected status before moving 'UNK' to 
# MAGIC                                                                                       pcp_prov - denied/rejected won't have PCP.
# MAGIC                                                                                       Hash file created in clm extract consisting of denied/rej 
# MAGIC                                                                                      claim numbers and checked here
# MAGIC                                                                                      Changed GetFkeyProv() to hash file lookup.
# MAGIC Sanderw                 12/08/2006     Project 1756          Reversal logix added for new status codes 89 and  and 99                    
# MAGIC Brent Leland          05/02/2007      IAD Prod. Supp.    Added current datetime parameters to replace FORMAT.DATE
# MAGIC                                                                                      routine call in transform stage.    
# MAGIC Steph Goddard      08/09/07          IAD Prod Supp      Deleted status 11 and 15 from first stage variable -                        testIDS30   
# MAGIC                                                                                      will allow these claims to show up on web for providers
# MAGIC Oliver Nielsen        08/15/2007      Balancing               Added Balancing Snapshot file                                                      devlIDS30                        Steph Goddard               8/30/07
# MAGIC Ralph Tucker        2008-07-02        3657 Primary Key   Changed primary key from hash file to DB2 table                           devlIDS                            Steph Goddrad              change required
# MAGIC 
# MAGIC Parik                     2008-08-14       3567 Primary Key    Made changes to the container within this job                                devlIDS                             Steph Goddard              08/19/2008
# MAGIC Brent Leland         2009-08-06       Prod Support           Changed SQL logic because Where clause used "AND" that       devlIDSnew                     Steph Goddard               08/19/2009               
# MAGIC                                                                                       excluded records and expect blank row in Facets table. 
# MAGIC                                                                                       Changed transform logic to correctly handle blank data.
# MAGIC 
# MAGIC Madhavan B	2017-06-20   5788 - BHI Updates    Added new column                                                                    IntegrateDev2                 Kalyan Neelam             2017-07-12
# MAGIC                                                                                         SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC 
# MAGIC Ravi Abburi             2018-01-29    5781 - HEDIS            Added the NTNL_PROV_ID                                                      IntegrateDev2                 Kalyan Neelam             2018-05-01
# MAGIC Prabhu ES              2022-02-28    S2S Remediation       MSSQL connection parameters added                                      IntegrateDev5                 Kalyan Neelam             2022-06-10

# MAGIC Search provider key file to see if provider exists
# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Provider Claim Data for a specific Time Period
# MAGIC Split the different provider fields into rows
# MAGIC Hash file (hf_clm_prov_allcol) cleared in the calling program
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC NascoClmProvTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC hash file built in FctsClmExtr
# MAGIC Gives claim numbers for denied/rejected claims so we don't reject for no PCP
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


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Stage: PROV (DB2Connector => df_PROV)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_PROV = f"SELECT PROV.PROV_ID as PROV_ID,PROV.NTNL_PROV_ID as NTNL_PROV_ID FROM {IDSOwner}.PROV PROV"
df_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROV)
    .load()
)

# Stage: hf_fctsclmprov_providlkup (CHashedFileStage, scenario A: deduplicate on key=PROV_ID)
df_hf_fctsclmprov_providlkup = dedup_sort(
    df_PROV,
    partition_cols=["PROV_ID"],
    sort_cols=[("PROV_ID", "A")]
)

# Stage: hf_clm_fcts_reversals (CHashedFileStage, scenario C: read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Stage: clm_nasco_dup_bypass (CHashedFileStage, scenario C: read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Stage: hf_clm_denied_rej (CHashedFileStage, scenario C: read parquet)
df_hf_clm_denied_rej = spark.read.parquet(f"{adls_path}/hf_clm_finalized_status_denied.parquet")

# Stage: hf_prov_svc (CHashedFileStage, scenario C: read parquet)
df_hf_prov_svc = spark.read.parquet(f"{adls_path}/hf_prov.parquet")

# Stage: hf_prov_bill (CHashedFileStage, scenario C: read parquet)
df_hf_prov_bill = spark.read.parquet(f"{adls_path}/hf_prov.parquet")

# Stage: hf_prov_ref (CHashedFileStage, scenario C: read parquet)
df_hf_prov_ref = spark.read.parquet(f"{adls_path}/hf_prov.parquet")

# Stage: IDS_K_PROV (DB2Connector => df_IDS_K_PROV)
query_IDS_K_PROV = (
    f"SELECT SRC_SYS_CD,\n"
    f"       PROV_ID,\n"
    f"       CRT_RUN_CYC_EXCTN_SK,\n"
    f"       PROV_SK \n"
    f"FROM {IDSOwner}.K_PROV\n"
    f"WHERE PROV_SK NOT IN (0,1)"
)
df_IDS_K_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_K_PROV)
    .load()
)

# Stage: hf_prov_pcp (CHashedFileStage, scenario A: deduplicate on key = SRC_SYS_CD, PROV_ID)
df_hf_prov_pcp = dedup_sort(
    df_IDS_K_PROV,
    partition_cols=["SRC_SYS_CD", "PROV_ID"],
    sort_cols=[("SRC_SYS_CD", "A"), ("PROV_ID", "A")]
)

# Stage: CMC_PRPR_PROV (ODBCConnector => df_CMC_PRPR_PROV)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_CMC_PRPR_PROV = (
    f"SELECT \n"
    f"A.CLCL_ID, \n"
    f"A.PRPR_ID, \n"
    f"CAST('' as char(12)) AS CLCL_PAYEE_PR_ID, \n"
    f"'' AS CLCL_PRPR_ID_PCP,\n"
    f"A.CLCL_CUR_STS,\n"
    f"B.MCTN_ID, \n"
    f"CAST(TRIM(A.CLCL_PAY_PR_IND) AS CHAR(1)) AS CLCL_PAY_PR_IND ,\n"
    f"A.PDPD_ID,\n"
    f"'' AS CLMF_PRPR_FA_NPI\n"
    f"FROM {FacetsOwner}.CMC_CLCL_CLAIM A, \n"
    f"     {FacetsOwner}.CMC_PRPR_PROV B,\n"
    f"     tempdb..{DriverTable} TMP \n"
    f"WHERE TMP.CLM_ID = A.CLCL_ID \n"
    f"      AND A.PRPR_ID = B.PRPR_ID \n"
    f"UNION\n"
    f"SELECT \n"
    f"A.CLCL_ID, \n"
    f"'' AS PRPR_ID, \n"
    f"CAST(TRIM(A.CLCL_PAYEE_PR_ID) as char(12)) as CLCL_PAYEE_PR_ID, \n"
    f"'' AS CLCL_PRPR_ID_PCP, \n"
    f"A.CLCL_CUR_STS,\n"
    f"B.MCTN_ID, \n"
    f"CAST(TRIM(A.CLCL_PAY_PR_IND) AS CHAR(1)) AS CLCL_PAY_PR_IND,\n"
    f"A.PDPD_ID,\n"
    f"'' AS CLMF_PRPR_FA_NPI\n"
    f"FROM {FacetsOwner}.CMC_CLCL_CLAIM A, \n"
    f"     {FacetsOwner}.CMC_PRPR_PROV B,\n"
    f"     tempdb..{DriverTable} TMP \n"
    f"WHERE TMP.CLM_ID = A.CLCL_ID \n"
    f"      AND A.CLCL_PAYEE_PR_ID = B.PRPR_ID \n"
    f"UNION\n"
    f"SELECT \n"
    f"A.CLCL_ID, \n"
    f"'' AS PRPR_ID, \n"
    f"CAST(''  as char(12))  AS CLCL_PAYEE_PR_ID, \n"
    f"A.CLCL_PRPR_ID_PCP, \n"
    f"A.CLCL_CUR_STS,\n"
    f"B.MCTN_ID, \n"
    f"CAST(TRIM(A.CLCL_PAY_PR_IND) AS CHAR(1)) AS CLCL_PAY_PR_IND,\n"
    f"A.PDPD_ID,\n"
    f"'' AS CLMF_PRPR_FA_NPI\n"
    f"FROM {FacetsOwner}.CMC_CLCL_CLAIM A, \n"
    f"     {FacetsOwner}.CMC_PRPR_PROV B,\n"
    f"     tempdb..{DriverTable} TMP \n"
    f"WHERE TMP.CLM_ID = A.CLCL_ID \n"
    f"      AND A.CLCL_PRPR_ID_PCP = B.PRPR_ID \n"
    f"UNION\n"
    f"SELECT \n"
    f"A.CLCL_ID, \n"
    f"'' AS PRPR_ID, \n"
    f"CAST('' as char(12)) AS CLCL_PAYEE_PR_ID, \n"
    f"'' AS CLCL_PRPR_ID_PCP, \n"
    f"A.CLCL_CUR_STS,\n"
    f"'NA' AS MCTN_ID, \n"
    f"'' AS CLCL_PAY_PR_IND,\n"
    f"'' AS PDPD_ID,\n"
    f"B.CLMF_PRPR_FA_NPI\n"
    f"FROM {FacetsOwner}.CMC_CLCL_CLAIM A, \n"
    f"     {FacetsOwner}.CMC_CLMF_MULT_FUNC B,\n"
    f"     tempdb..{DriverTable} TMP \n"
    f"WHERE TMP.CLM_ID = A.CLCL_ID \n"
    f"      AND A.CLCL_ID = B.CLCL_ID"
)
df_CMC_PRPR_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CMC_PRPR_PROV)
    .load()
)

# Stage: StripField (CTransformerStage)
df_StripField = df_CMC_PRPR_PROV.filter(
    ~(
        F.col("MCTN_ID").isNull() | 
        (trim(F.col("MCTN_ID")) == "")
    )
).select(
    F.when(
        F.col("CLCL_ID").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("CLCL_ID"))).alias("CLCL_ID"),
    F.when(
        F.col("PRPR_ID").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("PRPR_ID"))).alias("PRPR_ID"),
    F.when(
        F.col("CLCL_PAYEE_PR_ID").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("CLCL_PAYEE_PR_ID"))).alias("CLCL_PAYEE_PR_ID"),
    F.when(
        F.col("CLCL_PRPR_ID_PCP").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("CLCL_PRPR_ID_PCP"))).alias("CLCL_PRPR_ID_PCP"),
    F.lit("").alias("CLCL_PRPR_ID_REF"),
    strip_field(F.col("CLCL_CUR_STS")).alias("CLCL_CUR_STS"),
    strip_field(F.col("MCTN_ID")).alias("MCTN_ID"),
    F.lit("").alias("REF_MCTN_ID"),
    F.when(
        F.col("CLCL_PAY_PR_IND").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("CLCL_PAY_PR_IND"))).alias("CLCL_PAY_PR_IND"),
    strip_field(F.col("PDPD_ID")).alias("PDPD_ID"),
    F.when(
        F.col("CLMF_PRPR_FA_NPI").isNull(),
        F.lit("")
    ).otherwise(strip_field(F.col("CLMF_PRPR_FA_NPI"))).alias("CLMF_PRPR_FA_NPI")
)

# Stage: BusinessRules (CTransformerStage)
df_BusinessRulesJoined = (
    df_StripField.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_clm_denied_rej.alias("denied_rej"),
        F.col("Strip.CLCL_ID") == F.col("denied_rej.CLAIM_ID"),
        "left"
    )
    .join(
        df_hf_prov_pcp.alias("pcp"),
        (F.lit("FACETS") == F.col("pcp.SRC_SYS_CD")) &
        (F.col("Strip.CLCL_PRPR_ID_PCP") == F.col("pcp.PROV_ID")),
        "left"
    )
    .join(
        df_hf_prov_svc.alias("svc"),
        (F.lit("FACETS") == F.col("svc.SRC_SYS_CD")) &
        (F.col("Strip.PRPR_ID") == F.col("svc.PROV_ID")),
        "left"
    )
    .join(
        df_hf_prov_bill.alias("bill"),
        (F.lit("FACETS") == F.col("bill.SRC_SYS_CD")) &
        (F.col("Strip.CLCL_PAYEE_PR_ID") == F.col("bill.PROV_ID")),
        "left"
    )
    .join(
        df_hf_prov_ref.alias("ref"),
        (F.lit("FACETS") == F.col("ref.SRC_SYS_CD")) &
        (F.col("Strip.CLCL_PRPR_ID_REF") == F.col("ref.PROV_ID")),
        "left"
    )
)

df_BusinessRulesVars = (
    df_BusinessRulesJoined
    .withColumn("ClclCurStsFlag", F.col("Strip.CLCL_CUR_STS").isin(["81","82","93","97","99"]))
    .withColumn("NAorUNK", F.when(F.col("ClclCurStsFlag"), F.lit("NA")).otherwise(F.lit("UNK")))
    .withColumn("SrcSys", F.lit("FACETS"))
    .withColumn("bIsClosed", F.col("Strip.CLCL_CUR_STS").isin(["93","97","99"]))
    .withColumn(
        "SvcProv",
        F.when(
            F.col("bIsClosed"),
            F.when(F.col("svc.PROV_ID").isNull(), F.col("NAorUNK"))
             .otherwise(
                F.when(
                    (F.col("Strip.PRPR_ID").isNull()) | (trim(F.col("Strip.PRPR_ID")) == ""),
                    F.lit("UNK")
                )
                .otherwise(trim(F.col("Strip.PRPR_ID")))
             )
        )
        .otherwise(
            F.when(
                (F.col("Strip.PRPR_ID").isNull()) | (trim(F.col("Strip.PRPR_ID")) == ""),
                F.lit("UNK")
            )
            .otherwise(trim(F.col("Strip.PRPR_ID")))
        )
    )
    .withColumn(
        "BillProv",
        F.when(
            F.col("bIsClosed"),
            F.when(F.col("bill.PROV_ID").isNull(), F.col("NAorUNK"))
             .otherwise(
                F.when(
                    (F.col("Strip.CLCL_PAYEE_PR_ID").isNull()) | (trim(F.col("Strip.CLCL_PAYEE_PR_ID")) == ""),
                    F.lit("NA")
                )
                .otherwise(trim(F.col("Strip.CLCL_PAYEE_PR_ID")))
             )
        )
        .otherwise(
            F.when(
                (F.col("Strip.CLCL_PAYEE_PR_ID").isNull()) | (trim(F.col("Strip.CLCL_PAYEE_PR_ID")) == ""),
                F.lit("NA")
            )
            .otherwise(trim(F.col("Strip.CLCL_PAYEE_PR_ID")))
        )
    )
    .withColumn(
        "PcpProv",
        F.when(
            F.col("bIsClosed"),
            F.when(F.col("pcp.PROV_ID").isNull(), F.lit("NA"))
             .otherwise(F.col("Strip.CLCL_PRPR_ID_PCP"))
        )
        .otherwise(
            F.when(
                F.length(trim(F.col("Strip.CLCL_PRPR_ID_PCP"))) > 0,
                F.col("Strip.CLCL_PRPR_ID_PCP")
            )
            .otherwise(
                F.when(
                    F.col("Strip.PDPD_ID")[0:1] != F.lit("B"),
                    F.lit("NA")
                )
                .otherwise(
                    F.when(F.col("denied_rej.CLAIM_ID").isNull(), F.lit("UNK")).otherwise(F.lit("NA"))
                )
            )
        )
    )
    .withColumn(
        "RefProv",
        F.when(
            F.col("bIsClosed"),
            F.when(F.col("ref.PROV_ID").isNull(), F.col("NAorUNK"))
             .otherwise(
                F.when(
                    (F.col("Strip.CLCL_PRPR_ID_REF").isNull()) | (trim(F.col("Strip.CLCL_PRPR_ID_REF")) == ""),
                    F.lit("NA")
                )
                .otherwise(trim(F.col("Strip.CLCL_PRPR_ID_REF")))
             )
        )
        .otherwise(
            F.when(
                (F.col("Strip.CLCL_PRPR_ID_REF").isNull()) | (trim(F.col("Strip.CLCL_PRPR_ID_REF")) == ""),
                F.lit("NA")
            )
            .otherwise(trim(F.col("Strip.CLCL_PRPR_ID_REF")))
        )
    )
    .withColumn(
        "ClmId",
        F.when(
            (F.col("Strip.CLCL_ID").isNull()) | (trim(F.col("Strip.CLCL_ID")) == ""),
            F.lit("UNK")
        )
        .otherwise(trim(F.col("Strip.CLCL_ID")))
    )
    .withColumn("PkString", F.concat(F.col("SrcSys"), F.lit(";"), F.col("ClmId")))
    .withColumn("CurDateTime", F.lit(CurrDateTime))
)

# Create dataframes for each output link constraint, preserving column order
cols_for_collector = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID"
]

df_lnkSvcOut = (
    df_BusinessRulesVars
    .filter(~F.col("svc.PROV_ID").isNull() & F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit(";"), F.lit("SVC")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SvcProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkBillOut = (
    df_BusinessRulesVars
    .filter(~F.col("bill.PROV_ID").isNull() & F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit(";"), F.lit("BILL")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit("BILL").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BillProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkPCPOut = (
    df_BusinessRulesVars
    .filter(
        (F.col("PcpProv") != "NA") &
        (F.length(trim(F.col("Strip.CLCL_PRPR_ID_PCP"))) > 0) &
        F.col("nasco_dup_lkup.CLM_ID").isNull()
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit(";"), F.lit("PCP")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PcpProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkPayOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("bill.PROV_ID").isNull() &
        (F.col("Strip.CLCL_PAY_PR_IND") == "P") &
        F.col("nasco_dup_lkup.CLM_ID").isNull()
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit(";"), F.lit("PD")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit("PD").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BillProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkPayReversalOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("bill.PROV_ID").isNull() &
        (F.col("Strip.CLCL_PAY_PR_IND") == "P") &
        (~F.col("fcts_reversals.CLCL_ID").isNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit("R;"), F.lit("PD")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit("PD").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BillProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkSvcReversalOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("svc.PROV_ID").isNull() &
        (~F.col("fcts_reversals.CLCL_ID").isNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit("R;"), F.lit("SVC")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SvcProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkBillReversalOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("bill.PROV_ID").isNull() &
        (~F.col("fcts_reversals.CLCL_ID").isNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit("R;"), F.lit("BILL")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit("BILL").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("BillProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkPCPReversalOut = (
    df_BusinessRulesVars
    .filter(
        (F.col("PcpProv") != "NA") &
        (F.length(trim(F.col("Strip.CLCL_PRPR_ID_PCP"))) > 0) &
        (~F.col("fcts_reversals.CLCL_ID").isNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit("R;"), F.lit("PCP")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PcpProv").alias("PROV_ID"),
        F.when(
            F.col("Strip.MCTN_ID").isNull() | (trim(F.col("Strip.MCTN_ID")) == ""),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.MCTN_ID"))).alias("TAX_ID"),
        F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkSvcFcltyOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("Strip.CLMF_PRPR_FA_NPI").isNull() &
        (F.length(trim(F.col("Strip.CLMF_PRPR_FA_NPI"))) > 0) &
        F.col("nasco_dup_lkup.CLM_ID").isNull()
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit(";"), F.lit("SVCFCLTY")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.lit("SVCFCLTY").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("PROV_ID"),
        F.lit("NA").alias("TAX_ID"),
        F.when(
            trim(F.col("Strip.CLMF_PRPR_FA_NPI")) == "UNK",
            F.lit("NA")
        ).otherwise(F.col("Strip.CLMF_PRPR_FA_NPI")).alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

df_lnkSvcFcltyReversalOut = (
    df_BusinessRulesVars
    .filter(
        ~F.col("Strip.CLMF_PRPR_FA_NPI").isNull() &
        (F.length(trim(F.col("Strip.CLMF_PRPR_FA_NPI"))) > 0) &
        (~F.col("fcts_reversals.CLCL_ID").isNull()) &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurDateTime").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSys").alias("SRC_SYS_CD"),
        F.concat(F.col("PkString"), F.lit("R;"), F.lit("SVCFCLTY")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_PROV_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.lit("SVCFCLTY").alias("CLM_PROV_ROLE_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("NA").alias("PROV_ID"),
        F.lit("NA").alias("TAX_ID"),
        F.when(
            trim(F.col("Strip.CLMF_PRPR_FA_NPI")) == "UNK",
            F.lit("NA")
        ).otherwise(F.col("Strip.CLMF_PRPR_FA_NPI")).alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
    )
)

# Stage: Collector (CCollector => union all these links)
df_Collector = (
    df_lnkSvcOut
    .unionByName(df_lnkBillOut)
    .unionByName(df_lnkPCPOut)
    .unionByName(df_lnkPayOut)
    .unionByName(df_lnkSvcReversalOut)
    .unionByName(df_lnkBillReversalOut)
    .unionByName(df_lnkPCPReversalOut)
    .unionByName(df_lnkPayReversalOut)
    .unionByName(df_lnkSvcFcltyOut)
    .unionByName(df_lnkSvcFcltyReversalOut)
)

# Stage: SnapShot (CTransformerStage) with left lookup on df_hf_fctsclmprov_providlkup => "Collector.PROV_ID = NtnlProvId_lkup.PROV_ID"
df_SnapShotJoin = df_Collector.alias("Collector").join(
    df_hf_fctsclmprov_providlkup.alias("NtnlProvId_lkup"),
    F.col("Collector.PROV_ID") == F.col("NtnlProvId_lkup.PROV_ID"),
    "left"
)

# Output pin: "AllCol" => to ClmProvPK
df_SnapShot_AllCol = df_SnapShotJoin.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Collector.CLM_ID").alias("CLM_ID"),
    F.col("Collector.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("Collector.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Collector.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Collector.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Collector.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Collector.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Collector.ERR_CT").alias("ERR_CT"),
    F.col("Collector.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Collector.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Collector.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Collector.CLM_PROV_SK").alias("CLM_PROV_SK"),
    F.col("Collector.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Collector.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Collector.PROV_ID").alias("PROV_ID"),
    F.col("Collector.TAX_ID").alias("TAX_ID"),
    F.col("Collector.SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.when(
        F.col("NtnlProvId_lkup.NTNL_PROV_ID").isNull() |
        (F.length(F.col("NtnlProvId_lkup.NTNL_PROV_ID")) == 0) |
        (F.col("NtnlProvId_lkup.NTNL_PROV_ID") == "NA") |
        (F.col("NtnlProvId_lkup.NTNL_PROV_ID") == "UNK"),
        F.lit("UNK")
    ).otherwise(F.col("NtnlProvId_lkup.NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

# Output pin: "SnapShot" => to "Transformer"
df_SnapShot_SnapShot = df_SnapShotJoin.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("Collector.CLM_ID"), 18, " ").alias("CLM_ID"),
    F.rpad(F.col("Collector.CLM_PROV_ROLE_TYP_CD"), 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
    F.rpad(F.col("Collector.PROV_ID"), 12, " ").alias("PROV_ID")
)

# Output pin: "Transform" => to ClmProvPK
df_SnapShot_Transform = df_SnapShotJoin.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Collector.CLM_ID").alias("CLM_ID"),
    F.col("Collector.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD")
)

# Stage: Transformer => input: df_SnapShot_SnapShot => columns => "RowCount"
df_RowCount = df_SnapShot_SnapShot.withColumn(
    "svClmProvRole",
    GetFkeyCodes(
        F.lit("FACETS"),
        F.lit(0),
        F.lit("CLAIM PROVIDER ROLE TYPE"),
        F.col("CLM_PROV_ROLE_TYP_CD"),
        F.lit("X")
    )
).select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svClmProvRole").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
)

# Stage: B_CLM_PROV (CSeqFileStage)
df_B_CLM_PROV_out = df_RowCount.select(
    "SRC_SYS_CD_SK",
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    "CLM_PROV_ROLE_TYP_CD_SK",
    F.rpad(F.col("PROV_ID"), 12, " ").alias("PROV_ID")
)
write_files(
    df_B_CLM_PROV_out,
    f"{adls_path}/load/B_CLM_PROV.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: ClmProvPK (CContainerStage)
params_ClmProvPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "$IDSOwner": IDSOwner
}
df_ClmProvPK = ClmProvPK(df_SnapShot_AllCol, df_SnapShot_Transform, params_ClmProvPK)

# Stage: FctsClmProvExtr (CSeqFileStage)
df_FctsClmProvExtr_out = df_ClmProvPK.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK").alias("CLM_PROV_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID")
)
write_files(
    df_FctsClmProvExtr_out,
    f"{adls_path}/key/FctsClmProvExtr.FctsClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)