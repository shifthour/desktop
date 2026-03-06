# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmProvExtr
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
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2
# MAGIC 
# MAGIC Kalyan Neelam          2021-01-14     US-318408          Added query with join to PROV_LHO_CRSWALK_D in source odbc                                                                            IntegrateDev2
# MAGIC 
# MAGIC Goutham K               2021-07-13                      US-386524                                  Implemented Facets Upgrade REAS_CD changes            IntegrateDev2    Jeyaprasanna       2021-07-14
# MAGIC                                                                                                                 to mke the job compatable to run pointing to SYBATCH        
# MAGIC Prabhu ES               2022-03-29                       S2S                                             MSSQL ODBC conn params added                                   IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC hash file built in FctsClmExtr
# MAGIC Gives claim numbers for denied/rejected claims so we don't reject for no PCP
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC NascoClmProvTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Split the different provider fields into rows
# MAGIC Hash file (hf_clm_prov_allcol) cleared in the calling program
# MAGIC Pulling Facets Provider Claim Data for a specific Time Period
# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Search provider key file to see if provider exists
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length as spark_length, expr
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW','')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner','')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN','')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
lhofacetsstgowner_secret_name = get_widget_value('lhofacetsstgowner_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT PROV.PROV_ID as PROV_ID, PROV.NTNL_PROV_ID as NTNL_PROV_ID FROM {IDSOwner}.PROV PROV"
df_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

df_hf_fctsclmprov_providlkup = dedup_sort(df_PROV, ["PROV_ID"], [])

df_hf_prov = spark.read.parquet(f"{adls_path}/hf_prov.parquet")
df_hf_clm_denied_rej = spark.read.parquet(f"{adls_path}/hf_clm_finalized_status_denied.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

jdbc_url_lhofacetsstg, jdbc_props_lhofacetsstg = get_db_config(lhofacetsstgowner_secret_name)
extract_query_lhofacetsstg = f"""
SELECT 
A.CLCL_ID, 
A.PRPR_ID, 
'' CLCL_PAYEE_PR_ID, 
'' CLCL_PRPR_ID_PCP, 
A.CLCL_CUR_STS CLCL_CUR_STS,
B.MCTN_ID MCTN_ID, 
A.CLCL_PAY_PR_IND,
A.PDPD_ID PDPD_ID,
'' CLMF_PRPR_FA_NPI
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A, 
     {LhoFacetsStgOwner}.CMC_PRPR_PROV B,
     tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = A.CLCL_ID 
      AND A.PRPR_ID = B.PRPR_ID 
UNION
SELECT 
A.CLCL_ID, 
'' PRPR_ID, 
A.CLCL_PAYEE_PR_ID CLCL_PAYEE_PR_ID, 
'' CLCL_PRPR_ID_PCP, 
A.CLCL_CUR_STS CLCL_CUR_STS,
B.MCTN_ID MCTN_ID, 
A.CLCL_PAY_PR_IND,
A.PDPD_ID PDPD_ID,
'' CLMF_PRPR_FA_NPI
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A, 
     {LhoFacetsStgOwner}.CMC_PRPR_PROV B,
     tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = A.CLCL_ID 
      AND A.CLCL_PAYEE_PR_ID = B.PRPR_ID 
UNION
SELECT 
A.CLCL_ID, 
'' PRPR_ID, 
'' CLCL_PAYEE_PR_ID, 
A.CLCL_PRPR_ID_PCP, 
A.CLCL_CUR_STS CLCL_CUR_STS,
B.MCTN_ID MCTN_ID, 
A.CLCL_PAY_PR_IND,
A.PDPD_ID PDPD_ID,
'' CLMF_PRPR_FA_NPI
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A, 
     {LhoFacetsStgOwner}.CMC_PRPR_PROV B,
     tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = A.CLCL_ID 
      AND A.CLCL_PRPR_ID_PCP = B.PRPR_ID 
UNION
SELECT 
A.CLCL_ID, 
'' PRPR_ID, 
'' CLCL_PAYEE_PR_ID, 
'' CLCL_PRPR_ID_PCP, 
A.CLCL_CUR_STS CLCL_CUR_STS,
'NA' MCTN_ID, 
'' CLCL_PAY_PR_IND,
'' PDPD_ID,
B.CLMF_PRPR_FA_NPI CLMF_PRPR_FA_NPI
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM A, 
     {LhoFacetsStgOwner}.CMC_CLMF_MULT_FUNC B,
     tempdb..{DriverTable} TMP 
WHERE TMP.CLM_ID = A.CLCL_ID 
      AND A.CLCL_ID = B.CLCL_ID
"""
df_CMC_PRPR_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_lhofacetsstg)
    .options(**jdbc_props_lhofacetsstg)
    .option("query", extract_query_lhofacetsstg)
    .load()
)

df_Strip_pre = df_CMC_PRPR_PROV.filter(
    (~col("MCTN_ID").isNull()) & (spark_length(trim(col("MCTN_ID"))) > 0)
)
df_Strip = (
    df_Strip_pre
    .withColumn(
        "CLCL_ID",
        when(col("CLCL_ID").isNull(), lit("")).otherwise(strip_field(col("CLCL_ID")))
    )
    .withColumn(
        "PRPR_ID",
        when(col("PRPR_ID").isNull(), lit("")).otherwise(strip_field(col("PRPR_ID")))
    )
    .withColumn(
        "CLCL_PAYEE_PR_ID",
        when(col("CLCL_PAYEE_PR_ID").isNull(), lit("")).otherwise(strip_field(col("CLCL_PAYEE_PR_ID")))
    )
    .withColumn(
        "CLCL_PRPR_ID_PCP",
        when(col("CLCL_PRPR_ID_PCP").isNull(), lit("")).otherwise(strip_field(col("CLCL_PRPR_ID_PCP")))
    )
    .withColumn(
        "CLCL_PRPR_ID_REF",
        lit("")
    )
    .withColumn(
        "CLCL_CUR_STS",
        strip_field(trim(col("CLCL_CUR_STS")))
    )
    .withColumn(
        "MCTN_ID",
        strip_field(col("MCTN_ID"))
    )
    .withColumn(
        "REF_MCTN_ID",
        lit("")
    )
    .withColumn(
        "CLCL_PAY_PR_IND",
        when(col("CLCL_PAY_PR_IND").isNull(), lit("")).otherwise(strip_field(col("CLCL_PAY_PR_IND")))
    )
    .withColumn(
        "PDPD_ID",
        strip_field(trim(col("PDPD_ID")))
    )
    .withColumn(
        "CLMF_PRPR_FA_NPI",
        when(col("CLMF_PRPR_FA_NPI").isNull(), lit("")).otherwise(strip_field(col("CLMF_PRPR_FA_NPI")))
    )
)

df_BusinessRules_joined = (
    df_Strip.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        [col("Strip.CLCL_ID") == col("fcts_reversals.CLCL_ID")],
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        [col("Strip.CLCL_ID") == col("nasco_dup_lkup.CLM_ID")],
        "left"
    )
    .join(
        df_hf_clm_denied_rej.alias("denied_rej"),
        [col("Strip.CLCL_ID") == col("denied_rej.CLAIM_ID")],
        "left"
    )
    .join(
        df_hf_prov.alias("pcp"),
        [(lit("FACETS") == col("pcp.SRC_SYS_CD")), (col("Strip.CLCL_PRPR_ID_PCP") == col("pcp.PROV_ID"))],
        "left"
    )
    .join(
        df_hf_prov.alias("svc"),
        [(lit("FACETS") == col("svc.SRC_SYS_CD")), (col("Strip.PRPR_ID") == col("svc.PROV_ID"))],
        "left"
    )
    .join(
        df_hf_prov.alias("bill"),
        [(lit("FACETS") == col("bill.SRC_SYS_CD")), (col("Strip.CLCL_PAYEE_PR_ID") == col("bill.PROV_ID"))],
        "left"
    )
    .join(
        df_hf_prov.alias("ref"),
        [(lit("FACETS") == col("ref.SRC_SYS_CD")), (col("Strip.CLCL_PRPR_ID_REF") == col("ref.PROV_ID"))],
        "left"
    )
)

df_BusinessRules_vars = (
    df_BusinessRules_joined
    .withColumn(
        "_ClclCurStsFlag",
        (col("Strip.CLCL_CUR_STS").isin("81","82","93","97","99"))
    )
    .withColumn(
        "_NAorUNK",
        when(col("_ClclCurStsFlag"), lit("NA")).otherwise(lit("UNK"))
    )
    .withColumn(
        "_bIsClosed",
        when(
            expr('instr(",93,97,99,", concat(",",Strip.CLCL_CUR_STS,",")) > 0'),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "_SvcProv",
        when(
            col("_bIsClosed"),
            when(
                col("svc.PROV_ID").isNull(),
                col("_NAorUNK")
            ).otherwise(
                when(
                    (col("Strip.PRPR_ID").isNull()) | (spark_length(trim(col("Strip.PRPR_ID"))) == 0),
                    lit("UNK")
                ).otherwise(trim(col("Strip.PRPR_ID")))
            )
        ).otherwise(
            when(
                (col("Strip.PRPR_ID").isNull()) | (spark_length(trim(col("Strip.PRPR_ID"))) == 0),
                lit("UNK")
            ).otherwise(trim(col("Strip.PRPR_ID")))
        )
    )
    .withColumn(
        "_BillProv",
        when(
            col("_bIsClosed"),
            when(
                col("bill.PROV_ID").isNull(),
                col("_NAorUNK")
            ).otherwise(
                when(
                    (col("Strip.CLCL_PAYEE_PR_ID").isNull()) | (spark_length(trim(col("Strip.CLCL_PAYEE_PR_ID"))) == 0),
                    lit("NA")
                ).otherwise(trim(col("Strip.CLCL_PAYEE_PR_ID")))
            )
        ).otherwise(
            when(
                (col("Strip.CLCL_PAYEE_PR_ID").isNull()) | (spark_length(trim(col("Strip.CLCL_PAYEE_PR_ID"))) == 0),
                lit("NA")
            ).otherwise(trim(col("Strip.CLCL_PAYEE_PR_ID")))
        )
    )
    .withColumn(
        "_PcpProv",
        when(
            col("_bIsClosed"),
            when(
                col("pcp.PROV_ID").isNull(),
                lit("NA")
            ).otherwise(col("Strip.CLCL_PRPR_ID_PCP"))
        ).otherwise(
            when(
                spark_length(trim(col("Strip.CLCL_PRPR_ID_PCP"))) > 0,
                col("Strip.CLCL_PRPR_ID_PCP")
            ).otherwise(
                when(
                    col("Strip.PDPD_ID")[0:1] != lit("B"),
                    lit("NA")
                ).otherwise(
                    when(
                        col("denied_rej.CLAIM_ID").isNull(),
                        lit("UNK")
                    ).otherwise(lit("NA"))
                )
            )
        )
    )
    .withColumn(
        "_RefProv",
        when(
            col("_bIsClosed"),
            when(
                col("ref.PROV_ID").isNull(),
                col("_NAorUNK")
            ).otherwise(
                when(
                    (col("Strip.CLCL_PRPR_ID_REF").isNull()) | (spark_length(trim(col("Strip.CLCL_PRPR_ID_REF"))) == 0),
                    lit("NA")
                ).otherwise(trim(col("Strip.CLCL_PRPR_ID_REF")))
            )
        ).otherwise(
            when(
                (col("Strip.CLCL_PRPR_ID_REF").isNull()) | (spark_length(trim(col("Strip.CLCL_PRPR_ID_REF"))) == 0),
                lit("NA")
            ).otherwise(trim(col("Strip.CLCL_PRPR_ID_REF")))
        )
    )
    .withColumn(
        "_ClmId",
        when(
            (col("Strip.CLCL_ID").isNull()) | (spark_length(trim(col("Strip.CLCL_ID"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("Strip.CLCL_ID")))
    )
    .withColumn(
        "_PkString",
        col("SrcSysCd") + lit(";") + col("_ClmId")
    )
    .withColumn(
        "_CurDateTime",
        lit(CurrDateTime)
    )
)

df_biz_enriched = df_BusinessRules_vars

df_enriched = df_biz_enriched

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)

df_lnkSvcOut = df_enriched.filter(
    (~col("svc.PROV_ID").isNull()) & (col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit(";") + lit("SVC")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    col("_ClmId").alias("CLM_ID"),
    lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_SvcProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkBillOut = df_enriched.filter(
    (~col("bill.PROV_ID").isNull()) & (col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit(";") + lit("BILL")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    col("_ClmId").alias("CLM_ID"),
    lit("BILL").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_BillProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkPCPOut = df_enriched.filter(
    (col("_PcpProv") != lit("NA")) &
    (spark_length(trim(col("Strip.CLCL_PRPR_ID_PCP"))) > 0) &
    (col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit(";") + lit("PCP")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    col("_ClmId").alias("CLM_ID"),
    lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_PcpProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkPayOut = df_enriched.filter(
    (~col("bill.PROV_ID").isNull()) &
    (col("Strip.CLCL_PAY_PR_IND") == lit("P")) &
    (col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit(";") + lit("PD")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    col("_ClmId").alias("CLM_ID"),
    lit("PD").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_BillProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkPayReversalOut = df_enriched.filter(
    (~col("bill.PROV_ID").isNull()) &
    (col("Strip.CLCL_PAY_PR_IND") == lit("P")) &
    (~col("fcts_reversals.CLCL_ID").isNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit("R;") + lit("PD")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    (col("_ClmId") + lit("R")).alias("CLM_ID"),
    lit("PD").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_BillProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkSvcReversalOut = df_enriched.filter(
    (~col("svc.PROV_ID").isNull()) &
    (~col("fcts_reversals.CLCL_ID").isNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit("R;") + lit("SVC")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    (col("_ClmId") + lit("R")).alias("CLM_ID"),
    lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_SvcProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkBillReversalOut = df_enriched.filter(
    (~col("bill.PROV_ID").isNull()) &
    (~col("fcts_reversals.CLCL_ID").isNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit("R;") + lit("BILL")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    (col("_ClmId") + lit("R")).alias("CLM_ID"),
    lit("BILL").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_BillProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkPCPReversalOut = df_enriched.filter(
    (col("_PcpProv") != lit("NA")) &
    (spark_length(trim(col("Strip.CLCL_PRPR_ID_PCP"))) > 0) &
    (~col("fcts_reversals.CLCL_ID").isNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit("R;") + lit("PCP")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    (col("_ClmId") + lit("R")).alias("CLM_ID"),
    lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("_PcpProv").alias("PROV_ID"),
    when(
        (col("Strip.MCTN_ID").isNull()) | (spark_length(trim(col("Strip.MCTN_ID"))) == 0),
        lit("UNK")
    ).otherwise(trim(col("Strip.MCTN_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkSvcFcltyOut = df_enriched.filter(
    (~col("Strip.CLMF_PRPR_FA_NPI").isNull()) &
    (spark_length(trim(col("Strip.CLMF_PRPR_FA_NPI"))) > 0) &
    (col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit(";") + lit("SVCFCLTY")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    col("_ClmId").alias("CLM_ID"),
    lit("SVCFCLTY").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("NA").alias("PROV_ID"),
    lit("NA").alias("TAX_ID"),
    when(
        trim(col("Strip.CLMF_PRPR_FA_NPI")) == lit("UNK"),
        lit("NA")
    ).otherwise(col("Strip.CLMF_PRPR_FA_NPI")).alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_lnkSvcFcltyReversalOut = df_enriched.filter(
    (~col("Strip.CLMF_PRPR_FA_NPI").isNull()) &
    (spark_length(trim(col("Strip.CLMF_PRPR_FA_NPI"))) > 0) &
    (~col("fcts_reversals.CLCL_ID").isNull()) &
    (col("fcts_reversals.CLCL_CUR_STS").isin("89","91","99"))
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    col("_CurDateTime").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),
    (col("_PkString") + lit("R;") + lit("SVCFCLTY")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_PROV_SK"),
    (col("_ClmId") + lit("R")).alias("CLM_ID"),
    lit("SVCFCLTY").alias("CLM_PROV_ROLE_TYP_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("NA").alias("PROV_ID"),
    lit("NA").alias("TAX_ID"),
    when(
        trim(col("Strip.CLMF_PRPR_FA_NPI")) == lit("UNK"),
        lit("NA")
    ).otherwise(col("Strip.CLMF_PRPR_FA_NPI")).alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

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

df_Transformer_56 = df_Collector.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_PROV_SK").alias("CLM_PROV_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("TAX_ID").alias("TAX_ID"),
    col("SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID")
)

df_hf_lhofcts_clmprov_dedupe_pre = df_Transformer_56
df_hf_lhofcts_clmprov_dedupe = dedup_sort(
    df_hf_lhofcts_clmprov_dedupe_pre,
    ["SRC_SYS_CD","CLM_ID","CLM_PROV_ROLE_TYP_CD"],
    []
)

df_SnapShot_primary = df_hf_lhofcts_clmprov_dedupe.alias("Collector")

df_SnapShot_joined = df_SnapShot_primary.join(
    df_hf_fctsclmprov_providlkup.alias("NtnlProvId_lkup"),
    [col("Collector.PROV_ID") == col("NtnlProvId_lkup.PROV_ID")],
    "left"
)

df_SnapShot_AllCol = df_SnapShot_joined.select(
    lit("").alias("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),  
    col("Collector.CLM_ID").alias("CLM_ID"),
    col("Collector.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("Collector.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Collector.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Collector.DISCARD_IN").alias("DISCARD_IN"),
    col("Collector.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Collector.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Collector.ERR_CT").alias("ERR_CT"),
    col("Collector.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Collector.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Collector.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Collector.CLM_PROV_SK").alias("CLM_PROV_SK"),
    col("Collector.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Collector.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Collector.PROV_ID").alias("PROV_ID"),
    col("Collector.TAX_ID").alias("TAX_ID"),
    col("Collector.SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    when(
        col("NtnlProvId_lkup.NTNL_PROV_ID").isNull() |
        (spark_length(col("NtnlProvId_lkup.NTNL_PROV_ID")) == 0) |
        (col("NtnlProvId_lkup.NTNL_PROV_ID").isin("NA","UNK")),
        lit("UNK")
    ).otherwise(col("NtnlProvId_lkup.NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

df_SnapShot_SnapShot = df_SnapShot_joined.select(
    lit("").alias("SRC_SYS_CD_SK"),
    col("Collector.CLM_ID").alias("CLM_ID"),
    col("Collector.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("Collector.PROV_ID").alias("PROV_ID")
)

df_SnapShot_Transform = df_SnapShot_joined.select(
    lit("").alias("SRC_SYS_CD_SK"),
    col("Collector.CLM_ID").alias("CLM_ID"),
    col("Collector.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

params_ClmProvPK = {}
df_ClmProvPK = ClmProvPK(df_SnapShot_AllCol.alias("AllColOut"), df_SnapShot_Transform.alias("Keys"), params_ClmProvPK)

df_FctsClmProvExtr = df_ClmProvPK.select(
    expr("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROV_ID"),
    col("TAX_ID"),
    col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    col("NTNL_PROV_ID")
)

file_path_FctsClmProvExtr = f"{adls_path}/key/LhoFctsClmProvExtr.LhoFctsClmProv.dat.{RunID}"
df_FctsClmProvExtr_final = df_FctsClmProvExtr.select(
    rpad("JOB_EXCTN_RCRD_ERR_SK", 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad("INSRT_UPDT_CD", 10, " ").alias("INSRT_UPDT_CD"),
    rpad("DISCARD_IN", 1, " ").alias("DISCARD_IN"),
    rpad("PASS_THRU_IN", 1, " ").alias("PASS_THRU_IN"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    rpad("SRC_SYS_CD", spark_length(col("SRC_SYS_CD")), " ").alias("SRC_SYS_CD"),
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    rpad("CLM_ID", 18, " ").alias("CLM_ID"),
    rpad("CLM_PROV_ROLE_TYP_CD", 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    rpad("PROV_ID", spark_length(col("PROV_ID")), " ").alias("PROV_ID"),
    rpad("TAX_ID", 9, " ").alias("TAX_ID"),
    rpad("SVC_FCLTY_LOC_NTNL_PROV_ID", spark_length(col("SVC_FCLTY_LOC_NTNL_PROV_ID")), " ").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    rpad("NTNL_PROV_ID", spark_length(col("NTNL_PROV_ID")), " ").alias("NTNL_PROV_ID")
)
write_files(
    df_FctsClmProvExtr_final,
    file_path_FctsClmProvExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer_pre = df_SnapShot_SnapShot.withColumn(
    "svClmProvRole",
    GetFkeyCodes(col("SrcSysCd"), lit(0), lit("CLAIM PROVIDER ROLE TYPE"), col("CLM_PROV_ROLE_TYP_CD"), lit('X'))
)
df_Transformer = df_Transformer_pre

df_Transformer = SurrogateKeyGen(df_Transformer,<DB sequence name>,key_col,<schema>,<secret_name>)

df_RowCount = df_Transformer.select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("svClmProvRole").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    rpad("PROV_ID", 12, " ").alias("PROV_ID")
)

file_path_B_CLM_PROV = f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}"
df_B_CLM_PROV_final = df_RowCount.select(
    rpad("SRC_SYS_CD_SK", spark_length(col("SRC_SYS_CD_SK")), " ").alias("SRC_SYS_CD_SK"),
    rpad("CLM_ID", 18, " ").alias("CLM_ID"),
    rpad("CLM_PROV_ROLE_TYP_CD_SK", spark_length(col("CLM_PROV_ROLE_TYP_CD_SK")), " ").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    rpad("PROV_ID", 12, " ").alias("PROV_ID")
)
write_files(
    df_B_CLM_PROV_final,
    file_path_B_CLM_PROV,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)