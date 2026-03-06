# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnClnclEditExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDCE_LI_EDIT  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDCE_LI_EDIT
# MAGIC                 Joined to
# MAGIC                 TmpClaim to get specific records by date
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:    Output file is created with a temp. name.  
# MAGIC 
# MAGIC OUTPUTS:   Sequential file .
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC BJ Luce                   06/2004                                         Originally Programmed
# MAGIC SAndrew                  08 /2004                                        IDS 2.0 - changed clncl_edit_fmt_chg_cd_sk to CLM_LN_CLNCL_EDIT_FMT_CHG_CD_SK #1158
# MAGIC Suzanne Saylor       03/01/2006                                    Changed to combine extract, transform, pkey for sequencer 
# MAGIC BJ Luce                   03/20/2006                                     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                 12/08/2006                                     Project 1576  - Reversal logix added for new status codes 89 and  and 99
# MAGIC O. Nielsen                08/15/2007      Balancing              Add Snapshot extract for Balancing                                            devlIDS30                       Steph Goddard          8/30/07
# MAGIC 
# MAGIC Parik                         2008-08-06      3567(Primary Key)  Added the new primary keying process                                       devlIDS                            Steph Goddard         08/12/2008
# MAGIC Dan Long                11/22/2013      TFS-1114              Modified the Derivations for the PRI_KEY_STRING                    IntegrateNewDevl        Kalyan Neelam              2013-11-26
# MAGIC                                                                                        and CLM_LN_SEQ_NO in the ClmLnClnclEdit and 
# MAGIC                                                                                        reversals transformer stages to use Strip.CDML_SEQ_NO
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Nathan Reynolds   24 Oct 2016     TFS 13129              Change CLNCL_EDIT_TYPE_CD to be NA if before march     IntergrateDev2                  Jag Yelavarthi             2016-11-16
# MAGIC                                                                                        26 2016 (change in codes in source system on this date)
# MAGIC                                                                                          Add new field, CLNCL_DEIT_EXCD_CD for the code 
# MAGIC                                                                                             after this time.
# MAGIC 
# MAGIC Manasa Andru         2017-03/27      TFS - 18779          Updated the logic in the field COMBND_CHRG_IN                  IntegrateDev1                    Nathan Reynolds        2017-3-31
# MAGIC                                                                                     as per the mapping(N if the source value is blank/null else Y)
# MAGIC 
# MAGIC Emran.Mohammad    2021-11-22                                   Updated logic in the field EXCD_EDIT_TYPE as per the mapping  IntegrateDev2           Goutham Kalidindi      2021-11-29
# MAGIC                                                                                        and new requirement
# MAGIC Prabhu ES               2022-06-09               S2S              MSSQL ODBC conn params added                                                   IntegrateDev5                        Manasa Andru             2022-06-10

# MAGIC Balancing
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract Facets Claim Line Clinical Edit Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC This container is used in:
# MAGIC FctsClmLnClnclEditExtr
# MAGIC NascoClmLnClnclEditTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnClnclEditPK
# COMMAND ----------

RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
facets_secret_name = get_widget_value('facets_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDateParam = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
EditTypeCDSysChgDate = get_widget_value('EditTypeCDSysChgDate','')

hf_clm_fcts_reversals_schema = StructType([
    StructField("CLCL_ID", StringType(), nullable=False),
    StructField("CLCL_CUR_STS", StringType(), nullable=False),
    StructField("CLCL_PAID_DT", TimestampType(), nullable=False),
    StructField("CLCL_ID_ADJ_TO", StringType(), nullable=False),
    StructField("CLCL_ID_ADJ_FROM", StringType(), nullable=False)
])
df_hf_clm_fcts_reversals = spark.read.schema(hf_clm_fcts_reversals_schema).parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

clm_nasco_dup_bypass_schema = StructType([
    StructField("CLM_ID", StringType(), nullable=False)
])
df_clm_nasco_dup_bypass = spark.read.schema(clm_nasco_dup_bypass_schema).parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""SELECT CL_LINE.CLCL_ID,
CL_LINE.CDML_SEQ_NO,
CL_LINE.MEME_CK,
CL_LINE.CDCE_EDIT_ACT,
CL_LINE.CDCE_EDIT_TYPE,
CL_LINE.CDCE_FMT_IND,
CL_LINE.CDCE_COMB_EXCD,
CL_LINE.CDCE_CLCL_ID,
CL_LINE.CDCE_CDML_SEQ_NO,
CL_LINE.CDCE_LOCK_TOKEN,
CL_LINE.ATXR_SOURCE_ID,
CL.CLCL_ACPT_DTM,
CL_LINE.CDCE_VEND_EDIT_IND
FROM {FacetsOwner}.CMC_CDCE_LI_EDIT CL_LINE 
         INNER JOIN tempdb..{DriverTable}  TMP ON
             TMP.CLM_ID = CL_LINE.CLCL_ID
         INNER JOIN {FacetsOwner}.CMC_CLCL_CLAIM CL
           ON CL_LINE.CLCL_ID = CL.CLCL_ID
"""
df_CMC_CDCE_LI_EDIT1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_tTrimFields = (
    df_CMC_CDCE_LI_EDIT1
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CDML_SEQ_NO", F.col("CDML_SEQ_NO"))
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("CDCE_EDIT_ACT", strip_field(F.col("CDCE_EDIT_ACT")))
    .withColumn(
        "CDCE_EDIT_TYPE",
        F.when(
            F.col("CLCL_ACPT_DTM") < F.to_timestamp(F.lit(EditTypeCDSysChgDate), "yyyy-MM-dd"),
            strip_field(F.col("CDCE_EDIT_TYPE"))
        ).otherwise("NA")
    )
    .withColumn("CDCE_FMT_IND", strip_field(F.col("CDCE_FMT_IND")))
    .withColumn("CDCE_COMB_EXCD", strip_field(F.col("CDCE_COMB_EXCD")))
    .withColumn("CDCE_CLCL_ID", strip_field(F.col("CDCE_CLCL_ID")))
    .withColumn("CDCE_CDML_SEQ_NO", F.col("CDCE_CDML_SEQ_NO"))
    .withColumn("CDCE_LOCK_TOKEN", F.col("CDCE_LOCK_TOKEN"))
    .withColumn(
        "EXCD_EDIT_TYPE",
        F.when(
            (F.col("CLCL_ACPT_DTM") >= F.to_timestamp(F.lit(EditTypeCDSysChgDate), "yyyy-MM-dd"))
            & (F.col("CDCE_VEND_EDIT_IND").isNotNull())
            & (F.length(F.trim(F.col("CDCE_VEND_EDIT_IND"))) > 0),
            F.concat(F.col("CDCE_VEND_EDIT_IND"), F.col("CDCE_EDIT_TYPE"))
        ).otherwise("NA")
    )
    .withColumn("ATXR_SOURCE_ID", F.col("ATXR_SOURCE_ID"))
)

df_businessRules_joined = (
    df_tTrimFields.alias("Strip")
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
    .withColumn(
        "ClmId",
        F.when(
            (F.col("Strip.CLCL_ID").isNotNull()) & (F.trim(F.col("Strip.CLCL_ID")) != ""),
            F.trim(F.col("Strip.CLCL_ID"))
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "ClmLnId",
        F.when(
            (F.col("Strip.CDML_SEQ_NO").isNotNull()) & (F.trim(F.col("Strip.CDML_SEQ_NO")) != ""),
            F.col("Strip.CDML_SEQ_NO")
        ).otherwise(F.lit("NA"))
    )
    .withColumn("SrcSysCd", F.lit("FACETS"))
)

df_ClmLnClnclEdit = (
    df_businessRules_joined
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("Strip.CDCE_EDIT_ACT").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_EDIT_ACT"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_EDIT_ACT")))).alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
        F.when(
            (F.col("Strip.CDCE_FMT_IND").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_FMT_IND"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_FMT_IND")))).alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
        F.when(
            (F.col("Strip.CDCE_EDIT_TYPE").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_EDIT_TYPE"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_EDIT_TYPE")))).alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
        F.when(
            (F.col("Strip.CDCE_COMB_EXCD").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_COMB_EXCD"))) == 0),
            "N"
        ).otherwise(F.lit("Y")).alias("COMBND_CHRG_IN"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (F.trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CLCL_ID")
        ).otherwise(F.lit("NA")).alias("REF_CLM_ID"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (F.trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CDML_SEQ_NO")
        ).otherwise(F.lit(0)).alias("REF_CLM_LN_SEQ_NO"),
        F.trim(F.col("Strip.EXCD_EDIT_TYPE")).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
    )
)

df_reversals = (
    df_businessRules_joined
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("Strip.CDCE_EDIT_ACT").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_EDIT_ACT"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_EDIT_ACT")))).alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
        F.when(
            (F.col("Strip.CDCE_FMT_IND").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_FMT_IND"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_FMT_IND")))).alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
        F.when(
            (F.col("Strip.CDCE_EDIT_TYPE").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_EDIT_TYPE"))) == 0),
            "NA"
        ).otherwise(F.upper(F.trim(F.col("Strip.CDCE_EDIT_TYPE")))).alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
        F.when(
            (F.col("Strip.CDCE_COMB_EXCD").isNull())
            | (F.length(F.trim(F.col("Strip.CDCE_COMB_EXCD"))) == 0),
            "N"
        ).otherwise(F.lit("Y")).alias("COMBND_CHRG_IN"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (F.trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CLCL_ID")
        ).otherwise(F.lit("NA")).alias("REF_CLM_ID"),
        F.when(
            (F.col("Strip.CDCE_CLCL_ID").isNotNull()) & (F.trim(F.col("Strip.CDCE_CLCL_ID")) != ""),
            F.col("Strip.CDCE_CDML_SEQ_NO")
        ).otherwise(F.lit(0)).alias("REF_CLM_LN_SEQ_NO"),
        F.trim(F.col("Strip.EXCD_EDIT_TYPE")).alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD")
    )
)

df_collector = df_reversals.unionByName(df_ClmLnClnclEdit)

df_SnapShot_in = df_collector

df_Pkey = df_SnapShot_in.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_CLNCL_EDIT_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_CLNCL_EDIT_ACTN_CD",
    "CLM_LN_CLNCL_EDIT_FMT_CHG_CD",
    "CLM_LN_CLNCL_EDIT_TYP_CD",
    "COMBND_CHRG_IN",
    "REF_CLM_ID",
    "REF_CLM_LN_SEQ_NO",
    "CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"
)

df_Pkey = df_Pkey.withColumn(
    "CLM_LN_CLNCL_EDIT_EXCD_TYP_CD",
    F.when(
        (F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD") == "")
        | (F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD").isNull()),
        "NA"
    ).otherwise(F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"))
)

df_B_CLM_LN_CLNCL_EDIT = df_SnapShot_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    "CLM_ID",
    "CLM_LN_SEQ_NO"
).withColumn(
    "CLM_ID",
    F.rpad(F.col("CLM_ID"), 18, " ")
)

write_files(
    df_B_CLM_LN_CLNCL_EDIT,
    f"{adls_path}/load/B_CLM_LN_CLNCL_EDIT.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmLnClnclEditPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmLnClnclEditPK = ClmLnClnclEditPK(df_Pkey, params_ClmLnClnclEditPK)

df_FctsClmLnClnclEdit = df_ClmLnClnclEditPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_CLNCL_EDIT_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_CLNCL_EDIT_ACTN_CD",
    "CLM_LN_CLNCL_EDIT_FMT_CHG_CD",
    "CLM_LN_CLNCL_EDIT_TYP_CD",
    "COMBND_CHRG_IN",
    "REF_CLM_ID",
    "REF_CLM_LN_SEQ_NO",
    "CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"
)

df_FctsClmLnClnclEdit_final = (
    df_FctsClmLnClnclEdit
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("COMBND_CHRG_IN", F.rpad(F.col("COMBND_CHRG_IN"), 1, " "))
    .withColumn("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD", F.rpad(F.col("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"), 3, " "))
)

write_files(
    df_FctsClmLnClnclEdit_final,
    f"{adls_path}/key/FctsClmLnClnclEditExtr.FctsClmLnClnclEdit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)