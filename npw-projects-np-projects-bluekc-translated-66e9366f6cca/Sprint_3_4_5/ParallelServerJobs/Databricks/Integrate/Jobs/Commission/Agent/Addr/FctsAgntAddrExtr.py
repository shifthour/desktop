# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsAgntAddrExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COAD_ADDDRESS for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COAD_ADDDRESS 
# MAGIC 
# MAGIC 
# MAGIC 3;hf_agnt_addr_state_cd;hf_agent_addr_extr_bbb_cur_email;hf_agent_addr_extr_bbb_orig_email
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:   hf_agnt_addr
# MAGIC                         hf_agnt_addr_state_cd
# MAGIC hf_agent_addr_extr_bbb_cur_email;hf_agent_addr_extr_bbb_orig_email
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/11/2005  -  Originally Programmed
# MAGIC              Sharon Andrew    09/01/2006   -  For the Agent Address table,  Added BBB table view  AGENT_DATA_VIEW to get the EMAIL_ADDR_TX.  This field was being sourced from facets
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                                                       ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/6/2007          3264                              Added Balancing process to the overall                                                                                  devlIDS30                 Steph Goddard            09/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                            
# MAGIC 
# MAGIC Bhoomi Dasari                 2008-09-10        3567                           Changed primary key process from hash file to DB2 table                                                          devlIDS                      Steph Goddard             10/03/2008      
# MAGIC 
# MAGIC Manasa Andru                 2013-09-26        TTR - 1194                 Updated  AGNT_ADDR_CTRY_CD field                                                                                 IntegrateNewDevl       Kalyan Neelam             2013-10-04
# MAGIC                                                                                                   and added several stages to update the job as per
# MAGIC                                                                                               the mapping(to avoid duplicates differing by lettercase)
# MAGIC                                                   
# MAGIC 
# MAGIC Jag Yelavarthi                 2015-11-13         TTR#10776            Corrected  user defined SQL in CD_MPPNG Stage. It was incorrectly using                               IntegrateDev1            Kalyan Neelam              2015-11-17
# MAGIC                                                                                                   #IDSInstance# as the Database Owner in user defined SQL. Modified to use
# MAGIC                                                                                                   #IDSOwner#.   
# MAGIC 
# MAGIC Manasa Andru               2016-11-30          TFS - 10785            Added Snapshot file to the Transform1 stage to reflect the right number of records                     IntegrateDev1             Jag Yelavarthi               2016-12-07
# MAGIC                                                                                                            that are loaded into the base table.
# MAGIC Prabhu ES                     2022-03-01          S2S Remediation     MSSQL connection parameters added                                                                                       IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim leading and trailing blanks.
# MAGIC Extract Facets Agent Address Data
# MAGIC Apply business logic.
# MAGIC Extracts State codes
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/AgntAddrPK
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all parameter values and also define <database>_secret_name where applicable
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
BBBOwner = get_widget_value('BBBOwner','')
bbb_secret_name = get_widget_value('bbb_secret_name','')

# CD_MPPNG (DB2Connector) => df_CD_MPPNG
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_cd_mppng = (
    "SELECT SRC_CD,TRGT_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    "WHERE SRC_DOMAIN_NM = 'STATE' AND SRC_SYS_CD = 'FACETS'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_cd_mppng)
    .load()
)

# hf_agnt_addr_state_cd (Scenario A) => drop duplicates on key: SRC_CD
df_hf_agnt_addr_state_cd = df_CD_MPPNG.dropDuplicates(["SRC_CD"])

# IDS_AGNT (DB2Connector) => df_IDS_AGNT
query_ids_agnt = (
    "SELECT AGNT.AGNT_ID, max(AGNT.TERM_DT_SK) as TERM_DT_SK "
    f"FROM {IDSOwner}.AGNT AGNT "
    "GROUP BY AGNT.AGNT_ID "
    "ORDER BY UPPER(AGNT.AGNT_ID), max(AGNT.TERM_DT_SK), AGNT.AGNT_ID"
)
df_IDS_AGNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_ids_agnt)
    .load()
)

# hf_agnt_id_lkup (Scenario A) => drop duplicates on key: AGNT_ID
df_hf_agnt_id_lkup = df_IDS_AGNT.dropDuplicates(["AGNT_ID"])

# CMC_COAD_ADDRESS (ODBCConnector) => df_CMC_COAD_ADDRESS
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
query_cmc_coad = (
    f"SELECT COCE_ID,COAD_TYPE,COAD_ADDR1,COAD_ADDR2,COAD_ADDR3,COAD_CITY,COAD_STATE,COAD_ZIP,"
    f"COAD_COUNTY,COAD_CTRY_CD,COAD_PHONE,COAD_PHONE_EXT,COAD_FAX,COAD_FAX_EXT,COAD_EMAIL "
    f"FROM {FacetsOwner}.CMC_COAD_ADDRESS"
)
df_CMC_COAD_ADDRESS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_cmc_coad)
    .load()
)

# StripField (CTransformerStage) => df_StripField
df_StripField = (
    df_CMC_COAD_ADDRESS
    .withColumn(
        "COCE_ID",
        F.when(
            (F.col("COCE_ID").isNull()) | (F.length(trim(F.col("COCE_ID"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COCE_ID"))))
    )
    .withColumn(
        "COAD_TYPE",
        F.when(
            (F.col("COAD_TYPE").isNull()) | (F.length(trim(F.col("COAD_TYPE"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_TYPE"))))
    )
    .withColumn(
        "COAD_ADDR1",
        F.when(
            (F.col("COAD_ADDR1").isNull()) | (F.length(trim(F.col("COAD_ADDR1"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_ADDR1"))))
    )
    .withColumn(
        "COAD_ADDR2",
        F.when(
            (F.col("COAD_ADDR2").isNull()) | (F.length(trim(F.col("COAD_ADDR2"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_ADDR2"))))
    )
    .withColumn(
        "COAD_ADDR3",
        F.when(
            (F.col("COAD_ADDR3").isNull()) | (F.length(trim(F.col("COAD_ADDR3"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_ADDR3"))))
    )
    .withColumn(
        "COAD_CITY",
        F.when(
            (F.col("COAD_CITY").isNull()) | (F.length(trim(F.col("COAD_CITY"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_CITY"))))
    )
    .withColumn(
        "COAD_STATE",
        F.when(
            (F.col("COAD_STATE").isNull()) | (F.length(trim(F.col("COAD_STATE"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_STATE"))))
    )
    .withColumn(
        "COAD_ZIP",
        F.when(
            (F.col("COAD_ZIP").isNull()) | (F.length(trim(F.col("COAD_ZIP"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_ZIP"))))
    )
    .withColumn(
        "COAD_COUNTY",
        F.when(
            (F.col("COAD_COUNTY").isNull()) | (F.length(trim(F.col("COAD_COUNTY"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_COUNTY"))))
    )
    .withColumn(
        "COAD_CTRY_CD",
        F.when(
            (F.col("COAD_CTRY_CD").isNull()) | (F.length(trim(F.col("COAD_CTRY_CD"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_CTRY_CD"))))
    )
    .withColumn(
        "COAD_PHONE",
        F.when(
            (F.col("COAD_PHONE").isNull()) | (F.length(trim(F.col("COAD_PHONE"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_PHONE"))))
    )
    .withColumn(
        "COAD_PHONE_EXT",
        F.when(
            (F.col("COAD_PHONE_EXT").isNull()) | (F.length(trim(F.col("COAD_PHONE_EXT"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_PHONE_EXT"))))
    )
    .withColumn(
        "COAD_FAX",
        F.when(
            (F.col("COAD_FAX").isNull()) | (F.length(trim(F.col("COAD_FAX"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_FAX"))))
    )
    .withColumn(
        "COAD_FAX_EXT",
        F.when(
            (F.col("COAD_FAX_EXT").isNull()) | (F.length(trim(F.col("COAD_FAX_EXT"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_FAX_EXT"))))
    )
    .withColumn(
        "COAD_EMAIL",
        F.when(
            (F.col("COAD_EMAIL").isNull()) | (F.length(trim(F.col("COAD_EMAIL"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("COAD_EMAIL"))))
    )
)

# BlueBusiness (CODBCStage) => current_email, orig_email
jdbc_url_bbb, jdbc_props_bbb = get_db_config(bbb_secret_name)
query_current_email = (
    f"SELECT SRC_SYS_ASG_ID, EMAIL_ADDR_STRING "
    f"FROM {BBBOwner}.AGENT_DATA_VIEW "
    "WHERE len(ltrim(rtrim(EMAIL_ADDR_STRING))) > 0"
)
df_bluebusiness_current_email = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bbb)
    .options(**jdbc_props_bbb)
    .option("query", query_current_email)
    .load()
)

query_orig_email = (
    f"SELECT SRC_SYS_ASG_ID, ORIG_EMAIL_ADDR_STRING "
    f"FROM {BBBOwner}.AGENT_DATA_VIEW "
    "WHERE len(ltrim(rtrim(ORIG_EMAIL_ADDR_STRING))) > 0"
)
df_bluebusiness_orig_email = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bbb)
    .options(**jdbc_props_bbb)
    .option("query", query_orig_email)
    .load()
)

# Transformer_68 => df_Transformer_68
df_Transformer_68 = (
    df_bluebusiness_current_email
    .withColumn("SRC_SYS_ASG_ID", trim(F.col("SRC_SYS_ASG_ID")))
    .withColumn("EMAIL_ADDR_STRING", F.col("EMAIL_ADDR_STRING"))
)

# hf_agent_addr_extr_emails (Scenario A) for current_email => key = SRC_SYS_ASG_ID
df_cur_email = df_Transformer_68.dropDuplicates(["SRC_SYS_ASG_ID"])

# Transformer_69 => df_Transformer_69
df_Transformer_69 = (
    df_bluebusiness_orig_email
    .withColumn("SRC_SYS_ASG_ID", trim(F.col("SRC_SYS_ASG_ID")))
    .withColumn("EMAIL_ADDR_STRING", trim(F.col("ORIG_EMAIL_ADDR_STRING")))
)

# hf_agent_addr_extr_emails (Scenario A) for orig_email => key = SRC_SYS_ASG_ID
df_org_email = df_Transformer_69.dropDuplicates(["SRC_SYS_ASG_ID"])

# BusinessRules (CTransformerStage) => join primary link df_StripField with lookups
#   lnkStateCdOut (df_hf_agnt_addr_state_cd)
#   email_current (df_cur_email)
#   email_orig (df_org_email)
#   TernDtSk (df_hf_agnt_id_lkup)
df_BusinessRules_intermediate = (
    df_StripField.alias("Strip")
    .join(
        df_hf_agnt_addr_state_cd.alias("lnkStateCdOut"),
        F.col("Strip.COAD_STATE") == F.col("lnkStateCdOut.SRC_CD"),
        "left"
    )
    .join(
        df_cur_email.alias("email_current"),
        F.col("Strip.COCE_ID") == F.col("email_current.SRC_SYS_ASG_ID"),
        "left"
    )
    .join(
        df_org_email.alias("email_orig"),
        F.col("Strip.COCE_ID") == F.col("email_orig.SRC_SYS_ASG_ID"),
        "left"
    )
    .join(
        df_hf_agnt_id_lkup.alias("TernDtSk"),
        F.col("Strip.COCE_ID") == F.col("TernDtSk.AGNT_ID"),
        "left"
    )
)

# Create columns for stage variables
df_BusinessRules_stagevars = (
    df_BusinessRules_intermediate
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn(
        "svValidStateCd",
        F.when(
            (F.col("lnkStateCdOut.TRGT_CD").isNull())
            | (F.col("lnkStateCdOut.TRGT_CD") == F.lit("NA"))
            | (F.col("lnkStateCdOut.TRGT_CD") == F.lit("UNK")),
            F.lit(False)
        ).otherwise(F.lit(True))
    )
    .withColumn(
        "svCtryCd",
        F.when(
            F.col("svValidStateCd") == F.lit(True),
            F.lit("USA")
        ).otherwise(
            F.when(
                (F.col("Strip.COAD_CTRY_CD").isNull())
                | (F.length(F.col("Strip.COAD_CTRY_CD")) == 0),
                F.lit("NA")
            ).otherwise(
                F.when(
                    (F.col("Strip.COAD_CTRY_CD") != F.lit("USA"))
                    | (F.col("Strip.COAD_CTRY_CD") != F.lit("US")),
                    F.lit("NA")
                ).otherwise(F.col("Strip.COAD_CTRY_CD"))
            )
        )
    )
)

# Output link => "Sort"
df_BusinessRules = (
    df_BusinessRules_stagevars
    .withColumn("AGNT_ID_1", F.upper(trim(F.col("Strip.COCE_ID"))))
    .withColumn("ADDR_TYP_CD", F.col("Strip.COAD_TYPE"))
    .withColumn("TERM_DT_SK", F.col("TernDtSk.TERM_DT_SK"))
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk).cast(IntegerType()))
    .withColumn("AGNT_ID", F.col("Strip.COCE_ID"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("AGNT_ADDR_SK", F.lit(0))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.lit("FACETS;"), trim(F.col("Strip.COCE_ID")), F.lit(";"), trim(F.col("Strip.COAD_TYPE")))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("AGNT", F.lit(0))
    .withColumn("ADDR_LN_1", F.col("Strip.COAD_ADDR1"))
    .withColumn("ADDR_LN_2", F.col("Strip.COAD_ADDR2"))
    .withColumn("ADDR_LN_3", F.col("Strip.COAD_ADDR3"))
    .withColumn("CITY_NM", F.col("Strip.COAD_CITY"))
    .withColumn("AGNT_ADDR_ST_CD", F.col("Strip.COAD_STATE"))
    .withColumn("POSTAL_CD", F.col("Strip.COAD_ZIP"))
    .withColumn("CNTY_NM", F.col("Strip.COAD_COUNTY"))
    .withColumn("AGNT_ADDR_CTRY_CD", F.col("svCtryCd"))
    .withColumn("PHN_NO", F.col("Strip.COAD_PHONE"))
    .withColumn("PHN_NO_EXT", F.col("Strip.COAD_PHONE_EXT"))
    .withColumn("FAX_NO", F.col("Strip.COAD_FAX"))
    .withColumn("FAX_NO_EXT", F.col("Strip.COAD_FAX_EXT"))
    .withColumn(
        "EMAIL_ADDR_TX",
        F.when(
            F.col("email_current.SRC_SYS_ASG_ID").isNull(),
            F.when(
                F.col("email_orig.SRC_SYS_ASG_ID").isNull(),
                F.lit(None)
            ).otherwise(F.col("email_orig.EMAIL_ADDR_STRING"))
        ).otherwise(F.col("email_current.EMAIL_ADDR_STRING"))
    )
)

# Sort => df_Sort
df_Sort = df_BusinessRules.orderBy(
    F.col("AGNT_ID_1").asc(),
    F.col("ADDR_TYP_CD").asc(),
    F.col("TERM_DT_SK").asc(),
    F.col("AGNT_ID").asc()
)

# hf_dedupe_agnt_id (Scenario A) => keys = ["AGNT_ID_1","ADDR_TYP_CD"]
df_hf_dedupe_agnt_id = df_Sort.dropDuplicates(["AGNT_ID_1","ADDR_TYP_CD"])

# Transformer => define stage variable svAddrTypCdSk
# Create a column "svAddrTypCdSk" = GetFkeyCodes('FACETS',100,'ADDRESS TYPE', trim(Convert('\n\r\t','',ADDR_TYP_CD)),'X')
# Then produce 3 output links: Transform, AllCol, Snapshot
df_Transformer_with_sv = df_hf_dedupe_agnt_id.withColumn(
    "svAddrTypCdSk",
    GetFkeyCodes(
        F.lit("FACETS"),
        F.lit(100),
        F.lit("ADDRESS TYPE"),
        trim(
            Convert(
                F.lit("\n\r\t"),
                F.lit(""),
                F.col("ADDR_TYP_CD")
            )
        ),
        F.lit("X")
    )
)

# Output link => Transform
df_Transform = df_Transformer_with_sv.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("ADDR_TYP_CD").alias("ADDR_TYP_CD")
)

# Output link => AllCol
df_AllCol = df_Transformer_with_sv.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("ADDR_TYP_CD").alias("ADDR_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AGNT_ADDR_SK").alias("AGNT_ADDR_SK"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT").alias("AGNT"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("AGNT_ADDR_ST_CD").alias("AGNT_ADDR_ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("AGNT_ADDR_CTRY_CD").alias("AGNT_ADDR_CTRY_CD"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
)

# Output link => Snapshot => write to B_AGNT_ADDR.dat
df_Snapshot = df_Transformer_with_sv.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("svAddrTypCdSk").alias("AGNT_ADDR_TYP_CD_SK")
)

write_files(
    df_Snapshot.select(df_Snapshot.columns),
    f"{adls_path}/load/B_AGNT_ADDR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AgntAddrPK (Shared Container) => 2 inputs => df_Transform, df_AllCol => 1 output => df_Key
paramsContainer = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_Key = AgntAddrPK(df_Transform, df_AllCol, paramsContainer)

# IdsAgntAddrExtr (CSeqFileStage) => final write
# The columns in the JSON for IdsAgntAddrExtr, in order, some are char => apply rpad
df_final = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("AGNT_ADDR_SK").alias("AGNT_ADDR_SK"),
    F.col("AGNT_ID").alias("AGNT_ID"),
    F.col("ADDR_TYP_CD").alias("ADDR_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AGNT").alias("AGNT"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("AGNT_ADDR_ST_CD").alias("AGNT_ADDR_ST_CD"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("AGNT_ADDR_CTRY_CD").alias("AGNT_ADDR_CTRY_CD"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.rpad(F.col("PHN_NO_EXT"), 5, " ").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.rpad(F.col("FAX_NO_EXT"), 5, " ").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
)

write_files(
    df_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)