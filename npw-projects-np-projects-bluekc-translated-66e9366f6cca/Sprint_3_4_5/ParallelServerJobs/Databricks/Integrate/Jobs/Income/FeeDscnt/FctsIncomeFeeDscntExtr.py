# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 04/09/07 08:24:30 Batch  14344_30274 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 12/18/06 12:24:28 Batch  14232_44709 INIT bckcetl ids20 dsadm Income Backup for 12/18/2006 install
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:14:49 Batch  14001_45485 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:11:02 Batch  14001_40263 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/24/06 14:49:28 Batch  13904_53375 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:47:17 Batch  13904_53242 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/24/06 14:36:07 Batch  13904_52574 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_1 01/24/06 14:21:30 Batch  13904_51695 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 12/23/05 11:39:15 Batch  13872_41979 PROMOTE bckcett testIDS30 u10913 Move Income Commission to test
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeFeeDscntExtr
# MAGIC CALLED BY:   FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Fee Discount information from Facets for Income 
# MAGIC       
# MAGIC    Dates are not used as a criteria for which records are pulled.   The whole table is extracted.
# MAGIC 
# MAGIC 
# MAGIC INPUTS:
# MAGIC    CMC_PMFA_FD_ACCTS
# MAGIC   
# MAGIC HASH FILES:  hf_fee_dscnt
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   used the STRIP.FIELD to all of the character fields.
# MAGIC                   The DB2 timestamp value of the audit_ts is converted to a sybase timestamp with the transofrm  FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name. 
# MAGIC                   
# MAGIC                  This is a pulls all records.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Ralph Tucker  -  10/30/2005-   Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/13/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/26/2008        3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                    Steph Goddard            10/03/2008                 
# MAGIC                                                                                                        and SrcSysCd  
# MAGIC Prabhu ES                       2022-03-02         S2S Remediation    MSSQL ODBC conn added and other param changes  IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC Hash file hf_fee_dscnt_allcol cleared
# MAGIC Pulls all Fee Discounts rows from facets CMC_PMFA_FD_ACCTS table.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all required parameter values
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

FacetsOwner = get_widget_value('$FacetsOwner','')
IDSOwner = get_widget_value('$IDSOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# --------------------------------------------------------------------------------
# Stage: CMC_PMFA_FD_ACCTS (ODBCConnector)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT * FROM {FacetsOwner}.CMC_PMFA_FD_ACCTS"
df_CMC_PMFA_FD_ACCTS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
df_strip = df_CMC_PMFA_FD_ACCTS.select(
    strip_field(F.col("PMFA_ID")).alias("FEE_DSCNT_ID"),
    strip_field(F.col("PMFA_ACCT_CAT")).alias("FNCL_LOB"),
    strip_field(F.col("PMFA_FEE_DISC_IND")).alias("FEE_DSCNT_CD"),
    strip_field(F.col("LOBD_ID")).alias("FEE_DSCNT_LOB_CD")
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage) - produces two output links

# Output Link "AllCol"
df_enrichedAllCol = (
    df_strip
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("FEE_DSCNT_ID", trim(F.col("FEE_DSCNT_ID")))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS;"), F.trim(F.col("FEE_DSCNT_ID"))))
    .withColumn("FEE_DSCNT_SK", F.lit(0))
    .withColumn("FEE_DSCNT_CD", trim(F.col("FEE_DSCNT_CD")))
    .withColumn(
        "FNCL_LOB",
        F.when(
            F.length(F.trim(F.col("FNCL_LOB")))==0,
            F.lit("NA")
        ).otherwise(F.trim(F.col("FNCL_LOB")))
    )
    .withColumn("FEE_DSCNT_LOB_CD", trim(F.col("FEE_DSCNT_LOB_CD")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
)

# Output Link "Transform"
df_enrichedTransform = (
    df_strip
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("FEE_DSCNT_ID", trim(F.col("FEE_DSCNT_ID")))
)

# --------------------------------------------------------------------------------
# Stage: IncmFeeDscntPK (CContainerStage)
# MAGIC %run ../../../../shared_containers/PrimaryKey/IncmFeeDscntPK
# COMMAND ----------

incmFeeDscntPK_params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "$IDSOwner": IDSOwner
}
df_incmFeeDscntPK = IncmFeeDscntPK(df_enrichedAllCol, df_enrichedTransform, incmFeeDscntPK_params)

# --------------------------------------------------------------------------------
# Stage: IdsFeeDscnt (CSeqFileStage) - Writing to #TmpOutFile#
df_final_IdsFeeDscnt = df_incmFeeDscntPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FEE_DSCNT_SK",
    "FEE_DSCNT_ID",
    "FNCL_LOB_CD",
    "FEE_DSCNT_CD",
    "FEE_DSCNT_LOB_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
).withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "FEE_DSCNT_ID", F.rpad(F.col("FEE_DSCNT_ID"), 2, " ")
).withColumn(
    "FEE_DSCNT_CD", F.rpad(F.col("FEE_DSCNT_CD"), 1, " ")
).withColumn(
    "FEE_DSCNT_LOB_CD", F.rpad(F.col("FEE_DSCNT_LOB_CD"), 4, " ")
).withColumn(
    "FNCL_LOB_CD", F.rpad(F.col("FNCL_LOB_CD"), 4, " ")
)

write_files(
    df_final_IdsFeeDscnt,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Facets_Source (ODBCConnector)
jdbc_url_source, jdbc_props_source = get_db_config(facets_secret_name)
snapshot_query = f"SELECT * FROM {FacetsOwner}.CMC_PMFA_FD_ACCTS"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_source)
    .options(**jdbc_props_source)
    .option("query", snapshot_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage) with Stage Variable svFnclLobSk
df_transform_temp = df_Facets_Source.withColumn(
    "svFnclLobSk",
    GetFkeyFnclLob(
        F.lit("PSI"),
        F.lit(100),
        trim(strip_field(F.col("PMFA_ACCT_CAT"))),
        F.lit("X")
    )
)

df_transform = df_transform_temp.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(strip_field(F.col("PMFA_ID"))).alias("FEE_DSCNT_ID"),
    F.when(
        df_transform_temp["svFnclLobSk"].isNull() |
        (F.length(F.trim(df_transform_temp["svFnclLobSk"])) == 0),
        F.lit(1)
    ).otherwise(df_transform_temp["svFnclLobSk"]).alias("FNCL_LOB_SK")
)

# --------------------------------------------------------------------------------
# Stage: Snapshot_File (CSeqFileStage)
df_final_Snapshot_File = df_transform.select(
    "SRC_SYS_CD_SK",
    "FEE_DSCNT_ID",
    "FNCL_LOB_SK"
).withColumn(
    "FEE_DSCNT_ID", F.rpad(F.col("FEE_DSCNT_ID"), 2, " ")
)

write_files(
    df_final_Snapshot_File,
    f"{adls_path}/load/B_FEE_DSCNT.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)