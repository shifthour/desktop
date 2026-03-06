# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  PSJrnlEntryPeoplesoftExt
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Brent Leland      04/05/2005    Changed SQL lookups to hash files for FNCL_LOB and CD_MPPNG.
# MAGIC 
# MAGIC Developer  		Date		Project/Altiris #	Change Description					Development Project		Code Reviewer	Date Reviewed
# MAGIC =======================================================================================================================================================================================
# MAGIC Brent Leland                   	04/05/2005                                                	Changed SQL lookups to hash files for FNCL_LOB 
# MAGIC 							and CD_MPPNG.
# MAGIC Sudheer Champati         	09/11/2017        	5599               	Added the Sybase (GL_Cap_BCP_table) to pull  		           IntegrateDev2               	Jag Yelavarthi 	2017-09-12
# MAGIC                                                                                                    			the workday columns.
# MAGIC 
# MAGIC Tim Sieg                         	10/6/2017         	5599           		Corrected the IDS parameter names,                    	         IntegrateDev2       		Kalyan Neelam  	2017-10-10
# MAGIC                                                                                                     			missing $ in name
# MAGIC 
# MAGIC T.Sieg                  		2022-05-15      	S2S Remediation  	Replaced Sybase extract stage with ODBC connector        	          IntegrateDev5		Ken Bradmon	2022-06-07
# MAGIC 
# MAGIC 
# MAGIC Amritha A J                            2023-07-18                 US 588188           Updated FNCL_LOB_DESC column length to VARCHAR(255).           IntegrateDevB                              Goutham Kalidindi  2023-08-08


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Acctg_Dt = get_widget_value('Acctg_Dt','')
Trans_LOB = get_widget_value('Trans_LOB','')
FilePath = get_widget_value('FilePath','')
TmpOutFile = get_widget_value('TmpOutFile','')
TableInterface = get_widget_value('TableInterface','')
ColumnName = get_widget_value('ColumnName','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_JRNL_ENTRY_TRANS = (
    f"SELECT ACCTG_DT_SK as ACCTG_DT_SK,"
    f"JRNL_ENTRY_TRANS_AMT as JRNL_ENTRY_TRANS_AMT,"
    f"TRANS_LN_NO as TRANS_LN_NO,"
    f"ACCT_NO as ACCT_NO,"
    f"AFFILIATE_NO as AFFILIATE_NO,"
    f"BUS_UNIT_GL_NO as BUS_UNIT_GL_NO,"
    f"BUS_UNIT_NO as BUS_UNIT_NO,"
    f"CC_ID as CC_ID,"
    f"JRNL_LN_DESC as JRNL_LN_DESC,"
    f"OPR_UNIT_NO as OPR_UNIT_NO,"
    f"SUB_ACCT_NO as SUB_ACCT_NO,"
    f"FNCL_LOB_SK as FNCL_LOB_SK,"
    f"JRNL_ENTRY_TRANS_DR_CR_CD_SK as JRNL_ENTRY_TRANS_DR_CR_CD_SK,"
    f"DIST_GL_IN as DIST_GL_IN,"
    f"APP_JRNL_ID as APP_JRNL_ID,"
    f"JRNL_LN_REF_NO as JRNL_LN_REF_NO,"
    f"SRC_TRANS_CK "
    f"FROM {IDSOwner}.JRNL_ENTRY_TRANS "
    f"WHERE DIST_GL_IN = 'Y' "
    f"AND ACCTG_DT_SK = '{Acctg_Dt}' "
    f"AND TRANS_LN_NO = {Trans_LOB}"
)
df_JRNL_ENTRY_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_JRNL_ENTRY_TRANS)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

extract_query_FNCL_LOB = (
    f"SELECT FNCL_LOB_SK,"
    f"SRC_SYS_CD_SK as SRC_SYS_CD_SK,"
    f"FNCL_LOB_CD as FNCL_LOB_CD,"
    f"CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,"
    f"LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,"
    f"FNCL_LOB_STTUS_CD_SK as FNCL_LOB_STTUS_CD_SK,"
    f"EFF_DT_SK as EFF_DT_SK,"
    f"FNCL_LOB_DESC as FNCL_LOB_DESC "
    f"FROM {IDSOwner}.FNCL_LOB"
)
df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_FNCL_LOB)
    .load()
)
df_hf_jrnl_entry_ps_fncl_lob = dedup_sort(
    df_FNCL_LOB,
    ["FNCL_LOB_SK"],
    [("FNCL_LOB_SK","A")]
)

jdbc_url_b, jdbc_props_b = get_db_config(bcbs_secret_name)
extract_query_BCP = (
    f"SELECT \n"
    f"GL_BCP.GL_{ColumnName}_DTL_CK as GL_DTL_CK, \n"
    f"GL_BCP.ACCOUNT, \n"
    f"GL_BCP.OPERATING_UNIT, \n"
    f"GL_BCP.AFFILIATE, \n"
    f"GL_BCP.DEPTID, \n"
    f"GL_BCP.CHARTFIELD1,\n"
    f"GL_BCP.JRNL_LN_REF\n"
    f"FROM {FilePath.replace('#','')}..GL_{TableInterface}_DTL_BCP GL_BCP"
)
# Replace {FilePath.replace('#','')}.. with {bcbs_secret_name} database reference if needed.
# But we preserve the logic as is, because the job uses #$BCBSFINDB#.. 
# We interpret #$BCBSFINDB# as FilePath for the database name replacement or if environment needs it.
# The job literal is "#$BCBSFINDB#..GL_#TableInterface#_DTL_BCP", so let's do direct substitution for demonstration:
extract_query_BCP = (
    f"SELECT \n"
    f"GL_BCP.GL_{ColumnName}_DTL_CK as GL_DTL_CK, \n"
    f"GL_BCP.ACCOUNT, \n"
    f"GL_BCP.OPERATING_UNIT, \n"
    f"GL_BCP.AFFILIATE, \n"
    f"GL_BCP.DEPTID, \n"
    f"GL_BCP.CHARTFIELD1,\n"
    f"GL_BCP.JRNL_LN_REF\n"
    f"FROM {FilePath.replace('#','')}..GL_{TableInterface}_DTL_BCP GL_BCP"
)
df_GL_SUBJ_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_b)
    .options(**jdbc_props_b)
    .option("query", extract_query_BCP)
    .load()
)
df_hf_gl_interface_ps_dtl_bcp = dedup_sort(
    df_GL_SUBJ_BCP_table,
    ["GL_DTL_CK"],
    [("GL_DTL_CK","A")]
)

df_lnkJrnlDetail = (
    df_JRNL_ENTRY_TRANS.alias("lnkJrnlEntryIn")
    .join(
        df_hf_jrnl_entry_ps_fncl_lob.alias("lnkFNCL_LOBIn"),
        F.col("lnkJrnlEntryIn.FNCL_LOB_SK") == F.col("lnkFNCL_LOBIn.FNCL_LOB_SK"),
        "left"
    )
    .join(
        df_hf_etrnl_cd_mppng.alias("lnkCD_MAPPINGIn"),
        F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_DR_CR_CD_SK") == F.col("lnkCD_MAPPINGIn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_hf_gl_interface_ps_dtl_bcp.alias("bcpData"),
        F.col("lnkJrnlEntryIn.SRC_TRANS_CK") == F.col("bcpData.GL_DTL_CK"),
        "left"
    )
    .select(
        F.col("lnkJrnlEntryIn.BUS_UNIT_NO").alias("BUSINESS_UNIT"),
        F.col("lnkJrnlEntryIn.TRANS_LN_NO").alias("TRANSACTION_LINE"),
        Oconv(
            Iconv(F.col("lnkJrnlEntryIn.ACCTG_DT_SK"), "D-YMD[4,2,2]"),
            "D/MDY[2,2,4]"
        ).alias("ACCOUNTING_DT"),
        F.col("lnkJrnlEntryIn.BUS_UNIT_GL_NO").alias("BUSINESS_UNIT_GL"),
        F.col("bcpData.ACCOUNT").alias("ACCOUNT"),
        F.col("bcpData.DEPTID").alias("DEPTID"),
        F.col("bcpData.OPERATING_UNIT").alias("OPERATION_UNIT"),
        F.col("lnkFNCL_LOBIn.FNCL_LOB_CD").alias("PRODUCT"),
        F.col("bcpData.AFFILIATE").alias("AFFILIATE"),
        F.col("bcpData.CHARTFIELD1").alias("CHARTFIELD1"),
        F.when(
            F.col("lnkCD_MAPPINGIn.TRGT_CD") == F.lit("CR"),
            F.lit(0) - F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT")
        ).otherwise(F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT")).alias("MONETARY_AMOUNT"),
        F.col("lnkJrnlEntryIn.JRNL_LN_DESC").alias("LINE_DESC"),
        F.col("lnkJrnlEntryIn.APP_JRNL_ID").alias("APP_JRNL_ID"),
        F.col("bcpData.JRNL_LN_REF").alias("JRNL_LN_REF_NO")
    )
)

df_AggJrnlSum = (
    df_lnkJrnlDetail
    .groupBy(
        "BUSINESS_UNIT",
        "TRANSACTION_LINE",
        "ACCOUNTING_DT",
        "BUSINESS_UNIT_GL",
        "ACCOUNT",
        "DEPTID",
        "OPERATION_UNIT",
        "PRODUCT",
        "AFFILIATE",
        "CHARTFIELD1",
        "APP_JRNL_ID",
        "JRNL_LN_REF_NO"
    )
    .agg(
        F.sum("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        F.first("LINE_DESC").alias("LINE_DESC")
    )
)

df_lnkJrnlSumFieldsIntoCount = (
    df_AggJrnlSum
    .select(
        F.col("TRANSACTION_LINE"),
        F.when(F.col("MONETARY_AMOUNT") > 0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("CREDIT_AMOUNT"),
        F.when(F.col("MONETARY_AMOUNT") < 0, F.col("MONETARY_AMOUNT")).otherwise(F.lit(0.0)).alias("DEBIT_AMT")
    )
)

df_lnkJrnlSumFields = (
    df_AggJrnlSum
    .select(
        F.col("BUSINESS_UNIT"),
        F.col("TRANSACTION_LINE"),
        F.col("ACCOUNTING_DT"),
        F.col("BUSINESS_UNIT_GL"),
        trim(F.col("ACCOUNT")).alias("ACCOUNT"),
        trim(F.col("DEPTID")).alias("DEPTID"),
        trim(F.col("OPERATION_UNIT")).alias("OPERATION_UNIT"),
        trim(F.col("PRODUCT")).alias("PRODUCT"),
        trim(F.col("AFFILIATE")).alias("AFFILIATE"),
        trim(F.col("CHARTFIELD1")).alias("CHARTFIELD1"),
        F.col("MONETARY_AMOUNT"),
        F.col("LINE_DESC"),
        F.col("APP_JRNL_ID"),
        F.col("JRNL_LN_REF_NO")
    )
)

df_AggHeaderCount = (
    df_lnkJrnlSumFieldsIntoCount
    .groupBy("TRANSACTION_LINE")
    .agg(
        F.sum("CREDIT_AMOUNT").alias("CREDIT_SUM"),
        F.sum("DEBIT_AMT").alias("DEBIT_SUM"),
        F.count("TRANSACTION_LINE").alias("ROW_COUNT")
    )
)

df_trnsHeaderCounts = (
    df_AggHeaderCount
    .withColumn(
        "LOBType",
        F.when(F.lit(Trans_LOB) == F.lit("231"), F.lit("CAP"))
        .when(F.lit(Trans_LOB) == F.lit("240"), F.lit("DRG"))
        .when(F.lit(Trans_LOB) == F.lit("230"), F.lit("CLM"))
        .when(F.lit(Trans_LOB) == F.lit("210"), F.lit("INC"))
        .when(F.lit(Trans_LOB) == F.lit("270"), F.lit("COM"))
        .otherwise(F.lit("UNK"))
    )
    .select(
        F.concat(
            F.col("LOBType"), F.lit("\n"), F.lit("QQQ HEADER"), F.lit("\n")
        ).alias("HEADER"),
        F.col("CREDIT_SUM").alias("CREDIT"),
        F.col("DEBIT_SUM").alias("DEBIT"),
        F.col("ROW_COUNT").alias("ROWS"),
        F.concat(
            F.lit("\n"), F.col("LOBType"), F.lit("\n"), F.lit("QQQ DETAIL")
        ).alias("TRAILER")
    )
)

df_trnsJrnlSplit = (
    df_lnkJrnlSumFields
    .select(
        F.col("BUSINESS_UNIT"),
        F.col("TRANSACTION_LINE"),
        F.col("ACCOUNTING_DT"),
        F.col("BUSINESS_UNIT_GL"),
        F.when((F.col("ACCOUNT") == F.lit("0")) | (F.col("ACCOUNT") == F.lit("NA")), F.lit(" ")).otherwise(F.col("ACCOUNT")).alias("ACCOUNT"),
        F.when((F.col("DEPTID") == F.lit("0")) | (F.col("DEPTID") == F.lit("NA")), F.lit(" ")).otherwise(F.col("DEPTID")).alias("DEPTID"),
        F.when((F.col("OPERATION_UNIT") == F.lit("0")) | (F.col("OPERATION_UNIT") == F.lit("NA")), F.lit(" ")).otherwise(F.col("OPERATION_UNIT")).alias("OPERATION_UNIT"),
        F.when((F.col("PRODUCT") == F.lit("0")) | (F.col("PRODUCT") == F.lit("NA")), F.lit(" ")).otherwise(F.col("PRODUCT")).alias("PRODUCT"),
        F.when((F.col("AFFILIATE") == F.lit("0")) | (F.col("AFFILIATE") == F.lit("NA")), F.lit(" ")).otherwise(F.col("AFFILIATE")).alias("AFFILIATE"),
        F.when((F.col("CHARTFIELD1") == F.lit("0")) | (F.col("CHARTFIELD1") == F.lit("NA")), F.lit(" ")).otherwise(F.col("CHARTFIELD1")).alias("CHARTFIELD1"),
        F.col("MONETARY_AMOUNT"),
        F.col("LINE_DESC"),
        F.col("APP_JRNL_ID"),
        F.col("JRNL_LN_REF_NO")
    )
)

df_Dtl_PeopleSoftExtr_final = (
    df_trnsJrnlSplit
    .select(
        rpad(F.col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
        F.col("TRANSACTION_LINE").alias("TRANSACTION_LINE"),
        rpad(F.col("ACCOUNTING_DT"), 10, " ").alias("ACCOUNTING_DT"),
        rpad(F.col("BUSINESS_UNIT_GL"), 5, " ").alias("BUSINESS_UNIT_GL"),
        rpad(F.col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
        rpad(F.col("DEPTID"), 10, " ").alias("DEPTID"),
        rpad(F.col("OPERATION_UNIT"), 8, " ").alias("OPERATION_UNIT"),
        rpad(F.col("PRODUCT"), 6, " ").alias("PRODUCT"),
        rpad(F.col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
        rpad(F.col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
        F.col("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
        rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
        rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO")
    )
)

df_Hdr_PeopleSoftExtr_final = (
    df_trnsHeaderCounts
    .select(
        rpad(F.col("HEADER"), 15, " ").alias("HEADER"),
        F.col("CREDIT").alias("CREDIT"),
        F.col("DEBIT").alias("DEBIT"),
        F.col("ROWS").alias("ROWS"),
        rpad(F.col("TRAILER"), 15, " ").alias("TRAILER")
    )
)

write_files(
    df_Dtl_PeopleSoftExtr_final,
    f"{adls_path_publish}/external/DTL_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Hdr_PeopleSoftExtr_final,
    f"{adls_path_publish}/external/HDR_{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)