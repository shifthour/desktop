# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          IdsEdwRIMbrEnrDMbrPcpDBalSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/03/2007          3264                              Originally Programmed                           devlEDW10              Steph Goddard            10/08/2007        
# MAGIC 
# MAGIC Srikanth Mettpalli               07/13/2013         5114                              Originally Programmed                         EnterpriseWrhsDevl       
# MAGIC                                                                                                             (Server to Parallel Conversion)

# MAGIC Pull all the Matching Records from MBR_ENR_D and MBR_PCP_D Table.
# MAGIC Row Comparison on Pkey
# MAGIC Job Name: IdsEdwRIMbrEnrDMbrPcpDBal
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Write all the Matching Records.
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC IDS - EDW Member Enrollment Dim and Member PCP Dim Referential Integrity check Comparisons
# MAGIC Copy stage used to remove duplicates and create one record for the Notification file.
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Row Comparison on Nkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, isnull, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


ExtrRunCycle = get_widget_value('ExtrRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# ------------------------------------------------------
# Stage: db2_MbrEnr_MbrPcp_Matching_in -> seq_Matching_csv_out
# ------------------------------------------------------
extract_query = f"""SELECT
MBR_ENR_D.SRC_SYS_CD MBR_ENR_D_SRC_SYS_CD,
MBR_ENR_D.MBR_UNIQ_KEY MBR_ENR_D_MBR_UNIQ_KEY,
MBR_PCP_D.SRC_SYS_CD MBR_PCP_D_SRC_SYS_CD,
MBR_PCP_D.MBR_UNIQ_KEY MBR_PCP_D_MBR_UNIQ_KEY
FROM {EDWOwner}.MBR_ENR_D MBR_ENR_D
INNER JOIN {EDWOwner}.MBR_PCP_D MBR_PCP_D
ON MBR_ENR_D.SRC_SYS_CD = MBR_PCP_D.SRC_SYS_CD
AND MBR_ENR_D.MBR_UNIQ_KEY = MBR_PCP_D.MBR_UNIQ_KEY,
{EDWOwner}.PROD_D PROD_D
WHERE
MBR_ENR_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'
AND MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND MBR_PCP_D.MBR_PCP_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.PROD_SK = PROD_D.PROD_SK
AND PROD_D.PROD_LOB_CD = 'HMO'"""
df_db2_MbrEnr_MbrPcp_Matching_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_seq_Matching_csv_out = df_db2_MbrEnr_MbrPcp_Matching_in.select(
    "MBR_ENR_D_SRC_SYS_CD",
    "MBR_ENR_D_MBR_UNIQ_KEY",
    "MBR_PCP_D_SRC_SYS_CD",
    "MBR_PCP_D_MBR_UNIQ_KEY"
)
write_files(
    df_seq_Matching_csv_out,
    f"{adls_path}/balancing/sync/MbrEnrDMbrPcpDBalancingTotalMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# ------------------------------------------------------
# Stage: db2_PKey_Missing_in -> xfrm_BusinessLogic -> seq_ResearchFile1_csv_out, seq_ResearchFile2_csv_out, cpy_DDup -> seq_Notification_File_csv_out
# ------------------------------------------------------
extract_query = f"""SELECT
MBR_ENR_D.MBR_ENR_SK,
MBR_PCP_D.MBR_PCP_SK
FROM {EDWOwner}.MBR_ENR_D MBR_ENR_D
FULL OUTER JOIN {EDWOwner}.MBR_D MBR_D
ON MBR_ENR_D.MBR_SK = MBR_D.MBR_SK,
{EDWOwner}.MBR_PCP_D MBR_PCP_D,
{EDWOwner}.PROD_D PROD_D
WHERE
MBR_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND MBR_D.MBR_SK = MBR_PCP_D.MBR_SK
AND MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'
AND MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND MBR_PCP_D.MBR_PCP_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.PROD_SK = PROD_D.PROD_SK
AND PROD_D.PROD_LOB_CD = 'HMO'"""
df_db2_PKey_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer xfrm_BusinessLogic outputs:

# lnk_Research1 -> seq_ResearchFile1_csv_out (ISNULL(MBR_ENR_SK))
df_ResearchFile1 = df_db2_PKey_Missing_in.filter(isnull(col("MBR_ENR_SK"))).select(
    "MBR_ENR_SK",
    "MBR_PCP_SK"
)
write_files(
    df_ResearchFile1,
    f"{adls_path}/balancing/research/IdsEdwPkeyParChldMbrEnrDMbrPcpDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# lnk_Research2 -> seq_ResearchFile2_csv_out (ISNULL(MBR_PCP_SK))
df_ResearchFile2 = df_db2_PKey_Missing_in.filter(isnull(col("MBR_PCP_SK"))).select(
    "MBR_ENR_SK",
    "MBR_PCP_SK"
)
write_files(
    df_ResearchFile2,
    f"{adls_path}/balancing/research/IdsEdwPkeyChldParMbrEnrDMbrPcpDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# lnk_NotifyDDup_in -> cpy_DDup -> seq_Notification_File_csv_out (@INROWNUM=1)
w1 = Window.orderBy(lit(1))
df_notify1 = (
    df_db2_PKey_Missing_in
    .withColumn("rn", row_number().over(w1))
    .filter("rn=1")
    .select(
        lit("REFERENTIAL INTEGRITY BALANCING PRIMARY KEY IDS - EDW MBR ENR D AND MBR PCP D CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)
df_notify1 = dedup_sort(
    df_notify1,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)
df_notify1 = df_notify1.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_notify1,
    f"{adls_path}/balancing/notify/MembershipBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# ------------------------------------------------------
# Stage: db2_NKey_Missing_in -> xfrm_BusinessLogic1 -> seq_ResearchFile3_csv_out, seq_ResearchFile4_csv_out, cpy_DDup1 -> seq_Notification1_File_csv_out
# ------------------------------------------------------
extract_query = f"""SELECT
MBR_ENR_D.SRC_SYS_CD MBR_ENR_D_SRC_SYS_CD,
MBR_ENR_D.MBR_UNIQ_KEY MBR_ENR_D_MBR_UNIQ_KEY,
MBR_PCP_D.SRC_SYS_CD MBR_PCP_D_SRC_SYS_CD,
MBR_PCP_D.MBR_UNIQ_KEY MBR_PCP_D_MBR_UNIQ_KEY
FROM {EDWOwner}.MBR_ENR_D MBR_ENR_D
FULL OUTER JOIN {EDWOwner}.MBR_PCP_D MBR_PCP_D
ON MBR_ENR_D.SRC_SYS_CD = MBR_PCP_D.SRC_SYS_CD
AND MBR_ENR_D.MBR_UNIQ_KEY = MBR_PCP_D.MBR_UNIQ_KEY,
{EDWOwner}.PROD_D PROD_D
WHERE
MBR_ENR_D.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'
AND MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND MBR_PCP_D.MBR_PCP_TERM_DT_SK >= '{CurrDate}'
AND MBR_ENR_D.PROD_SK = PROD_D.PROD_SK
AND PROD_D.PROD_LOB_CD = 'HMO'"""
df_db2_NKey_Missing_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer xfrm_BusinessLogic1 outputs:

# lnk_Research1 -> seq_ResearchFile3_csv_out (ISNULL(MBR_ENR_D_MBR_UNIQ_KEY) OR ISNULL(MBR_ENR_D_SRC_SYS_CD))
df_ResearchFile3 = df_db2_NKey_Missing_in.filter(
    isnull(col("MBR_ENR_D_MBR_UNIQ_KEY")) | isnull(col("MBR_ENR_D_SRC_SYS_CD"))
).select(
    "MBR_ENR_D_SRC_SYS_CD",
    "MBR_ENR_D_MBR_UNIQ_KEY",
    "MBR_PCP_D_SRC_SYS_CD",
    "MBR_PCP_D_MBR_UNIQ_KEY"
)
write_files(
    df_ResearchFile3,
    f"{adls_path}/balancing/research/IdsEdwNatkeyParChldMbrEnrDMbrPcpDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# lnk_Research2 -> seq_ResearchFile4_csv_out (ISNULL(MBR_PCP_D_MBR_UNIQ_KEY) OR ISNULL(MBR_PCP_D_SRC_SYS_CD))
df_ResearchFile4 = df_db2_NKey_Missing_in.filter(
    isnull(col("MBR_PCP_D_MBR_UNIQ_KEY")) | isnull(col("MBR_PCP_D_SRC_SYS_CD"))
).select(
    "MBR_ENR_D_SRC_SYS_CD",
    "MBR_ENR_D_MBR_UNIQ_KEY",
    "MBR_PCP_D_SRC_SYS_CD",
    "MBR_PCP_D_MBR_UNIQ_KEY"
)
write_files(
    df_ResearchFile4,
    f"{adls_path}/balancing/research/IdsEdwNatkeyChldParMbrEnrDMbrPcpDRIResearch.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# lnk_NotifyDDup_in -> cpy_DDup1 -> seq_Notification1_File_csv_out (@INROWNUM=1)
w2 = Window.orderBy(lit(1))
df_notify2 = (
    df_db2_NKey_Missing_in
    .withColumn("rn", row_number().over(w2))
    .filter("rn=1")
    .select(
        lit("REFERENTIAL INTEGRITY BALANCING NATURAL KEYS IDS - EDW MBR ENR D AND MBR PCP D CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
    )
)
df_notify2 = dedup_sort(
    df_notify2,
    ["NOTIFICATION"],
    [("NOTIFICATION", "A")]
)
df_notify2 = df_notify2.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_notify2,
    f"{adls_path}/balancing/notify/MembershipBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)