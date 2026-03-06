# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 06/12/08 10:08:56 Batch  14774_36805 PROMOTE bckcetl ids20 dsadm bls for lk
# MAGIC ^1_1 06/12/08 09:53:05 Batch  14774_35590 INIT bckcett testIDS dsadm BLS FOR LK
# MAGIC ^1_2 06/02/08 20:20:40 Batch  14764_73245 PROMOTE bckcett testIDS u03651 steph for Laurel
# MAGIC ^1_2 06/02/08 20:14:00 Batch  14764_72849 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 05/25/08 15:25:52 Batch  14756_55555 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/31/07 10:57:41 Batch  14276_39482 PROMOTE bckcetl ids20 dsadm RC
# MAGIC ^1_1 01/31/07 10:53:40 Batch  14276_39257 INIT bckcett testIDS30 dsadm RC 
# MAGIC ^1_9 01/29/07 14:13:46 Batch  14274_51230 PROMOTE bckcett testIDS30 u150129 Laurel
# MAGIC ^1_9 01/29/07 14:11:38 Batch  14274_51101 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_8 01/26/07 09:43:57 Batch  14271_35041 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_6 12/20/06 14:43:13 Batch  14234_52996 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_5 12/20/06 14:38:24 Batch  14234_52708 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_4 12/19/06 09:19:47 Batch  14233_33591 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_3 11/22/06 12:05:13 Batch  14206_43515 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_2 11/22/06 12:04:50 Batch  14206_43492 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC ^1_1 11/22/06 12:00:32 Batch  14206_43235 INIT bckcett devlIDS30 u150129 Laurel
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:     IdsPCaeMbrPrmcyHist
# MAGIC 
# MAGIC CALLED BY:  IdsPCaeSeq
# MAGIC 
# MAGIC DESCRIPTION:  Extracts all member sk records for any record in P_CAE_MBR_PRMCY where the PRMCY_IND = "Y" and does the same thing for the P_CAE_MBR_DRVR table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-10-31      Laurel Kindley            initial programming
# MAGIC                         
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)                                   Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)                                           --------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Laurel Kindley\(9)2008-05-22\(9)3051\(9)\(9)Added history population for P_CAE_MBR_DRVR\(9)                                   devlIDS                                    Steph Goddard        05/28/2008
# MAGIC                         
# MAGIC 
# MAGIC Karthik 
# MAGIC Chintalapani \(9)2013-1-31\(9)TTR  1510          Updated the source query in the stage PRMCY                                              IntegrateNewDevl                    Kalyan Neelam      2013-02-06
# MAGIC                                                                                            to filter   SC members using PRNT_GRP_ID 
# MAGIC                                                                                            <>650600000 as per the new mapping rules.
# MAGIC 
# MAGIC Karthik 
# MAGIC Chintalapani \(9)2015-09-15\(9)5212 PI                      Added the filter HOST_MBR_IN<>Y' in the extract query                              IntegrateDev1          
# MAGIC                                                                                                  to exclude host members from being populated to the primacy table 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9) 
# MAGIC                                                                                           
# MAGIC                                                                                                                                                                                                                          
# MAGIC                                                                                                                                                                                                               
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)

# MAGIC Extract any MBR_SKs from IDS that are not already in the P_CAE_MBR_PRMCY table.  Pull only those MBR_SKs for the INDIV_BE_KEYs where the PRMCY_IND = 'Y'.
# MAGIC Extract any MBR_SKs from IDS that are not already in the P_CAE_MBR_DRVR table.  Pull only those MBR_SKs for the INDIV_BE_KEYs where the PRMCY_IND = 'Y'.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
CurrDate = get_widget_value('CurrDate', '2008-05-20')

# PRMCY Stage (DB2Connector) - Read from IDS database
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
MBR.MBR_SK,
P_CAE_MBR_PRMCY.CAE_UNIQ_KEY,
MBR.INDV_BE_KEY,
SUB.GRP_SK,
MBR.BRTH_DT_SK,
CD_MPPNG.TRGT_CD
FROM 
{IDSOwner}.p_cae_mbr_prmcy P_CAE_MBR_PRMCY,
{IDSOwner}.mbr MBR,
{IDSOwner}.sub SUB,
{IDSOwner}.cd_mppng CD_MPPNG,
{IDSOwner}.grp GRP,
{IDSOwner}.prnt_grp PRNT_GRP
WHERE 
GRP.PRNT_GRP_SK=PRNT_GRP.PRNT_GRP_SK AND
PRNT_GRP.PRNT_GRP_ID <> '650600000' AND
SUB.GRP_SK = GRP.GRP_SK AND
P_CAE_MBR_PRMCY.INDV_BE_KEY = MBR.INDV_BE_KEY AND
P_CAE_MBR_PRMCY.PRI_IN='Y' AND MBR.SUB_SK = SUB.SUB_SK AND
MBR.MBR_GNDR_CD_SK = CD_MPPNG.CD_MPPNG_SK AND
MBR.HOST_MBR_IN <> 'Y' AND
MBR.MBR_SK NOT IN (
  SELECT P_CAE_MBR_PRMCY2.MBR_SK 
  FROM {IDSOwner}.p_cae_mbr_prmcy P_CAE_MBR_PRMCY2
)
"""
df_PRMCY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Prmcy_Business_Rules (CTransformerStage)
df_Primcy_Business_Rules = df_PRMCY.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.lit("N").alias("PRI_IN"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("BRTH_DT_SK").alias("MBR_BRTH_DT"),
    F.col("TRGT_CD").alias("MBR_GNDR_CD"),
    F.lit(CurrDate).alias("LAST_UPDT_DT")
)

# P_MBR_PRMCY_dat (CSeqFileStage) - Write to file
df_P_MBR_PRMCY_dat = (
    df_Primcy_Business_Rules
    .withColumn("PRI_IN", F.rpad(F.col("PRI_IN"), 1, " "))
    .select(
        "MBR_SK",
        "CAE_UNIQ_KEY",
        "INDV_BE_KEY",
        "PRI_IN",
        "GRP_SK",
        "MBR_BRTH_DT",
        "MBR_GNDR_CD",
        "LAST_UPDT_DT"
    )
)
write_files(
    df_P_MBR_PRMCY_dat,
    f"{adls_path}/load/P_CAE_MBR_PRMCY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# DRVR Stage (DB2Connector) - Read from IDS database
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
MBR.MBR_SK as MBR_SK,
P_CAE_MBR_DRVR.CAE_UNIQ_KEY as CAE_UNIQ_KEY,
MBR.INDV_BE_KEY as INDV_BE_KEY,
SUB.GRP_SK as GRP_SK,
MBR.BRTH_DT_SK as BRTH_DT_SK,
CD_MPPNG.TRGT_CD as MBR_GNDR_CD
FROM 
{IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR,
{IDSOwner}.mbr MBR,
{IDSOwner}.sub SUB,
{IDSOwner}.cd_mppng CD_MPPNG
WHERE 
P_CAE_MBR_DRVR.INDV_BE_KEY = MBR.INDV_BE_KEY AND
P_CAE_MBR_DRVR.PRI_IN='Y' AND MBR.SUB_SK = SUB.SUB_SK AND
MBR.MBR_GNDR_CD_SK = CD_MPPNG.CD_MPPNG_SK AND
MBR.MBR_SK NOT IN (
  SELECT P_CAE_MBR_DRVR2.MBR_SK
  FROM {IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR2
)
"""
df_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Drvr_Business_Rules (CTransformerStage)
df_Drvr_Business_Rules = df_DRVR.select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.lit("N").alias("PRI_IN"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("BRTH_DT_SK").alias("MBR_BRTH_DT"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.lit(CurrDate).alias("LAST_UPDT_DT")
)

# P_CAE_MBR_DRVR_dat (CSeqFileStage) - Write to file
df_P_CAE_MBR_DRVR_dat = (
    df_Drvr_Business_Rules
    .withColumn("PRI_IN", F.rpad(F.col("PRI_IN"), 1, " "))
    .select(
        "MBR_SK",
        "CAE_UNIQ_KEY",
        "INDV_BE_KEY",
        "PRI_IN",
        "GRP_SK",
        "MBR_BRTH_DT",
        "MBR_GNDR_CD",
        "LAST_UPDT_DT"
    )
)
write_files(
    df_P_CAE_MBR_DRVR_dat,
    f"{adls_path}/load/P_CAE_MBR_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)