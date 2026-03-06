# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC Â© Copyright 2019 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : OptumACADrugLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      OptumRx ACA Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                            Project/Altiris #                           Change Description                                                                                                           Development Project      Code Reviewer              Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      08/05/2020      6131- PBM Replacement            Initial Programming                                                                                                                  IntegrateDev2                 Sravya Gorla                       2020/12/09
# MAGIC  
# MAGIC Rekha Radhakrishna    2020-02-05        PBM PhaseII                        Added LOB indicator to Program Info written to Audit File and DATESBM                       IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2021-02-09
# MAGIC Geetanjali Rajendran    2021-06-03        PBM PhaseII                        Added TIER_ID, DRUG_TYPE_CD and CLIENTDEF2 to the                                              IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2021-06-23
# MAGIC                                                                                                                              DrugClmPrice Landing file and modified the DRUG_TYP derivation in
# MAGIC                                                                                                                              PaidReversal Landing file                                                \(9)
# MAGIC 
# MAGIC Bill Schroeder               2023-07-21        US-586570                        Added extract file DRUG_CLM_ACCUM_IMPCT_prep and RunCycle parm in Job Properties.  IntegrateDev2         
# MAGIC 
# MAGIC Arpitha V                      2023-11-07        US 600306                        Added  PLN_DRUG_STTUS_CD  to the DrugClmPrice Landing file                                        IntegrateDevB             Jeyaprasanna         2024-01-01
# MAGIC 
# MAGIC Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                    IntegrateDev2                     Jeyaprasanna          2024-03-14

# MAGIC OptumRX Daily Claim File Pre-Processing.
# MAGIC 
# MAGIC Takes Optum Daily Claim File and breaks it down into files for Claims, Denied Rejected and Spread Pricing.
# MAGIC Member Not Found Error file is not created becase decision is not made on where to load it finally
# MAGIC 
# MAGIC All claims will be loaded , even when member not found  with MBR_UNIQ_KEY = 0
# MAGIC Append header record to audit file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Utility_DS_Integrate
# COMMAND ----------
# Databricks notebook source
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    length,
    regexp_replace,
    concat,
    lpad,
    rpad,
    trim as pyspark_trim,
    upper,
    to_date,
    date_format,
    sum as _sum,
    count as _count
)
from pyspark.sql.types import DecimalType, StringType, IntegerType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


# COMMAND ----------
# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunDate = get_widget_value('RunDate','')
InFile = get_widget_value('InFile','')
MbrTermDate = get_widget_value('MbrTermDate','')
RunCycle = get_widget_value('RunCycle','')

# Also retrieve the database secret name for IDS
ids_secret_name = get_widget_value('ids_secret_name','')

# COMMAND ----------
# Prepare JDBC configuration for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# STAGE: IDS_MBR (DB2Connector) => read from IDS database
# --------------------------------------------------------------------------------
extract_query = f"""
SELECT 
SUB.SUB_ID || MBR.MBR_SFX_NO as MEMBERID,
GRP.GRP_ID,
MBR.MBR_UNIQ_KEY,
SUB.SUB_ID,
MBR.MBR_SFX_NO
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.GRP GRP
WHERE MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR.TERM_DT_SK >= '{MbrTermDate}'
"""

df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_optum_clm_mbr_land (CHashedFileStage)
# Scenario A – Intermediate hashed file. 
# We deduplicate on key columns [MEMBER_ID, GRP_ID].
# The hashed file columns: [MEMBER_ID (true PK), GRP_ID (true PK), MBR_UNIQ_KEY, SUB_ID, MBR_SFX_NO].
# We'll rename them to match exactly how they're referenced:
# MEMBER_ID => from df_IDS_MBR["MEMBERID"]
# GRP_ID => from df_IDS_MBR["GRP_ID"]
# MBR_UNIQ_KEY => from df_IDS_MBR["MBR_UNIQ_KEY"]
# SUB_ID => from df_IDS_MBR["SUB_ID"]
# MBR_SFX_NO => from df_IDS_MBR["MBR_SFX_NO"]
# --------------------------------------------------------------------------------
# Deduplicate by [MEMBER_ID, GRP_ID]
df_hf_optum_clm_mbr_land = (
    df_IDS_MBR
    .withColumnRenamed("MEMBERID", "MEMBER_ID")
    .withColumnRenamed("GRP_ID", "GRP_ID")
    .withColumnRenamed("MBR_UNIQ_KEY", "MBR_UNIQ_KEY")
    .withColumnRenamed("SUB_ID", "SUB_ID")
    .withColumnRenamed("MBR_SFX_NO", "MBR_SFX_NO")
)

df_hf_optum_clm_mbr_land = dedup_sort(
    df_hf_optum_clm_mbr_land,
    partition_cols=["MEMBER_ID","GRP_ID"],
    sort_cols=[("MEMBER_ID","A")]
)

# --------------------------------------------------------------------------------
# STAGE: Optum (CSeqFileStage) => reading from file #InFile# in "landing" => thus from adls_path_raw
# We have a large schema. Since the job lists the columns, we define them for reading the CSV.
# ContainsHeader = false, columnDelimiter (assume comma), quoteChar="\""
# --------------------------------------------------------------------------------

# Build the schema from the stage columns. Everything read as string for transformations.
cols_Optum = [
  "RXCLMNBR",
  "CLMSEQNBR",
  "CLAIMSTS",
  "CARID",
  "SCARID",
  "CARRPROC",
  "CLNTID",
  "CLNTSGMNT",
  "CLNTREGION",
  "ACCOUNTID",
  "ACCTBENCDE",
  "VERSIONNBR",
  "GROUPID",
  "GROUPPLAN",
  "GRPCLIBENF",
  "GROUPSIC",
  "CLMRESPSTS",
  "MEMBERID",
  "MBRLSTNME",
  "MBRFSTNME",
  "MBRMDINIT",
  "MBRPRSNCDE",
  "MBRRELCDE",
  "MBRSEX",
  "MBRBIRTH",
  "MBRAGE",
  "MBRZIP",
  "SOCSECNBR",
  "DURKEY",
  "DURFLAG",
  "MBRFAMLYID",
  "MBRFAMLIND",
  "MBRFAMLTYP",
  "COBIND",
  "MBRPLAN",
  "MBRPRODCDE",
  "MBRRIDERCD",
  "CARENETID",
  "CAREQUALID",
  "CAREFACID",
  "CAREFACNAM",
  "MBRPCPHYS",
  "PPRSFSTNME",
  "PPRSLSTNME",
  "PPRSMDINIT",
  "PPRSSPCCDE",
  "PPRSTATE",
  "MBRALTINFL",
  "MBRALTINCD",
  "MBRALTINID",
  "MBRMEDDTE",
  "MBRMEDTYPE",
  "MBRHICCDE",
  "CARDHOLDER",
  "PATLASTNME",
  "PATFRSTNME",
  "PERSONCDE",
  "RELATIONCD",
  "SEXCODE",
  "BIRTHDTE",
  "ELIGCLARIF",
  "CUSTLOC",
  "SBMPLSRVCE",
  "SBMPATRESD",
  "PRMCAREPRV",
  "PRMCAREPRQ",
  "FACILITYID",
  "OTHCOVERAG",
  "BINNUMBER",
  "PROCESSOR",
  "GROUPNBR",
  "TRANSCDE",
  "DATESBM",
  "TIMESBM",
  "CHGDATE",
  "CHGTIME",
  "ORGPDSBMDT",
  "RVDATESBM",
  "CLMCOUNTER",
  "GENERICCTR",
  "FORMLRYCTR",
  "RXNUMBER",
  "RXNUMBERQL",
  "REFILL",
  "DISPSTATUS",
  "DTEFILLED",
  "COMPOUNDCD",
  "SBMCMPDTYP",
  "PRODTYPCDE",
  "PRODUCTID",
  "PRODUCTKEY",
  "METRICQTY",
  "DECIMALQTY",
  "DAYSSUPPLY",
  "PSC",
  "WRITTENDTE",
  "NBRFLSAUTH",
  "ORIGINCDE",
  "SBMCLARCD1",
  "SBMCLARCD2",
  "SBMCLARCD3",
  "PAMCNBR",
  "PAMCCDE",
  "PRAUTHNBR",
  "PRAUTHRSN",
  "PRAUTHFDTE",
  "PRAUTHTDTE",
  "LABELNAME",
  "PRODNM",
  "DRUGMFGRID",
  "DRUGMFGR",
  "GPINUMBER",
  "GNRCNME",
  "PRDPACUOM",
  "PRDPACSIZE",
  "DDID",
  "GCN",
  "GCNSEQ",
  "KDC",
  "AHFS",
  "DRUGDEACOD",
  "RXOTCIND",
  "MULTSRCCDE",
  "GENINDOVER",
  "PRDREIMIND",
  "BRNDTRDNME",
  "FDATHERAEQ",
  "METRICSTRG",
  "DRGSTRGUOM",
  "ADMINROUTE",
  "ADMINRTESN",
  "DOSAGEFORM",
  "NDA",
  "ANDA",
  "ANDAOR",
  "RXNORMCODE",
  "MNTDRUGCDE",
  "MNTSOURCE",
  "MNTCARPROR",
  "MNTGPILIST",
  "CPQSPCPRG",
  "CPQSPCPGIN",
  "CPQSPCSCHD",
  "THRDPARTYX",
  "DRGUNITDOS",
  "SBMUNITDOS",
  "ALTPRODTYP",
  "ALTPRODCDE",
  "SRXNETWRK",
  "SRXNETTYPE",
  "RXNETWORK",
  "RXNETWRKNM",
  "RXNETCARR",
  "RXNETWRKQL",
  "RXNETPRCQL",
  "REGIONCDE",
  "PHRAFFIL",
  "NETPRIOR",
  "NETTYPE",
  "NETSEQ",
  "PAYCNTR",
  "PHRNDCLST",
  "PHRGPILST",
  "SRVPROVID",
  "SRVPROVIDQ",
  "NPIPROV",
  "PRVNCPDPID",
  "SBMSRVPRID",
  "SBMSRVPRQL",
  "SRVPROVNME",
  "PROVLOCKQL",
  "PROVLOCKID",
  "STORENBR",
  "AFFILIATIN",
  "PAYEEID",
  "DISPRCLASS",
  "DISPROTHER",
  "PHARMZIP",
  "PRESNETWID",
  "PRESCRIBER",
  "PRESCRIDQL",
  "NPIPRESCR",
  "PRESCDEAID",
  "PRESLSTNME",
  "PRESFSTNME",
  "PRESMDINIT",
  "PRESSPCCDE",
  "PRESSPCCDQ",
  "PRSSTATE",
  "SBMRPHID",
  "SBMRPHIDQL",
  "FNLPLANCDE",
  "FNLPLANDTE",
  "PLANTYPE",
  "PLANQUAL",
  "PLNNDCLIST",
  "LSTQUALNDC",
  "PLNGPILIST",
  "LSTQUALGPI",
  "PLNPNDCLST",
  "PLNPGPILST",
  "PLANDRUGST",
  "PLANFRMLRY",
  "PLNFNLPSCH",
  "PHRDCSCHID",
  "PHRDCSCHSQ",
  "CLTDCSCHID",
  "CLTDCSCHSQ",
  "PHRDCCSCID",
  "PHRDCCSCSQ",
  "CLTDCCSCID",
  "CLTDCCSCSQ",
  "PHRPRTSCID",
  "CLTPRTSCID",
  "PHRRMSCHID",
  "CLTRMSCHID",
  "PRDPFLSTID",
  "PRFPRDSCID",
  "FORMULARY",
  "FORMLRFLAG",
  "TIERVALUE",
  "CONTHERAPY",
  "CTSCHEDID",
  "PRBASISCHD",
  "REGDISOR",
  "DRUGSTSTBL",
  "TRANSBEN",
  "MESSAGECD1",
  "MESSAGE1",
  "MESSAGECD2",
  "MESSAGE2",
  "MESSAGECD3",
  "MESSAGE3",
  "REJCNT",
  "REJCDE1",
  "REJCDE2",
  "REJCDE3",
  "RJCPLANID",
  "DURCONFLCT",
  "DURINTERVN",
  "DUROUTCOME",
  "LVLSERVICE",
  "PHARSRVTYP",
  "DIAGNOSIS",
  "DIAGNOSISQ",
  "RVDURCNFLC",
  "RVDURINTRV",
  "RVDUROUTCM",
  "RVLVLSERVC",
  "DRGCNFLCT1",
  "SEVERITY1",
  "OTHRPHARM1",
  "DTEPRVFIL1",
  "QTYPRVFIL1",
  "DATABASE1",
  "OTHRPRESC1",
  "FREETEXT1",
  "DRGCNFLCT2",
  "SEVERITY2",
  "OTHRPHARM2",
  "DTEPRVFIL2",
  "QTYPRVFIL2",
  "DATABASE2",
  "OTHRPRESC2",
  "FREETEXT2",
  "DRGCNFLCT3",
  "SEVERITY3",
  "OTHRPHARM3",
  "DTEPRVFIL3",
  "QTYPRVFIL3",
  "DATABASE3",
  "OTHRPRESC3",
  "FREETEXT3",
  "FEETYPE",
  "BENUNITCST",
  "AWPUNITCST",
  "WACUNITCST",
  "GEAPUNTCST",
  "CTYPEUCOST",
  "BASISCOST",
  "PRICEQTY",
  "PRODAYSSUP",
  "PROQTY",
  "RVINCNTVSB",
  "SBMINGRCST",
  "SBMDISPFEE",
  "SBMPSLSTX",
  "SBMFSLSTX",
  "SBMSLSTX",
  "SBMPATPAY",
  "SBMAMTDUE",
  "SBMINCENTV",
  "SBMPROFFEE",
  "SBMTOTHAMT",
  "SBMOPAMTCT",
  "SBMOPAMTQL",
  "USUALNCUST",
  "DENIALDTE",
  "OTHRPAYOR",
  "SBMMDPDAMT",
  "CALINGRCST",
  "CALDISPFEE",
  "CALPSTAX",
  "CALFSTAX",
  "CALSLSTAX",
  "CALPATPAY",
  "CALDUEAMT",
  "CALWITHHLD",
  "CALFCOPAY",
  "CALPCOPAY",
  "CALCOPAY",
  "CALPRODSEL",
  "CALATRTAX",
  "CALEXCEBFT",
  "CALINCENTV",
  "CALATRDED",
  "CALCOB",
  "CALTOTHAMT",
  "CALPROFFEE",
  "CALOTHPAYA",
  "CALCOSTSRC",
  "CALADMNFEE",
  "CALPROCFEE",
  "CALPATSTAX",
  "CALPLNSTAX",
  "CALPRVNSEL",
  "CALPSCBRND",
  "CALPSCNONP",
  "CALPSCBRNP",
  "CALCOVGAP",
  "CALINGCSTC",
  "CALDSPFEEC",
  "PHRINGRCST",
  "PHRDISPFEE",
  "PHRPPSTAX",
  "PHRFSTAX",
  "PHRSLSTAX",
  "PHRPATPAY",
  "PHRDUEAMT",
  "PHRWITHHLD",
  "PHRPPRCS",
  "PHRPRCST",
  "PHRPTPS",
  "PHRPTPST",
  "PHRCOPAYSC",
  "PHRCOPAYSS",
  "PHRFCOPAY",
  "PHRPCOPAY",
  "PHRCOPAY",
  "PHRPRODSEL",
  "PHRATRTAX",
  "PHREXCEBFT",
  "PHRINCENTV",
  "PHRATRDED",
  "PHRCOB",
  "PHRTOTHAMT",
  "PHRPROFFEE",
  "PHROTHPAYA",
  "PHRCOSTSRC",
  "PHRCOSTTYP",
  "PHRPRCTYPE",
  "PHRRATE",
  "PHRPROCFEE",
  "PHRPATSTAX",
  "PHRPLNSTAX",
  "PHRPRVNSEL",
  "PHRPSCBRND",
  "PHRPSCNONP",
  "PHRPSCBRNP",
  "PHRCOVGAP",
  "PHRINGCSTC",
  "PHRDSPFEEC",
  "POSINGRCST",
  "POSDISPFEE",
  "POSPSLSTAX",
  "POSFSLSTAX",
  "POSSLSTAX",
  "POSPATPAY",
  "POSDUEAMT",
  "POSWITHHLD",
  "POSCOPAY",
  "POSPRODSEL",
  "POSATRTAX",
  "POSEXCEBFT",
  "POSINCENTV",
  "POSATRDED",
  "POSTOTHAMT",
  "POSPROFFEE",
  "POSOTHPAYA",
  "POSCOSTSRC",
  "POSPROCFEE",
  "POSPATSTAX",
  "POSPLNSTAX",
  "POSPRVNSEL",
  "POSPSCBRND",
  "POSPSCNONP",
  "POSPSCBRNP",
  "POSCOVGAP",
  "POSINGCSTC",
  "POSDSPFEEC",
  "CLIENTFLAG",
  "CLTINGRCST",
  "CLTDISPFEE",
  "CLTSLSTAX",
  "CLTPATPAY",
  "CLTDUEAMT",
  "CLTWITHHLD",
  "CLTPRCS",
  "CLTPRCST",
  "CLTPTPS",
  "CLTPTPST",
  "CLTCOPAYS",
  "CLTCOPAYSS",
  "CLTFCOPAY",
  "CLTPCOPAY",
  "CLTCOPAY",
  "CLTPRODSEL",
  "CLTPSTAX",
  "CLTFSTAX",
  "CLTATRTAX",
  "CLTEXCEBFT",
  "CLTINCENTV",
  "CLTATRDED",
  "CLTTOTHAMT",
  "CLTPROFFEE",
  "CLTCOB",
  "CLTOTHPAYA",
  "CLTCOSTSRC",
  "CLTCOSTTYP",
  "CLTPRCTYPE",
  "CLTRATE",
  "CLTPRSCSTP",
  "CLTPRSCHNM",
  "CLTPROCFEE",
  "CLTPATSTAX",
  "CLTPLNSTAX",
  "CLTPRVNSEL",
  "CLTPSCBRND",
  "CLTPSCNONP",
  "CLTPSCBRNP",
  "CLTCOVGAP",
  "CLTINGCSTC",
  "CLTDSPFEEC",
  "RSPREIMBUR",
  "RSPINGRCST",
  "RSPDISPFEE",
  "RSPPSLSTAX",
  "RSPFSLSTAX",
  "RSPSLSTAX",
  "RSPPATPAY",
  "RSPDUEAMT",
  "RSPFCOPAY",
  "RSPPCOPAY",
  "RSPCOPAY",
  "RSPPRODSEL",
  "RSPATRTAX",
  "RSPEXCEBFT",
  "RSPINCENTV",
  "RSPATRDED",
  "RSPTOTHAMT",
  "RSPPROFEE",
  "RSPOTHPAYA",
  "RSPACCUDED",
  "RSPREMBFT",
  "RSPREMDED",
  "RSPPROCFEE",
  "RSPPATSTAX",
  "RSPPLNSTAX",
  "RSPPRVNSEL",
  "RSPPSCBRND",
  "RSPPSCNONP",
  "RSPPSCBRNP",
  "RSPCOVGAP",
  "RSPINGCSTC",
  "RSPDSPFEEC",
  "RSPPLANID",
  "BENSTGQL1",
  "BENSTGAMT1",
  "BENSTGQL2",
  "BENSTGAMT2",
  "BENSTGQL3",
  "BENSTGAMT3",
  "BENSTGQL4",
  "BENSTGAMT4",
  "ESTGENSAV",
  "SPDACCTREM",
  "HLTHPLNAMT",
  "INDDEDPTD",
  "INDDEDREM",
  "FAMDEDPTD",
  "FAMDEDREM",
  "DEDSCHED",
  "DEDACCC",
  "DEDFLAG",
  "INDLBFTUT_1",
  "INDLBFTPTD",
  "INDLBFTREM",
  "INDLBFTUT_2",
  "FAMLBFTPTD",
  "FAMLBFTREM",
  "LFTBFTMSCH",
  "LFTBFTACCC",
  "LFTBFTFLAG",
  "INDBFTUT",
  "INDBMAXPTD",
  "FAMBFTUT",
  "FAMBMAXPTD",
  "INDBMAXREM",
  "FAMBMAXREM",
  "BFTMAXSCHD",
  "BFTMAXACCC",
  "BFTMAXFLAG",
  "INDOOPPTD",
  "FAMOOPPTD",
  "INDOOPREM",
  "FAMOOPREM",
  "OOPSCHED",
  "OOPACCC",
  "OOPFLAG",
  "CONTRIBUT",
  "CONTBASIS",
  "CONTSCHED",
  "CONTACCCD",
  "CONTFLAG",
  "RXTFLAG",
  "REIMBURSMT",
  "CLMORIGIN",
  "HLDCLMFLAG",
  "HLDCLMDAYS",
  "PARTDFLAG",
  "COBEXTFLG",
  "PAEXTFLG",
  "HSAEXTIND",
  "FFPMEDRMST",
  "FFPMEDPXST",
  "FFPMEDMSST",
  "INCIDENTID",
  "ETCNBR",
  "DTEINJURY",
  "ADDUSER",
  "CHGUSER",
  "DMRUSERID",
  "PRAUSERID",
  "CLAIMREFID",
  "EOBDNOV",
  "EOBPDOV",
  "MANTRKNBR",
  "MANRECVDTE",
  "PASAUTHTYP",
  "PASAUTHID",
  "PASREQTYPE",
  "PASREQFROM",
  "PASREQTHRU",
  "PASBASISRQ",
  "PASREPFN",
  "PASREPLN",
  "PASSTREET",
  "PASCITY",
  "PASSTATE",
  "PASZIP",
  "PASPANBR",
  "PASAUTHNBR",
  "PASSDOCCT",
  "PAYERTYPE",
  "DELAYRSNCD",
  "MEDCDIND",
  "MEDCDID",
  "MEDCDAGNBR",
  "MEDCDTCN",
  "FMSTIER",
  "FMSSTATUS",
  "FMSDFLTIND",
  "FMSBENLST",
  "FMSLSTLVL3",
  "FMSLSTLVL2",
  "FMSLSTLVL1",
  "FMSRULESET",
  "FMSRULE",
  "FMSPROCCD",
  "CLIENTDEF1",
  "CLIENTDEF2",
  "CLIENTDEF3",
  "CLIENTDEF4",
  "CLIENTDEF5",
  "CCTRESERV1",
  "CCTRESERV2",
  "CCTRESERV3",
  "CCTRESERV4",
  "CCTRESERV5",
  "CCTRESERV6",
  "CCTRESERV7",
  "CCTRESERV8",
  "CCTRESERV9",
  "CCTRESRV10",
  "OOPAPPLIED",
  "CCTRESRV12",
  "CCTRESRV13",
  "CCTRESRV14",
  "USERFIELD",
  "EXTRACTDTE",
  "BATCHCTRL",
  "CLTTYPUCST",
  "REJCDE4",
  "REJCDE5",
  "REJCDE6",
  "MESSAGECD4",
  "MESSAGE4",
  "MESSAGECD5",
  "MESSAGE5",
  "MESSAGECD6",
  "MESSAGEG6",
  "CLIENT2FLAG",
  "CLT2INGRCST",
  "CLT2DISPFEE",
  "CLT2SLSTAX",
  "CLT2DUEAMT",
  "CLTPRSCSTP2",
  "CLTPRSCHNM2",
  "CLT2PRCS",
  "CLT2PRCST",
  "CLT2PSTAX",
  "CLT2FSTAX",
  "CLT2OTHAMT",
  "CLT2COSTSRC",
  "CLT2COSTTYP",
  "CLT2PRCTYPE",
  "CLT2RATE",
  "CLT2DCCSCID",
  "CLT2DCCSCSQ",
  "CLT2DCSCHID",
  "CLT2DCSCHSQ",
  "CALINCENTV2",
  "CLNT3FLAG",
  "CLT3INGRCST",
  "CLT3DISPFEE",
  "CLT3SLSTAX",
  "CLT3DUEAMT",
  "CLTPRSCSTP3",
  "CLTPRSCHNM3",
  "CLT3PRCS",
  "CLT3PRCST",
  "CLT3PSTAX",
  "CLT3FSTAX",
  "CLT3OTHAMT",
  "CLT3COSTSRC",
  "CLT3COSTTYP",
  "CLT3PRCTYPE",
  "CLT3RATE",
  "CLT3DCCSCID",
  "CLT3DCCSCSQ",
  "CLT3DCSCHID",
  "CLT3DCSCHSQ",
  "CALINCENTV3",
  "TBR_TBR_REJECT_REASON1",
  "TBR_TBR_REJECT_REASON2",
  "TBR_TBR_REJECT_REASON3",
  "SBM_QTY_INT_DIS",
  "SBM_DS_INT_DIS",
  "OTHR_AMT_CLMSBM_CT",
  "OTHR_AMT_CLMSBM_QLFR1",
  "OTHR_AMT_CLMSBM1",
  "OTHR_AMT_CLMSBM_QLFR2",
  "OTHR_AMT_CLMSBM2",
  "OTHR_AMT_CLMSBM_QLFR3",
  "OTHR_AMT_CLMSBM3",
  "OTHR_AMT_CLMSBM_QLFR4",
  "OTHR_AMT_CLMSBM4",
  "OTHR_AMT_CLMSBM_QLFR5",
  "OTHR_AMT_CLMSBM5",
  "RSN_SRVCDE1",
  "RSN_SRVCDE2",
  "RSN_SRVCDE3",
  "PROF_SRVCDE1",
  "PROF_SRVCDE2",
  "PROF_SRVCDE3",
  "RESULT_OF_SRV_CDE1",
  "RESULT_OF_SRV_CDE2",
  "RESULT_OF_SRV_CDE3",
  "SCH_RX_ID_NBR",
  "BASIS_CAL_DISPFEE",
  "BASIS_CAL_COPAY",
  "BASIS_CAL_FLT_TAX",
  "BASIS_CAL_PRCNT_TAX",
  "BASIS_CAL_COINSUR",
  "PAID_MSG_CODE_COUNT",
  "PAID_MESSAGE_CODE_1",
  "PAID_MESSAGE_CODE_2",
  "PAID_MESSAGE_CODE_3",
  "PAID_MESSAGE_CODE_4",
  "PAID_MESSAGE_CODE_5",
  "FILLER_FOR_REJECT_FIELD_IND"
]

schema_Optum = []
# We will read them all as string for subsequent transformations
for c in cols_Optum:
    schema_Optum.append(StringType())

# Read the file
file_path_Optum = f"{adls_path_raw}/landing/{InFile}"
df_Optum_raw = (
    spark.read
    .option("sep", ",")
    .option("quote", '"')
    .option("header", "false")
    .csv(file_path_Optum, schema=StringType())  # We'll read in a single string column first, then split
)

# Because the DS job has 200+ columns, we mimic that by splitting each row by comma ourselves:
# (Alternatively, we could define a struct schema with 200 columns. But that is extremely verbose.)
# We'll do a split on commas for the single column. Then each field goes to a separate col_i. Then rename.
split_cols = [pyspark_trim(col("_c0").split(",")[i]).alias(cols_Optum[i]) for i in range(len(cols_Optum))]
df_Optum = df_Optum_raw.select(split_cols)

# --------------------------------------------------------------------------------
# STAGE: DataCleansing (CTransformerStage)
# Has a Stage Variable: svDATESBM
# Then produces 2 output links: 
#   1) inMbrMatch => going to "Member_Lkp"
#   2) Aggregate => going to "CntByAdjDtRecType"
# --------------------------------------------------------------------------------

# We'll define a helper function to remove \r,\n,\t and do "yyyyMMdd" => "yyyy-MM-dd"
def format_date_expr(src_col):
    # if len(trim(src_col)) == 0 => "1753-01-01"
    # else parse as yyyymmdd -> yyyy-mm-dd
    cleaned = regexp_replace(pyspark_trim(col(src_col)), "[\\r\\n\\t]", "")
    return when(length(cleaned) == 0, lit("1753-01-01")).otherwise(
        date_format(to_date(cleaned, "yyyyMMdd"), "yyyy-MM-dd")
    )

def safe_num(col_name, default_value):
    return when(
        (col(col_name).isNull()) | (length(pyspark_trim(col(col_name))) == 0),
        lit(default_value)
    ).otherwise(col(col_name))

# Stage Variable svDATESBM
svDATESBM = format_date_expr("DATESBM")

# Build the output DataFrame for inMbrMatch
df_DataCleansing_inMbrMatch = (
    df_Optum
    .withColumn("svDATESBM", svDATESBM)
    .select(
        col("RXCLMNBR").alias("RXCLMNBR"),
        col("CLMSEQNBR").alias("CLMSEQNBR"),
        # PrimaryKey columns:
        pyspark_trim(col("CLAIMSTS")).alias("CLAIMSTS"),
        pyspark_trim(col("CARID")).alias("CARID"),
        when(length(pyspark_trim(col("ACCOUNTID")))==0, lit("UNK")).otherwise(pyspark_trim(col("ACCOUNTID"))).alias("ACCOUNTID"),
        when(length(pyspark_trim(col("GROUPID")))==0, lit("UNK")).otherwise(pyspark_trim(col("GROUPID"))).alias("GROUPID"),
        when(
            (col("MEMBERID").isNull()) | (length(col("MEMBERID"))==0), 
            lit("UNK")
        ).otherwise(col("MEMBERID").substr(1,11)).alias("MEMBERID"),
        when(length(pyspark_trim(col("MBRLSTNME")))==0, lit("UNK")).otherwise(upper(pyspark_trim(col("MBRLSTNME")))).alias("MBRLSTNME"),
        when(length(pyspark_trim(col("MBRFSTNME")))==0, lit("UNK")).otherwise(upper(pyspark_trim(col("MBRFSTNME")))).alias("MBRFSTNME"),
        pyspark_trim(col("MBRRELCDE")).alias("MBRRELCDE"),
        pyspark_trim(col("MBRSEX")).alias("MBRSEX"),
        when(length(col("MBRBIRTH"))==0, lit("1753-01-01"))
         .otherwise(format_date_expr("MBRBIRTH")).alias("MBRBIRTH"),
        pyspark_trim(col("SOCSECNBR")).alias("SOCSECNBR"),
        pyspark_trim(col("CUSTLOC")).alias("CUSTLOC"),
        pyspark_trim(col("SBMPLSRVCE")).alias("SBMPLSRVCE"),
        pyspark_trim(col("FACILITYID")).alias("FACILITYID"),
        pyspark_trim(col("OTHCOVERAG")).alias("OTHCOVERAG"),
        col("svDATESBM").alias("DATESBM"),
        col("TIMESBM").alias("TIMESBM"),
        col("ORGPDSBMDT").alias("ORGPDSBMDT"),
        pyspark_trim(col("RXNUMBER")).alias("RXNUMBER"),
        pyspark_trim(col("REFILL")).alias("REFILL"),
        pyspark_trim(col("DISPSTATUS")).alias("DISPSTATUS"),
        format_date_expr("DTEFILLED").alias("DTEFILLED"),
        pyspark_trim(col("COMPOUNDCD")).alias("COMPOUNDCD"),
        pyspark_trim(col("PRODUCTID")).alias("PRODUCTID"),
        safe_num("METRICQTY", -999999).alias("METRICQTY"),
        safe_num("DECIMALQTY", -999999).alias("DECIMALQTY"),
        when(col("DAYSSUPPLY").isNull() | (length(pyspark_trim(col("DAYSSUPPLY")))==0), lit(-999)).otherwise(pyspark_trim(col("DAYSSUPPLY"))).alias("DAYSSUPPLY"),
        pyspark_trim(col("PSC")).alias("PSC"),
        when(length(pyspark_trim(col("WRITTENDTE")))==0, lit("1753-01-01"))
         .otherwise(format_date_expr("WRITTENDTE")).alias("WRITTENDTE"),
        pyspark_trim(col("NBRFLSAUTH")).alias("NBRFLSAUTH"),
        pyspark_trim(col("ORIGINCDE")).alias("ORIGINCDE"),
        pyspark_trim(col("PRAUTHNBR")).alias("PRAUTHNBR"),
        pyspark_trim(col("LABELNAME")).alias("LABELNAME"),
        col("GPINUMBER").alias("GPINUMBER"),
        col("MULTSRCCDE").alias("MULTSRCCDE"),
        col("GENINDOVER").alias("GENINDOVER"),
        col("CPQSPCPRG").alias("CPQSPCPRG"),
        col("CPQSPCPGIN").alias("CPQSPCPGIN"),
        pyspark_trim(col("SRXNETWRK")).alias("SRXNETWRK"),
        pyspark_trim(col("RXNETWORK")).alias("RXNETWORK"),
        col("RXNETWRKQL").alias("RXNETWRKQL"),
        pyspark_trim(col("SRVPROVID")).alias("SRVPROVID"),
        when(length(pyspark_trim(col("SRVPROVIDQ")))==0, lit("00")).otherwise(pyspark_trim(col("SRVPROVIDQ"))).alias("SRVPROVIDQ"),
        when(col("NPIPROV").isNull() | (length(pyspark_trim(col("NPIPROV"))) == 0), lit("NA")).otherwise(pyspark_trim(col("NPIPROV"))).alias("NPIPROV"),
        when(col("PRVNCPDPID").isNull() | (length(pyspark_trim(col("PRVNCPDPID"))) == 0), lit("NA")).otherwise(pyspark_trim(col("PRVNCPDPID"))).alias("PRVNCPDPID"),
        col("SRVPROVNME").alias("SRVPROVNME"),
        pyspark_trim(col("DISPROTHER")).alias("DISPROTHER"),
        when(col("PRESCRIBER").isNull() | (length(pyspark_trim(col("PRESCRIBER"))) == 0), lit("NA")).otherwise(col("PRESCRIBER")).alias("PRESCRIBER"),
        when(length(pyspark_trim(col("PRESCRIDQL")))==0, lit("00")).otherwise(pyspark_trim(col("PRESCRIDQL"))).alias("PRESCRIDQL"),
        when(col("NPIPRESCR").isNull() | (length(pyspark_trim(col("NPIPRESCR"))) == 0), lit("NA")).otherwise(col("NPIPRESCR")).alias("NPIPRESCR"),
        when(col("PRESCDEAID").isNull() | (length(pyspark_trim(col("PRESCDEAID"))) == 0), lit("NA")).otherwise(pyspark_trim(col("PRESCDEAID"))).alias("PRESCDEAID"),
        when(length(pyspark_trim(col("PRESLSTNME")))==0, lit("")).otherwise(pyspark_trim(col("PRESLSTNME"))).alias("PRESLSTNME"),
        when(length(pyspark_trim(col("PRESFSTNME")))==0, lit("")).otherwise(pyspark_trim(col("PRESFSTNME"))).alias("PRESFSTNME"),
        when(length(pyspark_trim(col("PRESSPCCDE")))==0, lit("NA")).otherwise(pyspark_trim(col("PRESSPCCDE"))).alias("PRESSPCCDE"),
        col("PRSSTATE").alias("PRSSTATE"),
        col("FNLPLANCDE").alias("FNLPLANCDE"),
        col("FNLPLANDTE").alias("FNLPLANDTE"),
        pyspark_trim(col("PLANQUAL")).alias("PLANQUAL"),
        pyspark_trim(col("COBIND")).alias("COBIND"),
        col("FORMLRFLAG").alias("FORMLRFLAG"),
        col("REJCDE1").alias("REJCDE1"),
        col("REJCDE2").alias("REJCDE2"),
        when(col("AWPUNITCST").isNull() | (length(pyspark_trim(col("AWPUNITCST"))) == 0), lit("0.0")).otherwise(col("AWPUNITCST")).alias("AWPUNITCST"),
        col("WACUNITCST").alias("WACUNITCST"),
        col("CTYPEUCOST").alias("CTYPEUCOST"),
        pyspark_trim(col("BASISCOST")).alias("BASISCOST"),
        col("PROQTY").alias("PROQTY"),
        when(col("SBMINGRCST").isNull() | (length(pyspark_trim(col("SBMINGRCST"))) == 0), lit(0)).otherwise(col("SBMINGRCST")).alias("SBMINGRCST"),
        when(col("SBMDISPFEE").isNull() | (length(pyspark_trim(col("SBMDISPFEE"))) == 0), lit(0)).otherwise(col("SBMDISPFEE")).alias("SBMDISPFEE"),
        when(col("SBMAMTDUE").isNull() | (length(pyspark_trim(col("SBMAMTDUE"))) == 0), lit(0)).otherwise(col("SBMAMTDUE")).alias("SBMAMTDUE"),
        when(col("USUALNCUST").isNull() | (length(pyspark_trim(col("USUALNCUST"))) == 0), lit(0)).otherwise(col("USUALNCUST")).alias("USUALNCUST"),
        when(length(pyspark_trim(col("DENIALDTE")))==0, lit("1753-01-01"))
         .otherwise(format_date_expr("DENIALDTE")).alias("DENIALDTE"),
        when(col("PHRDISPFEE").isNull() | (length(pyspark_trim(col("PHRDISPFEE"))) == 0), lit(0)).otherwise(col("PHRDISPFEE")).alias("PHRDISPFEE"),
        when(col("CLTINGRCST").isNull() | (length(pyspark_trim(col("CLTINGRCST"))) == 0), lit(0)).otherwise(col("CLTINGRCST")).alias("CLTINGRCST"),
        when(col("CLTDISPFEE").isNull() | (length(pyspark_trim(col("CLTDISPFEE"))) == 0), lit(0)).otherwise(col("CLTDISPFEE")).alias("CLTDISPFEE"),
        when(col("CLTSLSTAX").isNull() | (length(pyspark_trim(col("CLTSLSTAX"))) == 0), lit(0)).otherwise(col("CLTSLSTAX")).alias("CLTSLSTAX"),
        when(col("CLTPATPAY").isNull() | (length(pyspark_trim(col("CLTPATPAY"))) == 0), lit(0)).otherwise(col("CLTPATPAY")).alias("CLTPATPAY"),
        when(col("CLTDUEAMT").isNull() | (length(pyspark_trim(col("CLTDUEAMT"))) == 0), lit(0.00)).otherwise(col("CLTDUEAMT")).alias("CLTDUEAMT"),
        when(col("CLTFCOPAY").isNull() | (length(pyspark_trim(col("CLTFCOPAY"))) == 0), lit(0)).otherwise(col("CLTFCOPAY")).alias("CLTFCOPAY"),
        when(col("CLTPCOPAY").isNull() | (length(pyspark_trim(col("CLTPCOPAY"))) == 0), lit(0)).otherwise(col("CLTPCOPAY")).alias("CLTPCOPAY"),
        when(col("CLTCOPAY").isNull() | (length(pyspark_trim(col("CLTCOPAY"))) == 0), lit(0)).otherwise(col("CLTCOPAY")).alias("CLTCOPAY"),
        when(col("CLTINCENTV").isNull() | (length(pyspark_trim(col("CLTINCENTV"))) == 0), lit(0)).otherwise(col("CLTINCENTV")).alias("CLTINCENTV"),
        when(col("CLTATRDED").isNull() | (length(pyspark_trim(col("CLTATRDED"))) == 0), lit(0)).otherwise(col("CLTATRDED")).alias("CLTATRDED"),
        col("CLTTOTHAMT").alias("CLTTOTHAMT"),
        when(col("CLTOTHPAYA").isNull() | (length(pyspark_trim(col("CLTOTHPAYA"))) == 0), lit(0)).otherwise(col("CLTOTHPAYA")).alias("CLTOTHPAYA"),
        col("CLTCOSTTYP").alias("CLTCOSTTYP"),
        col("CLTPRCTYPE").alias("CLTPRCTYPE"),
        col("CLTRATE").alias("CLTRATE"),
        col("CLTPSCBRND").alias("CLTPSCBRND"),
        col("REIMBURSMT").alias("REIMBURSMT"),
        col("CLMORIGIN").alias("CLMORIGIN"),
        col("CLTTYPUCST").alias("CLTTYPUCST"),
        when(col("CLT2INGRCST").isNull() | (length(pyspark_trim(col("CLT2INGRCST"))) == 0), lit(0.00)).otherwise(col("CLT2INGRCST")).alias("CLT2INGRCST"),
        when(col("CLT2DISPFEE").isNull() | (length(pyspark_trim(col("CLT2DISPFEE"))) == 0), lit(0.00)).otherwise(col("CLT2DISPFEE")).alias("CLT2DISPFEE"),
        when(col("CLT2SLSTAX").isNull() | (length(pyspark_trim(col("CLT2SLSTAX"))) == 0), lit(0.00)).otherwise(col("CLT2SLSTAX")).alias("CLT2SLSTAX"),
        when(col("CLT2DUEAMT").isNull() | (length(pyspark_trim(col("CLT2DUEAMT"))) == 0), lit(0.00)).otherwise(col("CLT2DUEAMT")).alias("CLT2DUEAMT"),
        when(col("CLT2PSTAX").isNull() | (length(pyspark_trim(col("CLT2PSTAX"))) == 0), lit(0.00)).otherwise(col("CLT2PSTAX")).alias("CLT2PSTAX"),
        when(col("CLT2FSTAX").isNull() | (length(pyspark_trim(col("CLT2FSTAX"))) == 0), lit(0.00)).otherwise(col("CLT2FSTAX")).alias("CLT2FSTAX"),
        col("CLT2COSTSRC").alias("CLT2COSTSRC"),
        col("CLT2COSTTYP").alias("CLT2COSTTYP"),
        col("CLT2PRCTYPE").alias("CLT2PRCTYPE"),
        col("CLT2RATE").alias("CLT2RATE"),
        when(length(pyspark_trim(col("PPRSFSTNME")))==0, lit("")).otherwise(pyspark_trim(col("PPRSFSTNME"))).alias("PPRSFSTNME"),
        when(length(pyspark_trim(col("PPRSLSTNME")))==0, lit("")).otherwise(pyspark_trim(col("PPRSLSTNME"))).alias("PPRSLSTNME"),
        pyspark_trim(col("PPRSSPCCDE")).alias("PPRSSPCCDE"),
        when(length(pyspark_trim(col("PPRSTATE")))==0, lit("")).otherwise(pyspark_trim(col("PPRSTATE"))).alias("PPRSTATE"),
        when(col("CLT2OTHAMT").isNull() | (length(pyspark_trim(col("CLT2OTHAMT"))) == 0), lit(0.00)).otherwise(col("CLT2OTHAMT")).alias("CLT2OTHAMT"),
        when(col("CALINCENTV2").isNull() | (length(pyspark_trim(col("CALINCENTV2"))) == 0), lit(0.00)).otherwise(col("CALINCENTV2")).alias("CALINCENTV2"),
        when(col("RXNETPRCQL").isNull() | (length(pyspark_trim(col("RXNETPRCQL"))) == 0), lit("N"))
          .otherwise(
             when(pyspark_trim(col("RXNETPRCQL"))=="Y", lit("Y")).otherwise(lit("N"))
          ).alias("RXNETPRCQL"),
        when(col("SBMPATRESD").isNull() | (length(pyspark_trim(col("SBMPATRESD"))) == 0), lit(0)).otherwise(col("SBMPATRESD")).alias("SBMPATRESD"),
        col("PRODTYPCDE").alias("PRODTYPCDE"),
        col("CONTHERAPY").alias("CONTHERAPY"),
        col("CTSCHEDID").alias("CTSCHEDID"),
        col("CLTPRODSEL").alias("CLTPRODSEL"),
        col("CLTATRTAX").alias("CLTATRTAX"),
        col("CLTPROCFEE").alias("CLTPROCFEE"),
        col("CLTPRVNSEL").alias("CLTPRVNSEL"),
        when(col("CLIENTDEF1").isNull() | (length(pyspark_trim(col("CLIENTDEF1"))) == 0), lit("N")).otherwise(pyspark_trim(col("CLIENTDEF1"))).alias("CLIENTDEF1"),
        pyspark_trim(col("CLIENTDEF2")).alias("CLNTDEF2"),
        pyspark_trim(col("FMSTIER")).alias("FMSTIER"),
        col("INDDEDPTD").alias("INDDEDPTD"),
        col("FAMDEDPTD").alias("FAMDEDPTD"),
        col("INDOOPPTD").alias("INDOOPPTD"),
        col("FAMOOPPTD").alias("FAMOOPPTD"),
        col("CLIENTDEF3").alias("CLIENTDEF3"),
        col("CCTRESERV8").alias("CCTRESERV8"),
        col("CCTRESERV9").alias("CCTRESERV9"),
        col("CCTRESRV10").alias("CCTRESRV10"),
        col("OOPAPPLIED").alias("OOPAPPLIED"),
        when((col("PLANDRUGST").isNull()) | (length(col("PLANDRUGST"))==0), lit("UNK")).otherwise(col("PLANDRUGST")).alias("PLANDRUGST"),
        when((col("PLANTYPE").isNull()) | (length(col("PLANTYPE"))==0), lit("UNK")).otherwise(col("PLANTYPE")).alias("PLANTYPE")
    )
)

# Also build the aggregator input from the same stage (the second output link: "Aggregate")
df_DataCleansing_Aggregate = (
    df_Optum
    .withColumn("svDATESBM", svDATESBM)
    .select(
        (col("RXCLMNBR").alias("INROW_NUM")),  # The job used @INROWNUM but we replicate it as if it's a row seed
        pyspark_trim(col("CLAIMSTS")).alias("CLAIMSTS"),
        col("CLT2DUEAMT").alias("AMT_BILL"),
        col("svDATESBM").alias("DATESBM")
    )
)

# --------------------------------------------------------------------------------
# STAGE: Member_Lkp (CTransformerStage)
# Inputs:
#   - inMbrMatch => from df_DataCleansing_inMbrMatch
#   - MBR_lkup => from df_hf_optum_clm_mbr_land
# DS uses reference link matching on (inMbrMatch.MEMBERID = MBR_lkup.MEMBER_ID) and (inMbrMatch.GROUPID = MBR_lkup.GRP_ID)
# Output: many columns including references to either input link
# --------------------------------------------------------------------------------

df_Member_Lkp_joined = df_DataCleansing_inMbrMatch.alias("inMbrMatch").join(
    df_hf_optum_clm_mbr_land.alias("MBR_lkup"),
    (
        (col("inMbrMatch.MEMBERID") == col("MBR_lkup.MEMBER_ID")) &
        (col("inMbrMatch.GROUPID") == col("MBR_lkup.GRP_ID"))
    ),
    how="left"
)

df_Sequential_File_443 = df_Member_Lkp_joined.select(
    col("inMbrMatch.RXCLMNBR").alias("RXCLMNBR"),
    col("inMbrMatch.CLMSEQNBR").alias("CLMSEQNBR"),
    col("inMbrMatch.CLAIMSTS").alias("CLAIMSTS"),
    col("inMbrMatch.CARID").alias("CARID"),
    col("inMbrMatch.ACCOUNTID").alias("ACCOUNTID"),
    # GROUPID is stored as "ACCOUNTID" in the expression for some reason in DataStage
    # But the job does: "GROUPID" -> Expression: inMbrMatch.ACCOUNTID. We'll replicate exactly:
    col("inMbrMatch.ACCOUNTID").alias("GROUPID"),
    rpad(pyspark_trim(col("inMbrMatch.MEMBERID")).substr(1,20),20," ").alias("MEMBERID"),
    col("inMbrMatch.MBRLSTNME").alias("MBRLSTNME"),
    col("inMbrMatch.MBRFSTNME").alias("MBRFSTNME"),
    col("inMbrMatch.MBRRELCDE").alias("MBRRELCDE"),
    col("inMbrMatch.MBRSEX").alias("MBRSEX"),
    col("inMbrMatch.MBRBIRTH").alias("MBRBIRTH"),
    col("inMbrMatch.SOCSECNBR").alias("SOCSECNBR"),
    col("inMbrMatch.CUSTLOC").alias("CUSTLOC"),
    col("inMbrMatch.SBMPLSRVCE").alias("SBMPLSRVCE"),
    col("inMbrMatch.FACILITYID").alias("FACILITYID"),
    col("inMbrMatch.OTHCOVERAG").alias("OTHCOVERAG"),
    col("inMbrMatch.DATESBM").alias("DATESBM"),
    col("inMbrMatch.TIMESBM").alias("TIMESBM"),
    col("inMbrMatch.ORGPDSBMDT").alias("ORGPDSBMDT"),
    col("inMbrMatch.RXNUMBER").alias("RXNUMBER"),
    col("inMbrMatch.REFILL").alias("REFILL"),
    col("inMbrMatch.DISPSTATUS").alias("DISPSTATUS"),
    col("inMbrMatch.DTEFILLED").alias("DTEFILLED"),
    col("inMbrMatch.COMPOUNDCD").alias("COMPOUNDCD"),
    col("inMbrMatch.PRODUCTID").alias("PRODUCTID"),
    col("inMbrMatch.METRICQTY").alias("METRICQTY"),
    col("inMbrMatch.DECIMALQTY").alias("DECIMALQTY"),
    col("inMbrMatch.DAYSSUPPLY").alias("DAYSSUPPLY"),
    col("inMbrMatch.PSC").alias("PSC"),
    col("inMbrMatch.WRITTENDTE").alias("WRITTENDTE"),
    col("inMbrMatch.NBRFLSAUTH").alias("NBRFLSAUTH"),
    col("inMbrMatch.ORIGINCDE").alias("ORIGINCDE"),
    col("inMbrMatch.PRAUTHNBR").alias("PRAUTHNBR"),
    col("inMbrMatch.LABELNAME").alias("LABELNAME"),
    col("inMbrMatch.GPINUMBER").alias("GPINUMBER"),
    col("inMbrMatch.MULTSRCCDE").alias("MULTSRCCDE"),
    col("inMbrMatch.GENINDOVER").alias("GENINDOVER"),
    col("inMbrMatch.CPQSPCPRG").alias("CPQSPCPRG"),
    col("inMbrMatch.CPQSPCPGIN").alias("CPQSPCPGIN"),
    col("inMbrMatch.SRXNETWRK").alias("SRXNETWRK"),
    col("inMbrMatch.RXNETWORK").alias("RXNETWORK"),
    col("inMbrMatch.RXNETWRKQL").alias("RXNETWRKQL"),
    rpad(col("inMbrMatch.SRVPROVID"),15," ").alias("SRVPROVID"),
    col("inMbrMatch.SRVPROVIDQ").alias("SRVPROVIDQ"),
    col("inMbrMatch.NPIPROV").alias("NPIPROV"),
    col("inMbrMatch.PRVNCPDPID").alias("PRVNCPDPID"),
    rpad(col("inMbrMatch.SRVPROVNME"),55," ").alias("SRVPROVNME"),
    rpad(col("inMbrMatch.DISPROTHER"),3," ").alias("DISPROTHER"),
    rpad(col("inMbrMatch.PRESCRIBER"),15," ").alias("PRESCRIBER"),
    col("inMbrMatch.PRESCRIDQL").alias("PRESCRIDQL"),
    col("inMbrMatch.NPIPRESCR").alias("NPIPRESCR"),
    rpad(col("inMbrMatch.PRESCDEAID"),15," ").alias("PRESCDEAID"),
    rpad(col("inMbrMatch.PRESLSTNME"),25," ").alias("PRESLSTNME"),
    rpad(col("inMbrMatch.PRESFSTNME"),15," ").alias("PRESFSTNME"),
    rpad(col("inMbrMatch.PRESSPCCDE"),6," ").alias("PRESSPCCDE"),
    rpad(col("inMbrMatch.PRSSTATE"),3," ").alias("PRSSTATE"),
    rpad(col("inMbrMatch.FNLPLANCDE"),10," ").alias("FNLPLANCDE"),
    col("inMbrMatch.FNLPLANDTE").alias("FNLPLANDTE"),
    rpad(col("inMbrMatch.PLANQUAL"),10," ").alias("PLANQUAL"),
    rpad(col("inMbrMatch.COBIND"),2," ").alias("COBIND"),
    rpad(col("inMbrMatch.FORMLRFLAG"),1," ").alias("FORMLRFLAG"),
    rpad(col("inMbrMatch.REJCDE1"),3," ").alias("REJCDE1"),
    rpad(col("inMbrMatch.REJCDE2"),3," ").alias("REJCDE2"),
    col("inMbrMatch.AWPUNITCST").alias("AWPUNITCST"),
    col("inMbrMatch.WACUNITCST").alias("WACUNITCST"),
    col("inMbrMatch.CTYPEUCOST").alias("CTYPEUCOST"),
    rpad(col("inMbrMatch.BASISCOST"),2," ").alias("BASISCOST"),
    col("inMbrMatch.PROQTY").alias("PROQTY"),
    col("inMbrMatch.SBMINGRCST").alias("SBMINGRCST"),
    col("inMbrMatch.SBMDISPFEE").alias("SBMDISPFEE"),
    col("inMbrMatch.SBMAMTDUE").alias("SBMAMTDUE"),
    col("inMbrMatch.USUALNCUST").alias("USUALNCUST"),
    col("inMbrMatch.DENIALDTE").alias("DENIALDTE"),
    col("inMbrMatch.PHRDISPFEE").alias("PHRDISPFEE"),
    col("inMbrMatch.CLTINGRCST").alias("CLTINGRCST"),
    col("inMbrMatch.CLTDISPFEE").alias("CLTDISPFEE"),
    col("inMbrMatch.CLTSLSTAX").alias("CLTSLSTAX"),
    col("inMbrMatch.CLTPATPAY").alias("CLTPATPAY"),
    col("inMbrMatch.CLTDUEAMT").alias("CLTDUEAMT"),
    col("inMbrMatch.CLTFCOPAY").alias("CLTFCOPAY"),
    col("inMbrMatch.CLTPCOPAY").alias("CLTPCOPAY"),
    col("inMbrMatch.CLTCOPAY").alias("CLTCOPAY"),
    col("inMbrMatch.CLTINCENTV").alias("CLTINCENTV"),
    col("inMbrMatch.CLTATRDED").alias("CLTATRDED"),
    col("inMbrMatch.CLTTOTHAMT").alias("CLTTOTHAMT"),
    col("inMbrMatch.CLTOTHPAYA").alias("CLTOTHPAYA"),
    rpad(col("inMbrMatch.CLTCOSTTYP"),10," ").alias("CLTCOSTTYP"),
    rpad(col("inMbrMatch.CLTPRCTYPE"),10," ").alias("CLTPRCTYPE"),
    col("inMbrMatch.CLTRATE").alias("CLTRATE"),
    col("inMbrMatch.CLTPSCBRND").alias("CLTPSCBRND"),
    rpad(col("inMbrMatch.REIMBURSMT"),1," ").alias("REIMBURSMT"),
    rpad(col("inMbrMatch.CLMORIGIN"),1," ").alias("CLMORIGIN"),
    col("inMbrMatch.CLTTYPUCST").alias("CLTTYPUCST"),
    col("inMbrMatch.CLT2INGRCST").alias("CLT2INGRCST"),
    col("inMbrMatch.CLT2DISPFEE").alias("CLT2DISPFEE"),
    col("inMbrMatch.CLT2SLSTAX").alias("CLT2SLSTAX"),
    col("inMbrMatch.CLT2DUEAMT").alias("CLT2DUEAMT"),
    col("inMbrMatch.CLT2PSTAX").alias("CLT2PSTAX"),
    col("inMbrMatch.CLT2FSTAX").alias("CLT2FSTAX"),
    rpad(col("inMbrMatch.CLT2COSTSRC"),1," ").alias("CLT2COSTSRC"),
    rpad(col("inMbrMatch.CLT2COSTTYP"),10," ").alias("CLT2COSTTYP"),
    rpad(col("inMbrMatch.CLT2PRCTYPE"),10," ").alias("CLT2PRCTYPE"),
    col("inMbrMatch.CLT2RATE").alias("CLT2RATE"),
    when(col("inMbrMatch.CLAIMSTS")=="X",
         concat(col("inMbrMatch.RXCLMNBR"), col("inMbrMatch.CLMSEQNBR"), lit("R"))
    ).otherwise(concat(col("inMbrMatch.RXCLMNBR"), col("inMbrMatch.CLMSEQNBR"))).alias("CLM_ID"),
    when(col("MBR_lkup.MBR_UNIQ_KEY").isNull(), lit(0)).otherwise(col("MBR_lkup.MBR_UNIQ_KEY")).alias("ORIG_MBR"),
    when(col("inMbrMatch.ACCOUNTID").isNull(), lit("UNK")).otherwise(col("MBR_lkup.GRP_ID")).alias("GRP_ID"),
    rpad(col("inMbrMatch.PPRSFSTNME"),15," ").alias("PPRSFSTNME"),
    rpad(col("inMbrMatch.PPRSLSTNME"),25," ").alias("PPRSLSTNME"),
    rpad(col("inMbrMatch.PPRSSPCCDE"),6," ").alias("PPRSSPCCDE"),
    rpad(col("inMbrMatch.PPRSTATE"),3," ").alias("PPRSTATE"),
    col("inMbrMatch.CLT2OTHAMT").alias("CLT2OTHAMT"),
    col("inMbrMatch.CALINCENTV2").alias("CALINCENTV2"),
    rpad(col("inMbrMatch.RXNETPRCQL"),1," ").alias("RXNETPRCQL"),
    rpad(col("inMbrMatch.SBMPATRESD"),2," ").alias("SBMPATRESD"),
    rpad(col("inMbrMatch.PRODTYPCDE"),2," ").alias("PRODTYPCDE"),
    rpad(col("inMbrMatch.CONTHERAPY"),1," ").alias("CONTHERAPY"),
    rpad(col("inMbrMatch.CTSCHEDID"),20," ").alias("CTSCHEDID"),
    col("inMbrMatch.CLTPRODSEL").alias("CLTPRODSEL"),
    col("inMbrMatch.CLTATRTAX").alias("CLTATRTAX"),
    col("inMbrMatch.CLTPROCFEE").alias("CLTPROCFEE"),
    col("inMbrMatch.CLTPRVNSEL").alias("CLTPRVNSEL"),
    rpad(col("inMbrMatch.CLIENTDEF1"),1," ").alias("CLIENTDEF1"),
    rpad(col("inMbrMatch.CLNTDEF2"),10," ").alias("CLNTDEF2"),
    col("inMbrMatch.FMSTIER").alias("FMSTIER"),
    col("inMbrMatch.INDDEDPTD").alias("INDDEDPTD"),
    col("inMbrMatch.FAMDEDPTD").alias("FAMDEDPTD"),
    col("inMbrMatch.INDOOPPTD").alias("INDOOPPTD"),
    col("inMbrMatch.FAMOOPPTD").alias("FAMOOPPTD"),
    rpad(col("inMbrMatch.CLIENTDEF3"),10," ").alias("CLIENTDEF3"),
    rpad(col("inMbrMatch.CCTRESERV8"),10," ").alias("CCTRESERV8"),
    rpad(col("inMbrMatch.CCTRESERV9"),20," ").alias("CCTRESERV9"),
    rpad(col("inMbrMatch.CCTRESRV10"),20," ").alias("CCTRESRV10"),
    col("inMbrMatch.OOPAPPLIED").alias("OOPAPPLIED"),
    rpad(col("inMbrMatch.PLANDRUGST"),1," ").alias("PLANDRUGST"),
    rpad(col("inMbrMatch.PLANTYPE"),8," ").alias("PLANTYPE")
)

# Write the "Sequential_File_443" to #$FilePath#/verified => f"{adls_path}/verified/Optum_BuyPriceValidation1{RunID}.dat"
df_Sequential_File_443_out = df_Sequential_File_443.select(df_Sequential_File_443.columns)  # maintain order

# --------------------------------------------------------------------------------
# STAGE: CntByAdjDtRecType (AGGREGATOR)
# Input => df_DataCleansing_Aggregate
# Output => group by (CLAIMSTS, DATESBM), RCRD_CNT => count(*), TOT_AMT => sum(AMT_BILL)
# --------------------------------------------------------------------------------
df_CntByAdjDtRecType = (
    df_DataCleansing_Aggregate
    .groupBy("CLAIMSTS","DATESBM")
    .agg(
        _count("*").alias("RCRD_CNT"),
        _sum(col("AMT_BILL").cast(DecimalType(18,2))).alias("TOT_AMT")
    )
    .select(
        col("CLAIMSTS"),
        col("RCRD_CNT"),
        col("TOT_AMT"),
        col("DATESBM")
    )
)

# --------------------------------------------------------------------------------
# STAGE: PrepAuditFile (CTransformerStage)
# Input => df_CntByAdjDtRecType
# Output => columns: 
#   Run_Date (char(10)) => "RunDate" param
#   Filler1 (char(2)) => "  "
#   Frequency (char(9)) => "DAILY"
#   Filler2 (char(3)) => "*  "
#   Program (char(9)) => "ORX-ACA-" : DSLink192.CLAIMSTS
#   File_date (char(11)) => DSLink192.DATESBM
#   Row_Count (char(8)) => DSLink192.RCRD_CNT
#   Pd_amount (char(12)) => TOT_AMT
# --------------------------------------------------------------------------------

df_PrepAuditFile = (
    df_CntByAdjDtRecType
    .withColumn("Run_Date", rpad(lit(RunDate),10," "))
    .withColumn("Filler1", rpad(lit(""),2," "))
    .withColumn("Frequency", rpad(lit("DAILY"),9," "))
    .withColumn("Filler2", rpad(lit("*"),3," "))
    .withColumn("Program", rpad(concat(lit("ORX-ACA-"), col("CLAIMSTS")),9," "))
    .withColumn("File_date", rpad(col("DATESBM"),11," "))
    .withColumn("Row_Count", rpad(col("RCRD_CNT").cast(StringType()),8," "))
    .withColumn("Pd_amount", rpad(col("TOT_AMT").cast(StringType()),12," "))
    .select(
        "Run_Date",
        "Filler1",
        "Frequency",
        "Filler2",
        "Program",
        "File_date",
        "Row_Count",
        "Pd_amount"
    )
)

# --------------------------------------------------------------------------------
# STAGE: Drug_Header_Record (CSeqFileStage)
# The file is landing/Drug_Header_Record.dat => f"{adls_path_raw}/landing/Drug_Header_Record.dat"
# ContainsHeader=false, WriteMode=append
# --------------------------------------------------------------------------------

df_Drug_Header_Record_out = df_PrepAuditFile.select(df_PrepAuditFile.columns)

write_files(
    df_Drug_Header_Record_out,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# -------------------------------------------------------------------------
# Retrieve parameter values
# -------------------------------------------------------------------------
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunDate = get_widget_value('RunDate','')
InFile = get_widget_value('InFile','')
MbrTermDate = get_widget_value('MbrTermDate','')
RunCycle = get_widget_value('RunCycle','')

# -------------------------------------------------------------------------
# Read from source file: Sequential_File_442
#   (This CSeqFileStage is reading a file with all listed columns)
# -------------------------------------------------------------------------
df_Sequential_File_442 = df_Sequential_File_443_out

# -------------------------------------------------------------------------
# Transformations in MapToAllDownstreamCommonFiles (CTransformerStage)
#   Define stage variables in order
# -------------------------------------------------------------------------

df_enriched = df_Sequential_File_442

# svServicingProviderIDisNABP
df_enriched = df_enriched.withColumn(
    "svServicingProviderIDisNABP",
    F.when(trim(F.col("SRVPROVIDQ")) == '07', F.lit('Y')).otherwise(F.lit('N'))
)

# svServicingProviderIDisNPI
df_enriched = df_enriched.withColumn(
    "svServicingProviderIDisNPI",
    F.when(
        (trim(F.col("SRVPROVIDQ")) == '01') & (ProviderNPIValidator(F.col("SRVPROVID")) == 'Y'),
        F.lit('Y')
    ).otherwise(F.lit('N'))
)

# svServicingProviderNCPDP
df_enriched = df_enriched.withColumn(
    "svServicingProviderNCPDP",
    F.when(F.col("PRVNCPDPID").cast("string").isNotNull() & (trim(F.col("PRVNCPDPID")) > F.lit('0')), 'Y').otherwise('N')
)

# svNPIProvhasNPI
df_enriched = df_enriched.withColumn(
    "svNPIProvhasNPI",
    F.when(ProviderNPIValidator(F.col("NPIPROV")) == 'Y', 'Y').otherwise('N')
)

# svPrescribingIDisDEA
df_enriched = df_enriched.withColumn(
    "svPrescribingIDisDEA",
    F.when(
        (trim(F.col("PRESCRIDQL")) == '12') & (ProviderDEAValidator(F.col("PRESCRIBER")) == 'Y'),
        'Y'
    ).otherwise('N')
)

# svPrescribingIDisNPI
df_enriched = df_enriched.withColumn(
    "svPrescribingIDisNPI",
    F.when(
        (trim(F.col("PRESCRIDQL")) == '01') & (ProviderNPIValidator(F.col("PRESCRIBER")) == 'Y'),
        'Y'
    ).otherwise('N')
)

# svPrescribingDEAIDisDEA
df_enriched = df_enriched.withColumn(
    "svPrescribingDEAIDisDEA",
    F.when(ProviderDEAValidator(F.col("PRESCDEAID")) == 'Y', 'Y').otherwise('N')
)

# svNPIPresciberHasNPI
df_enriched = df_enriched.withColumn(
    "svNPIPresciberHasNPI",
    F.when(ProviderNPIValidator(F.col("NPIPRESCR")) == 'Y', 'Y').otherwise('N')
)

# svDEAsMatchTest
df_enriched = df_enriched.withColumn(
    "svDEAsMatchTest",
    F.when(
        (F.col("svPrescribingIDisDEA") == 'Y') &
        (F.col("svPrescribingDEAIDisDEA") == 'Y') &
        (trim(F.col("PRESCRIBER")) == trim(F.col("PRESCDEAID"))),
        'Y'
    ).otherwise('N')
)

# svServiceIDToUse
df_enriched = df_enriched.withColumn(
    "svServiceIDToUse",
    F.when(
        F.col("svServicingProviderIDisNABP") == 'Y',
        F.col("SRVPROVID")
    ).when(
        F.col("svServicingProviderNCPDP") == 'Y',
        F.col("PRVNCPDPID")
    ).otherwise(F.lit("NA"))
)

# svServiceNPIToUse
df_enriched = df_enriched.withColumn(
    "svServiceNPIToUse",
    F.when(
        F.col("svServicingProviderIDisNPI") == 'Y',
        trim(F.col("SRVPROVID"))
    ).when(
        F.col("svNPIProvhasNPI") == 'Y',
        trim(F.col("NPIPROV"))
    ).otherwise(F.lit("NA"))
)

# svPrescriberDEAtoUse
df_enriched = df_enriched.withColumn(
    "svPrescriberDEAtoUse",
    F.when(
        F.col("svPrescribingIDisDEA") == 'Y',
        trim(F.col("PRESCRIBER"))
    ).when(
        F.col("svPrescribingDEAIDisDEA") == 'Y',
        trim(F.col("PRESCDEAID"))
    ).otherwise(F.lit("NA"))
)

# svPrescriberNPItoUse
df_enriched = df_enriched.withColumn(
    "svPrescriberNPItoUse",
    F.when(
        F.col("svPrescribingIDisNPI") == 'Y',
        trim(F.col("PRESCRIBER"))
    ).when(
        F.col("svNPIPresciberHasNPI") == 'Y',
        trim(F.col("NPIPRESCR"))
    ).otherwise(F.lit("NA"))
)

# svSellClientIngredientCosts
df_enriched = df_enriched.withColumn(
    "svSellClientIngredientCosts",
    F.when(F.col("CLTINGRCST").isNull(), F.lit(0.00)).otherwise(F.col("CLTINGRCST"))
)

# svSellClientDispenseFee
df_enriched = df_enriched.withColumn(
    "svSellClientDispenseFee",
    F.when(F.col("CLTDISPFEE").isNull(), F.lit(0.00)).otherwise(F.col("CLTDISPFEE"))
)

# svSellClientSalesTax
df_enriched = df_enriched.withColumn(
    "svSellClientSalesTax",
    F.when(F.col("CLTSLSTAX").isNull(), F.lit(0.00)).otherwise(F.col("CLTSLSTAX"))
)

# svSellClientIncentive
df_enriched = df_enriched.withColumn(
    "svSellClientIncentive",
    F.when(F.col("CLTINCENTV").isNull(), F.lit(0.00)).otherwise(F.col("CLTINCENTV"))
)

# svSellClientOtherAmt
df_enriched = df_enriched.withColumn(
    "svSellClientOtherAmt",
    F.when(F.col("CLTTOTHAMT").isNull(), F.lit(0.00)).otherwise(F.col("CLTTOTHAMT"))
)

# svSellClientOtherPayAmt
df_enriched = df_enriched.withColumn(
    "svSellClientOtherPayAmt",
    F.when(F.col("CLTOTHPAYA").isNull(), F.lit(0.00)).otherwise(F.col("CLTOTHPAYA"))
)

# svSellClientPatientPaid
df_enriched = df_enriched.withColumn(
    "svSellClientPatientPaid",
    F.when(F.col("CLTPATPAY").isNull(), F.lit(0.00)).otherwise(F.col("CLTPATPAY"))
)

# svSellClientDueAmount
df_enriched = df_enriched.withColumn(
    "svSellClientDueAmount",
    F.when(F.col("CLTDUEAMT").isNull(), F.lit(0.00)).otherwise(F.col("CLTDUEAMT"))
)

# svSellDeriveActualPaidPerFields
df_enriched = df_enriched.withColumn(
    "svSellDeriveActualPaidPerFields",
    (
        F.col("svSellClientIngredientCosts") +
        F.col("svSellClientDispenseFee")     +
        F.col("svSellClientSalesTax")        +
        F.col("svSellClientOtherAmt")        +
        F.col("svSellClientIncentive")
    ) - (
        F.col("svSellClientOtherPayAmt") +
        F.col("svSellClientPatientPaid")
    )
)

# svDoesActualPaidEqClientDueAmt
df_enriched = df_enriched.withColumn(
    "svDoesActualPaidEqClientDueAmt",
    F.when(
        F.col("svSellDeriveActualPaidPerFields") == F.col("svSellClientDueAmount"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

# svBuyClientIngredientCosts
df_enriched = df_enriched.withColumn(
    "svBuyClientIngredientCosts",
    F.when(F.col("CLT2INGRCST").isNull(), F.lit(0.00)).otherwise(F.col("CLT2INGRCST"))
)

# svBuyClientDispenseFee
df_enriched = df_enriched.withColumn(
    "svBuyClientDispenseFee",
    F.when(F.col("CLT2DISPFEE").isNull(), F.lit(0.00)).otherwise(F.col("CLT2DISPFEE"))
)

# svBuyClientPSTax
df_enriched = df_enriched.withColumn(
    "svBuyClientPSTax",
    F.when(F.col("CLT2PSTAX").isNull(), F.lit(0.00)).otherwise(F.col("CLT2PSTAX"))
)

# svBuyClientFSTax
df_enriched = df_enriched.withColumn(
    "svBuyClientFSTax",
    F.when(F.col("CLT2FSTAX").isNull(), F.lit(0.00)).otherwise(F.col("CLT2FSTAX"))
)

# svBuyClientIncentive
df_enriched = df_enriched.withColumn(
    "svBuyClientIncentive",
    F.when(F.col("CALINCENTV2").isNull(), F.lit(0.00)).otherwise(F.col("CALINCENTV2"))
)

# svBuyClientOtherAmt
df_enriched = df_enriched.withColumn(
    "svBuyClientOtherAmt",
    F.when(F.col("CLT2OTHAMT").isNull(), F.lit(0.00)).otherwise(F.col("CLT2OTHAMT"))
)

# svBuyClientDueAmount
df_enriched = df_enriched.withColumn(
    "svBuyClientDueAmount",
    F.when(F.col("CLT2DUEAMT").isNull(), F.lit(0.00)).otherwise(F.col("CLT2DUEAMT"))
)

# svBuyDeriveActualPaidPerFields
df_enriched = df_enriched.withColumn(
    "svBuyDeriveActualPaidPerFields",
    (
        F.col("svBuyClientIngredientCosts") +
        F.col("svBuyClientDispenseFee")     +
        F.col("svBuyClientPSTax")           +
        F.col("svBuyClientFSTax")           +
        F.col("svBuyClientIncentive")       +
        F.col("svBuyClientOtherAmt")
    ) - (
        F.col("svSellClientOtherPayAmt") +
        F.col("svSellClientPatientPaid")
    )
)

# svDoesBuyActualPaidEqClientDueAmt
df_enriched = df_enriched.withColumn(
    "svDoesBuyActualPaidEqClientDueAmt",
    F.when(
        F.col("svBuyDeriveActualPaidPerFields") == F.col("svBuyClientDueAmount"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

# -------------------------------------------------------------------------
# Create branching outputs (constraints) from MapToAllDownstreamCommonFiles
# -------------------------------------------------------------------------

# 1) Landing_File_Paid_Reversal => PDX_CLM_STD_INPT_Land
df_Landing_File_Paid_Reversal = df_enriched.filter(
    (F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')
)

# 2) Landing_File_DeniedClaims => DeniedClm_Land
df_Landing_File_DeniedClaims = df_enriched.filter(
    (F.col("CLAIMSTS") == 'R')
)

# 3) Load_ENR => W_DRUG_ENR
df_Load_ENR = df_enriched.filter(
    (F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')
)

# 4) Load_PCP => W_DRUG_CLM_PCP
df_Load_PCP = df_enriched.filter(
    (F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')
)

# 5) Load_DrugClmPrice => DrugClmPrice_Land
df_Load_DrugClmPrice = df_enriched.filter(
    (F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')
)

# 6) BuyPriceNotEqClntDueAmt => BuyPriceValidation
df_BuyPriceNotEqClntDueAmt = df_enriched.filter(
    ((F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')) &
    (F.col("svDoesBuyActualPaidEqClientDueAmt") == 'N')
)

# 7) SellPriceNotEqClntDueAmt => SellPriceValidation
df_SellPriceNotEqClntDueAmt = df_enriched.filter(
    ((F.col("CLAIMSTS") == 'P') | (F.col("CLAIMSTS") == 'X')) &
    (F.col("svDoesActualPaidEqClientDueAmt") == 'N')
)

# 8) Load_DRUG_CLM_ACCUM => DRUG_CLM_ACCUM_IMPCT_prep
#    This link has no constraint mentioned besides what's in the JSON
#    The JSON does not show a special constraint, so it feeds from same transformer
#    Actually the link has columns but no constraint. We'll take all rows (the JSON doesn't specify filter).
#    But from job snippet: "OutputPins" => "V0S440|V0S440P1" => no explicit constraint, so pass all or partial?
#    The job's JSON just has "Columns" => let's see:
#    The snippet doesn't have a direct constraint line. So it's unconditional? 
#    We'll produce df_Load_DRUG_CLM_ACCUM from df_enriched directly.
df_Load_DRUG_CLM_ACCUM = df_enriched

# -------------------------------------------------------------------------
# Now select the columns in the correct order for each output
#   and apply rpad on char/varchar columns as the instructions demand
# -------------------------------------------------------------------------

# 1) PDX_CLM_STD_INPT_Land
df_PDX_CLM_STD_INPT_Land = df_Landing_File_Paid_Reversal.select(
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    rpad(F.col("DATESBM"), 10, " ").alias("FILE_RCVD_DT"),
    F.lit(None).alias("RCRD_ID"),
    F.lit(None).alias("PRCSR_NO"),
    F.lit(None).alias("BTCH_NO"),
    F.col("svServiceIDToUse").alias("PDX_NO"),
    F.col("RXNUMBER").alias("RX_NO"),
    rpad(F.col("DTEFILLED"), 10, " ").alias("FILL_DT"),
    rpad(F.col("PRODUCTID"), 20, " ").alias("NDC"),
    F.col("LABELNAME").alias("DRUG_DESC"),
    F.col("REFILL").alias("NEW_OR_RFL_CD"),
    F.col("DECIMALQTY").alias("METRIC_QTY"),
    F.col("DAYSSUPPLY").alias("DAYS_SUPL"),
    rpad(F.lit(None), 2, " ").alias("BSS_OF_CST_DTRM"),
    F.col("CLTINGRCST").alias("INGR_CST_AMT"),
    F.col("CLTDISPFEE").alias("DISPNS_FEE_AMT"),
    F.col("CLTFCOPAY").alias("COPAY_AMT"),
    F.col("CLTSLSTAX").alias("SLS_TAX_AMT"),
    F.col("CLTDUEAMT").alias("BILL_AMT"),
    F.col("MBRFSTNME").alias("PATN_FIRST_NM"),
    F.col("MBRLSTNME").alias("PATN_LAST_NM"),
    rpad(F.col("MBRBIRTH"), 10, " ").alias("BRTH_DT"),
    F.when(
       F.upper(trim(F.col("MBRSEX"))) == 'M',
       F.lit(1)
    ).when(
       F.upper(trim(F.col("MBRSEX"))) == 'F',
       F.lit(2)
    ).otherwise(F.lit(0)).alias("SEX_CD"),
    F.when(
       (F.col("MEMBERID").isNull()) | (F.length(trim(F.col("MEMBERID"))) == 0),
       F.lit("UNK")
    ).when(
       F.length(trim(F.col("MEMBERID"))) == 11,
       trim(F.col("MEMBERID"))[0:9]
    ).otherwise(F.lit("UNK")).alias("CARDHLDR_ID_NO"),
    F.col("MBRRELCDE").alias("RELSHP_CD"),
    F.col("ACCOUNTID").alias("GRP_NO"),
    F.lit(None).alias("HOME_PLN"),
    F.lit(None).alias("HOST_PLN"),
    F.col("svPrescriberDEAtoUse").alias("PRSCRBR_ID"),
    F.lit(None).alias("DIAG_CD"),
    F.lit(None).alias("CARDHLDR_FIRST_NM"),
    F.lit(None).alias("CARDHLDR_LAST_NM"),
    rpad(F.col("PRAUTHNBR"), 12, " ").alias("PRAUTH_NO"),
    F.lit(None).alias("PA_MC_SC_NO"),
    F.col("CUSTLOC").alias("CUST_LOC"),
    F.lit(None).alias("RESUB_CYC_CT"),
    rpad(F.lit(None), 10, " ").alias("RX_DT"),
    rpad(
       F.when(
         (F.col("PSC").isNull()) | (F.length(trim(F.col("PSC"))) == 0),
         F.lit("UNK")
       ).otherwise(F.col("PSC")),
       1, " "
    ).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.when(
       (F.col("MEMBERID").isNull()) | (F.length(F.col("MEMBERID")) == 0),
       F.lit("UNK")
    ).when(
       F.length(trim(F.col("MEMBERID"))) == 11,
       trim(F.col("MEMBERID"))[9:11]
    ).otherwise(F.lit("UNK")).alias("PRSN_CD"),
    F.col("OTHCOVERAG").alias("OTHR_COV_CD"),
    F.lit(None).alias("ELIG_CLRFCTN_CD"),
    F.col("COMPOUNDCD").alias("CMPND_CD"),
    F.col("NBRFLSAUTH").alias("NO_OF_RFLS_AUTH"),
    F.lit(None).alias("LVL_OF_SVC"),
    F.col("ORIGINCDE").alias("RX_ORIG_CD"),
    F.lit(None).alias("RX_DENIAL_CLRFCTN"),
    F.lit(None).alias("PRI_PRSCRBR"),
    F.lit(None).alias("CLNC_ID_NO"),
    F.when(
       ((F.col("GENINDOVER").isNull()) | (F.length(trim(F.col("GENINDOVER"))) == 0)) &
       ((trim(F.col("MULTSRCCDE")) == 'M') | (trim(F.col("MULTSRCCDE")) == 'N')),
       F.lit('1')
    ).when(
       ((F.col("GENINDOVER").isNull()) | (F.length(trim(F.col("GENINDOVER"))) == 0)) &
       (trim(F.col("MULTSRCCDE")) == 'Y'),
       F.lit('3')
    ).when(
       ((F.col("GENINDOVER").isNull()) | (F.length(trim(F.col("GENINDOVER"))) == 0)) &
       (trim(F.col("MULTSRCCDE")) == 'O'),
       F.lit('5')
    ).when(
       (trim(F.col("GENINDOVER")) == 'M') | (trim(F.col("GENINDOVER")) == 'N'),
       F.lit('1')
    ).when(
       (trim(F.col("GENINDOVER")) == 'Y'),
       F.lit('3')
    ).when(
       (trim(F.col("GENINDOVER")) == 'O'),
       F.lit('5')
    ).otherwise(F.lit('0')).alias("DRUG_TYP"),
    F.when(
       (F.length(F.col("PRESLSTNME")) == 0) & (F.length(F.col("PRESFSTNME")) == 0),
       F.lit("UNK")
    ).when(
       (F.length(F.col("PRESFSTNME")) != 0) & (F.length(F.col("PRESLSTNME")) == 0),
       F.col("PRESFSTNME")
    ).when(
       (F.length(F.col("PRESFSTNME")) == 0) & (F.length(F.col("PRESLSTNME")) != 0),
       F.col("PRESLSTNME")
    ).otherwise(
       F.concat(F.col("PRESFSTNME"), F.lit(" "), F.col("PRESLSTNME"))
    ).alias("PRSCRBR_LAST_NM"),
    F.lit(0.00).alias("POSTAGE_AMT"),
    F.lit(None).alias("UNIT_DOSE_IN"),
    F.col("CLTOTHPAYA").alias("OTHR_PAYOR_AMT"),
    F.lit(None).alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.when(
       F.col("COMPOUNDCD") == '2',
       F.round((F.col("USUALNCUST") + F.col("CLTDISPFEE") + F.col("CLTSLSTAX")), 2)
    ).otherwise(
       F.round(((F.col("AWPUNITCST") * F.col("DECIMALQTY")) + F.col("CLTDISPFEE") + F.col("CLTSLSTAX")), 2)
    ).alias("FULL_AVG_WHLSL_PRICE"),
    rpad(F.lit(None), 1, " ").alias("EXPNSN_AREA"),
    F.col("CARID").alias("MSTR_CAR"),
    F.lit(None).alias("SUBCAR"),
    rpad(
       F.when(F.col("CLAIMSTS") == 'X', F.lit('R')).otherwise(F.lit('P')),
       1, " "
    ).alias("CLM_TYP"),
    F.lit(None).alias("SUBGRP"),
    rpad(F.lit(None), 1, " ").alias("PLN_DSGNR"),
    rpad(F.col("DATESBM"), 10, " ").alias("ADJDCT_DT"),
    F.lit(0.0).alias("ADMIN_FEE_AMT"),
    F.lit(0.00).alias("CAP_AMT"),
    F.col("SBMINGRCST").alias("INGR_CST_SUB_AMT"),
    F.lit(0.00).alias("MBR_NON_COPAY_AMT"),
    rpad(F.lit(None), 2, " ").alias("MBR_PAY_CD"),
    F.col("CLTINCENTV").alias("INCNTV_FEE_AMT"),
    F.lit(0.00).alias("CLM_ADJ_AMT"),
    rpad(F.lit(None), 2, " ").alias("CLM_ADJ_CD"),
    rpad(F.col("FORMLRFLAG"), 1, " ").alias("FRMLRY_FLAG"),
    F.lit(None).alias("GNRC_CLS_NO"),
    F.lit(None).alias("THRPTC_CLS_AHFS"),
    rpad(F.col("DISPROTHER"), 3, " ").alias("PDX_TYP"),
    rpad(
       F.when(
           (F.col("BASISCOST").isNull()) | (F.length(trim(F.col("BASISCOST"))) == 0),
           F.lit("NA")
       ).otherwise(F.col("BASISCOST")),
       2, " "
    ).alias("BILL_BSS_CD"),
    F.col("USUALNCUST").alias("USL_AND_CUST_CHRG_AMT"),
    rpad(F.lit("1753-01-01"), 10, " ").alias("PD_DT"),
    F.lit(None).alias("BNF_CD"),
    F.lit(None).alias("DRUG_STRG"),
    F.col("ORIG_MBR").alias("ORIG_MBR"),
    rpad(F.lit(None), 10, " ").alias("INJRY_DT"),
    F.lit(0.00).alias("FEE_AMT"),
    F.col("RXCLMNBR").alias("REF_NO"),
    F.lit(None).alias("CLNT_CUST_ID"),
    F.lit(None).alias("PLN_TYP"),
    F.lit(None).alias("ADJDCT_REF_NO"),
    F.when(
       (F.col("CLTPSCBRND").isNull()) | (F.length(trim(F.col("CLTPSCBRND"))) == 0),
       F.lit('0.0')
    ).otherwise(F.col("CLTPSCBRND")).alias("ANCLRY_AMT"),
    F.concat(
      F.format_string("%-10s", F.col("FACILITYID")),
      F.format_string("%-2s", F.col("PRESCRIDQL")),
      F.format_string("%-15s", F.col("PRESCDEAID")),
      F.format_string("%-11s", F.col("SBMDISPFEE")),
      F.format_string("%-6s", F.col("SRXNETWRK")),
      F.format_string("%-6s", F.col("RXNETWORK")),
      F.format_string("%-10s", F.col("PLANQUAL")),
      F.format_string("%-2s", F.col("COBIND")),
      F.format_string("%-1s", F.col("RXNETPRCQL")),
      F.format_string("%-1s", F.col("CLIENTDEF1")),
      F.format_string("%-2s", F.col("SBMPLSRVCE"))
    ).alias("CLNT_GNRL_PRPS_AREA"),
    rpad(
       F.when(
         (F.col("DISPSTATUS").isNull()) | (F.length(trim(F.col("DISPSTATUS"))) == 0),
         F.lit("BLANK")
       ).otherwise(F.col("DISPSTATUS")),
       1, " "
    ).alias("PRTL_FILL_STTUS_CD"),
    rpad(F.lit("1753-01-01"), 10, " ").alias("BILL_DT"),
    F.lit(None).alias("FSA_VNDR_CD"),
    rpad(F.lit(None), 1, " ").alias("PICA_DRUG_CD"),
    F.when(
       (F.col("CLTPATPAY").isNull()) | (F.length(trim(F.col("CLTPATPAY"))) == 0),
       F.lit('0.0')
    ).otherwise(F.col("CLTPATPAY")).alias("CLM_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    rpad(F.lit(None), 1, " ").alias("FED_DRUG_CLS_CD"),
    F.col("CLTATRDED").alias("DEDCT_AMT"),
    rpad(F.lit(None), 1, " ").alias("BNF_COPAY_100"),
    rpad(F.lit(None), 1, " ").alias("CLM_PRCS_TYP"),
    F.lit(None).alias("INDEM_HIER_TIER_NO"),
    rpad(F.lit(None), 1, " ").alias("MCARE_D_COV_DRUG"),
    rpad(F.lit(None), 1, " ").alias("RETRO_LICS_CD"),
    F.lit(0.00).alias("RETRO_LICS_AMT"),
    F.lit(0.00).alias("LICS_SBSDY_AMT"),
    rpad(F.lit(None), 1, " ").alias("MCARE_B_DRUG"),
    rpad(F.lit(None), 1, " ").alias("MCARE_B_CLM"),
    rpad(F.col("PRESCRIDQL"), 2, " ").alias("PRSCRBR_QLFR"),
    F.col("svPrescriberNPItoUse").alias("PRSCRBR_NTNL_PROV_ID"),
    rpad(F.col("SRVPROVIDQ"), 2, " ").alias("PDX_QLFR"),
    F.col("svServiceNPIToUse").alias("PDX_NTNL_PROV_ID"),
    F.lit(0.00).alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    F.lit(None).alias("THER_CLS"),
    F.lit(None).alias("HIC_NO"),
    rpad(F.lit(None), 1, " ").alias("HLTH_RMBRMT_ARGMT_FLAG"),
    F.lit(None).alias("DOSE_CD"),
    rpad(F.lit(None), 1, " ").alias("LOW_INCM"),
    rpad(F.lit(None), 2, " ").alias("RTE_OF_ADMIN"),
    F.lit(None).alias("DEA_SCHD"),
    F.lit(None).alias("COPAY_BNF_OPT"),
    F.when(
       (F.col("GPINUMBER").isNull()) | (F.length(trim(F.col("GPINUMBER"))) == 0),
       F.lit("NA")
    ).otherwise(F.col("GPINUMBER")).alias("GNRC_PROD_IN"),
    F.when(
       (F.col("PRESSPCCDE").isNull()) | (F.length(trim(F.col("PRESSPCCDE"))) == 0),
       F.lit("NA")
    ).otherwise(F.col("PRESSPCCDE")).alias("PRSCRBR_SPEC"),
    F.lit(None).alias("VAL_CD"),
    F.lit(None).alias("PRI_CARE_PDX"),
    rpad(F.lit(None), 1, " ").alias("OFC_OF_INSPECTOR_GNRL"),
    F.lit(None).alias("PATN_SSN"),
    F.lit(None).alias("CARDHLDR_SSN"),
    F.lit(None).alias("CARDHLDR_BRTH_DT"),
    F.lit(None).alias("CARDHLDR_ADDR"),
    F.lit(None).alias("CARDHLDR_CITY"),
    F.lit(None).alias("CHADHLDR_ST"),
    F.lit(None).alias("CARDHLDR_ZIP_CD"),
    F.lit(0.00).alias("PSL_FMLY_MET_AMT"),
    F.lit(0.00).alias("PSL_MBR_MET_AMT"),
    F.lit(0.00).alias("PSL_FMLY_AMT"),
    F.lit(0.00).alias("DEDCT_FMLY_MET_AMT"),
    F.lit(0.00).alias("DEDCT_FMLY_AMT"),
    F.lit(0.00).alias("MOPS_FMLY_AMT"),
    F.lit(0.00).alias("MOPS_FMLY_MET_AMT"),
    F.lit(0.00).alias("MOPS_MBR_MET_AMT"),
    F.lit(0.00).alias("DEDCT_MBR_MET_AMT"),
    F.lit(0.00).alias("PSL_APLD_AMT"),
    F.lit(0.00).alias("MOPS_APLD_AMT"),
    rpad(
       F.when(
         ((F.col("REIMBURSMT") == 'M') & (F.col("SBMPATRESD").isin('92', '93', '98'))),
         F.lit('N')
       ).otherwise(F.lit('Y')),
       1, " "
    ).alias("PAR_PDX_IN"),
    F.col("CLTPCOPAY").alias("COPAY_PCT_AMT"),
    F.col("CLTFCOPAY").alias("COPAY_FLAT_AMT"),
    rpad(F.col("CLMORIGIN"), 1, " ").alias("CLM_TRNSMSN_METH"),
    F.lit(None).alias("RX_NO_2012"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLAIMSTS").alias("CLM_STTUS_CD"),
    F.lit("NA").alias("ADJ_FROM_CLM_ID"),
    F.lit("NA").alias("ADJ_TO_CLM_ID"),
    F.col("PRODTYPCDE").alias("SUBMT_PROD_ID_QLFR"),
    F.col("CONTHERAPY").alias("CNTNGNT_THER_FLAG"),
    F.when(
       (F.col("CTSCHEDID").isNull()) | (F.length(trim(F.col("CTSCHEDID"))) == 0),
       F.lit("NA")
    ).otherwise(F.col("CTSCHEDID")).alias("CNTNGNT_THER_SCHD"),
    F.when(
       (F.col("CLTPRODSEL").isNull()) | (F.length(trim(F.col("CLTPRODSEL"))) == 0),
       F.lit(0.00)
    ).otherwise(F.col("CLTPRODSEL")).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.when(
       (F.col("CLTATRTAX").isNull()) | (F.length(trim(F.col("CLTATRTAX"))) == 0),
       F.lit(0.00)
    ).otherwise(F.col("CLTATRTAX")).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.when(
       (F.col("CLTPROCFEE").isNull()) | (F.length(trim(F.col("CLTPROCFEE"))) == 0),
       F.lit(0.00)
    ).otherwise(F.col("CLTPROCFEE")).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.when(
       (F.col("CLTPRVNSEL").isNull()) | (F.length(trim(F.col("CLTPRVNSEL"))) == 0),
       F.lit(0.00)
    ).otherwise(F.col("CLTPRVNSEL")).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.lit("ACA").alias("LOB_IN")
)

# 2) DeniedClm_Land
df_DeniedClm_Land = df_Landing_File_DeniedClaims.select(
    rpad(F.col("MEMBERID"), 20, " ").alias("MEMBERID"),
    rpad(F.col("ACCOUNTID"), 15, " ").alias("ACCOUNTID"),
    rpad(F.col("NPIPROV"), 10, " ").alias("NPIPROV"),
    rpad(F.col("DENIALDTE"), 8, " ").alias("DENIALDTE"),
    rpad(F.col("RXNUMBER"), 12, " ").alias("RXNUMBER"),
    rpad(F.col("DATESBM"), 8, " ").alias("DATESBM"),
    rpad(F.col("TIMESBM"), 8, " ").alias("TIMESBM"),
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    rpad(F.col("MBRSEX"), 1, " ").alias("MBRSEX"),
    rpad(F.col("MBRRELCDE"), 1, " ").alias("MBRRELCDE"),
    rpad(F.col("REJCDE1"), 3, " ").alias("REJCDE1"),
    rpad(F.col("REJCDE2"), 3, " ").alias("REJCDE2"),
    rpad(F.col("RXNETWRKQL"), 1, " ").alias("RXNETWRKQL"),
    rpad(F.col("MBRBIRTH"), 8, " ").alias("MBRBIRTH"),
    F.col("PHRDISPFEE").alias("PHRDISPFEE"),
    F.col("CLTDUEAMT").alias("CLTDUEAMT"),
    F.col("CLT2DUEAMT").alias("CLT2DUEAMT"),
    F.col("CLTPATPAY").alias("CLTPATPAY"),
    F.col("CLTINGRCST").alias("CLTINGRCST"),
    rpad(F.col("MBRFSTNME"), 15, " ").alias("MBRFSTNME"),
    rpad(F.col("MBRLSTNME"), 25, " ").alias("MBRLSTNME"),
    rpad(F.col("SOCSECNBR"), 9, " ").alias("SOCSECNBR"),
    rpad(F.col("SRVPROVNME"), 55, " ").alias("SRVPROVNME"),
    rpad(
       F.when(
         (F.length(trim(F.col("PRESCRIBER"))) != 0) & (trim(F.col("PRESCRIDQL")) == '12'),
         trim(F.col("PRESCRIBER"))
       ).when(
         F.length(trim(F.col("PRESCDEAID"))) != 0,
         trim(F.col("PRESCDEAID"))
       ).otherwise(F.lit('NA')),
       15, " "
    ).alias("PRESCDEAID"),
    rpad(F.col("NPIPRESCR"), 10, " ").alias("NPIPRESCR"),
    rpad(F.col("PRESFSTNME"), 15, " ").alias("PPRSFSTNME"),
    rpad(F.col("PRESLSTNME"), 25, " ").alias("PPRSLSTNME"),
    rpad(F.col("PRSSTATE"), 3, " ").alias("PPRSTATE"),
    rpad(F.col("LABELNAME"), 30, " ").alias("LABELNAME"),
    rpad(F.col("SRVPROVID"), 15, " ").alias("SRVPROVID"),
    rpad(F.col("CARID"), 9, " ").alias("CARID"),
    rpad(F.col("ACCOUNTID"), 15, " ").alias("GROUPID"),
    rpad(F.col("CLMORIGIN"), 1, " ").alias("CLMORIGIN"),
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    rpad(F.col("PRODUCTID"), 20, " ").alias("PRODUCTID"),
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.lit("ACA").alias("LOB_IN")
)

# 3) W_DRUG_ENR
df_W_DRUG_ENR = df_Load_ENR.select(
    F.col("CLM_ID").alias("CLM_ID"),
    rpad(F.col("DTEFILLED"), 10, " ").alias("FILL_DT_SK"),
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY")
)

# 4) W_DRUG_CLM_PCP
df_W_DRUG_CLM_PCP = df_Load_PCP.select(
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
    rpad(F.col("DTEFILLED"), 10, " ").alias("FILL_DT_SK")
)

# 5) DrugClmPrice_Land
df_DrugClmPrice_Land = df_Load_DrugClmPrice.select(
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    F.col("CLMSEQNBR").alias("CLMSEQNBR"),
    rpad(F.col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
    F.col("ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.col("METRICQTY").alias("METRICQTY"),
    F.when(
       (F.col("GPINUMBER").isNull()) | (F.length(trim(F.col("GPINUMBER"))) == 0),
       F.lit("NA")
    ).otherwise(trim(F.col("GPINUMBER"))).alias("GPINUMBER"),
    rpad(F.col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
    rpad(F.col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
    rpad(F.col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
    rpad(F.col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
    F.col("FNLPLANDTE").alias("FNLPLANDTE"),
    rpad(F.col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
    F.col("AWPUNITCST").alias("AWPUNITCST"),
    F.col("WACUNITCST").alias("WACUNITCST"),
    F.col("CTYPEUCOST").alias("CTYPEUCOST"),
    F.col("PROQTY").alias("PROQTY"),
    rpad(F.col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
    rpad(F.col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
    F.col("CLTRATE").alias("CLTRATE"),
    F.col("CLTTYPUCST").alias("CLTTYPUCST"),
    F.col("CLT2INGRCST").alias("CLT2INGRCST"),
    F.col("CLT2DISPFEE").alias("CLT2DISPFEE"),
    rpad(F.col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
    rpad(F.col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
    rpad(F.col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE"),
    F.col("CLT2RATE").alias("CLT2RATE"),
    F.col("RXNETWRKQL").alias("RXNETWRKQL"),
    F.col("CLT2SLSTAX").alias("CLT2SLSTAX"),
    F.col("CLT2PSTAX").alias("CLT2PSTAX"),
    F.col("CLT2FSTAX").alias("CLT2FSTAX"),
    F.col("CLT2DUEAMT").alias("CLT2DUEAMT"),
    F.col("CLT2OTHAMT").alias("CLT2OTHAMT"),
    F.col("CLTINCENTV").alias("CLTINCENTV"),
    F.col("CALINCENTV2").alias("CALINCENTV2"),
    F.col("CLTOTHPAYA").alias("CLTOTHPAYA"),
    F.col("CLTTOTHAMT").alias("CLTTOTHAMT"),
    rpad(F.col("CLNTDEF2"), 10, " ").alias("CLNTDEF2"),
    F.col("FMSTIER").alias("TIER_ID"),
    rpad(F.col("MULTSRCCDE"), 1, " ").alias("DRUG_TYP_CD"),
    rpad(F.col("PLANDRUGST"), 1, " ").alias("PLN_DRUG_STTUS_CD"),
    rpad(F.col("PLANTYPE"), 8, " ").alias("PLANTYPE")
)

# 6) BuyPriceValidation
df_BuyPriceValidation = df_BuyPriceNotEqClntDueAmt.select(
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    F.col("CLMSEQNBR").alias("CLMSEQNBR"),
    rpad(F.col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
    F.col("ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.col("METRICQTY").alias("METRICQTY"),
    rpad(F.col("GPINUMBER"), 14, " ").alias("GPINUMBER"),
    rpad(F.col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
    rpad(F.col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
    rpad(F.col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
    rpad(F.col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
    F.col("FNLPLANDTE").alias("FNLPLANDTE"),
    rpad(F.col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
    F.col("AWPUNITCST").alias("AWPUNITCST"),
    F.col("WACUNITCST").alias("WACUNITCST"),
    F.col("CTYPEUCOST").alias("CTYPEUCOST"),
    F.col("PROQTY").alias("PROQTY"),
    rpad(F.col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
    rpad(F.col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
    F.col("CLTTYPUCST").alias("CLTTYPUCST"),
    F.col("RXNETWRKQL").alias("RXNETWRKQL"),
    F.col("CLTRATE").alias("CLTRATE"),
    F.col("CLT2RATE").alias("CLT2RATE"),
    F.col("CLTINGRCST").alias("CLTINGRCST"),
    F.col("CLTDISPFEE").alias("CLTDISPFEE"),
    F.col("CLTSLSTAX").alias("CLTSLSTAX"),
    F.col("CLTINCENTV").alias("CLTINCENTV"),
    F.col("CLTPATPAY").alias("CLTPATPAY"),
    F.col("CLTTOTHAMT").alias("CLTTOTHAMT"),
    F.col("CLTOTHPAYA").alias("CLTOTHPAYA"),
    F.col("CLT2PSTAX").alias("CLT2PSTAX"),
    F.col("CLT2FSTAX").alias("CLT2FSTAX"),
    F.col("CLTDUEAMT").alias("CLTDUEAMT"),
    F.col("CLT2INGRCST").alias("CLT2INGRCST"),
    F.col("CLT2DISPFEE").alias("CLT2DISPFEE"),
    F.col("CLT2SLSTAX").alias("CLT2SLSTAX"),
    F.col("CLT2OTHAMT").alias("CLT2OTHAMT"),
    F.col("CLT2DUEAMT").alias("CLT2DUEAMT"),
    rpad(F.col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
    rpad(F.col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
    rpad(F.col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE")
)

# 7) SellPriceValidation
df_SellPriceValidation = df_SellPriceNotEqClntDueAmt.select(
    F.lit("OPTUMRX").alias("SRC_SYS_CD"),
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    F.col("CLMSEQNBR").alias("CLMSEQNBR"),
    rpad(F.col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
    F.col("ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.col("METRICQTY").alias("METRICQTY"),
    rpad(F.col("GPINUMBER"), 14, " ").alias("GPINUMBER"),
    rpad(F.col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
    rpad(F.col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
    rpad(F.col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
    rpad(F.col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
    F.col("FNLPLANDTE").alias("FNLPLANDTE"),
    rpad(F.col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
    F.col("AWPUNITCST").alias("AWPUNITCST"),
    F.col("WACUNITCST").alias("WACUNITCST"),
    F.col("CTYPEUCOST").alias("CTYPEUCOST"),
    F.col("PROQTY").alias("PROQTY"),
    rpad(F.col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
    rpad(F.col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
    F.col("CLTRATE").alias("CLTRATE"),
    F.col("CLTINGRCST").alias("CLTINGRCST"),
    F.col("CLTDISPFEE").alias("CLTDISPFEE"),
    F.col("CLTSLSTAX").alias("CLTSLSTAX"),
    F.col("CLTINCENTV").alias("CLTINCENTV"),
    F.col("CLTPATPAY").alias("CLTPATPAY"),
    F.col("CLTTOTHAMT").alias("CLTTOTHAMT"),
    F.col("CLTOTHPAYA").alias("CLTOTHPAYA"),
    F.col("CLT2PSTAX").alias("CLT2PSTAX"),
    F.col("CLT2FSTAX").alias("CLT2FSTAX"),
    F.col("CLTDUEAMT").alias("CLTDUEAMT"),
    F.col("CLT2INGRCST").alias("CLT2INGRCST"),
    F.col("CLT2DISPFEE").alias("CLT2DISPFEE"),
    F.col("CLT2SLSTAX").alias("CLT2SLSTAX"),
    F.col("CLT2OTHAMT").alias("CLT2OTHAMT"),
    F.col("CLT2DUEAMT").alias("CLT2DUEAMT"),
    rpad(F.col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
    rpad(F.col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
    rpad(F.col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE")
)

# 8) DRUG_CLM_ACCUM_IMPCT_prep
df_DRUG_CLM_ACCUM_IMPCT_prep = df_Load_DRUG_CLM_ACCUM.select(
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(
        F.when(
            (F.col("CLIENTDEF3").isNull()) | (F.length(trim(F.col("CLIENTDEF3"))) == 0),
            F.lit("NA")
        ).otherwise(trim(F.col("CLIENTDEF3"))),
        10, " "
    ).alias("CLIENTDEF3"),
    F.when(
        (F.col("CCTRESRV10").isNull()) | (F.length(trim(F.col("CCTRESRV10"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CCTRESRV10")).alias("CCAA_COUPON_AMT"),
    F.when(
        (F.col("FAMDEDPTD").isNull()) | (F.length(trim(F.col("FAMDEDPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("FAMDEDPTD")).alias("FMLY_ACCUM_DEDCT_AMT"),
    F.when(
        (F.col("FAMOOPPTD").isNull()) | (F.length(trim(F.col("FAMOOPPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("FAMOOPPTD")).alias("FMLY_ACCUM_OOP_AMT"),
    F.when(
        (F.col("INDDEDPTD").isNull()) | (F.length(trim(F.col("INDDEDPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("INDDEDPTD")).alias("INDV_ACCUM_DEDCT_AMT"),
    F.when(
        (F.col("INDOOPPTD").isNull()) | (F.length(trim(F.col("INDOOPPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("INDOOPPTD")).alias("INDV_ACCUM_OOP_AMT"),
    F.when(
        (F.col("CCTRESERV9").isNull()) | (F.length(trim(F.col("CCTRESERV9"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CCTRESERV9")).alias("INDV_APLD_DEDCT_AMT"),
    F.when(
        (F.col("OOPAPPLIED").isNull()) | (F.length(trim(F.col("OOPAPPLIED"))) == 0),
        F.lit(0)
    ).otherwise(F.col("OOPAPPLIED")).alias("INDV_APLD_OOP_AMT")
)

# -------------------------------------------------------------------------
# Write all outputs to files
# -------------------------------------------------------------------------

# 1) PDX_CLM_STD_INPT_Land
write_files(
    df_PDX_CLM_STD_INPT_Land,
    f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 2) DeniedClm_Land
write_files(
    df_DeniedClm_Land,
    f"{adls_path}/verified/{SrcSysCd}_DeniedClaims_Landing.dat.{RunID}",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# 3) W_DRUG_CLM_PCP
write_files(
    df_W_DRUG_CLM_PCP,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 4) W_DRUG_ENR
write_files(
    df_W_DRUG_ENR,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 5) DrugClmPrice_Land
write_files(
    df_DrugClmPrice_Land,
    f"{adls_path}/verified/DrugClmPrice_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# 6) BuyPriceValidation
write_files(
    df_BuyPriceValidation,
    f"{adls_path}/verified/Optum_BuyPriceValidation_{RunID}.dat",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# 7) SellPriceValidation
write_files(
    df_SellPriceValidation,
    f"{adls_path}/verified/Optum_SellPriceValidation_{RunID}.dat",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# 8) DRUG_CLM_ACCUM_IMPCT_prep
write_files(
    df_DRUG_CLM_ACCUM_IMPCT_prep,
    f"{adls_path}/load/DRUG_CLM_ACCUM_IMPCT_prep.dat",
    delimiter=',',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)  

# COMMAND ----------