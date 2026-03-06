# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC Â© Copyright 2019 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : OptumDrugLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      OptumRx Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      09/20/2019      6131- PBM Replacement            Initial Programming                                                                                             IntegrateDev1                Kalyan Neelam           10/16/2019
# MAGIC Sri Nannapaneni             10/25/2019      6131- PBM Replacement            Modifed to include spread price extract                                                              IntegrateDev1                
# MAGIC Sri Nannapaneni             11/21/2019      6131- PBM Replacement            Pharmacy ID qualifier taken from RxCLAIM.                                                       IntegrateDev1                
# MAGIC 
# MAGIC 
# MAGIC                                                                                                                    Transformer  to the MapToAllDownstreamCommonFiles transformer.
# MAGIC Sharon Andrew               2020-01-07       6131- PBM Replacement             Changed aggregation for Drug_Header_Record.dat 
# MAGIC                                                                                                                      Added aggregation to sniff out COB claims written to /verified/Optum_SellPriceValidation_#RunID#.dat                                   2020-01-09
# MAGIC 
# MAGIC Peter Gichiri                    2020-01-14       6131- PBM Replacement            Moved the logic Fix(Extract.AWPUNITCST,2) from the DataCleansing              IntegrateDev2                Kalyan Neelam            2020-01-14
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-01-28       6131- PBM Replacement             Added BuyPrice Audit Validation written to /verified/Optum_BuyPriceValidation_#RunID#.dat               Kalyan Neelam            2020-01-29
# MAGIC                                                                                                                     Mapped 3 fields PLANQUAL,SRXNETWK,RXNUMBER to CLNT_GNRL_PRPS_AREA field  O/P     
# MAGIC Rekha Radhakrishna     2020-01-30       6131- PBM Replacement             Mapped fields CLTOTHPAYA, CLTINCENTV, CLTTOTHAMT, CALINCENTV2 for SpreadPricing O/P  Kalyan Neelam            2020-02-06
# MAGIC                                                                                                                   
# MAGIC Rekha Radhakrishna     2020-02-06       6131- PBM Replacement             Changed logic for FULL_AVG_WHLSL_PRICE                                                                                       Kalyan Neelam            2020-02-06
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-02-10       6131- PBM Replacement             Mapped fields  CLT2PSTAX and CLT2FSTAX to DRUG_CLM_PRICE O/P     IntegrateDev2                Kalyan Neelam           2020-02-10
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-02-25       6131- PBM Replacement             Changed COPAY_AMT mapping to CLTFCOPAY                                              IntegrateDev2                Hugh Sisson               2020-03-06
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-03-10       6131- PBM Replacement             Changed OTH_PAYOR_AMT mapping to CLTOTHPAYA                                 IntegrateDev2                Hugh Sisson               2020-03-10
# MAGIC 
# MAGIC Velmani                          2020-03-13       6131- PBM Replacement             Mapped new column for No Bill No Pay Indicator                                               IntegrateDev2                                                                                                                        
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-03-20       6131- PBM Replacement             Changed mapping for PAR_PDX_IN                                                                  IntegrateDev2   
# MAGIC 
# MAGIC Velmani                          2020-03-31       6131- PBM Replacement             Mapped new column for Speciality  Indicator                                                      IntegrateDev2                      
# MAGIC 
# MAGIC Rekha Radhakrishna    2020-08-30        PBM PhaseII government programs    Added LOB_IN field and default 'COM'                                                       IntegrateDev2               Sravya Gorla                2020-12-09
# MAGIC                                                                                                                            and mapped GPINUMBER to GNRC_PROD_IN field   
# MAGIC 
# MAGIC 
# MAGIC Rekha Radhakrishna    2020-02-05        PBM PhaseII                        Added LOB indicator to Program Info written to Audit File and DATESBM                IntegrateDev2\(9)Abhiram Dasarathy\(9)2021-02-09
# MAGIC 
# MAGIC Peter Gichiri                   2021-04-08        PBM PhaseII - US 362873     Mapped FMSTIER to MCARE_D_COV_DRUG  based on                                     IntegrateDev2            Jaideep Mankala          04/12/2021
# MAGIC                                                                                                               \(9)\(9)  the GROUPPLAN logic
# MAGIC Geetanjali Rajendran    2021-06-03        PBM PhaseII                        Added TIER_ID, DRUG_TYPE_CD and CLIENTDEF2 to the                                              IntegrateDev2\(9)Abhiram Dasarathy\(9)2021-06-23
# MAGIC                                                                                                                              DrugClmPrice Landing file  and modified derivation for DRUG_TYP in
# MAGIC                                                                                                                              PaidReversal Landing file
# MAGIC Bill Schroeder               2023-07-12        US-586570                        Added extract file DRUG_CLM_ACCUM_IMPCT_prep and RunCycle parm in Job Properties.  IntegrateDev2             Jeyaprasanna    2023-08-13
# MAGIC 
# MAGIC Arpitha V                      2023-11-07        US 600305                        Added  PLN_DRUG_STTUS_CD  to the DrugClmPrice Landing file                              IntegrateDevB                     Jeyaprasanna     2024-01-01
# MAGIC 
# MAGIC Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                    IntegrateDev2                  Jeyaprasanna      2024-03-14

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
# Databricks PySpark Notebook

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
RunID = get_widget_value('RunID', '')
InFile = get_widget_value('InFile', '')
MbrTermDate = get_widget_value('MbrTermDate', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
SrcSysCdSK = get_widget_value('SrcSysCdSK', '')
RunCycle = get_widget_value('RunCycle', '')
RunDate = get_widget_value('RunDate', '')

# Obtain JDBC connection for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from IDS_MBR (DB2 Connector)
extract_query = (
    f"SELECT "
    f"SUB.SUB_ID||MBR.MBR_SFX_NO as MEMBERID, "
    f"GRP.GRP_ID, "
    f"MBR.MBR_UNIQ_KEY, "
    f"SUB.SUB_ID, "
    f"MBR.MBR_SFX_NO "
    f"FROM {IDSOwner}.MBR MBR, "
    f"{IDSOwner}.SUB SUB, "
    f"{IDSOwner}.GRP GRP "
    f"WHERE MBR.SUB_SK = SUB.SUB_SK "
    f"AND SUB.GRP_SK = GRP.GRP_SK "
    f"AND MBR.TERM_DT_SK >= '{MbrTermDate}'"
)

df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Match columns to hashed file definition and rename
df_IDS_MBR_renamed = (
    df_IDS_MBR.select(
        F.col("MEMBERID").alias("MEMBER_ID"),
        F.col("GRP_ID"),
        F.col("MBR_UNIQ_KEY"),
        F.col("SUB_ID"),
        F.col("MBR_SFX_NO")
    )
)

# Deduplicate to replace hf_optum_clm_mbr_land (Scenario A)
df_hf_optum_clm_mbr_land = dedup_sort(
    df_IDS_MBR_renamed,
    partition_cols=["MEMBER_ID", "GRP_ID"],
    sort_cols=[]
)

# Read from Optum (CSeqFileStage). No schema given, so read all columns as strings.
df_Optum_raw = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

# Assign column names in the order specified by the stage (all read as string initially).
# This list matches the output columns of stage "Optum" exactly in the same order:
optum_columns = [
    "RXCLMNBR", "CLMSEQNBR", "CLAIMSTS", "CARID", "SCARID", "CARRPROC", "CLNTID", "CLNTSGMNT",
    "CLNTREGION", "ACCOUNTID", "ACCTBENCDE", "VERSIONNBR", "GROUPID", "GROUPPLAN", "GRPCLIBENF",
    "GROUPSIC", "CLMRESPSTS", "MEMBERID", "MBRLSTNME", "MBRFSTNME", "MBRMDINIT", "MBRPRSNCDE",
    "MBRRELCDE", "MBRSEX", "MBRBIRTH", "MBRAGE", "MBRZIP", "SOCSECNBR", "DURKEY", "DURFLAG",
    "MBRFAMLYID", "MBRFAMLIND", "MBRFAMLTYP", "COBIND", "MBRPLAN", "MBRPRODCDE", "MBRRIDERCD",
    "CARENETID", "CAREQUALID", "CAREFACID", "CAREFACNAM", "MBRPCPHYS", "PPRSFSTNME", "PPRSLSTNME",
    "PPRSMDINIT", "PPRSSPCCDE", "PPRSTATE", "MBRALTINFL", "MBRALTINCD", "MBRALTINID", "MBRMEDDTE",
    "MBRMEDTYPE", "MBRHICCDE", "CARDHOLDER", "PATLASTNME", "PATFRSTNME", "PERSONCDE", "RELATIONCD",
    "SEXCODE", "BIRTHDTE", "ELIGCLARIF", "CUSTLOC", "SBMPLSRVCE", "SBMPATRESD", "PRMCAREPRV",
    "PRMCAREPRQ", "FACILITYID", "OTHCOVERAG", "BINNUMBER", "PROCESSOR", "GROUPNBR", "TRANSCDE",
    "DATESBM", "TIMESBM", "CHGDATE", "CHGTIME", "ORGPDSBMDT", "RVDATESBM", "CLMCOUNTER", "GENERICCTR",
    "FORMLRYCTR", "RXNUMBER", "RXNUMBERQL", "REFILL", "DISPSTATUS", "DTEFILLED", "COMPOUNDCD",
    "SBMCMPDTYP", "PRODTYPCDE", "PRODUCTID", "PRODUCTKEY", "METRICQTY", "DECIMALQTY", "DAYSSUPPLY",
    "PSC", "WRITTENDTE", "NBRFLSAUTH", "ORIGINCDE", "SBMCLARCD1", "SBMCLARCD2", "SBMCLARCD3",
    "PAMCNBR", "PAMCCDE", "PRAUTHNBR", "PRAUTHRSN", "PRAUTHFDTE", "PRAUTHTDTE", "LABELNAME",
    "PRODNM", "DRUGMFGRID", "DRUGMFGR", "GPINUMBER", "GNRCNME", "PRDPACUOM", "PRDPACSIZE", "DDID",
    "GCN", "GCNSEQ", "KDC", "AHFS", "DRUGDEACOD", "RXOTCIND", "MULTSRCCDE", "GENINDOVER", "PRDREIMIND",
    "BRNDTRDNME", "FDATHERAEQ", "METRICSTRG", "DRGSTRGUOM", "ADMINROUTE", "ADMINRTESN", "DOSAGEFORM",
    "NDA", "ANDA", "ANDAOR", "RXNORMCODE", "MNTDRUGCDE", "MNTSOURCE", "MNTCARPROR", "MNTGPILIST",
    "CPQSPCPRG", "CPQSPCPGIN", "CPQSPCSCHD", "THRDPARTYX", "DRGUNITDOS", "SBMUNITDOS", "ALTPRODTYP",
    "ALTPRODCDE", "SRXNETWRK", "SRXNETTYPE", "RXNETWORK", "RXNETWRKNM", "RXNETCARR", "RXNETWRKQL",
    "RXNETPRCQL", "REGIONCDE", "PHRAFFIL", "NETPRIOR", "NETTYPE", "NETSEQ", "PAYCNTR", "PHRNDCLST",
    "PHRGPILST", "SRVPROVID", "SRVPROVIDQ", "NPIPROV", "PRVNCPDPID", "SBMSRVPRID", "SBMSRVPRQL",
    "SRVPROVNME", "PROVLOCKQL", "PROVLOCKID", "STORENBR", "AFFILIATIN", "PAYEEID", "DISPRCLASS",
    "DISPROTHER", "PHARMZIP", "PRESNETWID", "PRESCRIBER", "PRESCRIDQL", "NPIPRESCR", "PRESCDEAID",
    "PRESLSTNME", "PRESFSTNME", "PRESMDINIT", "PRESSPCCDE", "PRESSPCCDQ", "PRSSTATE", "SBMRPHID",
    "SBMRPHIDQL", "FNLPLANCDE", "FNLPLANDTE", "PLANTYPE", "PLANQUAL", "PLNNDCLIST", "LSTQUALNDC",
    "PLNGPILIST", "LSTQUALGPI", "PLNPNDCLST", "PLNPGPILST", "PLANDRUGST", "PLANFRMLRY", "PLNFNLPSCH",
    "PHRDCSCHID", "PHRDCSCHSQ", "CLTDCSCHID", "CLTDCSCHSQ", "PHRDCCSCID", "PHRDCCSCSQ", "CLTDCCSCID",
    "CLTDCCSCSQ", "PHRPRTSCID", "CLTPRTSCID", "PHRRMSCHID", "CLTRMSCHID", "PRDPFLSTID", "PRFPRDSCID",
    "FORMULARY", "FORMLRFLAG", "TIERVALUE", "CONTHERAPY", "CTSCHEDID", "PRBASISCHD", "REGDISOR",
    "DRUGSTSTBL", "TRANSBEN", "MESSAGECD1", "MESSAGE1", "MESSAGECD2", "MESSAGE2", "MESSAGECD3",
    "MESSAGE3", "REJCNT", "REJCDE1", "REJCDE2", "REJCDE3", "RJCPLANID", "DURCONFLCT", "DURINTERVN",
    "DUROUTCOME", "LVLSERVICE", "PHARSRVTYP", "DIAGNOSIS", "DIAGNOSISQ", "RVDURCNFLC", "RVDURINTRV",
    "RVDUROUTCM", "RVLVLSERVC", "DRGCNFLCT1", "SEVERITY1", "OTHRPHARM1", "DTEPRVFIL1", "QTYPRVFIL1",
    "DATABASE1", "OTHRPRESC1", "FREETEXT1", "DRGCNFLCT2", "SEVERITY2", "OTHRPHARM2", "DTEPRVFIL2",
    "QTYPRVFIL2", "DATABASE2", "OTHRPRESC2", "FREETEXT2", "DRGCNFLCT3", "SEVERITY3", "OTHRPHARM3",
    "DTEPRVFIL3", "QTYPRVFIL3", "DATABASE3", "OTHRPRESC3", "FREETEXT3", "FEETYPE", "BENUNITCST",
    "AWPUNITCST", "WACUNITCST", "GEAPUNTCST", "CTYPEUCOST", "BASISCOST", "PRICEQTY", "PRODAYSSUP",
    "PROQTY", "RVINCNTVSB", "SBMINGRCST", "SBMDISPFEE", "SBMPSLSTX", "SBMFSLSTX", "SBMSLSTX",
    "SBMPATPAY", "SBMAMTDUE", "SBMINCENTV", "SBMPROFFEE", "SBMTOTHAMT", "SBMOPAMTCT", "SBMOPAMTQL",
    "USUALNCUST", "DENIALDTE", "OTHRPAYOR", "SBMMDPDAMT", "CALINGRCST", "CALDISPFEE", "CALPSTAX",
    "CALFSTAX", "CALSLSTAX", "CALPATPAY", "CALDUEAMT", "CALWITHHLD", "CALFCOPAY", "CALPCOPAY",
    "CALCOPAY", "CALPRODSEL", "CALATRTAX", "CALEXCEBFT", "CALINCENTV", "CALATRDED", "CALCOB",
    "CALTOTHAMT", "CALPROFFEE", "CALOTHPAYA", "CALCOSTSRC", "CALADMNFEE", "CALPROCFEE", "CALPATSTAX",
    "CALPLNSTAX", "CALPRVNSEL", "CALPSCBRND", "CALPSCNONP", "CALPSCBRNP", "CALCOVGAP", "CALINGCSTC",
    "CALDSPFEEC", "PHRINGRCST", "PHRDISPFEE", "PHRPPSTAX", "PHRFSTAX", "PHRSLSTAX", "PHRPATPAY",
    "PHRDUEAMT", "PHRWITHHLD", "PHRPPRCS", "PHRPRCST", "PHRPTPS", "PHRPTPST", "PHRCOPAYSC",
    "PHRCOPAYSS", "PHRFCOPAY", "PHRPCOPAY", "PHRCOPAY", "PHRPRODSEL", "PHRATRTAX", "PHREXCEBFT",
    "PHRINCENTV", "PHRATRDED", "PHRCOB", "PHRTOTHAMT", "PHRPROFFEE", "PHROTHPAYA", "PHRCOSTSRC",
    "PHRCOSTTYP", "PHRPRCTYPE", "PHRRATE", "PHRPROCFEE", "PHRPATSTAX", "PHRPLNSTAX", "PHRPRVNSEL",
    "PHRPSCBRND", "PHRPSCNONP", "PHRPSCBRNP", "PHRCOVGAP", "PHRINGCSTC", "PHRDSPFEEC", "POSINGRCST",
    "POSDISPFEE", "POSPSLSTAX", "POSFSLSTAX", "POSSLSTAX", "POSPATPAY", "POSDUEAMT", "POSWITHHLD",
    "POSCOPAY", "POSPRODSEL", "POSATRTAX", "POSEXCEBFT", "POSINCENTV", "POSATRDED", "POSTOTHAMT",
    "POSPROFFEE", "POSOTHPAYA", "POSCOSTSRC", "POSPROCFEE", "POSPATSTAX", "POSPLNSTAX", "POSPRVNSEL",
    "POSPSCBRND", "POSPSCNONP", "POSPSCBRNP", "POSCOVGAP", "POSINGCSTC", "POSDSPFEEC", "CLIENTFLAG",
    "CLTINGRCST", "CLTDISPFEE", "CLTSLSTAX", "CLTPATPAY", "CLTDUEAMT", "CLTWITHHLD", "CLTPRCS",
    "CLTPRCST", "CLTPTPS", "CLTPTPST", "CLTCOPAYS", "CLTCOPAYSS", "CLTFCOPAY", "CLTPCOPAY", "CLTCOPAY",
    "CLTPRODSEL", "CLTPSTAX", "CLTFSTAX", "CLTATRTAX", "CLTEXCEBFT", "CLTINCENTV", "CLTATRDED",
    "CLTTOTHAMT", "CLTPROFFEE", "CLTCOB", "CLTOTHPAYA", "CLTCOSTSRC", "CLTCOSTTYP", "CLTPRCTYPE",
    "CLTRATE", "CLTPRSCSTP", "CLTPRSCHNM", "CLTPROCFEE", "CLTPATSTAX", "CLTPLNSTAX", "CLTPRVNSEL",
    "CLTPSCBRND", "CLTPSCNONP", "CLTPSCBRNP", "CLTCOVGAP", "CLTINGCSTC", "CLTDSPFEEC", "RSPREIMBUR",
    "RSPINGRCST", "RSPDISPFEE", "RSPPSLSTAX", "RSPFSLSTAX", "RSPSLSTAX", "RSPPATPAY", "RSPDUEAMT",
    "RSPFCOPAY", "RSPPCOPAY", "RSPCOPAY", "RSPPRODSEL", "RSPATRTAX", "RSPEXCEBFT", "RSPINCENTV",
    "RSPATRDED", "RSPTOTHAMT", "RSPPROFEE", "RSPOTHPAYA", "RSPACCUDED", "RSPREMBFT", "RSPREMDED",
    "RSPPROCFEE", "RSPPATSTAX", "RSPPLNSTAX", "RSPPRVNSEL", "RSPPSCBRND", "RSPPSCNONP", "RSPPSCBRNP",
    "RSPCOVGAP", "RSPINGCSTC", "RSPDSPFEEC", "RSPPLANID", "BENSTGQL1", "BENSTGAMT1", "BENSTGQL2",
    "BENSTGAMT2", "BENSTGQL3", "BENSTGAMT3", "BENSTGQL4", "BENSTGAMT4", "ESTGENSAV", "SPDACCTREM",
    "HLTHPLNAMT", "INDDEDPTD", "INDDEDREM", "FAMDEDPTD", "FAMDEDREM", "DEDSCHED", "DEDACCC",
    "DEDFLAG", "INDLBFTUT_1", "INDLBFTPTD", "INDLBFTREM", "INDLBFTUT_2", "FAMLBFTPTD", "FAMLBFTREM",
    "LFTBFTMSCH", "LFTBFTACCC", "LFTBFTFLAG", "INDBFTUT", "INDBMAXPTD", "FAMBFTUT", "FAMBMAXPTD",
    "INDBMAXREM", "FAMBMAXREM", "BFTMAXSCHD", "BFTMAXACCC", "BFTMAXFLAG", "INDOOPPTD", "FAMOOPPTD",
    "INDOOPREM", "FAMOOPREM", "OOPSCHED", "OOPACCC", "OOPFLAG", "CONTRIBUT", "CONTBASIS",
    "CONTSCHED", "CONTACCCD", "CONTFLAG", "RXTFLAG", "REIMBURSMT", "CLMORIGIN", "HLDCLMFLAG",
    "HLDCLMDAYS", "PARTDFLAG", "COBEXTFLG", "PAEXTFLG", "HSAEXTIND", "FFPMEDRMST", "FFPMEDPXST",
    "FFPMEDMSST", "INCIDENTID", "ETCNBR", "DTEINJURY", "ADDUSER", "CHGUSER", "DMRUSERID", "PRAUSERID",
    "CLAIMREFID", "EOBDNOV", "EOBPDOV", "MANTRKNBR", "MANRECVDTE", "PASAUTHTYP", "PASAUTHID",
    "PASREQTYPE", "PASREQFROM", "PASREQTHRU", "PASBASISRQ", "PASREPFN", "PASREPLN", "PASSTREET",
    "PASCITY", "PASSTATE", "PASZIP", "PASPANBR", "PASAUTHNBR", "PASSDOCCT", "PAYERTYPE", "DELAYRSNCD",
    "MEDCDIND", "MEDCDID", "MEDCDAGNBR", "MEDCDTCN", "FMSTIER", "FMSSTATUS", "FMSDFLTIND",
    "FMSBENLST", "FMSLSTLVL3", "FMSLSTLVL2", "FMSLSTLVL1", "FMSRULESET", "FMSRULE", "FMSPROCCD",
    "CLIENTDEF1", "CLIENTDEF2", "CLIENTDEF3", "CLIENTDEF4", "CLIENTDEF5", "CCTRESERV1", "CCTRESERV2",
    "CCTRESERV3", "CCTRESERV4", "CCTRESERV5", "CCTRESERV6", "CCTRESERV7", "CCTRESERV8", "CCTRESERV9",
    "CCTRESRV10", "OOPAPPLIED", "CCTRESRV12", "CCTRESRV13", "CCTRESRV14", "USERFIELD", "EXTRACTDTE",
    "BATCHCTRL", "CLTTYPUCST", "REJCDE4", "REJCDE5", "REJCDE6", "MESSAGECD4", "MESSAGE4",
    "MESSAGECD5", "MESSAGE5", "MESSAGECD6", "MESSAGEG6", "CLIENT2FLAG", "CLT2INGRCST",
    "CLT2DISPFEE", "CLT2SLSTAX", "CLT2DUEAMT", "CLTPRSCSTP2", "CLTPRSCHNM2", "CLT2PRCS", "CLT2PRCST",
    "CLT2PSTAX", "CLT2FSTAX", "CLT2OTHAMT", "CLT2COSTSRC", "CLT2COSTTYP", "CLT2PRCTYPE", "CLT2RATE",
    "CLT2DCCSCID", "CLT2DCCSCSQ", "CLT2DCSCHID", "CLT2DCSCHSQ", "CALINCENTV2", "CLNT3FLAG",
    "CLT3INGRCST", "CLT3DISPFEE", "CLT3SLSTAX", "CLT3DUEAMT", "CLTPRSCSTP3", "CLTPRSCHNM3",
    "CLT3PRCS", "CLT3PRCST", "CLT3PSTAX", "CLT3FSTAX", "CLT3OTHAMT", "CLT3COSTSRC", "CLT3COSTTYP",
    "CLT3PRCTYPE", "CLT3RATE", "CLT3DCCSCID", "CLT3DCCSCSQ", "CLT3DCSCHID", "CLT3DCSCHSQ",
    "CALINCENTV3", "TBR_TBR_REJECT_REASON1", "TBR_TBR_REJECT_REASON2", "TBR_TBR_REJECT_REASON3",
    "SBM_QTY_INT_DIS", "SBM_DS_INT_DIS", "OTHR_AMT_CLMSBM_CT", "OTHR_AMT_CLMSBM_QLFR1",
    "OTHR_AMT_CLMSBM1", "OTHR_AMT_CLMSBM_QLFR2", "OTHR_AMT_CLMSBM2", "OTHR_AMT_CLMSBM_QLFR3",
    "OTHR_AMT_CLMSBM3", "OTHR_AMT_CLMSBM_QLFR4", "OTHR_AMT_CLMSBM4", "OTHR_AMT_CLMSBM_QLFR5",
    "OTHR_AMT_CLMSBM5", "RSN_SRVCDE1", "RSN_SRVCDE2", "RSN_SRVCDE3", "PROF_SRVCDE1", "PROF_SRVCDE2",
    "PROF_SRVCDE3", "RESULT_OF_SRV_CDE1", "RESULT_OF_SRV_CDE2", "RESULT_OF_SRV_CDE3", "SCH_RX_ID_NBR",
    "BASIS_CAL_DISPFEE", "BASIS_CAL_COPAY", "BASIS_CAL_FLT_TAX", "BASIS_CAL_PRCNT_TAX",
    "BASIS_CAL_COINSUR", "PAID_MSG_CODE_COUNT", "PAID_MESSAGE_CODE_1", "PAID_MESSAGE_CODE_2",
    "PAID_MESSAGE_CODE_3", "PAID_MESSAGE_CODE_4", "PAID_MESSAGE_CODE_5",
    "FILLER_FOR_REJECT_FIELD_IND"
]

df_Optum = df_Optum_raw
for i, c in enumerate(optum_columns):
    df_Optum = df_Optum.withColumnRenamed(f"_c{i}", c)

# Build DataCleansing transformations (as a single DataFrame). The stage variable svDATESBM:
df_DataCleansing = df_Optum

# svDATESBM logic:
df_DataCleansing = df_DataCleansing.withColumn(
    "svDATESBM",
    F.when(
        (F.length(trim(F.col("DATESBM"))) == 0) | (F.col("DATESBM").isNull()),
        F.lit("1753-01-01")
    ).otherwise(
        F.date_format(
            F.to_date(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("DATESBM"), "\n", ""),
                        "\r", ""
                    ),
                    "\t", ""
                ),
                "yyyyMMdd"
            ),
            "yyyy-MM-dd"
        )
    )
)

# There are many expressions in "OutputPins" (inMbrMatch). We apply them directly in a select.
# Build the columns for inMbrMatch:

df_inMbrMatch = df_DataCleansing.select(
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    F.col("CLMSEQNBR").alias("CLMSEQNBR"),
    F.trim(F.col("CLAIMSTS")).alias("CLAIMSTS"),
    F.trim(F.col("CARID")).alias("CARID"),
    F.when(
        (F.length(trim(F.col("ACCOUNTID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("ACCOUNTID"))).alias("ACCOUNTID"),
    F.when(
        (F.length(trim(F.col("GROUPID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("GROUPID"))).alias("GROUPID"),
    F.when(
        (F.col("MEMBERID").isNull()) | (F.length(F.col("MEMBERID")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("MEMBERID").substr(F.lit(1), F.lit(11))).alias("MEMBERID"),
    F.when(
        (F.length(trim(F.col("MBRLSTNME"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper(F.col("MBRLSTNME")))).alias("MBRLSTNME"),
    F.when(
        (F.length(trim(F.col("MBRFSTNME"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper(F.col("MBRFSTNME")))).alias("MBRFSTNME"),
    F.trim(F.col("MBRRELCDE")).alias("MBRRELCDE"),
    F.trim(F.col("MBRSEX")).alias("MBRSEX"),
    F.when(
        F.length(F.col("MBRBIRTH")) == 0,
        F.lit("1753-01-01")
    ).otherwise(
        F.date_format(
            F.to_date(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("MBRBIRTH"), "\n", ""),
                        "\r", ""
                    ),
                    "\t", ""
                ),
                "yyyyMMdd"
            ),
            "yyyy-MM-dd"
        )
    ).alias("MBRBIRTH"),
    F.trim(F.col("SOCSECNBR")).alias("SOCSECNBR"),
    F.trim(F.col("CUSTLOC")).alias("CUSTLOC"),
    F.trim(F.col("FACILITYID")).alias("FACILITYID"),
    F.trim(F.col("OTHCOVERAG")).alias("OTHCOVERAG"),
    F.col("svDATESBM").alias("DATESBM"),
    F.col("TIMESBM").alias("TIMESBM"),
    F.col("ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.trim(F.col("RXNUMBER")).alias("RXNUMBER"),
    F.trim(F.col("REFILL")).alias("REFILL"),
    F.trim(F.col("DISPSTATUS")).alias("DISPSTATUS"),
    F.date_format(
        F.to_date(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("DTEFILLED"), "\n", ""),
                    "\r", ""
                ),
                "\t", ""
            ),
            "yyyyMMdd"
        ),
        "yyyy-MM-dd"
    ).alias("DTEFILLED"),
    F.trim(F.col("COMPOUNDCD")).alias("COMPOUNDCD"),
    F.trim(F.col("PRODUCTID")).alias("PRODUCTID"),
    F.when(
        F.col("METRICQTY").isNull(),
        F.lit(-999999)
    ).otherwise(F.col("METRICQTY")).alias("METRICQTY"),
    F.when(
        F.col("DECIMALQTY").isNull(),
        F.lit(-999999)
    ).otherwise(F.col("DECIMALQTY")).alias("DECIMALQTY"),
    F.when(
        F.col("DAYSSUPPLY").isNull(),
        F.lit(-999)
    ).otherwise(trim(F.col("DAYSSUPPLY"))).alias("DAYSSUPPLY"),
    F.trim(F.col("PSC")).alias("PSC"),
    F.when(
        (F.length(trim(F.col("WRITTENDTE"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(
        F.date_format(
            F.to_date(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("WRITTENDTE"), "\n", ""),
                        "\r", ""
                    ),
                    "\t", ""
                ),
                "yyyyMMdd"
            ),
            "yyyy-MM-dd"
        )
    ).alias("WRITTENDTE"),
    F.trim(F.col("NBRFLSAUTH")).alias("NBRFLSAUTH"),
    F.trim(F.col("ORIGINCDE")).alias("ORIGINCDE"),
    F.trim(F.col("PRAUTHNBR")).alias("PRAUTHNBR"),
    F.trim(F.col("LABELNAME")).alias("LABELNAME"),
    F.col("GPINUMBER").alias("GPINUMBER"),
    F.col("MULTSRCCDE").alias("MULTSRCCDE"),
    F.col("GENINDOVER").alias("GENINDOVER"),
    F.col("CPQSPCPRG").alias("CPQSPCPRG"),
    F.col("CPQSPCPGIN").alias("CPQSPCPGIN"),
    F.trim(F.col("SRXNETWRK")).alias("SRXNETWRK"),
    F.trim(F.col("RXNETWORK")).alias("RXNETWORK"),
    F.col("RXNETWRKQL").alias("RXNETWRKQL"),
    F.trim(F.col("SRVPROVID")).alias("SRVPROVID"),
    F.when(
        F.length(trim(F.col("SRVPROVIDQ"))) == 0,
        F.lit("00")
    ).otherwise(trim(F.col("SRVPROVIDQ"))).alias("SRVPROVIDQ"),
    F.when(
        (F.col("NPIPROV").isNull()) | (F.length(trim(F.col("NPIPROV"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("NPIPROV"))).alias("NPIPROV"),
    F.when(
        (F.col("PRVNCPDPID").isNull()) | (F.length(trim(F.col("PRVNCPDPID"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("PRVNCPDPID"))).alias("PRVNCPDPID"),
    F.col("SRVPROVNME").alias("SRVPROVNME"),
    F.trim(F.col("DISPROTHER")).alias("DISPROTHER"),
    F.when(
        (F.col("PRESCRIBER").isNull()) | (F.length(trim(F.col("PRESCRIBER"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("PRESCRIBER")).alias("PRESCRIBER"),
    F.when(
        F.length(trim(F.col("PRESCRIDQL"))) == 0,
        F.lit("00")
    ).otherwise(trim(F.col("PRESCRIDQL"))).alias("PRESCRIDQL"),
    F.when(
        (F.col("NPIPRESCR").isNull()) | (F.length(trim(F.col("NPIPRESCR"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("NPIPRESCR")).alias("NPIPRESCR"),
    F.when(
        (F.col("PRESCDEAID").isNull()) | (F.length(trim(F.col("PRESCDEAID"))) == 0),
        F.lit("NA")
    ).otherwise(trim(F.col("PRESCDEAID"))).alias("PRESCDEAID"),
    F.when(
        F.length(trim(F.col("PRESLSTNME"))) == 0,
        F.lit("")
    ).otherwise(trim(F.col("PRESLSTNME"))).alias("PRESLSTNME"),
    F.when(
        F.length(trim(F.col("PRESFSTNME"))) == 0,
        F.lit("")
    ).otherwise(trim(F.col("PRESFSTNME"))).alias("PRESFSTNME"),
    F.when(
        F.length(trim(F.col("PRESSPCCDE"))) == 0,
        F.lit("NA")
    ).otherwise(trim(F.col("PRESSPCCDE"))).alias("PRESSPCCDE"),
    F.col("PRSSTATE").alias("PRSSTATE"),
    F.col("FNLPLANCDE").alias("FNLPLANCDE"),
    F.col("FNLPLANDTE").alias("FNLPLANDTE"),
    F.trim(F.col("PLANQUAL")).alias("PLANQUAL"),
    F.trim(F.col("COBIND")).alias("COBIND"),
    F.col("FORMLRFLAG").alias("FORMLRFLAG"),
    F.col("REJCDE1").alias("REJCDE1"),
    F.col("REJCDE2").alias("REJCDE2"),
    F.when(
        (F.col("AWPUNITCST").isNull()) | (F.length(trim(F.col("AWPUNITCST"))) == 0),
        F.lit("0.0")
    ).otherwise(F.col("AWPUNITCST")).alias("AWPUNITCST"),
    F.col("WACUNITCST").alias("WACUNITCST"),
    F.col("CTYPEUCOST").alias("CTYPEUCOST"),
    F.trim(F.col("BASISCOST")).alias("BASISCOST"),
    F.col("PROQTY").alias("PROQTY"),
    F.when(
        (F.col("SBMINGRCST").isNull()) | (F.length(trim(F.col("SBMINGRCST"))) == 0),
        F.lit(0)
    ).otherwise(F.col("SBMINGRCST")).alias("SBMINGRCST"),
    F.when(
        (F.col("SBMDISPFEE").isNull()) | (F.length(trim(F.col("SBMDISPFEE"))) == 0),
        F.lit(0)
    ).otherwise(F.col("SBMDISPFEE")).alias("SBMDISPFEE"),
    F.when(
        (F.col("SBMAMTDUE").isNull()) | (F.length(trim(F.col("SBMAMTDUE"))) == 0),
        F.lit(0)
    ).otherwise(F.col("SBMAMTDUE")).alias("SBMAMTDUE"),
    F.when(
        (F.col("USUALNCUST").isNull()) | (F.length(trim(F.col("USUALNCUST"))) == 0),
        F.lit(0)
    ).otherwise(F.col("USUALNCUST")).alias("USUALNCUST"),
    F.when(
        (F.length(trim(F.col("DENIALDTE"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(
        F.date_format(
            F.to_date(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("DENIALDTE"), "\n", ""),
                        "\r", ""
                    ),
                    "\t", ""
                ),
                "yyyyMMdd"
            ),
            "yyyy-MM-dd"
        )
    ).alias("DENIALDTE"),
    F.when(
        (F.col("PHRDISPFEE").isNull()) | (F.length(trim(F.col("PHRDISPFEE"))) == 0),
        F.lit(0)
    ).otherwise(F.col("PHRDISPFEE")).alias("PHRDISPFEE"),
    F.when(
        (F.col("CLTINGRCST").isNull()) | (F.length(trim(F.col("CLTINGRCST"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTINGRCST")).alias("CLTINGRCST"),
    F.when(
        (F.col("CLTDISPFEE").isNull()) | (F.length(trim(F.col("CLTDISPFEE"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTDISPFEE")).alias("CLTDISPFEE"),
    F.when(
        (F.col("CLTSLSTAX").isNull()) | (F.length(trim(F.col("CLTSLSTAX"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTSLSTAX")).alias("CLTSLSTAX"),
    F.when(
        (F.col("CLTPATPAY").isNull()) | (F.length(trim(F.col("CLTPATPAY"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTPATPAY")).alias("CLTPATPAY"),
    F.when(
        (F.col("CLTDUEAMT").isNull()) | (F.length(trim(F.col("CLTDUEAMT"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLTDUEAMT")).alias("CLTDUEAMT"),
    F.when(
        (F.col("CLTFCOPAY").isNull()) | (F.length(trim(F.col("CLTFCOPAY"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTFCOPAY")).alias("CLTFCOPAY"),
    F.when(
        (F.col("CLTPCOPAY").isNull()) | (F.length(trim(F.col("CLTPCOPAY"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTPCOPAY")).alias("CLTPCOPAY"),
    F.when(
        (F.col("CLTCOPAY").isNull()) | (F.length(trim(F.col("CLTCOPAY"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTCOPAY")).alias("CLTCOPAY"),
    F.when(
        (F.col("CLTINCENTV").isNull()) | (F.length(trim(F.col("CLTINCENTV"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTINCENTV")).alias("CLTINCENTV"),
    F.when(
        (F.col("CLTATRDED").isNull()) | (F.length(trim(F.col("CLTATRDED"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTATRDED")).alias("CLTATRDED"),
    F.col("CLTTOTHAMT").alias("CLTTOTHAMT"),
    F.when(
        (F.col("CLTOTHPAYA").isNull()) | (F.length(trim(F.col("CLTOTHPAYA"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CLTOTHPAYA")).alias("CLTOTHPAYA"),
    F.col("CLTCOSTTYP").alias("CLTCOSTTYP"),
    F.col("CLTPRCTYPE").alias("CLTPRCTYPE"),
    F.col("CLTRATE").alias("CLTRATE"),
    F.col("CLTPSCBRND").alias("CLTPSCBRND"),
    F.col("REIMBURSMT").alias("REIMBURSMT"),
    F.col("CLMORIGIN").alias("CLMORIGIN"),
    F.col("CLTTYPUCST").alias("CLTTYPUCST"),
    F.when(
        (F.col("CLT2INGRCST").isNull()) | (F.length(trim(F.col("CLT2INGRCST"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2INGRCST")).alias("CLT2INGRCST"),
    F.when(
        (F.col("CLT2DISPFEE").isNull()) | (F.length(trim(F.col("CLT2DISPFEE"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2DISPFEE")).alias("CLT2DISPFEE"),
    F.when(
        (F.col("CLT2SLSTAX").isNull()) | (F.length(trim(F.col("CLT2SLSTAX"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2SLSTAX")).alias("CLT2SLSTAX"),
    F.when(
        (F.col("CLT2DUEAMT").isNull()) | (F.length(trim(F.col("CLT2DUEAMT"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2DUEAMT")).alias("CLT2DUEAMT"),
    F.when(
        (F.col("CLT2PSTAX").isNull()) | (F.length(trim(F.col("CLT2PSTAX"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2PSTAX")).alias("CLT2PSTAX"),
    F.when(
        (F.col("CLT2FSTAX").isNull()) | (F.length(trim(F.col("CLT2FSTAX"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2FSTAX")).alias("CLT2FSTAX"),
    F.col("CLT2COSTSRC").alias("CLT2COSTSRC"),
    F.col("CLT2COSTTYP").alias("CLT2COSTTYP"),
    F.col("CLT2PRCTYPE").alias("CLT2PRCTYPE"),
    F.col("CLT2RATE").alias("CLT2RATE"),
    F.when(
        F.length(trim(F.col("PPRSFSTNME"))) == 0,
        F.lit("")
    ).otherwise(trim(F.col("PPRSFSTNME"))).alias("PPRSFSTNME"),
    F.when(
        F.length(trim(F.col("PPRSLSTNME"))) == 0,
        F.lit("")
    ).otherwise(trim(F.col("PPRSLSTNME"))).alias("PPRSLSTNME"),
    F.when(
        F.length(trim(F.col("PPRSTATE"))) == 0,
        F.lit("")
    ).otherwise(trim(F.col("PPRSTATE"))).alias("PPRSTATE"),
    F.when(
        (F.col("CLT2OTHAMT").isNull()) | (F.length(trim(F.col("CLT2OTHAMT"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CLT2OTHAMT")).alias("CLT2OTHAMT"),
    F.when(
        (F.col("CALINCENTV2").isNull()) | (F.length(trim(F.col("CALINCENTV2"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("CALINCENTV2")).alias("CALINCENTV2"),
    F.when(
        (F.col("RXNETPRCQL").isNull()) | (F.length(trim(F.col("RXNETPRCQL"))) == 0),
        F.lit("N")
    ).otherwise(
        F.when(F.trim(F.col("RXNETPRCQL")) == F.lit("Y"), F.lit("Y")).otherwise(F.lit("N"))
    ).alias("RXNETPRCQL"),
    F.when(
        (F.col("SBMPATRESD").isNull()) | (F.length(trim(F.col("SBMPATRESD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("SBMPATRESD")).alias("SBMPATRESD"),
    F.col("PRODTYPCDE").alias("PRODTYPCDE"),
    F.col("CONTHERAPY").alias("CONTHERAPY"),
    F.col("CTSCHEDID").alias("CTSCHEDID"),
    F.col("CLTPRODSEL").alias("CLTPRODSEL"),
    F.col("CLTATRTAX").alias("CLTATRTAX"),
    F.col("CLTPROCFEE").alias("CLTPROCFEE"),
    F.col("CLTPRVNSEL").alias("CLTPRVNSEL"),
    F.when(
        (F.col("CLIENTDEF1").isNull()) | (F.length(trim(F.col("CLIENTDEF1"))) == 0),
        F.lit("N")
    ).otherwise(trim(F.col("CLIENTDEF1"))).alias("CLIENTDEF1"),
    F.trim(F.col("GROUPPLAN")).alias("GROUPPLAN"),
    F.trim(F.col("FMSTIER")).alias("FMSTIER"),
    F.trim(F.col("CLIENTDEF2")).alias("CLNTDEF2"),
    F.col("INDDEDPTD").alias("INDDEDPTD"),
    F.col("FAMDEDPTD").alias("FAMDEDPTD"),
    F.col("INDOOPPTD").alias("INDOOPPTD"),
    F.col("FAMOOPPTD").alias("FAMOOPPTD"),
    F.col("CLIENTDEF3").alias("CLIENTDEF3"),
    F.col("CCTRESERV8").alias("CCTRESERV8"),
    F.col("CCTRESERV9").alias("CCTRESERV9"),
    F.col("CCTRESRV10").alias("CCTRESRV10"),
    F.col("OOPAPPLIED").alias("OOPAPPLIED"),
    F.when(
        (F.col("PLANDRUGST").isNull()) | (F.length(F.col("PLANDRUGST")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("PLANDRUGST")).alias("PLANDRUGST"),
    F.when(
        (F.col("PLANTYPE").isNull()) | (F.length(F.col("PLANTYPE")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("PLANTYPE")).alias("PLANTYPE")
)

# Also build the aggregator input (Aggregate link) from df_DataCleansing
df_aggregate_input = df_DataCleansing.select(
    F.col("CLAIMSTS").alias("CLAIMSTS"),
    F.monotonically_increasing_id().alias("INROW_NUM"),
    F.col("CLT2DUEAMT").alias("AMT_BILL"),
    F.col("svDATESBM").alias("DATESBM")
)

# Perform the aggregator: group by CLAIMSTS, DATESBM
df_CntByAdjDtRecType = (
    df_aggregate_input
    .groupBy("CLAIMSTS", "DATESBM")
    .agg(
        F.count("INROW_NUM").alias("RCRD_CNT"),
        F.sum("AMT_BILL").alias("TOT_AMT")
    )
    .select("CLAIMSTS", "RCRD_CNT", "TOT_AMT", "DATESBM")
)

# Now handle the Member_Lkp stage: join df_inMbrMatch with df_hf_optum_clm_mbr_land
# Key: inMbrMatch.GROUPID -> hf_optum_clm_mbr_land.GRP_ID, inMbrMatch.MEMBERID -> hf_optum_clm_mbr_land.MEMBER_ID
df_Member_Lkp_joined = (
    df_inMbrMatch.alias("inMbrMatch")
    .join(
        df_hf_optum_clm_mbr_land.alias("MBR_lkup"),
        [
            F.col("inMbrMatch.GROUPID") == F.col("MBR_lkup.GRP_ID"),
            F.col("inMbrMatch.MEMBERID") == F.col("MBR_lkup.MEMBER_ID")
        ],
        how="left"
    )
)

# Build the columns from the "Member_Lkp" output pin to "Sequential_File_460"
df_Sequential_File_460 = df_Member_Lkp_joined.select(
    F.col("inMbrMatch.RXCLMNBR").alias("RXCLMNBR"),
    F.col("inMbrMatch.CLMSEQNBR").alias("CLMSEQNBR"),
    F.col("inMbrMatch.CLAIMSTS").alias("CLAIMSTS"),
    F.rpad(F.col("inMbrMatch.CARID"), 9, " ").alias("CARID"),
    F.rpad(F.col("inMbrMatch.ACCOUNTID"), 15, " ").alias("ACCOUNTID"),
    F.rpad(F.col("inMbrMatch.ACCOUNTID"), 15, " ").alias("GROUPID"),
    F.rpad(F.trim(F.col("inMbrMatch.MEMBERID")).substr(F.lit(1), F.lit(20)), 20, " ").alias("MEMBERID"),
    F.rpad(F.col("inMbrMatch.MBRLSTNME"), 25, " ").alias("MBRLSTNME"),
    F.rpad(F.col("inMbrMatch.MBRFSTNME"), 15, " ").alias("MBRFSTNME"),
    F.rpad(F.col("inMbrMatch.MBRRELCDE"), 1, " ").alias("MBRRELCDE"),
    F.rpad(F.col("inMbrMatch.MBRSEX"), 1, " ").alias("MBRSEX"),
    F.col("inMbrMatch.MBRBIRTH").alias("MBRBIRTH"),
    F.rpad(F.col("inMbrMatch.SOCSECNBR"), 9, " ").alias("SOCSECNBR"),
    F.rpad(F.col("inMbrMatch.CUSTLOC"), 2, " ").alias("CUSTLOC"),
    F.rpad(F.col("inMbrMatch.FACILITYID"), 10, " ").alias("FACILITYID"),
    F.rpad(F.col("inMbrMatch.OTHCOVERAG"), 2, " ").alias("OTHCOVERAG"),
    F.rpad(F.col("inMbrMatch.DATESBM"), 8, " ").alias("DATESBM"),
    F.col("inMbrMatch.TIMESBM").alias("TIMESBM"),
    F.col("inMbrMatch.ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.rpad(F.col("inMbrMatch.RXNUMBER"), 12, " ").alias("RXNUMBER"),
    F.rpad(F.col("inMbrMatch.REFILL"), 2, " ").alias("REFILL"),
    F.rpad(F.col("inMbrMatch.DISPSTATUS"), 1, " ").alias("DISPSTATUS"),
    F.col("inMbrMatch.DTEFILLED").alias("DTEFILLED"),
    F.rpad(F.col("inMbrMatch.COMPOUNDCD"), 1, " ").alias("COMPOUNDCD"),
    F.rpad(F.col("inMbrMatch.PRODUCTID"), 20, " ").alias("PRODUCTID"),
    F.col("inMbrMatch.METRICQTY").alias("METRICQTY"),
    F.col("inMbrMatch.DECIMALQTY").alias("DECIMALQTY"),
    F.col("inMbrMatch.DAYSSUPPLY").alias("DAYSSUPPLY"),
    F.rpad(F.col("inMbrMatch.PSC"), 1, " ").alias("PSC"),
    F.col("inMbrMatch.WRITTENDTE").alias("WRITTENDTE"),
    F.col("inMbrMatch.NBRFLSAUTH").alias("NBRFLSAUTH"),
    F.rpad(F.col("inMbrMatch.ORIGINCDE"), 1, " ").alias("ORIGINCDE"),
    F.rpad(F.col("inMbrMatch.PRAUTHNBR"), 11, " ").alias("PRAUTHNBR"),
    F.rpad(F.col("inMbrMatch.LABELNAME"), 30, " ").alias("LABELNAME"),
    F.rpad(F.col("inMbrMatch.GPINUMBER"), 14, " ").alias("GPINUMBER"),
    F.rpad(F.col("inMbrMatch.MULTSRCCDE"), 1, " ").alias("MULTSRCCDE"),
    F.rpad(F.col("inMbrMatch.GENINDOVER"), 1, " ").alias("GENINDOVER"),
    F.rpad(F.col("inMbrMatch.CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
    F.rpad(F.col("inMbrMatch.CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
    F.rpad(F.col("inMbrMatch.SRXNETWRK"), 6, " ").alias("SRXNETWRK"),
    F.rpad(F.col("inMbrMatch.RXNETWORK"), 6, " ").alias("RXNETWORK"),
    F.rpad(F.col("inMbrMatch.RXNETWRKQL"), 1, " ").alias("RXNETWRKQL"),
    F.rpad(F.col("inMbrMatch.SRVPROVID"), 15, " ").alias("SRVPROVID"),
    F.rpad(F.col("inMbrMatch.SRVPROVIDQ"), 2, " ").alias("SRVPROVIDQ"),
    F.rpad(F.col("inMbrMatch.NPIPROV"), 10, " ").alias("NPIPROV"),
    F.rpad(F.col("inMbrMatch.PRVNCPDPID"), 12, " ").alias("PRVNCPDPID"),
    F.rpad(F.col("inMbrMatch.SRVPROVNME"), 55, " ").alias("SRVPROVNME"),
    F.rpad(F.col("inMbrMatch.DISPROTHER"), 3, " ").alias("DISPROTHER"),
    F.rpad(F.col("inMbrMatch.PRESCRIBER"), 15, " ").alias("PRESCRIBER"),
    F.rpad(F.col("inMbrMatch.PRESCRIDQL"), 2, " ").alias("PRESCRIDQL"),
    F.rpad(F.col("inMbrMatch.NPIPRESCR"), 10, " ").alias("NPIPRESCR"),
    F.rpad(F.col("inMbrMatch.PRESCDEAID"), 15, " ").alias("PRESCDEAID"),
    F.rpad(F.col("inMbrMatch.PRESLSTNME"), 25, " ").alias("PRESLSTNME"),
    F.rpad(F.col("inMbrMatch.PRESFSTNME"), 15, " ").alias("PRESFSTNME"),
    F.rpad(F.col("inMbrMatch.PRESSPCCDE"), 6, " ").alias("PRESSPCCDE"),
    F.rpad(F.col("inMbrMatch.PRSSTATE"), 3, " ").alias("PRSSTATE"),
    F.rpad(F.col("inMbrMatch.FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
    F.col("inMbrMatch.FNLPLANDTE").alias("FNLPLANDTE"),
    F.rpad(F.col("inMbrMatch.PLANQUAL"), 10, " ").alias("PLANQUAL"),
    F.rpad(F.col("inMbrMatch.COBIND"), 2, " ").alias("COBIND"),
    F.rpad(F.col("inMbrMatch.FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
    F.rpad(F.col("inMbrMatch.REJCDE1"), 3, " ").alias("REJCDE1"),
    F.rpad(F.col("inMbrMatch.REJCDE2"), 3, " ").alias("REJCDE2"),
    F.col("inMbrMatch.AWPUNITCST").alias("AWPUNITCST"),
    F.col("inMbrMatch.WACUNITCST").alias("WACUNITCST"),
    F.col("inMbrMatch.CTYPEUCOST").alias("CTYPEUCOST"),
    F.rpad(F.col("inMbrMatch.BASISCOST"), 2, " ").alias("BASISCOST"),
    F.col("inMbrMatch.PROQTY").alias("PROQTY"),
    F.col("inMbrMatch.SBMINGRCST").alias("SBMINGRCST"),
    F.col("inMbrMatch.SBMDISPFEE").alias("SBMDISPFEE"),
    F.col("inMbrMatch.SBMAMTDUE").alias("SBMAMTDUE"),
    F.col("inMbrMatch.USUALNCUST").alias("USUALNCUST"),
    F.col("inMbrMatch.DENIALDTE").alias("DENIALDTE"),
    F.col("inMbrMatch.PHRDISPFEE").alias("PHRDISPFEE"),
    F.col("inMbrMatch.CLTINGRCST").alias("CLTINGRCST"),
    F.col("inMbrMatch.CLTDISPFEE").alias("CLTDISPFEE"),
    F.col("inMbrMatch.CLTSLSTAX").alias("CLTSLSTAX"),
    F.col("inMbrMatch.CLTPATPAY").alias("CLTPATPAY"),
    F.col("inMbrMatch.CLTDUEAMT").alias("CLTDUEAMT"),
    F.col("inMbrMatch.CLTFCOPAY").alias("CLTFCOPAY"),
    F.col("inMbrMatch.CLTPCOPAY").alias("CLTPCOPAY"),
    F.col("inMbrMatch.CLTCOPAY").alias("CLTCOPAY"),
    F.col("inMbrMatch.CLTINCENTV").alias("CLTINCENTV"),
    F.col("inMbrMatch.CLTATRDED").alias("CLTATRDED"),
    F.col("inMbrMatch.CLTTOTHAMT").alias("CLTTOTHAMT"),
    F.col("inMbrMatch.CLTOTHPAYA").alias("CLTOTHPAYA"),
    F.rpad(F.col("inMbrMatch.CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
    F.rpad(F.col("inMbrMatch.CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
    F.col("inMbrMatch.CLTRATE").alias("CLTRATE"),
    F.col("inMbrMatch.CLTPSCBRND").alias("CLTPSCBRND"),
    F.rpad(F.col("inMbrMatch.REIMBURSMT"), 1, " ").alias("REIMBURSMT"),
    F.rpad(F.col("inMbrMatch.CLMORIGIN"), 1, " ").alias("CLMORIGIN"),
    F.col("inMbrMatch.CLTTYPUCST").alias("CLTTYPUCST"),
    F.col("inMbrMatch.CLT2INGRCST").alias("CLT2INGRCST"),
    F.col("inMbrMatch.CLT2DISPFEE").alias("CLT2DISPFEE"),
    F.col("inMbrMatch.CLT2SLSTAX").alias("CLT2SLSTAX"),
    F.col("inMbrMatch.CLT2DUEAMT").alias("CLT2DUEAMT"),
    F.col("inMbrMatch.CLT2PSTAX").alias("CLT2PSTAX"),
    F.col("inMbrMatch.CLT2FSTAX").alias("CLT2FSTAX"),
    F.rpad(F.col("inMbrMatch.CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
    F.rpad(F.col("inMbrMatch.CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
    F.rpad(F.col("inMbrMatch.CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE"),
    F.col("inMbrMatch.CLT2RATE").alias("CLT2RATE"),
    F.when(
        F.col("inMbrMatch.CLAIMSTS") == F.lit("X"),
        F.concat(F.col("inMbrMatch.RXCLMNBR"), F.col("inMbrMatch.CLMSEQNBR"), F.lit("R"))
    ).otherwise(F.concat(F.col("inMbrMatch.RXCLMNBR"), F.col("inMbrMatch.CLMSEQNBR"))).alias("CLM_ID"),
    F.when(
        F.col("MBR_lkup.MBR_UNIQ_KEY").isNull(),
        F.lit(0)
    ).otherwise(F.col("MBR_lkup.MBR_UNIQ_KEY")).alias("ORIG_MBR"),
    F.when(
        F.col("inMbrMatch.ACCOUNTID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("MBR_lkup.GRP_ID")).alias("GRP_ID"),
    F.rpad(F.col("inMbrMatch.PPRSFSTNME"), 15, " ").alias("PPRSFSTNME"),
    F.rpad(F.col("inMbrMatch.PPRSLSTNME"), 25, " ").alias("PPRSLSTNME"),
    F.rpad(F.col("inMbrMatch.PPRSTATE"), 3, " ").alias("PPRSTATE"),
    F.col("inMbrMatch.CLT2OTHAMT").alias("CLT2OTHAMT"),
    F.col("inMbrMatch.CALINCENTV2").alias("CALINCENTV2"),
    F.rpad(F.col("inMbrMatch.RXNETPRCQL"), 1, " ").alias("RXNETPRCQL"),
    F.rpad(F.col("inMbrMatch.SBMPATRESD"), 2, " ").alias("SBMPATRESD"),
    F.rpad(F.col("inMbrMatch.PRODTYPCDE"), 2, " ").alias("PRODTYPCDE"),
    F.rpad(F.col("inMbrMatch.CONTHERAPY"), 1, " ").alias("CONTHERAPY"),
    F.rpad(F.col("inMbrMatch.CTSCHEDID"), 20, " ").alias("CTSCHEDID"),
    F.col("inMbrMatch.CLTPRODSEL").alias("CLTPRODSEL"),
    F.col("inMbrMatch.CLTATRTAX").alias("CLTATRTAX"),
    F.col("inMbrMatch.CLTPROCFEE").alias("CLTPROCFEE"),
    F.col("inMbrMatch.CLTPRVNSEL").alias("CLTPRVNSEL"),
    F.rpad(F.col("inMbrMatch.CLIENTDEF1"), 1, " ").alias("CLIENTDEF1"),
    F.rpad(F.col("inMbrMatch.GROUPPLAN"), 10, " ").alias("GROUPPLAN"),
    F.rpad(F.col("inMbrMatch.FMSTIER"), 2, " ").alias("FMSTIER"),
    F.rpad(F.col("inMbrMatch.CLNTDEF2"), 10, " ").alias("CLNTDEF2"),
    F.col("inMbrMatch.INDDEDPTD").alias("INDDEDPTD"),
    F.col("inMbrMatch.FAMDEDPTD").alias("FAMDEDPTD"),
    F.col("inMbrMatch.INDOOPPTD").alias("INDOOPPTD"),
    F.col("inMbrMatch.FAMOOPPTD").alias("FAMOOPPTD"),
    F.rpad(F.col("inMbrMatch.CLIENTDEF3"), 10, " ").alias("CLIENTDEF3"),
    F.rpad(F.col("inMbrMatch.CCTRESERV8"), 10, " ").alias("CCTRESERV8"),
    F.rpad(F.col("inMbrMatch.CCTRESERV9"), 20, " ").alias("CCTRESERV9"),
    F.rpad(F.col("inMbrMatch.CCTRESRV10"), 20, " ").alias("CCTRESRV10"),
    F.col("inMbrMatch.OOPAPPLIED").alias("OOPAPPLIED"),
    F.rpad(F.col("inMbrMatch.PLANDRUGST"), 1, " ").alias("PLANDRUGST"),
    F.rpad(F.col("inMbrMatch.PLANTYPE"), 8, " ").alias("PLANTYPE")
)

# Next handle aggregator => PrepAuditFile => Drug_Header_Record
# "PrepAuditFile" input is df_CntByAdjDtRecType with columns: CLAIMSTS, RCRD_CNT, TOT_AMT, DATESBM
df_PrepAuditFile = df_CntByAdjDtRecType.select(
    F.lit(RunDate).alias("Run_Date"),
    F.rpad(F.lit(""), 2, " ").alias("Filler1"),
    F.lit("DAILY").alias("Frequency"),
    F.lit("*  ").alias("Filler2"),
    F.concat(F.lit("ORX"), F.lit("-"), F.lit("COM"), F.lit("-"), F.col("CLAIMSTS")).alias("Program"),
    F.col("DATESBM").alias("File_date"),
    F.col("RCRD_CNT").cast(StringType()).alias("Row_Count"),
    F.col("TOT_AMT").cast(StringType()).alias("Pd_amount")
)

# Finally write Drug_Header_Record (append) to "landing"
# "Drug_Header_Record.dat"
write_files(
    df_PrepAuditFile,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunDate = get_widget_value('RunDate','')
InFile = get_widget_value('InFile','')
MbrTermDate = get_widget_value('MbrTermDate','')
RunCycle = get_widget_value('RunCycle','')

df_extract = df_Sequential_File_460

df_with_stage_vars = (
    df_extract
    .withColumn("svServicingProviderIDisNABP",
        F.when(F.trim(F.col("SRVPROVIDQ")) == "07", "Y").otherwise("N")
    )
    .withColumn("svServicingProviderIDisNPI",
        F.when(
            (F.trim(F.col("SRVPROVIDQ")) == "01") & (ProviderNPIValidator(F.col("SRVPROVID")) == "Y"),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svServicingProviderNCPDP",
        F.when(F.trim(F.col("PRVNCPDPID")).cast("long") > 0, "Y").otherwise("N")
    )
    .withColumn("svNPIProvhasNPI",
        F.when(ProviderNPIValidator(F.col("NPIPROV")) == "Y", "Y").otherwise("N")
    )
    .withColumn("svPrescribingIDisDEA",
        F.when(
            (F.trim(F.col("PRESCRIDQL")) == "12") & (ProviderDEAValidator(F.col("PRESCRIBER")) == "Y"),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svPrescribingIDisNPI",
        F.when(
            (F.trim(F.col("PRESCRIDQL")) == "01") & (ProviderNPIValidator(F.col("PRESCRIBER")) == "Y"),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svPrescribingDEAIDisDEA",
        F.when(ProviderDEAValidator(F.col("PRESCDEAID")) == "Y", "Y").otherwise("N")
    )
    .withColumn("svNPIPresciberHasNPI",
        F.when(ProviderNPIValidator(F.col("NPIPRESCR")) == "Y", "Y").otherwise("N")
    )
    .withColumn("svDEAsMatchTest",
        F.when(
            (F.col("svPrescribingIDisDEA") == "Y") &
            (F.col("svPrescribingDEAIDisDEA") == "Y") &
            (F.trim(F.col("PRESCRIBER")) == F.trim(F.col("PRESCDEAID"))),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svServiceIDToUse",
        F.when(
            F.col("svServicingProviderIDisNABP") == "Y",
            F.col("SRVPROVID")
        ).when(
            F.col("svServicingProviderNCPDP") == "Y",
            F.col("PRVNCPDPID")
        ).otherwise("NA")
    )
    .withColumn("svServiceNPIToUse",
        F.when(
            F.col("svServicingProviderIDisNPI") == "Y",
            F.trim(F.col("SRVPROVID"))
        ).when(
            F.col("svNPIProvhasNPI") == "Y",
            F.trim(F.col("NPIPROV"))
        ).otherwise("NA")
    )
    .withColumn("svPrescriberDEAtoUse",
        F.when(
            F.col("svPrescribingIDisDEA") == "Y",
            F.trim(F.col("PRESCRIBER"))
        ).when(
            F.col("svPrescribingDEAIDisDEA") == "Y",
            F.trim(F.col("PRESCDEAID"))
        ).otherwise("NA")
    )
    .withColumn("svPrescriberNPItoUse",
        F.when(
            F.col("svPrescribingIDisNPI") == "Y",
            F.trim(F.col("PRESCRIBER"))
        ).when(
            F.col("svNPIPresciberHasNPI") == "Y",
            F.trim(F.col("NPIPRESCR"))
        ).otherwise("NA")
    )
    .withColumn("svSellClientIngredientCosts",
        F.when(F.isnull(F.col("CLTINGRCST")), F.lit(0.00)).otherwise(F.col("CLTINGRCST").cast("double"))
    )
    .withColumn("svSellClientDispenseFee",
        F.when(F.isnull(F.col("CLTDISPFEE")), F.lit(0.00)).otherwise(F.col("CLTDISPFEE").cast("double"))
    )
    .withColumn("svSellClientSalesTax",
        F.when(F.isnull(F.col("CLTSLSTAX")), F.lit(0.00)).otherwise(F.col("CLTSLSTAX").cast("double"))
    )
    .withColumn("svSellClientIncentive",
        F.when(F.isnull(F.col("CLTINCENTV")), F.lit(0.00)).otherwise(F.col("CLTINCENTV").cast("double"))
    )
    .withColumn("svSellClientOtherAmt",
        F.when(F.isnull(F.col("CLTTOTHAMT")), F.lit(0.00)).otherwise(F.col("CLTTOTHAMT").cast("double"))
    )
    .withColumn("svSellClientOtherPayAmt",
        F.when(F.isnull(F.col("CLTOTHPAYA")), F.lit(0.00)).otherwise(F.col("CLTOTHPAYA").cast("double"))
    )
    .withColumn("svSellClientPatientPaid",
        F.when(F.isnull(F.col("CLTPATPAY")), F.lit(0.00)).otherwise(F.col("CLTPATPAY").cast("double"))
    )
    .withColumn("svSellClientDueAmount",
        F.when(F.isnull(F.col("CLTDUEAMT")), F.lit(0.00)).otherwise(F.col("CLTDUEAMT").cast("double"))
    )
    .withColumn("svSellDeriveActualPaidPerFields",
        (
          F.col("svSellClientIngredientCosts")+
          F.col("svSellClientDispenseFee")+
          F.col("svSellClientSalesTax")+
          F.col("svSellClientOtherAmt")+
          F.col("svSellClientIncentive")
        ) - (
          F.col("svSellClientOtherPayAmt")+
          F.col("svSellClientPatientPaid")
        )
    )
    .withColumn("svDoesActualPaidEqClientDueAmt",
        F.when(
            F.col("svSellDeriveActualPaidPerFields") == F.col("svSellClientDueAmount"),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svBuyClientIngredientCosts",
        F.when(F.isnull(F.col("CLT2INGRCST")), F.lit(0.00)).otherwise(F.col("CLT2INGRCST").cast("double"))
    )
    .withColumn("svBuyClientDispenseFee",
        F.when(F.isnull(F.col("CLT2DISPFEE")), F.lit(0.00)).otherwise(F.col("CLT2DISPFEE").cast("double"))
    )
    .withColumn("svBuyClientPSTax",
        F.when(F.isnull(F.col("CLT2PSTAX")), F.lit(0.00)).otherwise(F.col("CLT2PSTAX").cast("double"))
    )
    .withColumn("svBuyClientFSTax",
        F.when(F.isnull(F.col("CLT2FSTAX")), F.lit(0.00)).otherwise(F.col("CLT2FSTAX").cast("double"))
    )
    .withColumn("svBuyClientIncentive",
        F.when(F.isnull(F.col("CALINCENTV2")), F.lit(0.00)).otherwise(F.col("CALINCENTV2").cast("double"))
    )
    .withColumn("svBuyClientOtherAmt",
        F.when(F.isnull(F.col("CLT2OTHAMT")), F.lit(0.00)).otherwise(F.col("CLT2OTHAMT").cast("double"))
    )
    .withColumn("svBuyClientDueAmount",
        F.when(F.isnull(F.col("CLT2DUEAMT")), F.lit(0.00)).otherwise(F.col("CLT2DUEAMT").cast("double"))
    )
    .withColumn("svBuyDeriveActualPaidPerFields",
        (
          F.col("svBuyClientIngredientCosts")+
          F.col("svBuyClientDispenseFee")+
          F.col("svBuyClientPSTax")+
          F.col("svBuyClientFSTax")+
          F.col("svBuyClientIncentive")+
          F.col("svBuyClientOtherAmt")
        ) - (
          F.col("svSellClientOtherPayAmt")+
          F.col("svSellClientPatientPaid")
        )
    )
    .withColumn("svDoesBuyActualPaidEqClientDueAmt",
        F.when(
            F.col("svBuyDeriveActualPaidPerFields") == F.col("svBuyClientDueAmount"),
            "Y"
        ).otherwise("N")
    )
)

df_paid_reversal = df_with_stage_vars.filter(
    (F.col("CLAIMSTS") == "P") | (F.col("CLAIMSTS") == "X")
)
df_denied_claims = df_with_stage_vars.filter(
    F.col("CLAIMSTS") == "R"
)

df_accum_all = df_with_stage_vars  # For whichever links feed accum or additional outputs

df_landing_file_paid_reversal = df_paid_reversal.select(
    rpad(F.lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
    rpad(F.col("DATESBM"), 10, " ").alias("FILE_RCVD_DT"),
    F.lit(None).alias("RCRD_ID"),
    F.lit(None).alias("PRCSR_NO"),
    F.lit(None).alias("BTCH_NO"),
    F.col("svServiceIDToUse").alias("PDX_NO"),
    rpad(F.col("RXNUMBER"), 12, " ").alias("RX_NO"),
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
        F.upper(F.col("MBRSEX")) == "M", F.lit(1)
    ).when(
        F.upper(F.col("MBRSEX")) == "F", F.lit(2)
    ).otherwise(F.lit(0)).alias("SEX_CD"),
    F.when(
        (F.isnull(F.col("MEMBERID")) | (F.length(F.col("MEMBERID")) == 0)),
        "UNK"
    ).when(
        F.length(F.trim(F.col("MEMBERID"))) == 11,
        F.trim(F.col("MEMBERID").substr(F.lit(1), F.lit(9)))
    ).otherwise("UNK").alias("CARDHLDR_ID_NO"),
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
    F.when(
        (F.isnull(F.col("PSC")) | (F.length(F.trim(F.col("PSC"))) == 0)),
        "UNK"
    ).otherwise(F.col("PSC")).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    F.when(
        (F.isnull(F.col("MEMBERID")) | (F.length(F.col("MEMBERID")) == 0)),
        "UNK"
    ).when(
        F.length(F.trim(F.col("MEMBERID"))) == 11,
        F.trim(F.col("MEMBERID").substr(F.lit(10), F.lit(2)))
    ).otherwise("UNK").alias("PRSN_CD"),
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
        (
          (F.isnull(F.col("GENINDOVER")) | (F.length(F.trim(F.col("GENINDOVER"))) == 0))
          & ((F.trim(F.col("MULTSRCCDE")) == "M") | (F.trim(F.col("MULTSRCCDE")) == "N"))
        ),
        "1"
    ).when(
        (
          (F.isnull(F.col("GENINDOVER")) | (F.length(F.trim(F.col("GENINDOVER"))) == 0))
          & (F.trim(F.col("MULTSRCCDE")) == "Y")
        ),
        "3"
    ).when(
        (
          (F.isnull(F.col("GENINDOVER")) | (F.length(F.trim(F.col("GENINDOVER"))) == 0))
          & (F.trim(F.col("MULTSRCCDE")) == "O")
        ),
        "5"
    ).when(
        (F.trim(F.col("GENINDOVER")) == "M") | (F.trim(F.col("GENINDOVER")) == "N"), "1"
    ).when(
        F.trim(F.col("GENINDOVER")) == "Y", "3"
    ).when(
        F.trim(F.col("GENINDOVER")) == "O", "5"
    ).otherwise("0").alias("DRUG_TYP"),
    F.when(
        (F.length(F.col("PRESLSTNME")) == 0) & (F.length(F.col("PRESFSTNME")) == 0),
        "UNK"
    ).when(
        (F.length(F.col("PRESFSTNME")) != 0) & (F.length(F.col("PRESLSTNME")) == 0),
        F.col("PRESFSTNME")
    ).when(
        (F.length(F.col("PRESFSTNME")) == 0) & (F.length(F.col("PRESLSTNME")) != 0),
        F.col("PRESLSTNME")
    ).otherwise(F.concat(F.col("PRESFSTNME"), F.lit(" "), F.col("PRESLSTNME"))).alias("PRSCRBR_LAST_NM"),
    F.lit(None).alias("POSTAGE_AMT"),
    F.lit(None).alias("UNIT_DOSE_IN"),
    F.col("CLTOTHPAYA").alias("OTHR_PAYOR_AMT"),
    F.lit(None).alias("BSS_OF_DAYS_SUPL_DTRM"),
    F.when(
        (F.col("COMPOUNDCD") == "2"),
        F.round(
            (F.col("USUALNCUST").cast("double") + F.col("CLTDISPFEE").cast("double") + F.col("CLTSLSTAX").cast("double")),
            2
        )
    ).otherwise(
        F.round(
            (F.col("AWPUNITCST").cast("double")*F.col("DECIMALQTY").cast("double"))
             + F.col("CLTDISPFEE").cast("double")
             + F.col("CLTSLSTAX").cast("double"),
            2
        )
    ).alias("FULL_AVG_WHLSL_PRICE"),
    rpad(F.lit(None), 1, " ").alias("EXPNSN_AREA"),
    F.col("CARID").alias("MSTR_CAR"),
    F.lit(None).alias("SUBCAR"),
    F.when(F.col("CLAIMSTS") == "X", "R").otherwise("P").alias("CLM_TYP"),
    F.lit(None).alias("SUBGRP"),
    rpad(F.lit(None), 1, " ").alias("PLN_DSGNR"),
    rpad(F.col("DATESBM"), 10, " ").alias("ADJDCT_DT"),
    F.lit(0.0).alias("ADMIN_FEE_AMT"),
    F.lit(0.00).alias("CAP_AMT"),
    F.col("SBMINGRCST").alias("INGR_CST_SUB_AMT"),
    F.lit(None).alias("MBR_NON_COPAY_AMT"),
    rpad(F.lit(None), 2, " ").alias("MBR_PAY_CD"),
    F.col("CLTINCENTV").alias("INCNTV_FEE_AMT"),
    F.lit(None).alias("CLM_ADJ_AMT"),
    rpad(F.lit(None), 2, " ").alias("CLM_ADJ_CD"),
    rpad(F.col("FORMLRFLAG"), 1, " ").alias("FRMLRY_FLAG"),
    F.lit(None).alias("GNRC_CLS_NO"),
    F.lit(None).alias("THRPTC_CLS_AHFS"),
    rpad(F.col("DISPROTHER"), 3, " ").alias("PDX_TYP"),
    F.when(
        (F.isnull(F.col("BASISCOST")) | (F.length(F.trim(F.col("BASISCOST"))) == 0)),
        "NA"
    ).otherwise(F.col("BASISCOST")).alias("BILL_BSS_CD"),
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
        (F.isnull(F.col("CLTPSCBRND")) | (F.length(F.trim(F.col("CLTPSCBRND"))) == 0)),
        "0.0"
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
        F.format_string("%-1s", F.col("CLIENTDEF1"))
    ).alias("CLNT_GNRL_PRPS_AREA"),
    F.when(
        (F.isnull(F.col("DISPSTATUS")) | (F.length(F.trim(F.col("DISPSTATUS"))) == 0)),
        "BLANK"
    ).otherwise(F.col("DISPSTATUS")).alias("PRTL_FILL_STTUS_CD"),
    rpad(F.lit("1753-01-01"), 10, " ").alias("BILL_DT"),
    F.lit(None).alias("FSA_VNDR_CD"),
    rpad(F.lit(None), 1, " ").alias("PICA_DRUG_CD"),
    F.when(
        (F.isnull(F.col("CLTPATPAY")) | (F.length(F.trim(F.col("CLTPATPAY"))) == 0)),
        "0.0"
    ).otherwise(F.col("CLTPATPAY")).alias("CLM_AMT"),
    F.lit(None).alias("DSALW_AMT"),
    rpad(F.lit(None), 1, " ").alias("FED_DRUG_CLS_CD"),
    F.col("CLTATRDED").alias("DEDCT_AMT"),
    rpad(F.lit(None), 1, " ").alias("BNF_COPAY_100"),
    rpad(F.lit(None), 1, " ").alias("CLM_PRCS_TYP"),
    F.lit(None).alias("INDEM_HIER_TIER_NO"),
    F.when(
        (F.col("GROUPPLAN") == "BKC1001")
        | (F.col("GROUPPLAN") == "BKC1003")
        | (F.col("GROUPPLAN") == "BKC1007")
        | (F.col("GROUPPLAN") == "BKC6001")
        | (F.col("GROUPPLAN") == "BKC6002"),
        F.col("FMSTIER")
    ).otherwise(F.lit(None)).alias("MCARE_D_COV_DRUG"),
    rpad(F.lit(None), 1, " ").alias("RETRO_LICS_CD"),
    F.lit(None).alias("RETRO_LICS_AMT"),
    F.lit(None).alias("LICS_SBSDY_AMT"),
    rpad(F.lit(None), 1, " ").alias("MCARE_B_DRUG"),
    rpad(F.lit(None), 1, " ").alias("MCARE_B_CLM"),
    rpad(F.col("PRESCRIDQL"), 2, " ").alias("PRSCRBR_QLFR"),
    F.col("svPrescriberNPItoUse").alias("PRSCRBR_NTNL_PROV_ID"),
    rpad(F.col("SRVPROVIDQ"), 2, " ").alias("PDX_QLFR"),
    F.col("svServiceNPIToUse").alias("PDX_NTNL_PROV_ID"),
    F.lit(None).alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    F.lit(None).alias("THER_CLS"),
    F.lit(None).alias("HIC_NO"),
    rpad(F.lit(None), 1, " ").alias("HLTH_RMBRMT_ARGMT_FLAG"),
    F.lit(None).alias("DOSE_CD"),
    rpad(F.lit(None), 1, " ").alias("LOW_INCM"),
    rpad(F.lit(None), 2, " ").alias("RTE_OF_ADMIN"),
    F.lit(None).alias("DEA_SCHD"),
    F.lit(None).alias("COPAY_BNF_OPT"),
    F.when(
        (F.isnull(F.col("GPINUMBER")) | (F.length(F.trim(F.col("GPINUMBER"))) == 0)),
        "NA"
    ).otherwise(F.col("GPINUMBER")).alias("GNRC_PROD_IN"),
    F.col("PRESSPCCDE").alias("PRSCRBR_SPEC"),
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
    F.lit(None).alias("PSL_FMLY_MET_AMT"),
    F.lit(None).alias("PSL_MBR_MET_AMT"),
    F.lit(None).alias("PSL_FMLY_AMT"),
    F.lit(None).alias("DEDCT_FMLY_MET_AMT"),
    F.lit(None).alias("DEDCT_FMLY_AMT"),
    F.lit(None).alias("MOPS_FMLY_AMT"),
    F.lit(None).alias("MOPS_FMLY_MET_AMT"),
    F.lit(None).alias("MOPS_MBR_MET_AMT"),
    F.lit(None).alias("DEDCT_MBR_MET_AMT"),
    F.lit(None).alias("PSL_APLD_AMT"),
    F.lit(None).alias("MOPS_APLD_AMT"),
    F.when(
        (
          (F.col("REIMBURSMT") == "M")
          & ((F.col("SBMPATRESD") == "92")
             | (F.col("SBMPATRESD") == "93")
             | (F.col("SBMPATRESD") == "98"))
        ),
        "N"
    ).otherwise("Y").alias("PAR_PDX_IN"),
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
        (F.isnull(F.col("CTSCHEDID")) | (F.length(F.trim(F.col("CTSCHEDID"))) == 0)),
        "NA"
    ).otherwise(F.col("CLTPRODSEL")).alias("CNTNGNT_THER_SCHD"),
    F.when(
        (F.isnull(F.col("CLTPRODSEL")) | (F.length(F.trim(F.col("CLTPRODSEL"))) == 0)),
        F.lit(0.00)
    ).otherwise(F.col("CLTPRODSEL").cast("double")).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.when(
        (F.isnull(F.col("CLTATRTAX")) | (F.length(F.trim(F.col("CLTATRTAX"))) == 0)),
        F.lit(0.00)
    ).otherwise(F.col("CLTATRTAX").cast("double")).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.when(
        (F.isnull(F.col("CLTPROCFEE")) | (F.length(F.trim(F.col("CLTPROCFEE"))) == 0)),
        F.lit(0.00)
    ).otherwise(F.col("CLTPROCFEE").cast("double")).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.when(
        (F.isnull(F.col("CLTPRVNSEL")) | (F.length(F.trim(F.col("CLTPRVNSEL"))) == 0)),
        F.lit(0.00)
    ).otherwise(F.col("CLTPRVNSEL").cast("double")).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.lit("COM").alias("LOB_IN")
)

write_files(
    df_landing_file_paid_reversal,
    f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_landing_file_denied = df_denied_claims.select(
    rpad(F.col("MEMBERID"), 20, " ").alias("MEMBERID"),
    rpad(F.col("ACCOUNTID"), 15, " ").alias("ACCOUNTID"),
    rpad(F.col("NPIPROV"), 10, " ").alias("NPIPROV"),
    rpad(F.col("DENIALDTE"), 8, " ").alias("DENIALDTE"),
    rpad(F.col("RXNUMBER"), 12, " ").alias("RXNUMBER"),
    rpad(F.col("DATESBM"), 8, " ").alias("DATESBM"),
    rpad(F.col("TIMESBM"), 8, " ").alias("TIMESBM"),
    rpad(F.lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
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
            (F.length(F.trim(F.col("PRESCRIBER"))) != 0) &
            (F.trim(F.col("PRESCRIDQL")) == "12"),
            F.col("PRESCRIBER")
        ).when(
            F.length(F.trim(F.col("PRESCDEAID"))) != 0,
            F.trim(F.col("PRESCDEAID"))
        ).otherwise("NA"),
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
    F.lit("COM").alias("LOB_IN")
)

write_files(
    df_landing_file_denied,
    f"{adls_path}/verified/{SrcSysCd}_DeniedClaims_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_load_enr = df_paid_reversal.select(
    F.col("CLM_ID").alias("CLM_ID"),
    rpad(F.col("DTEFILLED"), 10, " ").alias("FILL_DT_SK"),
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY")
)

write_files(
    df_load_enr,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_load_pcp = df_paid_reversal.select(
    F.col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
    rpad(F.col("DTEFILLED"), 10, " ").alias("FILL_DT_SK")
)

write_files(
    df_load_pcp,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_load_drugclmprice = df_paid_reversal.select(
    rpad(F.lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
    F.col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),  # Overwritten below if needed
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("RXCLMNBR").alias("RXCLMNBR"),
    F.col("CLMSEQNBR").alias("CLMSEQNBR"),
    rpad(F.col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
    F.col("ORGPDSBMDT").alias("ORGPDSBMDT"),
    F.col("METRICQTY").alias("METRICQTY"),
    F.when(
        (F.isnull(F.col("GPINUMBER")) | (F.length(F.trim(F.col("GPINUMBER"))) == 0)),
        "NA"
    ).otherwise(F.col("GPINUMBER")).alias("GPINUMBER"),
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
).withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK))

write_files(
    df_load_drugclmprice,
    f"{adls_path}/verified/DrugClmPrice_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_buy_validation = df_with_stage_vars.filter(
    ((F.col("CLAIMSTS") == "P") | (F.col("CLAIMSTS") == "X"))
    & (F.col("svDoesBuyActualPaidEqClientDueAmt") == "N")
).select(
    rpad(F.lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
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

write_files(
    df_buy_validation,
    f"{adls_path}/verified/Optum_BuyPriceValidation_{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_sell_validation = df_with_stage_vars.filter(
    ((F.col("CLAIMSTS") == "P") | (F.col("CLAIMSTS") == "X"))
    & (F.col("svDoesActualPaidEqClientDueAmt") == "N")
).select(
    rpad(F.lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
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

write_files(
    df_sell_validation,
    f"{adls_path}/verified/Optum_SellPriceValidation_{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_load_drug_clm_accum = df_with_stage_vars.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(
        F.when(
            (F.isnull(F.col("CLIENTDEF3"))) | (F.length(F.trim(F.col("CLIENTDEF3"))) == 0),
            "NA"
        ).otherwise(F.trim(F.col("CLIENTDEF3"))),
        10, " "
    ).alias("CLIENTDEF3"),
    F.when(
        (F.isnull(F.col("CCTRESRV10"))) | (F.length(F.trim(F.col("CCTRESRV10"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CCTRESRV10").cast("double")).alias("CCAA_COUPON_AMT"),
    F.when(
        (F.isnull(F.col("FAMDEDPTD"))) | (F.length(F.trim(F.col("FAMDEDPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("FAMDEDPTD").cast("double")).alias("FMLY_ACCUM_DEDCT_AMT"),
    F.when(
        (F.isnull(F.col("FAMOOPPTD"))) | (F.length(F.trim(F.col("FAMOOPPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("FAMOOPPTD").cast("double")).alias("FMLY_ACCUM_OOP_AMT"),
    F.when(
        (F.isnull(F.col("INDDEDPTD"))) | (F.length(F.trim(F.col("INDDEDPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("INDDEDPTD").cast("double")).alias("INDV_ACCUM_DEDCT_AMT"),
    F.when(
        (F.isnull(F.col("INDOOPPTD"))) | (F.length(F.trim(F.col("INDOOPPTD"))) == 0),
        F.lit(0)
    ).otherwise(F.col("INDOOPPTD").cast("double")).alias("INDV_ACCUM_OOP_AMT"),
    F.when(
        (F.isnull(F.col("CCTRESERV9"))) | (F.length(F.trim(F.col("CCTRESERV9"))) == 0),
        F.lit(0)
    ).otherwise(F.col("CCTRESERV9").cast("double")).alias("INDV_APLD_DEDCT_AMT"),
    F.when(
        (F.isnull(F.col("OOPAPPLIED"))) | (F.length(F.trim(F.col("OOPAPPLIED"))) == 0),
        F.lit(0)
    ).otherwise(F.col("OOPAPPLIED").cast("double")).alias("INDV_APLD_OOP_AMT")
)

write_files(
    df_load_drug_clm_accum,
    f"{adls_path}/load/DRUG_CLM_ACCUM_IMPCT_prep.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)