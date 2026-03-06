# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
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
# MAGIC      
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Sri Nannapaneni               2020-07-15     PBM Phase II                               Initial Programming                                                                                              IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2021-05-07

# MAGIC Recon between optum claim detail and optum claim control file @ groupid and amounts level
# MAGIC Need to remove total line from last
# MAGIC Recon between optum claim detail and optum claim control file @ full rollup amounts level
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    when,
    length,
    lit,
    sum as F_sum,
    count as F_count
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
RunDate = get_widget_value('RunDate','')
InFile = get_widget_value('InFile','')
ControlInFile = get_widget_value('ControlInFile','')
MbrTermDate = get_widget_value('MbrTermDate','')

# Read from OptumDetail_Seq (landing)
schema_OptumDetail_Seq = StructType([
    StructField("RXCLMNBR", DoubleType(), True),
    StructField("CLMSEQNBR", DoubleType(), True),
    StructField("CLAIMSTS", StringType(), True),
    StructField("CARID", StringType(), True),
    StructField("SCARID", StringType(), True),
    StructField("CARRPROC", DoubleType(), True),
    StructField("CLNTID", StringType(), True),
    StructField("CLNTSGMNT", StringType(), True),
    StructField("CLNTREGION", StringType(), True),
    StructField("ACCOUNTID", StringType(), True),
    StructField("ACCTBENCDE", StringType(), True),
    StructField("VERSIONNBR", StringType(), True),
    StructField("GROUPID", StringType(), True),
    StructField("GROUPPLAN", StringType(), True),
    StructField("GRPCLIBENF", StringType(), True),
    StructField("GROUPSIC", StringType(), True),
    StructField("CLMRESPSTS", StringType(), True),
    StructField("MEMBERID", StringType(), True),
    StructField("MBRLSTNME", StringType(), True),
    StructField("MBRFSTNME", StringType(), True),
    StructField("MBRMDINIT", StringType(), True),
    StructField("MBRPRSNCDE", StringType(), True),
    StructField("MBRRELCDE", StringType(), True),
    StructField("MBRSEX", StringType(), True),
    StructField("MBRBIRTH", DoubleType(), True),
    StructField("MBRAGE", DoubleType(), True),
    StructField("MBRZIP", StringType(), True),
    StructField("SOCSECNBR", StringType(), True),
    StructField("DURKEY", StringType(), True),
    StructField("DURFLAG", StringType(), True),
    StructField("MBRFAMLYID", StringType(), True),
    StructField("MBRFAMLIND", StringType(), True),
    StructField("MBRFAMLTYP", StringType(), True),
    StructField("COBIND", StringType(), True),
    StructField("MBRPLAN", StringType(), True),
    StructField("MBRPRODCDE", StringType(), True),
    StructField("MBRRIDERCD", StringType(), True),
    StructField("CARENETID", StringType(), True),
    StructField("CAREQUALID", StringType(), True),
    StructField("CAREFACID", StringType(), True),
    StructField("CAREFACNAM", StringType(), True),
    StructField("MBRPCPHYS", StringType(), True),
    StructField("PPRSFSTNME", StringType(), True),
    StructField("PPRSLSTNME", StringType(), True),
    StructField("PPRSMDINIT", StringType(), True),
    StructField("PPRSSPCCDE", StringType(), True),
    StructField("PPRSTATE", StringType(), True),
    StructField("MBRALTINFL", StringType(), True),
    StructField("MBRALTINCD", StringType(), True),
    StructField("MBRALTINID", StringType(), True),
    StructField("MBRMEDDTE", DoubleType(), True),
    StructField("MBRMEDTYPE", StringType(), True),
    StructField("MBRHICCDE", StringType(), True),
    StructField("CARDHOLDER", StringType(), True),
    StructField("PATLASTNME", StringType(), True),
    StructField("PATFRSTNME", StringType(), True),
    StructField("PERSONCDE", StringType(), True),
    StructField("RELATIONCD", StringType(), True),
    StructField("SEXCODE", StringType(), True),
    StructField("BIRTHDTE", DoubleType(), True),
    StructField("ELIGCLARIF", StringType(), True),
    StructField("CUSTLOC", StringType(), True),
    StructField("SBMPLSRVCE", StringType(), True),
    StructField("SBMPATRESD", StringType(), True),
    StructField("PRMCAREPRV", StringType(), True),
    StructField("PRMCAREPRQ", StringType(), True),
    StructField("FACILITYID", StringType(), True),
    StructField("OTHCOVERAG", StringType(), True),
    StructField("BINNUMBER", StringType(), True),
    StructField("PROCESSOR", StringType(), True),
    StructField("GROUPNBR", StringType(), True),
    StructField("TRANSCDE", StringType(), True),
    StructField("DATESBM", DoubleType(), True),
    StructField("TIMESBM", DoubleType(), True),
    StructField("CHGDATE", DoubleType(), True),
    StructField("CHGTIME", DoubleType(), True),
    StructField("ORGPDSBMDT", DoubleType(), True),
    StructField("RVDATESBM", DoubleType(), True),
    StructField("CLMCOUNTER", DoubleType(), True),
    StructField("GENERICCTR", DoubleType(), True),
    StructField("FORMLRYCTR", DoubleType(), True),
    StructField("RXNUMBER", StringType(), True),
    StructField("RXNUMBERQL", StringType(), True),
    StructField("REFILL", StringType(), True),
    StructField("DISPSTATUS", StringType(), True),
    StructField("DTEFILLED", DoubleType(), True),
    StructField("COMPOUNDCD", StringType(), True),
    StructField("SBMCMPDTYP", StringType(), True),
    StructField("PRODTYPCDE", StringType(), True),
    StructField("PRODUCTID", StringType(), True),
    StructField("PRODUCTKEY", DoubleType(), True),
    StructField("METRICQTY", DoubleType(), True),
    StructField("DECIMALQTY", DoubleType(), True),
    StructField("DAYSSUPPLY", DoubleType(), True),
    StructField("PSC", StringType(), True),
    StructField("WRITTENDTE", DoubleType(), True),
    StructField("NBRFLSAUTH", DoubleType(), True),
    StructField("ORIGINCDE", StringType(), True),
    StructField("SBMCLARCD1", StringType(), True),
    StructField("SBMCLARCD2", StringType(), True),
    StructField("SBMCLARCD3", StringType(), True),
    StructField("PAMCNBR", StringType(), True),
    StructField("PAMCCDE", StringType(), True),
    StructField("PRAUTHNBR", StringType(), True),
    StructField("PRAUTHRSN", StringType(), True),
    StructField("PRAUTHFDTE", DoubleType(), True),
    StructField("PRAUTHTDTE", DoubleType(), True),
    StructField("LABELNAME", StringType(), True),
    StructField("PRODNM", StringType(), True),
    StructField("DRUGMFGRID", StringType(), True),
    StructField("DRUGMFGR", StringType(), True),
    StructField("GPINUMBER", StringType(), True),
    StructField("GNRCNME", StringType(), True),
    StructField("PRDPACUOM", StringType(), True),
    StructField("PRDPACSIZE", DoubleType(), True),
    StructField("DDID", DoubleType(), True),
    StructField("GCN", DoubleType(), True),
    StructField("GCNSEQ", DoubleType(), True),
    StructField("KDC", DoubleType(), True),
    StructField("AHFS", StringType(), True),
    StructField("DRUGDEACOD", StringType(), True),
    StructField("RXOTCIND", StringType(), True),
    StructField("MULTSRCCDE", StringType(), True),
    StructField("GENINDOVER", StringType(), True),
    StructField("PRDREIMIND", StringType(), True),
    StructField("BRNDTRDNME", StringType(), True),
    StructField("FDATHERAEQ", StringType(), True),
    StructField("METRICSTRG", DoubleType(), True),
    StructField("DRGSTRGUOM", StringType(), True),
    StructField("ADMINROUTE", StringType(), True),
    StructField("ADMINRTESN", StringType(), True),
    StructField("DOSAGEFORM", StringType(), True),
    StructField("NDA", StringType(), True),
    StructField("ANDA", StringType(), True),
    StructField("ANDAOR", StringType(), True),
    StructField("RXNORMCODE", StringType(), True),
    StructField("MNTDRUGCDE", StringType(), True),
    StructField("MNTSOURCE", StringType(), True),
    StructField("MNTCARPROR", StringType(), True),
    StructField("MNTGPILIST", StringType(), True),
    StructField("CPQSPCPRG", StringType(), True),
    StructField("CPQSPCPGIN", StringType(), True),
    StructField("CPQSPCSCHD", StringType(), True),
    StructField("THRDPARTYX", StringType(), True),
    StructField("DRGUNITDOS", StringType(), True),
    StructField("SBMUNITDOS", StringType(), True),
    StructField("ALTPRODTYP", StringType(), True),
    StructField("ALTPRODCDE", StringType(), True),
    StructField("SRXNETWRK", StringType(), True),
    StructField("SRXNETTYPE", StringType(), True),
    StructField("RXNETWORK", StringType(), True),
    StructField("RXNETWRKNM", StringType(), True),
    StructField("RXNETCARR", StringType(), True),
    StructField("RXNETWRKQL", StringType(), True),
    StructField("RXNETPRCQL", StringType(), True),
    StructField("REGIONCDE", StringType(), True),
    StructField("PHRAFFIL", StringType(), True),
    StructField("NETPRIOR", StringType(), True),
    StructField("NETTYPE", StringType(), True),
    StructField("NETSEQ", DoubleType(), True),
    StructField("PAYCNTR", StringType(), True),
    StructField("PHRNDCLST", StringType(), True),
    StructField("PHRGPILST", StringType(), True),
    StructField("SRVPROVID", StringType(), True),
    StructField("SRVPROVIDQ", StringType(), True),
    StructField("NPIPROV", StringType(), True),
    StructField("PRVNCPDPID", StringType(), True),
    StructField("SBMSRVPRID", StringType(), True),
    StructField("SBMSRVPRQL", StringType(), True),
    StructField("SRVPROVNME", StringType(), True),
    StructField("PROVLOCKQL", StringType(), True),
    StructField("PROVLOCKID", StringType(), True),
    StructField("STORENBR", StringType(), True),
    StructField("AFFILIATIN", StringType(), True),
    StructField("PAYEEID", StringType(), True),
    StructField("DISPRCLASS", StringType(), True),
    StructField("DISPROTHER", StringType(), True),
    StructField("PHARMZIP", StringType(), True),
    StructField("PRESNETWID", StringType(), True),
    StructField("PRESCRIBER", StringType(), True),
    StructField("PRESCRIDQL", StringType(), True),
    StructField("NPIPRESCR", StringType(), True),
    StructField("PRESCDEAID", StringType(), True),
    StructField("PRESLSTNME", StringType(), True),
    StructField("PRESFSTNME", StringType(), True),
    StructField("PRESMDINIT", StringType(), True),
    StructField("PRESSPCCDE", StringType(), True),
    StructField("PRESSPCCDQ", StringType(), True),
    StructField("PRSSTATE", StringType(), True),
    StructField("SBMRPHID", StringType(), True),
    StructField("SBMRPHIDQL", StringType(), True),
    StructField("FNLPLANCDE", StringType(), True),
    StructField("FNLPLANDTE", DoubleType(), True),
    StructField("PLANTYPE", StringType(), True),
    StructField("PLANQUAL", StringType(), True),
    StructField("PLNNDCLIST", StringType(), True),
    StructField("LSTQUALNDC", StringType(), True),
    StructField("PLNGPILIST", StringType(), True),
    StructField("LSTQUALGPI", StringType(), True),
    StructField("PLNPNDCLST", StringType(), True),
    StructField("PLNPGPILST", StringType(), True),
    StructField("PLANDRUGST", StringType(), True),
    StructField("PLANFRMLRY", StringType(), True),
    StructField("PLNFNLPSCH", StringType(), True),
    StructField("PHRDCSCHID", StringType(), True),
    StructField("PHRDCSCHSQ", DoubleType(), True),
    StructField("CLTDCSCHID", StringType(), True),
    StructField("CLTDCSCHSQ", DoubleType(), True),
    StructField("PHRDCCSCID", StringType(), True),
    StructField("PHRDCCSCSQ", DoubleType(), True),
    StructField("CLTDCCSCID", StringType(), True),
    StructField("CLTDCCSCSQ", DoubleType(), True),
    StructField("PHRPRTSCID", StringType(), True),
    StructField("CLTPRTSCID", StringType(), True),
    StructField("PHRRMSCHID", StringType(), True),
    StructField("CLTRMSCHID", StringType(), True),
    StructField("PRDPFLSTID", StringType(), True),
    StructField("PRFPRDSCID", StringType(), True),
    StructField("FORMULARY", StringType(), True),
    StructField("FORMLRFLAG", StringType(), True),
    StructField("TIERVALUE", DoubleType(), True),
    StructField("CONTHERAPY", StringType(), True),
    StructField("CTSCHEDID", StringType(), True),
    StructField("PRBASISCHD", StringType(), True),
    StructField("REGDISOR", StringType(), True),
    StructField("DRUGSTSTBL", StringType(), True),
    StructField("TRANSBEN", StringType(), True),
    StructField("MESSAGECD1", StringType(), True),
    StructField("MESSAGE1", StringType(), True),
    StructField("MESSAGECD2", StringType(), True),
    StructField("MESSAGE2", StringType(), True),
    StructField("MESSAGECD3", StringType(), True),
    StructField("MESSAGE3", StringType(), True),
    StructField("REJCNT", DoubleType(), True),
    StructField("REJCDE1", StringType(), True),
    StructField("REJCDE2", StringType(), True),
    StructField("REJCDE3", StringType(), True),
    StructField("RJCPLANID", StringType(), True),
    StructField("DURCONFLCT", StringType(), True),
    StructField("DURINTERVN", StringType(), True),
    StructField("DUROUTCOME", StringType(), True),
    StructField("LVLSERVICE", StringType(), True),
    StructField("PHARSRVTYP", StringType(), True),
    StructField("DIAGNOSIS", StringType(), True),
    StructField("DIAGNOSISQ", StringType(), True),
    StructField("RVDURCNFLC", StringType(), True),
    StructField("RVDURINTRV", StringType(), True),
    StructField("RVDUROUTCM", StringType(), True),
    StructField("RVLVLSERVC", StringType(), True),
    StructField("DRGCNFLCT1", StringType(), True),
    StructField("SEVERITY1", StringType(), True),
    StructField("OTHRPHARM1", StringType(), True),
    StructField("DTEPRVFIL1", DoubleType(), True),
    StructField("QTYPRVFIL1", DoubleType(), True),
    StructField("DATABASE1", StringType(), True),
    StructField("OTHRPRESC1", StringType(), True),
    StructField("FREETEXT1", StringType(), True),
    StructField("DRGCNFLCT2", StringType(), True),
    StructField("SEVERITY2", StringType(), True),
    StructField("OTHRPHARM2", StringType(), True),
    StructField("DTEPRVFIL2", DoubleType(), True),
    StructField("QTYPRVFIL2", DoubleType(), True),
    StructField("DATABASE2", StringType(), True),
    StructField("OTHRPRESC2", StringType(), True),
    StructField("FREETEXT2", StringType(), True),
    StructField("DRGCNFLCT3", StringType(), True),
    StructField("SEVERITY3", StringType(), True),
    StructField("OTHRPHARM3", StringType(), True),
    StructField("DTEPRVFIL3", DoubleType(), True),
    StructField("QTYPRVFIL3", DoubleType(), True),
    StructField("DATABASE3", StringType(), True),
    StructField("OTHRPRESC3", StringType(), True),
    StructField("FREETEXT3", StringType(), True),
    StructField("FEETYPE", StringType(), True),
    StructField("BENUNITCST", DoubleType(), True),
    StructField("AWPUNITCST", DoubleType(), True),
    StructField("WACUNITCST", DoubleType(), True),
    StructField("GEAPUNTCST", DoubleType(), True),
    StructField("CTYPEUCOST", DoubleType(), True),
    StructField("BASISCOST", StringType(), True),
    StructField("PRICEQTY", DoubleType(), True),
    StructField("PRODAYSSUP", DoubleType(), True),
    StructField("PROQTY", DoubleType(), True),
    StructField("RVINCNTVSB", DoubleType(), True),
    StructField("SBMINGRCST", DoubleType(), True),
    StructField("SBMDISPFEE", DoubleType(), True),
    StructField("SBMPSLSTX", DoubleType(), True),
    StructField("SBMFSLSTX", DoubleType(), True),
    StructField("SBMSLSTX", DoubleType(), True),
    StructField("SBMPATPAY", DoubleType(), True),
    StructField("SBMAMTDUE", DoubleType(), True),
    StructField("SBMINCENTV", DoubleType(), True),
    StructField("SBMPROFFEE", DoubleType(), True),
    StructField("SBMTOTHAMT", DoubleType(), True),
    StructField("SBMOPAMTCT", DoubleType(), True),
    StructField("SBMOPAMTQL", StringType(), True),
    StructField("USUALNCUST", DoubleType(), True),
    StructField("DENIALDTE", DoubleType(), True),
    StructField("OTHRPAYOR", DoubleType(), True),
    StructField("SBMMDPDAMT", DoubleType(), True),
    StructField("CALINGRCST", DoubleType(), True),
    StructField("CALDISPFEE", DoubleType(), True),
    StructField("CALPSTAX", DoubleType(), True),
    StructField("CALFSTAX", DoubleType(), True),
    StructField("CALSLSTAX", DoubleType(), True),
    StructField("CALPATPAY", DoubleType(), True),
    StructField("CALDUEAMT", DoubleType(), True),
    StructField("CALWITHHLD", DoubleType(), True),
    StructField("CALFCOPAY", DoubleType(), True),
    StructField("CALPCOPAY", DoubleType(), True),
    StructField("CALCOPAY", DoubleType(), True),
    StructField("CALPRODSEL", DoubleType(), True),
    StructField("CALATRTAX", DoubleType(), True),
    StructField("CALEXCEBFT", DoubleType(), True),
    StructField("CALINCENTV", DoubleType(), True),
    StructField("CALATRDED", DoubleType(), True),
    StructField("CALCOB", DoubleType(), True),
    StructField("CALTOTHAMT", DoubleType(), True),
    StructField("CALPROFFEE", DoubleType(), True),
    StructField("CALOTHPAYA", DoubleType(), True),
    StructField("CALCOSTSRC", StringType(), True),
    StructField("CALADMNFEE", DoubleType(), True),
    StructField("CALPROCFEE", DoubleType(), True),
    StructField("CALPATSTAX", DoubleType(), True),
    StructField("CALPLNSTAX", DoubleType(), True),
    StructField("CALPRVNSEL", DoubleType(), True),
    StructField("CALPSCBRND", DoubleType(), True),
    StructField("CALPSCNONP", DoubleType(), True),
    StructField("CALPSCBRNP", DoubleType(), True),
    StructField("CALCOVGAP", DoubleType(), True),
    StructField("CALINGCSTC", DoubleType(), True),
    StructField("CALDSPFEEC", DoubleType(), True),
    StructField("PHRINGRCST", DoubleType(), True),
    StructField("PHRDISPFEE", DoubleType(), True),
    StructField("PHRPPSTAX", DoubleType(), True),
    StructField("PHRFSTAX", DoubleType(), True),
    StructField("PHRSLSTAX", DoubleType(), True),
    StructField("PHRPATPAY", DoubleType(), True),
    StructField("PHRDUEAMT", DoubleType(), True),
    StructField("PHRWITHHLD", DoubleType(), True),
    StructField("PHRPPRCS", StringType(), True),
    StructField("PHRPRCST", StringType(), True),
    StructField("PHRPTPS", StringType(), True),
    StructField("PHRPTPST", StringType(), True),
    StructField("PHRCOPAYSC", StringType(), True),
    StructField("PHRCOPAYSS", DoubleType(), True),
    StructField("PHRFCOPAY", DoubleType(), True),
    StructField("PHRPCOPAY", DoubleType(), True),
    StructField("PHRCOPAY", DoubleType(), True),
    StructField("PHRPRODSEL", DoubleType(), True),
    StructField("PHRATRTAX", DoubleType(), True),
    StructField("PHREXCEBFT", DoubleType(), True),
    StructField("PHRINCENTV", DoubleType(), True),
    StructField("PHRATRDED", DoubleType(), True),
    StructField("PHRCOB", DoubleType(), True),
    StructField("PHRTOTHAMT", DoubleType(), True),
    StructField("PHRPROFFEE", DoubleType(), True),
    StructField("PHROTHPAYA", DoubleType(), True),
    StructField("PHRCOSTSRC", StringType(), True),
    StructField("PHRCOSTTYP", StringType(), True),
    StructField("PHRPRCTYPE", StringType(), True),
    StructField("PHRRATE", DoubleType(), True),
    StructField("PHRPROCFEE", DoubleType(), True),
    StructField("PHRPATSTAX", DoubleType(), True),
    StructField("PHRPLNSTAX", DoubleType(), True),
    StructField("PHRPRVNSEL", DoubleType(), True),
    StructField("PHRPSCBRND", DoubleType(), True),
    StructField("PHRPSCNONP", DoubleType(), True),
    StructField("PHRPSCBRNP", DoubleType(), True),
    StructField("PHRCOVGAP", DoubleType(), True),
    StructField("PHRINGCSTC", DoubleType(), True),
    StructField("PHRDSPFEEC", DoubleType(), True),
    StructField("POSINGRCST", DoubleType(), True),
    StructField("POSDISPFEE", DoubleType(), True),
    StructField("POSPSLSTAX", DoubleType(), True),
    StructField("POSFSLSTAX", DoubleType(), True),
    StructField("POSSLSTAX", DoubleType(), True),
    StructField("POSPATPAY", DoubleType(), True),
    StructField("POSDUEAMT", DoubleType(), True),
    StructField("POSWITHHLD", DoubleType(), True),
    StructField("POSCOPAY", DoubleType(), True),
    StructField("POSPRODSEL", DoubleType(), True),
    StructField("POSATRTAX", DoubleType(), True),
    StructField("POSEXCEBFT", DoubleType(), True),
    StructField("POSINCENTV", DoubleType(), True),
    StructField("POSATRDED", DoubleType(), True),
    StructField("POSTOTHAMT", DoubleType(), True),
    StructField("POSPROFFEE", DoubleType(), True),
    StructField("POSOTHPAYA", DoubleType(), True),
    StructField("POSCOSTSRC", StringType(), True),
    StructField("POSPROCFEE", DoubleType(), True),
    StructField("POSPATSTAX", DoubleType(), True),
    StructField("POSPLNSTAX", DoubleType(), True),
    StructField("POSPRVNSEL", DoubleType(), True),
    StructField("POSPSCBRND", DoubleType(), True),
    StructField("POSPSCNONP", DoubleType(), True),
    StructField("POSPSCBRNP", DoubleType(), True),
    StructField("POSCOVGAP", DoubleType(), True),
    StructField("POSINGCSTC", DoubleType(), True),
    StructField("POSDSPFEEC", DoubleType(), True),
    StructField("CLIENTFLAG", StringType(), True),
    StructField("CLTINGRCST", DoubleType(), True),
    StructField("CLTDISPFEE", DoubleType(), True),
    StructField("CLTSLSTAX", DoubleType(), True),
    StructField("CLTPATPAY", DoubleType(), True),
    StructField("CLTDUEAMT", DoubleType(), True),
    StructField("CLTWITHHLD", DoubleType(), True),
    StructField("CLTPRCS", StringType(), True),
    StructField("CLTPRCST", StringType(), True),
    StructField("CLTPTPS", StringType(), True),
    StructField("CLTPTPST", StringType(), True),
    StructField("CLTCOPAYS", StringType(), True),
    StructField("CLTCOPAYSS", DoubleType(), True),
    StructField("CLTFCOPAY", DoubleType(), True),
    StructField("CLTPCOPAY", DoubleType(), True),
    StructField("CLTCOPAY", DoubleType(), True),
    StructField("CLTPRODSEL", DoubleType(), True),
    StructField("CLTPSTAX", DoubleType(), True),
    StructField("CLTFSTAX", DoubleType(), True),
    StructField("CLTATRTAX", DoubleType(), True),
    StructField("CLTEXCEBFT", DoubleType(), True),
    StructField("CLTINCENTV", DoubleType(), True),
    StructField("CLTATRDED", DoubleType(), True),
    StructField("CLTTOTHAMT", DoubleType(), True),
    StructField("CLTPROFFEE", DoubleType(), True),
    StructField("CLTCOB", DoubleType(), True),
    StructField("CLTOTHPAYA", DoubleType(), True),
    StructField("CLTCOSTSRC", StringType(), True),
    StructField("CLTCOSTTYP", StringType(), True),
    StructField("CLTPRCTYPE", StringType(), True),
    StructField("CLTRATE", DoubleType(), True),
    StructField("CLTPRSCSTP", DoubleType(), True),
    StructField("CLTPRSCHNM", StringType(), True),
    StructField("CLTPROCFEE", DoubleType(), True),
    StructField("CLTPATSTAX", DoubleType(), True),
    StructField("CLTPLNSTAX", DoubleType(), True),
    StructField("CLTPRVNSEL", DoubleType(), True),
    StructField("CLTPSCBRND", DoubleType(), True),
    StructField("CLTPSCNONP", DoubleType(), True),
    StructField("CLTPSCBRNP", DoubleType(), True),
    StructField("CLTCOVGAP", DoubleType(), True),
    StructField("CLTINGCSTC", DoubleType(), True),
    StructField("CLTDSPFEEC", DoubleType(), True),
    StructField("RSPREIMBUR", StringType(), True),
    StructField("RSPINGRCST", DoubleType(), True),
    StructField("RSPDISPFEE", DoubleType(), True),
    StructField("RSPPSLSTAX", DoubleType(), True),
    StructField("RSPFSLSTAX", DoubleType(), True),
    StructField("RSPSLSTAX", DoubleType(), True),
    StructField("RSPPATPAY", DoubleType(), True),
    StructField("RSPDUEAMT", DoubleType(), True),
    StructField("RSPFCOPAY", DoubleType(), True),
    StructField("RSPPCOPAY", DoubleType(), True),
    StructField("RSPCOPAY", DoubleType(), True),
    StructField("RSPPRODSEL", DoubleType(), True),
    StructField("RSPATRTAX", DoubleType(), True),
    StructField("RSPEXCEBFT", DoubleType(), True),
    StructField("RSPINCENTV", DoubleType(), True),
    StructField("RSPATRDED", DoubleType(), True),
    StructField("RSPTOTHAMT", DoubleType(), True),
    StructField("RSPPROFEE", DoubleType(), True),
    StructField("RSPOTHPAYA", DoubleType(), True),
    StructField("RSPACCUDED", DoubleType(), True),
    StructField("RSPREMBFT", DoubleType(), True),
    StructField("RSPREMDED", DoubleType(), True),
    StructField("RSPPROCFEE", DoubleType(), True),
    StructField("RSPPATSTAX", DoubleType(), True),
    StructField("RSPPLNSTAX", DoubleType(), True),
    StructField("RSPPRVNSEL", DoubleType(), True),
    StructField("RSPPSCBRND", DoubleType(), True),
    StructField("RSPPSCNONP", DoubleType(), True),
    StructField("RSPPSCBRNP", DoubleType(), True),
    StructField("RSPCOVGAP", DoubleType(), True),
    StructField("RSPINGCSTC", DoubleType(), True),
    StructField("RSPDSPFEEC", DoubleType(), True),
    StructField("RSPPLANID", StringType(), True),
    StructField("BENSTGQL1", StringType(), True),
    StructField("BENSTGAMT1", DoubleType(), True),
    StructField("BENSTGQL2", StringType(), True),
    StructField("BENSTGAMT2", DoubleType(), True),
    StructField("BENSTGQL3", StringType(), True),
    StructField("BENSTGAMT3", DoubleType(), True),
    StructField("BENSTGQL4", StringType(), True),
    StructField("BENSTGAMT4", DoubleType(), True),
    StructField("ESTGENSAV", DoubleType(), True),
    StructField("SPDACCTREM", DoubleType(), True),
    StructField("HLTHPLNAMT", DoubleType(), True),
    StructField("INDDEDPTD", DoubleType(), True),
    StructField("INDDEDREM", DoubleType(), True),
    StructField("FAMDEDPTD", DoubleType(), True),
    StructField("FAMDEDREM", DoubleType(), True),
    StructField("DEDSCHED", StringType(), True),
    StructField("DEDACCC", StringType(), True),
    StructField("DEDFLAG", StringType(), True),
    StructField("INDLBFTUT_1", DoubleType(), True),
    StructField("INDLBFTPTD", DoubleType(), True),
    StructField("INDLBFTREM", DoubleType(), True),
    StructField("INDLBFTUT_2", DoubleType(), True),
    StructField("FAMLBFTPTD", DoubleType(), True),
    StructField("FAMLBFTREM", DoubleType(), True),
    StructField("LFTBFTMSCH", StringType(), True),
    StructField("LFTBFTACCC", StringType(), True),
    StructField("LFTBFTFLAG", StringType(), True),
    StructField("INDBFTUT", DoubleType(), True),
    StructField("INDBMAXPTD", DoubleType(), True),
    StructField("FAMBFTUT", DoubleType(), True),
    StructField("FAMBMAXPTD", DoubleType(), True),
    StructField("INDBMAXREM", DoubleType(), True),
    StructField("FAMBMAXREM", DoubleType(), True),
    StructField("BFTMAXSCHD", StringType(), True),
    StructField("BFTMAXACCC", StringType(), True),
    StructField("BFTMAXFLAG", StringType(), True),
    StructField("INDOOPPTD", DoubleType(), True),
    StructField("FAMOOPPTD", DoubleType(), True),
    StructField("INDOOPREM", DoubleType(), True),
    StructField("FAMOOPREM", DoubleType(), True),
    StructField("OOPSCHED", StringType(), True),
    StructField("OOPACCC", StringType(), True),
    StructField("OOPFLAG", StringType(), True),
    StructField("CONTRIBUT", DoubleType(), True),
    StructField("CONTBASIS", StringType(), True),
    StructField("CONTSCHED", StringType(), True),
    StructField("CONTACCCD", StringType(), True),
    StructField("CONTFLAG", StringType(), True),
    StructField("RXTFLAG", StringType(), True),
    StructField("REIMBURSMT", StringType(), True),
    StructField("CLMORIGIN", StringType(), True),
    StructField("HLDCLMFLAG", StringType(), True),
    StructField("HLDCLMDAYS", DoubleType(), True),
    StructField("PARTDFLAG", StringType(), True),
    StructField("COBEXTFLG", StringType(), True),
    StructField("PAEXTFLG", StringType(), True),
    StructField("HSAEXTIND", StringType(), True),
    StructField("FFPMEDRMST", StringType(), True),
    StructField("FFPMEDPXST", StringType(), True),
    StructField("FFPMEDMSST", StringType(), True),
    StructField("INCIDENTID", StringType(), True),
    StructField("ETCNBR", StringType(), True),
    StructField("DTEINJURY", DoubleType(), True),
    StructField("ADDUSER", StringType(), True),
    StructField("CHGUSER", StringType(), True),
    StructField("DMRUSERID", StringType(), True),
    StructField("PRAUSERID", StringType(), True),
    StructField("CLAIMREFID", StringType(), True),
    StructField("EOBDNOV", StringType(), True),
    StructField("EOBPDOV", StringType(), True),
    StructField("MANTRKNBR", StringType(), True),
    StructField("MANRECVDTE", DoubleType(), True),
    StructField("PASAUTHTYP", StringType(), True),
    StructField("PASAUTHID", StringType(), True),
    StructField("PASREQTYPE", StringType(), True),
    StructField("PASREQFROM", DoubleType(), True),
    StructField("PASREQTHRU", DoubleType(), True),
    StructField("PASBASISRQ", StringType(), True),
    StructField("PASREPFN", StringType(), True),
    StructField("PASREPLN", StringType(), True),
    StructField("PASSTREET", StringType(), True),
    StructField("PASCITY", StringType(), True),
    StructField("PASSTATE", StringType(), True),
    StructField("PASZIP", StringType(), True),
    StructField("PASPANBR", StringType(), True),
    StructField("PASAUTHNBR", StringType(), True),
    StructField("PASSDOCCT", DoubleType(), True),
    StructField("PAYERTYPE", StringType(), True),
    StructField("DELAYRSNCD", StringType(), True),
    StructField("MEDCDIND", StringType(), True),
    StructField("MEDCDID", StringType(), True),
    StructField("MEDCDAGNBR", StringType(), True),
    StructField("MEDCDTCN", StringType(), True),
    StructField("FMSTIER", StringType(), True),
    StructField("FMSSTATUS", StringType(), True),
    StructField("FMSDFLTIND", StringType(), True),
    StructField("FMSBENLST", StringType(), True),
    StructField("FMSLSTLVL3", StringType(), True),
    StructField("FMSLSTLVL2", StringType(), True),
    StructField("FMSLSTLVL1", StringType(), True),
    StructField("FMSRULESET", StringType(), True),
    StructField("FMSRULE", StringType(), True),
    StructField("FMSPROCCD", StringType(), True),
    StructField("CLIENTDEF1", StringType(), True),
    StructField("CLIENTDEF2", StringType(), True),
    StructField("CLIENTDEF3", StringType(), True),
    StructField("CLIENTDEF4", StringType(), True),
    StructField("CLIENTDEF5", StringType(), True),
    StructField("CCTRESERV1", StringType(), True),
    StructField("CCTRESERV2", StringType(), True),
    StructField("CCTRESERV3", StringType(), True),
    StructField("CCTRESERV4", StringType(), True),
    StructField("CCTRESERV5", StringType(), True),
    StructField("CCTRESERV6", StringType(), True),
    StructField("CCTRESERV7", StringType(), True),
    StructField("CCTRESERV8", StringType(), True),
    StructField("CCTRESERV9", StringType(), True),
    StructField("CCTRESRV10", StringType(), True),
    StructField("OOPAPPLIED", DoubleType(), True),
    StructField("CCTRESRV12", DoubleType(), True),
    StructField("CCTRESRV13", DoubleType(), True),
    StructField("CCTRESRV14", DoubleType(), True),
    StructField("USERFIELD", StringType(), True),
    StructField("EXTRACTDTE", DoubleType(), True),
    StructField("BATCHCTRL", StringType(), True),
    StructField("CLTTYPUCST", DoubleType(), True),
    StructField("REJCDE4", StringType(), True),
    StructField("REJCDE5", StringType(), True),
    StructField("REJCDE6", StringType(), True),
    StructField("MESSAGECD4", StringType(), True),
    StructField("MESSAGE4", StringType(), True),
    StructField("MESSAGECD5", StringType(), True),
    StructField("MESSAGE5", StringType(), True),
    StructField("MESSAGECD6", StringType(), True),
    StructField("MESSAGEG6", StringType(), True),
    StructField("CLIENT2FLAG", StringType(), True),
    StructField("CLT2INGRCST", DoubleType(), True),
    StructField("CLT2DISPFEE", DoubleType(), True),
    StructField("CLT2SLSTAX", DoubleType(), True),
    StructField("CLT2DUEAMT", DoubleType(), True),
    StructField("CLTPRSCSTP2", DoubleType(), True),
    StructField("CLTPRSCHNM2", DoubleType(), True),
    StructField("CLT2PRCS", StringType(), True),
    StructField("CLT2PRCST", StringType(), True),
    StructField("CLT2PSTAX", DoubleType(), True),
    StructField("CLT2FSTAX", DoubleType(), True),
    StructField("CLT2OTHAMT", DoubleType(), True),
    StructField("CLT2COSTSRC", StringType(), True),
    StructField("CLT2COSTTYP", StringType(), True),
    StructField("CLT2PRCTYPE", StringType(), True),
    StructField("CLT2RATE", DoubleType(), True),
    StructField("CLT2DCCSCID", StringType(), True),
    StructField("CLT2DCCSCSQ", DoubleType(), True),
    StructField("CLT2DCSCHID", StringType(), True),
    StructField("CLT2DCSCHSQ", DoubleType(), True),
    StructField("CALINCENTV2", DoubleType(), True),
    StructField("CLNT3FLAG", StringType(), True),
    StructField("CLT3INGRCST", DoubleType(), True),
    StructField("CLT3DISPFEE", DoubleType(), True),
    StructField("CLT3SLSTAX", DoubleType(), True),
    StructField("CLT3DUEAMT", DoubleType(), True),
    StructField("CLTPRSCSTP3", DoubleType(), True),
    StructField("CLTPRSCHNM3", DoubleType(), True),
    StructField("CLT3PRCS", StringType(), True),
    StructField("CLT3PRCST", StringType(), True),
    StructField("CLT3PSTAX", DoubleType(), True),
    StructField("CLT3FSTAX", DoubleType(), True),
    StructField("CLT3OTHAMT", DoubleType(), True),
    StructField("CLT3COSTSRC", StringType(), True),
    StructField("CLT3COSTTYP", StringType(), True),
    StructField("CLT3PRCTYPE", StringType(), True),
    StructField("CLT3RATE", DoubleType(), True),
    StructField("CLT3DCCSCID", StringType(), True),
    StructField("CLT3DCCSCSQ", DoubleType(), True),
    StructField("CLT3DCSCHID", StringType(), True),
    StructField("CLT3DCSCHSQ", DoubleType(), True),
    StructField("CALINCENTV3", DoubleType(), True),
    StructField("TBR_TBR_REJECT_REASON1", StringType(), True),
    StructField("TBR_TBR_REJECT_REASON2", StringType(), True),
    StructField("TBR_TBR_REJECT_REASON3", StringType(), True),
    StructField("SBM_QTY_INT_DIS", StringType(), True),
    StructField("SBM_DS_INT_DIS", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_CT", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_QLFR1", StringType(), True),
    StructField("OTHR_AMT_CLMSBM1", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_QLFR2", StringType(), True),
    StructField("OTHR_AMT_CLMSBM2", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_QLFR3", StringType(), True),
    StructField("OTHR_AMT_CLMSBM3", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_QLFR4", StringType(), True),
    StructField("OTHR_AMT_CLMSBM4", StringType(), True),
    StructField("OTHR_AMT_CLMSBM_QLFR5", StringType(), True),
    StructField("OTHR_AMT_CLMSBM5", StringType(), True),
    StructField("RSN_SRVCDE1", StringType(), True),
    StructField("RSN_SRVCDE2", StringType(), True),
    StructField("RSN_SRVCDE3", StringType(), True),
    StructField("PROF_SRVCDE1", StringType(), True),
    StructField("PROF_SRVCDE2", StringType(), True),
    StructField("PROF_SRVCDE3", StringType(), True),
    StructField("RESULT_OF_SRV_CDE1", StringType(), True),
    StructField("RESULT_OF_SRV_CDE2", StringType(), True),
    StructField("RESULT_OF_SRV_CDE3", StringType(), True),
    StructField("SCH_RX_ID_NBR", StringType(), True),
    StructField("BASIS_CAL_DISPFEE", StringType(), True),
    StructField("BASIS_CAL_COPAY", StringType(), True),
    StructField("BASIS_CAL_FLT_TAX", StringType(), True),
    StructField("BASIS_CAL_PRCNT_TAX", StringType(), True),
    StructField("BASIS_CAL_COINSUR", StringType(), True),
    StructField("PAID_MSG_CODE_COUNT", StringType(), True),
    StructField("PAID_MESSAGE_CODE_1", StringType(), True),
    StructField("PAID_MESSAGE_CODE_2", StringType(), True),
    StructField("PAID_MESSAGE_CODE_3", StringType(), True),
    StructField("PAID_MESSAGE_CODE_4", StringType(), True),
    StructField("PAID_MESSAGE_CODE_5", StringType(), True),
    StructField("FILLER_FOR_REJECT_FIELD_IND", StringType(), True),
])

df_OptumDetail_Seq = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OptumDetail_Seq)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# DataCleansing transformer => 2 Output dataframes
df_DataCleansing_DetailGroupLevelFields_Lnk = (
    df_OptumDetail_Seq
    .filter(length(trim(col("GROUPID"))) > 0)
    .withColumn(
        "GROUPID",
        trim(col("GROUPID"))
    )
    .withColumn(
        "CLTINGRCST",
        when(
            (col("CLTINGRCST").isNull()) | (trim(col("CLTINGRCST")) == ""),
            lit("0.0")
        ).otherwise(col("CLTINGRCST"))
    )
    .withColumn(
        "CLTPATPAY",
        when(
            (col("CLTPATPAY").isNull()) | (trim(col("CLTPATPAY")) == ""),
            lit("0.0")
        ).otherwise(col("CLTPATPAY"))
    )
    .withColumn(
        "CLTDUEAMT",
        when(
            (col("CLTDUEAMT").isNull()) | (trim(col("CLTDUEAMT")) == ""),
            lit("0.0")
        ).otherwise(col("CLTDUEAMT"))
    )
    .select(
        "GROUPID",
        "CLTINGRCST",
        "CLTPATPAY",
        "CLTDUEAMT"
    )
)

df_DataCleansing_FullAgg_Amts = (
    df_OptumDetail_Seq
    .withColumn(
        "CLTINGRCST",
        when(
            (col("CLTINGRCST").isNull()) | (trim(col("CLTINGRCST")) == ""),
            lit("0.0")
        ).otherwise(col("CLTINGRCST"))
    )
    .withColumn(
        "CLTPATPAY",
        when(
            (col("CLTPATPAY").isNull()) | (trim(col("CLTPATPAY")) == ""),
            lit("0.0")
        ).otherwise(col("CLTPATPAY"))
    )
    .withColumn(
        "CLTDUEAMT",
        when(
            (col("CLTDUEAMT").isNull()) | (trim(col("CLTDUEAMT")) == ""),
            lit("0.0")
        ).otherwise(col("CLTDUEAMT"))
    )
    .select(
        lit("1").alias("ID"),
        "CLTINGRCST",
        "CLTPATPAY",
        "CLTDUEAMT"
    )
)

# DetailGroupLevel_Aggr (AGGREGATOR)
df_DetailGroupLevel_Aggr = (
    df_DataCleansing_DetailGroupLevelFields_Lnk
    .groupBy("GROUPID")
    .agg(
        F_count("GROUPID").alias("GROUPID_CNT"),
        F_sum(col("CLTINGRCST").cast("double")).alias("CLTINGRCST"),
        F_sum(col("CLTPATPAY").cast("double")).alias("CLTPATPAY"),
        F_sum(col("CLTDUEAMT").cast("double")).alias("CLTDUEAMT")
    )
)

# DetailAmountLevel_Aggr (AGGREGATOR)
df_DetailAmountLevel_Aggr = (
    df_DataCleansing_FullAgg_Amts
    .groupBy("ID")
    .agg(
        F_sum(col("CLTINGRCST").cast("double")).alias("CLTINGRCST"),
        F_sum(col("CLTPATPAY").cast("double")).alias("CLTPATPAY"),
        F_sum(col("CLTDUEAMT").cast("double")).alias("CLTDUEAMT")
    )
)

# OptumControl_Seq (verified)
schema_OptumControl_Seq = StructType([
    StructField("DATE", StringType(), True),
    StructField("GROUP", StringType(), True),
    StructField("TOTAL_RECORDS", DoubleType(), True),
    StructField("INGREDIENT_COST", DoubleType(), True),
    StructField("AMT_COPAY", DoubleType(), True),
    StructField("AMT_PAYMENT", DoubleType(), True),
    StructField("QUANTITY", DoubleType(), True),
])

df_OptumControl_Seq = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OptumControl_Seq)
    .load(f"{adls_path}/verified/{ControlInFile}.temp")
)

# ControlFile_Xfr => 2 output dataframes
df_ControlFile_Xfr_OptumContolFileIndvl = (
    df_OptumControl_Seq
    .withColumn("GROUP", trim(col("GROUP")))
    .withColumn("TOTAL_RECORDS", trim(col("TOTAL_RECORDS").cast("string")))
    .select(
        trim(col("GROUP")).alias("GROUP"),
        trim(col("TOTAL_RECORDS")).alias("TOTAL_RECORDS"),
        col("DATE"),
        when(col("INGREDIENT_COST").isNull(), lit("")).otherwise(
            col("INGREDIENT_COST")
        ).alias("INGREDIENT_COST"),
        when(col("AMT_COPAY").isNull(), lit("")).otherwise(
            col("AMT_COPAY")
        ).alias("AMT_COPAY"),
        when(col("AMT_PAYMENT").isNull(), lit("")).otherwise(
            col("AMT_PAYMENT")
        ).alias("AMT_PAYMENT"),
        col("QUANTITY")
    )
)

df_ControlFile_Xfr_OptumContolFileGroup = (
    df_OptumControl_Seq
    .select(
        lit("1").alias("ID"),
        when(col("INGREDIENT_COST").isNull(), lit("")).otherwise(
            col("INGREDIENT_COST")
        ).alias("INGREDIENT_COST"),
        when(col("AMT_COPAY").isNull(), lit("")).otherwise(
            col("AMT_COPAY")
        ).alias("AMT_COPAY"),
        when(col("AMT_PAYMENT").isNull(), lit("")).otherwise(
            col("AMT_PAYMENT")
        ).alias("AMT_PAYMENT")
    )
)

# ControlGroupLevel_HashFile => Scenario A
# Deduplicate on keys: GROUP, TOTAL_RECORDS, INGREDIENT_COST (join condition from next transformer)
df_ControlGroupLevel_HashFile_dedup = dedup_sort(
    df_ControlFile_Xfr_OptumContolFileIndvl,
    ["GROUP", "TOTAL_RECORDS", "INGREDIENT_COST"],
    []
)

# GroupLevelLogic_Xfr => left join
df_GroupLevelLogic_joined = (
    df_DetailGroupLevel_Aggr.alias("DetailGroupLevel_lkp")
    .join(
        df_ControlGroupLevel_HashFile_dedup.alias("ControlGroupLevel_lkp"),
        (
            (col("DetailGroupLevel_lkp.GROUPID") == col("ControlGroupLevel_lkp.GROUP")) &
            (col("DetailGroupLevel_lkp.GROUPID_CNT") == col("ControlGroupLevel_lkp.TOTAL_RECORDS").cast("double")) &
            (col("DetailGroupLevel_lkp.CLTINGRCST") == col("ControlGroupLevel_lkp.INGREDIENT_COST").cast("double"))
        ),
        how="left"
    )
)

df_GroupLevelLogic_Lnk = df_GroupLevelLogic_joined.filter(
    (col("DetailGroupLevel_lkp.GROUPID") == col("ControlGroupLevel_lkp.GROUP")) &
    (
        (col("DetailGroupLevel_lkp.GROUPID_CNT") != col("ControlGroupLevel_lkp.TOTAL_RECORDS").cast("double"))
        | (col("DetailGroupLevel_lkp.CLTINGRCST") != col("ControlGroupLevel_lkp.INGREDIENT_COST").cast("double"))
        | (col("DetailGroupLevel_lkp.CLTPATPAY") != col("ControlGroupLevel_lkp.AMT_COPAY").cast("double"))
        | (col("DetailGroupLevel_lkp.CLTDUEAMT") != col("ControlGroupLevel_lkp.AMT_PAYMENT").cast("double"))
    )
).select(
    col("DetailGroupLevel_lkp.GROUPID").alias("GROUPID"),
    col("DetailGroupLevel_lkp.GROUPID_CNT").alias("GROUPID_CNT"),
    col("DetailGroupLevel_lkp.CLTINGRCST").alias("CLTINGRCST"),
    col("DetailGroupLevel_lkp.CLTPATPAY").alias("CLTPATPAY"),
    col("DetailGroupLevel_lkp.CLTDUEAMT").alias("CLTDUEAMT"),
    col("ControlGroupLevel_lkp.DATE").alias("DATE"),
    col("ControlGroupLevel_lkp.GROUP").alias("GROUP"),
    col("ControlGroupLevel_lkp.TOTAL_RECORDS").alias("TOTAL_RECORDS"),
    col("ControlGroupLevel_lkp.INGREDIENT_COST").alias("INGREDIENT_COST"),
    col("ControlGroupLevel_lkp.AMT_COPAY").alias("AMT_COPAY"),
    col("ControlGroupLevel_lkp.AMT_PAYMENT").alias("AMT_PAYMENT"),
    col("ControlGroupLevel_lkp.QUANTITY").alias("QUANTITY")
)

# GroupLevelCompare_Seq => write file with header
# Apply rpad for char/varchar: GROUP (char(20)), DATE (char(10)), etc.
# Non-char columns remain as is
df_GroupLevelCompare_Seq_out = (
    df_GroupLevelLogic_Lnk
    .select(
        col("GROUPID"),
        col("GROUPID_CNT"),
        col("CLTINGRCST"),
        col("CLTPATPAY"),
        col("CLTDUEAMT"),
        rpad(col("DATE"), 10, " ").alias("DATE"),
        rpad(col("GROUP"), 20, " ").alias("GROUP"),
        rpad(col("TOTAL_RECORDS"), 10, " ").alias("TOTAL_RECORDS"),  # DS stage had them as varchar too
        col("INGREDIENT_COST"),
        col("AMT_COPAY"),
        col("AMT_PAYMENT"),
        col("QUANTITY")
    )
)

write_files(
    df_GroupLevelCompare_Seq_out,
    f"{adls_path}/verified/Optum_reject_grouplevel_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# AmountLevel_Aggr => aggregator output
df_AmountLevel_Aggr = (
    df_ControlFile_Xfr_OptumContolFileGroup
    .groupBy("ID")
    .agg(
        F_sum(col("INGREDIENT_COST").cast("double")).alias("INGREDIENT_COST"),
        F_sum(col("AMT_COPAY").cast("double")).alias("AMT_COPAY"),
        F_sum(col("AMT_PAYMENT").cast("double")).alias("AMT_PAYMENT")
    )
)

# AmountLevel_HashFile => Scenario A
# Deduplicate on keys: ID, INGREDIENT_COST (join condition)
df_AmountLevel_HashFile_dedup = dedup_sort(
    df_AmountLevel_Aggr.select(
        "ID",
        "INGREDIENT_COST",
        "AMT_COPAY",
        "AMT_PAYMENT"
    ),
    ["ID", "INGREDIENT_COST"],
    []
)

# AmountsLevelLogic_Xfr => left join
df_AmountsLevelLogic_joined = (
    df_DetailAmountLevel_Aggr.alias("DetailAmountLevel_Lnk")
    .join(
        df_AmountLevel_HashFile_dedup.alias("AmoutLevelCompare_lkp"),
        (
            (col("DetailAmountLevel_Lnk.ID") == col("AmoutLevelCompare_lkp.ID")) &
            (col("DetailAmountLevel_Lnk.CLTINGRCST") == col("AmoutLevelCompare_lkp.INGREDIENT_COST").cast("double"))
        ),
        how="left"
    )
)

df_AmountsLevelLogic_Lnk = df_AmountsLevelLogic_joined.filter(
    (col("DetailAmountLevel_Lnk.ID") == col("AmoutLevelCompare_lkp.ID")) &
    (
        (col("DetailAmountLevel_Lnk.CLTINGRCST") != col("AmoutLevelCompare_lkp.INGREDIENT_COST").cast("double"))
        | (col("DetailAmountLevel_Lnk.CLTPATPAY") != col("AmoutLevelCompare_lkp.AMT_COPAY").cast("double"))
        | (col("DetailAmountLevel_Lnk.CLTDUEAMT") != col("AmoutLevelCompare_lkp.AMT_PAYMENT").cast("double"))
    )
).select(
    col("DetailAmountLevel_Lnk.CLTINGRCST").alias("CLTINGRCST"),
    col("DetailAmountLevel_Lnk.CLTPATPAY").alias("CLTPATPAY"),
    col("DetailAmountLevel_Lnk.CLTDUEAMT").alias("CLTDUEAMT"),
    col("AmoutLevelCompare_lkp.INGREDIENT_COST").alias("INGREDIENT_COST"),
    col("AmoutLevelCompare_lkp.AMT_COPAY").alias("AMT_COPAY"),
    col("AmoutLevelCompare_lkp.AMT_PAYMENT").alias("AMT_PAYMENT")
)

# AmoutLevelCompare_Seq => write file with no header
df_AmoutLevelCompare_Seq_out = (
    df_AmountsLevelLogic_Lnk
    .select(
        col("CLTINGRCST"),
        col("CLTPATPAY"),
        col("CLTDUEAMT"),
        col("INGREDIENT_COST"),
        col("AMT_COPAY"),
        col("AMT_PAYMENT")
    )
)

write_files(
    df_AmoutLevelCompare_Seq_out,
    f"{adls_path}/verified/Optum_reject_amtlevel_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)