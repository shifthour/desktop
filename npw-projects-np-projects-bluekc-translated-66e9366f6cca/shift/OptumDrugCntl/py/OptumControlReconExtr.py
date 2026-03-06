#!/usr/bin/python3

from npadf import *

def OptumControlReconExtrActivities(ctx):
  def dfOptumControlReconExtr():
    def OptumDetailSeq():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='OptumDetailSeq', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def DataCleansingV168S9P4filter():
       return DataflowSegment([
         Transformation(name='DataCleansingV168S9P4filter')])
    def DataCleansingV168S9P4():
       return DataflowSegment([
         Transformation(name='DataCleansingV168S9P4Derived'),
         Transformation(name='DataCleansingV168S9P4')])
    def DetailGroupLevelAggr():
       return DataflowSegment([Transformation(name='DetailGroupLevelAggr', description='Placeholder for AGGREGATOR')])
    def DataCleansingV168S9P8():
       return DataflowSegment([
         Transformation(name='DataCleansingV168S9P8Derived'),
         Transformation(name='DataCleansingV168S9P8')])
    def DetailAmountLevelAggr():
       return DataflowSegment([Transformation(name='DetailAmountLevelAggr', description='Placeholder for AGGREGATOR')])
    def OptumControlSeq():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='OptumControlSeq', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def ControlFileXfrV0S448P5():
       return DataflowSegment([
         Transformation(name='ControlFileXfrV0S448P5Derived'),
         Transformation(name='ControlFileXfrV0S448P5')])
    def AmountLevelAggr():
       return DataflowSegment([Transformation(name='AmountLevelAggr', description='Placeholder for AGGREGATOR')])
    def AmountLevelHashFile():
       return DataflowSegment([
         Transformation(name='AmountLevelHashFileDerived'),
         Transformation(name='AmountLevelHashFile')])
    def AmountsLevelLogicXfrlookup():
       return DataflowSegment([
         Transformation(name="AmountsLevelLogicXfrlookup"),
         Transformation(name="AmountsLevelLogicXfrlookupJoin1"),Transformation(name="AmountsLevelLogicXfrlookupDerived1")])
    def AmountsLevelLogicXfrfilter():
       return DataflowSegment([
         Transformation(name='AmountsLevelLogicXfrfilter')])
    def AmountsLevelLogicXfr():
       return DataflowSegment([
         Transformation(name='AmountsLevelLogicXfrDerived'),
         Transformation(name='AmountsLevelLogicXfr')])
    def AmoutLevelCompareSeq():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='AmoutLevelCompareSeq', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    def ControlFileXfrV0S448P3():
       return DataflowSegment([
         Transformation(name='ControlFileXfrV0S448P3Derived'),
         Transformation(name='ControlFileXfrV0S448P3')])
    def ControlGroupLevelHashFile():
       return DataflowSegment([
         Transformation(name='ControlGroupLevelHashFileDerived'),
         Transformation(name='ControlGroupLevelHashFile')])
    def GroupLevelLogicXfrlookup():
       return DataflowSegment([
         Transformation(name="GroupLevelLogicXfrlookup"),
         Transformation(name="GroupLevelLogicXfrlookupJoin1"),Transformation(name="GroupLevelLogicXfrlookupDerived1")])
    def GroupLevelLogicXfrfilter():
       return DataflowSegment([
         Transformation(name='GroupLevelLogicXfrfilter')])
    def GroupLevelLogicXfr():
       return DataflowSegment([
         Transformation(name='GroupLevelLogicXfrDerived'),
         Transformation(name='GroupLevelLogicXfr')])
    def GroupLevelCompareSeq():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='GroupLevelCompareSeq', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "OptumControlReconExtr"
    buffer = DataflowBuffer()
    buffer.append(OptumDetailSeq())
    buffer.append(DataCleansingV168S9P4filter())
    buffer.append(DataCleansingV168S9P4())
    buffer.append(DetailGroupLevelAggr())
    buffer.append(DataCleansingV168S9P8())
    buffer.append(DetailAmountLevelAggr())
    buffer.append(OptumControlSeq())
    buffer.append(ControlFileXfrV0S448P5())
    buffer.append(AmountLevelAggr())
    buffer.append(AmountLevelHashFile())
    buffer.append(AmountsLevelLogicXfrlookup())
    buffer.append(AmountsLevelLogicXfrfilter())
    buffer.append(AmountsLevelLogicXfr())
    buffer.append(AmoutLevelCompareSeq())
    buffer.append(ControlFileXfrV0S448P3())
    buffer.append(ControlGroupLevelHashFile())
    buffer.append(GroupLevelLogicXfrlookup())
    buffer.append(GroupLevelLogicXfrfilter())
    buffer.append(GroupLevelLogicXfr())
    buffer.append(GroupLevelCompareSeq())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/claim/Optum"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        RunID as string ,
        SrcSysCd as string ,
        SrcSysCdSK as integer ,
        RunDate as string ,
        InFile as string ,
        ControlInFile as string ,
        MbrTermDate as string 
      }}
    source(output(
        RXCLMNBR as decimal(15, 0),
        CLMSEQNBR as decimal(3, 0),
        CLAIMSTS as string,
        CARID as string,
        SCARID as string,
        CARRPROC as decimal(10, 0),
        CLNTID as string,
        CLNTSGMNT as string,
        CLNTREGION as string,
        ACCOUNTID as string,
        ACCTBENCDE as string,
        VERSIONNBR as string,
        GROUPID as string,
        GROUPPLAN as string,
        GRPCLIBENF as string,
        GROUPSIC as string,
        CLMRESPSTS as string,
        MEMBERID as string,
        MBRLSTNME as string,
        MBRFSTNME as string,
        MBRMDINIT as string,
        MBRPRSNCDE as string,
        MBRRELCDE as string,
        MBRSEX as string,
        MBRBIRTH as decimal(8, 0),
        MBRAGE as decimal(3, 0),
        MBRZIP as string,
        SOCSECNBR as string,
        DURKEY as string,
        DURFLAG as string,
        MBRFAMLYID as string,
        MBRFAMLIND as string,
        MBRFAMLTYP as string,
        COBIND as string,
        MBRPLAN as string,
        MBRPRODCDE as string,
        MBRRIDERCD as string,
        CARENETID as string,
        CAREQUALID as string,
        CAREFACID as string,
        CAREFACNAM as string,
        MBRPCPHYS as string,
        PPRSFSTNME as string,
        PPRSLSTNME as string,
        PPRSMDINIT as string,
        PPRSSPCCDE as string,
        PPRSTATE as string,
        MBRALTINFL as string,
        MBRALTINCD as string,
        MBRALTINID as string,
        MBRMEDDTE as decimal(8, 0),
        MBRMEDTYPE as string,
        MBRHICCDE as string,
        CARDHOLDER as string,
        PATLASTNME as string,
        PATFRSTNME as string,
        PERSONCDE as string,
        RELATIONCD as string,
        SEXCODE as string,
        BIRTHDTE as decimal(8, 0),
        ELIGCLARIF as string,
        CUSTLOC as string,
        SBMPLSRVCE as string,
        SBMPATRESD as string,
        PRMCAREPRV as string,
        PRMCAREPRQ as string,
        FACILITYID as string,
        OTHCOVERAG as string,
        BINNUMBER as string,
        PROCESSOR as string,
        GROUPNBR as string,
        TRANSCDE as string,
        DATESBM as decimal(8, 0),
        TIMESBM as decimal(6, 0),
        CHGDATE as decimal(7, 0),
        CHGTIME as decimal(6, 0),
        ORGPDSBMDT as decimal(8, 0),
        RVDATESBM as decimal(8, 0),
        CLMCOUNTER as decimal(2, 0),
        GENERICCTR as decimal(2, 0),
        FORMLRYCTR as decimal(2, 0),
        RXNUMBER as string,
        RXNUMBERQL as string,
        REFILL as string,
        DISPSTATUS as string,
        DTEFILLED as decimal(8, 0),
        COMPOUNDCD as string,
        SBMCMPDTYP as string,
        PRODTYPCDE as string,
        PRODUCTID as string,
        PRODUCTKEY as decimal(9, 0),
        METRICQTY as decimal(6, 0),
        DECIMALQTY as decimal(13, 0),
        DAYSSUPPLY as decimal(3, 0),
        PSC as string,
        WRITTENDTE as decimal(8, 0),
        NBRFLSAUTH as decimal(2, 0),
        ORIGINCDE as string,
        SBMCLARCD1 as string,
        SBMCLARCD2 as string,
        SBMCLARCD3 as string,
        PAMCNBR as string,
        PAMCCDE as string,
        PRAUTHNBR as string,
        PRAUTHRSN as string,
        PRAUTHFDTE as decimal(7, 0),
        PRAUTHTDTE as decimal(7, 0),
        LABELNAME as string,
        PRODNM as string,
        DRUGMFGRID as string,
        DRUGMFGR as string,
        GPINUMBER as string,
        GNRCNME as string,
        PRDPACUOM as string,
        PRDPACSIZE as decimal(12, 0),
        DDID as decimal(6, 0),
        GCN as decimal(5, 0),
        GCNSEQ as decimal(6, 0),
        KDC as decimal(10, 0),
        AHFS as string,
        DRUGDEACOD as string,
        RXOTCIND as string,
        MULTSRCCDE as string,
        GENINDOVER as string,
        PRDREIMIND as string,
        BRNDTRDNME as string,
        FDATHERAEQ as string,
        METRICSTRG as decimal(12, 0),
        DRGSTRGUOM as string,
        ADMINROUTE as string,
        ADMINRTESN as string,
        DOSAGEFORM as string,
        NDA as string,
        ANDA as string,
        ANDAOR as string,
        RXNORMCODE as string,
        MNTDRUGCDE as string,
        MNTSOURCE as string,
        MNTCARPROR as string,
        MNTGPILIST as string,
        CPQSPCPRG as string,
        CPQSPCPGIN as string,
        CPQSPCSCHD as string,
        THRDPARTYX as string,
        DRGUNITDOS as string,
        SBMUNITDOS as string,
        ALTPRODTYP as string,
        ALTPRODCDE as string,
        SRXNETWRK as string,
        SRXNETTYPE as string,
        RXNETWORK as string,
        RXNETWRKNM as string,
        RXNETCARR as string,
        RXNETWRKQL as string,
        RXNETPRCQL as string,
        REGIONCDE as string,
        PHRAFFIL as string,
        NETPRIOR as string,
        NETTYPE as string,
        NETSEQ as decimal(5, 0),
        PAYCNTR as string,
        PHRNDCLST as string,
        PHRGPILST as string,
        SRVPROVID as string,
        SRVPROVIDQ as string,
        NPIPROV as string,
        PRVNCPDPID as string,
        SBMSRVPRID as string,
        SBMSRVPRQL as string,
        SRVPROVNME as string,
        PROVLOCKQL as string,
        PROVLOCKID as string,
        STORENBR as string,
        AFFILIATIN as string,
        PAYEEID as string,
        DISPRCLASS as string,
        DISPROTHER as string,
        PHARMZIP as string,
        PRESNETWID as string,
        PRESCRIBER as string,
        PRESCRIDQL as string,
        NPIPRESCR as string,
        PRESCDEAID as string,
        PRESLSTNME as string,
        PRESFSTNME as string,
        PRESMDINIT as string,
        PRESSPCCDE as string,
        PRESSPCCDQ as string,
        PRSSTATE as string,
        SBMRPHID as string,
        SBMRPHIDQL as string,
        FNLPLANCDE as string,
        FNLPLANDTE as decimal(7, 0),
        PLANTYPE as string,
        PLANQUAL as string,
        PLNNDCLIST as string,
        LSTQUALNDC as string,
        PLNGPILIST as string,
        LSTQUALGPI as string,
        PLNPNDCLST as string,
        PLNPGPILST as string,
        PLANDRUGST as string,
        PLANFRMLRY as string,
        PLNFNLPSCH as string,
        PHRDCSCHID as string,
        PHRDCSCHSQ as decimal(3, 0),
        CLTDCSCHID as string,
        CLTDCSCHSQ as decimal(3, 0),
        PHRDCCSCID as string,
        PHRDCCSCSQ as decimal(3, 0),
        CLTDCCSCID as string,
        CLTDCCSCSQ as decimal(3, 0),
        PHRPRTSCID as string,
        CLTPRTSCID as string,
        PHRRMSCHID as string,
        CLTRMSCHID as string,
        PRDPFLSTID as string,
        PRFPRDSCID as string,
        FORMULARY as string,
        FORMLRFLAG as string,
        TIERVALUE as decimal(2, 0),
        CONTHERAPY as string,
        CTSCHEDID as string,
        PRBASISCHD as string,
        REGDISOR as string,
        DRUGSTSTBL as string,
        TRANSBEN as string,
        MESSAGECD1 as string,
        MESSAGE1 as string,
        MESSAGECD2 as string,
        MESSAGE2 as string,
        MESSAGECD3 as string,
        MESSAGE3 as string,
        REJCNT as decimal(2, 0),
        REJCDE1 as string,
        REJCDE2 as string,
        REJCDE3 as string,
        RJCPLANID as string,
        DURCONFLCT as string,
        DURINTERVN as string,
        DUROUTCOME as string,
        LVLSERVICE as string,
        PHARSRVTYP as string,
        DIAGNOSIS as string,
        DIAGNOSISQ as string,
        RVDURCNFLC as string,
        RVDURINTRV as string,
        RVDUROUTCM as string,
        RVLVLSERVC as string,
        DRGCNFLCT1 as string,
        SEVERITY1 as string,
        OTHRPHARM1 as string,
        DTEPRVFIL1 as decimal(8, 0),
        QTYPRVFIL1 as decimal(12, 0),
        DATABASE1 as string,
        OTHRPRESC1 as string,
        FREETEXT1 as string,
        DRGCNFLCT2 as string,
        SEVERITY2 as string,
        OTHRPHARM2 as string,
        DTEPRVFIL2 as decimal(8, 0),
        QTYPRVFIL2 as decimal(12, 0),
        DATABASE2 as string,
        OTHRPRESC2 as string,
        FREETEXT2 as string,
        DRGCNFLCT3 as string,
        SEVERITY3 as string,
        OTHRPHARM3 as string,
        DTEPRVFIL3 as decimal(8, 0),
        QTYPRVFIL3 as decimal(12, 0),
        DATABASE3 as string,
        OTHRPRESC3 as string,
        FREETEXT3 as string,
        FEETYPE as string,
        BENUNITCST as decimal(14, 0),
        AWPUNITCST as decimal(14, 0),
        WACUNITCST as decimal(14, 0),
        GEAPUNTCST as decimal(14, 0),
        CTYPEUCOST as decimal(14, 0),
        BASISCOST as string,
        PRICEQTY as decimal(13, 0),
        PRODAYSSUP as decimal(3, 0),
        PROQTY as decimal(12, 0),
        RVINCNTVSB as decimal(11, 0),
        SBMINGRCST as decimal(11, 0),
        SBMDISPFEE as decimal(11, 0),
        SBMPSLSTX as decimal(11, 0),
        SBMFSLSTX as decimal(11, 0),
        SBMSLSTX as decimal(11, 0),
        SBMPATPAY as decimal(11, 0),
        SBMAMTDUE as decimal(11, 0),
        SBMINCENTV as decimal(11, 0),
        SBMPROFFEE as decimal(11, 0),
        SBMTOTHAMT as decimal(11, 0),
        SBMOPAMTCT as decimal(1, 0),
        SBMOPAMTQL as string,
        USUALNCUST as decimal(11, 0),
        DENIALDTE as decimal(8, 0),
        OTHRPAYOR as decimal(11, 0),
        SBMMDPDAMT as decimal(11, 0),
        CALINGRCST as decimal(11, 0),
        CALDISPFEE as decimal(11, 0),
        CALPSTAX as decimal(11, 0),
        CALFSTAX as decimal(11, 0),
        CALSLSTAX as decimal(11, 0),
        CALPATPAY as decimal(11, 0),
        CALDUEAMT as decimal(11, 0),
        CALWITHHLD as decimal(11, 0),
        CALFCOPAY as decimal(11, 0),
        CALPCOPAY as decimal(11, 0),
        CALCOPAY as decimal(11, 0),
        CALPRODSEL as decimal(11, 0),
        CALATRTAX as decimal(11, 0),
        CALEXCEBFT as decimal(11, 0),
        CALINCENTV as decimal(11, 0),
        CALATRDED as decimal(11, 0),
        CALCOB as decimal(11, 0),
        CALTOTHAMT as decimal(11, 0),
        CALPROFFEE as decimal(11, 0),
        CALOTHPAYA as decimal(11, 0),
        CALCOSTSRC as string,
        CALADMNFEE as decimal(11, 0),
        CALPROCFEE as decimal(11, 0),
        CALPATSTAX as decimal(11, 0),
        CALPLNSTAX as decimal(11, 0),
        CALPRVNSEL as decimal(11, 0),
        CALPSCBRND as decimal(11, 0),
        CALPSCNONP as decimal(11, 0),
        CALPSCBRNP as decimal(11, 0),
        CALCOVGAP as decimal(11, 0),
        CALINGCSTC as decimal(11, 0),
        CALDSPFEEC as decimal(11, 0),
        PHRINGRCST as decimal(11, 0),
        PHRDISPFEE as decimal(11, 0),
        PHRPPSTAX as decimal(11, 0),
        PHRFSTAX as decimal(11, 0),
        PHRSLSTAX as decimal(11, 0),
        PHRPATPAY as decimal(11, 0),
        PHRDUEAMT as decimal(11, 0),
        PHRWITHHLD as decimal(11, 0),
        PHRPPRCS as string,
        PHRPRCST as string,
        PHRPTPS as string,
        PHRPTPST as string,
        PHRCOPAYSC as string,
        PHRCOPAYSS as decimal(2, 0),
        PHRFCOPAY as decimal(11, 0),
        PHRPCOPAY as decimal(11, 0),
        PHRCOPAY as decimal(11, 0),
        PHRPRODSEL as decimal(11, 0),
        PHRATRTAX as decimal(11, 0),
        PHREXCEBFT as decimal(11, 0),
        PHRINCENTV as decimal(11, 0),
        PHRATRDED as decimal(11, 0),
        PHRCOB as decimal(11, 0),
        PHRTOTHAMT as decimal(11, 0),
        PHRPROFFEE as decimal(11, 0),
        PHROTHPAYA as decimal(11, 0),
        PHRCOSTSRC as string,
        PHRCOSTTYP as string,
        PHRPRCTYPE as string,
        PHRRATE as decimal(8, 0),
        PHRPROCFEE as decimal(11, 0),
        PHRPATSTAX as decimal(11, 0),
        PHRPLNSTAX as decimal(11, 0),
        PHRPRVNSEL as decimal(11, 0),
        PHRPSCBRND as decimal(11, 0),
        PHRPSCNONP as decimal(11, 0),
        PHRPSCBRNP as decimal(11, 0),
        PHRCOVGAP as decimal(11, 0),
        PHRINGCSTC as decimal(11, 0),
        PHRDSPFEEC as decimal(11, 0),
        POSINGRCST as decimal(11, 0),
        POSDISPFEE as decimal(11, 0),
        POSPSLSTAX as decimal(11, 0),
        POSFSLSTAX as decimal(11, 0),
        POSSLSTAX as decimal(11, 0),
        POSPATPAY as decimal(11, 0),
        POSDUEAMT as decimal(11, 0),
        POSWITHHLD as decimal(11, 0),
        POSCOPAY as decimal(11, 0),
        POSPRODSEL as decimal(11, 0),
        POSATRTAX as decimal(11, 0),
        POSEXCEBFT as decimal(11, 0),
        POSINCENTV as decimal(11, 0),
        POSATRDED as decimal(11, 0),
        POSTOTHAMT as decimal(11, 0),
        POSPROFFEE as decimal(11, 0),
        POSOTHPAYA as decimal(11, 0),
        POSCOSTSRC as string,
        POSPROCFEE as decimal(11, 0),
        POSPATSTAX as decimal(11, 0),
        POSPLNSTAX as decimal(11, 0),
        POSPRVNSEL as decimal(11, 0),
        POSPSCBRND as decimal(11, 0),
        POSPSCNONP as decimal(11, 0),
        POSPSCBRNP as decimal(11, 0),
        POSCOVGAP as decimal(11, 0),
        POSINGCSTC as decimal(11, 0),
        POSDSPFEEC as decimal(11, 0),
        CLIENTFLAG as string,
        CLTINGRCST as decimal(11, 0),
        CLTDISPFEE as decimal(11, 0),
        CLTSLSTAX as decimal(11, 0),
        CLTPATPAY as decimal(11, 0),
        CLTDUEAMT as decimal(11, 0),
        CLTWITHHLD as decimal(11, 0),
        CLTPRCS as string,
        CLTPRCST as string,
        CLTPTPS as string,
        CLTPTPST as string,
        CLTCOPAYS as string,
        CLTCOPAYSS as decimal(2, 0),
        CLTFCOPAY as decimal(11, 0),
        CLTPCOPAY as decimal(11, 0),
        CLTCOPAY as decimal(11, 0),
        CLTPRODSEL as decimal(11, 0),
        CLTPSTAX as decimal(11, 0),
        CLTFSTAX as decimal(11, 0),
        CLTATRTAX as decimal(11, 0),
        CLTEXCEBFT as decimal(11, 0),
        CLTINCENTV as decimal(11, 0),
        CLTATRDED as decimal(11, 0),
        CLTTOTHAMT as decimal(11, 0),
        CLTPROFFEE as decimal(11, 0),
        CLTCOB as decimal(11, 0),
        CLTOTHPAYA as decimal(11, 0),
        CLTCOSTSRC as string,
        CLTCOSTTYP as string,
        CLTPRCTYPE as string,
        CLTRATE as decimal(8, 0),
        CLTPRSCSTP as decimal(3, 0),
        CLTPRSCHNM as string,
        CLTPROCFEE as decimal(11, 0),
        CLTPATSTAX as decimal(11, 0),
        CLTPLNSTAX as decimal(11, 0),
        CLTPRVNSEL as decimal(11, 0),
        CLTPSCBRND as decimal(11, 0),
        CLTPSCNONP as decimal(11, 0),
        CLTPSCBRNP as decimal(11, 0),
        CLTCOVGAP as decimal(11, 0),
        CLTINGCSTC as decimal(11, 0),
        CLTDSPFEEC as decimal(11, 0),
        RSPREIMBUR as string,
        RSPINGRCST as decimal(11, 0),
        RSPDISPFEE as decimal(11, 0),
        RSPPSLSTAX as decimal(11, 0),
        RSPFSLSTAX as decimal(11, 0),
        RSPSLSTAX as decimal(11, 0),
        RSPPATPAY as decimal(11, 0),
        RSPDUEAMT as decimal(11, 0),
        RSPFCOPAY as decimal(11, 0),
        RSPPCOPAY as decimal(11, 0),
        RSPCOPAY as decimal(11, 0),
        RSPPRODSEL as decimal(11, 0),
        RSPATRTAX as decimal(11, 0),
        RSPEXCEBFT as decimal(11, 0),
        RSPINCENTV as decimal(11, 0),
        RSPATRDED as decimal(11, 0),
        RSPTOTHAMT as decimal(11, 0),
        RSPPROFEE as decimal(11, 0),
        RSPOTHPAYA as decimal(11, 0),
        RSPACCUDED as decimal(11, 0),
        RSPREMBFT as decimal(11, 0),
        RSPREMDED as decimal(11, 0),
        RSPPROCFEE as decimal(11, 0),
        RSPPATSTAX as decimal(11, 0),
        RSPPLNSTAX as decimal(11, 0),
        RSPPRVNSEL as decimal(11, 0),
        RSPPSCBRND as decimal(11, 0),
        RSPPSCNONP as decimal(11, 0),
        RSPPSCBRNP as decimal(11, 0),
        RSPCOVGAP as decimal(11, 0),
        RSPINGCSTC as decimal(11, 0),
        RSPDSPFEEC as decimal(11, 0),
        RSPPLANID as string,
        BENSTGQL1 as string,
        BENSTGAMT1 as decimal(11, 0),
        BENSTGQL2 as string,
        BENSTGAMT2 as decimal(11, 0),
        BENSTGQL3 as string,
        BENSTGAMT3 as decimal(11, 0),
        BENSTGQL4 as string,
        BENSTGAMT4 as decimal(11, 0),
        ESTGENSAV as decimal(11, 0),
        SPDACCTREM as decimal(11, 0),
        HLTHPLNAMT as decimal(11, 0),
        INDDEDPTD as decimal(11, 0),
        INDDEDREM as decimal(11, 0),
        FAMDEDPTD as decimal(11, 0),
        FAMDEDREM as decimal(11, 0),
        DEDSCHED as string,
        DEDACCC as string,
        DEDFLAG as string,
        INDLBFTUT_1 as decimal(11, 0),
        INDLBFTPTD as decimal(11, 0),
        INDLBFTREM as decimal(11, 0),
        INDLBFTUT_2 as decimal(11, 0),
        FAMLBFTPTD as decimal(11, 0),
        FAMLBFTREM as decimal(11, 0),
        LFTBFTMSCH as string,
        LFTBFTACCC as string,
        LFTBFTFLAG as string,
        INDBFTUT as decimal(11, 0),
        INDBMAXPTD as decimal(11, 0),
        FAMBFTUT as decimal(11, 0),
        FAMBMAXPTD as decimal(11, 0),
        INDBMAXREM as decimal(11, 0),
        FAMBMAXREM as decimal(11, 0),
        BFTMAXSCHD as string,
        BFTMAXACCC as string,
        BFTMAXFLAG as string,
        INDOOPPTD as decimal(11, 0),
        FAMOOPPTD as decimal(11, 0),
        INDOOPREM as decimal(11, 0),
        FAMOOPREM as decimal(11, 0),
        OOPSCHED as string,
        OOPACCC as string,
        OOPFLAG as string,
        CONTRIBUT as decimal(11, 0),
        CONTBASIS as string,
        CONTSCHED as string,
        CONTACCCD as string,
        CONTFLAG as string,
        RXTFLAG as string,
        REIMBURSMT as string,
        CLMORIGIN as string,
        HLDCLMFLAG as string,
        HLDCLMDAYS as decimal(3, 0),
        PARTDFLAG as string,
        COBEXTFLG as string,
        PAEXTFLG as string,
        HSAEXTIND as string,
        FFPMEDRMST as string,
        FFPMEDPXST as string,
        FFPMEDMSST as string,
        INCIDENTID as string,
        ETCNBR as string,
        DTEINJURY as decimal(8, 0),
        ADDUSER as string,
        CHGUSER as string,
        DMRUSERID as string,
        PRAUSERID as string,
        CLAIMREFID as string,
        EOBDNOV as string,
        EOBPDOV as string,
        MANTRKNBR as string,
        MANRECVDTE as decimal(8, 0),
        PASAUTHTYP as string,
        PASAUTHID as string,
        PASREQTYPE as string,
        PASREQFROM as decimal(8, 0),
        PASREQTHRU as decimal(8, 0),
        PASBASISRQ as string,
        PASREPFN as string,
        PASREPLN as string,
        PASSTREET as string,
        PASCITY as string,
        PASSTATE as string,
        PASZIP as string,
        PASPANBR as string,
        PASAUTHNBR as string,
        PASSDOCCT as decimal(3, 0),
        PAYERTYPE as string,
        DELAYRSNCD as string,
        MEDCDIND as string,
        MEDCDID as string,
        MEDCDAGNBR as string,
        MEDCDTCN as string,
        FMSTIER as string,
        FMSSTATUS as string,
        FMSDFLTIND as string,
        FMSBENLST as string,
        FMSLSTLVL3 as string,
        FMSLSTLVL2 as string,
        FMSLSTLVL1 as string,
        FMSRULESET as string,
        FMSRULE as string,
        FMSPROCCD as string,
        CLIENTDEF1 as string,
        CLIENTDEF2 as string,
        CLIENTDEF3 as string,
        CLIENTDEF4 as string,
        CLIENTDEF5 as string,
        CCTRESERV1 as string,
        CCTRESERV2 as string,
        CCTRESERV3 as string,
        CCTRESERV4 as string,
        CCTRESERV5 as string,
        CCTRESERV6 as string,
        CCTRESERV7 as string,
        CCTRESERV8 as string,
        CCTRESERV9 as string,
        CCTRESRV10 as string,
        OOPAPPLIED as decimal(11, 0),
        CCTRESRV12 as decimal(11, 0),
        CCTRESRV13 as decimal(11, 0),
        CCTRESRV14 as decimal(11, 0),
        USERFIELD as string,
        EXTRACTDTE as decimal(8, 0),
        BATCHCTRL as string,
        CLTTYPUCST as decimal(14, 0),
        REJCDE4 as string,
        REJCDE5 as string,
        REJCDE6 as string,
        MESSAGECD4 as string,
        MESSAGE4 as string,
        MESSAGECD5 as string,
        MESSAGE5 as string,
        MESSAGECD6 as string,
        MESSAGEG6 as string,
        CLIENT2FLAG as string,
        CLT2INGRCST as decimal(11, 0),
        CLT2DISPFEE as decimal(11, 0),
        CLT2SLSTAX as decimal(11, 0),
        CLT2DUEAMT as decimal(11, 0),
        CLTPRSCSTP2 as decimal(3, 0),
        CLTPRSCHNM2 as decimal(18, 0),
        CLT2PRCS as string,
        CLT2PRCST as string,
        CLT2PSTAX as decimal(11, 0),
        CLT2FSTAX as decimal(11, 0),
        CLT2OTHAMT as decimal(11, 0),
        CLT2COSTSRC as string,
        CLT2COSTTYP as string,
        CLT2PRCTYPE as string,
        CLT2RATE as decimal(8, 0),
        CLT2DCCSCID as string,
        CLT2DCCSCSQ as decimal(3, 0),
        CLT2DCSCHID as string,
        CLT2DCSCHSQ as decimal(3, 0),
        CALINCENTV2 as decimal(11, 0),
        CLNT3FLAG as string,
        CLT3INGRCST as decimal(11, 0),
        CLT3DISPFEE as decimal(11, 0),
        CLT3SLSTAX as decimal(11, 0),
        CLT3DUEAMT as decimal(11, 0),
        CLTPRSCSTP3 as decimal(3, 0),
        CLTPRSCHNM3 as decimal(18, 0),
        CLT3PRCS as string,
        CLT3PRCST as string,
        CLT3PSTAX as decimal(11, 0),
        CLT3FSTAX as decimal(11, 0),
        CLT3OTHAMT as decimal(11, 0),
        CLT3COSTSRC as string,
        CLT3COSTTYP as string,
        CLT3PRCTYPE as string,
        CLT3RATE as decimal(8, 0),
        CLT3DCCSCID as string,
        CLT3DCCSCSQ as decimal(3, 0),
        CLT3DCSCHID as string,
        CLT3DCSCHSQ as decimal(3, 0),
        CALINCENTV3 as decimal(11, 0),
        TBR_TBR_REJECT_REASON1 as string,
        TBR_TBR_REJECT_REASON2 as string,
        TBR_TBR_REJECT_REASON3 as string,
        SBM_QTY_INT_DIS as string,
        SBM_DS_INT_DIS as string,
        OTHR_AMT_CLMSBM_CT as string,
        OTHR_AMT_CLMSBM_QLFR1 as string,
        OTHR_AMT_CLMSBM1 as string,
        OTHR_AMT_CLMSBM_QLFR2 as string,
        OTHR_AMT_CLMSBM2 as string,
        OTHR_AMT_CLMSBM_QLFR3 as string,
        OTHR_AMT_CLMSBM3 as string,
        OTHR_AMT_CLMSBM_QLFR4 as string,
        OTHR_AMT_CLMSBM4 as string,
        OTHR_AMT_CLMSBM_QLFR5 as string,
        OTHR_AMT_CLMSBM5 as string,
        RSN_SRVCDE1 as string,
        RSN_SRVCDE2 as string,
        RSN_SRVCDE3 as string,
        PROF_SRVCDE1 as string,
        PROF_SRVCDE2 as string,
        PROF_SRVCDE3 as string,
        RESULT_OF_SRV_CDE1 as string,
        RESULT_OF_SRV_CDE2 as string,
        RESULT_OF_SRV_CDE3 as string,
        SCH_RX_ID_NBR as string,
        BASIS_CAL_DISPFEE as string,
        BASIS_CAL_COPAY as string,
        BASIS_CAL_FLT_TAX as string,
        BASIS_CAL_PRCNT_TAX as string,
        BASIS_CAL_COINSUR as string,
        PAID_MSG_CODE_COUNT as string,
        PAID_MESSAGE_CODE_1 as string,
        PAID_MESSAGE_CODE_2 as string,
        PAID_MESSAGE_CODE_3 as string,
        PAID_MESSAGE_CODE_4 as string,
        PAID_MESSAGE_CODE_5 as string,
        FILLER_FOR_REJECT_FIELD_IND as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/landing/' + $InFile")}),
      fileName: ({ctx.getFileName("$FilePath + '/landing/' + $InFile")}) ,
      columnDelimiter: '|',
      columnNamesAsHeader:false) ~> OptumDetailSeq
    OptumDetailSeq
        filter(
            length(npw_DS_TRIM_1(GROUPID)) > 0
        ) ~> DataCleansingV168S9P4filter
    DataCleansingV168S9P4filter
        derive(
              GROUPID = npw_DS_TRIM_1(GROUPID),
              CLTINGRCST =  case((isNull(CLTINGRCST) == true() || length(npw_DS_TRIM_1(CLTINGRCST)) == 0), "0.0", CLTINGRCST),
              CLTPATPAY =  case((isNull(CLTPATPAY) == true() || length(npw_DS_TRIM_1(CLTPATPAY)) == 0), "0.0", CLTPATPAY),
              CLTDUEAMT =  case((isNull(CLTDUEAMT) == true() || length(npw_DS_TRIM_1(CLTDUEAMT)) == 0), "0.0", CLTDUEAMT)) ~> DataCleansingV168S9P4Derived
        DataCleansingV168S9P4Derived
        select(mapColumn(
            GROUPID,
            CLTINGRCST,
            CLTPATPAY,
            CLTDUEAMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> DataCleansingV168S9P4
    DataCleansingV168S9P4
      select(mapColumn(
      ),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true) ~> DetailGroupLevelAggr
    OptumDetailSeq
        derive(
              ID = 1,
              CLTINGRCST =  case((isNull(CLTINGRCST) == true() || length(npw_DS_TRIM_1(CLTINGRCST)) == 0), "0.0", CLTINGRCST),
              CLTPATPAY =  case((isNull(CLTPATPAY) == true() || length(npw_DS_TRIM_1(CLTPATPAY)) == 0), "0.0", CLTPATPAY),
              CLTDUEAMT =  case((isNull(CLTDUEAMT) == true() || length(npw_DS_TRIM_1(CLTDUEAMT)) == 0), "0.0", CLTDUEAMT)) ~> DataCleansingV168S9P8Derived
        DataCleansingV168S9P8Derived
        select(mapColumn(
            ID,
            CLTINGRCST,
            CLTPATPAY,
            CLTDUEAMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> DataCleansingV168S9P8
    DataCleansingV168S9P8
      select(mapColumn(
      ),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true) ~> DetailAmountLevelAggr
    source(output(
        DATE as string,
        GROUP as string,
        TOTAL_RECORDS as decimal(9, 0),
        INGREDIENT_COST as decimal(13, 0),
        AMT_COPAY as decimal(13, 0),
        AMT_PAYMENT as decimal(13, 0),
        QUANTITY as decimal(9, 0)),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/verified/' + $ControlInFile + '.temp'")}),
      fileName: ({ctx.getFileName("$FilePath + '/verified/' + $ControlInFile + '.temp'")}) ,
      columnDelimiter: '|',
      columnNamesAsHeader:false) ~> OptumControlSeq
    OptumControlSeq
        derive(
              ID = 1,
              INGREDIENT_COST = translate(INGREDIENT_COST, "() ", ""),
              AMT_COPAY = translate(AMT_COPAY, "() ", ""),
              AMT_PAYMENT = translate(AMT_PAYMENT, "() ", "")) ~> ControlFileXfrV0S448P5Derived
        ControlFileXfrV0S448P5Derived
        select(mapColumn(
            ID,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> ControlFileXfrV0S448P5
    ControlFileXfrV0S448P5
      select(mapColumn(
      ),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true) ~> AmountLevelAggr
    AmountLevelAggr
        derive(
              ID = ID,
              INGREDIENT_COST = INGREDIENT_COST,
              AMT_COPAY = AMT_COPAY,
              AMT_PAYMENT = AMT_PAYMENT) ~> AmountLevelHashFileDerived
        AmountLevelHashFileDerived
        select(mapColumn(
            ID,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> AmountLevelHashFile
    {ctx.joinScript(joinType='left',
      master=['DetailAmountLevelAggr', ''],
      transformationName='AmountsLevelLogicXfrlookup',
      references=[['AmountLevelHashFile', ctx.joinConditionForLookup(master={'name':'DetailAmountLevelAggr','alias':'DetailAmountLevel_Lnk','types':{}}, reference={'name':'AmountLevelHashFile','alias':'AmoutLevelCompare_lkp','types':{'ID':'string','INGREDIENT_COST':'decimal(11, 2)','AMT_COPAY':'decimal(11, 2)','AMT_PAYMENT':'decimal(11, 2)'}}, joinFields=[{'master':'DetailAmountLevel_Lnk.ID','reference':'AmoutLevelCompare_lkp.ID', 'operator':'=='},{'master':'DetailAmountLevel_Lnk.CLTINGRCST','reference':'AmoutLevelCompare_lkp.INGREDIENT_COST', 'operator':'=='}]), 'ID,INGREDIENT_COST,AMT_COPAY,AMT_PAYMENT']],
      schema="AmountLevel_Lnk_CLTINGRCST,AmountLevel_Lnk_CLTPATPAY,AmountLevel_Lnk_CLTDUEAMT,AmountLevel_Lnk_INGREDIENT_COST,AmountLevel_Lnk_AMT_COPAY,AmountLevel_Lnk_AMT_PAYMENT,filter_AmountLevel_Lnk",
      map=['DetailAmountLevel_Lnk.CLTINGRCST','DetailAmountLevel_Lnk.CLTPATPAY','DetailAmountLevel_Lnk.CLTDUEAMT','AmoutLevelCompare_lkp.INGREDIENT_COST','AmoutLevelCompare_lkp.AMT_COPAY','AmoutLevelCompare_lkp.AMT_PAYMENT','case((DetailAmountLevel_Lnk.ID == AmoutLevelCompare_lkp.ID && (((isNull(DetailAmountLevel_Lnk.CLTINGRCST) && !isNull(AmoutLevelCompare_lkp.INGREDIENT_COST)) || (!isNull(DetailAmountLevel_Lnk.CLTINGRCST) && isNull(AmoutLevelCompare_lkp.INGREDIENT_COST)) || DetailAmountLevel_Lnk.CLTINGRCST != AmoutLevelCompare_lkp.INGREDIENT_COST) || ((isNull(DetailAmountLevel_Lnk.CLTPATPAY) && !isNull(AmoutLevelCompare_lkp.AMT_COPAY)) || (!isNull(DetailAmountLevel_Lnk.CLTPATPAY) && isNull(AmoutLevelCompare_lkp.AMT_COPAY)) || DetailAmountLevel_Lnk.CLTPATPAY != AmoutLevelCompare_lkp.AMT_COPAY) || ((isNull(DetailAmountLevel_Lnk.CLTDUEAMT) && !isNull(AmoutLevelCompare_lkp.AMT_PAYMENT)) || (!isNull(DetailAmountLevel_Lnk.CLTDUEAMT) && isNull(AmoutLevelCompare_lkp.AMT_PAYMENT)) || DetailAmountLevel_Lnk.CLTDUEAMT != AmoutLevelCompare_lkp.AMT_PAYMENT))), 1, 0)'],
      aliasFrom="DetailAmountLevel_Lnk,AmoutLevelCompare_lkp",
      aliasTo="DetailAmountLevelAggr,AmountLevelHashFile")}
    AmountsLevelLogicXfrlookup
        filter(
            filter_AmountLevel_Lnk == 1
        ) ~> AmountsLevelLogicXfrfilter
    AmountsLevelLogicXfrfilter
        derive(
              CLTINGRCST = AmountLevel_Lnk_CLTINGRCST,
              CLTPATPAY = AmountLevel_Lnk_CLTPATPAY,
              CLTDUEAMT = AmountLevel_Lnk_CLTDUEAMT,
              INGREDIENT_COST = AmountLevel_Lnk_INGREDIENT_COST,
              AMT_COPAY = AmountLevel_Lnk_AMT_COPAY,
              AMT_PAYMENT = AmountLevel_Lnk_AMT_PAYMENT) ~> AmountsLevelLogicXfrDerived
        AmountsLevelLogicXfrDerived
        select(mapColumn(
            CLTINGRCST,
            CLTPATPAY,
            CLTDUEAMT,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> AmountsLevelLogicXfr
    AmountsLevelLogicXfr sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/verified/Optum_reject_amtlevel_Land_' + $SrcSysCd + '.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/verified/Optum_reject_amtlevel_Land_' + $SrcSysCd + '.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> AmoutLevelCompareSeq
    OptumControlSeq
        derive(
              GROUP = npw_DS_TRIM_1(GROUP),
              TOTAL_RECORDS = npw_DS_TRIM_1(TOTAL_RECORDS),
              DATE = DATE,
              INGREDIENT_COST = translate(INGREDIENT_COST, "() ", ""),
              AMT_COPAY = translate(AMT_COPAY, "() ", ""),
              AMT_PAYMENT = translate(AMT_PAYMENT, "() ", ""),
              QUANTITY = QUANTITY) ~> ControlFileXfrV0S448P3Derived
        ControlFileXfrV0S448P3Derived
        select(mapColumn(
            GROUP,
            TOTAL_RECORDS,
            DATE,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT,
            QUANTITY
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> ControlFileXfrV0S448P3
    ControlFileXfrV0S448P3
        derive(
              GROUP = GROUP,
              TOTAL_RECORDS = TOTAL_RECORDS,
              DATE = DATE,
              INGREDIENT_COST = INGREDIENT_COST,
              AMT_COPAY = AMT_COPAY,
              AMT_PAYMENT = AMT_PAYMENT,
              QUANTITY = QUANTITY) ~> ControlGroupLevelHashFileDerived
        ControlGroupLevelHashFileDerived
        select(mapColumn(
            GROUP,
            TOTAL_RECORDS,
            DATE,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT,
            QUANTITY
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> ControlGroupLevelHashFile
    {ctx.joinScript(joinType='left',
      master=['DetailGroupLevelAggr', ''],
      transformationName='GroupLevelLogicXfrlookup',
      references=[['ControlGroupLevelHashFile', ctx.joinConditionForLookup(master={'name':'DetailGroupLevelAggr','alias':'DetailGroupLevel_lkp','types':{}}, reference={'name':'ControlGroupLevelHashFile','alias':'ControlGroupLevel_lkp','types':{'GROUP':'string','TOTAL_RECORDS':'string','DATE':'string','INGREDIENT_COST':'decimal(11, 2)','AMT_COPAY':'decimal(11, 2)','AMT_PAYMENT':'decimal(11, 2)','QUANTITY':'string'}}, joinFields=[{'master':'DetailGroupLevel_lkp.GROUPID','reference':'ControlGroupLevel_lkp.GROUP', 'operator':'=='},{'master':'DetailGroupLevel_lkp.GROUPID_CNT','reference':'ControlGroupLevel_lkp.TOTAL_RECORDS', 'operator':'=='},{'master':'DetailGroupLevel_lkp.CLTINGRCST','reference':'ControlGroupLevel_lkp.INGREDIENT_COST', 'operator':'=='}]), 'GROUP,TOTAL_RECORDS,DATE,INGREDIENT_COST,AMT_COPAY,AMT_PAYMENT,QUANTITY']],
      schema="GroupLevel_Lnk_GROUPID,GroupLevel_Lnk_GROUPID_CNT,GroupLevel_Lnk_CLTINGRCST,GroupLevel_Lnk_CLTPATPAY,GroupLevel_Lnk_CLTDUEAMT,GroupLevel_Lnk_DATE,GroupLevel_Lnk_GROUP,GroupLevel_Lnk_TOTAL_RECORDS,GroupLevel_Lnk_INGREDIENT_COST,GroupLevel_Lnk_AMT_COPAY,GroupLevel_Lnk_AMT_PAYMENT,GroupLevel_Lnk_QUANTITY,filter_GroupLevel_Lnk",
      map=['DetailGroupLevel_lkp.GROUPID','DetailGroupLevel_lkp.GROUPID_CNT','DetailGroupLevel_lkp.CLTINGRCST','DetailGroupLevel_lkp.CLTPATPAY','DetailGroupLevel_lkp.CLTDUEAMT','ControlGroupLevel_lkp.DATE','ControlGroupLevel_lkp.GROUP','ControlGroupLevel_lkp.TOTAL_RECORDS','ControlGroupLevel_lkp.INGREDIENT_COST','ControlGroupLevel_lkp.AMT_COPAY','ControlGroupLevel_lkp.AMT_PAYMENT','ControlGroupLevel_lkp.QUANTITY','case((DetailGroupLevel_lkp.GROUPID == ControlGroupLevel_lkp.GROUP && (((isNull(DetailGroupLevel_lkp.GROUPID_CNT) && !isNull(ControlGroupLevel_lkp.TOTAL_RECORDS)) || (!isNull(DetailGroupLevel_lkp.GROUPID_CNT) && isNull(ControlGroupLevel_lkp.TOTAL_RECORDS)) || DetailGroupLevel_lkp.GROUPID_CNT != ControlGroupLevel_lkp.TOTAL_RECORDS) || ((isNull(DetailGroupLevel_lkp.CLTINGRCST) && !isNull(ControlGroupLevel_lkp.INGREDIENT_COST)) || (!isNull(DetailGroupLevel_lkp.CLTINGRCST) && isNull(ControlGroupLevel_lkp.INGREDIENT_COST)) || DetailGroupLevel_lkp.CLTINGRCST != ControlGroupLevel_lkp.INGREDIENT_COST) || ((isNull(DetailGroupLevel_lkp.CLTPATPAY) && !isNull(ControlGroupLevel_lkp.AMT_COPAY)) || (!isNull(DetailGroupLevel_lkp.CLTPATPAY) && isNull(ControlGroupLevel_lkp.AMT_COPAY)) || DetailGroupLevel_lkp.CLTPATPAY != ControlGroupLevel_lkp.AMT_COPAY) || ((isNull(DetailGroupLevel_lkp.CLTDUEAMT) && !isNull(ControlGroupLevel_lkp.AMT_PAYMENT)) || (!isNull(DetailGroupLevel_lkp.CLTDUEAMT) && isNull(ControlGroupLevel_lkp.AMT_PAYMENT)) || DetailGroupLevel_lkp.CLTDUEAMT != ControlGroupLevel_lkp.AMT_PAYMENT))), 1, 0)'],
      aliasFrom="DetailGroupLevel_lkp,ControlGroupLevel_lkp",
      aliasTo="DetailGroupLevelAggr,ControlGroupLevelHashFile")}
    GroupLevelLogicXfrlookup
        filter(
            filter_GroupLevel_Lnk == 1
        ) ~> GroupLevelLogicXfrfilter
    GroupLevelLogicXfrfilter
        derive(
              GROUPID = GroupLevel_Lnk_GROUPID,
              GROUPID_CNT = GroupLevel_Lnk_GROUPID_CNT,
              CLTINGRCST = GroupLevel_Lnk_CLTINGRCST,
              CLTPATPAY = GroupLevel_Lnk_CLTPATPAY,
              CLTDUEAMT = GroupLevel_Lnk_CLTDUEAMT,
              DATE = GroupLevel_Lnk_DATE,
              GROUP = GroupLevel_Lnk_GROUP,
              TOTAL_RECORDS = GroupLevel_Lnk_TOTAL_RECORDS,
              INGREDIENT_COST = GroupLevel_Lnk_INGREDIENT_COST,
              AMT_COPAY = GroupLevel_Lnk_AMT_COPAY,
              AMT_PAYMENT = GroupLevel_Lnk_AMT_PAYMENT,
              QUANTITY = GroupLevel_Lnk_QUANTITY) ~> GroupLevelLogicXfrDerived
        GroupLevelLogicXfrDerived
        select(mapColumn(
            GROUPID,
            GROUPID_CNT,
            CLTINGRCST,
            CLTPATPAY,
            CLTDUEAMT,
            DATE,
            GROUP,
            TOTAL_RECORDS,
            INGREDIENT_COST,
            AMT_COPAY,
            AMT_PAYMENT,
            QUANTITY
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> GroupLevelLogicXfr
    GroupLevelLogicXfr sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/verified/Optum_reject_grouplevel_Land_' + $SrcSysCd + '.dat.' + $RunID""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/verified/Optum_reject_grouplevel_Land_' + $SrcSysCd + '.dat.' + $RunID""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:true) ~> GroupLevelCompareSeq""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfOptumControlReconExtr()
  activityName = "OptumControlReconExtr"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "SrcSysCdSK":  {"value": "'@{pipeline().parameters.SrcSysCdSK}'","type": "Expression"},
  "RunDate":  {"value": "'@{pipeline().parameters.RunDate}'","type": "Expression"},
  "InFile":  {"value": "'@{pipeline().parameters.InFile}'","type": "Expression"},
  "ControlInFile":  {"value": "'@{pipeline().parameters.ControlInFile}'","type": "Expression"},
  "MbrTermDate":  {"value": "'@{pipeline().parameters.MbrTermDate}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'),
    depends_on = [ActivityDependency( activity = ctx.removeAlias("DSU.UnixSH"),
      dependency_conditions = [DependencyCondition.SUCCEEDED])])
  return artifacts, [activity,
    CustomActivity(
      name = ctx.removeAlias("DSU.UnixSH"),
      command = ctx.getRoutineCommand(ctx.removeAlias("DSU.UnixSH"), isSubroutine=True),
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service)),
    CustomActivity(
      name = ctx.removeAlias("DSU.HASH.CLEAR\\optumclm_controlfile,optumclm_controlfile_agg"),
      command = ctx.getRoutineCommand(ctx.removeAlias("DSU.HASH.CLEAR\\optumclm_controlfile,optumclm_controlfile_agg"), isSubroutine=True),
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service),
      depends_on = [ActivityDependency( activity = activityName,
        dependency_conditions = [DependencyCondition.COMPLETED])]),
    FailActivity(
      name = "Fail",
      message = "Dataflow execution failed",
      error_code = "1",
      depends_on = [ActivityDependency(activity = activityName,
        dependency_conditions = [DependencyCondition.FAILED]),
        ActivityDependency(activity = ctx.removeAlias("DSU.HASH.CLEAR\\optumclm_controlfile,optumclm_controlfile_agg"),
        dependency_conditions = [DependencyCondition.COMPLETED])])]

def OptumControlReconExtr(ctx):
  name = "OptumControlReconExtr"
  artifacts, activities = OptumControlReconExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/claim/Optum"),
    activities = activities,
    description = """
      Recon between optum claim detail and optum claim control file @ full rollup amounts level
      © Copyright 2019 Blue Cross/Blue Shield of Kansas City


      CALLED BY : OptumDrugLandSeq


      DESCRIPTION:      OptumRx Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim
                                     

      MODIFICATIONS:

           
      Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
      ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
      Sri Nannapaneni               2020-07-15     PBM Phase II                               Initial Programming                                                                                              IntegrateDev2		Abhiram Dasarathy	2021-05-07
      Parameters:
      -----------
      FilePath:
        File Path
      RunID:
        RunID
      SrcSysCd:
        SrcSysCd
        OPTUMRX
      SrcSysCdSK:
        SrcSysCdSK
        -1951778164
      RunDate:
        RunDate
      InFile:
        InFile
        OPTUMRX.PBM.DrugClm_20191122.dat
      ControlInFile:
        ControlInFile
      MbrTermDate:
        MbrTermDate
        Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years.""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "RunID": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSK": ParameterSpecification(type="Int"),
      "RunDate": ParameterSpecification(type="String"),
      "InFile": ParameterSpecification(type="String"),
      "ControlInFile": ParameterSpecification(type="String"),
      "MbrTermDate": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumControlReconExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
