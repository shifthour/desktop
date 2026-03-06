#!/usr/bin/python3

from npadf import *

def OptumIdsDrugClmPriceFkeyActivities(ctx):
  def dfOptumIdsDrugClmPriceFkey():
    def SRCDOMAINTRNSLTN():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='SRCDOMAINTRNSLTN',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def xfmSrcDomainTrnsltn():
       return DataflowSegment([
         Transformation(name='xfmSrcDomainTrnsltnDerived'),
         Transformation(name='xfmSrcDomainTrnsltn')])
    def db2dsDRUGCLM():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2dsDRUGCLM',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def SeqWDRUGCLMPRICE():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='SeqWDRUGCLMPRICE', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def xfmDrugClmPriceSrc():
       return DataflowSegment([
         Transformation(name='xfmDrugClmPriceSrcDerived'),
         Transformation(name='xfmDrugClmPriceSrc')])
    def LkupDrugClm():
       return DataflowSegment([
         Transformation(name="LkupDrugClm"),
         Transformation(name="LkupDrugClmJoin1"),Transformation(name="LkupDrugClmDerived1")])
    def db2CDMPPNGSK():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2CDMPPNGSK',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def fltrDataV493S0P6():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P6')])
    def fltrDataV493S0P5():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P5')])
    def fltrDataV493S0P8():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P8')])
    def fltrDataV493S0P7():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P7')])
    def fltrDataV493S0P10():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P10')])
    def LkpSrcDomain():
       return DataflowSegment([
         Transformation(name="LkpSrcDomain"),
         Transformation(name="LkpSrcDomainJoin1"),Transformation(name="LkpSrcDomainDerived1")])
    def fltrDataV493S0P9():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P9')])
    def fltrDataV493S0P2():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P2')])
    def fltrDataV493S0P4():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P4')])
    def fltrDataV493S0P3():
       return DataflowSegment([
         Transformation(name='fltrDataV493S0P3')])
    def LkupFkey():
       return DataflowSegment([
         Transformation(name="LkupFkey"),
         Transformation(name="LkupFkeyJoin1"),Transformation(name="LkupFkeyDerived1"),
         Transformation(name="LkupFkeyJoin2"),Transformation(name="LkupFkeyDerived2"),
         Transformation(name="LkupFkeyJoin3"),Transformation(name="LkupFkeyDerived3"),
         Transformation(name="LkupFkeyJoin4"),Transformation(name="LkupFkeyDerived4"),
         Transformation(name="LkupFkeyJoin5"),Transformation(name="LkupFkeyDerived5"),
         Transformation(name="LkupFkeyJoin6"),Transformation(name="LkupFkeyDerived6"),
         Transformation(name="LkupFkeyJoin7"),Transformation(name="LkupFkeyDerived7"),
         Transformation(name="LkupFkeyJoin8"),Transformation(name="LkupFkeyDerived8"),
         Transformation(name="LkupFkeyJoin9"),Transformation(name="LkupFkeyDerived9")])
    def db2dsFRMLRY():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2dsFRMLRY',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def LkpFrmly():
       return DataflowSegment([
         Transformation(name="LkpFrmly"),
         Transformation(name="LkpFrmlyJoin1"),Transformation(name="LkpFrmlyDerived1")])
    def xfmCheckLkpResults():
       return DataflowSegment([
         Transformation(name='xfmCheckLkpResultsDerived'),
         Transformation(name='xfmCheckLkpResults')])
    def SeqDrugClmPriceFkey():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='SeqDrugClmPriceFkey', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "OptumIdsDrugClmPriceFkey"
    buffer = DataflowBuffer()
    buffer.append(SRCDOMAINTRNSLTN())
    buffer.append(xfmSrcDomainTrnsltn())
    buffer.append(db2dsDRUGCLM())
    buffer.append(SeqWDRUGCLMPRICE())
    buffer.append(xfmDrugClmPriceSrc())
    buffer.append(LkupDrugClm())
    buffer.append(db2CDMPPNGSK())
    buffer.append(fltrDataV493S0P6())
    buffer.append(fltrDataV493S0P5())
    buffer.append(fltrDataV493S0P8())
    buffer.append(fltrDataV493S0P7())
    buffer.append(fltrDataV493S0P10())
    buffer.append(LkpSrcDomain())
    buffer.append(fltrDataV493S0P9())
    buffer.append(fltrDataV493S0P2())
    buffer.append(fltrDataV493S0P4())
    buffer.append(fltrDataV493S0P3())
    buffer.append(LkupFkey())
    buffer.append(db2dsFRMLRY())
    buffer.append(LkpFrmly())
    buffer.append(xfmCheckLkpResults())
    buffer.append(SeqDrugClmPriceFkey())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/DrugClmPrice"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSPW as string ,
        IDSAcct as string ,
        UWSOwner as string ("{ctx.UWSOwner}"),
        UWSDB as string ("{ctx.UWSDB}"),
        UWSAcct as string ,
        UWSPW as string ,
        SrcSysCd as string ,
        RunID as string ,
        IDSRunCycle as string ,
        CurrentDate as string 
      }}
    source(output(
        SRC_DOMAIN_TX as string,
        TRGT_DOMAIN_TX as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT SRC_DOMAIN_TX, TRGT_DOMAIN_TX \\nFROM " + toString($UWSOwner) + ".SRC_DOMAIN_TRNSLTN \\nWHERE EFF_DT_SK <= '" + toString($CurrentDate) + "' \\nAND TERM_DT_SK >= '" + toString($CurrentDate) + "'\\nAND SRC_SYS_CD = '" + toString($SrcSysCd) + "'\\nAND DOMAIN_ID = 'PLAN_DRUG_ST' \\n--AND SRC_DOMAIN_TX in ('3','5','7','8','9','A','a','B','b','C','c','D','E','e','F','f','G','g','H','h','J','K','L','M','N','O','P','Q','R','S','s','T','t','U','V','W','x','Y','y','Z','z')\\n;"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> SRCDOMAINTRNSLTN
    SRCDOMAINTRNSLTN
        derive(
              SRC_DOMAIN_TX = SRC_DOMAIN_TX,
              TRGT_DOMAIN_TX = TRGT_DOMAIN_TX) ~> xfmSrcDomainTrnsltnDerived
        xfmSrcDomainTrnsltnDerived
        select(mapColumn(
            SRC_DOMAIN_TX,
            TRGT_DOMAIN_TX
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> xfmSrcDomainTrnsltn
    source(output(
        DRUG_CLM_SK as integer,
        SRC_SYS_CD_SK as integer,
        CLM_ID2 as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer,
        DISPNS_FEE_AMT as decimal(13, 2),
        INGR_CST_CHRGD_AMT as decimal(13, 2),
        GNRC_DRUG_IN as string,
        SLS_TAX_AMT as decimal(13, 2)),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("Select DRUG_CLM_SK, SRC_SYS_CD_SK, CLM_ID as CLM_ID2 , CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, DISPNS_FEE_AMT, INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT  from " + toString($IDSOwner) + ".DRUG_CLM Where CLM_ID= ORCHESTRATE.CLM_ID and SRC_SYS_CD_SK = ORCHESTRATE.SRC_SYS_CD_SK"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2dsDRUGCLM
    source(output(
        SRC_SYS_CD as string,
        SRC_SYS_CD_SK as integer,
        CLM_ID as string,
        RXCLMNBR as decimal(15, 0),
        CLMSEQNBR as decimal(3, 0),
        CLAIMSTS as string,
        ORGPDSBMDT as decimal(8, 0),
        METRICQTY as decimal(6, 0),
        GPINUMBER as string,
        GENINDOVER as string,
        CPQSPCPRG as string,
        CPQSPCPGIN as string,
        FNLPLANCDE as string,
        FNLPLANDTE as decimal(7, 0),
        FORMLRFLAG as string,
        AWPUNITCST as decimal(13, 5),
        WACUNITCST as decimal(13, 5),
        CTYPEUCOST as decimal(13, 5),
        PROQTY as decimal(12, 0),
        CLTCOSTTYP as string,
        CLTPRCTYPE as string,
        CLTRATE as decimal(7, 2),
        CLTTYPUCST as decimal(13, 5),
        CLT2INGRCST as decimal(13, 2),
        CLT2DISPFEE as decimal(13, 2),
        CLT2COSTSRC as string,
        CLT2COSTTYP as string,
        CLT2PRCTYPE as string,
        CLT2RATE as decimal(7, 2),
        RXNETWRKQL as string,
        CLT2SLSTAX as decimal(13, 2),
        CLT2PSTAX as decimal(13, 2),
        CLT2FSTAX as decimal(13, 2),
        CLT2DUEAMT as decimal(13, 2),
        CLT2OTHAMT as decimal(13, 2),
        CLTINCENTV as decimal(13, 2),
        CALINCENTV2 as decimal(13, 2),
        CLTOTHPAYA as decimal(13, 2),
        CLTTOTHAMT as decimal(13, 2),
        CLNTDEF2 as string,
        TIER_ID as string,
        DRUG_TYP_CD as string,
        PLN_DRUG_STTUS_CD as string,
        PLANTYPE as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/verified/DrugClmPrice_Land_' + $SrcSysCd + '.dat.' + $RunID")}),
      fileName: ({ctx.getFileName("$FilePath + '/verified/DrugClmPrice_Land_' + $SrcSysCd + '.dat.' + $RunID")}) ,
      columnDelimiter: ',',
      rowDelimiter: '\\n',
      columnNamesAsHeader:true) ~> SeqWDRUGCLMPRICE
    SeqWDRUGCLMPRICE
        derive(
              SRC_SYS_CD = SRC_SYS_CD,
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = CLM_ID,
              RXCLMNBR = trim(RXCLMNBR),
              CLMSEQNBR = trim(CLMSEQNBR),
              CLAIMSTS = trim(CLAIMSTS),
              ORGPDSBMDT =  case(length(trim(ORGPDSBMDT)) == 0 || npw_DS_TRIM_1(ORGPDSBMDT) == 0 || isNull(ORGPDSBMDT), "1753-01-01", trim(ORGPDSBMDT)),
              METRICQTY = trim(METRICQTY),
              GPINUMBER = trim(GPINUMBER),
              GENINDOVER = trim(GENINDOVER),
              CPQSPCPRG = trim(CPQSPCPRG),
              CPQSPCPGIN = trim(CPQSPCPGIN),
              FNLPLANCDE = trim(FNLPLANCDE),
              FNLPLANDTE =  case(length(trim(FNLPLANDTE)) == 0 || npw_DS_TRIM_1(FNLPLANDTE) == 0, "1753-01-01", trim(FNLPLANDTE)),
              FORMLRFLAG = trim(FORMLRFLAG),
              AWPUNITCST = trim(AWPUNITCST),
              WACUNITCST = trim(WACUNITCST),
              CTYPEUCOST = trim(CTYPEUCOST),
              PROQTY = trim(PROQTY),
              CLTCOSTTYP = npw_DS_TRIM_1(CLTCOSTTYP),
              CLTPRCTYPE = trim(CLTPRCTYPE),
              CLTRATE = trim(CLTRATE),
              CLTTYPUCST = trim(CLTTYPUCST),
              CLT2INGRCST = trim(CLT2INGRCST),
              CLT2DISPFEE = trim(CLT2DISPFEE),
              CLT2COSTSRC = trim(CLT2COSTSRC),
              CLT2COSTTYP = trim(CLT2COSTTYP),
              CLT2PRCTYPE = trim(CLT2PRCTYPE),
              CLT2RATE = trim(CLT2RATE),
              RXNETWRKQL = npw_DS_TRIM_1(RXNETWRKQL),
              CLT2SLSTAX = CLT2SLSTAX,
              CLT2PSTAX = CLT2PSTAX,
              CLT2FSTAX = CLT2FSTAX,
              CLT2DUEAMT = CLT2DUEAMT,
              CLT2OTHAMT = CLT2OTHAMT,
              CLTINCENTV = CLTINCENTV,
              CALINCENTV2 = CALINCENTV2,
              CLTOTHPAYA = CLTOTHPAYA,
              CLTOTHAMT = CLTTOTHAMT,
              CLNTDEF2 = CLNTDEF2,
              TIER_ID = TIER_ID,
              DRUG_TYP_CD = DRUG_TYP_CD,
              PLN_DRUG_STTUS_CD = PLN_DRUG_STTUS_CD,
              PLANTYPE = PLANTYPE,
              StageVar = :StageVar,
              SVClmId = :SVClmId,
              StageVar := {'ltrim(ORGPDSBMDT,"0")'.replace("Lnk_W_Drug_Clm_Price.","")},
              SVClmId := {' case(trim(CLAIMSTS) == "X", trim(RXCLMNBR) + trim(CLMSEQNBR) + "R", trim(RXCLMNBR) + trim(CLMSEQNBR))'.replace("Lnk_W_Drug_Clm_Price.","")}) ~> xfmDrugClmPriceSrcDerived
        xfmDrugClmPriceSrcDerived
        select(mapColumn(
            SRC_SYS_CD,
            SRC_SYS_CD_SK,
            CLM_ID,
            RXCLMNBR,
            CLMSEQNBR,
            CLAIMSTS,
            ORGPDSBMDT,
            METRICQTY,
            GPINUMBER,
            GENINDOVER,
            CPQSPCPRG,
            CPQSPCPGIN,
            FNLPLANCDE,
            FNLPLANDTE,
            FORMLRFLAG,
            AWPUNITCST,
            WACUNITCST,
            CTYPEUCOST,
            PROQTY,
            CLTCOSTTYP,
            CLTPRCTYPE,
            CLTRATE,
            CLTTYPUCST,
            CLT2INGRCST,
            CLT2DISPFEE,
            CLT2COSTSRC,
            CLT2COSTTYP,
            CLT2PRCTYPE,
            CLT2RATE,
            RXNETWRKQL,
            CLT2SLSTAX,
            CLT2PSTAX,
            CLT2FSTAX,
            CLT2DUEAMT,
            CLT2OTHAMT,
            CLTINCENTV,
            CALINCENTV2,
            CLTOTHPAYA,
            CLTOTHAMT,
            CLNTDEF2,
            TIER_ID,
            DRUG_TYP_CD,
            PLN_DRUG_STTUS_CD,
            PLANTYPE
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> xfmDrugClmPriceSrc
    {ctx.joinScript(joinType='inner',
      master=['xfmDrugClmPriceSrc', 'SRC_SYS_CD,SRC_SYS_CD_SK,CLM_ID,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,CLNTDEF2,TIER_ID,DRUG_TYP_CD,PLN_DRUG_STTUS_CD,PLANTYPE,StageVar,SVClmId'],
      transformationName='LkupDrugClm',
      references=[['db2dsDRUGCLM', ctx.joinConditionForLookup(master={'name':'xfmDrugClmPriceSrc','alias':'xfm_src_results','types':{'SRC_SYS_CD':'string','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string','StageVar':'string','SVClmId':'string'}}, reference={'name':'db2dsDRUGCLM','alias':'Lnk_DRUG_CLM_SK','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID2':'string','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)'}}, joinFields=[]), 'DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID2,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT']],
      schema="DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,CLNTDEF2,TIER_ID,DRUG_TYP_CD,PLN_DRUG_STTUS_CD,PLANTYPE",
      map=['Lnk_DRUG_CLM_SK.DRUG_CLM_SK','Lnk_DRUG_CLM_SK.SRC_SYS_CD_SK','xfm_src_results.CLM_ID','xfm_src_results.RXCLMNBR','xfm_src_results.CLMSEQNBR','xfm_src_results.CLAIMSTS','xfm_src_results.ORGPDSBMDT','xfm_src_results.METRICQTY','xfm_src_results.GPINUMBER','xfm_src_results.GENINDOVER','xfm_src_results.CPQSPCPRG','xfm_src_results.CPQSPCPGIN','xfm_src_results.FNLPLANCDE','xfm_src_results.FNLPLANDTE','xfm_src_results.FORMLRFLAG','xfm_src_results.AWPUNITCST','xfm_src_results.WACUNITCST','xfm_src_results.CTYPEUCOST','xfm_src_results.PROQTY','xfm_src_results.CLTCOSTTYP','xfm_src_results.CLTPRCTYPE','xfm_src_results.CLTRATE','xfm_src_results.CLTTYPUCST','xfm_src_results.CLT2INGRCST','xfm_src_results.CLT2DISPFEE','xfm_src_results.CLT2COSTSRC','xfm_src_results.CLT2COSTTYP','xfm_src_results.CLT2PRCTYPE','xfm_src_results.CLT2RATE','xfm_src_results.RXNETWRKQL','xfm_src_results.CLT2SLSTAX','xfm_src_results.CLT2PSTAX','xfm_src_results.CLT2FSTAX','xfm_src_results.CLT2DUEAMT','xfm_src_results.CLT2OTHAMT','Lnk_DRUG_CLM_SK.CRT_RUN_CYC_EXCTN_SK','Lnk_DRUG_CLM_SK.LAST_UPDT_RUN_CYC_EXCTN_SK','Lnk_DRUG_CLM_SK.DISPNS_FEE_AMT','Lnk_DRUG_CLM_SK.INGR_CST_CHRGD_AMT','Lnk_DRUG_CLM_SK.GNRC_DRUG_IN','Lnk_DRUG_CLM_SK.SLS_TAX_AMT','xfm_src_results.CLTINCENTV','xfm_src_results.CALINCENTV2','xfm_src_results.CLTOTHPAYA','xfm_src_results.CLTOTHAMT','xfm_src_results.CLNTDEF2','xfm_src_results.TIER_ID','xfm_src_results.DRUG_TYP_CD','xfm_src_results.PLN_DRUG_STTUS_CD','xfm_src_results.PLANTYPE'],
      aliasFrom="xfm_src_results,Lnk_DRUG_CLM_SK",
      aliasTo="xfmDrugClmPriceSrc,db2dsDRUGCLM")}
    source(output(
        CD_MPPNG_SK as integer,
        SRC_CD as string,
        SRC_CD_NM as string,
        SRC_CLCTN_CD as string,
        SRC_DRVD_LKUP_VAL as string,
        SRC_DOMAIN_NM as string,
        SRC_SYS_CD as string,
        TRGT_CD as string,
        TRGT_CD_NM as string,
        TRGT_CLCTN_CD as string,
        TRGT_DOMAIN_NM as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT DISTINCT\\nCD_MPPNG_SK,\\nSRC_CD,\\nSRC_CD_NM,\\nSRC_CLCTN_CD,\\nSRC_DRVD_LKUP_VAL,\\nSRC_DOMAIN_NM,\\nSRC_SYS_CD,\\nTRGT_CD,\\nTRGT_CD_NM,\\nTRGT_CLCTN_CD,\\nTRGT_DOMAIN_NM\\nFROM " + toString($IDSOwner) + ".CD_MPPNG"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2CDMPPNGSK
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "DRUG COST TYPE" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "DRUG COST TYPE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P6
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "DRUG PRICE TYPE" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "DRUG PRICE TYPE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P5
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "PHARMACY NETWORK QUALIFIER" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "PHARMACY NETWORK QUALIFIER"  && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P8
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "DRUG GENERIC OVERRIDE" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "DRUG GENERIC OVERRIDE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P7
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "PLAN DRUG STATUS"  && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "PLAN DRUG STATUS" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P10
    {ctx.joinScript(joinType='left',
      master=['xfmSrcDomainTrnsltn', 'SRC_DOMAIN_TX,TRGT_DOMAIN_TX'],
      transformationName='LkpSrcDomain',
      references=[['fltrDataV493S0P10', ctx.joinConditionForLookup(master={'name':'xfmSrcDomainTrnsltn','alias':'Lnk_Src_Domain_Trnsltn_out','types':{'SRC_DOMAIN_TX':'string','TRGT_DOMAIN_TX':'string'}}, reference={'name':'fltrDataV493S0P10','alias':'Lnk_PLN_DRUG_STTUS_CD','types':{'SRC_CD':'string','CD_MPPNG_SK':'integer'}}, joinFields=[{'master':'Lnk_Src_Domain_Trnsltn_out.TRGT_DOMAIN_TX','reference':'Lnk_PLN_DRUG_STTUS_CD.SRC_CD', 'operator':'=='}]), 'SRC_CD,CD_MPPNG_SK']],
      schema="CD_MPPNG_SK,SRC_DOMAIN_TX",
      map=['Lnk_PLN_DRUG_STTUS_CD.CD_MPPNG_SK','Lnk_Src_Domain_Trnsltn_out.SRC_DOMAIN_TX'],
      aliasFrom="Lnk_Src_Domain_Trnsltn_out,Lnk_PLN_DRUG_STTUS_CD",
      aliasTo="xfmSrcDomainTrnsltn,fltrDataV493S0P10")}
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "DRUG TYPE"  && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "DRUG TYPE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P9
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "SOURCE SYSTEM" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "SOURCE SYSTEM" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P2
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "DRUG COST TYPE" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "DRUG COST TYPE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P4
    db2CDMPPNGSK
        filter(
            (SRC_SYS_CD == "#SrcSysCd#" && SRC_DOMAIN_NM == "BUY COST SOURCE" && SRC_CLCTN_CD == "#SrcSysCd#" && TRGT_DOMAIN_NM == "BUY COST SOURCE" && TRGT_CLCTN_CD == "IDS")
        ) ~> fltrDataV493S0P3
    {ctx.joinScript(joinType='left',
      master=['LkupDrugClm', 'DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,CLNTDEF2,TIER_ID,DRUG_TYP_CD,PLN_DRUG_STTUS_CD,PLANTYPE'],
      transformationName='LkupFkey',
      references=[['fltrDataV493S0P2', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P2','alias':'Lnk_SRC_SYS_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_SYS_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.SRC_SYS_CD_SK','reference':'Lnk_SRC_SYS_CD_SK.CD_MPPNG_SK', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_SYS_CD'], 
    ['fltrDataV493S0P3', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P3','alias':'Lnk_BUY_CST_SRC_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.CLT2COSTSRC','reference':'Lnk_BUY_CST_SRC_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P4', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P4','alias':'Lnk_BUY_CST_TYP_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.CLT2COSTTYP','reference':'Lnk_BUY_CST_TYP_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P5', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P5','alias':'Lnk_BUY_PRICE_TYP_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.CLT2PRCTYPE','reference':'Lnk_BUY_PRICE_TYP_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P6', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P6','alias':'Lnk_CST_TYP_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.CLTCOSTTYP','reference':'Lnk_CST_TYP_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P7', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P7','alias':'Lnk_GNRC_OVRD_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.GENINDOVER','reference':'Lnk_GNRC_OVRD_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P8', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P8','alias':'Lnk_PDX_NTWK_QLFR_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.RXNETWRKQL','reference':'Lnk_PDX_NTWK_QLFR_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['fltrDataV493S0P9', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'fltrDataV493S0P9','alias':'Lnk_DRUG_TYP_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_CD':'string'}}, joinFields=[{'master':'Lnk_src_lkp.DRUG_TYP_CD','reference':'Lnk_DRUG_TYP_CD_SK.SRC_CD', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_CD'], 
    ['LkpSrcDomain', ctx.joinConditionForLookup(master={'name':'LkupDrugClm','alias':'Lnk_src_lkp','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD':'string','PLN_DRUG_STTUS_CD':'string','PLANTYPE':'string'}}, reference={'name':'LkpSrcDomain','alias':'Lnk_PLN_DRUG_STTUS_CD_SK','types':{'CD_MPPNG_SK':'integer','SRC_DOMAIN_TX':'string'}}, joinFields=[{'master':'Lnk_src_lkp.PLN_DRUG_STTUS_CD','reference':'Lnk_PLN_DRUG_STTUS_CD_SK.SRC_DOMAIN_TX', 'operator':'=='}]), 'CD_MPPNG_SK,SRC_DOMAIN_TX']],
      schema="DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,BUY_CST_SRC_CD_SK,BUY_CST_TYP_CD_SK,BUY_PRICE_TYP_CD_SK,CST_TYP_CD_SK,GNRC_OVRD_CD_SK,PDX_NTWK_QLFR_CD_SK,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,CLNTDEF2,TIER_ID,DRUG_TYP_CD_SK,PLN_DRUG_STTUS_CD_SK,PLANTYPE",
      map=['Lnk_src_lkp.DRUG_CLM_SK','Lnk_src_lkp.SRC_SYS_CD_SK','Lnk_src_lkp.CLM_ID','Lnk_BUY_CST_SRC_CD_SK.CD_MPPNG_SK','Lnk_BUY_CST_TYP_CD_SK.CD_MPPNG_SK','Lnk_BUY_PRICE_TYP_CD_SK.CD_MPPNG_SK','Lnk_CST_TYP_CD_SK.CD_MPPNG_SK','Lnk_GNRC_OVRD_CD_SK.CD_MPPNG_SK','Lnk_PDX_NTWK_QLFR_CD_SK.CD_MPPNG_SK','Lnk_src_lkp.RXCLMNBR','Lnk_src_lkp.CLMSEQNBR','Lnk_src_lkp.CLAIMSTS','Lnk_src_lkp.ORGPDSBMDT','Lnk_src_lkp.METRICQTY','Lnk_src_lkp.GPINUMBER','Lnk_src_lkp.GENINDOVER','Lnk_src_lkp.CPQSPCPRG','Lnk_src_lkp.CPQSPCPGIN','Lnk_src_lkp.FNLPLANCDE','Lnk_src_lkp.FNLPLANDTE','Lnk_src_lkp.FORMLRFLAG','Lnk_src_lkp.AWPUNITCST','Lnk_src_lkp.WACUNITCST','Lnk_src_lkp.CTYPEUCOST','Lnk_src_lkp.PROQTY','Lnk_src_lkp.CLTCOSTTYP','Lnk_src_lkp.CLTPRCTYPE','Lnk_src_lkp.CLTRATE','Lnk_src_lkp.CLTTYPUCST','Lnk_src_lkp.CLT2INGRCST','Lnk_src_lkp.CLT2DISPFEE','Lnk_src_lkp.CLT2COSTSRC','Lnk_src_lkp.CLT2COSTTYP','Lnk_src_lkp.CLT2PRCTYPE','Lnk_src_lkp.CLT2RATE','Lnk_src_lkp.RXNETWRKQL','Lnk_src_lkp.CLT2SLSTAX','Lnk_src_lkp.CLT2PSTAX','Lnk_src_lkp.CLT2FSTAX','Lnk_src_lkp.CLT2DUEAMT','Lnk_src_lkp.CLT2OTHAMT','Lnk_src_lkp.CRT_RUN_CYC_EXCTN_SK','Lnk_src_lkp.LAST_UPDT_RUN_CYC_EXCTN_SK','Lnk_src_lkp.DISPNS_FEE_AMT','Lnk_src_lkp.INGR_CST_CHRGD_AMT','Lnk_src_lkp.GNRC_DRUG_IN','Lnk_src_lkp.SLS_TAX_AMT','Lnk_src_lkp.CLTINCENTV','Lnk_src_lkp.CALINCENTV2','Lnk_src_lkp.CLTOTHPAYA','Lnk_src_lkp.CLTOTHAMT','Lnk_src_lkp.CLNTDEF2','Lnk_src_lkp.TIER_ID','Lnk_DRUG_TYP_CD_SK.CD_MPPNG_SK','Lnk_PLN_DRUG_STTUS_CD_SK.CD_MPPNG_SK','Lnk_src_lkp.PLANTYPE'],
      aliasFrom="Lnk_src_lkp,Lnk_SRC_SYS_CD_SK,Lnk_BUY_CST_SRC_CD_SK,Lnk_BUY_CST_TYP_CD_SK,Lnk_BUY_PRICE_TYP_CD_SK,Lnk_CST_TYP_CD_SK,Lnk_GNRC_OVRD_CD_SK,Lnk_PDX_NTWK_QLFR_CD_SK,Lnk_DRUG_TYP_CD_SK,Lnk_PLN_DRUG_STTUS_CD_SK",
      aliasTo="LkupDrugClm,fltrDataV493S0P2,fltrDataV493S0P3,fltrDataV493S0P4,fltrDataV493S0P5,fltrDataV493S0P6,fltrDataV493S0P7,fltrDataV493S0P8,fltrDataV493S0P9,LkpSrcDomain")}
    source(output(
        FRMLRY_SK as integer,
        FRMLRY_ID as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT DISTINCT\\nFRMLRY_ID,\\nFRMLRY_SK\\nFROM " + toString($IDSOwner) + ".FRMLRY"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2dsFRMLRY
    {ctx.joinScript(joinType='left',
      master=['LkupFkey', 'DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,BUY_CST_SRC_CD_SK,BUY_CST_TYP_CD_SK,BUY_PRICE_TYP_CD_SK,CST_TYP_CD_SK,GNRC_OVRD_CD_SK,PDX_NTWK_QLFR_CD_SK,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,CLNTDEF2,TIER_ID,DRUG_TYP_CD_SK,PLN_DRUG_STTUS_CD_SK,PLANTYPE'],
      transformationName='LkpFrmly',
      references=[['db2dsFRMLRY', ctx.joinConditionForLookup(master={'name':'LkupFkey','alias':'Lnk_IdsDrugClmPriceFkey_Lkp_Out1','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','BUY_CST_SRC_CD_SK':'integer','BUY_CST_TYP_CD_SK':'integer','BUY_PRICE_TYP_CD_SK':'integer','CST_TYP_CD_SK':'integer','GNRC_OVRD_CD_SK':'integer','PDX_NTWK_QLFR_CD_SK':'integer','RXCLMNBR':'string','CLMSEQNBR':'string','CLAIMSTS':'string','ORGPDSBMDT':'string','METRICQTY':'string','GPINUMBER':'string','GENINDOVER':'string','CPQSPCPRG':'string','CPQSPCPGIN':'string','FNLPLANCDE':'string','FNLPLANDTE':'string','FORMLRFLAG':'string','AWPUNITCST':'decimal(13, 5)','WACUNITCST':'decimal(13, 5)','CTYPEUCOST':'decimal(13, 5)','PROQTY':'string','CLTCOSTTYP':'string','CLTPRCTYPE':'string','CLTRATE':'decimal(7, 2)','CLTTYPUCST':'decimal(13, 5)','CLT2INGRCST':'decimal(13, 2)','CLT2DISPFEE':'decimal(13, 2)','CLT2COSTSRC':'string','CLT2COSTTYP':'string','CLT2PRCTYPE':'string','CLT2RATE':'decimal(7, 2)','RXNETWRKQL':'string','CLT2SLSTAX':'decimal(13, 2)','CLT2PSTAX':'decimal(13, 2)','CLT2FSTAX':'decimal(13, 2)','CLT2DUEAMT':'decimal(13, 2)','CLT2OTHAMT':'decimal(13, 2)','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','DISPNS_FEE_AMT':'decimal(13, 2)','INGR_CST_CHRGD_AMT':'decimal(13, 2)','GNRC_DRUG_IN':'string','SLS_TAX_AMT':'decimal(13, 2)','CLTINCENTV':'decimal(13, 2)','CALINCENTV2':'decimal(13, 2)','CLTOTHPAYA':'decimal(13, 2)','CLTOTHAMT':'decimal(13, 2)','CLNTDEF2':'string','TIER_ID':'string','DRUG_TYP_CD_SK':'integer','PLN_DRUG_STTUS_CD_SK':'integer','PLANTYPE':'string'}}, reference={'name':'db2dsFRMLRY','alias':'Lnk_FRMLRY_SK','types':{'FRMLRY_SK':'integer','FRMLRY_ID':'string'}}, joinFields=[{'master':'Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLNTDEF2','reference':'Lnk_FRMLRY_SK.FRMLRY_ID', 'operator':'=='}]), 'FRMLRY_SK,FRMLRY_ID']],
      schema="DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,BUY_CST_SRC_CD_SK,BUY_CST_TYP_CD_SK,BUY_PRICE_TYP_CD_SK,CST_TYP_CD_SK,GNRC_OVRD_CD_SK,PDX_NTWK_QLFR_CD_SK,RXCLMNBR,CLMSEQNBR,CLAIMSTS,ORGPDSBMDT,METRICQTY,GPINUMBER,GENINDOVER,CPQSPCPRG,CPQSPCPGIN,FNLPLANCDE,FNLPLANDTE,FORMLRFLAG,AWPUNITCST,WACUNITCST,CTYPEUCOST,PROQTY,CLTCOSTTYP,CLTPRCTYPE,CLTRATE,CLTTYPUCST,CLT2INGRCST,CLT2DISPFEE,CLT2COSTSRC,CLT2COSTTYP,CLT2PRCTYPE,CLT2RATE,RXNETWRKQL,CLT2SLSTAX,CLT2PSTAX,CLT2FSTAX,CLT2DUEAMT,CLT2OTHAMT,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,DISPNS_FEE_AMT,INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT,CLTINCENTV,CALINCENTV2,CLTOTHPAYA,CLTOTHAMT,FRMLRY_SK,TIER_ID,DRUG_TYP_CD_SK,PLN_DRUG_STTUS_CD_SK,PLANTYPE",
      map=['Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DRUG_CLM_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.SRC_SYS_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLM_ID','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_CST_SRC_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_CST_TYP_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_PRICE_TYP_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CST_TYP_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GNRC_OVRD_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PDX_NTWK_QLFR_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.RXCLMNBR','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLMSEQNBR','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLAIMSTS','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.ORGPDSBMDT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.METRICQTY','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GPINUMBER','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GENINDOVER','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CPQSPCPRG','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CPQSPCPGIN','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FNLPLANCDE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FNLPLANDTE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FORMLRFLAG','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.AWPUNITCST','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.WACUNITCST','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CTYPEUCOST','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PROQTY','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTCOSTTYP','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTPRCTYPE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTRATE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTTYPUCST','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2INGRCST','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2DISPFEE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2COSTSRC','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2COSTTYP','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2PRCTYPE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2RATE','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.RXNETWRKQL','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2SLSTAX','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2PSTAX','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2FSTAX','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2DUEAMT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2OTHAMT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CRT_RUN_CYC_EXCTN_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.LAST_UPDT_RUN_CYC_EXCTN_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DISPNS_FEE_AMT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.INGR_CST_CHRGD_AMT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GNRC_DRUG_IN','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.SLS_TAX_AMT','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTINCENTV','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CALINCENTV2','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTOTHPAYA','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTOTHAMT','Lnk_FRMLRY_SK.FRMLRY_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.TIER_ID','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DRUG_TYP_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PLN_DRUG_STTUS_CD_SK','Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PLANTYPE'],
      aliasFrom="Lnk_IdsDrugClmPriceFkey_Lkp_Out1,Lnk_FRMLRY_SK",
      aliasTo="LkupFkey,db2dsFRMLRY")}
    LkpFrmly
        derive(
              DRUG_CLM_SK = DRUG_CLM_SK,
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = npw_DS_TRIM_1(CLM_ID),
              CRT_RUN_CYC_EXCTN_SK = $IDSRunCycle,
              LAST_UPDT_RUN_CYC_EXCTN_SK = $IDSRunCycle,
              BUY_CST_SRC_CD_SK = BUY_CST_SRC_CD_SK,
              GNRC_OVRD_CD_SK = GNRC_OVRD_CD_SK,
              PDX_NTWK_QLFR_CD_SK = PDX_NTWK_QLFR_CD_SK,
              FRMLRY_PROTOCOL_IN = FORMLRFLAG,
              RECON_IN = toString(null()),
              SPEC_PGM_IN = CPQSPCPGIN,
              FINL_PLN_EFF_DT_SK =  case(FNLPLANDTE == "0000000", toDate("1753-01-01", "yyyy-MM-dd"), toDate(substring(:SVDate3, 1, 4) + "-" + substring(:SVDate3, 5, 2) + "-" + substring(:SVDate3, 7, 2), "yyyy-MM-dd")),
              ORIG_PD_TRANS_SUBMT_DT_SK =  case(ORGPDSBMDT == "00000000", toDate("1753-01-01", "yyyy-MM-dd"), toDate(substring(ORGPDSBMDT, 1, 4) + "-" + substring(ORGPDSBMDT, 5, 2) + "-" + substring(ORGPDSBMDT, 7, 2), "yyyy-MM-dd")),
              PRORTD_DISPNS_QTY = PROQTY,
              SUBMT_DISPNS_METRIC_QTY = METRICQTY,
              AVG_WHLSL_PRICE_UNIT_CST_AMT = AWPUNITCST,
              WHLSL_ACQSTN_CST_UNIT_CST_AMT = WACUNITCST,
              BUY_DISPNS_FEE_AMT = CLT2DISPFEE,
              BUY_INGR_CST_AMT = CLT2INGRCST,
              BUY_RATE_PCT = CLT2RATE,
              BUY_SLS_TAX_AMT = :SVBuySlsTaxAmt,
              BUY_TOT_DUE_AMT = CLT2DUEAMT,
              BUY_TOT_OTHR_AMT = CLT2OTHAMT,
              CST_TYP_UNIT_CST_AMT = CTYPEUCOST,
              INVC_TOT_DUE_AMT = 0.00,
              SELL_CST_TYP_UNIT_CST_AMT = CLTTYPUCST,
              SELL_RATE_PCT = npw_DS_TRIM_1(CLTRATE),
              SPREAD_DISPNS_FEE_AMT =  case(isNull(CLT2DISPFEE), 0,  case(CLT2DISPFEE == 0, CLT2DISPFEE, DISPNS_FEE_AMT - CLT2DISPFEE)),
              SPREAD_INGR_CST_AMT =  case(isNull(CLT2INGRCST), 0,  case(CLT2INGRCST == 0, CLT2INGRCST, INGR_CST_CHRGD_AMT - CLT2INGRCST)),
              SPREAD_SLS_TAX_AMT = SLS_TAX_AMT - :SVBuySlsTaxAmt,
              BUY_CST_TYP_ID = npw_DS_TRIM_1(CLT2COSTTYP),
              BUY_PRICE_TYP_ID = npw_DS_TRIM_1(CLT2PRCTYPE),
              CST_TYP_ID = npw_DS_TRIM_1(CLTCOSTTYP),
              DRUG_TYP_ID = :SVDrugTypCd,
              FINL_PLN_ID = npw_DS_TRIM_1(FNLPLANCDE),
              GNRC_PROD_ID = npw_DS_TRIM_1(GPINUMBER),
              SELL_PRICE_TYP_ID = npw_DS_TRIM_1(CLTPRCTYPE),
              SPEC_PGM_ID = npw_DS_TRIM_1(CPQSPCPRG),
              BUY_INCNTV_FEE_AMT = CALINCENTV2,
              SELL_INCNTV_FEE_AMT = CLTINCENTV,
              SPREAD_INCNTV_FEE_AMT = CLTINCENTV - CALINCENTV2,
              SELL_OTHR_AMT = CLTOTHAMT,
              SELL_OTHR_PAYOR_AMT = CLTOTHPAYA,
              FRMLRY_SK = FRMLRY_SK,
              TIER_ID = TIER_ID,
              DRUG_TYP_CD_SK = DRUG_TYP_CD_SK,
              PLN_DRUG_STTUS_CD_SK = PLN_DRUG_STTUS_CD_SK,
              DRUG_PLN_TYP_ID = PLANTYPE,
              SVDate1 = :SVDate1,
              SVDate2 = :SVDate2,
              SVDate3 = :SVDate3,
              SVDrugTypCd = :SVDrugTypCd,
              SVBuySlsTaxAmt = :SVBuySlsTaxAmt,
              SVDate1 := {'trim(translate(FNLPLANDTE, "-", ""))'.replace("Lnk_IdsDrugClmPriceFkey_Lkp_Out.","")},
              SVDate2 := {'substring(:SVDate1, 1, 1)'.replace("Lnk_IdsDrugClmPriceFkey_Lkp_Out.","")},
              SVDate3 := {' case(:SVDate2 == "1", "20" + substring(:SVDate1, 2, 6), "19" + substring(:SVDate1, 2, 6))'.replace("Lnk_IdsDrugClmPriceFkey_Lkp_Out.","")},
              SVDrugTypCd := {' case(npw_DS_TRIM_1(GNRC_DRUG_IN) == "Y", "GENERIC", "BRAND")'.replace("Lnk_IdsDrugClmPriceFkey_Lkp_Out.","")},
              SVBuySlsTaxAmt := {'CLT2PSTAX + CLT2FSTAX'.replace("Lnk_IdsDrugClmPriceFkey_Lkp_Out.","")}) ~> xfmCheckLkpResultsDerived
        xfmCheckLkpResultsDerived
        select(mapColumn(
            DRUG_CLM_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            BUY_CST_SRC_CD_SK,
            GNRC_OVRD_CD_SK,
            PDX_NTWK_QLFR_CD_SK,
            FRMLRY_PROTOCOL_IN,
            RECON_IN,
            SPEC_PGM_IN,
            FINL_PLN_EFF_DT_SK,
            ORIG_PD_TRANS_SUBMT_DT_SK,
            PRORTD_DISPNS_QTY,
            SUBMT_DISPNS_METRIC_QTY,
            AVG_WHLSL_PRICE_UNIT_CST_AMT,
            WHLSL_ACQSTN_CST_UNIT_CST_AMT,
            BUY_DISPNS_FEE_AMT,
            BUY_INGR_CST_AMT,
            BUY_RATE_PCT,
            BUY_SLS_TAX_AMT,
            BUY_TOT_DUE_AMT,
            BUY_TOT_OTHR_AMT,
            CST_TYP_UNIT_CST_AMT,
            INVC_TOT_DUE_AMT,
            SELL_CST_TYP_UNIT_CST_AMT,
            SELL_RATE_PCT,
            SPREAD_DISPNS_FEE_AMT,
            SPREAD_INGR_CST_AMT,
            SPREAD_SLS_TAX_AMT,
            BUY_CST_TYP_ID,
            BUY_PRICE_TYP_ID,
            CST_TYP_ID,
            DRUG_TYP_ID,
            FINL_PLN_ID,
            GNRC_PROD_ID,
            SELL_PRICE_TYP_ID,
            SPEC_PGM_ID,
            BUY_INCNTV_FEE_AMT,
            SELL_INCNTV_FEE_AMT,
            SPREAD_INCNTV_FEE_AMT,
            SELL_OTHR_AMT,
            SELL_OTHR_PAYOR_AMT,
            FRMLRY_SK,
            TIER_ID,
            DRUG_TYP_CD_SK,
            PLN_DRUG_STTUS_CD_SK,
            DRUG_PLN_TYP_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> xfmCheckLkpResults
    xfmCheckLkpResults sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/DRUG_CLM_PRICE.dat'""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/DRUG_CLM_PRICE.dat'""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> SeqDrugClmPriceFkey""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfOptumIdsDrugClmPriceFkey()
  activityName = "OptumIdsDrugClmPriceFkey"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "UWSOwner":  {"value": "'@{pipeline().parameters.UWSOwner}'","type": "Expression"},
  "UWSDB":  {"value": "'@{pipeline().parameters.UWSDB}'","type": "Expression"},
  "UWSAcct":  {"value": "'@{pipeline().parameters.UWSAcct}'","type": "Expression"},
  "UWSPW":  {"value": "'@{pipeline().parameters.UWSPW}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "IDSRunCycle":  {"value": "'@{pipeline().parameters.IDSRunCycle}'","type": "Expression"},
  "CurrentDate":  {"value": "'@{pipeline().parameters.CurrentDate}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def OptumIdsDrugClmPriceFkey(ctx):
  name = "OptumIdsDrugClmPriceFkey"
  artifacts, activities = OptumIdsDrugClmPriceFkeyActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/DrugClmPrice"),
    activities = activities,
    description = """
      Perform Foreign Key and code mapping Lookups to populate DRUG_CLM_PRICE File And this job is called in OptumIdsDrugClmPriceLoadSeq sequence job
      MODIFICATIONS:
                                                                                                                                                                                                                                                                                                  DATASTAGE               CODE                           DATE
      DEVELOPER             DATE                PROJECT                                   DESCRIPTION                                                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
      ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
      Sri Nannapaneni        10-22-2019        Spread price                             Perform Foreign Key and code mapping Lookups to populate DRUG_CLM_PRICE File                                      IntegrateDev2             Sharon Andrew              11/11/2019
      Sri Nannapaneni         11-21-2-19                                                         updated based on new mapping                                                                                                                                                              Kalyan Neelam             2019-11-26
      Rekha Radhakrishna  2020-01-30                                                       Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                           IntegrateDev2            Kalyan Neelam             2020-02-06
                                                                                                                  ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT

      Rekha Radhakrishna    2020-02-10    6131- PBM Replacement         Mapped fields  CLT2PSTAX and CLT2FSTAX to calculate BUY_SLS_TAX_AMT                                               IntegrateDev2            Kalyan Neelam             2020-02-10     
                                                                                                                   and changed SPREAD_SLS_TAX_AMT calculation

      Velmani Kondappan     2020-04-02   6131- PBM Replacement           Removed the errror warnings coming from Lkup_Fkey lookup                                                                                                                  Kalyan Neelam            2020-04-09

      Rekha Radhakrishna   2020-10-14     6131 - PBM Replacement         parameterized input source file                                                                                                                         IntegrateDev2                   Sravya Gorla	2020-12-09
      Geetanjali Rajendran    2021-06-03        PBM PhaseII                              Perform code mapping lookup to populate DRUG_TYPE_CD_SK and FRMLRY lookup          IntegrateDev2		Abhiram Dasarathy	2021-06-23
                                                                                                                                         to populate FRMLRY_SK 

      Arpitha V                      2023-11-07       US 600305                              Perform code mapping lookup to populate PLN_DRUG_STTUS_CD_SK                                                        IntegrateDevB               Jeyaprasanna           2024-01-03

      Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                                                        IntegrateDev2                 Jeyaprasanna            2024-03-14
      Parameters:
      -----------
      FilePath:
        File Path
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSPW:
        IDS Password
      IDSAcct:
        IDS Account
      UWSOwner:
        UWS Table Owner
      UWSDB:
        UWS Database
      UWSAcct:
        UWS Account
      UWSPW:
        UWS Password
      SrcSysCd:
        SrcSysCd
      RunID:
        RunID
      IDSRunCycle:
        IDSRunCycle
      CurrentDate:
        CurrentDate""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSAcct": ParameterSpecification(type="String"),
      "UWSOwner": ParameterSpecification(type="String", default_value=ctx.UWSOwner),
      "UWSDB": ParameterSpecification(type="String", default_value=ctx.UWSDB),
      "UWSAcct": ParameterSpecification(type="String"),
      "UWSPW": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "IDSRunCycle": ParameterSpecification(type="String"),
      "CurrentDate": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumIdsDrugClmPriceFkey(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
