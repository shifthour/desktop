model:
  config:
    origin: datastage
  jobs:
    - name: OptumControlReconExtr
      origin: datastage
      type: dataflow
      folder: "claim/claim/Optum"
      runAfterSubrOnJobSuccess: enabled
      description: "Recon between optum claim detail and optum claim control file @ full rollup amounts level"
      fullDescription: "© Copyright 2019 Blue Cross/Blue Shield of Kansas City\n\n\nCALLED BY : OptumDrugLandSeq\n\n\nDESCRIPTION:      OptumRx Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim\n                               \n\nMODIFICATIONS:\n\n     \nDeveloper                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       \n------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      \nSri Nannapaneni               2020-07-15     PBM Phase II                               Initial Programming                                                                                              IntegrateDev2\x09\x09Abhiram Dasarathy\x092021-05-07"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: RunID
          type: string
          description: RunID
        - name: SrcSysCd
          type: string
          description: SrcSysCd
          longDescription: OPTUMRX
        - name: SrcSysCdSK
          type: integer
          description: SrcSysCdSK
          longDescription: "-1951778164"
        - name: RunDate
          type: string
          description: RunDate
        - name: InFile
          type: string
          description: InFile
          longDescription: "OPTUMRX.PBM.DrugClm_20191122.dat"
        - name: ControlInFile
          type: string
          description: ControlInFile
        - name: MbrTermDate
          type: string
          description: MbrTermDate
          longDescription: "Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years."
      beforeSubr:
        subrType: "DSU.UnixSH"
        subr: "sed '$d' #$FilePath#/landing/#ControlInFile# > #$FilePath#/verified/#ControlInFile#.temp"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\optumclm_controlfile,optumclm_controlfile_agg"
        subr: "optumclm_controlfile,optumclm_controlfile_agg"
      steps:
        - name: OptumDetail_Seq
          type: fileReader
          urls: 
            - "#$FilePath#/landing/#InFile#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: "|"
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: CARID
              type: char
              width: 9
            - name: SCARID
              type: char
              width: 9
            - name: CARRPROC
              type: decimal
              width: 10
            - name: CLNTID
              type: char
              width: 3
            - name: CLNTSGMNT
              type: char
              width: 3
            - name: CLNTREGION
              type: char
              width: 2
            - name: ACCOUNTID
              type: char
              width: 15
            - name: ACCTBENCDE
              type: char
              width: 10
            - name: VERSIONNBR
              type: char
              width: 2
            - name: GROUPID
              type: char
              width: 15
            - name: GROUPPLAN
              type: char
              width: 10
            - name: GRPCLIBENF
              type: char
              width: 10
            - name: GROUPSIC
              type: char
              width: 4
            - name: CLMRESPSTS
              type: char
              width: 1
            - name: MEMBERID
              type: char
              width: 20
            - name: MBRLSTNME
              type: char
              width: 25
            - name: MBRFSTNME
              type: char
              width: 15
            - name: MBRMDINIT
              type: char
              width: 1
            - name: MBRPRSNCDE
              type: char
              width: 3
            - name: MBRRELCDE
              type: char
              width: 1
            - name: MBRSEX
              type: char
              width: 1
            - name: MBRBIRTH
              type: decimal
              width: 8
            - name: MBRAGE
              type: decimal
              width: 3
            - name: MBRZIP
              type: char
              width: 15
            - name: SOCSECNBR
              type: char
              width: 9
            - name: DURKEY
              type: char
              width: 18
            - name: DURFLAG
              type: char
              width: 1
            - name: MBRFAMLYID
              type: char
              width: 20
            - name: MBRFAMLIND
              type: char
              width: 1
            - name: MBRFAMLTYP
              type: char
              width: 1
            - name: COBIND
              type: char
              width: 2
            - name: MBRPLAN
              type: char
              width: 10
            - name: MBRPRODCDE
              type: char
              width: 6
            - name: MBRRIDERCD
              type: char
              width: 6
            - name: CARENETID
              type: char
              width: 10
            - name: CAREQUALID
              type: char
              width: 10
            - name: CAREFACID
              type: char
              width: 10
            - name: CAREFACNAM
              type: char
              width: 25
            - name: MBRPCPHYS
              type: char
              width: 15
            - name: PPRSFSTNME
              type: char
              width: 15
            - name: PPRSLSTNME
              type: char
              width: 25
            - name: PPRSMDINIT
              type: char
              width: 1
            - name: PPRSSPCCDE
              type: char
              width: 6
            - name: PPRSTATE
              type: char
              width: 3
            - name: MBRALTINFL
              type: char
              width: 1
            - name: MBRALTINCD
              type: char
              width: 10
            - name: MBRALTINID
              type: char
              width: 20
            - name: MBRMEDDTE
              type: decimal
              width: 8
            - name: MBRMEDTYPE
              type: char
              width: 1
            - name: MBRHICCDE
              type: char
              width: 11
            - name: CARDHOLDER
              type: char
              width: 20
            - name: PATLASTNME
              type: char
              width: 15
            - name: PATFRSTNME
              type: char
              width: 12
            - name: PERSONCDE
              type: char
              width: 3
            - name: RELATIONCD
              type: char
              width: 1
            - name: SEXCODE
              type: char
              width: 1
            - name: BIRTHDTE
              type: decimal
              width: 8
            - name: ELIGCLARIF
              type: char
              width: 1
            - name: CUSTLOC
              type: char
              width: 2
            - name: SBMPLSRVCE
              type: char
              width: 2
            - name: SBMPATRESD
              type: char
              width: 2
            - name: PRMCAREPRV
              type: char
              width: 15
            - name: PRMCAREPRQ
              type: char
              width: 2
            - name: FACILITYID
              type: char
              width: 10
            - name: OTHCOVERAG
              type: char
              width: 2
            - name: BINNUMBER
              type: char
              width: 6
            - name: PROCESSOR
              type: char
              width: 10
            - name: GROUPNBR
              type: char
              width: 15
            - name: TRANSCDE
              type: char
              width: 2
            - name: DATESBM
              type: decimal
              width: 8
            - name: TIMESBM
              type: decimal
              width: 6
            - name: CHGDATE
              type: decimal
              width: 7
            - name: CHGTIME
              type: decimal
              width: 6
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: RVDATESBM
              type: decimal
              width: 8
            - name: CLMCOUNTER
              type: decimal
              width: 2
            - name: GENERICCTR
              type: decimal
              width: 2
            - name: FORMLRYCTR
              type: decimal
              width: 2
            - name: RXNUMBER
              type: char
              width: 12
            - name: RXNUMBERQL
              type: char
              width: 1
            - name: REFILL
              type: char
              width: 2
            - name: DISPSTATUS
              type: char
              width: 1
            - name: DTEFILLED
              type: decimal
              width: 8
            - name: COMPOUNDCD
              type: char
              width: 1
            - name: SBMCMPDTYP
              type: char
              width: 2
            - name: PRODTYPCDE
              type: char
              width: 2
            - name: PRODUCTID
              type: char
              width: 20
            - name: PRODUCTKEY
              type: decimal
              width: 9
            - name: METRICQTY
              type: decimal
              width: 6
            - name: DECIMALQTY
              type: decimal
              width: 13
            - name: DAYSSUPPLY
              type: decimal
              width: 3
            - name: PSC
              type: char
              width: 1
            - name: WRITTENDTE
              type: decimal
              width: 8
            - name: NBRFLSAUTH
              type: decimal
              width: 2
            - name: ORIGINCDE
              type: char
              width: 1
            - name: SBMCLARCD1
              type: char
              width: 2
            - name: SBMCLARCD2
              type: char
              width: 2
            - name: SBMCLARCD3
              type: char
              width: 2
            - name: PAMCNBR
              type: char
              width: 11
            - name: PAMCCDE
              type: char
              width: 2
            - name: PRAUTHNBR
              type: char
              width: 11
            - name: PRAUTHRSN
              type: char
              width: 2
            - name: PRAUTHFDTE
              type: decimal
              width: 7
            - name: PRAUTHTDTE
              type: decimal
              width: 7
            - name: LABELNAME
              type: char
              width: 30
            - name: PRODNM
              type: char
              width: 70
            - name: DRUGMFGRID
              type: char
              width: 5
            - name: DRUGMFGR
              type: char
              width: 10
            - name: GPINUMBER
              type: char
              width: 14
            - name: GNRCNME
              type: char
              width: 60
            - name: PRDPACUOM
              type: char
              width: 2
            - name: PRDPACSIZE
              type: decimal
              width: 12
            - name: DDID
              type: decimal
              width: 6
            - name: GCN
              type: decimal
              width: 5
            - name: GCNSEQ
              type: decimal
              width: 6
            - name: KDC
              type: decimal
              width: 10
            - name: AHFS
              type: char
              width: 8
            - name: DRUGDEACOD
              type: char
              width: 1
            - name: RXOTCIND
              type: char
              width: 1
            - name: MULTSRCCDE
              type: char
              width: 1
            - name: GENINDOVER
              type: char
              width: 1
            - name: PRDREIMIND
              type: char
              width: 1
            - name: BRNDTRDNME
              type: char
              width: 1
            - name: FDATHERAEQ
              type: char
              width: 2
            - name: METRICSTRG
              type: decimal
              width: 12
            - name: DRGSTRGUOM
              type: char
              width: 10
            - name: ADMINROUTE
              type: char
              width: 2
            - name: ADMINRTESN
              type: char
              width: 11
            - name: DOSAGEFORM
              type: char
              width: 4
            - name: NDA
              type: char
              width: 1
            - name: ANDA
              type: char
              width: 1
            - name: ANDAOR
              type: char
              width: 1
            - name: RXNORMCODE
              type: char
              width: 10
            - name: MNTDRUGCDE
              type: char
              width: 1
            - name: MNTSOURCE
              type: char
              width: 1
            - name: MNTCARPROR
              type: char
              width: 1
            - name: MNTGPILIST
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: CPQSPCSCHD
              type: char
              width: 10
            - name: THRDPARTYX
              type: char
              width: 1
            - name: DRGUNITDOS
              type: char
              width: 1
            - name: SBMUNITDOS
              type: char
              width: 1
            - name: ALTPRODTYP
              type: char
              width: 1
            - name: ALTPRODCDE
              type: char
              width: 13
            - name: SRXNETWRK
              type: char
              width: 6
            - name: SRXNETTYPE
              type: char
              width: 2
            - name: RXNETWORK
              type: char
              width: 6
            - name: RXNETWRKNM
              type: char
              width: 25
            - name: RXNETCARR
              type: char
              width: 9
            - name: RXNETWRKQL
              type: char
              width: 1
            - name: RXNETPRCQL
              type: char
              width: 1
            - name: REGIONCDE
              type: char
              width: 10
            - name: PHRAFFIL
              type: char
              width: 10
            - name: NETPRIOR
              type: char
              width: 3
            - name: NETTYPE
              type: char
              width: 2
            - name: NETSEQ
              type: decimal
              width: 5
            - name: PAYCNTR
              type: char
              width: 10
            - name: PHRNDCLST
              type: char
              width: 10
            - name: PHRGPILST
              type: char
              width: 10
            - name: SRVPROVID
              type: char
              width: 15
            - name: SRVPROVIDQ
              type: char
              width: 2
            - name: NPIPROV
              type: char
              width: 10
            - name: PRVNCPDPID
              type: char
              width: 12
            - name: SBMSRVPRID
              type: char
              width: 15
            - name: SBMSRVPRQL
              type: char
              width: 2
            - name: SRVPROVNME
              type: char
              width: 55
            - name: PROVLOCKQL
              type: char
              width: 1
            - name: PROVLOCKID
              type: char
              width: 15
            - name: STORENBR
              type: char
              width: 15
            - name: AFFILIATIN
              type: char
              width: 6
            - name: PAYEEID
              type: char
              width: 12
            - name: DISPRCLASS
              type: char
              width: 3
            - name: DISPROTHER
              type: char
              width: 3
            - name: PHARMZIP
              type: char
              width: 10
            - name: PRESNETWID
              type: char
              width: 6
            - name: PRESCRIBER
              type: char
              width: 15
            - name: PRESCRIDQL
              type: char
              width: 2
            - name: NPIPRESCR
              type: char
              width: 10
            - name: PRESCDEAID
              type: char
              width: 15
            - name: PRESLSTNME
              type: char
              width: 25
            - name: PRESFSTNME
              type: char
              width: 15
            - name: PRESMDINIT
              type: char
              width: 1
            - name: PRESSPCCDE
              type: char
              width: 6
            - name: PRESSPCCDQ
              type: char
              width: 3
            - name: PRSSTATE
              type: char
              width: 3
            - name: SBMRPHID
              type: char
              width: 15
            - name: SBMRPHIDQL
              type: char
              width: 2
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: PLANTYPE
              type: char
              width: 8
            - name: PLANQUAL
              type: char
              width: 10
            - name: PLNNDCLIST
              type: char
              width: 10
            - name: LSTQUALNDC
              type: char
              width: 10
            - name: PLNGPILIST
              type: char
              width: 10
            - name: LSTQUALGPI
              type: char
              width: 10
            - name: PLNPNDCLST
              type: char
              width: 10
            - name: PLNPGPILST
              type: char
              width: 10
            - name: PLANDRUGST
              type: char
              width: 1
            - name: PLANFRMLRY
              type: char
              width: 1
            - name: PLNFNLPSCH
              type: char
              width: 10
            - name: PHRDCSCHID
              type: char
              width: 13
            - name: PHRDCSCHSQ
              type: decimal
              width: 3
            - name: CLTDCSCHID
              type: char
              width: 13
            - name: CLTDCSCHSQ
              type: decimal
              width: 3
            - name: PHRDCCSCID
              type: char
              width: 14
            - name: PHRDCCSCSQ
              type: decimal
              width: 3
            - name: CLTDCCSCID
              type: char
              width: 14
            - name: CLTDCCSCSQ
              type: decimal
              width: 3
            - name: PHRPRTSCID
              type: char
              width: 13
            - name: CLTPRTSCID
              type: char
              width: 13
            - name: PHRRMSCHID
              type: char
              width: 10
            - name: CLTRMSCHID
              type: char
              width: 10
            - name: PRDPFLSTID
              type: char
              width: 10
            - name: PRFPRDSCID
              type: char
              width: 10
            - name: FORMULARY
              type: char
              width: 1
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: TIERVALUE
              type: decimal
              width: 2
            - name: CONTHERAPY
              type: char
              width: 1
            - name: CTSCHEDID
              type: char
              width: 20
            - name: PRBASISCHD
              type: char
              width: 10
            - name: REGDISOR
              type: char
              width: 10
            - name: DRUGSTSTBL
              type: char
              width: 13
            - name: TRANSBEN
              type: char
              width: 10
            - name: MESSAGECD1
              type: char
              width: 10
            - name: MESSAGE1
              type: char
              width: 40
            - name: MESSAGECD2
              type: char
              width: 10
            - name: MESSAGE2
              type: char
              width: 40
            - name: MESSAGECD3
              type: char
              width: 10
            - name: MESSAGE3
              type: char
              width: 40
            - name: REJCNT
              type: decimal
              width: 2
            - name: REJCDE1
              type: char
              width: 3
            - name: REJCDE2
              type: char
              width: 3
            - name: REJCDE3
              type: char
              width: 3
            - name: RJCPLANID
              type: char
              width: 8
            - name: DURCONFLCT
              type: char
              width: 2
            - name: DURINTERVN
              type: char
              width: 2
            - name: DUROUTCOME
              type: char
              width: 2
            - name: LVLSERVICE
              type: char
              width: 2
            - name: PHARSRVTYP
              type: char
              width: 2
            - name: DIAGNOSIS
              type: char
              width: 15
            - name: DIAGNOSISQ
              type: char
              width: 2
            - name: RVDURCNFLC
              type: char
              width: 2
            - name: RVDURINTRV
              type: char
              width: 2
            - name: RVDUROUTCM
              type: char
              width: 2
            - name: RVLVLSERVC
              type: char
              width: 2
            - name: DRGCNFLCT1
              type: char
              width: 2
            - name: SEVERITY1
              type: char
              width: 1
            - name: OTHRPHARM1
              type: char
              width: 1
            - name: DTEPRVFIL1
              type: decimal
              width: 8
            - name: QTYPRVFIL1
              type: decimal
              width: 12
            - name: DATABASE1
              type: char
              width: 1
            - name: OTHRPRESC1
              type: char
              width: 1
            - name: FREETEXT1
              type: char
              width: 30
            - name: DRGCNFLCT2
              type: char
              width: 2
            - name: SEVERITY2
              type: char
              width: 1
            - name: OTHRPHARM2
              type: char
              width: 1
            - name: DTEPRVFIL2
              type: decimal
              width: 8
            - name: QTYPRVFIL2
              type: decimal
              width: 12
            - name: DATABASE2
              type: char
              width: 1
            - name: OTHRPRESC2
              type: char
              width: 1
            - name: FREETEXT2
              type: char
              width: 30
            - name: DRGCNFLCT3
              type: char
              width: 2
            - name: SEVERITY3
              type: char
              width: 1
            - name: OTHRPHARM3
              type: char
              width: 1
            - name: DTEPRVFIL3
              type: decimal
              width: 8
            - name: QTYPRVFIL3
              type: decimal
              width: 12
            - name: DATABASE3
              type: char
              width: 1
            - name: OTHRPRESC3
              type: char
              width: 1
            - name: FREETEXT3
              type: varchar
              width: 30
            - name: FEETYPE
              type: char
              width: 2
            - name: BENUNITCST
              type: decimal
              width: 14
            - name: AWPUNITCST
              type: decimal
              width: 14
            - name: WACUNITCST
              type: decimal
              width: 14
            - name: GEAPUNTCST
              type: decimal
              width: 14
            - name: CTYPEUCOST
              type: decimal
              width: 14
            - name: BASISCOST
              type: char
              width: 2
            - name: PRICEQTY
              type: decimal
              width: 13
            - name: PRODAYSSUP
              type: decimal
              width: 3
            - name: PROQTY
              type: decimal
              width: 12
            - name: RVINCNTVSB
              type: decimal
              width: 11
            - name: SBMINGRCST
              type: decimal
              width: 11
            - name: SBMDISPFEE
              type: decimal
              width: 11
            - name: SBMPSLSTX
              type: decimal
              width: 11
            - name: SBMFSLSTX
              type: decimal
              width: 11
            - name: SBMSLSTX
              type: decimal
              width: 11
            - name: SBMPATPAY
              type: decimal
              width: 11
            - name: SBMAMTDUE
              type: decimal
              width: 11
            - name: SBMINCENTV
              type: decimal
              width: 11
            - name: SBMPROFFEE
              type: decimal
              width: 11
            - name: SBMTOTHAMT
              type: decimal
              width: 11
            - name: SBMOPAMTCT
              type: decimal
              width: 1
            - name: SBMOPAMTQL
              type: char
              width: 2
            - name: USUALNCUST
              type: decimal
              width: 11
            - name: DENIALDTE
              type: decimal
              width: 8
            - name: OTHRPAYOR
              type: decimal
              width: 11
            - name: SBMMDPDAMT
              type: decimal
              width: 11
            - name: CALINGRCST
              type: decimal
              width: 11
            - name: CALDISPFEE
              type: decimal
              width: 11
            - name: CALPSTAX
              type: decimal
              width: 11
            - name: CALFSTAX
              type: decimal
              width: 11
            - name: CALSLSTAX
              type: decimal
              width: 11
            - name: CALPATPAY
              type: decimal
              width: 11
            - name: CALDUEAMT
              type: decimal
              width: 11
            - name: CALWITHHLD
              type: decimal
              width: 11
            - name: CALFCOPAY
              type: decimal
              width: 11
            - name: CALPCOPAY
              type: decimal
              width: 11
            - name: CALCOPAY
              type: decimal
              width: 11
            - name: CALPRODSEL
              type: decimal
              width: 11
            - name: CALATRTAX
              type: decimal
              width: 11
            - name: CALEXCEBFT
              type: decimal
              width: 11
            - name: CALINCENTV
              type: decimal
              width: 11
            - name: CALATRDED
              type: decimal
              width: 11
            - name: CALCOB
              type: decimal
              width: 11
            - name: CALTOTHAMT
              type: decimal
              width: 11
            - name: CALPROFFEE
              type: decimal
              width: 11
            - name: CALOTHPAYA
              type: decimal
              width: 11
            - name: CALCOSTSRC
              type: char
              width: 1
            - name: CALADMNFEE
              type: decimal
              width: 11
            - name: CALPROCFEE
              type: decimal
              width: 11
            - name: CALPATSTAX
              type: decimal
              width: 11
            - name: CALPLNSTAX
              type: decimal
              width: 11
            - name: CALPRVNSEL
              type: decimal
              width: 11
            - name: CALPSCBRND
              type: decimal
              width: 11
            - name: CALPSCNONP
              type: decimal
              width: 11
            - name: CALPSCBRNP
              type: decimal
              width: 11
            - name: CALCOVGAP
              type: decimal
              width: 11
            - name: CALINGCSTC
              type: decimal
              width: 11
            - name: CALDSPFEEC
              type: decimal
              width: 11
            - name: PHRINGRCST
              type: decimal
              width: 11
            - name: PHRDISPFEE
              type: decimal
              width: 11
            - name: PHRPPSTAX
              type: decimal
              width: 11
            - name: PHRFSTAX
              type: decimal
              width: 11
            - name: PHRSLSTAX
              type: decimal
              width: 11
            - name: PHRPATPAY
              type: decimal
              width: 11
            - name: PHRDUEAMT
              type: decimal
              width: 11
            - name: PHRWITHHLD
              type: decimal
              width: 11
            - name: PHRPPRCS
              type: char
              width: 10
            - name: PHRPRCST
              type: char
              width: 10
            - name: PHRPTPS
              type: char
              width: 10
            - name: PHRPTPST
              type: char
              width: 13
            - name: PHRCOPAYSC
              type: char
              width: 10
            - name: PHRCOPAYSS
              type: decimal
              width: 2
            - name: PHRFCOPAY
              type: decimal
              width: 11
            - name: PHRPCOPAY
              type: decimal
              width: 11
            - name: PHRCOPAY
              type: decimal
              width: 11
            - name: PHRPRODSEL
              type: decimal
              width: 11
            - name: PHRATRTAX
              type: decimal
              width: 11
            - name: PHREXCEBFT
              type: decimal
              width: 11
            - name: PHRINCENTV
              type: decimal
              width: 11
            - name: PHRATRDED
              type: decimal
              width: 11
            - name: PHRCOB
              type: decimal
              width: 11
            - name: PHRTOTHAMT
              type: decimal
              width: 11
            - name: PHRPROFFEE
              type: decimal
              width: 11
            - name: PHROTHPAYA
              type: decimal
              width: 11
            - name: PHRCOSTSRC
              type: char
              width: 1
            - name: PHRCOSTTYP
              type: char
              width: 10
            - name: PHRPRCTYPE
              type: char
              width: 10
            - name: PHRRATE
              type: decimal
              width: 8
            - name: PHRPROCFEE
              type: decimal
              width: 11
            - name: PHRPATSTAX
              type: decimal
              width: 11
            - name: PHRPLNSTAX
              type: decimal
              width: 11
            - name: PHRPRVNSEL
              type: decimal
              width: 11
            - name: PHRPSCBRND
              type: decimal
              width: 11
            - name: PHRPSCNONP
              type: decimal
              width: 11
            - name: PHRPSCBRNP
              type: decimal
              width: 11
            - name: PHRCOVGAP
              type: decimal
              width: 11
            - name: PHRINGCSTC
              type: decimal
              width: 11
            - name: PHRDSPFEEC
              type: decimal
              width: 11
            - name: POSINGRCST
              type: decimal
              width: 11
            - name: POSDISPFEE
              type: decimal
              width: 11
            - name: POSPSLSTAX
              type: decimal
              width: 11
            - name: POSFSLSTAX
              type: decimal
              width: 11
            - name: POSSLSTAX
              type: decimal
              width: 11
            - name: POSPATPAY
              type: decimal
              width: 11
            - name: POSDUEAMT
              type: decimal
              width: 11
            - name: POSWITHHLD
              type: decimal
              width: 11
            - name: POSCOPAY
              type: decimal
              width: 11
            - name: POSPRODSEL
              type: decimal
              width: 11
            - name: POSATRTAX
              type: decimal
              width: 11
            - name: POSEXCEBFT
              type: decimal
              width: 11
            - name: POSINCENTV
              type: decimal
              width: 11
            - name: POSATRDED
              type: decimal
              width: 11
            - name: POSTOTHAMT
              type: decimal
              width: 11
            - name: POSPROFFEE
              type: decimal
              width: 11
            - name: POSOTHPAYA
              type: decimal
              width: 11
            - name: POSCOSTSRC
              type: char
              width: 1
            - name: POSPROCFEE
              type: decimal
              width: 11
            - name: POSPATSTAX
              type: decimal
              width: 11
            - name: POSPLNSTAX
              type: decimal
              width: 11
            - name: POSPRVNSEL
              type: decimal
              width: 11
            - name: POSPSCBRND
              type: decimal
              width: 11
            - name: POSPSCNONP
              type: decimal
              width: 11
            - name: POSPSCBRNP
              type: decimal
              width: 11
            - name: POSCOVGAP
              type: decimal
              width: 11
            - name: POSINGCSTC
              type: decimal
              width: 11
            - name: POSDSPFEEC
              type: decimal
              width: 11
            - name: CLIENTFLAG
              type: char
              width: 1
            - name: CLTINGRCST
              type: decimal
              width: 11
            - name: CLTDISPFEE
              type: decimal
              width: 11
            - name: CLTSLSTAX
              type: decimal
              width: 11
            - name: CLTPATPAY
              type: decimal
              width: 11
            - name: CLTDUEAMT
              type: decimal
              width: 11
            - name: CLTWITHHLD
              type: decimal
              width: 11
            - name: CLTPRCS
              type: char
              width: 10
            - name: CLTPRCST
              type: char
              width: 10
            - name: CLTPTPS
              type: char
              width: 10
            - name: CLTPTPST
              type: char
              width: 13
            - name: CLTCOPAYS
              type: char
              width: 10
            - name: CLTCOPAYSS
              type: decimal
              width: 2
            - name: CLTFCOPAY
              type: decimal
              width: 11
            - name: CLTPCOPAY
              type: decimal
              width: 11
            - name: CLTCOPAY
              type: decimal
              width: 11
            - name: CLTPRODSEL
              type: decimal
              width: 11
            - name: CLTPSTAX
              type: decimal
              width: 11
            - name: CLTFSTAX
              type: decimal
              width: 11
            - name: CLTATRTAX
              type: decimal
              width: 11
            - name: CLTEXCEBFT
              type: decimal
              width: 11
            - name: CLTINCENTV
              type: decimal
              width: 11
            - name: CLTATRDED
              type: decimal
              width: 11
            - name: CLTTOTHAMT
              type: decimal
              width: 11
            - name: CLTPROFFEE
              type: decimal
              width: 11
            - name: CLTCOB
              type: decimal
              width: 11
            - name: CLTOTHPAYA
              type: decimal
              width: 11
            - name: CLTCOSTSRC
              type: char
              width: 1
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTRATE
              type: decimal
              width: 8
            - name: CLTPRSCSTP
              type: decimal
              width: 3
            - name: CLTPRSCHNM
              type: char
              width: 18
            - name: CLTPROCFEE
              type: decimal
              width: 11
            - name: CLTPATSTAX
              type: decimal
              width: 11
            - name: CLTPLNSTAX
              type: decimal
              width: 11
            - name: CLTPRVNSEL
              type: decimal
              width: 11
            - name: CLTPSCBRND
              type: decimal
              width: 11
            - name: CLTPSCNONP
              type: decimal
              width: 11
            - name: CLTPSCBRNP
              type: decimal
              width: 11
            - name: CLTCOVGAP
              type: decimal
              width: 11
            - name: CLTINGCSTC
              type: decimal
              width: 11
            - name: CLTDSPFEEC
              type: decimal
              width: 11
            - name: RSPREIMBUR
              type: char
              width: 2
            - name: RSPINGRCST
              type: decimal
              width: 11
            - name: RSPDISPFEE
              type: decimal
              width: 11
            - name: RSPPSLSTAX
              type: decimal
              width: 11
            - name: RSPFSLSTAX
              type: decimal
              width: 11
            - name: RSPSLSTAX
              type: decimal
              width: 11
            - name: RSPPATPAY
              type: decimal
              width: 11
            - name: RSPDUEAMT
              type: decimal
              width: 11
            - name: RSPFCOPAY
              type: decimal
              width: 11
            - name: RSPPCOPAY
              type: decimal
              width: 11
            - name: RSPCOPAY
              type: decimal
              width: 11
            - name: RSPPRODSEL
              type: decimal
              width: 11
            - name: RSPATRTAX
              type: decimal
              width: 11
            - name: RSPEXCEBFT
              type: decimal
              width: 11
            - name: RSPINCENTV
              type: decimal
              width: 11
            - name: RSPATRDED
              type: decimal
              width: 11
            - name: RSPTOTHAMT
              type: decimal
              width: 11
            - name: RSPPROFEE
              type: decimal
              width: 11
            - name: RSPOTHPAYA
              type: decimal
              width: 11
            - name: RSPACCUDED
              type: decimal
              width: 11
            - name: RSPREMBFT
              type: decimal
              width: 11
            - name: RSPREMDED
              type: decimal
              width: 11
            - name: RSPPROCFEE
              type: decimal
              width: 11
            - name: RSPPATSTAX
              type: decimal
              width: 11
            - name: RSPPLNSTAX
              type: decimal
              width: 11
            - name: RSPPRVNSEL
              type: decimal
              width: 11
            - name: RSPPSCBRND
              type: decimal
              width: 11
            - name: RSPPSCNONP
              type: decimal
              width: 11
            - name: RSPPSCBRNP
              type: decimal
              width: 11
            - name: RSPCOVGAP
              type: decimal
              width: 11
            - name: RSPINGCSTC
              type: decimal
              width: 11
            - name: RSPDSPFEEC
              type: decimal
              width: 11
            - name: RSPPLANID
              type: char
              width: 8
            - name: BENSTGQL1
              type: char
              width: 2
            - name: BENSTGAMT1
              type: decimal
              width: 11
            - name: BENSTGQL2
              type: char
              width: 2
            - name: BENSTGAMT2
              type: decimal
              width: 11
            - name: BENSTGQL3
              type: char
              width: 2
            - name: BENSTGAMT3
              type: decimal
              width: 11
            - name: BENSTGQL4
              type: char
              width: 2
            - name: BENSTGAMT4
              type: decimal
              width: 11
            - name: ESTGENSAV
              type: decimal
              width: 11
            - name: SPDACCTREM
              type: decimal
              width: 11
            - name: HLTHPLNAMT
              type: decimal
              width: 11
            - name: INDDEDPTD
              type: decimal
              width: 11
            - name: INDDEDREM
              type: decimal
              width: 11
            - name: FAMDEDPTD
              type: decimal
              width: 11
            - name: FAMDEDREM
              type: decimal
              width: 11
            - name: DEDSCHED
              type: char
              width: 10
            - name: DEDACCC
              type: char
              width: 10
            - name: DEDFLAG
              type: char
              width: 1
            - name: INDLBFTUT_1
              type: decimal
              width: 11
            - name: INDLBFTPTD
              type: decimal
              width: 11
            - name: INDLBFTREM
              type: decimal
              width: 11
            - name: INDLBFTUT_2
              type: decimal
              width: 11
            - name: FAMLBFTPTD
              type: decimal
              width: 11
            - name: FAMLBFTREM
              type: decimal
              width: 11
            - name: LFTBFTMSCH
              type: char
              width: 10
            - name: LFTBFTACCC
              type: char
              width: 10
            - name: LFTBFTFLAG
              type: char
              width: 1
            - name: INDBFTUT
              type: decimal
              width: 11
            - name: INDBMAXPTD
              type: decimal
              width: 11
            - name: FAMBFTUT
              type: decimal
              width: 11
            - name: FAMBMAXPTD
              type: decimal
              width: 11
            - name: INDBMAXREM
              type: decimal
              width: 11
            - name: FAMBMAXREM
              type: decimal
              width: 11
            - name: BFTMAXSCHD
              type: char
              width: 10
            - name: BFTMAXACCC
              type: char
              width: 10
            - name: BFTMAXFLAG
              type: char
              width: 1
            - name: INDOOPPTD
              type: decimal
              width: 11
            - name: FAMOOPPTD
              type: decimal
              width: 11
            - name: INDOOPREM
              type: decimal
              width: 11
            - name: FAMOOPREM
              type: decimal
              width: 11
            - name: OOPSCHED
              type: char
              width: 10
            - name: OOPACCC
              type: char
              width: 10
            - name: OOPFLAG
              type: char
              width: 1
            - name: CONTRIBUT
              type: decimal
              width: 11
            - name: CONTBASIS
              type: char
              width: 2
            - name: CONTSCHED
              type: char
              width: 10
            - name: CONTACCCD
              type: char
              width: 10
            - name: CONTFLAG
              type: char
              width: 1
            - name: RXTFLAG
              type: char
              width: 1
            - name: REIMBURSMT
              type: char
              width: 1
            - name: CLMORIGIN
              type: char
              width: 1
            - name: HLDCLMFLAG
              type: char
              width: 1
            - name: HLDCLMDAYS
              type: decimal
              width: 3
            - name: PARTDFLAG
              type: char
              width: 1
            - name: COBEXTFLG
              type: char
              width: 1
            - name: PAEXTFLG
              type: char
              width: 1
            - name: HSAEXTIND
              type: char
              width: 1
            - name: FFPMEDRMST
              type: char
              width: 1
            - name: FFPMEDPXST
              type: char
              width: 1
            - name: FFPMEDMSST
              type: char
              width: 1
            - name: INCIDENTID
              type: char
              width: 25
            - name: ETCNBR
              type: char
              width: 30
            - name: DTEINJURY
              type: decimal
              width: 8
            - name: ADDUSER
              type: char
              width: 10
            - name: CHGUSER
              type: char
              width: 10
            - name: DMRUSERID
              type: char
              width: 10
            - name: PRAUSERID
              type: char
              width: 10
            - name: CLAIMREFID
              type: char
              width: 30
            - name: EOBDNOV
              type: char
              width: 3
            - name: EOBPDOV
              type: char
              width: 3
            - name: MANTRKNBR
              type: char
              width: 14
            - name: MANRECVDTE
              type: decimal
              width: 8
            - name: PASAUTHTYP
              type: char
              width: 2
            - name: PASAUTHID
              type: char
              width: 11
            - name: PASREQTYPE
              type: char
              width: 1
            - name: PASREQFROM
              type: decimal
              width: 8
            - name: PASREQTHRU
              type: decimal
              width: 8
            - name: PASBASISRQ
              type: char
              width: 2
            - name: PASREPFN
              type: char
              width: 12
            - name: PASREPLN
              type: char
              width: 15
            - name: PASSTREET
              type: char
              width: 30
            - name: PASCITY
              type: char
              width: 20
            - name: PASSTATE
              type: char
              width: 2
            - name: PASZIP
              type: char
              width: 15
            - name: PASPANBR
              type: char
              width: 11
            - name: PASAUTHNBR
              type: char
              width: 20
            - name: PASSDOCCT
              type: decimal
              width: 3
            - name: PAYERTYPE
              type: char
              width: 1
            - name: DELAYRSNCD
              type: char
              width: 2
            - name: MEDCDIND
              type: char
              width: 2
            - name: MEDCDID
              type: char
              width: 20
            - name: MEDCDAGNBR
              type: char
              width: 15
            - name: MEDCDTCN
              type: char
              width: 20
            - name: FMSTIER
              type: char
              width: 2
            - name: FMSSTATUS
              type: char
              width: 2
            - name: FMSDFLTIND
              type: char
              width: 1
            - name: FMSBENLST
              type: char
              width: 10
            - name: FMSLSTLVL3
              type: char
              width: 10
            - name: FMSLSTLVL2
              type: char
              width: 10
            - name: FMSLSTLVL1
              type: char
              width: 10
            - name: FMSRULESET
              type: char
              width: 10
            - name: FMSRULE
              type: char
              width: 10
            - name: FMSPROCCD
              type: char
              width: 2
            - name: CLIENTDEF1
              type: char
              width: 10
            - name: CLIENTDEF2
              type: char
              width: 10
            - name: CLIENTDEF3
              type: char
              width: 10
            - name: CLIENTDEF4
              type: char
              width: 10
            - name: CLIENTDEF5
              type: char
              width: 10
            - name: CCTRESERV1
              type: char
              width: 10
            - name: CCTRESERV2
              type: char
              width: 10
            - name: CCTRESERV3
              type: char
              width: 10
            - name: CCTRESERV4
              type: char
              width: 10
            - name: CCTRESERV5
              type: char
              width: 10
            - name: CCTRESERV6
              type: char
              width: 10
            - name: CCTRESERV7
              type: char
              width: 10
            - name: CCTRESERV8
              type: char
              width: 10
            - name: CCTRESERV9
              type: char
              width: 20
            - name: CCTRESRV10
              type: char
              width: 20
            - name: OOPAPPLIED
              type: decimal
              width: 11
            - name: CCTRESRV12
              type: decimal
              width: 11
            - name: CCTRESRV13
              type: decimal
              width: 11
            - name: CCTRESRV14
              type: decimal
              width: 11
            - name: USERFIELD
              type: char
              width: 10
            - name: EXTRACTDTE
              type: decimal
              width: 8
            - name: BATCHCTRL
              type: char
              width: 21
            - name: CLTTYPUCST
              type: decimal
              width: 14
            - name: REJCDE4
              type: char
              width: 3
            - name: REJCDE5
              type: char
              width: 3
            - name: REJCDE6
              type: char
              width: 3
            - name: MESSAGECD4
              type: char
              width: 10
            - name: MESSAGE4
              type: char
              width: 40
            - name: MESSAGECD5
              type: char
              width: 10
            - name: MESSAGE5
              type: char
              width: 40
            - name: MESSAGECD6
              type: char
              width: 10
            - name: MESSAGEG6
              type: char
              width: 40
            - name: CLIENT2FLAG
              type: char
              width: 1
            - name: CLT2INGRCST
              type: decimal
              width: 11
            - name: CLT2DISPFEE
              type: decimal
              width: 11
            - name: CLT2SLSTAX
              type: decimal
              width: 11
            - name: CLT2DUEAMT
              type: decimal
              width: 11
            - name: CLTPRSCSTP2
              type: decimal
              width: 3
            - name: CLTPRSCHNM2
              type: decimal
              width: 18
            - name: CLT2PRCS
              type: char
              width: 10
            - name: CLT2PRCST
              type: char
              width: 10
            - name: CLT2PSTAX
              type: decimal
              width: 11
            - name: CLT2FSTAX
              type: decimal
              width: 11
            - name: CLT2OTHAMT
              type: decimal
              width: 11
            - name: CLT2COSTSRC
              type: char
              width: 1
            - name: CLT2COSTTYP
              type: char
              width: 10
            - name: CLT2PRCTYPE
              type: char
              width: 10
            - name: CLT2RATE
              type: decimal
              width: 8
            - name: CLT2DCCSCID
              type: char
              width: 14
            - name: CLT2DCCSCSQ
              type: decimal
              width: 3
            - name: CLT2DCSCHID
              type: char
              width: 13
            - name: CLT2DCSCHSQ
              type: decimal
              width: 3
            - name: CALINCENTV2
              type: decimal
              width: 11
            - name: CLNT3FLAG
              type: char
              width: 1
            - name: CLT3INGRCST
              type: decimal
              width: 11
            - name: CLT3DISPFEE
              type: decimal
              width: 11
            - name: CLT3SLSTAX
              type: decimal
              width: 11
            - name: CLT3DUEAMT
              type: decimal
              width: 11
            - name: CLTPRSCSTP3
              type: decimal
              width: 3
            - name: CLTPRSCHNM3
              type: decimal
              width: 18
            - name: CLT3PRCS
              type: char
              width: 10
            - name: CLT3PRCST
              type: char
              width: 10
            - name: CLT3PSTAX
              type: decimal
              width: 11
            - name: CLT3FSTAX
              type: decimal
              width: 11
            - name: CLT3OTHAMT
              type: decimal
              width: 11
            - name: CLT3COSTSRC
              type: char
              width: 1
            - name: CLT3COSTTYP
              type: char
              width: 10
            - name: CLT3PRCTYPE
              type: char
              width: 10
            - name: CLT3RATE
              type: decimal
              width: 8
            - name: CLT3DCCSCID
              type: char
              width: 14
            - name: CLT3DCCSCSQ
              type: decimal
              width: 3
            - name: CLT3DCSCHID
              type: char
              width: 13
            - name: CLT3DCSCHSQ
              type: decimal
              width: 3
            - name: CALINCENTV3
              type: decimal
              width: 11
            - name: TBR_TBR_REJECT_REASON1
              type: char
              width: 5
            - name: TBR_TBR_REJECT_REASON2
              type: char
              width: 5
            - name: TBR_TBR_REJECT_REASON3
              type: char
              width: 5
            - name: SBM_QTY_INT_DIS
              type: char
              width: 13
            - name: SBM_DS_INT_DIS
              type: char
              width: 3
            - name: OTHR_AMT_CLMSBM_CT
              type: char
              width: 3
            - name: OTHR_AMT_CLMSBM_QLFR1
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM1
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR2
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM2
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR3
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM3
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR4
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM4
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR5
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM5
              type: char
              width: 11
            - name: RSN_SRVCDE1
              type: char
              width: 2
            - name: RSN_SRVCDE2
              type: char
              width: 2
            - name: RSN_SRVCDE3
              type: char
              width: 2
            - name: PROF_SRVCDE1
              type: char
              width: 2
            - name: PROF_SRVCDE2
              type: char
              width: 2
            - name: PROF_SRVCDE3
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE1
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE2
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE3
              type: char
              width: 2
            - name: SCH_RX_ID_NBR
              type: char
              width: 12
            - name: BASIS_CAL_DISPFEE
              type: char
              width: 2
            - name: BASIS_CAL_COPAY
              type: char
              width: 2
            - name: BASIS_CAL_FLT_TAX
              type: char
              width: 2
            - name: BASIS_CAL_PRCNT_TAX
              type: char
              width: 2
            - name: BASIS_CAL_COINSUR
              type: char
              width: 2
            - name: PAID_MSG_CODE_COUNT
              type: char
              width: 1
            - name: PAID_MESSAGE_CODE_1
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_2
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_3
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_4
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_5
              type: char
              width: 3
            - name: FILLER_FOR_REJECT_FIELD_IND
              type: char
              width: 26
        - name: OptumControl_Seq
          type: fileReader
          urls: 
            - "#$FilePath#/verified/#ControlInFile#.temp"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: "|"
          schema: 
            - name: DATE
              type: char
              width: 10
            - name: GROUP
              type: char
              width: 20
            - name: TOTAL_RECORDS
              type: decimal
              width: 9
            - name: INGREDIENT_COST
              type: decimal
              width: 13
            - name: AMT_COPAY
              type: decimal
              width: 13
            - name: AMT_PAYMENT
              type: decimal
              width: 13
            - name: QUANTITY
              type: decimal
              width: 9
        - name: GroupLevelCompare_Seq
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/Optum_reject_grouplevel_Land_#SrcSysCd#.dat.#RunID#"
          source: GroupLevelLogic_Xfr
          option: 
            updateMode: overwrite
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
        - name: AmoutLevelCompare_Seq
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/Optum_reject_amtlevel_Land_#SrcSysCd#.dat.#RunID#"
          source: AmountsLevelLogic_Xfr
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: ControlGroupLevel_HashFile
          type: map
          source: "ControlFile_Xfr.V0S448P3"
          alias: OptumContolFileIndvl
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: GROUP
              type: varchar
              width: 20
              map: "OptumContolFileIndvl.GROUP"
            - name: TOTAL_RECORDS
              type: varchar
              width: 9
              map: "OptumContolFileIndvl.TOTAL_RECORDS"
            - name: DATE
              type: char
              width: 10
              map: "OptumContolFileIndvl.DATE"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "OptumContolFileIndvl.INGREDIENT_COST"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "OptumContolFileIndvl.AMT_COPAY"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "OptumContolFileIndvl.AMT_PAYMENT"
            - name: QUANTITY
              type: varchar
              width: 7
              map: "OptumContolFileIndvl.QUANTITY"
        - name: AmountLevel_HashFile
          type: map
          source: AmountLevel_Aggr
          alias: Cnt_FullAgg_Amts
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: ID
              type: varchar
              width: 20
              map: "Cnt_FullAgg_Amts.ID"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "Cnt_FullAgg_Amts.INGREDIENT_COST"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "Cnt_FullAgg_Amts.AMT_COPAY"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "Cnt_FullAgg_Amts.AMT_PAYMENT"
        - name: GroupLevelLogic_Xfr.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: DetailGroupLevel_Aggr
              alias: DetailGroupLevel_lkp
              joinFilter: ""
              joinExpression: []
            - name: ControlGroupLevel_HashFile
              alias: ControlGroupLevel_lkp
              joinFilter: ""
              joinExpression:
              - fromExpr: "ControlGroupLevel_lkp.GROUP"
                toExpr: "DetailGroupLevel_lkp.GROUPID"
                caseInSensitive: true
              - fromExpr: "ControlGroupLevel_lkp.TOTAL_RECORDS"
                toExpr: "DetailGroupLevel_lkp.GROUPID_CNT"
                caseInSensitive: true
              - fromExpr: "ControlGroupLevel_lkp.INGREDIENT_COST"
                toExpr: "DetailGroupLevel_lkp.CLTINGRCST"
                caseInSensitive: true
          joinCondition: 
            - expression: "ControlGroupLevel_lkp.GROUP = DetailGroupLevel_lkp.GROUPID"
              caseInSensitive: true
            - expression: "ControlGroupLevel_lkp.TOTAL_RECORDS = DetailGroupLevel_lkp.GROUPID_CNT"
              caseInSensitive: true
            - expression: "ControlGroupLevel_lkp.INGREDIENT_COST = DetailGroupLevel_lkp.CLTINGRCST"
              caseInSensitive: true
          schema: 
            - name: GroupLevel_Lnk_GROUPID
              type: varchar
              width: 20
              map: "DetailGroupLevel_lkp.GROUPID"
            - name: GroupLevel_Lnk_GROUPID_CNT
              type: varchar
              width: 9
              map: "DetailGroupLevel_lkp.GROUPID_CNT"
            - name: GroupLevel_Lnk_CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: "DetailGroupLevel_lkp.CLTINGRCST"
            - name: GroupLevel_Lnk_CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: "DetailGroupLevel_lkp.CLTPATPAY"
            - name: GroupLevel_Lnk_CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: "DetailGroupLevel_lkp.CLTDUEAMT"
            - name: GroupLevel_Lnk_DATE
              type: char
              width: 10
              map: "ControlGroupLevel_lkp.DATE"
            - name: GroupLevel_Lnk_GROUP
              type: varchar
              width: 20
              map: "ControlGroupLevel_lkp.GROUP"
            - name: GroupLevel_Lnk_TOTAL_RECORDS
              type: varchar
              width: 9
              map: "ControlGroupLevel_lkp.TOTAL_RECORDS"
            - name: GroupLevel_Lnk_INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "ControlGroupLevel_lkp.INGREDIENT_COST"
            - name: GroupLevel_Lnk_AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "ControlGroupLevel_lkp.AMT_COPAY"
            - name: GroupLevel_Lnk_AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "ControlGroupLevel_lkp.AMT_PAYMENT"
            - name: GroupLevel_Lnk_QUANTITY
              type: varchar
              width: 9
              map: "ControlGroupLevel_lkp.QUANTITY"
            - name: filter_GroupLevel_Lnk
              type: smallint
              width: 2
              nullable: N
              map: "if (DetailGroupLevel_lkp.GROUPID = ControlGroupLevel_lkp.GROUP and (DetailGroupLevel_lkp.GROUPID_CNT <> ControlGroupLevel_lkp.TOTAL_RECORDS or DetailGroupLevel_lkp.CLTINGRCST <> ControlGroupLevel_lkp.INGREDIENT_COST or DetailGroupLevel_lkp.CLTPATPAY <> ControlGroupLevel_lkp.AMT_COPAY or DetailGroupLevel_lkp.CLTDUEAMT <> ControlGroupLevel_lkp.AMT_PAYMENT)) then 1 else 0"
        - name: GroupLevelLogic_Xfr.filter
          type: filter
          source: GroupLevelLogic_Xfr.lookup
          filter: "filter_GroupLevel_Lnk = 1"
          schema: 
            - name: GroupLevel_Lnk_GROUPID
              type: varchar
              width: 20
            - name: GroupLevel_Lnk_GROUPID_CNT
              type: varchar
              width: 9
            - name: GroupLevel_Lnk_CLTINGRCST
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_CLTPATPAY
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_DATE
              type: char
              width: 10
            - name: GroupLevel_Lnk_GROUP
              type: varchar
              width: 20
            - name: GroupLevel_Lnk_TOTAL_RECORDS
              type: varchar
              width: 9
            - name: GroupLevel_Lnk_INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_AMT_COPAY
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
            - name: GroupLevel_Lnk_QUANTITY
              type: varchar
              width: 9
        - name: GroupLevelLogic_Xfr
          type: map
          source: GroupLevelLogic_Xfr.filter
          alias: GroupLevel_Lnk
          schema: 
            - name: GROUPID
              type: varchar
              width: 20
              map: "GroupLevel_Lnk.GroupLevel_Lnk_GROUPID"
            - name: GROUPID_CNT
              type: varchar
              width: 9
              map: "GroupLevel_Lnk.GroupLevel_Lnk_GROUPID_CNT"
            - name: CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_CLTINGRCST"
            - name: CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_CLTDUEAMT"
            - name: DATE
              type: char
              width: 10
              map: "GroupLevel_Lnk.GroupLevel_Lnk_DATE"
            - name: GROUP
              type: varchar
              width: 20
              map: "GroupLevel_Lnk.GroupLevel_Lnk_GROUP"
            - name: TOTAL_RECORDS
              type: varchar
              width: 9
              map: "GroupLevel_Lnk.GroupLevel_Lnk_TOTAL_RECORDS"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_INGREDIENT_COST"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_AMT_COPAY"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "GroupLevel_Lnk.GroupLevel_Lnk_AMT_PAYMENT"
            - name: QUANTITY
              type: varchar
              width: 9
              map: "GroupLevel_Lnk.GroupLevel_Lnk_QUANTITY"
        - name: AmountsLevelLogic_Xfr.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: DetailAmountLevel_Aggr
              alias: DetailAmountLevel_Lnk
              joinFilter: ""
              joinExpression: []
            - name: AmountLevel_HashFile
              alias: AmoutLevelCompare_lkp
              joinFilter: ""
              joinExpression:
              - fromExpr: "AmoutLevelCompare_lkp.ID"
                toExpr: "DetailAmountLevel_Lnk.ID"
                caseInSensitive: true
              - fromExpr: "AmoutLevelCompare_lkp.INGREDIENT_COST"
                toExpr: "DetailAmountLevel_Lnk.CLTINGRCST"
                caseInSensitive: true
          joinCondition: 
            - expression: "AmoutLevelCompare_lkp.ID = DetailAmountLevel_Lnk.ID"
              caseInSensitive: true
            - expression: "AmoutLevelCompare_lkp.INGREDIENT_COST = DetailAmountLevel_Lnk.CLTINGRCST"
              caseInSensitive: true
          schema: 
            - name: AmountLevel_Lnk_CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: "DetailAmountLevel_Lnk.CLTINGRCST"
            - name: AmountLevel_Lnk_CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: "DetailAmountLevel_Lnk.CLTPATPAY"
            - name: AmountLevel_Lnk_CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: "DetailAmountLevel_Lnk.CLTDUEAMT"
            - name: AmountLevel_Lnk_INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "AmoutLevelCompare_lkp.INGREDIENT_COST"
            - name: AmountLevel_Lnk_AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "AmoutLevelCompare_lkp.AMT_COPAY"
            - name: AmountLevel_Lnk_AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "AmoutLevelCompare_lkp.AMT_PAYMENT"
            - name: filter_AmountLevel_Lnk
              type: smallint
              width: 2
              nullable: N
              map: "if (DetailAmountLevel_Lnk.ID = AmoutLevelCompare_lkp.ID and (DetailAmountLevel_Lnk.CLTINGRCST <> AmoutLevelCompare_lkp.INGREDIENT_COST or DetailAmountLevel_Lnk.CLTPATPAY <> AmoutLevelCompare_lkp.AMT_COPAY or DetailAmountLevel_Lnk.CLTDUEAMT <> AmoutLevelCompare_lkp.AMT_PAYMENT)) then 1 else 0"
        - name: AmountsLevelLogic_Xfr.filter
          type: filter
          source: AmountsLevelLogic_Xfr.lookup
          filter: "filter_AmountLevel_Lnk = 1"
          schema: 
            - name: AmountLevel_Lnk_CLTINGRCST
              type: decimal
              width: 11
              precision: 2
            - name: AmountLevel_Lnk_CLTPATPAY
              type: decimal
              width: 11
              precision: 2
            - name: AmountLevel_Lnk_CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
            - name: AmountLevel_Lnk_INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
            - name: AmountLevel_Lnk_AMT_COPAY
              type: decimal
              width: 11
              precision: 2
            - name: AmountLevel_Lnk_AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
        - name: AmountsLevelLogic_Xfr
          type: map
          source: AmountsLevelLogic_Xfr.filter
          alias: AmountLevel_Lnk
          schema: 
            - name: CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_CLTINGRCST"
            - name: CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_CLTDUEAMT"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_INGREDIENT_COST"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_AMT_COPAY"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "AmountLevel_Lnk.AmountLevel_Lnk_AMT_PAYMENT"
        - name: ControlFile_Xfr.V0S448P3
          type: map
          source: OptumControl_Seq
          alias: Extract
          schema: 
            - name: GROUP
              type: varchar
              width: 20
              map: "Trim(Extract.GROUP)"
            - name: TOTAL_RECORDS
              type: varchar
              width: 9
              map: "Trim(Extract.TOTAL_RECORDS)"
            - name: DATE
              type: char
              width: 10
              map: "Extract.DATE"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.INGREDIENT_COST)"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.AMT_COPAY)"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.AMT_PAYMENT)"
            - name: QUANTITY
              type: decimal
              width: 7
              precision: 2
              map: "Extract.QUANTITY"
          keys:
            - GROUP
            - TOTAL_RECORDS
        - name: ControlFile_Xfr.V0S448P5
          type: map
          source: OptumControl_Seq
          alias: Extract
          schema: 
            - name: ID
              type: varchar
              width: 20
              map: "1"
            - name: INGREDIENT_COST
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.INGREDIENT_COST)"
            - name: AMT_COPAY
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.AMT_COPAY)"
            - name: AMT_PAYMENT
              type: decimal
              width: 11
              precision: 2
              map: "Convert('() ', '', Extract.AMT_PAYMENT)"
          keys:
            - ID
        - name: DataCleansing.V168S9P4
          type: map
          source: DataCleansing.V168S9P4.filter
          alias: Extract
          schema: 
            - name: GROUPID
              type: varchar
              width: 20
              map: "Trim(Extract.GROUPID)"
            - name: CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTINGRCST) = @TRUE or Len(Trim(Extract.CLTINGRCST)) = 0) then '0.0' Else Extract.CLTINGRCST"
            - name: CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTPATPAY) = @TRUE or Len(Trim(Extract.CLTPATPAY)) = 0) then '0.0' Else Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTDUEAMT) = @TRUE or Len(Trim(Extract.CLTDUEAMT)) = 0) then '0.0' Else Extract.CLTDUEAMT"
        - name: DataCleansing.V168S9P4.filter
          type: filter
          source: OptumDetail_Seq
          filter: "Len(Trim(GROUPID)) > 0"
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: CARID
              type: char
              width: 9
            - name: SCARID
              type: char
              width: 9
            - name: CARRPROC
              type: decimal
              width: 10
            - name: CLNTID
              type: char
              width: 3
            - name: CLNTSGMNT
              type: char
              width: 3
            - name: CLNTREGION
              type: char
              width: 2
            - name: ACCOUNTID
              type: char
              width: 15
            - name: ACCTBENCDE
              type: char
              width: 10
            - name: VERSIONNBR
              type: char
              width: 2
            - name: GROUPID
              type: char
              width: 15
            - name: GROUPPLAN
              type: char
              width: 10
            - name: GRPCLIBENF
              type: char
              width: 10
            - name: GROUPSIC
              type: char
              width: 4
            - name: CLMRESPSTS
              type: char
              width: 1
            - name: MEMBERID
              type: char
              width: 20
            - name: MBRLSTNME
              type: char
              width: 25
            - name: MBRFSTNME
              type: char
              width: 15
            - name: MBRMDINIT
              type: char
              width: 1
            - name: MBRPRSNCDE
              type: char
              width: 3
            - name: MBRRELCDE
              type: char
              width: 1
            - name: MBRSEX
              type: char
              width: 1
            - name: MBRBIRTH
              type: decimal
              width: 8
            - name: MBRAGE
              type: decimal
              width: 3
            - name: MBRZIP
              type: char
              width: 15
            - name: SOCSECNBR
              type: char
              width: 9
            - name: DURKEY
              type: char
              width: 18
            - name: DURFLAG
              type: char
              width: 1
            - name: MBRFAMLYID
              type: char
              width: 20
            - name: MBRFAMLIND
              type: char
              width: 1
            - name: MBRFAMLTYP
              type: char
              width: 1
            - name: COBIND
              type: char
              width: 2
            - name: MBRPLAN
              type: char
              width: 10
            - name: MBRPRODCDE
              type: char
              width: 6
            - name: MBRRIDERCD
              type: char
              width: 6
            - name: CARENETID
              type: char
              width: 10
            - name: CAREQUALID
              type: char
              width: 10
            - name: CAREFACID
              type: char
              width: 10
            - name: CAREFACNAM
              type: char
              width: 25
            - name: MBRPCPHYS
              type: char
              width: 15
            - name: PPRSFSTNME
              type: char
              width: 15
            - name: PPRSLSTNME
              type: char
              width: 25
            - name: PPRSMDINIT
              type: char
              width: 1
            - name: PPRSSPCCDE
              type: char
              width: 6
            - name: PPRSTATE
              type: char
              width: 3
            - name: MBRALTINFL
              type: char
              width: 1
            - name: MBRALTINCD
              type: char
              width: 10
            - name: MBRALTINID
              type: char
              width: 20
            - name: MBRMEDDTE
              type: decimal
              width: 8
            - name: MBRMEDTYPE
              type: char
              width: 1
            - name: MBRHICCDE
              type: char
              width: 11
            - name: CARDHOLDER
              type: char
              width: 20
            - name: PATLASTNME
              type: char
              width: 15
            - name: PATFRSTNME
              type: char
              width: 12
            - name: PERSONCDE
              type: char
              width: 3
            - name: RELATIONCD
              type: char
              width: 1
            - name: SEXCODE
              type: char
              width: 1
            - name: BIRTHDTE
              type: decimal
              width: 8
            - name: ELIGCLARIF
              type: char
              width: 1
            - name: CUSTLOC
              type: char
              width: 2
            - name: SBMPLSRVCE
              type: char
              width: 2
            - name: SBMPATRESD
              type: char
              width: 2
            - name: PRMCAREPRV
              type: char
              width: 15
            - name: PRMCAREPRQ
              type: char
              width: 2
            - name: FACILITYID
              type: char
              width: 10
            - name: OTHCOVERAG
              type: char
              width: 2
            - name: BINNUMBER
              type: char
              width: 6
            - name: PROCESSOR
              type: char
              width: 10
            - name: GROUPNBR
              type: char
              width: 15
            - name: TRANSCDE
              type: char
              width: 2
            - name: DATESBM
              type: decimal
              width: 8
            - name: TIMESBM
              type: decimal
              width: 6
            - name: CHGDATE
              type: decimal
              width: 7
            - name: CHGTIME
              type: decimal
              width: 6
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: RVDATESBM
              type: decimal
              width: 8
            - name: CLMCOUNTER
              type: decimal
              width: 2
            - name: GENERICCTR
              type: decimal
              width: 2
            - name: FORMLRYCTR
              type: decimal
              width: 2
            - name: RXNUMBER
              type: char
              width: 12
            - name: RXNUMBERQL
              type: char
              width: 1
            - name: REFILL
              type: char
              width: 2
            - name: DISPSTATUS
              type: char
              width: 1
            - name: DTEFILLED
              type: decimal
              width: 8
            - name: COMPOUNDCD
              type: char
              width: 1
            - name: SBMCMPDTYP
              type: char
              width: 2
            - name: PRODTYPCDE
              type: char
              width: 2
            - name: PRODUCTID
              type: char
              width: 20
            - name: PRODUCTKEY
              type: decimal
              width: 9
            - name: METRICQTY
              type: decimal
              width: 6
            - name: DECIMALQTY
              type: decimal
              width: 13
            - name: DAYSSUPPLY
              type: decimal
              width: 3
            - name: PSC
              type: char
              width: 1
            - name: WRITTENDTE
              type: decimal
              width: 8
            - name: NBRFLSAUTH
              type: decimal
              width: 2
            - name: ORIGINCDE
              type: char
              width: 1
            - name: SBMCLARCD1
              type: char
              width: 2
            - name: SBMCLARCD2
              type: char
              width: 2
            - name: SBMCLARCD3
              type: char
              width: 2
            - name: PAMCNBR
              type: char
              width: 11
            - name: PAMCCDE
              type: char
              width: 2
            - name: PRAUTHNBR
              type: char
              width: 11
            - name: PRAUTHRSN
              type: char
              width: 2
            - name: PRAUTHFDTE
              type: decimal
              width: 7
            - name: PRAUTHTDTE
              type: decimal
              width: 7
            - name: LABELNAME
              type: char
              width: 30
            - name: PRODNM
              type: char
              width: 70
            - name: DRUGMFGRID
              type: char
              width: 5
            - name: DRUGMFGR
              type: char
              width: 10
            - name: GPINUMBER
              type: char
              width: 14
            - name: GNRCNME
              type: char
              width: 60
            - name: PRDPACUOM
              type: char
              width: 2
            - name: PRDPACSIZE
              type: decimal
              width: 12
            - name: DDID
              type: decimal
              width: 6
            - name: GCN
              type: decimal
              width: 5
            - name: GCNSEQ
              type: decimal
              width: 6
            - name: KDC
              type: decimal
              width: 10
            - name: AHFS
              type: char
              width: 8
            - name: DRUGDEACOD
              type: char
              width: 1
            - name: RXOTCIND
              type: char
              width: 1
            - name: MULTSRCCDE
              type: char
              width: 1
            - name: GENINDOVER
              type: char
              width: 1
            - name: PRDREIMIND
              type: char
              width: 1
            - name: BRNDTRDNME
              type: char
              width: 1
            - name: FDATHERAEQ
              type: char
              width: 2
            - name: METRICSTRG
              type: decimal
              width: 12
            - name: DRGSTRGUOM
              type: char
              width: 10
            - name: ADMINROUTE
              type: char
              width: 2
            - name: ADMINRTESN
              type: char
              width: 11
            - name: DOSAGEFORM
              type: char
              width: 4
            - name: NDA
              type: char
              width: 1
            - name: ANDA
              type: char
              width: 1
            - name: ANDAOR
              type: char
              width: 1
            - name: RXNORMCODE
              type: char
              width: 10
            - name: MNTDRUGCDE
              type: char
              width: 1
            - name: MNTSOURCE
              type: char
              width: 1
            - name: MNTCARPROR
              type: char
              width: 1
            - name: MNTGPILIST
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: CPQSPCSCHD
              type: char
              width: 10
            - name: THRDPARTYX
              type: char
              width: 1
            - name: DRGUNITDOS
              type: char
              width: 1
            - name: SBMUNITDOS
              type: char
              width: 1
            - name: ALTPRODTYP
              type: char
              width: 1
            - name: ALTPRODCDE
              type: char
              width: 13
            - name: SRXNETWRK
              type: char
              width: 6
            - name: SRXNETTYPE
              type: char
              width: 2
            - name: RXNETWORK
              type: char
              width: 6
            - name: RXNETWRKNM
              type: char
              width: 25
            - name: RXNETCARR
              type: char
              width: 9
            - name: RXNETWRKQL
              type: char
              width: 1
            - name: RXNETPRCQL
              type: char
              width: 1
            - name: REGIONCDE
              type: char
              width: 10
            - name: PHRAFFIL
              type: char
              width: 10
            - name: NETPRIOR
              type: char
              width: 3
            - name: NETTYPE
              type: char
              width: 2
            - name: NETSEQ
              type: decimal
              width: 5
            - name: PAYCNTR
              type: char
              width: 10
            - name: PHRNDCLST
              type: char
              width: 10
            - name: PHRGPILST
              type: char
              width: 10
            - name: SRVPROVID
              type: char
              width: 15
            - name: SRVPROVIDQ
              type: char
              width: 2
            - name: NPIPROV
              type: char
              width: 10
            - name: PRVNCPDPID
              type: char
              width: 12
            - name: SBMSRVPRID
              type: char
              width: 15
            - name: SBMSRVPRQL
              type: char
              width: 2
            - name: SRVPROVNME
              type: char
              width: 55
            - name: PROVLOCKQL
              type: char
              width: 1
            - name: PROVLOCKID
              type: char
              width: 15
            - name: STORENBR
              type: char
              width: 15
            - name: AFFILIATIN
              type: char
              width: 6
            - name: PAYEEID
              type: char
              width: 12
            - name: DISPRCLASS
              type: char
              width: 3
            - name: DISPROTHER
              type: char
              width: 3
            - name: PHARMZIP
              type: char
              width: 10
            - name: PRESNETWID
              type: char
              width: 6
            - name: PRESCRIBER
              type: char
              width: 15
            - name: PRESCRIDQL
              type: char
              width: 2
            - name: NPIPRESCR
              type: char
              width: 10
            - name: PRESCDEAID
              type: char
              width: 15
            - name: PRESLSTNME
              type: char
              width: 25
            - name: PRESFSTNME
              type: char
              width: 15
            - name: PRESMDINIT
              type: char
              width: 1
            - name: PRESSPCCDE
              type: char
              width: 6
            - name: PRESSPCCDQ
              type: char
              width: 3
            - name: PRSSTATE
              type: char
              width: 3
            - name: SBMRPHID
              type: char
              width: 15
            - name: SBMRPHIDQL
              type: char
              width: 2
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: PLANTYPE
              type: char
              width: 8
            - name: PLANQUAL
              type: char
              width: 10
            - name: PLNNDCLIST
              type: char
              width: 10
            - name: LSTQUALNDC
              type: char
              width: 10
            - name: PLNGPILIST
              type: char
              width: 10
            - name: LSTQUALGPI
              type: char
              width: 10
            - name: PLNPNDCLST
              type: char
              width: 10
            - name: PLNPGPILST
              type: char
              width: 10
            - name: PLANDRUGST
              type: char
              width: 1
            - name: PLANFRMLRY
              type: char
              width: 1
            - name: PLNFNLPSCH
              type: char
              width: 10
            - name: PHRDCSCHID
              type: char
              width: 13
            - name: PHRDCSCHSQ
              type: decimal
              width: 3
            - name: CLTDCSCHID
              type: char
              width: 13
            - name: CLTDCSCHSQ
              type: decimal
              width: 3
            - name: PHRDCCSCID
              type: char
              width: 14
            - name: PHRDCCSCSQ
              type: decimal
              width: 3
            - name: CLTDCCSCID
              type: char
              width: 14
            - name: CLTDCCSCSQ
              type: decimal
              width: 3
            - name: PHRPRTSCID
              type: char
              width: 13
            - name: CLTPRTSCID
              type: char
              width: 13
            - name: PHRRMSCHID
              type: char
              width: 10
            - name: CLTRMSCHID
              type: char
              width: 10
            - name: PRDPFLSTID
              type: char
              width: 10
            - name: PRFPRDSCID
              type: char
              width: 10
            - name: FORMULARY
              type: char
              width: 1
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: TIERVALUE
              type: decimal
              width: 2
            - name: CONTHERAPY
              type: char
              width: 1
            - name: CTSCHEDID
              type: char
              width: 20
            - name: PRBASISCHD
              type: char
              width: 10
            - name: REGDISOR
              type: char
              width: 10
            - name: DRUGSTSTBL
              type: char
              width: 13
            - name: TRANSBEN
              type: char
              width: 10
            - name: MESSAGECD1
              type: char
              width: 10
            - name: MESSAGE1
              type: char
              width: 40
            - name: MESSAGECD2
              type: char
              width: 10
            - name: MESSAGE2
              type: char
              width: 40
            - name: MESSAGECD3
              type: char
              width: 10
            - name: MESSAGE3
              type: char
              width: 40
            - name: REJCNT
              type: decimal
              width: 2
            - name: REJCDE1
              type: char
              width: 3
            - name: REJCDE2
              type: char
              width: 3
            - name: REJCDE3
              type: char
              width: 3
            - name: RJCPLANID
              type: char
              width: 8
            - name: DURCONFLCT
              type: char
              width: 2
            - name: DURINTERVN
              type: char
              width: 2
            - name: DUROUTCOME
              type: char
              width: 2
            - name: LVLSERVICE
              type: char
              width: 2
            - name: PHARSRVTYP
              type: char
              width: 2
            - name: DIAGNOSIS
              type: char
              width: 15
            - name: DIAGNOSISQ
              type: char
              width: 2
            - name: RVDURCNFLC
              type: char
              width: 2
            - name: RVDURINTRV
              type: char
              width: 2
            - name: RVDUROUTCM
              type: char
              width: 2
            - name: RVLVLSERVC
              type: char
              width: 2
            - name: DRGCNFLCT1
              type: char
              width: 2
            - name: SEVERITY1
              type: char
              width: 1
            - name: OTHRPHARM1
              type: char
              width: 1
            - name: DTEPRVFIL1
              type: decimal
              width: 8
            - name: QTYPRVFIL1
              type: decimal
              width: 12
            - name: DATABASE1
              type: char
              width: 1
            - name: OTHRPRESC1
              type: char
              width: 1
            - name: FREETEXT1
              type: char
              width: 30
            - name: DRGCNFLCT2
              type: char
              width: 2
            - name: SEVERITY2
              type: char
              width: 1
            - name: OTHRPHARM2
              type: char
              width: 1
            - name: DTEPRVFIL2
              type: decimal
              width: 8
            - name: QTYPRVFIL2
              type: decimal
              width: 12
            - name: DATABASE2
              type: char
              width: 1
            - name: OTHRPRESC2
              type: char
              width: 1
            - name: FREETEXT2
              type: char
              width: 30
            - name: DRGCNFLCT3
              type: char
              width: 2
            - name: SEVERITY3
              type: char
              width: 1
            - name: OTHRPHARM3
              type: char
              width: 1
            - name: DTEPRVFIL3
              type: decimal
              width: 8
            - name: QTYPRVFIL3
              type: decimal
              width: 12
            - name: DATABASE3
              type: char
              width: 1
            - name: OTHRPRESC3
              type: char
              width: 1
            - name: FREETEXT3
              type: varchar
              width: 30
            - name: FEETYPE
              type: char
              width: 2
            - name: BENUNITCST
              type: decimal
              width: 14
            - name: AWPUNITCST
              type: decimal
              width: 14
            - name: WACUNITCST
              type: decimal
              width: 14
            - name: GEAPUNTCST
              type: decimal
              width: 14
            - name: CTYPEUCOST
              type: decimal
              width: 14
            - name: BASISCOST
              type: char
              width: 2
            - name: PRICEQTY
              type: decimal
              width: 13
            - name: PRODAYSSUP
              type: decimal
              width: 3
            - name: PROQTY
              type: decimal
              width: 12
            - name: RVINCNTVSB
              type: decimal
              width: 11
            - name: SBMINGRCST
              type: decimal
              width: 11
            - name: SBMDISPFEE
              type: decimal
              width: 11
            - name: SBMPSLSTX
              type: decimal
              width: 11
            - name: SBMFSLSTX
              type: decimal
              width: 11
            - name: SBMSLSTX
              type: decimal
              width: 11
            - name: SBMPATPAY
              type: decimal
              width: 11
            - name: SBMAMTDUE
              type: decimal
              width: 11
            - name: SBMINCENTV
              type: decimal
              width: 11
            - name: SBMPROFFEE
              type: decimal
              width: 11
            - name: SBMTOTHAMT
              type: decimal
              width: 11
            - name: SBMOPAMTCT
              type: decimal
              width: 1
            - name: SBMOPAMTQL
              type: char
              width: 2
            - name: USUALNCUST
              type: decimal
              width: 11
            - name: DENIALDTE
              type: decimal
              width: 8
            - name: OTHRPAYOR
              type: decimal
              width: 11
            - name: SBMMDPDAMT
              type: decimal
              width: 11
            - name: CALINGRCST
              type: decimal
              width: 11
            - name: CALDISPFEE
              type: decimal
              width: 11
            - name: CALPSTAX
              type: decimal
              width: 11
            - name: CALFSTAX
              type: decimal
              width: 11
            - name: CALSLSTAX
              type: decimal
              width: 11
            - name: CALPATPAY
              type: decimal
              width: 11
            - name: CALDUEAMT
              type: decimal
              width: 11
            - name: CALWITHHLD
              type: decimal
              width: 11
            - name: CALFCOPAY
              type: decimal
              width: 11
            - name: CALPCOPAY
              type: decimal
              width: 11
            - name: CALCOPAY
              type: decimal
              width: 11
            - name: CALPRODSEL
              type: decimal
              width: 11
            - name: CALATRTAX
              type: decimal
              width: 11
            - name: CALEXCEBFT
              type: decimal
              width: 11
            - name: CALINCENTV
              type: decimal
              width: 11
            - name: CALATRDED
              type: decimal
              width: 11
            - name: CALCOB
              type: decimal
              width: 11
            - name: CALTOTHAMT
              type: decimal
              width: 11
            - name: CALPROFFEE
              type: decimal
              width: 11
            - name: CALOTHPAYA
              type: decimal
              width: 11
            - name: CALCOSTSRC
              type: char
              width: 1
            - name: CALADMNFEE
              type: decimal
              width: 11
            - name: CALPROCFEE
              type: decimal
              width: 11
            - name: CALPATSTAX
              type: decimal
              width: 11
            - name: CALPLNSTAX
              type: decimal
              width: 11
            - name: CALPRVNSEL
              type: decimal
              width: 11
            - name: CALPSCBRND
              type: decimal
              width: 11
            - name: CALPSCNONP
              type: decimal
              width: 11
            - name: CALPSCBRNP
              type: decimal
              width: 11
            - name: CALCOVGAP
              type: decimal
              width: 11
            - name: CALINGCSTC
              type: decimal
              width: 11
            - name: CALDSPFEEC
              type: decimal
              width: 11
            - name: PHRINGRCST
              type: decimal
              width: 11
            - name: PHRDISPFEE
              type: decimal
              width: 11
            - name: PHRPPSTAX
              type: decimal
              width: 11
            - name: PHRFSTAX
              type: decimal
              width: 11
            - name: PHRSLSTAX
              type: decimal
              width: 11
            - name: PHRPATPAY
              type: decimal
              width: 11
            - name: PHRDUEAMT
              type: decimal
              width: 11
            - name: PHRWITHHLD
              type: decimal
              width: 11
            - name: PHRPPRCS
              type: char
              width: 10
            - name: PHRPRCST
              type: char
              width: 10
            - name: PHRPTPS
              type: char
              width: 10
            - name: PHRPTPST
              type: char
              width: 13
            - name: PHRCOPAYSC
              type: char
              width: 10
            - name: PHRCOPAYSS
              type: decimal
              width: 2
            - name: PHRFCOPAY
              type: decimal
              width: 11
            - name: PHRPCOPAY
              type: decimal
              width: 11
            - name: PHRCOPAY
              type: decimal
              width: 11
            - name: PHRPRODSEL
              type: decimal
              width: 11
            - name: PHRATRTAX
              type: decimal
              width: 11
            - name: PHREXCEBFT
              type: decimal
              width: 11
            - name: PHRINCENTV
              type: decimal
              width: 11
            - name: PHRATRDED
              type: decimal
              width: 11
            - name: PHRCOB
              type: decimal
              width: 11
            - name: PHRTOTHAMT
              type: decimal
              width: 11
            - name: PHRPROFFEE
              type: decimal
              width: 11
            - name: PHROTHPAYA
              type: decimal
              width: 11
            - name: PHRCOSTSRC
              type: char
              width: 1
            - name: PHRCOSTTYP
              type: char
              width: 10
            - name: PHRPRCTYPE
              type: char
              width: 10
            - name: PHRRATE
              type: decimal
              width: 8
            - name: PHRPROCFEE
              type: decimal
              width: 11
            - name: PHRPATSTAX
              type: decimal
              width: 11
            - name: PHRPLNSTAX
              type: decimal
              width: 11
            - name: PHRPRVNSEL
              type: decimal
              width: 11
            - name: PHRPSCBRND
              type: decimal
              width: 11
            - name: PHRPSCNONP
              type: decimal
              width: 11
            - name: PHRPSCBRNP
              type: decimal
              width: 11
            - name: PHRCOVGAP
              type: decimal
              width: 11
            - name: PHRINGCSTC
              type: decimal
              width: 11
            - name: PHRDSPFEEC
              type: decimal
              width: 11
            - name: POSINGRCST
              type: decimal
              width: 11
            - name: POSDISPFEE
              type: decimal
              width: 11
            - name: POSPSLSTAX
              type: decimal
              width: 11
            - name: POSFSLSTAX
              type: decimal
              width: 11
            - name: POSSLSTAX
              type: decimal
              width: 11
            - name: POSPATPAY
              type: decimal
              width: 11
            - name: POSDUEAMT
              type: decimal
              width: 11
            - name: POSWITHHLD
              type: decimal
              width: 11
            - name: POSCOPAY
              type: decimal
              width: 11
            - name: POSPRODSEL
              type: decimal
              width: 11
            - name: POSATRTAX
              type: decimal
              width: 11
            - name: POSEXCEBFT
              type: decimal
              width: 11
            - name: POSINCENTV
              type: decimal
              width: 11
            - name: POSATRDED
              type: decimal
              width: 11
            - name: POSTOTHAMT
              type: decimal
              width: 11
            - name: POSPROFFEE
              type: decimal
              width: 11
            - name: POSOTHPAYA
              type: decimal
              width: 11
            - name: POSCOSTSRC
              type: char
              width: 1
            - name: POSPROCFEE
              type: decimal
              width: 11
            - name: POSPATSTAX
              type: decimal
              width: 11
            - name: POSPLNSTAX
              type: decimal
              width: 11
            - name: POSPRVNSEL
              type: decimal
              width: 11
            - name: POSPSCBRND
              type: decimal
              width: 11
            - name: POSPSCNONP
              type: decimal
              width: 11
            - name: POSPSCBRNP
              type: decimal
              width: 11
            - name: POSCOVGAP
              type: decimal
              width: 11
            - name: POSINGCSTC
              type: decimal
              width: 11
            - name: POSDSPFEEC
              type: decimal
              width: 11
            - name: CLIENTFLAG
              type: char
              width: 1
            - name: CLTINGRCST
              type: decimal
              width: 11
            - name: CLTDISPFEE
              type: decimal
              width: 11
            - name: CLTSLSTAX
              type: decimal
              width: 11
            - name: CLTPATPAY
              type: decimal
              width: 11
            - name: CLTDUEAMT
              type: decimal
              width: 11
            - name: CLTWITHHLD
              type: decimal
              width: 11
            - name: CLTPRCS
              type: char
              width: 10
            - name: CLTPRCST
              type: char
              width: 10
            - name: CLTPTPS
              type: char
              width: 10
            - name: CLTPTPST
              type: char
              width: 13
            - name: CLTCOPAYS
              type: char
              width: 10
            - name: CLTCOPAYSS
              type: decimal
              width: 2
            - name: CLTFCOPAY
              type: decimal
              width: 11
            - name: CLTPCOPAY
              type: decimal
              width: 11
            - name: CLTCOPAY
              type: decimal
              width: 11
            - name: CLTPRODSEL
              type: decimal
              width: 11
            - name: CLTPSTAX
              type: decimal
              width: 11
            - name: CLTFSTAX
              type: decimal
              width: 11
            - name: CLTATRTAX
              type: decimal
              width: 11
            - name: CLTEXCEBFT
              type: decimal
              width: 11
            - name: CLTINCENTV
              type: decimal
              width: 11
            - name: CLTATRDED
              type: decimal
              width: 11
            - name: CLTTOTHAMT
              type: decimal
              width: 11
            - name: CLTPROFFEE
              type: decimal
              width: 11
            - name: CLTCOB
              type: decimal
              width: 11
            - name: CLTOTHPAYA
              type: decimal
              width: 11
            - name: CLTCOSTSRC
              type: char
              width: 1
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTRATE
              type: decimal
              width: 8
            - name: CLTPRSCSTP
              type: decimal
              width: 3
            - name: CLTPRSCHNM
              type: char
              width: 18
            - name: CLTPROCFEE
              type: decimal
              width: 11
            - name: CLTPATSTAX
              type: decimal
              width: 11
            - name: CLTPLNSTAX
              type: decimal
              width: 11
            - name: CLTPRVNSEL
              type: decimal
              width: 11
            - name: CLTPSCBRND
              type: decimal
              width: 11
            - name: CLTPSCNONP
              type: decimal
              width: 11
            - name: CLTPSCBRNP
              type: decimal
              width: 11
            - name: CLTCOVGAP
              type: decimal
              width: 11
            - name: CLTINGCSTC
              type: decimal
              width: 11
            - name: CLTDSPFEEC
              type: decimal
              width: 11
            - name: RSPREIMBUR
              type: char
              width: 2
            - name: RSPINGRCST
              type: decimal
              width: 11
            - name: RSPDISPFEE
              type: decimal
              width: 11
            - name: RSPPSLSTAX
              type: decimal
              width: 11
            - name: RSPFSLSTAX
              type: decimal
              width: 11
            - name: RSPSLSTAX
              type: decimal
              width: 11
            - name: RSPPATPAY
              type: decimal
              width: 11
            - name: RSPDUEAMT
              type: decimal
              width: 11
            - name: RSPFCOPAY
              type: decimal
              width: 11
            - name: RSPPCOPAY
              type: decimal
              width: 11
            - name: RSPCOPAY
              type: decimal
              width: 11
            - name: RSPPRODSEL
              type: decimal
              width: 11
            - name: RSPATRTAX
              type: decimal
              width: 11
            - name: RSPEXCEBFT
              type: decimal
              width: 11
            - name: RSPINCENTV
              type: decimal
              width: 11
            - name: RSPATRDED
              type: decimal
              width: 11
            - name: RSPTOTHAMT
              type: decimal
              width: 11
            - name: RSPPROFEE
              type: decimal
              width: 11
            - name: RSPOTHPAYA
              type: decimal
              width: 11
            - name: RSPACCUDED
              type: decimal
              width: 11
            - name: RSPREMBFT
              type: decimal
              width: 11
            - name: RSPREMDED
              type: decimal
              width: 11
            - name: RSPPROCFEE
              type: decimal
              width: 11
            - name: RSPPATSTAX
              type: decimal
              width: 11
            - name: RSPPLNSTAX
              type: decimal
              width: 11
            - name: RSPPRVNSEL
              type: decimal
              width: 11
            - name: RSPPSCBRND
              type: decimal
              width: 11
            - name: RSPPSCNONP
              type: decimal
              width: 11
            - name: RSPPSCBRNP
              type: decimal
              width: 11
            - name: RSPCOVGAP
              type: decimal
              width: 11
            - name: RSPINGCSTC
              type: decimal
              width: 11
            - name: RSPDSPFEEC
              type: decimal
              width: 11
            - name: RSPPLANID
              type: char
              width: 8
            - name: BENSTGQL1
              type: char
              width: 2
            - name: BENSTGAMT1
              type: decimal
              width: 11
            - name: BENSTGQL2
              type: char
              width: 2
            - name: BENSTGAMT2
              type: decimal
              width: 11
            - name: BENSTGQL3
              type: char
              width: 2
            - name: BENSTGAMT3
              type: decimal
              width: 11
            - name: BENSTGQL4
              type: char
              width: 2
            - name: BENSTGAMT4
              type: decimal
              width: 11
            - name: ESTGENSAV
              type: decimal
              width: 11
            - name: SPDACCTREM
              type: decimal
              width: 11
            - name: HLTHPLNAMT
              type: decimal
              width: 11
            - name: INDDEDPTD
              type: decimal
              width: 11
            - name: INDDEDREM
              type: decimal
              width: 11
            - name: FAMDEDPTD
              type: decimal
              width: 11
            - name: FAMDEDREM
              type: decimal
              width: 11
            - name: DEDSCHED
              type: char
              width: 10
            - name: DEDACCC
              type: char
              width: 10
            - name: DEDFLAG
              type: char
              width: 1
            - name: INDLBFTUT_1
              type: decimal
              width: 11
            - name: INDLBFTPTD
              type: decimal
              width: 11
            - name: INDLBFTREM
              type: decimal
              width: 11
            - name: INDLBFTUT_2
              type: decimal
              width: 11
            - name: FAMLBFTPTD
              type: decimal
              width: 11
            - name: FAMLBFTREM
              type: decimal
              width: 11
            - name: LFTBFTMSCH
              type: char
              width: 10
            - name: LFTBFTACCC
              type: char
              width: 10
            - name: LFTBFTFLAG
              type: char
              width: 1
            - name: INDBFTUT
              type: decimal
              width: 11
            - name: INDBMAXPTD
              type: decimal
              width: 11
            - name: FAMBFTUT
              type: decimal
              width: 11
            - name: FAMBMAXPTD
              type: decimal
              width: 11
            - name: INDBMAXREM
              type: decimal
              width: 11
            - name: FAMBMAXREM
              type: decimal
              width: 11
            - name: BFTMAXSCHD
              type: char
              width: 10
            - name: BFTMAXACCC
              type: char
              width: 10
            - name: BFTMAXFLAG
              type: char
              width: 1
            - name: INDOOPPTD
              type: decimal
              width: 11
            - name: FAMOOPPTD
              type: decimal
              width: 11
            - name: INDOOPREM
              type: decimal
              width: 11
            - name: FAMOOPREM
              type: decimal
              width: 11
            - name: OOPSCHED
              type: char
              width: 10
            - name: OOPACCC
              type: char
              width: 10
            - name: OOPFLAG
              type: char
              width: 1
            - name: CONTRIBUT
              type: decimal
              width: 11
            - name: CONTBASIS
              type: char
              width: 2
            - name: CONTSCHED
              type: char
              width: 10
            - name: CONTACCCD
              type: char
              width: 10
            - name: CONTFLAG
              type: char
              width: 1
            - name: RXTFLAG
              type: char
              width: 1
            - name: REIMBURSMT
              type: char
              width: 1
            - name: CLMORIGIN
              type: char
              width: 1
            - name: HLDCLMFLAG
              type: char
              width: 1
            - name: HLDCLMDAYS
              type: decimal
              width: 3
            - name: PARTDFLAG
              type: char
              width: 1
            - name: COBEXTFLG
              type: char
              width: 1
            - name: PAEXTFLG
              type: char
              width: 1
            - name: HSAEXTIND
              type: char
              width: 1
            - name: FFPMEDRMST
              type: char
              width: 1
            - name: FFPMEDPXST
              type: char
              width: 1
            - name: FFPMEDMSST
              type: char
              width: 1
            - name: INCIDENTID
              type: char
              width: 25
            - name: ETCNBR
              type: char
              width: 30
            - name: DTEINJURY
              type: decimal
              width: 8
            - name: ADDUSER
              type: char
              width: 10
            - name: CHGUSER
              type: char
              width: 10
            - name: DMRUSERID
              type: char
              width: 10
            - name: PRAUSERID
              type: char
              width: 10
            - name: CLAIMREFID
              type: char
              width: 30
            - name: EOBDNOV
              type: char
              width: 3
            - name: EOBPDOV
              type: char
              width: 3
            - name: MANTRKNBR
              type: char
              width: 14
            - name: MANRECVDTE
              type: decimal
              width: 8
            - name: PASAUTHTYP
              type: char
              width: 2
            - name: PASAUTHID
              type: char
              width: 11
            - name: PASREQTYPE
              type: char
              width: 1
            - name: PASREQFROM
              type: decimal
              width: 8
            - name: PASREQTHRU
              type: decimal
              width: 8
            - name: PASBASISRQ
              type: char
              width: 2
            - name: PASREPFN
              type: char
              width: 12
            - name: PASREPLN
              type: char
              width: 15
            - name: PASSTREET
              type: char
              width: 30
            - name: PASCITY
              type: char
              width: 20
            - name: PASSTATE
              type: char
              width: 2
            - name: PASZIP
              type: char
              width: 15
            - name: PASPANBR
              type: char
              width: 11
            - name: PASAUTHNBR
              type: char
              width: 20
            - name: PASSDOCCT
              type: decimal
              width: 3
            - name: PAYERTYPE
              type: char
              width: 1
            - name: DELAYRSNCD
              type: char
              width: 2
            - name: MEDCDIND
              type: char
              width: 2
            - name: MEDCDID
              type: char
              width: 20
            - name: MEDCDAGNBR
              type: char
              width: 15
            - name: MEDCDTCN
              type: char
              width: 20
            - name: FMSTIER
              type: char
              width: 2
            - name: FMSSTATUS
              type: char
              width: 2
            - name: FMSDFLTIND
              type: char
              width: 1
            - name: FMSBENLST
              type: char
              width: 10
            - name: FMSLSTLVL3
              type: char
              width: 10
            - name: FMSLSTLVL2
              type: char
              width: 10
            - name: FMSLSTLVL1
              type: char
              width: 10
            - name: FMSRULESET
              type: char
              width: 10
            - name: FMSRULE
              type: char
              width: 10
            - name: FMSPROCCD
              type: char
              width: 2
            - name: CLIENTDEF1
              type: char
              width: 10
            - name: CLIENTDEF2
              type: char
              width: 10
            - name: CLIENTDEF3
              type: char
              width: 10
            - name: CLIENTDEF4
              type: char
              width: 10
            - name: CLIENTDEF5
              type: char
              width: 10
            - name: CCTRESERV1
              type: char
              width: 10
            - name: CCTRESERV2
              type: char
              width: 10
            - name: CCTRESERV3
              type: char
              width: 10
            - name: CCTRESERV4
              type: char
              width: 10
            - name: CCTRESERV5
              type: char
              width: 10
            - name: CCTRESERV6
              type: char
              width: 10
            - name: CCTRESERV7
              type: char
              width: 10
            - name: CCTRESERV8
              type: char
              width: 10
            - name: CCTRESERV9
              type: char
              width: 20
            - name: CCTRESRV10
              type: char
              width: 20
            - name: OOPAPPLIED
              type: decimal
              width: 11
            - name: CCTRESRV12
              type: decimal
              width: 11
            - name: CCTRESRV13
              type: decimal
              width: 11
            - name: CCTRESRV14
              type: decimal
              width: 11
            - name: USERFIELD
              type: char
              width: 10
            - name: EXTRACTDTE
              type: decimal
              width: 8
            - name: BATCHCTRL
              type: char
              width: 21
            - name: CLTTYPUCST
              type: decimal
              width: 14
            - name: REJCDE4
              type: char
              width: 3
            - name: REJCDE5
              type: char
              width: 3
            - name: REJCDE6
              type: char
              width: 3
            - name: MESSAGECD4
              type: char
              width: 10
            - name: MESSAGE4
              type: char
              width: 40
            - name: MESSAGECD5
              type: char
              width: 10
            - name: MESSAGE5
              type: char
              width: 40
            - name: MESSAGECD6
              type: char
              width: 10
            - name: MESSAGEG6
              type: char
              width: 40
            - name: CLIENT2FLAG
              type: char
              width: 1
            - name: CLT2INGRCST
              type: decimal
              width: 11
            - name: CLT2DISPFEE
              type: decimal
              width: 11
            - name: CLT2SLSTAX
              type: decimal
              width: 11
            - name: CLT2DUEAMT
              type: decimal
              width: 11
            - name: CLTPRSCSTP2
              type: decimal
              width: 3
            - name: CLTPRSCHNM2
              type: decimal
              width: 18
            - name: CLT2PRCS
              type: char
              width: 10
            - name: CLT2PRCST
              type: char
              width: 10
            - name: CLT2PSTAX
              type: decimal
              width: 11
            - name: CLT2FSTAX
              type: decimal
              width: 11
            - name: CLT2OTHAMT
              type: decimal
              width: 11
            - name: CLT2COSTSRC
              type: char
              width: 1
            - name: CLT2COSTTYP
              type: char
              width: 10
            - name: CLT2PRCTYPE
              type: char
              width: 10
            - name: CLT2RATE
              type: decimal
              width: 8
            - name: CLT2DCCSCID
              type: char
              width: 14
            - name: CLT2DCCSCSQ
              type: decimal
              width: 3
            - name: CLT2DCSCHID
              type: char
              width: 13
            - name: CLT2DCSCHSQ
              type: decimal
              width: 3
            - name: CALINCENTV2
              type: decimal
              width: 11
            - name: CLNT3FLAG
              type: char
              width: 1
            - name: CLT3INGRCST
              type: decimal
              width: 11
            - name: CLT3DISPFEE
              type: decimal
              width: 11
            - name: CLT3SLSTAX
              type: decimal
              width: 11
            - name: CLT3DUEAMT
              type: decimal
              width: 11
            - name: CLTPRSCSTP3
              type: decimal
              width: 3
            - name: CLTPRSCHNM3
              type: decimal
              width: 18
            - name: CLT3PRCS
              type: char
              width: 10
            - name: CLT3PRCST
              type: char
              width: 10
            - name: CLT3PSTAX
              type: decimal
              width: 11
            - name: CLT3FSTAX
              type: decimal
              width: 11
            - name: CLT3OTHAMT
              type: decimal
              width: 11
            - name: CLT3COSTSRC
              type: char
              width: 1
            - name: CLT3COSTTYP
              type: char
              width: 10
            - name: CLT3PRCTYPE
              type: char
              width: 10
            - name: CLT3RATE
              type: decimal
              width: 8
            - name: CLT3DCCSCID
              type: char
              width: 14
            - name: CLT3DCCSCSQ
              type: decimal
              width: 3
            - name: CLT3DCSCHID
              type: char
              width: 13
            - name: CLT3DCSCHSQ
              type: decimal
              width: 3
            - name: CALINCENTV3
              type: decimal
              width: 11
            - name: TBR_TBR_REJECT_REASON1
              type: char
              width: 5
            - name: TBR_TBR_REJECT_REASON2
              type: char
              width: 5
            - name: TBR_TBR_REJECT_REASON3
              type: char
              width: 5
            - name: SBM_QTY_INT_DIS
              type: char
              width: 13
            - name: SBM_DS_INT_DIS
              type: char
              width: 3
            - name: OTHR_AMT_CLMSBM_CT
              type: char
              width: 3
            - name: OTHR_AMT_CLMSBM_QLFR1
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM1
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR2
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM2
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR3
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM3
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR4
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM4
              type: char
              width: 11
            - name: OTHR_AMT_CLMSBM_QLFR5
              type: char
              width: 2
            - name: OTHR_AMT_CLMSBM5
              type: char
              width: 11
            - name: RSN_SRVCDE1
              type: char
              width: 2
            - name: RSN_SRVCDE2
              type: char
              width: 2
            - name: RSN_SRVCDE3
              type: char
              width: 2
            - name: PROF_SRVCDE1
              type: char
              width: 2
            - name: PROF_SRVCDE2
              type: char
              width: 2
            - name: PROF_SRVCDE3
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE1
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE2
              type: char
              width: 2
            - name: RESULT_OF_SRV_CDE3
              type: char
              width: 2
            - name: SCH_RX_ID_NBR
              type: char
              width: 12
            - name: BASIS_CAL_DISPFEE
              type: char
              width: 2
            - name: BASIS_CAL_COPAY
              type: char
              width: 2
            - name: BASIS_CAL_FLT_TAX
              type: char
              width: 2
            - name: BASIS_CAL_PRCNT_TAX
              type: char
              width: 2
            - name: BASIS_CAL_COINSUR
              type: char
              width: 2
            - name: PAID_MSG_CODE_COUNT
              type: char
              width: 1
            - name: PAID_MESSAGE_CODE_1
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_2
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_3
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_4
              type: char
              width: 3
            - name: PAID_MESSAGE_CODE_5
              type: char
              width: 3
            - name: FILLER_FOR_REJECT_FIELD_IND
              type: char
              width: 26
        - name: DataCleansing.V168S9P8
          type: map
          source: OptumDetail_Seq
          alias: Extract
          schema: 
            - name: ID
              type: varchar
              width: 20
              map: "1"
            - name: CLTINGRCST
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTINGRCST) = @TRUE or Len(Trim(Extract.CLTINGRCST)) = 0) then '0.0' Else Extract.CLTINGRCST"
            - name: CLTPATPAY
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTPATPAY) = @TRUE or Len(Trim(Extract.CLTPATPAY)) = 0) then '0.0' Else Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
              map: " If (IsNull(Extract.CLTDUEAMT) = @TRUE or Len(Trim(Extract.CLTDUEAMT)) = 0) then '0.0' Else Extract.CLTDUEAMT"
        - name: AmountLevel_Aggr
          type: unknown
          source: ControlFile_Xfr.V0S448P5
          alias: OptumContolFileGroup
          origType: AGGREGATOR
        - name: DetailGroupLevel_Aggr
          type: unknown
          source: DataCleansing.V168S9P4
          alias: DetailGroupLevelFields_Lnk
          origType: AGGREGATOR
        - name: DetailAmountLevel_Aggr
          type: unknown
          source: DataCleansing.V168S9P8
          alias: FullAgg_Amts
          origType: AGGREGATOR


