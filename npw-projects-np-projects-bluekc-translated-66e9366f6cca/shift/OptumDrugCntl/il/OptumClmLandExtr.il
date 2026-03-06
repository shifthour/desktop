model:
  config:
    origin: datastage
  jobs:
    - name: OptumClmLandExtr
      origin: datastage
      type: dataflow
      folder: "claim/claim/Optum"
      runAfterSubrOnJobSuccess: enabled
      description: "Reads OPTUMRX source file and creates BCBSKC Common Landing File"
      fullDescription: "© Copyright 2019 Blue Cross/Blue Shield of Kansas City\n\n\nCALLED BY : OptumDrugLandSeq\n\n\nDESCRIPTION:      OptumRx Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim\n                               \n\nMODIFICATIONS:\n\nDeveloper                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       \n------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      \nRekha Radhakrishna      09/20/2019      6131- PBM Replacement            Initial Programming                                                                                             IntegrateDev1                Kalyan Neelam           10/16/2019\nSri Nannapaneni             10/25/2019      6131- PBM Replacement            Modifed to include spread price extract                                                              IntegrateDev1                \nSri Nannapaneni             11/21/2019      6131- PBM Replacement            Pharmacy ID qualifier taken from RxCLAIM.                                                       IntegrateDev1                \n\n\n                                                                                                                   Transformer  to the MapToAllDownstreamCommonFiles transformer.\nSharon Andrew               2020-01-07       6131- PBM Replacement             Changed aggregation for Drug_Header_Record.dat \n                                                                                                                     Added aggregation to sniff out COB claims written to /verified/Optum_SellPriceValidation_#RunID#.dat                                   2020-01-09\n\nPeter Gichiri                    2020-01-14       6131- PBM Replacement            Moved the logic Fix(Extract.AWPUNITCST,2) from the DataCleansing              IntegrateDev2                Kalyan Neelam            2020-01-14\n\nRekha Radhakrishna     2020-01-28       6131- PBM Replacement             Added BuyPrice Audit Validation written to /verified/Optum_BuyPriceValidation_#RunID#.dat               Kalyan Neelam            2020-01-29\n                                                                                                                    Mapped 3 fields PLANQUAL,SRXNETWK,RXNUMBER to CLNT_GNRL_PRPS_AREA field  O/P     \nRekha Radhakrishna     2020-01-30       6131- PBM Replacement             Mapped fields CLTOTHPAYA, CLTINCENTV, CLTTOTHAMT, CALINCENTV2 for SpreadPricing O/P  Kalyan Neelam            2020-02-06\n                                                                                                                  \nRekha Radhakrishna     2020-02-06       6131- PBM Replacement             Changed logic for FULL_AVG_WHLSL_PRICE                                                                                       Kalyan Neelam            2020-02-06\n\nRekha Radhakrishna     2020-02-10       6131- PBM Replacement             Mapped fields  CLT2PSTAX and CLT2FSTAX to DRUG_CLM_PRICE O/P     IntegrateDev2                Kalyan Neelam           2020-02-10\n\nRekha Radhakrishna     2020-02-25       6131- PBM Replacement             Changed COPAY_AMT mapping to CLTFCOPAY                                              IntegrateDev2                Hugh Sisson               2020-03-06\n\nRekha Radhakrishna     2020-03-10       6131- PBM Replacement             Changed OTH_PAYOR_AMT mapping to CLTOTHPAYA                                 IntegrateDev2                Hugh Sisson               2020-03-10\n\nVelmani                          2020-03-13       6131- PBM Replacement             Mapped new column for No Bill No Pay Indicator                                               IntegrateDev2                                                                                                                        \n\nRekha Radhakrishna     2020-03-20       6131- PBM Replacement             Changed mapping for PAR_PDX_IN                                                                  IntegrateDev2   \n\nVelmani                          2020-03-31       6131- PBM Replacement             Mapped new column for Speciality  Indicator                                                      IntegrateDev2                      \n\nRekha Radhakrishna    2020-08-30        PBM PhaseII government programs    Added LOB_IN field and default 'COM'                                                       IntegrateDev2               Sravya Gorla                2020-12-09\n                                                                                                                           and mapped GPINUMBER to GNRC_PROD_IN field   \n\n\nRekha Radhakrishna    2020-02-05        PBM PhaseII                        Added LOB indicator to Program Info written to Audit File and DATESBM                IntegrateDev2\x09Abhiram Dasarathy\x092021-02-09\n\nPeter Gichiri                   2021-04-08        PBM PhaseII - US 362873     Mapped FMSTIER to MCARE_D_COV_DRUG  based on                                     IntegrateDev2            Jaideep Mankala          04/12/2021\n                                                                                                              \x09\x09  the GROUPPLAN logic\nGeetanjali Rajendran    2021-06-03        PBM PhaseII                        Added TIER_ID, DRUG_TYPE_CD and CLIENTDEF2 to the                                              IntegrateDev2\x09Abhiram Dasarathy\x092021-06-23\n                                                                                                                             DrugClmPrice Landing file  and modified derivation for DRUG_TYP in\n                                                                                                                             PaidReversal Landing file\nBill Schroeder               2023-07-12        US-586570                        Added extract file DRUG_CLM_ACCUM_IMPCT_prep and RunCycle parm in Job Properties.  IntegrateDev2             Jeyaprasanna    2023-08-13\n\nArpitha V                      2023-11-07        US 600305                        Added  PLN_DRUG_STTUS_CD  to the DrugClmPrice Landing file                              IntegrateDevB                     Jeyaprasanna     2024-01-01\n\nAshok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                    IntegrateDev2                  Jeyaprasanna      2024-03-14"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: RunID
          type: string
          description: RunID
        - name: SrcSysCd
          type: string
          description: SrcSysCd
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
        - name: MbrTermDate
          type: string
          description: MbrTermDate
          longDescription: "Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years."
        - name: RunCycle
          type: integer
          description: RunCycle
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\1;hf_optum_clm_mbr_land"
        subr: "1;hf_optum_clm_mbr_land"
      steps:
        - name: Optum
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
        - name: DRUG_CLM_ACCUM_IMPCT_prep
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM_ACCUM_IMPCT_prep.dat"
          source: "MapToAllDownstreamCommonFiles.V168S12P9"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: Drug_Header_Record
          type: fileWriter
          urls: 
            - "#$FilePath#/landing/Drug_Header_Record.dat"
          source: PrepAuditFile
          option: 
            updateMode: append
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: DeniedClm_Land
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/#SrcSysCd#_DeniedClaims_Landing.dat.#RunID#"
          source: "MapToAllDownstreamCommonFiles.V168S12P3"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "^"
            fieldDelimiter: ","
        - name: W_DRUG_CLM_PCP
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_DRUG_CLM_PCP.dat"
          source: "MapToAllDownstreamCommonFiles.V168S12P5"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: W_DRUG_ENR
          type: fileWriter
          urls: 
            - "#$FilePath#/load/W_DRUG_ENR.dat"
          source: "MapToAllDownstreamCommonFiles.V168S12P4"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: PDX_CLM_STD_INPT_Land
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#"
          source: "MapToAllDownstreamCommonFiles.V168S12P2"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: DrugClmPrice_Land
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/DrugClmPrice_Land_#SrcSysCd#.dat.#RunID#"
          source: "MapToAllDownstreamCommonFiles.V168S12P6"
          option: 
            updateMode: overwrite
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
        - name: BuyPriceValidation
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/Optum_BuyPriceValidation_#RunID#.dat"
          source: "MapToAllDownstreamCommonFiles.V168S12P7"
          option: 
            updateMode: overwrite
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
        - name: SellPriceValidation
          type: fileWriter
          urls: 
            - "#$FilePath#/verified/Optum_SellPriceValidation_#RunID#.dat"
          source: "MapToAllDownstreamCommonFiles.V168S12P8"
          option: 
            updateMode: overwrite
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_optum_clm_mbr_land
          type: map
          source: IDS_MBR
          alias: MBR
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: MEMBER_ID
              type: varchar
              width: 22
              nullable: N
              map: "MBR.MEMBER_ID"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "MBR.GRP_ID"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "MBR.MBR_UNIQ_KEY"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "MBR.SUB_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "MBR.MBR_SFX_NO"
        - name: IDS_MBR
          type: dbReader
          originalDb: db2
          query: "SELECT \nSUB.SUB_ID||MBR.MBR_SFX_NO as MEMBERID ,\nGRP_ID,\nMBR_UNIQ_KEY,\nSUB_ID,\nMBR_SFX_NO\n\nFROM \n#$IDSOwner#.MBR MBR,\n#$IDSOwner#.SUB SUB,\n#$IDSOwner#.GRP GRP \nWHERE MBR.SUB_SK = SUB.SUB_SK \nAnd         SUB.GRP_SK = GRP.GRP_SK\nand         MBR.TERM_DT_SK >= '#MbrTermDate#'\n"
          schema: 
            - name: MEMBERID
              type: varchar
              width: 22
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
          keys:
            - MEMBERID
            - GRP_ID
        - name: Member_Lkp
          type: lookup
          option:
            joinType: left
          sources: 
            - name: DataCleansing.V168S9P2
              alias: inMbrMatch
              joinFilter: ""
              joinExpression: []
            - name: hf_optum_clm_mbr_land
              alias: MBR_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "MBR_lkup.MEMBER_ID"
                toExpr: "inMbrMatch.MEMBERID"
                caseInSensitive: true
              - fromExpr: "MBR_lkup.GRP_ID"
                toExpr: "inMbrMatch.ACCOUNTID"
                caseInSensitive: true
          joinCondition: 
            - expression: "MBR_lkup.MEMBER_ID = inMbrMatch.MEMBERID"
              caseInSensitive: true
            - expression: "MBR_lkup.GRP_ID = inMbrMatch.ACCOUNTID"
              caseInSensitive: true
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "inMbrMatch.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "inMbrMatch.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "inMbrMatch.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "inMbrMatch.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "inMbrMatch.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "inMbrMatch.ACCOUNTID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Trim(inMbrMatch.MEMBERID)[1, 20]"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "inMbrMatch.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "inMbrMatch.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "inMbrMatch.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "inMbrMatch.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "inMbrMatch.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "inMbrMatch.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "inMbrMatch.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "inMbrMatch.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "inMbrMatch.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "inMbrMatch.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "inMbrMatch.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "inMbrMatch.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "inMbrMatch.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "inMbrMatch.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "inMbrMatch.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "inMbrMatch.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "inMbrMatch.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "inMbrMatch.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "inMbrMatch.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "inMbrMatch.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "inMbrMatch.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "inMbrMatch.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "inMbrMatch.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "inMbrMatch.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "inMbrMatch.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "inMbrMatch.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "inMbrMatch.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "inMbrMatch.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "inMbrMatch.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "inMbrMatch.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "inMbrMatch.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "inMbrMatch.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "inMbrMatch.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "inMbrMatch.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "inMbrMatch.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "inMbrMatch.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "inMbrMatch.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "inMbrMatch.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "inMbrMatch.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "inMbrMatch.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "inMbrMatch.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "inMbrMatch.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "inMbrMatch.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "inMbrMatch.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "inMbrMatch.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "inMbrMatch.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "inMbrMatch.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "inMbrMatch.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "inMbrMatch.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "inMbrMatch.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "inMbrMatch.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "inMbrMatch.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "inMbrMatch.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "inMbrMatch.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "inMbrMatch.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "inMbrMatch.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: "inMbrMatch.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "inMbrMatch.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "inMbrMatch.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "inMbrMatch.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "inMbrMatch.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "inMbrMatch.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "inMbrMatch.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "inMbrMatch.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "inMbrMatch.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "inMbrMatch.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "inMbrMatch.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "inMbrMatch.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "inMbrMatch.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "inMbrMatch.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "inMbrMatch.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "inMbrMatch.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "inMbrMatch.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "inMbrMatch.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "inMbrMatch.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "inMbrMatch.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "inMbrMatch.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "inMbrMatch.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "inMbrMatch.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "inMbrMatch.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "inMbrMatch.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "inMbrMatch.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "inMbrMatch.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: "inMbrMatch.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: "inMbrMatch.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              precision: 2
              map: "inMbrMatch.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              precision: 2
              nullable: N
              map: "inMbrMatch.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "inMbrMatch.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "inMbrMatch.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "inMbrMatch.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "inMbrMatch.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "inMbrMatch.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "inMbrMatch.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: " If inMbrMatch.CLAIMSTS = 'X' Then inMbrMatch.RXCLMNBR : inMbrMatch.CLMSEQNBR : 'R' Else inMbrMatch.RXCLMNBR : inMbrMatch.CLMSEQNBR"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: " If IsNull(MBR_lkup.MBR_UNIQ_KEY) Then 0 Else MBR_lkup.MBR_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(inMbrMatch.ACCOUNTID) Then 'UNK' Else MBR_lkup.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "inMbrMatch.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "inMbrMatch.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "inMbrMatch.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: "inMbrMatch.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "inMbrMatch.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "inMbrMatch.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "inMbrMatch.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "inMbrMatch.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "inMbrMatch.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "inMbrMatch.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "inMbrMatch.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "inMbrMatch.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "inMbrMatch.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "inMbrMatch.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "inMbrMatch.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "inMbrMatch.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "inMbrMatch.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "inMbrMatch.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "inMbrMatch.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "inMbrMatch.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "inMbrMatch.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "inMbrMatch.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "inMbrMatch.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "inMbrMatch.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "inMbrMatch.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "inMbrMatch.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "inMbrMatch.PLANTYPE"
        - name: MapToAllDownstreamCommonFiles.V168S12P2.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: varchar
              width: 20
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "\"OPTUMRX\""
            - name: FILE_RCVD_DT
              type: char
              width: 10
              map: "Extract.DATESBM"
            - name: RCRD_ID
              type: decimal
              width: 2
              map: "@Null"
            - name: PRCSR_NO
              type: varchar
              width: 10
              map: "@Null"
            - name: BTCH_NO
              type: decimal
              width: 5
              map: "@Null"
            - name: PDX_NO
              type: varchar
              width: 20
              map: svServiceIDToUse
            - name: RX_NO
              type: decimal
              width: 9
              map: "Extract.RXNUMBER"
            - name: FILL_DT
              type: char
              width: 10
              map: "Extract.DTEFILLED"
            - name: NDC
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: DRUG_DESC
              type: varchar
              width: 30
              map: "Extract.LABELNAME"
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
              map: "Extract.REFILL"
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
              map: "Extract.DECIMALQTY"
            - name: DAYS_SUPL
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: BSS_OF_CST_DTRM
              type: char
              width: 2
              map: "@Null"
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTINGRCST"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTDISPFEE"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTFCOPAY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTSLSTAX"
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTDUEAMT"
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
              map: "Extract.MBRFSTNME"
            - name: PATN_LAST_NM
              type: varchar
              width: 75
              map: "Extract.MBRLSTNME"
            - name: BRTH_DT
              type: char
              width: 10
              map: "Extract.MBRBIRTH"
            - name: SEX_CD
              type: decimal
              width: 1
              map: " If upcase(Extract.MBRSEX) = 'M' then 1 else  if upcase(Extract.MBRSEX) = 'F' then 2 else 0"
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
              map: " If (IsNull(Extract.MEMBERID) = @TRUE or Len(Extract.MEMBERID)) = 0 then 'UNK' else  If Len(TRIM(Extract.MEMBERID)) = 11 THEN TRIM(Extract.MEMBERID[1, 9]) else \"UNK\""
            - name: RELSHP_CD
              type: decimal
              width: 1
              map: "Extract.MBRRELCDE"
            - name: GRP_NO
              type: varchar
              width: 20
              map: "Extract.ACCOUNTID"
            - name: HOME_PLN
              type: varchar
              width: 3
              map: "@Null"
            - name: HOST_PLN
              type: varchar
              width: 3
              map: "@Null"
            - name: PRSCRBR_ID
              type: varchar
              width: 20
              map: svPrescriberDEAtoUse
            - name: DIAG_CD
              type: varchar
              width: 20
              map: "@Null"
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
              map: "@Null"
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
              map: "@Null"
            - name: PRAUTH_NO
              type: char
              width: 12
              map: "Extract.PRAUTHNBR"
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
              map: "@Null"
            - name: CUST_LOC
              type: decimal
              width: 2
              map: "Extract.CUSTLOC"
            - name: RESUB_CYC_CT
              type: decimal
              width: 2
              map: "@Null"
            - name: RX_DT
              type: char
              width: 10
              map: "@Null"
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: char
              width: 1
              map: " If (IsNull(Extract.PSC) = @TRUE or Len(Trim(Extract.PSC)) = 0) then 'UNK' else Extract.PSC"
            - name: PRSN_CD
              type: varchar
              width: 3
              map: " If (IsNull(Extract.MEMBERID) = @TRUE or Len(Extract.MEMBERID)) = 0 then 'UNK' else  If Len(TRIM(Extract.MEMBERID)) = 11 THEN TRIM(Extract.MEMBERID[10, 2]) else \"UNK\""
            - name: OTHR_COV_CD
              type: decimal
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: ELIG_CLRFCTN_CD
              type: decimal
              width: 1
              map: "@Null"
            - name: CMPND_CD
              type: decimal
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: NO_OF_RFLS_AUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: LVL_OF_SVC
              type: decimal
              width: 2
              map: "@Null"
            - name: RX_ORIG_CD
              type: decimal
              width: 1
              map: "Extract.ORIGINCDE"
            - name: RX_DENIAL_CLRFCTN
              type: decimal
              width: 2
              map: "@Null"
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
              map: "@Null"
            - name: CLNC_ID_NO
              type: decimal
              width: 5
              map: "@Null"
            - name: DRUG_TYP
              type: decimal
              width: 1
              map: " If ((IsNull(Extract.GENINDOVER) = @TRUE or Len(Trim(Extract.GENINDOVER)) = 0) And (Trim(Extract.MULTSRCCDE) = 'M' or Trim(Extract.MULTSRCCDE) = 'N')) Then '1' Else  If ((IsNull(Extract.GENINDOVER) = @TRUE or Len(Trim(Extract.GENINDOVER)) = 0) And Trim(Extract.MULTSRCCDE) = 'Y') Then '3' Else  If ((IsNull(Extract.GENINDOVER) = @TRUE or Len(Trim(Extract.GENINDOVER)) = 0) And Trim(Extract.MULTSRCCDE) = 'O') Then '5' Else  If (Trim(Extract.GENINDOVER) = 'M' or Trim(Extract.GENINDOVER) = 'N') Then '1' Else  If Trim(Extract.GENINDOVER) = 'Y' Then '3' Else  If Trim(Extract.GENINDOVER) = 'O' Then '5' Else '0'"
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
              map: " If Len(Extract.PRESLSTNME) = 0 and Len(Extract.PRESFSTNME) = 0 then 'UNK' Else  If Len(Extract.PRESFSTNME) <> 0 and Len(Extract.PRESLSTNME) = 0 Then Extract.PRESFSTNME Else  If Len(Extract.PRESFSTNME) = 0 and Len(Extract.PRESLSTNME) <> 0 Then Extract.PRESLSTNME Else Extract.PRESFSTNME : ' ' : Extract.PRESLSTNME"
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: UNIT_DOSE_IN
              type: decimal
              width: 1
              map: "@Null"
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTOTHPAYA"
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: decimal
              width: 1
              map: "@Null"
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
              map: " If Extract.COMPOUNDCD = 2 Then Fix((Extract.USUALNCUST + Extract.CLTDISPFEE + Extract.CLTSLSTAX), 2) Else Fix(((Extract.AWPUNITCST * Extract.DECIMALQTY) + Extract.CLTDISPFEE + Extract.CLTSLSTAX), 2)"
            - name: EXPNSN_AREA
              type: char
              width: 1
              map: "@Null"
            - name: MSTR_CAR
              type: varchar
              width: 20
              map: "Extract.CARID"
            - name: SUBCAR
              type: varchar
              width: 20
              map: "@Null"
            - name: CLM_TYP
              type: char
              width: 1
              map: " If Extract.CLAIMSTS = 'X' Then 'R' Else 'P'"
            - name: SUBGRP
              type: varchar
              width: 20
              map: "@Null"
            - name: PLN_DSGNR
              type: char
              width: 1
              map: "@Null"
            - name: ADJDCT_DT
              type: char
              width: 10
              map: "Extract.DATESBM"
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0.0"
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0.00"
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.SBMINGRCST"
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MBR_PAY_CD
              type: char
              width: 2
              map: "@Null"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTINCENTV"
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: CLM_ADJ_CD
              type: char
              width: 2
              map: "@Null"
            - name: FRMLRY_FLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
              map: "@Null"
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
              map: "@Null"
            - name: PDX_TYP
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: BILL_BSS_CD
              type: char
              width: 2
              map: " If IsNull(Extract.BASISCOST) = @TRUE Or Len(Trim(Extract.BASISCOST)) = 0 Then 'NA' Else Extract.BASISCOST"
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.USUALNCUST"
            - name: PD_DT
              type: char
              width: 10
              map: "\"1753-01-01\""
            - name: BNF_CD
              type: varchar
              width: 10
              map: "@Null"
            - name: DRUG_STRG
              type: varchar
              width: 20
              map: "@Null"
            - name: INJRY_DT
              type: char
              width: 10
              map: "@Null"
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "0.00"
            - name: REF_NO
              type: varchar
              width: 20
              map: "Extract.RXCLMNBR"
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
              map: "@Null"
            - name: PLN_TYP
              type: varchar
              width: 20
              map: "@Null"
            - name: ADJDCT_REF_NO
              type: decimal
              width: 9
              map: "@Null"
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTPSCBRND) = @TRUE or Len(Trim(Extract.CLTPSCBRND)) = 0) then '0.0' Else Extract.CLTPSCBRND"
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 40
              map: "FMT(Extract.FACILITYID, '10\" \"R') : FMT(Extract.PRESCRIDQL, '2\" \"R') : FMT(Extract.PRESCDEAID, '15\" \"R') : FMT(Extract.SBMDISPFEE, '11\" \"R') : FMT(Extract.SRXNETWRK, '6\" \"R') : FMT(Extract.RXNETWORK, '6\" \"R') : FMT(Extract.PLANQUAL, '10\" \"R') : FMT(Extract.COBIND, '2\" \"R') : FMT(Extract.RXNETPRCQL, '1\" \"R') : FMT(Extract.CLIENTDEF1, '1\" \"R')"
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
              map: " If (IsNull(Extract.DISPSTATUS) = @TRUE or Len(Trim(Extract.DISPSTATUS)) = 0) Then 'BLANK' Else Extract.DISPSTATUS"
            - name: BILL_DT
              type: char
              width: 10
              map: "\"1753-01-01\""
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
              map: "@Null"
            - name: PICA_DRUG_CD
              type: char
              width: 1
              map: "@Null"
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTPATPAY) = @TRUE or Len(Trim(Extract.CLTPATPAY)) = 0) then '0.0' Else Extract.CLTPATPAY"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: FED_DRUG_CLS_CD
              type: char
              width: 1
              map: "@Null"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTATRDED"
            - name: BNF_COPAY_100
              type: char
              width: 1
              map: "@Null"
            - name: CLM_PRCS_TYP
              type: char
              width: 1
              map: "@Null"
            - name: INDEM_HIER_TIER_NO
              type: decimal
              width: 4
              map: "@Null"
            - name: MCARE_D_COV_DRUG
              type: char
              width: 1
              map: " If ((Extract.GROUPPLAN = 'BKC1001') Or (Extract.GROUPPLAN = 'BKC1003') Or (Extract.GROUPPLAN = 'BKC1007') Or (Extract.GROUPPLAN = 'BKC6001') Or (Extract.GROUPPLAN = 'BKC6002')) Then Extract.FMSTIER Else @Null"
            - name: RETRO_LICS_CD
              type: char
              width: 1
              map: "@Null"
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MCARE_B_DRUG
              type: char
              width: 1
              map: "@Null"
            - name: MCARE_B_CLM
              type: char
              width: 1
              map: "@Null"
            - name: PRSCRBR_QLFR
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
              map: svPrescriberNPItoUse
            - name: PDX_QLFR
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: svServiceNPIToUse
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 11
              map: "@Null"
            - name: THER_CLS
              type: decimal
              width: 6
              map: "@Null"
            - name: HIC_NO
              type: varchar
              width: 20
              map: "@Null"
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: char
              width: 1
              map: "@Null"
            - name: DOSE_CD
              type: decimal
              width: 4
              map: "@Null"
            - name: LOW_INCM
              type: char
              width: 1
              map: "@Null"
            - name: RTE_OF_ADMIN
              type: char
              width: 2
              map: "@Null"
            - name: DEA_SCHD
              type: decimal
              width: 1
              map: "@Null"
            - name: COPAY_BNF_OPT
              type: decimal
              width: 10
              map: "@Null"
            - name: GNRC_PROD_IN
              type: decimal
              width: 14
              map: " If Isnull(Extract.GPINUMBER) = @TRUE OR len(Trim(Extract.GPINUMBER)) = 0 then 'NA' else Extract.GPINUMBER"
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
              map: "Extract.PRESSPCCDE"
            - name: VAL_CD
              type: varchar
              width: 20
              map: "@Null"
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
              map: "@Null"
            - name: OFC_OF_INSPECTOR_GNRL
              type: char
              width: 1
              map: "@Null"
            - name: PATN_SSN
              type: varchar
              width: 11
              map: "@Null"
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
              map: "@Null"
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
              map: "@Null"
            - name: CARDHLDR_ADDR
              type: varchar
              width: 40
              map: "@Null"
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
              map: "@Null"
            - name: CHADHLDR_ST
              type: varchar
              width: 2
              map: "@Null"
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
              map: "@Null"
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: DEDCT_FMLY_MET_AMT
              type: varchar
              width: 20
              map: "@Null"
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "@Null"
            - name: PAR_PDX_IN
              type: char
              width: 1
              map: " If ((Extract.REIMBURSMT = 'M' And Extract.SBMPATRESD = '92') Or (Extract.REIMBURSMT = 'M' And Extract.SBMPATRESD = '93') Or (Extract.REIMBURSMT = 'M' And Extract.SBMPATRESD = '98')) Then 'N' Else 'Y'"
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTPCOPAY"
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTFCOPAY"
            - name: CLM_TRNSMSN_METH
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: RX_NO_2012
              type: decimal
              width: 12
              map: "@Null"
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
              map: "Extract.CLAIMSTS"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              map: "'NA'"
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
              map: " If (IsNull(Extract.CTSCHEDID) = @TRUE Or Len(Trim(Extract.CTSCHEDID)) = 0) Then 'NA' Else Extract.CLTPRODSEL"
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTPRODSEL) = @TRUE Or Len(Trim(Extract.CLTPRODSEL)) = 0) Then 0.00 Else Extract.CLTPRODSEL"
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTATRTAX) = @TRUE Or Len(Trim(Extract.CLTATRTAX)) = 0) Then 0.00 Else Extract.CLTATRTAX"
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTPROCFEE) = @TRUE Or Len(Trim(Extract.CLTPROCFEE)) = 0) Then 0.00 Else Extract.CLTPROCFEE"
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CLTPRVNSEL) = @TRUE Or Len(Trim(Extract.CLTPRVNSEL)) = 0) Then 0.00 Else Extract.CLTPRVNSEL"
            - name: LOB_IN
              type: varchar
              width: 5
              map: "'COM'"
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P2
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P2.map
          filter: "CLAIMSTS = 'P' Or CLAIMSTS = 'X'"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: FILE_RCVD_DT
              type: char
              width: 10
            - name: RCRD_ID
              type: decimal
              width: 2
            - name: PRCSR_NO
              type: varchar
              width: 10
            - name: BTCH_NO
              type: decimal
              width: 5
            - name: PDX_NO
              type: varchar
              width: 20
            - name: RX_NO
              type: decimal
              width: 9
            - name: FILL_DT
              type: char
              width: 10
            - name: NDC
              type: char
              width: 20
            - name: DRUG_DESC
              type: varchar
              width: 30
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
            - name: DAYS_SUPL
              type: decimal
              width: 3
            - name: BSS_OF_CST_DTRM
              type: char
              width: 2
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
            - name: PATN_LAST_NM
              type: varchar
              width: 75
            - name: BRTH_DT
              type: char
              width: 10
            - name: SEX_CD
              type: decimal
              width: 1
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: decimal
              width: 1
            - name: GRP_NO
              type: varchar
              width: 20
            - name: HOME_PLN
              type: varchar
              width: 3
            - name: HOST_PLN
              type: varchar
              width: 3
            - name: PRSCRBR_ID
              type: varchar
              width: 20
            - name: DIAG_CD
              type: varchar
              width: 20
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
            - name: PRAUTH_NO
              type: char
              width: 12
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
            - name: CUST_LOC
              type: decimal
              width: 2
            - name: RESUB_CYC_CT
              type: decimal
              width: 2
            - name: RX_DT
              type: char
              width: 10
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: char
              width: 1
            - name: PRSN_CD
              type: varchar
              width: 3
            - name: OTHR_COV_CD
              type: decimal
              width: 2
            - name: ELIG_CLRFCTN_CD
              type: decimal
              width: 1
            - name: CMPND_CD
              type: decimal
              width: 1
            - name: NO_OF_RFLS_AUTH
              type: decimal
              width: 2
            - name: LVL_OF_SVC
              type: decimal
              width: 2
            - name: RX_ORIG_CD
              type: decimal
              width: 1
            - name: RX_DENIAL_CLRFCTN
              type: decimal
              width: 2
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
            - name: CLNC_ID_NO
              type: decimal
              width: 5
            - name: DRUG_TYP
              type: decimal
              width: 1
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: UNIT_DOSE_IN
              type: decimal
              width: 1
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: decimal
              width: 1
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
            - name: EXPNSN_AREA
              type: char
              width: 1
            - name: MSTR_CAR
              type: varchar
              width: 20
            - name: SUBCAR
              type: varchar
              width: 20
            - name: CLM_TYP
              type: char
              width: 1
            - name: SUBGRP
              type: varchar
              width: 20
            - name: PLN_DSGNR
              type: char
              width: 1
            - name: ADJDCT_DT
              type: char
              width: 10
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_PAY_CD
              type: char
              width: 2
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_CD
              type: char
              width: 2
            - name: FRMLRY_FLAG
              type: char
              width: 1
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
            - name: PDX_TYP
              type: char
              width: 3
            - name: BILL_BSS_CD
              type: char
              width: 2
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PD_DT
              type: char
              width: 10
            - name: BNF_CD
              type: varchar
              width: 10
            - name: DRUG_STRG
              type: varchar
              width: 20
            - name: ORIG_MBR
              type: varchar
              width: 20
            - name: INJRY_DT
              type: char
              width: 10
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: REF_NO
              type: varchar
              width: 20
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
            - name: PLN_TYP
              type: varchar
              width: 20
            - name: ADJDCT_REF_NO
              type: decimal
              width: 9
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 40
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
            - name: BILL_DT
              type: char
              width: 10
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
            - name: PICA_DRUG_CD
              type: char
              width: 1
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FED_DRUG_CLS_CD
              type: char
              width: 1
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BNF_COPAY_100
              type: char
              width: 1
            - name: CLM_PRCS_TYP
              type: char
              width: 1
            - name: INDEM_HIER_TIER_NO
              type: decimal
              width: 4
            - name: MCARE_D_COV_DRUG
              type: char
              width: 1
            - name: RETRO_LICS_CD
              type: char
              width: 1
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MCARE_B_DRUG
              type: char
              width: 1
            - name: MCARE_B_CLM
              type: char
              width: 1
            - name: PRSCRBR_QLFR
              type: char
              width: 2
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: PDX_QLFR
              type: char
              width: 2
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 11
            - name: THER_CLS
              type: decimal
              width: 6
            - name: HIC_NO
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: char
              width: 1
            - name: DOSE_CD
              type: decimal
              width: 4
            - name: LOW_INCM
              type: char
              width: 1
            - name: RTE_OF_ADMIN
              type: char
              width: 2
            - name: DEA_SCHD
              type: decimal
              width: 1
            - name: COPAY_BNF_OPT
              type: decimal
              width: 10
            - name: GNRC_PROD_IN
              type: decimal
              width: 14
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
            - name: VAL_CD
              type: varchar
              width: 20
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
            - name: OFC_OF_INSPECTOR_GNRL
              type: char
              width: 1
            - name: PATN_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
            - name: CARDHLDR_ADDR
              type: varchar
              width: 40
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
            - name: CHADHLDR_ST
              type: varchar
              width: 2
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_MET_AMT
              type: varchar
              width: 20
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PAR_PDX_IN
              type: char
              width: 1
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_TRNSMSN_METH
              type: char
              width: 1
            - name: RX_NO_2012
              type: decimal
              width: 12
            - name: CLM_ID
              type: varchar
              width: 20
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LOB_IN
              type: varchar
              width: 5
        - name: MapToAllDownstreamCommonFiles.V168S12P3.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: char
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: char
              width: 8
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: " If (LEN(Trim(Extract.PRESCRIBER)) <> 0 and trim(Extract.PRESCRIDQL = '12')) then Extract.PRESCRIBER else  if Len(Trim(Extract.PRESCDEAID)) <> 0 then Trim(Extract.PRESCDEAID) else 'NA'"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: char
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "\"OPTUMRX\""
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: LOB_IN
              type: varchar
              width: 5
              map: "'COM'"
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P3
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P3.map
          filter: "CLAIMSTS = 'R'"
          schema: 
            - name: MEMBERID
              type: char
              width: 20
            - name: ACCOUNTID
              type: char
              width: 15
            - name: NPIPROV
              type: char
              width: 10
            - name: DENIALDTE
              type: char
              width: 8
            - name: RXNUMBER
              type: char
              width: 12
            - name: DATESBM
              type: char
              width: 8
            - name: TIMESBM
              type: char
              width: 8
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: MBRSEX
              type: char
              width: 1
            - name: MBRRELCDE
              type: char
              width: 1
            - name: REJCDE1
              type: char
              width: 3
            - name: REJCDE2
              type: char
              width: 3
            - name: RXNETWRKQL
              type: char
              width: 1
            - name: MBRBIRTH
              type: char
              width: 8
            - name: PHRDISPFEE
              type: decimal
              width: 11
            - name: CLTDUEAMT
              type: decimal
              width: 11
            - name: CLT2DUEAMT
              type: decimal
              width: 11
            - name: CLTPATPAY
              type: decimal
              width: 11
            - name: CLTINGRCST
              type: decimal
              width: 11
            - name: MBRFSTNME
              type: char
              width: 15
            - name: MBRLSTNME
              type: char
              width: 25
            - name: SOCSECNBR
              type: char
              width: 9
            - name: SRVPROVNME
              type: char
              width: 55
            - name: PRESCDEAID
              type: char
              width: 15
            - name: NPIPRESCR
              type: char
              width: 10
            - name: PPRSFSTNME
              type: char
              width: 15
            - name: PPRSLSTNME
              type: char
              width: 25
            - name: PPRSTATE
              type: char
              width: 3
            - name: LABELNAME
              type: char
              width: 30
            - name: SRVPROVID
              type: char
              width: 15
            - name: CARID
              type: char
              width: 9
            - name: GROUPID
              type: char
              width: 15
            - name: CLMORIGIN
              type: char
              width: 1
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: PRODUCTID
              type: char
              width: 20
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: LOB_IN
              type: varchar
              width: 5
        - name: MapToAllDownstreamCommonFiles.V168S12P4.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Extract.DTEFILLED"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P4
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P4.map
          filter: "CLAIMSTS = 'P' Or CLAIMSTS = 'X'"
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_ID
        - name: MapToAllDownstreamCommonFiles.V168S12P5.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Extract.DTEFILLED"
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P5
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P5.map
          filter: "CLAIMSTS = 'P' Or CLAIMSTS = 'X'"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: FILL_DT_SK
              type: char
              width: 10
              nullable: N
          keys:
            - MBR_UNIQ_KEY
            - FILL_DT_SK
        - name: MapToAllDownstreamCommonFiles.V168S12P6.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: varchar
              width: 14
              map: " If Isnull(Extract.GPINUMBER) = @TRUE OR len(Trim(Extract.GPINUMBER)) = 0 then 'NA' else Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "\"OPTUMRX\""
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Extract.FMSTIER"
            - name: DRUG_TYP_CD
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: PLN_DRUG_STTUS_CD
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P6
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P6.map
          filter: "CLAIMSTS = 'P' Or CLAIMSTS = 'X'"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: METRICQTY
              type: decimal
              width: 6
            - name: GPINUMBER
              type: varchar
              width: 14
            - name: GENINDOVER
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
            - name: PROQTY
              type: decimal
              width: 12
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
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
              width: 7
              precision: 2
            - name: RXNETWRKQL
              type: varchar
              width: 1
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
            - name: CLTTOTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNTDEF2
              type: char
              width: 10
            - name: TIER_ID
              type: varchar
              width: 2
            - name: DRUG_TYP_CD
              type: char
              width: 1
            - name: PLN_DRUG_STTUS_CD
              type: char
              width: 1
            - name: PLANTYPE
              type: char
              width: 8
        - name: MapToAllDownstreamCommonFiles.V168S12P7.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "\"OPTUMRX\""
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P7
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P7.map
          filter: "(CLAIMSTS = 'P' Or CLAIMSTS = 'X') and svDoesBuyActualPaidEqClientDueAmt = 'N'"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: METRICQTY
              type: decimal
              width: 6
            - name: GPINUMBER
              type: char
              width: 14
            - name: GENINDOVER
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
            - name: PROQTY
              type: decimal
              width: 12
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
            - name: RXNETWRKQL
              type: varchar
              width: 1
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
            - name: CLTINGRCST
              type: decimal
              width: 11
            - name: CLTDISPFEE
              type: decimal
              width: 11
            - name: CLTSLSTAX
              type: decimal
              width: 11
            - name: CLTINCENTV
              type: decimal
              width: 11
            - name: CLTPATPAY
              type: decimal
              width: 11
            - name: CLTTOTHAMT
              type: decimal
              width: 11
            - name: CLTOTHPAYA
              type: decimal
              width: 11
            - name: CLT2PSTAX
              type: decimal
              width: 11
            - name: CLT2FSTAX
              type: decimal
              width: 11
            - name: CLTDUEAMT
              type: decimal
              width: 11
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLT2COSTSRC
              type: char
              width: 1
            - name: CLT2COSTTYP
              type: char
              width: 10
            - name: CLT2PRCTYPE
              type: char
              width: 10
        - name: MapToAllDownstreamCommonFiles.V168S12P8.map
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Extract.CLAIMSTS"
            - name: CARID
              type: char
              width: 9
              map: "Extract.CARID"
            - name: ACCOUNTID
              type: char
              width: 15
              map: "Extract.ACCOUNTID"
            - name: GROUPID
              type: char
              width: 15
              map: "Extract.GROUPID"
            - name: MEMBERID
              type: char
              width: 20
              map: "Extract.MEMBERID"
            - name: MBRLSTNME
              type: char
              width: 25
              map: "Extract.MBRLSTNME"
            - name: MBRFSTNME
              type: char
              width: 15
              map: "Extract.MBRFSTNME"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "Extract.MBRRELCDE"
            - name: MBRSEX
              type: char
              width: 1
              map: "Extract.MBRSEX"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: "Extract.MBRBIRTH"
            - name: SOCSECNBR
              type: char
              width: 9
              map: "Extract.SOCSECNBR"
            - name: CUSTLOC
              type: char
              width: 2
              map: "Extract.CUSTLOC"
            - name: FACILITYID
              type: char
              width: 10
              map: "Extract.FACILITYID"
            - name: OTHCOVERAG
              type: char
              width: 2
              map: "Extract.OTHCOVERAG"
            - name: DATESBM
              type: char
              width: 8
              map: "Extract.DATESBM"
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: char
              width: 12
              map: "Extract.RXNUMBER"
            - name: REFILL
              type: char
              width: 2
              map: "Extract.REFILL"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "Extract.DISPSTATUS"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "Extract.DTEFILLED"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "Extract.COMPOUNDCD"
            - name: PRODUCTID
              type: char
              width: 20
              map: "Extract.PRODUCTID"
            - name: METRICQTY
              type: decimal
              width: 6
              map: "Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: "Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: "Extract.DAYSSUPPLY"
            - name: PSC
              type: char
              width: 1
              map: "Extract.PSC"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: "Extract.WRITTENDTE"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "Extract.NBRFLSAUTH"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "Extract.ORIGINCDE"
            - name: PRAUTHNBR
              type: char
              width: 11
              map: "Extract.PRAUTHNBR"
            - name: LABELNAME
              type: char
              width: 30
              map: "Extract.LABELNAME"
            - name: GPINUMBER
              type: char
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: char
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Extract.SRXNETWRK"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Extract.RXNETWORK"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: char
              width: 15
              map: "Extract.SRVPROVID"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: "Extract.SRVPROVIDQ"
            - name: NPIPROV
              type: char
              width: 10
              map: "Extract.NPIPROV"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: "Extract.PRVNCPDPID"
            - name: SRVPROVNME
              type: char
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: char
              width: 3
              map: "Extract.DISPROTHER"
            - name: PRESCRIBER
              type: char
              width: 15
              map: "Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: char
              width: 2
              map: "Extract.PRESCRIDQL"
            - name: NPIPRESCR
              type: char
              width: 10
              map: "Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: char
              width: 15
              map: "Extract.PRESCDEAID"
            - name: PRESLSTNME
              type: char
              width: 25
              map: "Extract.PRESLSTNME"
            - name: PRESFSTNME
              type: char
              width: 15
              map: "Extract.PRESFSTNME"
            - name: PRESSPCCDE
              type: char
              width: 6
              map: "Extract.PRESSPCCDE"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: char
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Extract.PLANQUAL"
            - name: COBIND
              type: char
              width: 2
              map: "Extract.COBIND"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: char
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: char
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: char
              width: 2
              map: "Extract.BASISCOST"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: "Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: "Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: "Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: "Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: "Extract.DENIALDTE"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: "Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: "Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: "Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: "Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: "Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: "Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: "Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: "Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: "Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: "Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: char
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: "Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: char
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Extract.CLM_ID"
            - name: ORIG_MBR
              type: int
              width: 4
              nullable: N
              map: "Extract.ORIG_MBR"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: "Extract.PPRSFSTNME"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: "Extract.PPRSLSTNME"
            - name: PPRSTATE
              type: char
              width: 3
              map: "Extract.PPRSTATE"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: "Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: "Extract.RXNETPRCQL"
            - name: SBMPATRESD
              type: char
              width: 2
              map: "Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: "Extract.CLIENTDEF1"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "Extract.GROUPPLAN"
            - name: FMSTIER
              type: char
              width: 2
              map: "Extract.FMSTIER"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "Extract.CLNTDEF2"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: "Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Extract.PLANTYPE"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "\"OPTUMRX\""
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: svServicingProviderIDisNABP
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: svServicingProviderIDisNPI
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: svServicingProviderNCPDP
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: svNPIProvhasNPI
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: svPrescribingIDisDEA
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: svPrescribingIDisNPI
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: svPrescribingDEAIDisDEA
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: svNPIPresciberHasNPI
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: svDEAsMatchTest
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: svServiceIDToUse
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: svServiceNPIToUse
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: svPrescriberDEAtoUse
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: svPrescriberNPItoUse
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: svSellClientIngredientCosts
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: svSellClientDispenseFee
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: svSellClientSalesTax
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: svSellClientPatientPaid
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: svSellClientDueAmount
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svSellDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: svBuyClientIngredientCosts
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: svBuyClientDispenseFee
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: svBuyClientPSTax
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: svBuyClientOtherAmt
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: svBuyClientDueAmount
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: svBuyDeriveActualPaidPerFields
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              initValue: "\"N\""
        - name: MapToAllDownstreamCommonFiles.V168S12P8
          type: filter
          source: MapToAllDownstreamCommonFiles.V168S12P8.map
          filter: "(CLAIMSTS = 'P' Or CLAIMSTS = 'X') and svDoesActualPaidEqClientDueAmt = 'N'"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: METRICQTY
              type: decimal
              width: 6
            - name: GPINUMBER
              type: char
              width: 14
            - name: GENINDOVER
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
            - name: PROQTY
              type: decimal
              width: 12
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
            - name: CLTINGRCST
              type: decimal
              width: 11
            - name: CLTDISPFEE
              type: decimal
              width: 11
            - name: CLTSLSTAX
              type: decimal
              width: 11
            - name: CLTINCENTV
              type: decimal
              width: 11
            - name: CLTPATPAY
              type: decimal
              width: 11
            - name: CLTTOTHAMT
              type: decimal
              width: 11
            - name: CLTOTHPAYA
              type: decimal
              width: 11
            - name: CLT2PSTAX
              type: decimal
              width: 11
            - name: CLT2FSTAX
              type: decimal
              width: 11
            - name: CLTDUEAMT
              type: decimal
              width: 11
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLT2COSTSRC
              type: char
              width: 1
            - name: CLT2COSTTYP
              type: char
              width: 10
            - name: CLT2PRCTYPE
              type: char
              width: 10
        - name: MapToAllDownstreamCommonFiles.V168S12P9
          type: map
          source: Member_Lkp
          alias: Extract
          stageVars: 
            - name: svServicingProviderIDisNABP
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '07' then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: decimal
              width: 0
              map: " If Trim(Extract.SRVPROVIDQ) = '01' and ProviderNPIValidator(Extract.SRVPROVID) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: decimal
              width: 0
              map: " If Trim(Extract.PRVNCPDPID) > 0 then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPROV) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '12') AND ProviderDEAValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: decimal
              width: 0
              map: " If (trim(Extract.PRESCRIDQL = '01') AND ProviderNPIValidator(Extract.PRESCRIBER) = 'Y') then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: decimal
              width: 0
              map: " if ProviderDEAValidator(Extract.PRESCDEAID) = 'Y' then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: decimal
              width: 0
              map: " if ProviderNPIValidator(Extract.NPIPRESCR) = 'Y' then 'Y' ELSE 'N'"
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: decimal
              width: 0
              map: " If (svPrescribingIDisDEA = 'Y' and svPrescribingDEAIDisDEA = 'Y' and trim(Extract.PRESCRIBER) = trim(Extract.PRESCDEAID)) then 'Y' else 'N'"
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: decimal
              width: 0
              map: " If svServicingProviderIDisNABP = 'Y' then Extract.SRVPROVID else  if svServicingProviderNCPDP = 'Y' then Extract.PRVNCPDPID else \"NA\""
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: decimal
              width: 0
              map: " if svServicingProviderIDisNPI = 'Y' then tRIM(Extract.SRVPROVID) else  if svNPIProvhasNPI = 'Y' then Trim(Extract.NPIPROV) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisDEA = 'Y' then Trim(Extract.PRESCRIBER) else  if svPrescribingDEAIDisDEA = 'Y' then Trim(Extract.PRESCDEAID) else \"NA\""
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: decimal
              width: 0
              map: " If svPrescribingIDisNPI = 'Y' then Trim(Extract.PRESCRIBER) else  if svNPIPresciberHasNPI = 'Y' then Trim(Extract.NPIPRESCR) else \"NA\""
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTINGRCST) = @TRUE then 0.00 else Extract.CLTINGRCST"
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDISPFEE) = @TRUE then 0.00 else Extract.CLTDISPFEE"
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTSLSTAX) = @TRUE then 0.00 else Extract.CLTSLSTAX"
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTINCENTV) = @TRUE then 0.00 else Extract.CLTINCENTV"
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTTOTHAMT) = @TRUE then 0.00 else Extract.CLTTOTHAMT"
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLTOTHPAYA) = @TRUE then 0.00 else Extract.CLTOTHPAYA"
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTPATPAY) = @TRUE then 0.00 else Extract.CLTPATPAY"
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLTDUEAMT) = @TRUE then 0.00 else Extract.CLTDUEAMT"
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svSellClientIngredientCosts + svSellClientDispenseFee + svSellClientSalesTax + svSellClientOtherAmt + svSellClientIncentive) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svSellDeriveActualPaidPerFields = svSellClientDueAmount) then \"Y\" else \"N\""
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2INGRCST) = @TRUE then 0.00 else Extract.CLT2INGRCST"
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DISPFEE) = @TRUE then 0.00 else Extract.CLT2DISPFEE"
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2PSTAX) = @TRUE then 0.00 else Extract.CLT2PSTAX"
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CLT2FSTAX) = @TRUE then 0.00 else Extract.CLT2FSTAX"
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(Extract.CALINCENTV2) = @TRUE then 0.00 else Extract.CALINCENTV2"
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2OTHAMT) = @TRUE then 0.00 else Extract.CLT2OTHAMT"
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: decimal
              width: 0
              map: " If IsNull(Extract.CLT2DUEAMT) = @TRUE then 0.00 else Extract.CLT2DUEAMT"
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: decimal
              width: 0
              map: "(svBuyClientIngredientCosts + svBuyClientDispenseFee + svBuyClientPSTax + svBuyClientFSTax + svBuyClientIncentive + svBuyClientOtherAmt) - (svSellClientOtherPayAmt + svSellClientPatientPaid)"
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: decimal
              width: 0
              map: " If (svBuyDeriveActualPaidPerFields = svBuyClientDueAmount) Then \"Y\" Else \"N\""
              initValue: "\"N\""
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_ID"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: CLIENTDEF3
              type: char
              width: 10
              nullable: N
              map: " If (IsNull(Extract.CLIENTDEF3) = @TRUE or Len(Trim(Extract.CLIENTDEF3)) = 0) then 'NA' Else Trim(Extract.CLIENTDEF3)"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If (IsNull(Extract.CCTRESRV10) = @TRUE or Len(Trim(Extract.CCTRESRV10)) = 0) then 0 Else (Extract.CCTRESRV10)"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.FAMDEDPTD) = @TRUE or Len(Trim(Extract.FAMDEDPTD)) = 0) then 0 Else (Extract.FAMDEDPTD)"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.FAMOOPPTD) = @TRUE or Len(Trim(Extract.FAMOOPPTD)) = 0) then 0 Else (Extract.FAMOOPPTD)"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.INDDEDPTD) = @TRUE or Len(Trim(Extract.INDDEDPTD)) = 0) then 0 Else (Extract.INDDEDPTD)"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.INDOOPPTD) = @TRUE or Len(Trim(Extract.INDOOPPTD)) = 0) then 0 Else (Extract.INDOOPPTD)"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.CCTRESERV9) = @TRUE or Len(Trim(Extract.CCTRESERV9)) = 0) then 0 Else (Extract.CCTRESERV9)"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (IsNull(Extract.OOPAPPLIED) = @TRUE or Len(Trim(Extract.OOPAPPLIED)) = 0) then 0 Else (Extract.OOPAPPLIED)"
            - name: svServicingProviderIDisNABP
              type: varchar
              width: 0
              map: svServicingProviderIDisNABP
              dropStageVar: true
              initValue: "\"N\""
            - name: svServicingProviderIDisNPI
              type: varchar
              width: 0
              map: svServicingProviderIDisNPI
              dropStageVar: true
              initValue: "\"N\""
            - name: svServicingProviderNCPDP
              type: varchar
              width: 0
              map: svServicingProviderNCPDP
              dropStageVar: true
              initValue: "\"N\""
            - name: svNPIProvhasNPI
              type: varchar
              width: 0
              map: svNPIProvhasNPI
              dropStageVar: true
              initValue: "\"N\""
            - name: svPrescribingIDisDEA
              type: varchar
              width: 0
              map: svPrescribingIDisDEA
              dropStageVar: true
              initValue: "\"N\""
            - name: svPrescribingIDisNPI
              type: varchar
              width: 0
              map: svPrescribingIDisNPI
              dropStageVar: true
              initValue: "\"N\""
            - name: svPrescribingDEAIDisDEA
              type: varchar
              width: 0
              map: svPrescribingDEAIDisDEA
              dropStageVar: true
              initValue: "\"N\""
            - name: svNPIPresciberHasNPI
              type: varchar
              width: 0
              map: svNPIPresciberHasNPI
              dropStageVar: true
              initValue: "\"N\""
            - name: svDEAsMatchTest
              type: varchar
              width: 0
              map: svDEAsMatchTest
              dropStageVar: true
              initValue: "\"N\""
            - name: svServiceIDToUse
              type: varchar
              width: 0
              map: svServiceIDToUse
              dropStageVar: true
              initValue: "\"NA\""
            - name: svServiceNPIToUse
              type: varchar
              width: 0
              map: svServiceNPIToUse
              dropStageVar: true
              initValue: "\"NA\""
            - name: svPrescriberDEAtoUse
              type: varchar
              width: 0
              map: svPrescriberDEAtoUse
              dropStageVar: true
              initValue: "\"NA\""
            - name: svPrescriberNPItoUse
              type: varchar
              width: 0
              map: svPrescriberNPItoUse
              dropStageVar: true
              initValue: "\"NA\""
            - name: svSellClientIngredientCosts
              type: varchar
              width: 0
              map: svSellClientIngredientCosts
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientDispenseFee
              type: varchar
              width: 0
              map: svSellClientDispenseFee
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientSalesTax
              type: varchar
              width: 0
              map: svSellClientSalesTax
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svSellClientIncentive
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientOtherAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherAmt
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientOtherPayAmt
              type: decimal
              width: 18
              precision: 4
              map: svSellClientOtherPayAmt
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientPatientPaid
              type: varchar
              width: 0
              map: svSellClientPatientPaid
              dropStageVar: true
              initValue: "0.00"
            - name: svSellClientDueAmount
              type: varchar
              width: 0
              map: svSellClientDueAmount
              dropStageVar: true
              initValue: "0.00"
            - name: svSellDeriveActualPaidPerFields
              type: varchar
              width: 0
              map: svSellDeriveActualPaidPerFields
              dropStageVar: true
              initValue: "0.00"
            - name: svDoesActualPaidEqClientDueAmt
              type: varchar
              width: 0
              map: svDoesActualPaidEqClientDueAmt
              dropStageVar: true
              initValue: "\"N\""
            - name: svBuyClientIngredientCosts
              type: varchar
              width: 0
              map: svBuyClientIngredientCosts
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientDispenseFee
              type: varchar
              width: 0
              map: svBuyClientDispenseFee
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientPSTax
              type: varchar
              width: 0
              map: svBuyClientPSTax
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientFSTax
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientFSTax
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientIncentive
              type: decimal
              width: 18
              precision: 4
              map: svBuyClientIncentive
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientOtherAmt
              type: varchar
              width: 0
              map: svBuyClientOtherAmt
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyClientDueAmount
              type: varchar
              width: 0
              map: svBuyClientDueAmount
              dropStageVar: true
              initValue: "0.00"
            - name: svBuyDeriveActualPaidPerFields
              type: varchar
              width: 0
              map: svBuyDeriveActualPaidPerFields
              dropStageVar: true
              initValue: "0.00"
            - name: svDoesBuyActualPaidEqClientDueAmt
              type: varchar
              width: 0
              map: svDoesBuyActualPaidEqClientDueAmt
              dropStageVar: true
              initValue: "\"N\""
        - name: PrepAuditFile
          type: map
          source: CntByAdjDtRecType
          alias: DSLink192
          schema: 
            - name: Run_Date
              type: char
              width: 10
              nullable: N
              map: RunDate
            - name: Filler1
              type: char
              width: 2
              nullable: N
              map: "Space(2)"
            - name: Frequency
              type: char
              width: 9
              nullable: N
              map: "'DAILY'"
            - name: Filler2
              type: char
              width: 3
              nullable: N
              map: "'*  '"
            - name: Program
              type: char
              width: 9
              nullable: N
              map: "'ORX' : \"-\" : 'COM' : \"-\" : DSLink192.CLAIMSTS"
            - name: File_date
              type: char
              width: 11
              nullable: N
              map: "DSLink192.DATESBM"
            - name: Row_Count
              type: char
              width: 8
              nullable: N
              map: "DSLink192.RCRD_CNT"
            - name: Pd_amount
              type: char
              width: 12
              nullable: N
              map: "DSLink192.TOT_AMT"
        - name: DataCleansing.V168S9P2
          type: map
          source: Optum
          alias: Extract
          stageVars: 
            - name: svDATESBM
              type: decimal
              width: 0
              map: " If Len(tRIM(Extract.DATESBM)) = 0 then \"1753-01-01\" else FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.DATESBM))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
              initValue: "\"1753-01-01\""
          schema: 
            - name: RXCLMNBR
              type: decimal
              width: 15
              map: "Extract.RXCLMNBR"
            - name: CLMSEQNBR
              type: decimal
              width: 3
              map: "Extract.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "TRIM(Extract.CLAIMSTS)"
            - name: CARID
              type: varchar
              width: 9
              map: "TRIM(Extract.CARID)"
            - name: ACCOUNTID
              type: varchar
              width: 15
              map: " If Len(Trim(Extract.ACCOUNTID)) = 0 then \"UNK\" ELSE TRIM(Extract.ACCOUNTID)"
            - name: GROUPID
              type: varchar
              width: 15
              map: " If Len(Trim(Extract.GROUPID)) = 0 then \"UNK\" ELSE TRIM(Extract.GROUPID)"
            - name: MEMBERID
              type: varchar
              width: 20
              map: " If (IsNull(Extract.MEMBERID) = @TRUE or Len(Extract.MEMBERID)) = 0 then 'UNK' else Extract.MEMBERID[1, 11]"
            - name: MBRLSTNME
              type: varchar
              width: 25
              map: " If Len(Trim(Extract.MBRLSTNME)) = 0 then \"UNK\" ELSE TRIM(UPCASE(Extract.MBRLSTNME))"
            - name: MBRFSTNME
              type: varchar
              width: 15
              map: " If Len(Trim(Extract.MBRFSTNME)) = 0 then \"UNK\" ELSE TRIM(UPCASE(Extract.MBRFSTNME))"
            - name: MBRRELCDE
              type: char
              width: 1
              map: "trim(Extract.MBRRELCDE)"
            - name: MBRSEX
              type: char
              width: 1
              map: "Trim(Extract.MBRSEX)"
            - name: MBRBIRTH
              type: decimal
              width: 8
              map: " If Len(Extract.MBRBIRTH) = 0 then \"1753-01-01\" else FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.MBRBIRTH))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
            - name: SOCSECNBR
              type: varchar
              width: 9
              map: "TRIM(Extract.SOCSECNBR)"
            - name: CUSTLOC
              type: varchar
              width: 2
              map: "TRIM(Extract.CUSTLOC)"
            - name: FACILITYID
              type: varchar
              width: 10
              map: "TRIM(Extract.FACILITYID)"
            - name: OTHCOVERAG
              type: varchar
              width: 2
              map: "TRIM(Extract.OTHCOVERAG)"
            - name: DATESBM
              type: varchar
              width: 8
              map: svDATESBM
            - name: TIMESBM
              type: decimal
              width: 6
              map: "Extract.TIMESBM"
            - name: ORGPDSBMDT
              type: decimal
              width: 8
              map: "Extract.ORGPDSBMDT"
            - name: RXNUMBER
              type: varchar
              width: 12
              map: "TRIM(Extract.RXNUMBER)"
            - name: REFILL
              type: varchar
              width: 2
              map: "TRIM(Extract.REFILL)"
            - name: DISPSTATUS
              type: char
              width: 1
              map: "TRIM(Extract.DISPSTATUS)"
            - name: DTEFILLED
              type: decimal
              width: 8
              map: "FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.DTEFILLED))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
            - name: COMPOUNDCD
              type: char
              width: 1
              map: "TRIM(Extract.COMPOUNDCD)"
            - name: PRODUCTID
              type: varchar
              width: 20
              map: "trim(Extract.PRODUCTID)"
            - name: METRICQTY
              type: decimal
              width: 6
              map: " If IsNull(Extract.METRICQTY) = @TRUE then -999999 else Extract.METRICQTY"
            - name: DECIMALQTY
              type: decimal
              width: 13
              map: " If IsNull(Extract.DECIMALQTY) = @TRUE then -999999 else Extract.DECIMALQTY"
            - name: DAYSSUPPLY
              type: decimal
              width: 3
              map: " If IsNull(Extract.DAYSSUPPLY) = @TRUE then -999 else TRIM(Extract.DAYSSUPPLY)"
            - name: PSC
              type: char
              width: 1
              map: "TRIM(Extract.PSC)"
            - name: WRITTENDTE
              type: decimal
              width: 8
              map: " If Len(tRIM(Extract.WRITTENDTE)) = 0 then \"1753-01-01\" else FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.WRITTENDTE))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
            - name: NBRFLSAUTH
              type: decimal
              width: 2
              map: "TRIM(Extract.NBRFLSAUTH)"
            - name: ORIGINCDE
              type: char
              width: 1
              map: "TRIM(Extract.ORIGINCDE)"
            - name: PRAUTHNBR
              type: varchar
              width: 11
              map: "TRIM(Extract.PRAUTHNBR)"
            - name: LABELNAME
              type: varchar
              width: 30
              map: "TRIM(Extract.LABELNAME)"
            - name: GPINUMBER
              type: varchar
              width: 14
              map: "Extract.GPINUMBER"
            - name: MULTSRCCDE
              type: char
              width: 1
              map: "Extract.MULTSRCCDE"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Extract.GENINDOVER"
            - name: CPQSPCPRG
              type: varchar
              width: 10
              map: "Extract.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Extract.CPQSPCPGIN"
            - name: SRXNETWRK
              type: char
              width: 6
              map: "Trim(Extract.SRXNETWRK)"
            - name: RXNETWORK
              type: char
              width: 6
              map: "Trim(Extract.RXNETWORK)"
            - name: RXNETWRKQL
              type: char
              width: 1
              map: "Extract.RXNETWRKQL"
            - name: SRVPROVID
              type: varchar
              width: 15
              map: "TRIM(Extract.SRVPROVID)"
            - name: SRVPROVIDQ
              type: char
              width: 2
              map: " If len(trim(Extract.SRVPROVIDQ)) = 0 then \"00\" else trim(Extract.SRVPROVIDQ)"
            - name: NPIPROV
              type: varchar
              width: 10
              map: " If (IsNull(Extract.NPIPROV) = @TRUE or Len(Trim(Extract.NPIPROV)) = 0) then 'NA' else TRIM(Extract.NPIPROV)"
            - name: PRVNCPDPID
              type: char
              width: 12
              map: " If (IsNull(Extract.PRVNCPDPID) = @TRUE or Len(Trim(Extract.PRVNCPDPID)) = 0) then 'NA' else TRIM(Extract.PRVNCPDPID)"
            - name: SRVPROVNME
              type: varchar
              width: 55
              map: "Extract.SRVPROVNME"
            - name: DISPROTHER
              type: varchar
              width: 3
              map: "TRIM(Extract.DISPROTHER)"
            - name: PRESCRIBER
              type: varchar
              width: 15
              map: " If (IsNull(Extract.PRESCRIBER) = @TRUE or Len(Trim(Extract.PRESCRIBER)) = 0) then 'NA' else Extract.PRESCRIBER"
            - name: PRESCRIDQL
              type: varchar
              width: 2
              map: " If Len(Trim(Extract.PRESCRIDQL)) = 0 then \"00\" else trim(Extract.PRESCRIDQL)"
            - name: NPIPRESCR
              type: varchar
              width: 10
              map: " If (IsNull(Extract.NPIPRESCR) = @TRUE or Len(Trim(Extract.NPIPRESCR)) = 0) then 'NA' else Extract.NPIPRESCR"
            - name: PRESCDEAID
              type: varchar
              width: 15
              map: " If (IsNull(Extract.PRESCDEAID) = @TRUE or Len(Trim(Extract.PRESCDEAID)) = 0) then 'NA' else Trim(Extract.PRESCDEAID)"
            - name: PRESLSTNME
              type: varchar
              width: 25
              map: " iF Len(Trim(Extract.PRESLSTNME)) = 0 then \"\" else Trim(Extract.PRESLSTNME)"
            - name: PRESFSTNME
              type: varchar
              width: 15
              map: " iF Len(Trim(Extract.PRESFSTNME)) = 0 then \"\" else Trim(Extract.PRESFSTNME)"
            - name: PRESSPCCDE
              type: varchar
              width: 6
              map: " If Len(Trim(Extract.PRESSPCCDE)) = 0 then \"NA\" else Trim(Extract.PRESSPCCDE)"
            - name: PRSSTATE
              type: char
              width: 3
              map: "Extract.PRSSTATE"
            - name: FNLPLANCDE
              type: varchar
              width: 10
              map: "Extract.FNLPLANCDE"
            - name: FNLPLANDTE
              type: decimal
              width: 7
              map: "Extract.FNLPLANDTE"
            - name: PLANQUAL
              type: char
              width: 10
              map: "Trim(Extract.PLANQUAL)"
            - name: COBIND
              type: char
              width: 2
              map: "Trim(Extract.COBIND)"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Extract.FORMLRFLAG"
            - name: REJCDE1
              type: varchar
              width: 3
              map: "Extract.REJCDE1"
            - name: REJCDE2
              type: varchar
              width: 3
              map: "Extract.REJCDE2"
            - name: AWPUNITCST
              type: decimal
              width: 14
              map: " If (IsNull(Extract.AWPUNITCST) = @TRUE or Len(Trim(Extract.AWPUNITCST)) = 0) then '0.0' Else (Extract.AWPUNITCST)"
            - name: WACUNITCST
              type: decimal
              width: 14
              map: "Extract.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 14
              map: "Extract.CTYPEUCOST"
            - name: BASISCOST
              type: varchar
              width: 2
              map: "TRIM(Extract.BASISCOST)"
            - name: PROQTY
              type: decimal
              width: 12
              map: "Extract.PROQTY"
            - name: SBMINGRCST
              type: decimal
              width: 11
              map: " If (IsNull(Extract.SBMINGRCST) = @TRUE or Len(Trim(Extract.SBMINGRCST)) = 0) then 0 Else Extract.SBMINGRCST"
            - name: SBMDISPFEE
              type: decimal
              width: 11
              map: " If (IsNull(Extract.SBMDISPFEE) = @TRUE or Len(Trim(Extract.SBMDISPFEE)) = 0) then 0 else Extract.SBMDISPFEE"
            - name: SBMAMTDUE
              type: decimal
              width: 11
              map: " If (IsNull(Extract.SBMAMTDUE) = @TRUE or Len(Trim(Extract.SBMAMTDUE)) = 0) then 0 else Extract.SBMAMTDUE"
            - name: USUALNCUST
              type: decimal
              width: 11
              map: " If (IsNull(Extract.USUALNCUST) = @TRUE or Len(Trim(Extract.USUALNCUST)) = 0) then 0 else Extract.USUALNCUST"
            - name: DENIALDTE
              type: decimal
              width: 8
              map: " If Len(tRIM(Extract.DENIALDTE)) = 0 then \"1753-01-01\" else FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.DENIALDTE))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
            - name: PHRDISPFEE
              type: decimal
              width: 11
              map: " If (IsNull(Extract.PHRDISPFEE) = @TRUE or Len(Trim(Extract.PHRDISPFEE)) = 0) then 0 else Extract.PHRDISPFEE"
            - name: CLTINGRCST
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTINGRCST) = @TRUE or Len(Trim(Extract.CLTINGRCST)) = 0) then 0 else Extract.CLTINGRCST"
            - name: CLTDISPFEE
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTDISPFEE) = @TRUE or Len(Trim(Extract.CLTDISPFEE)) = 0) then 0 else Extract.CLTDISPFEE"
            - name: CLTSLSTAX
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTSLSTAX) = @TRUE or Len(Trim(Extract.CLTSLSTAX)) = 0) then 0 else Extract.CLTSLSTAX"
            - name: CLTPATPAY
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTPATPAY) = @TRUE or Len(Trim(Extract.CLTPATPAY)) = 0) then 0 Else Extract.CLTPATPAY"
            - name: CLTDUEAMT
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTDUEAMT) = @TRUE or Len(Trim(Extract.CLTDUEAMT)) = 0) then 0.00 Else Extract.CLTDUEAMT"
            - name: CLTFCOPAY
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTFCOPAY) = @TRUE or Len(Trim(Extract.CLTFCOPAY)) = 0) then 0 Else Extract.CLTFCOPAY"
            - name: CLTPCOPAY
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTPCOPAY) = @TRUE or Len(Trim(Extract.CLTPCOPAY)) = 0) then 0 Else Extract.CLTPCOPAY"
            - name: CLTCOPAY
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTCOPAY) = @TRUE or Len(Trim(Extract.CLTCOPAY)) = 0) then 0 else Extract.CLTCOPAY"
            - name: CLTINCENTV
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTINCENTV) = @TRUE or Len(Trim(Extract.CLTINCENTV)) = 0) then 0 Else Extract.CLTINCENTV"
            - name: CLTATRDED
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTATRDED) = @TRUE or Len(Trim(Extract.CLTATRDED)) = 0) then 0 Else Extract.CLTATRDED"
            - name: CLTTOTHAMT
              type: decimal
              width: 11
              map: "Extract.CLTTOTHAMT"
            - name: CLTOTHPAYA
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLTOTHPAYA) = @TRUE or Len(Trim(Extract.CLTOTHPAYA)) = 0) then 0 Else Extract.CLTOTHPAYA"
            - name: CLTCOSTTYP
              type: char
              width: 10
              map: "Extract.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: varchar
              width: 10
              map: "Extract.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 8
              map: "Extract.CLTRATE"
            - name: CLTPSCBRND
              type: decimal
              width: 11
              map: "Extract.CLTPSCBRND"
            - name: REIMBURSMT
              type: char
              width: 1
              map: "Extract.REIMBURSMT"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Extract.CLMORIGIN"
            - name: CLTTYPUCST
              type: decimal
              width: 14
              map: "Extract.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2INGRCST) = @TRUE or Len(Trim(Extract.CLT2INGRCST)) = 0) then 0.00 Else Extract.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2DISPFEE) = @TRUE or Len(Trim(Extract.CLT2DISPFEE)) = 0) then 0.00 Else Extract.CLT2DISPFEE"
            - name: CLT2SLSTAX
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2SLSTAX) = @TRUE or Len(Trim(Extract.CLT2SLSTAX)) = 0) then 0.00 Else Extract.CLT2SLSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2DUEAMT) = @TRUE or Len(Trim(Extract.CLT2DUEAMT)) = 0) then 0.00 Else Extract.CLT2DUEAMT"
            - name: CLT2PSTAX
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2PSTAX) = @TRUE or Len(Trim(Extract.CLT2PSTAX)) = 0) then 0.00 Else Extract.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2FSTAX) = @TRUE or Len(Trim(Extract.CLT2FSTAX)) = 0) then 0.00 Else Extract.CLT2FSTAX"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Extract.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: varchar
              width: 10
              map: "Extract.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: char
              width: 10
              map: "Extract.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 8
              map: "Extract.CLT2RATE"
            - name: PPRSFSTNME
              type: char
              width: 15
              map: " iF Len(Trim(Extract.PPRSFSTNME)) = 0 then \"\" else Trim(Extract.PPRSFSTNME)"
            - name: PPRSLSTNME
              type: char
              width: 25
              map: " iF Len(Trim(Extract.PPRSLSTNME)) = 0 then \"\" else Trim(Extract.PPRSLSTNME)"
            - name: PPRSTATE
              type: char
              width: 3
              map: " iF Len(Trim(Extract.PPRSTATE)) = 0 then \"\" else Trim(Extract.PPRSTATE)"
            - name: CLT2OTHAMT
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CLT2OTHAMT) = @TRUE or Len(Trim(Extract.CLT2OTHAMT)) = 0) then 0.00 Else Extract.CLT2OTHAMT"
            - name: CALINCENTV2
              type: decimal
              width: 11
              map: " If (IsNull(Extract.CALINCENTV2) = @TRUE or Len(Trim(Extract.CALINCENTV2)) = 0) then 0.00 Else Extract.CALINCENTV2"
            - name: RXNETPRCQL
              type: char
              width: 1
              map: " If (IsNull(Extract.RXNETPRCQL) = @TRUE or Len(Trim(Extract.RXNETPRCQL)) = 0) then 'N' Else  If Trim(Extract.RXNETPRCQL) = 'Y' Then 'Y' Else 'N'"
            - name: SBMPATRESD
              type: char
              width: 2
              map: " If (IsNull(Extract.SBMPATRESD) = @TRUE or Len(Trim(Extract.SBMPATRESD)) = 0) then 0 Else Extract.SBMPATRESD"
            - name: PRODTYPCDE
              type: char
              width: 2
              map: "Extract.PRODTYPCDE"
            - name: CONTHERAPY
              type: char
              width: 1
              map: "Extract.CONTHERAPY"
            - name: CTSCHEDID
              type: char
              width: 20
              map: "Extract.CTSCHEDID"
            - name: CLTPRODSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRODSEL"
            - name: CLTATRTAX
              type: decimal
              width: 11
              map: "Extract.CLTATRTAX"
            - name: CLTPROCFEE
              type: decimal
              width: 11
              map: "Extract.CLTPROCFEE"
            - name: CLTPRVNSEL
              type: decimal
              width: 11
              map: "Extract.CLTPRVNSEL"
            - name: CLIENTDEF1
              type: char
              width: 1
              map: " If (IsNull(Extract.CLIENTDEF1) = @TRUE or Len(Trim(Extract.CLIENTDEF1)) = 0) then 'N' Else Trim(Extract.CLIENTDEF1)"
            - name: GROUPPLAN
              type: char
              width: 10
              map: "TRIM(Extract.GROUPPLAN)"
            - name: FMSTIER
              type: char
              width: 2
              map: "TRIM(Extract.FMSTIER)"
            - name: CLNTDEF2
              type: char
              width: 10
              map: "TRIM(Extract.CLIENTDEF2)"
            - name: INDDEDPTD
              type: decimal
              width: 11
              map: "Extract.INDDEDPTD"
            - name: FAMDEDPTD
              type: decimal
              width: 11
              map: "Extract.FAMDEDPTD"
            - name: INDOOPPTD
              type: decimal
              width: 11
              map: "Extract.INDOOPPTD"
            - name: FAMOOPPTD
              type: decimal
              width: 11
              map: "Extract.FAMOOPPTD"
            - name: CLIENTDEF3
              type: char
              width: 10
              map: "Extract.CLIENTDEF3"
            - name: CCTRESERV8
              type: char
              width: 10
              map: "Extract.CCTRESERV8"
            - name: CCTRESERV9
              type: char
              width: 20
              map: "Extract.CCTRESERV9"
            - name: CCTRESRV10
              type: char
              width: 20
              map: "Extract.CCTRESRV10"
            - name: OOPAPPLIED
              type: decimal
              width: 11
              map: "Extract.OOPAPPLIED"
            - name: PLANDRUGST
              type: char
              width: 1
              map: " If (IsNull(Extract.PLANDRUGST) = @TRUE or Len(Extract.PLANDRUGST)) = 0 then 'UNK' else Extract.PLANDRUGST"
            - name: PLANTYPE
              type: char
              width: 8
              map: " If (IsNull(Extract.PLANTYPE) = @TRUE or Len(Extract.PLANTYPE)) = 0 then 'UNK' else Extract.PLANTYPE"
            - name: svDATESBM
              type: varchar
              width: 0
              map: svDATESBM
              dropStageVar: true
              initValue: "\"1753-01-01\""
        - name: DataCleansing.V168S9P3
          type: map
          source: Optum
          alias: Extract
          stageVars: 
            - name: svDATESBM
              type: decimal
              width: 0
              map: " If Len(tRIM(Extract.DATESBM)) = 0 then \"1753-01-01\" else FORMAT.DATE(Trim(Convert(CHAR(10) : CHAR(13) : CHAR(9), \"\", (Extract.DATESBM))), \"CHAR\", \"CCYYMMDD\", \"CCYY-MM-DD\")"
              initValue: "\"1753-01-01\""
          schema: 
            - name: INROW_NUM
              type: int
              width: 4
              nullable: N
              map: "@INROWNUM"
            - name: CLAIMSTS
              type: char
              width: 1
              nullable: N
              map: "Extract.CLAIMSTS"
            - name: AMT_BILL
              type: decimal
              width: 11
              precision: 2
              nullable: N
              map: "Extract.CLT2DUEAMT"
            - name: DATESBM
              type: varchar
              width: 8
              map: svDATESBM
            - name: svDATESBM
              type: varchar
              width: 0
              map: svDATESBM
              dropStageVar: true
              initValue: "\"1753-01-01\""
        - name: CntByAdjDtRecType
          type: unknown
          source: DataCleansing.V168S9P3
          alias: Aggregate
          origType: AGGREGATOR


