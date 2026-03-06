model:
  config:
    origin: datastage
  jobs:
    - name: OptumDrugLandSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqOptum"
      checkpoint: true
      description: "OptumRX Drug Claim Landing Sequencer"
      parameters:
        - name: CurrentDate
          type: string
          description: CurrentDate
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "Run Cycle"
        - name: ExclusionList
          type: string
          description: ExclusionList
          defaultType: expr
          defaultValue: "\"\""
        - name: RunID
          type: string
          description: "Run ID"
        - name: SrcSysCd
          type: string
          description: "Source System"
        - name: SrcSysCdSK
          type: integer
          description: "Source Sys Cd SK"
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS DSN"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
        - name: CactusServer
          type: string
          description: "Cactus Server"
          defaultType: project
        - name: CactusDB
          type: string
          description: "Cactus Database"
          defaultType: project
        - name: CactusOwner
          type: string
          description: "Cactus Table Owner"
          defaultType: project
        - name: CactusAcct
          type: string
          description: "Cactus Account"
        - name: CactusPW
          type: encrypted
          description: "Cactus Password"
        - name: VendorEmail
          type: string
          description: VendorEmail
        - name: Logging
          type: string
          description: Logging
        - name: LANAcct
          type: string
          description: "LAN Account"
        - name: LANPW
          type: encrypted
          description: "LAN Password"
        - name: ProvRunCycle
          type: integer
          description: ProvRunCycle
        - name: InFile
          type: string
          description: InFile
        - name: Env
          type: string
          description: Env
        - name: MbrTermDate
          type: string
          description: MbrTermDate
          longDescription: "Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years."
          defaultType: expr
          defaultValue: "2016-01-01"
      activities:
        - name: BCBSKCCommClmPKExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmPKExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSK"
              expression: SrcSysCdSK
            - name: "CurrRunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: Or_2B
            - activity: If_2E
        - name: BCBSKCCommClmLnPKExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmLnPKExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSK"
              expression: SrcSysCdSK
            - name: "CurrRunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: If_2D
            - activity: Or_2C
        - name: BCBSKCCommDrugExtrSeq
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommDrugExtrSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "CactusServer"
              expression: "$CactusServer"
            - name: "CactusDB"
              expression: "$CactusDB"
            - name: "CactusOwner"
              expression: "$CactusOwner"
            - name: "CactusAcct"
              expression: "$CactusAcct"
            - name: "CactusPW"
              expression: "$CactusPW"
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "RunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "ExclusionList"
              expression: ExclusionList
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSK
            - name: "Logging"
              expression: Logging
            - name: "ProvRunCycle"
              expression: ProvRunCycle
          triggers: 
            - activity: Or_1B
        - name: Any1
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any2
        - name: Any2
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3B
        - name: Or_5B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2B
        - name: LoadW_DRUG_ENR_PCP
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"W_DRUG_CLM_PCP\""
            - name: "Arg2"
              expression: "\"REPLACE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"W_DRUG_CLM_PCP.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_5B
        - name: OptumClmLandExtr
          type: dataflow
          executionType: reset then run
          jobname: OptumClmLandExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "RunID"
              expression: RunID
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSK"
              expression: SrcSysCdSK
            - name: "RunDate"
              expression: CurrentDate
            - name: "InFile"
              expression: InFile
            - name: "MbrTermDate"
              expression: MbrTermDate
            - name: "RunCycle"
              expression: RunCycle
          triggers: 
            - activity: Or_2A
        - name: Sequencer_395
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: If_1C
        - name: If_2B2
          type: switch
          triggers: 
            - activity: Or_2B2
              conditionType: Otherwise
            - activity: OptumDrugClmAccumImpctExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"OptumDrugClmAccumImpctExtr\" :\";\") = 0"
        - name: Or_2B2
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2C
        - name: OptumDrugClmAccumImpctExtr
          type: dataflow
          executionType: reset then run
          jobname: OptumDrugClmAccumImpctExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSK"
              expression: SrcSysCdSK
            - name: "CurrRunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: Or_2B2
            - activity: If_2E2
        - name: If_2E2
          type: switch
          triggers: 
            - activity: Load_DRUG_CLM_ACCUM_IMPCT
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"Load_DRUG_CLM_ACCUM_IMPCT\" :\";\") = 0"
        - name: Load_DRUG_CLM_ACCUM_IMPCT
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"DRUG_CLM_ACCUM_IMPCT\""
            - name: "Arg2"
              expression: "\"UPDATE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"DRUG_CLM_ACCUM_IMPCT.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_1B
          type: switch
          triggers: 
            - activity: BCBSKCCommDrugExtrSeq
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommDrugExtrSeq\" :\";\") = 0"
            - activity: Or_1B
              conditionType: Otherwise
        - name: Any3
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Sequencer_395
        - name: If_2E
          type: switch
          triggers: 
            - activity: LoadKClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadKClm\" :\";\") = 0"
        - name: If_2D
          type: switch
          triggers: 
            - activity: LoadKClmLn
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadKClmLn\" :\";\") = 0"
        - name: If_2B
          type: switch
          triggers: 
            - activity: BCBSKCCommClmPKExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmPKExtr\" :\";\") = 0"
            - activity: Or_2B
              conditionType: Otherwise
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2B2
        - name: LoadKClmLn
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"K_CLM_LN\""
            - name: "Arg2"
              expression: "\"UPDATE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"K_CLM_LN.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: LoadKClm
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"K_CLM\""
            - name: "Arg2"
              expression: "\"UPDATE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"K_CLM.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: Or_2C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1B
        - name: If_2C
          type: switch
          triggers: 
            - activity: BCBSKCCommClmLnPKExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmLnPKExtr\" :\";\") = 0"
            - activity: Or_2C
              conditionType: Otherwise
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3A
        - name: If_1A
          type: switch
          triggers: 
            - activity: OptumClmLandExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"OptumClmLandExtr\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: If_3A
          type: switch
          triggers: 
            - activity: LoadW_DRUG_ENR
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadW_DRUG_ENR\" :\";\") = 0"
            - activity: Or_5A
              conditionType: Otherwise
        - name: Or_5A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any1
        - name: LoadW_DRUG_ENR
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"W_DRUG_ENR\""
            - name: "Arg2"
              expression: "\"REPLACE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"W_DRUG_ENR.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_5A
        - name: If_3B
          type: switch
          triggers: 
            - activity: LoadW_DRUG_ENR_PCP
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadW_DRUG_ENR_PCP\" :\";\") = 0"
            - activity: Or_5B
              conditionType: Otherwise
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any3
        - name: If_1D
          type: switch
          triggers: 
            - activity: notify_BuyPricevalidation
              conditionType: Custom
              condition: "UnixFileRecCnt($FilePath:'/verified', 'Optum_BuyPriceValidation_':RunID :'.dat') >1"
            - activity: Or_1D
              conditionType: Otherwise
        - name: Or_1D
          type: sequencer
          sequencerType: any
        - name: notify_BuyPricevalidation
          type: sendEmail
          mailServer: "mail.bcbskc.com"
          to: "#VendorEmail#"
          from: "_PBM_SPREAD_BALANCE_ERRORS@BlueKC.com"
          subject: "Alert - #$IDSDB#_OPTUMRX BuyPriceValidation Audit File"
          messageSource: "Please find attached the Audit file for OPTUMRX Claims with Unmatched Buy Price \n\nThanks\n\nThis is an auto generated email alert. Do not reply."
          triggers: 
            - activity: mvBuyPriceValidation
        - name: mvBuyPriceValidation
          type: shell
          path: cd
          command: "#$FilePath#/verified; mv -f Optum_BuyPriceValidation_#RunID#.dat   processed/;    compress -f   processed/Optum_BuyPriceValidation_#RunID#.dat"
          triggers: 
            - activity: Or_1D
        - name: mvSellPriceValidation
          type: shell
          path: cd
          command: "#$FilePath#/verified; mv -f Optum_SellPriceValidation_#RunID#.dat   processed/;    compress -f   processed/Optum_SellPriceValidation_#RunID#.dat"
          triggers: 
            - activity: Or_1C
        - name: notify_SellPriceValidation
          type: sendEmail
          mailServer: "mail.bcbskc.com"
          to: "#VendorEmail#"
          from: "_PBM_SPREAD_BALANCE_ERRORS@BlueKC.com"
          subject: "Alert - #$IDSDB#_OPTUMRX BuyPriceValidation Audit File"
          messageSource: "Please find attached the Audit file for OPTUMRX Claims with unmatched Sell Price \n\nThanks\n\nThis is an auto generated email alert. Do not reply."
          triggers: 
            - activity: mvSellPriceValidation
        - name: Or_1C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1D
        - name: If_1C
          type: switch
          triggers: 
            - activity: notify_SellPriceValidation
              conditionType: Custom
              condition: "UnixFileRecCnt($FilePath:'/verified', 'Optum_SellPriceValidation_':RunID :'.dat') >1"
            - activity: Or_1C
              conditionType: Otherwise


