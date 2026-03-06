model:
  config:
    origin: datastage
  jobs:
    - name: OptumIdsMbrPdxDeniedTransLoadSeq
      origin: datastage
      type: controlflow
      folder: "DataWarehouse/Claim/Seq"
      checkpoint: true
      description: "6131 - PBM REPLACEMENT: Extract OPTUMRX Denied data from FILE and load IDS tables"
      parameters:
        - name: APT_CONFIG_FILE
          type: string
          description: "Configuration file"
          longDescription: "The Parallel job configuration file."
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS ODBC DSN"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSOwner
          type: string
          description: "IDS Owner"
          defaultType: project
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: SrcSysCd
          type: string
          description: SrcSysCd
          defaultType: expr
          defaultValue: OPTUMRX
        - name: RunID
          type: string
          description: RunID
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
        - name: IDSDB2ArraySize
          type: string
          description: "IDS DB2 Array Size for Load"
          defaultType: project
        - name: IDSDB2RecordCount
          type: string
          description: "IDS DB2 Record Count for Load"
          defaultType: project
        - name: ExclusionList
          type: string
          description: ExclusionList
        - name: PrefixFkeyFailedFileName
          type: string
          description: PrefixFkeyFailedFileName
          defaultType: expr
          defaultValue: PrefixFkeyFailedFileName
        - name: CurrDate
          type: string
          description: CurrDate
      activities:
        - name: Sequencer_675
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: DeniedClaimsLandingFile
        - name: DeniedClaimsLandingFile
          type: shell
          path: cd
          command: "#$FilePath#/verified; mv -f #SrcSysCd#_DeniedClaims_Landing.dat.#RunID#   processed/;    compress -f   processed/#SrcSysCd#_DeniedClaims_Landing.dat.#RunID# "
          triggers: 
            - activity: DeniedPkeyFile
        - name: IdsMbrPdxDeniedClaimsFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsMbrPdxDeniedClmFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "APT_CONFIG_FILE"
              expression: "$APT_CONFIG_FILE"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "IDSRunCycle"
              expression: IDSRunCycle
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "RunID"
              expression: RunID
            - name: "PrefixFkeyFailedFileName"
              expression: PrefixFkeyFailedFileName
          triggers: 
            - activity: Or_1C
        - name: IdsMbrPdxDeniedClmPkey
          type: dataflow
          executionType: reset then run
          jobname: IdsMbrPdxDeniedClmPkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "APT_CONFIG_FILE"
              expression: "$APT_CONFIG_FILE"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSRunCycle"
              expression: IDSRunCycle
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "RunID"
              expression: RunID
          triggers: 
            - activity: Or_1B
        - name: OptumIdsMbrPdxDeniedClmXfm
          type: dataflow
          executionType: reset then run
          jobname: OptumIdsMbrPdxDeniedClmXfm
          parameters:
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "APT_CONFIG_FILE"
              expression: "$APT_CONFIG_FILE"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "IDSRunCycle"
              expression: IDSRunCycle
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "RunID"
              expression: RunID
            - name: "CurrDate"
              expression: CurrDate
          triggers: 
            - activity: Or_1A
        - name: If_1C
          type: switch
          triggers: 
            - activity: IdsMbrPdxDeniedClaimsFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsMbrPdxDeniedTransFkey\" :\";\") = 0"
            - activity: Or_1C
              conditionType: Otherwise
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1C
        - name: If_1B
          type: switch
          triggers: 
            - activity: Or_1B
              conditionType: Otherwise
            - activity: IdsMbrPdxDeniedClmPkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsMbrPdxDeniedTransPkey\" :\";\") = 0"
        - name: Or_1A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1B
        - name: Or_1C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1D
        - name: If_1A
          type: switch
          triggers: 
            - activity: OptumIdsMbrPdxDeniedClmXfm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"OptumIdsMbrPdxDeniedClmXfm\" :\";\") = 0"
            - activity: Or_1A
              conditionType: Otherwise
        - name: Or_1D
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Sequencer_675
        - name: If_1D
          type: switch
          triggers: 
            - activity: IdsMbrPdxDeniedClmLoad
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsMbrPdxDeniedTransLoad\" :\";\") = 0"
            - activity: Or_1D
              conditionType: Otherwise
        - name: IdsMbrPdxDeniedClmLoad
          type: dataflow
          executionType: reset then run
          jobname: IdsMbrPdxDeniedClmLoad
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "RunID"
              expression: RunID
          triggers: 
            - activity: Or_1D
        - name: DeniedPkeyFile
          type: shell
          path: cd
          command: "#$FilePath#/key/; mv -f MBR_PDX_DENIED_TRANS.#SrcSysCd#.pkey.#RunID#.dat processed/; compress -f   processed/MBR_PDX_DENIED_TRANS.#SrcSysCd#.pkey.#RunID#.dat"
          triggers: 
            - activity: DeniedLoadile
        - name: DeniedLoadile
          type: shell
          path: cd
          command: "#$FilePath#/load/; mv -f MBR_PDX_DENIED_TRANS.#SrcSysCd#.#RunID#.dat processed/; compress -f   processed/MBR_PDX_DENIED_TRANS.#SrcSysCd#.#RunID#.dat"


