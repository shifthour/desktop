model:
  config:
    origin: datastage
  jobs:
    - name: OptumIdsDrugClmPriceLoadSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqOptum"
      checkpoint: true
      description: "Extract Spread pricing data perform lookup in fkey job on Optum file and load IDS DRUG_CLM_PRICE tables."
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
          defaultType: expr
          defaultValue: "{iisenc}eHG47YUwCsilbsYLdMUwUw=="
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
        - name: SrcSysCdSK
          type: integer
          description: SrcSysCdSK
          defaultType: expr
          defaultValue: "-1951778164"
        - name: RunID
          type: string
          description: RunID
          defaultType: expr
          defaultValue: "101"
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
          defaultType: expr
          defaultValue: "102"
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
        - name: UWSAcct
          type: string
          description: "UWS Account"
          defaultType: project
        - name: UWSPW
          type: encrypted
          description: "UWS Password"
          defaultType: expr
          defaultValue: "{iisenc}eHG47YUwCsilbsYLdMUwUw=="
        - name: UWSOwner
          type: string
          description: "UWS Table Owner"
          defaultType: project
        - name: UWSDB
          type: string
          description: "UWS Database"
          defaultType: project
        - name: Current_Dt
          type: string
          description: Current_Dt
      activities:
        - name: OptumIdsMbrPdxDrugClmPriceFkey
          type: dataflow
          executionType: reset then run
          jobname: OptumIdsDrugClmPriceFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "UWSOwner"
              expression: "$UWSOwner"
            - name: "UWSDB"
              expression: "$UWSDB"
            - name: "UWSAcct"
              expression: "$UWSAcct"
            - name: "UWSPW"
              expression: "$UWSPW"
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "RunID"
              expression: RunID
            - name: "IDSRunCycle"
              expression: IDSRunCycle
            - name: "CurrentDate"
              expression: Current_Dt
          triggers: 
            - activity: Or_1B
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
            - activity: OptumIdsMbrPdxDrugClmPriceFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"OptumIdsMbrPdxDrugClmPriceFkey\" :\";\") = 0"
        - name: Or_1C
          type: sequencer
          sequencerType: any
        - name: If_1C
          type: switch
          triggers: 
            - activity: OptumIdsMbrPdxDrugClmPriceLoad
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"OptumIdsMbrPdxDrugClmPriceLoad\" :\";\") = 0"
            - activity: Or_1C
              conditionType: Otherwise
        - name: OptumIdsMbrPdxDrugClmPriceLoad
          type: dataflow
          executionType: reset then run
          jobname: OptumIdsDrugClmPriceLoad
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
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "IDSOwner"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_1C


