model:
  config:
    origin: datastage
  jobs:
    - name: IdsWebDMDrugPrereqSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqDrug"
      checkpoint: true
      description: "Argus prerequisit data processing for Web Data Mart"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: ExclusionList
          type: string
          description: "Exclusion List"
          defaultType: expr
          defaultValue: "\"\""
        - name: RunID
          type: string
          description: RunID
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: expr
          defaultValue: "#PROJDEF"
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: expr
          defaultValue: "#PROJDEF"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: expr
          defaultValue: "#PROJDEF"
        - name: IDSDSN
          type: string
          description: "IDS DSN"
          defaultType: expr
          defaultValue: "#PROJDEF"
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
          defaultType: expr
          defaultValue: "#PROJDEF"
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
      activities:
        - name: LoadW_WEBDM_ETL_DRVR
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"W_WEBDM_ETL_DRVR\""
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
              expression: "\"W_WEBDM_ETL_DRVR.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_2B
              conditionType: OK
        - name: If_2B
          type: switch
          triggers: 
            - activity: LoadW_WEBDM_ETL_DRVR
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadW_WEBDM_ETL_DRVR\" :\";\") = 0"
            - activity: Or_2B
              conditionType: Otherwise
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2B
        - name: If_2A
          type: switch
          triggers: 
            - activity: ConcatClmMartDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmMartDrug\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: ConcatClmMartDrug
          type: shell
          path: cat
          command: "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.Drug*  > #$FilePath#/load/W_WEBDM_ETL_DRVR.dat"
          triggers: 
            - activity: Or_2A
              conditionType: OK
        - name: RmClmMartDrug
          type: shell
          path: rm
          command: " -f #$FilePath#/load/W_WEBDM_ETL_DRVR.dat.Drug*"
        - name: If_2C
          type: switch
          triggers: 
            - activity: RmClmMartDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClmMartDrug\" :\";\") = 0"
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2C


