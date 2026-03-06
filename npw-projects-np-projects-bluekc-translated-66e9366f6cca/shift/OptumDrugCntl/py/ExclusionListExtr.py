#!/usr/bin/python3

from npadf import *

def ExclusionListExtrActivities(ctx):
  def dfExclusionListExtr():
    dataflowName = "ExclusionListExtr"
    buffer = DataflowBuffer()
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="JobSequencer/CommonComponents"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ("{ctx.IDSAcct}"),
        IDSPW as string (toString("HDI@IJV8O9JN064IL:JD1K95")),
        Subject as string ,
        DSE_UVNLSOFF_SKIP_WARNINGS as string (toString("1")),
        UVNLSOFF as string (toString("1"))
      }}
  """, type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfExclusionListExtr()
  activityName = "ExclusionListExtr"
  activityParameters = {"IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "Subject":  {"value": "'@{pipeline().parameters.Subject}'","type": "Expression"},
  "DSE_UVNLSOFF_SKIP_WARNINGS":  {"value": "'@{pipeline().parameters.DSE_UVNLSOFF_SKIP_WARNINGS}'","type": "Expression"},
  "UVNLSOFF":  {"value": "'@{pipeline().parameters.UVNLSOFF}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def ExclusionListExtr(ctx):
  name = "ExclusionListExtr"
  artifacts, activities = ExclusionListExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="JobSequencer/CommonComponents"),
    activities = activities,
    description = """
      Extract subject records from W_TASK_EXCL
      * VC LOGS *
      ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
      ^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
      ^1_1 11/21/07 15:09:17 Batch  14570_54560 INIT bckcetl ids20 dsadm dsadm
      ^1_1 08/23/07 08:22:09 Batch  14480_30134 INIT bckcetl ids20 dsadm dsadm
      ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
      ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
      ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
      ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
      ^1_1 07/06/05 08:44:13 Batch  13702_31459 PROMOTE bckcetl ids20 dsadm Gina Parr
      ^1_1 07/06/05 08:41:57 Batch  13702_31322 INIT bckcett testIDS30 dsadm Gina Parr
      ^1_4 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent
      ^1_3 06/15/05 11:06:14 Batch  13681_39980 PROMOTE bckcett VERSION u08717 Brent
      ^1_3 06/15/05 11:05:50 Batch  13681_39953 INIT bckcett devlIDS30 u08717 Brent
      ^1_2 05/25/05 14:24:33 Batch  13660_51876 INIT bckcett devlIDS30 u08717 Brent
      ^1_1 05/25/05 14:13:44 Batch  13660_51232 PROMOTE bckcett VERSION u08717 Brent
      ^1_1 05/25/05 14:02:38 Batch  13660_50563 INIT bckccdt ids20 dsadm Brent

      COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


      JOB NAME:    ExclusionListExtr


      DESCRIPTION:   Extract the records for a subject from the DB2 - W_TASK_EXCL table.
            

      INPUTS:	 Subject name

        
      HASH FILES:  TASK_EXCL


      TRANSFORMS:  None
                              
         
      PROCESSING:   Task rows from DB2 table are concatenated together with a semi-colon delimiter.  Each row overwrites the previous row in the TASK_EXCL UV table.  The last entry is the combination of all DB2 entries.
       

      OUTPUTS:  A semi-colon delimited string of tasks to exclude for a subject.
                          


      MODIFICATIONS:
                    B Leland           05/25/2005  -  Initial programming
      Parameters:
      -----------
      IDSDB:
        IDS Database
      IDSOwner:
        IDS DB Owner
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password
      Subject:
        Subject Name
        Passed in by the calling job.
      DSE_UVNLSOFF_SKIP_WARNINGS:
        Warning
      UVNLSOFF:
        Warn""",
    parameters = {
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String", default_value=ctx.IDSAcct),
      "IDSPW": ParameterSpecification(type="String", default_value="HDI@IJV8O9JN064IL:JD1K95"),
      "Subject": ParameterSpecification(type="String"),
      "DSE_UVNLSOFF_SKIP_WARNINGS": ParameterSpecification(type="String", default_value="1"),
      "UVNLSOFF": ParameterSpecification(type="String", default_value="1")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (ExclusionListExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
