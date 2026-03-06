#!/usr/bin/python3

from npadf import *

def IdsWebDMDrugPrereqSeqActivities(ctx):
  def If_2A():
    def ConcatClmMartDrug():
      activities = [CustomActivity(
        name = "ConcatClmMartDrug",
        command = ctx.formatShellCommand('cat' toString($FilePath) + '/load/W_WEBDM_ETL_DRVR.dat.Drug*  > ' + toString($FilePath) + '/load/W_WEBDM_ETL_DRVR.dat'),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('ConcatClmMartDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        ConcatClmMartDrug()))]
    return artifacts, activities
  def If_2B():
    def LoadW_WEBDM_ETL_DRVR():
      activities = [CustomActivity(
        name = "LoadW_WEBDM_ETL_DRVR",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadW_WEBDM_ETL_DRVR', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadW_WEBDM_ETL_DRVR()),
      depends_on = [
        ActivityDependency(
          activity = "If_2A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  def If_2C():
    def RmClmMartDrug():
      activities = [CustomActivity(
        name = "RmClmMartDrug",
        command = ctx.formatShellCommand('rm' ' -f ' + toString($FilePath) + '/load/W_WEBDM_ETL_DRVR.dat.Drug*'),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2C",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('RmClmMartDrug', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        RmClmMartDrug()),
      depends_on = [
        ActivityDependency(
          activity = "If_2B",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_2C(),
    If_2B(),
    If_2A())

def IdsWebDMDrugPrereqSeq(ctx):
  name = "IdsWebDMDrugPrereqSeq"
  artifacts, activities = IdsWebDMDrugPrereqSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqDrug"),
    activities = activities,
    description = """
      Argus prerequisit data processing for Web Data Mart
      Parameters:
      -----------
      FilePath:
        File Path
      ExclusionList:
        Exclusion List
      RunID:
        RunID
      IDSInstance:
        IDS DB Instance
      IDSOwner:
        IDS DB Owner
      IDSDB:
        IDS Database
      IDSDSN:
        IDS DSN
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "RunID": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value="#PROJDEF"),
      "IDSOwner": ParameterSpecification(type="String", default_value="#PROJDEF"),
      "IDSDB": ParameterSpecification(type="String", default_value="#PROJDEF"),
      "IDSDSN": ParameterSpecification(type="String", default_value="#PROJDEF"),
      "IDSAcct": ParameterSpecification(type="String", default_value="#PROJDEF"),
      "IDSPW": ParameterSpecification(type="String")},
    variables = {
      "If_2C_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsWebDMDrugPrereqSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
