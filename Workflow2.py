################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 11 MAY 2017
##INPUT         :
##DESCRIPTION   : Wrapper script for workflow 2 of SureKids.
################################################
import json


def read_config(config_json):
    """Read the configuration file"""
    json_data = open(config_json)
    data = json.loads(json_data.read())
    json_data.close()
    workflow_config = data["DNAnexus"]
    return workflow_config


def form_command_multiple_file(workflow_config, file_list, json_key):
    """Form the part of the command line that may be multiplied by file of family members"""
    command = ""
    for input_file in file_list:
        command += "-i%s=%s " % (workflow_config[json_key], input_file)
    command = command.strip()
    return command


def form_dx_command(workflow_config, vcf_file_list, tbi_file_list, bai_file_list, bam_file_list):
    """Form the DNAnexus command line"""
    family_id = ""
    output_folder = ""
    ped_file = ""
    vcf_command = form_command_multiple_file(workflow_config, vcf_file_list, "DNA_WF2_VCF")
    tbi_command = form_command_multiple_file(workflow_config, tbi_file_list, "DNA_WF2_TBI")
    bam_command = form_command_multiple_file(workflow_config, bam_file_list, "DNA_WF2_BAM")
    bai_command = form_command_multiple_file(workflow_config, bai_file_list, "DNA_WF2_BAI")
    command = "dx run %s %s %s -i%s=%s -y --brief --destination %s -i%s=%s %s %s -i%s=%s -i%s=%s" \
              % (workflow_config["DNA_SK_WORKFLOW"],
                 vcf_command, tbi_command,
                 workflow_config["DNA_WF2_VCF"].split(".")[0] + ".prefix", family_id,
                 output_folder,
                 workflow_config["DNA_WF2_NAME"], family_id,
                 bam_command, bai_command,
                 workflow_config["DNA_WF2_BAM"].split(".")[0] + ".sample_name", family_id,
                 workflow_config["DNA_WF2_PED"], ped_file)
    return command

if __name__=="__main__":
    config = read_config("C:\Users\pix1\LIMS\EPP\.idea\SUREKIDS.REFERENCE.CONF.json")
    vcf_file_list = ["vcf1", "vcf2", "vcf3"]
    tbi_file_list = ["tbi1", "tbi2", "tbi3"]
    bai_file_list = ["bai1", "bai2", "bai3"]
    bam_file_list = ["bam1", "bam2", "bam3"]
    print form_dx_command(config, vcf_file_list, tbi_file_list, bai_file_list, bam_file_list)