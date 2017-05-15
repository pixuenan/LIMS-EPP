################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 11 MAY 2017
##INPUT         :
##DESCRIPTION   : Wrapper script for workflow 2 of SureKids.
################################################
import json
import subprocess
import retrieve_LIMS
import time
from datetime import date
import os

def read_config(config_json):
    """Read the configuration file"""
    json_data = open(config_json)
    data = json.loads(json_data.read())
    json_data.close()
    workflow_config = data["DNAnexus"]
    return workflow_config

def group_family(input_dict):
    '''
    Group sample by family ID
    :param input_dict: sample info dictionary {(sample_id: (family_id, sample_uri)}
    :return: family_dict: family info dictionary {family_id: [[sample_id_list], [sample_uri_list]]}
    '''
    family_dict = dict()
    for sample_id, (family_id, sample_uri) in input_dict.items():
        if family_id not in family_dict.keys():
            family_dict[family_id] = [[sample_id], [sample_uri]]
        else:
            family_dict[family_id][0] += [sample_id]
            family_dict[family_id][1] += [sample_uri]
    return family_dict

def form_command_multiple_file(workflow_config, file_list, json_key):
    """Form the part of the command line that may be multiplied by file of family members"""
    command = ""
    for input_file in file_list:
        command += "-i%s=%s " % (workflow_config[json_key], input_file)
    command = command.strip()
    return command

def make_family_output_folder(output_folder, family_id):
    '''
    Make family
    '''
    family_output_folder = output_folder + "/" + family_id + "/"
    command = "dx mkdir -p " + family_output_folder
    subprocess.check_call(command, shell=True)
    return family_output_folder

def run_command_dx(workflow_config, vcf_file_list, tbi_file_list, bai_file_list, bam_file_list):
    '''
    Run the main command of workflow2 on DNAnexus
    '''
    family_id = ""
    output_folder = ""
    ped_file = ""
    family_output_folder = make_family_output_folder(output_folder, family_id)
    vcf_command = form_command_multiple_file(workflow_config, vcf_file_list, "DNA_WF2_VCF")
    tbi_command = form_command_multiple_file(workflow_config, tbi_file_list, "DNA_WF2_TBI")
    bam_command = form_command_multiple_file(workflow_config, bam_file_list, "DNA_WF2_BAM")
    bai_command = form_command_multiple_file(workflow_config, bai_file_list, "DNA_WF2_BAI")
    command = "dx run %s %s %s -i%s=%s -y --brief --destination %s -i%s=%s %s %s -i%s=%s -i%s=%s" \
              % (workflow_config["DNA_SK_WORKFLOW"],
                 vcf_command, tbi_command,
                 workflow_config["DNA_WF2_VCF"].split(".")[0] + ".prefix", family_id,
                 family_output_folder,
                 workflow_config["DNA_WF2_NAME"], family_id,
                 bam_command, bai_command,
                 workflow_config["DNA_WF2_BAM"].split(".")[0] + ".sample_name", family_id,
                 workflow_config["DNA_WF2_PED"], ped_file)
    subprocess.check_call(command, shell=True)

def check_file(sample_id_list, output_folder):
    '''
    Check if all needed files are existed three times
    :return:
    '''
    vcf_file_list = [output_folder + "/" + sample_id + "/" + sample_id + ".recalibrated.g.vcf.gz" for sample_id in sample_id_list]
    tbi_file_list = [output_folder + "/" + sample_id + "/" + sample_id + ".recalibrated.g.vcf.gz.tbi" for sample_id in sample_id_list]
    bam_file_list = [output_folder + "/" + sample_id + "/" + sample_id + ".recalibrated.bam" for sample_id in sample_id_list]
    bai_file_list = [output_folder + "/" + sample_id + "/" + sample_id + ".recalibrated.bam.bai" for sample_id in sample_id_list]
    for i in range(3):
        score_list = []
        for need_file in vcf_file_list, tbi_file_list, bam_file_list, bai_file_list:
            if os.path.isfile(need_file):
                score_list += 1
            else:
                score_list += 0
        if sum(score_list) < len(sample_id_list) * 4:
            time.sleep(300)
            pass
        else:
            return vcf_file_list, tbi_file_list, bam_file_list, bai_file_list
    if sum(score_list) < len(sample_id_list) * 4:
        exit(1)

def retrieve_sample_info(version, username, password, processLIMS_id):
    '''
    Retrieve sample information from LIMS
    :return: dict {sample_id: (family_id, sample_uri)}
    '''
    api, base_uri = retrieve_LIMS.initiate_LIMS_api(version, username, password)
    p_uri = base_uri + "processes/" + processLIMS_id
    p_dom = retrieve_LIMS.get_dom(api, p_uri)

    sample_sheet_uri = retrieve_LIMS.get_sample_sheet(p_dom)
    sample_info_dict = retrieve_LIMS.get_sample_info(api, sample_sheet_uri)
    return sample_info_dict

def download_file(output_folder, sample_id_list, family_id, run_id, pipeline_version):
    '''
    Download file from DNAnexus to PolarisPool
    '''
    flowcell = run_id.split("_")[0][1:]
    cur_date = date.today().strftime("%Y%m%d")[2:]
    destination_folder = "/mnt/seq/polarisbioit/PolarisPool/LIMS_PRD_Ver_%s_%s_%s" % (pipeline_version, cur_date, flowcell)
    # enter the destination folder
    os.mkdir(destination_folder, 0750)
    os.chdir(destination_folder)
    # download files from DNAnexus
    bwa_stats_file_list = [output_folder + "/" + sample_id + "/" + sample_id + ".recalibrated.bam.BWA.stats" for sample_id in sample_id_list]
    xml_file = output_folder + "/" + family_id + "/" + family_id + ".dec.nor.vep_filtered_variants_selected.xml"
    txt_file = output_folder + "/" + family_id + "/" + family_id + ".dec.nor.vep_filtered_variants_selected.txt"
    vep_html = output_folder + "/" + family_id + "/" + family_id + ".dec.nor.vep.html"
    vcf_stats = output_folder + "/" + family_id + "/" + family_id + ".vcf.stats"
    excel_file = output_folder + "/" + family_id + "/" + family_id + ".dec.nor.vep_filtered_variants.xlsx"
    download_command = "dx download %s %s %s %s %s %s" % ("".join(bwa_stats_file_list), xml_file, txt_file, vep_html, vcf_stats, excel_file)
    subprocess.check_call(download_command, shell=True)
    local_xml_path = destination_folder + "/" + xml_file
    return local_xml_path

def generate_report(sample_uri_list, xml_path):
    report_command = "python SureKids_report.py -u %s -x %s" % ("|".join(sample_uri_list), xml_path)
    subprocess.check_call(report_command, shell=True)

def workflow2(input_dict, output_folder, config_json, run_id, pipeline_version):
    input_dict = retrieve_sample_info("", "", "", "")
    family_dict = group_family(input_dict)
    for family_id, [sample_id_list, sample_uri_list] in family_dict.items():
        vcf_file_list, tbi_file_list, bam_file_list, bai_file_list = check_file(sample_id_list, output_folder)
        work_config = read_config(config_json)
        run_command_dx(work_config, vcf_file_list, tbi_file_list, bam_file_list, bai_file_list)
        xml_path = download_file(output_folder, sample_id_list, family_id, run_id, pipeline_version)
        generate_report(sample_uri_list, xml_path)

if __name__=="__main__":
    config = read_config("C:\Users\pix1\LIMS\EPP\.idea\SUREKIDS.REFERENCE.CONF.json")
    vcf_file_list = ["vcf1", "vcf2", "vcf3"]
    tbi_file_list = ["tbi1", "tbi2", "tbi3"]
    bai_file_list = ["bai1", "bai2", "bai3"]
    bam_file_list = ["bam1", "bam2", "bam3"]
    # print form_dx_command(config, vcf_file_list, tbi_file_list, bai_file_list, bam_file_list)
    # workflow2(sample_dict, "", config)