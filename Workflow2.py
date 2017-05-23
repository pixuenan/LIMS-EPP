################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 11 MAY 2017
##INPUT         :
##DESCRIPTION   : Wrapper script for workflow 2 of SureKids.
################################################
import json
import subprocess
from retrieve_LIMS import RetrieveLIMS, copy_file_from_sftp
import time
from datetime import date
import os
import ConfigParser
import DNAnexus_command
import logging


def read_config(config_json):
    """Read the configuration file"""
    json_data = open(config_json)
    data = json.loads(json_data.read())
    json_data.close()
    workflow_config = data["DNAnexus"]
    return workflow_config


def write_ini(ini_file, info_dict):
    '''
    Write the information of affected sample into a ini file
    :param ini_file: name of the output ini file
    :param info_dict:
    :return:
    '''
    config = ConfigParser.RawConfigParser()
    config.optionxform = str
    config.add_section("Patient_information")
    config.add_section("Client_information")
    config.add_section("Sample_information")
    config.add_section("Test_information")

    config.set("Patient_information", "RUN_DATE", date.today())
    config.set("Patient_information", "Pat_name", info_dict["Pt Name"])
    config.set("Patient_information", "Date_of_Birth", info_dict["Pt D.O.B"])
    config.set("Patient_information", "Gender", info_dict["Pt Gender"])
    config.set("Patient_information", "Patient_ID", info_dict["Patient_ID"])
    config.set("Patient_information", "Race", info_dict["Pt Race"])

    config.set("Client_information", "Hospital", info_dict["Pt Hospital"])
    config.set("Client_information", "Department", info_dict["Pt Dept"])
    config.set("Client_information", "Client", info_dict["Req Client"])
    config.set("Client_information", "Physician", info_dict["Req Physician"])
    config.set("Client_information", "Pathologist", info_dict["Req Pathologist"])
    config.set("Client_information", "Laboratory", info_dict["Req Test Lab"])

    config.set("Sample_information", "Sample_Internal_ID", info_dict["Polaris Sample id"])
    config.set("Sample_information", "Sample_External_ID", info_dict["Pt External ID"])
    config.set("Sample_information", "DNA_Concentration", info_dict["Sample Conc. (ng/uL)"])
    config.set("Sample_information", "Date_time_received", info_dict["Date Submitted"])
    config.set("Sample_information", "Specimen_site", info_dict["Site"])
    config.set("Sample_information", "Specimen_type", info_dict["Sample Type"])
    config.set("Sample_information", "Met_performance_standards", "PASS")

    config.set("Test_information", "Test_ordered", "SureKids")
    config.set("Test_information", "Diagnosis", "")

    ini_content = open(ini_file, 'w+')
    config.write(ini_content)
    ini_content.close()


def group_family(input_dict):
    '''
    Group sample by family ID
    :param input_dict: sample info dictionary {(sample_id: (family_id, pedigree_path, info_dict/None, sample_result_limsid)}
    :return: family_dict: family info dictionary {family_id: [[sample_id_list], pedigree_path, affected_dict]]}
    '''
    family_dict = dict()
    for sample_id, value in input_dict.items():
        if value[0] != "Negative Control":
            family_id, pedigree_path, affect_status, sample_result_limsid = value
            if family_id not in family_dict.keys():
                family_dict[family_id] = [[sample_id], pedigree_path, [affect_status]]
            else:
                family_dict[family_id][0] += [sample_id]
                family_dict[family_id][2] += [affect_status]

    # concatenate the info_dict if there are multiple affected individuals
    for family_id, [[sample_id_list], pedigree_path, affect_status_list] in family_dict.keys():
        affect_dict_list = []
        for affect_status in affect_status_list:
            if affect_status is not None:
                affect_dict_list += affect_status
        if len(affect_dict_list) > 1:
            final_dict = dict()
            for key in affect_dict_list[0].keys():
                final_dict[key] = ",".join([affect_dict[key] for affect_dict in affect_dict_list])
            family_dict[family_id][2] = final_dict
        else:
            family_dict[family_id][2] = affect_dict_list[0]
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
    Make family output folder on DNAnexus
    '''
    family_output_folder = output_folder + family_id + "/"
    if DNAnexus_command.make_folder(family_output_folder) == 0:
        logger.info("Make family folder on DNAnexus at: %s" % family_output_folder)
        return family_output_folder
    else:
        logger.debug("Failed to make family folder on DNAnexus at: %s" % family_output_folder)
        return False


def make_local_download_folder(run_id, pipeline_version, family_id):
    '''
    Make the local folder for downloading all result file from DNAnexus and the pedigree file
    '''
    flowcell = run_id.split("_")[-1][1:]
    cur_date = date.today().strftime("%Y%m%d")[2:]
    destination_folder = "/mnt/seq/polarisbioit/PolarisPool/LIMS_PRD_Ver_%s_%s_%s/" % (pipeline_version, cur_date, flowcell)
    try:
        os.mkdir(destination_folder, 0750)
    except:
        logger.debug("Failed to make local folder to download files at: %s" % destination_folder)
        return False
    else:
        logger.info("Make local folder to download files at: %s" % destination_folder)
        return destination_folder


def main_dx_command(workflow_config, vcf_file_list, tbi_file_list, bam_file_list, bai_file_list, family_output_folder,
                    family_id):
    '''
    Run the main command of workflow2 on DNAnexus
    '''
    vcf_command = form_command_multiple_file(workflow_config, vcf_file_list, "DNA_WF2_VCF")
    tbi_command = form_command_multiple_file(workflow_config, tbi_file_list, "DNA_WF2_TBI")
    bam_command = form_command_multiple_file(workflow_config, bam_file_list, "DNA_WF2_BAM")
    bai_command = form_command_multiple_file(workflow_config, bai_file_list, "DNA_WF2_BAI")
    ped_file = family_output_folder + family_id + ".ped"
    command = "dx run %s %s %s -i%s=%s -y --brief --destination %s -i%s=%s %s %s -i%s=%s -i%s=%s" \
              % (workflow_config["DNA_SK_WORKFLOW"],
                 vcf_command, tbi_command,
                 workflow_config["DNA_WF2_VCF"].split(".")[0] + ".prefix", family_id,
                 family_output_folder,
                 workflow_config["DNA_WF2_NAME"], family_id,
                 bam_command, bai_command,
                 workflow_config["DNA_WF2_BAM"].split(".")[0] + ".sample_name", family_id,
                 workflow_config["DNA_WF2_PED"], ped_file)
    return command


def check_file(sample_id_list, output_folder):
    '''
    Check if all needed files are existed three times
    :return:
    '''
    vcf_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.g.vcf.gz" for sample_id in sample_id_list]
    tbi_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.g.vcf.gz.tbi" for sample_id in sample_id_list]
    bam_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.bam" for sample_id in sample_id_list]
    bai_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.bam.bai" for sample_id in sample_id_list]
    for i in range(3):
        score = 0
        for need_file in vcf_file_list + tbi_file_list + bam_file_list + bai_file_list:
            check_result = DNAnexus_command.check_file(need_file)
            if check_result and check_result == "closed":
                score += 1
        if score < len(sample_id_list) * 4:
            time.sleep(300)
        else:
            logger.info("All sample input files for family member %s are exists on DNAnexus" % ",".join(sample_id_list))
            return vcf_file_list, tbi_file_list, bam_file_list, bai_file_list
    if score < len(sample_id_list) * 4:
        logger.debug("Not all sample input files for family member %s are exists on DNAnexus" % ",".join(sample_id_list))
        return False


def process_ini(family_id, local_folder, info_dict, family_output_folder):
    '''
    Create ini file for affected individuals and upload the file on DNAnexus
    :param family_output_folder: family output folder on DNAnexus
    :param local_folder: local destination folder for downloading
    :return:
    '''
    # download the file from LIMS to local
    local_file_path = local_folder + family_id + ".ini"
    write_ini(local_file_path, info_dict)
    if DNAnexus_command.upload_file(family_output_folder, local_file_path) == 0:
        logger.info("Upload ini file to DNAnexus at: %s" % family_output_folder)
        return family_output_folder + family_id + ".ini"
    else:
        logger.debug("Failed to upload ini file to DNAnexus at: %s" % family_output_folder)
        return False


def process_pedigree(family_id, local_folder, pedigree_path, hostname, username, password, family_output_folder):
    '''
    Download pedigree file from LIMS server and upload the file on DNAnexus
    :param pedigree_path: sftp address of the pedigree_path on LIMS server
    :param hostname: hostname of the LIMS server
    :param local_folder: local destination folder for downloading
    :return:
    '''
    # download the file from LIMS to local
    local_file_path = local_folder + family_id + ".ped"
    copy_file_from_sftp(hostname, local_file_path, pedigree_path, username, password)
    logger.info("Copying file %s from LIMS server to local folder %s" % (pedigree_path, local_file_path))
    if DNAnexus_command.upload_file(family_output_folder, local_file_path) == 0:
        logger.info("Upload pedigree file to DNAnexus at: %s" % family_output_folder)
        return family_output_folder + family_id + ".ped"
    else:
        logger.debug("Failed to upload pedigree file to DNAnexus at: %s" % family_output_folder)
        return False


def update_bioinfo_status(LIMS_object, current_status, sample_result_limsid):
    '''
    Update the sample bioinformatic status
    '''
    sample_uri = LIMS_object.base_uri + "artifacts/" + sample_result_limsid
    LIMS_object.update_udf(sample_uri, "Status", current_status)


def download_file(output_folder, sample_id_list, family_id, destination_folder):
    '''
    Download necessary result file from DNAnexus to PolarisPool
    '''
    # enter the destination folder
    os.chdir(destination_folder)
    # download files from DNAnexus
    bwa_stats_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.bam.BWA.stats"
                           for sample_id in sample_id_list]
    sample_vcf_file_list = [output_folder + sample_id + "/" + sample_id + ".recalibrated.g.vcf.gz"
                            for sample_id in sample_id_list]
    family_need_file_suffix = [".dec.nor.vep_filtered_variants_selected.txt", ".dec.nor.vep_filtered_variants.xlsx",
                               ".dec.nor.vep_filtered_variants_selected.xml",
                               ".dec.nor.vep.html", ".dec.nor.vep.vcf.gz", ".vcf.stats", ".vcf.gz"
                               ".png", "_gene_summary", "_sample_summary", ".dec.nor.vep.vcf.gz"]
    family_need_file = [output_folder + family_id + "/" + family_id + suffix for suffix in family_need_file_suffix]
    if DNAnexus_command.download_batch_file("".join(bwa_stats_file_list + sample_vcf_file_list + family_need_file)) == 0:
        logger.info("Download files from DNAnexus: \n%s" % "\n".join(bwa_stats_file_list + sample_vcf_file_list +
                                                                     family_need_file))
        return True
    else:
        logger.info("Failed to download files from DNAnexus: \n%s" % "\n".join(bwa_stats_file_list +
                                                                               sample_vcf_file_list + family_need_file))
        return False


def workflow2(LIMS_api, input_dict, config_json, run_id, pipeline_version, hostname):
    '''
    :param input_dict: sample info dict
    :param output_DNAnexus: output folder for the SureKids project on DNAnexus
    :param hostname: LIMS server hostname https://......
    '''
    sftp_username = None # glsftp
    sftp_password = None # glsftp password
    work_config = read_config(config_json)
    output_folder = "%s:/SureKids/%s/" % (work_config["DNAnexus"]["DNA_OUTPUT_PROJECT"], run_id)
    logger.info("Output folder on DNAnexus: %s" % output_folder)
    family_dict = group_family(input_dict)

    for family_id, [sample_id_list, pedigree_path, affected_dict] in family_dict.items():
        # check per sample input files from workflow1 and create local and DNAnexus folders, process pedigree and ini files
        logger.info("Check per sample input files for family %s" % family_id)
        need_sample_files = check_file(sample_id_list, output_folder)
        destination_folder = make_local_download_folder(run_id, pipeline_version, family_id)
        family_output_folder = make_family_output_folder(output_folder, family_id)
        pedigree_file = process_pedigree(family_id, destination_folder, pedigree_path, hostname, sftp_username, sftp_password, family_output_folder)
        ini_file = process_ini(family_id, destination_folder, affected_dict, family_output_folder)

        if need_sample_files and destination_folder and family_output_folder and pedigree_file and ini_file:
            # execute dx command for workflow2
            vcf_file_list, tbi_file_list, bam_file_list, bai_file_list = need_sample_files
            command = main_dx_command(work_config, vcf_file_list, tbi_file_list, bam_file_list, bai_file_list, family_output_folder, family_id)
            process = subprocess.Popen(command, shell=True)
            logger.info("Start running pipeline workflow2 for family %s: %s" % (family_id, command))
            [update_bioinfo_status(LIMS_api, "Running workflow2", input_dict[sample_id][3]) for sample_id in sample_id_list]
            family_dict[family_id] += [process, destination_folder]

        else:
            logger.debug("Error: \n " +
                         "Sample files: %s\n Local_folder: %s\n DNAnexus folder: %s\n Pedigree file: %s\n Ini file: %s\n"\
                         % (need_sample_files, destination_folder, family_output_folder, pedigree_file, ini_file))
            [update_bioinfo_status(LIMS_api, "Error workflow2", input_dict[sample_id][3]) for sample_id in sample_id_list]
            family_dict[family_id] += ["Null", destination_folder]

    # download files of the family after the DNAnexus pipeline finished
    for family_id, [sample_id_list, pedigree_path, affected_dict, process, destination_folder] in family_dict.items():
        if process != "Null":
            if process.poll() is None:
                process.wait()
            [update_bioinfo_status(LIMS_api, "Downloading files", input_dict[sample_id][3]) for sample_id in sample_id_list]
            if download_file(output_folder, sample_id_list, family_id, destination_folder):
                [update_bioinfo_status(LIMS_api, "Pipeline finished", input_dict[sample_id][3]) for sample_id in sample_id_list]
                logger.info("Bioinformatics pipeline for family %s finished" % family_id)

if __name__=="__main__":
    # vcf_file_list = ["vcf1", "vcf2", "vcf3"]
    # tbi_file_list = ["tbi1", "tbi2", "tbi3"]
    # bai_file_list = ["bai1", "bai2", "bai3"]
    # bam_file_list = ["bam1", "bam2", "bam3"]
    global logger
    logging.basicConfig(level=logging.DEBUG,
                        filename="test.log",
                        format='%(levelname)s:%(asctime)s %(message)s')
    logger = logging.getLogger()
    # print form_dx_command(config, vcf_file_list, tbi_file_list, bai_file_list, bam_file_list)
    # workflow2(sample_dict, "", config)
    # LIMS_api = RetrieveLIMS("", "", "")
    # LIMS_api.initiate_LIMS_api()
    # input_dict = LIMS_api.get_sample_info(process_limsid)
    # update_bioinfo_status(LIMS_api, "Test1", "92-91651")

