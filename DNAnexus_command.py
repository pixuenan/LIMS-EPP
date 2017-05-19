################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 19 MAY 2017
##INPUT         :
##DESCRIPTION   : Script for run basic command on DNAnexus.
################################################

import subprocess


def execute(command):
    '''
    Execute command and wait until the command finished
    :return: 1: failed, 0: success
    '''
    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError, e:
        return 1
    else:
        return 0


def download_batch_file(file_path):
    '''
    Download batch of files on DNAnexus to current location
    :param file_path: path of the file on DNAnexus, can be single file or multiple file seperated by whitespace
    :return:
    '''
    download_command = "dx download " + file_path
    return execute(download_command)


def upload_file(DNAnexus_folder, local_file_path):
    '''
    Upload local file to DNAnexus
    :param DNAnexus_folder: folder path includes project istage
    '''
    command = "dx upload --path %s %s" % (DNAnexus_folder, local_file_path)
    return execute(command)


def make_folder(folder_name):
    '''
    :param folder_name: full path of the folder on DNAnexus, includes the project istage
    '''
    command = "dx mkdir -p " + folder_name
    return execute(command)


def check_file(full_file_path):
    '''
    Check if the file exists on DNAnexus or not
    :param full_file_path: full path of the file on DNAnexus includes the project istage
    :return: status of the file (closed) or False
    '''
    file_path = "/".join(full_file_path.split("/")[:-1])
    file_name = full_file_path.split("/")[-1]
    command = "dx find data --path %s --name %s" % (file_path, file_name)
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    result = proc.communicate()[0].strip().split()
    return result and result[0] or False