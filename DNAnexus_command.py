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


def login(env_source, token):
    '''
    Source the environment variables and login with token
    :param env_source: path to the environment source
    :return: 1: failed, 0: success
    '''
    env_command = "source %s" % env_source
    subprocess.check_call(env_command, shell=True)
    command = "dx login --token %s" % token
    return execute(command)


def dx_exit():
    command = "dx exit"
    return execute(command)


def download_batch_file(file_path):
    '''
    Download batch of files on DNAnexus to current location
    :param file_path: path of the file on DNAnexus, can be single file or multiple file seperated by whitespace
    :return: 1: failed, 0: success
    '''
    download_command = "dx download " + file_path
    return execute(download_command)


def upload_file(DNAnexus_folder, local_file_path):
    '''
    Upload local file to DNAnexus
    :param DNAnexus_folder: folder path includes project istage
    :return: 1: failed, 0: success
    '''
    command = "dx upload --path %s %s" % (DNAnexus_folder, local_file_path)
    return execute(command)


def make_folder(folder_name):
    '''
    If the folder already existed, dx will return as succeed but in fact nothing happened.
    :param folder_name: full path of the folder on DNAnexus, includes the project istage
    :return: 1: failed, 0: success
    '''
    command = "dx mkdir -p " + folder_name
    return execute(command)


def rm_folder(folder_name):
    '''
    :param folder_name: full path of the folder on DNAnexus, includes the project istage
    :return: 1: failed, 0: success
    '''
    command = "dx rm -r " + folder_name
    return execute(command)


def copy_batch_file(ori_file_path, dest_file_folder):
    '''
    Copy batch of file from original file path to one destination folder, the file name will be copied as exactly
    :param ori_file_path: original file path includes project istage and file name
           can be single file or multiple file seperated by whitespace
    :param dest_file_path: destination file folder includes project istage and file name
    :return: 1: failed, 0: success
    '''
    command = "dx cp %s %s" % (ori_file_path, dest_file_folder)
    return execute(command)


def check_file(full_file_path):
    '''
    Check if the file exists on DNAnexus or not
    :param full_file_path: full path of the file on DNAnexus includes the project istage
    :return: status of the file True or False
    '''
    file_path = "/".join(full_file_path.split("/")[:-1])
    file_name = full_file_path.split("/")[-1]
    command = "dx find data --path %s --name %s" % (file_path, file_name)
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    result = proc.communicate()[0].strip().split()
    if result:
        return result[0] == "closed"
    else:
        return False
