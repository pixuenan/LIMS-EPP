################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 15 MAY 2017
##INPUT         :
##DESCRIPTION   : Script for retrieve information from LIMS.
################################################
import xml.dom.minidom
import glsapiutil
from xml.dom.minidom import parseString
import socket
import re
import paramiko


def set_base_uri(hostname, version):
    base_uri = hostname + "/api/" + version + "/"
    return base_uri


def create_api(hostname, version, username, password):
    '''
    Create an API object
    '''
    api = glsapiutil.glsapiutil()
    api.setHostname(hostname)
    api.setVersion(version)
    api.setup(username, password)
    return api


def get_dom(api, uri):
    xml = api.getResourceByURI(uri)
    dom = parseString(xml)
    return dom


def get_sample_sheet(p_dom):
    '''
    Get sample sheet uri
    :param p_dom: DOM of the process
    :return:
    '''
    artifacts_fields = p_dom.getElementsByTagName("input-output-map")
    for artifact in artifacts_fields:
        output_field = artifact.getElementsByTagName("output")[0]
        if output_field.attributes["output-generation-type"].value == "PerAllInputs":
            input_field = artifact.getElementsByTagName("input")[0]
            sample_sheet_uri = input_field.attributes["post-process-uri"].value
            sample_sheet_uri = sample_sheet_uri.split('?')[0]
            break

    return sample_sheet_uri


def get_file_location(api, base_uri, file_id):
    f_uri = base_uri + "files/" + file_id
    f_dom = get_dom(api, f_uri)

    file_loc = f_dom.getElementsByTagName("content-location")[0].firstChild.nodeValue
    return file_loc


def get_sample_info(api, sample_sheet_uri):
    '''
    Get sample information from sample sheet uri.
    Form sample and family id as "external id_polaris id"
    :return: dict {sample_id: (family_id, pedigree_path)}
    '''
    sample_sheet_dom = get_dom(api, sample_sheet_uri)
    sample_doms = sample_sheet_dom.getElementsByTagName("sample")
    sample_info_dict = dict()
    for sample_dom in sample_doms:
        sample_uri = sample_dom.attributes["uri"].value
        sample_dom = get_dom(api, sample_uri)
        control = sample_dom.getElementsByTagName("control-type")
        if not control:
            polaris_sample_id = sample_dom.getElementsByTagName("name")[0].firstChild.nodeValue
            udf_fields = sample_dom.getElementsByTagName("udf:field")
            for udf_field in udf_fields:
                udf_name = udf_field.attributes["name"].value
                external_sample_id = udf_name == "Sample External ID" and udf_field.firstChild.nodeValue or None
                external_family_id = udf_name == "External Family ID" and udf_field.firstChild.nodeValue or None
                polaris_family_id = udf_name == "Polaris Family ID" and udf_field.firstChild.nodeValue or None
                pedigree_file_id = udf_name == "Pedigree Path" and udf_field.firstChild.nodeValue or None
                pedigree_path = pedigree_file_id and get_file_location(pedigree_file_id) or None
            sample_id = external_sample_id + "." + polaris_sample_id
            family_id = external_family_id + "." + polaris_family_id
            sample_info_dict[sample_id] = (family_id, pedigree_path)
    return sample_info_dict


def initiate_LIMS_api(version, username, password, hostname):
    api = create_api(hostname, version, username, password)
    base_uri = set_base_uri(hostname, version)
    return api, base_uri


def remote_exists(sftp, remote_path):
    '''
    Check if the remote path exists on the sftp server
    '''
    try:
        sftp.stat(remote_path)
    except IOError, e:
        if 'No such file' in str(e):
            return False
        raise
    else:
        return True


def copy_file_from_sftp(local_file_path, sftp_path, hostname, username, password):
    '''
    Copy a file from sftp server to local path
    '''
    # get the remote path on the remote server
    remote_file_path = re.sub("sftp://.*?/", "/", sftp_path)

    # the hostname should not have the https:// part
    hostname = hostname.strip().split("//")[1]
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # copy the file if the remote path exists
    if remote_exists(sftp, remote_file_path):
        sftp.get(remote_file_path, local_file_path)
    sftp.close()
    transport.close()



