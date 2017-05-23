################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 15 MAY 2017
##INPUT         :
##DESCRIPTION   : Script for retrieve information from LIMS.
################################################
import glsapiutil
from xml.dom.minidom import parseString
import re
import paramiko


class RetrieveLIMS(object):
    def __init__(self, hostname, username, password):
        self.version = "v2"
        self.base_uri = ""
        self.hostname = hostname
        self.api = None
        self.username = username
        self.password = password

    def set_base_uri(self):
        self.base_uri = self.hostname + "/api/" + self.version + "/"

    def create_api(self):
        '''
        Create an API object
        '''
        self.api = glsapiutil.glsapiutil2()
        self.api.setHostname(self.hostname)
        self.api.setVersion(self.version)
        self.api.setup(self.username, self.password)

    def get_dom_from_id(self, uri_type, uri_id):
        uri = self.base_uri + uri_type + "/" + uri_id
        dom = self.get_dom_from_uri(uri)
        return dom

    def get_dom_from_uri(self, uri):
        xml = self.api.GET(uri)
        dom = parseString(xml)
        return dom

    def update_udf(self, uri, udf_name, udf_value):
        dom = self.get_dom_from_uri(uri)
        modified_dom = self.api.setUDF(dom, udf_name, udf_value)
        self.api.PUT(modified_dom.toxml(), uri)

    @staticmethod
    def get_sample_result_list(p_dom):
        '''
        Get sample result uri list
        :param p_dom: DOM of the process
        :return:
        '''
        sample_result_limsid_list = []
        artifacts_fields = p_dom.getElementsByTagName("input-output-map")
        for artifact in artifacts_fields:
            output_field = artifact.getElementsByTagName("output")[0]
            if output_field.attributes["output-type"].value == "ResultFile":
                sample_result_limsid = output_field.attributes["limsid"].value
                sample_result_limsid_list += [sample_result_limsid]

        return sample_result_limsid_list

    def get_per_sample_info(self, sample_result_limsid, sample_info_dict):
        sample_result_dom = self.get_dom_from_id("artifacts", sample_result_limsid)
        polaris_sample_id = sample_result_dom.getElementsByTagName("name")[0].firstChild.nodeValue
        sample_info_uri = sample_result_dom.getElementsByTagName("sample")[0].attributes["uri"].value
        sample_info_dict = self.get_per_submitted_sample_info(sample_info_dict, sample_info_uri, polaris_sample_id, sample_result_limsid)
        return sample_info_dict

    def get_file_location(self, file_id):
        f_dom = self.get_dom_from_id("files", file_id)

        file_loc = f_dom.getElementsByTagName("content-location")[0].firstChild.nodeValue
        return file_loc

    @staticmethod
    def udf_info_dict(udf_fields, info_list):
        '''
        Retrieve UDF info from UDF fields
        :param udf_fields: list of udf objects
        :param info_list: list of udf names needed to be retrieved
        :return: info_dict: {needed_udf: value("" if the udf not exists)}
        '''
        info_dict = dict((key, "") for key in info_list)
        for udf_field in udf_fields:
            udf_name = udf_field.attributes["name"].value
            if udf_name in info_dict.keys():
                info_dict[udf_name] = udf_field.firstChild.nodeValue
        return info_dict

    def get_sample_info(self, process_limsid):
        '''
        Get sample info for SureKids project
        :return: dict {sample_id: (family_id, pedigree_path, info_dict/None, sample_result_limsid),
                       polaris_id."Control": ("Negative Control", sample_result_limsid)}
        '''
        sample_info_dict = dict()
        p_dom = self.get_dom_from_id("processes", process_limsid)
        sample_result_limsid_list = self.get_sample_result_list(p_dom)
        for sample_result_limsid in sample_result_limsid_list:
            sample_info_dict = self.get_per_sample_info(sample_result_limsid, sample_info_dict)
        return sample_info_dict

    def get_per_submitted_sample_info(self, sample_info_dict, sample_uri, polaris_sample_id, sample_result_limsid):
        '''
        Get sample information from sample sheet uri.
        Form sample and family id as "external id._polaris id"
        :return: dict {sample_id: (family_id, pedigree_path, info_dict/None, sample_result_limsid, pipeline_status),
                       polaris_id."Control": ("Negative Control", sample_result_limsid)}
        '''
        needed_udf_list = ["Sample External ID", "External Family ID", "Polaris Family ID", "Pedigree Path", "Affected"]
        info_udf_list = ["Pt Name", "Pt D.O.B", "Pt Gender", "Pt Race", "Pt Hospital", "Pt Dept",
                         "Req Client", "Req Physician",  "Req Pathologist", "Req Test Lab",
                         "Sample Conc. (ng/uL)", "Date Submitted", "Site", "Sample Type"]
        sample_dom = self.get_dom_from_uri(sample_uri)
        control = sample_dom.getElementsByTagName("control-type")
        if not control:
            udf_fields = sample_dom.getElementsByTagName("udf:field")
            udf_needed_dict = self.udf_info_dict(udf_fields, needed_udf_list)
            pedigree_path = udf_needed_dict["Pedigree Path"] and self.get_file_location(udf_needed_dict["Pedigree Path"]) or None
            sample_id = udf_needed_dict["Sample External ID"] + "." + polaris_sample_id
            family_id = udf_needed_dict["External Family ID"] + "." + udf_needed_dict["Polaris Family ID"]
            pipeline_status = glsapiutil.glsapiutil2.getUDF(self.get_dom_from_id("artifacts", sample_result_limsid), "Status")

            if udf_needed_dict["Affected"]:
                info_dict = self.udf_info_dict(udf_fields, info_udf_list)
                info_dict["Patient_ID"] = udf_needed_dict["Sample External ID"]
                info_dict["Polaris Sample id"] = polaris_sample_id
                info_dict["Pt External ID"] = udf_needed_dict["Sample External ID"]
                sample_info_dict[sample_id] = (family_id, pedigree_path, info_dict, sample_result_limsid, pipeline_status)
            else:
                sample_info_dict[sample_id] = (family_id, pedigree_path, None, sample_result_limsid, pipeline_status)

        else:
            sample_info_dict[polaris_sample_id+".Control"] = ("Negative Control", sample_result_limsid)
        return sample_info_dict

    def initiate_LIMS_api(self):
        self.create_api()
        self.set_base_uri()


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


def copy_file_from_sftp(hostname, local_file_path, sftp_path, username, password):
    '''
    Copy a file from sftp server to local path
    :param username: username for glsftp
    :param password: password for glsftp
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



