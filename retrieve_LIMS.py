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


def set_base_uri(hostname, version):
    base_uri = hostname + "/api/" + version + "/"
    return base_uri


def get_hostname():
    temp = socket.gethostname()
    hostname = "https://" + temp + ".gis.a-star.edu.sg"

    return hostname


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


def get_sample_info(api, sample_sheet_uri):
    '''
    Get sample information from sample sheet uri.
    Form sample and family id as "external id_polaris id"
    :return: dict {sample_id: (family_id, sample_uri)}
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
                if udf_field.attributes["name"].value == "Sample External ID":
                    external_sample_id = udf_field.firstChild.nodeValue
                elif udf_field.attributes["name"].value == "External Family ID":
                    external_family_id = udf_field.firstChild.nodeValue
                elif udf_field.attributes["name"].value == "Polaris Family ID":
                    polaris_family_id = udf_field.firstChild.nodeValue
            sample_id = external_sample_id + "." + polaris_sample_id
            family_id = external_family_id + "." + polaris_family_id
            sample_info_dict[sample_id] = (family_id, sample_uri)
    return sample_info_dict


def initiate_LIMS_api(version, username, password):
    hostname = get_hostname()
    api = create_api(hostname, version, username, password)
    base_uri = set_base_uri(hostname, version)
    return api, base_uri




