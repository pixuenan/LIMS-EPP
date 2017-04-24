################################################
##SCRIPT BY     : Xuenan Pi
##CREATED       : 05 APR 2017
##INPUT         :
##DESCRIPTION   : EPP OJT.
################################################

import sys
import xml.dom.minidom
import glsapiutil
import re
import paramiko
from xml.dom.minidom import parseString
import socket
import os
import argparse
import csv
import ftplib


class ScreenCapture(object):
    def __init__(self):
        self.VERSION = "v2"
        self.BASE_URI = ""
        self.hostname = ""
        self.api = None
        self.ARGS = None
        self.criteria_list = []
        self.QC_list = []
        self.art_URI = None
        self.sftp = None
        self.output_filename = 'printScreen_OJT.csv'
        self.local_path = os.getcwd() + '/printScreen_OJT.csv'

    def set_BASEURI(self):
        self.BASE_URI = self.hostname + "/api/" + self.VERSION + "/"

    def get_hostname(self):
        ## retrive host name using UNIX command
        temp = socket.gethostname()
        self.hostname = "http://" + temp + ".gis.a-star.edu.sg:8080"

    def get_arguments(self):
        parser = argparse.ArgumentParser(description="Screen Capture")
        parser.add_argument('-l', '--processLimsId', required=True)
        parser.add_argument('-u', '--username', required=True)
        parser.add_argument('-p', '--password', required=True)
        parser.add_argument('-f', '--fileID', required=True)
        self.ARGS = parser.parse_args()

    def create_api(self):
        ## create an API object
        self.api = glsapiutil.glsapiutil()
        self.api.setHostname(self.hostname)
        self.api.setVersion(self.VERSION)
        self.api.setup(self.ARGS.username, self.ARGS.password)

    def write_csv(self):
        with open(self.output_filename, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for row in zip(*self.criteria_list):
                writer.writerow(row)
            writer.writerow("\n")
            # consider null sample info
            # add all column name together as header
            header = list(set([column_name for sample_dict in self.QC_list for column_name in sample_dict.keys()]))
            writer.writerow(header)
            for sample_dict in self.QC_list:
                writer.writerow([sample_dict.get(name, "Null") for name in header])

    def get_screen(self):
        # get process xml
        pURI = self.BASE_URI + "processes/" + self.ARGS.processLimsId
        pDOM = self.get_dom(pURI)

        self.criteria_list = self.get_UDF(pDOM, self.criteria_list)
        self.get_sample_QC(pDOM)

    def get_dom(self, uri):
        xml = self.api.getResourceByURI(uri)
        dom = parseString(xml)
        return dom

    @staticmethod
    def get_UDF(DOM, target_list):
        """
        Get the UDF criteria fields
        :return: criteria_list[(criteria_name, criteria_value)]
        """
        UDF_fields = DOM.getElementsByTagName("udf:field")
        for UDF_DOM in UDF_fields:
            criteria_name = UDF_DOM.attributes["name"].value
            criteria_value = UDF_DOM.firstChild.nodeValue
            target_list += [(criteria_name, criteria_value)]
        return target_list

    def get_sample_QC(self, pDOM):
        """
        Get the sample QC information
        :return: QC_list[{QC_name: QC_value}]
        """
        # get the sample uri
        artifacts_fields = pDOM.getElementsByTagName("input-output-map")
        for artifact in artifacts_fields:
            output_field = artifact.getElementsByTagName("output")[0]
            if output_field.attributes["output-generation-type"].value == "PerInput":
                sample_uri = output_field.attributes["uri"].value

                # get the sample QC information
                sample_dom = self.get_dom(sample_uri)
                sample_name_uri = sample_dom.getElementsByTagName("sample")[0].attributes["uri"].value
                sample_list = []
                sample_list += [("Sample name", self.get_sample_name(sample_name_uri))]
                sample_list = self.get_UDF(sample_dom, sample_list)
                sample_QC_dict = {}
                for sample_cell in sample_list:
                    sample_QC_dict[sample_cell[0]] = sample_cell[1]
                self.QC_list += [sample_QC_dict]

    def get_sample_name(self, uri):
        dom = self.get_dom(uri)
        sample_name = dom.getElementsByTagName("name")[0].firstChild.nodeValue
        return sample_name

    def create_placeholder(self):
        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<file:file xmlns:file="http://genologics.com/ri/file">'
        xml += '<attached-to>' + self.art_URI + '</attached-to>'
        xml += '<original-location>' + self.local_path + '</original-location>'
        xml += '</file:file>'

        res = self.api.createObject(xml, self.BASE_URI + "glsstorage")
        storeXML = parseString(res)

        contlocTag = storeXML.getElementsByTagName("content-location")
        contloc = self.api.getInnerXml(contlocTag[0].toxml(), "content-location")

        return contloc

    @staticmethod
    def exists(sftp, remote_path):
        try:
            sftp.stat(remote_path)
        except IOError, e:
            if 'No such file' in str(e):
                return False
            raise
        else:
            return True

    def setFile(self, remote_path):
        remote_path = re.sub("sftp://.*?/", "/", remote_path)
        remote_dir = os.path.dirname(os.path.abspath(remote_path))

        hostname = socket.gethostname() + ".gis.a-star.edu.sg"
        transport = paramiko.Transport((hostname, 22))
        transport.connect(username=self.GLSFTP, password=self.GLSFTPPW)
        sftp = paramiko.SFTPClient.from_transport(transport)

        if not self.exists(sftp, remote_dir):
            sftp.mkdir(remote_dir)
        if not self.exists(sftp, remote_path):
            sftp.put(self.local_path, remote_path)
        sftp.close()
        transport.close()

    def attach_file(self):
        self.art_URI = self.BASE_URI + "artifacts/" + self.ARGS.fileID
        contloc = self.create_placeholder()
        self.setFile(contloc)

        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<file:file xmlns:file="http://genologics.com/ri/file">'
        xml += '<content-location>' + contloc + '</content-location>'
        xml += '<attached-to>' + self.art_URI + '</attached-to>'
        xml += '<original-location>' + self.local_path + '</original-location>'
        xml += '</file:file>'

        response = self.api.createObject(xml, self.BASE_URI + "files")
        if not re.search("error", response.lower()):
            print "Attachment Done!"
        else:
            print "Something went wrong!"

        os.remove(self.local_path)

    def main(self):
        self.get_hostname()
        self.set_BASEURI()
        self.get_arguments()
        self.create_api()
        self.get_screen()
        self.write_csv()
        self.attach_file()

if __name__=="__main__":
    test = ScreenCapture()
    test.main()

