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
from xml.dom.minidom import parseString
import socket
import os
import argparse
import csv


class ScreenCapture(object):
    def __init__(self):
        self.VERSION = "v2"
        self.BASE_URI = ""
        self.hostname = ""
        self.api = None
        self.ARGS = None
        self.criteria_list = []


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
        self.ARGS = parser.parse_args()

    def create_api(self):
        ## create an API object
        self.api = glsapiutil.glsapiutil()
        self.api.setHostname(self.hostname)
        self.api.setVersion(self.VERSION)
        self.api.setup(self.ARGS.username, self.ARGS.password)

    def write_csv(self, filename, row_list):
        with open(filename, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for row in row_list:
                writer.writerow(row)

    def main(self):
        self.get_hostname()
        self.set_BASEURI()
        self.get_arguments()
        self.create_api()
        self.get_criteria()
        self.write_csv("printScreen.csv", zip(*self.criteria_list))

    def get_criteria(self):
        """
        Get the UDF criteria fields
        :return: criteria_list[(criteria_name, criteria_value)]
        """
        pURI = self.BASE_URI + "processes/" + self.ARGS.processLimsId
        # print pURI
        pXML = self.api.getResourceByURI(pURI)
        pDOM = parseString(pXML)
        UDF_fields = pDOM.getElementsByTagName("udf:field")
        for UDF_DOM in UDF_fields:
            criteria_name = UDF_DOM.attributes["name"].value
            criteria_value = UDF_DOM.firstChild.nodeValue
            self.criteria_list += [(criteria_name, criteria_value)]


if __name__=="__main__":
    test = ScreenCapture()
    test.main()





