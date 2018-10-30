#!/usr/local/bin/python3

import os, getopt
import requests
import json
import sys

class UdnsClient:

    generate_token_url = 'https://os-identity.vip.ebayc3.com/v2.0/tokens'
    create_cname_url = 'https://udns.vip.ebay.com/dnsv2/views/ebay-cloud/zones/stratus.%s.ebay.com'
    query_cname_url = 'https://udns.vip.ebay.com/dnsv2/views/ebay-cloud/zones/stratus.%s.ebay.com/recordtypes/cname/records/%s'

    # ctor client with zone info: lvs, phx or slc
    def __init__(self, zone):
        self.zone = zone
        self.token_id = None

    # call token API to generate token that is to be used for authentication
    def generateToken(self):
        print('generating token...')
        with open('credentials.json', 'r') as fp_read:
            credentials = fp_read.read()
            response = requests.post(self.generate_token_url, data=credentials, headers={'Content-Type':'application/json','Accept':'application/json'})
            if response.status_code == 200:
                result = response.json()
                token = result['access']['token']
                self.token_id = token['id']

    '''
    data is a json string like below:
    {
        "records": [
            {
                "resourceType": "CNAME",
                "records": [
                    {
                        "recordName": "choco-es1",
                        "ttl": 0,
                        "canonicalName": "lvschocolatepits-1448900.stratus.lvs.ebay.com"
                    }
                ]
            }
        ]
    }
    '''
    def createCname(self, json_data):
        print('creating cname...')
        print(json_data)
        url = self.create_cname_url%self.zone
        response = requests.post(url, data=json_data, headers={'X-Auth-Token':self.token_id,'Content-Type':'application/json'}, verify=False)
        if response.status_code == 200:
            result = response.json()
            print('result: ', result)
        else:
            print(response)

    def queryCname(self, cname):
        print('querying cname %s...'%cname)
        url = self.query_cname_url%(self.zone,cname)
        response = requests.get(url, headers={'X-Auth-Token':self.token_id,'Content-Type':'application/json'}, verify=False)
        if response.status_code == 200:
            result = response.json()
            print('result: ', result)
        else:
            print(response)

def printHelp():
    print('udns.py -z [zone] -f [file] -c [cname]')
    print('zone: lvs, phx, slc')
    print('file: contains cname record that is going to be created')
    print('cname: cname to query')

def main(argv):
    zone = ''
    file = ''
    cname = ''
    try:
        opts, args = getopt.getopt(argv, 'z:f:c:')
    except getopt.GetoptError:
        printHelp()
        exit(2)

    for opt, arg in opts:
        if opt == '-z':
            zone = arg
        elif opt == '-f':
            file = arg
        elif opt == '-c':
            cname = arg

    if zone == '' and (file == '' or cname == ''):
        printHelp()
        exit(2)

    if 'lvs' == zone or 'phx' == zone or 'slc' == zone:
        client = UdnsClient(zone)
        client.generateToken()

        if file != '':
            with open(file, 'r') as fp:
                json_data = fp.read()
                client.createCname(json_data)
        elif cname != '':
            client.queryCname(cname)
    else:
        print('unknown zone %s'%zone)


if __name__ == '__main__':
    main(sys.argv[1:])