#!/usr/bin/env python3
#pip3 install --upgrade cloudvision

from concurrent.futures import ThreadPoolExecutor
from cloudvision.Connector.grpc_client import GRPCClient, create_query
from cloudvision.Connector.codec import Wildcard, Path
import urllib3
import json
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
debug = 0

def main(key=None, ca=None, certs=None):
    apiserverAddr = 'CVPHOSTHERE'
    token = './token.txt'

    with GRPCClient(apiserverAddr, token=token, key=key,
                    ca=ca, certs=certs) as client:
        #get deviceID to name mappings for active devices
        devToName = getDevToName(client)

        #all ARPS
        perDevArps = getAllDevArps(client, devToName)

        #per device MAC dict
        perDevMacs = getAllDevMacs(client, devToName)


    #GRPC closed, process per device mac into allMacs
    allArps = devToAll(perDevArps)
    allMacs = devToAll(perDevMacs)
    print("Uncomment print statements below to dump ARP/MAC data in JSON")
    #print(json.dumps(allMacs, indent=2))
    #print(json.dumps(allArps, indent=2))

def getAllDevArps(client, devToName):
    ''' Returns per device dict of mac data '''
    perDevArps = {}
    for devId, devName in devToName.items():
        if debug > 2:
            print(f"DeviceID: {devId}")
        arps = getDevArps(client, devId, devName)
        if len(arps):
            perDevArps[devId] = arps
    return perDevArps

def getDevVrfs(client, devId):
    #vrfData = {}
    output = get(client, devId, 'Smash/vrf/vrfIdMapStatus/vrfIdToName'.split('/'))
    #for key, value in output.items():
    #    if value['key'] and value['key']['value']:
            #key: {'value': 1}
            #value: {'key': {'value': 1},
            #        'name': 'MGMT'}
    #        print(value)
    #return vrfData
    #use vrfid 'complex key' instead of iterating and recreating new dict with just ID#
    return output
  
def getDevArps(client, devId, devName):
    ''' Get the ARP Table for a device '''
    perDevArps = {}
    pathElts = [
        "Smash",
        "arp",
        "status",
        Wildcard()
    ]
    vrfData = getDevVrfs(client, devId)
    #sys.exit()
    output = get(client, devId, pathElts)
    for key, value in output.items():
        if 'key' in value and 'addr' in value['key'] and 'ethAddr' in value:
            #print(value.__dict__)
            ipAddr = value['key']['addr']
            if ipAddr not in perDevArps:
                perDevArps[ipAddr] = []
            arpData = {
                'devId': devId,
                'devName': devName,
                'vrf': vrfData[value['key']['vrfId']]['name'],
                'intf': value['key']['intfId'],
                'mac': value['ethAddr']
            }

            perDevArps[ipAddr].append(arpData)
            #print(json.dumps(arpData, indent=2))
            #{'key': {'addr': '192.168.102.65',
            #         'vrfId': {'value': 0},
            #         'intfId': 'Ethernet1/1'},
            #'source': {'value': 1},
            #'ethAddr': 'd4:af:f7:08:ab:25',
            #'isStatic': False
            #}
            #if macAddr not in endpointDict:
            #    endpointDict[macAddr] = copy.deepcopy(emptyEndpoint)
            #endpointDict[macAddr]['ipAddrSet'].add(ipAddr)
    return perDevArps

def getAllDevMacs(client, devToName):
    ''' Returns per device dict of mac data '''
    perDevMacs = {}
    for devId, devName in devToName.items():
        if debug > 2:
            print(f"DeviceID: {devId}")
        macs = getDevMacs(client, devId, devName)
        if len(macs):
            perDevMacs[devId] = macs
    return perDevMacs

def devToAll(perDevData):
    ''' Returns dict of data by key across all devices '''
    allData = {}
    for devId, devData in perDevData.items():
         for ent, entData in devData.items():
             if ent not in allData:
                 allData[ent] = []
             allData[ent].extend(entData)
    return allData

def getDevToName(client):
    ''' Returns dict of active devices containing id to name mappings '''
    devToName = {}
    switches_info = get(client, "analytics", "DatasetInfo/Devices".split('/'))
    for sk, sv in switches_info.items():
        #sv info
        #{
        #"mac": "aa:f7:07:aa:bb:cc",
        #"status": "active",
        #"hostname": "cvx-2",
        #"modelName": "vEOS",
        #"deviceType": "EOS",
        #"domainName": "",
        #"eosVersion": "4.29.1F",
        #"sourceType": "",
        #"capabilities": [
        #  "all"
        #],
        #"isProvisioned": false,
        #"terminAttrVersion": "v1.24.2",
        #"primaryManagementIP": "10.10.111.111"
        #}

        #only add active devices
        if sv['status'] == 'active':
            devToName[sk]=sv['hostname']
            if debug > 3:
                print(f"Added: {sk} => {sv['hostname']}")
    if debug > 1:
        print(f"Devces: (Total: {len(switches_info)}, Active: {len(devToName)})")
    return devToName

def get(client, dataset, pathElts):
    ''' Returns a query on a path element '''
    result = {}
    query = [
        create_query([(pathElts, [])], dataset)
    ]

    for batch in client.get(query):
        for notif in batch['notifications']:
            if debug > 4:
                print(json.dumps(notif['updates'], indent=2))
            result.update(notif['updates'])
    return result

def getDevMacs(client, devId, devName):
    ''' Returns dict of mac addresses w/ list of dicts containing location data '''
    macDict = {}
    output = get(client, devId, 'Smash/bridging/status/smashFdbStatus'.split('/'))
    for key, value in output.items():
        if value['key'] and value['key']['addr']:
            macAddr = value['key']['addr']
            if macAddr not in macDict:
                macDict[macAddr] = []
            macData = {
                'devId': devId,
                'devName': devName,
                'intf': value['intf'],
                'vlan': value['key']['fid']['value'],
                #'moves': value['moves'],
                #'moveTime': value['lastMoveTime']
            }
            macDict[macAddr].append(macData)
            # macData from API
            #{'00:1c:73:00:dc:01':
            #  {'key': {'fid': {'value': 1007},
            #           'addr': '00:1c:73:00:dc:01'},
            #   'intf': 'Router',
            #   'moves': 1,
            #   'dropMode': {'Name': 'dropModeNone', 'Value': 0},
            #    'entryType': {'Name': 'configuredStaticMac', 'Value': 4},
            #    'lastMoveTime': 0.0
    return macDict

if __name__ == "__main__":
    main()
