#!/usr/bin/env python
# -*- coding: utf-8; -*-
#
# (c) 2016 siveo, http://www.siveo.net
#
# This file is part of Pulse 2, http://www.siveo.net
#
# Pulse 2 is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Pulse 2 is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Pulse 2; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.

import netifaces
import json
import subprocess
import threading
import sys
import os
import logging
import random
import re
import traceback
from pprint import pprint
import hashlib
import base64
import urllib
import urllib2
import pickle
from  agentconffile import conffilename
import ConfigParser
DEBUGPULSE=25


if sys.platform.startswith('win'):
    import wmi
    import pythoncom
    import _winreg as wr
    import win32net
    import win32netcon
    import socket


def Setdirectorytempinfo():
    """
    function Setdirectorytempinfo
    : return path directory INFO Temporaly and key RSA
    """
    dirtempinfo = os.path.join(os.path.dirname(os.path.realpath(__file__)), "INFOSTMP")
    if not os.path.exists(dirtempinfo):
        os.makedirs(dirtempinfo, mode=0007)
    return dirtempinfo

def cleanbacktodeploy(objectxmpp):
    delsession = [session for session in objectxmpp.back_to_deploy  if not objectxmpp.session.isexist(session)]
    for session in delsession:
        del (objectxmpp.back_to_deploy[session])
    if len(delsession) != 0:
        logging.log(DEBUGPULSE,"Clear dependency : %s"%delsession)
        save_back_to_deploy(objectxmpp.back_to_deploy)

def networkinfoexist():
    filenetworkinfo = os.path.join(Setdirectorytempinfo(), 'fingerprintnetwork')
    if os.path.exists(filenetworkinfo):
        return True
    return False

def save_back_to_deploy(obj):
    fileback_to_deploy = os.path.join(Setdirectorytempinfo(), 'back_to_deploy')
    save_obj(obj, fileback_to_deploy )

def load_back_to_deploy():
    fileback_to_deploy = os.path.join(Setdirectorytempinfo(), 'back_to_deploy')
    return load_obj( fileback_to_deploy )

def listback_to_deploy(objectxmpp):
    if len(objectxmpp.back_to_deploy)!= 0:
        print "list session pris en compte back_to_deploy"
        for u in objectxmpp.back_to_deploy:
            print u

def testagentconf(typeconf):
    if typeconf == "relayserver":
        return True
    Config = ConfigParser.ConfigParser()
    namefileconfig = conffilename(typeconf)
    Config.read(namefileconfig)
    if Config.has_option("type", "guacamole_baseurl")\
                            and Config.has_option('connection', 'port')\
                            and Config.has_option('connection', 'server')\
                            and Config.has_option('global', 'relayserver_agent')\
                            and Config.get('type', 'guacamole_baseurl') != ""\
                            and Config.get('connection', 'port') != ""\
                            and Config.get('connection', 'server') != ""\
                            and Config.get('global', 'relayserver_agent') != "":
        return True
    return False

def createfingerprintnetwork():
    md5network = ""
    if sys.platform.startswith('win'):
        obj = simplecommandstr("ipconfig")
        md5network = hashlib.md5(obj['result']).hexdigest()
    elif sys.platform.startswith('linux'):
        obj = simplecommandstr("LANG=C ifconfig | egrep '.*(inet|HWaddr).*'")
        md5network = hashlib.md5(obj['result']).hexdigest()
    elif sys.platform.startswith('darwin'):
        obj = simplecommandstr("ipconfig")
        md5network = hashlib.md5(obj['result']).hexdigest()
    return md5network

def createfingerprintconf(typeconf):
    namefileconfig = conffilename(typeconf)
    return hashlib.md5(file_get_contents(namefileconfig)).hexdigest()


def confinfoexist():
    filenetworkinfo = os.path.join(Setdirectorytempinfo(), 'fingerprintconf')
    if os.path.exists(filenetworkinfo):
        return True
    return False

def confchanged(typeconf):
    if confinfoexist():
        fingerprintconf = file_get_contents(os.path.join(Setdirectorytempinfo(), 'fingerprintconf'))
        newfingerprintconf = createfingerprintconf(typeconf)
        if newfingerprintconf == fingerprintconf:
            return False
    return True

def refreshfingerprintconf(typeconf):
    fp =  createfingerprintconf(typeconf)
    file_put_contents(os.path.join(Setdirectorytempinfo(), 'fingerprintconf'), fp )
    return fp

def networkchanged():
    if networkinfoexist():
        fingerprintnetwork = file_get_contents(os.path.join(Setdirectorytempinfo(), 'fingerprintnetwork'))
        newfingerprint = createfingerprintnetwork()
        if fingerprintnetwork == newfingerprint:
            return False
    else:
        return True

def refreshfingerprint():
    fp =  createfingerprintnetwork()
    file_put_contents(os.path.join(Setdirectorytempinfo(), 'fingerprintnetwork'), fp )
    return fp

def file_get_contents(filename, use_include_path = 0, context = None, offset = -1, maxlen = -1):
    if (filename.find('://') > 0):
        ret = urllib2.urlopen(filename).read()
        if (offset > 0):
            ret = ret[offset:]
        if (maxlen > 0):
            ret = ret[:maxlen]
        return ret
    else:
        fp = open(filename,'rb')
        try:
            if (offset > 0):
                fp.seek(offset)
            ret = fp.read(maxlen)
            return ret
        finally:
            fp.close( )

def file_put_contents(filename,  data):
    f = open(filename, 'w')
    f.write(data)
    f.close()

def save_obj(obj, name ):
    """
    funct save serialised object
    """
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj( name ):
    """
    function load serialized object
    """
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def getCurrentWorkingDirectory():
    return os.path.abspath(os.getcwd())

def getScriptPath():
    return os.path.abspath(os.path.join(getCurrentWorkingDirectory(),"script"))

def getPluginsPath():
    return os.path.abspath(os.path.join(getCurrentWorkingDirectory(),"plugins"))

def getLibPath():
    return os.path.abspath(os.path.join(getCurrentWorkingDirectory(),"lib"))

def getPerlScriptPath(name):
    return os.path.abspath(os.path.join(getCurrentWorkingDirectory(),"script","perl",name))

def showJSONData(jsondata):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(jsondata)

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, debug=logging.INFO):
        self.logger = logger
        self.debug = debug
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.debug, line.rstrip())

#windows
def get_connection_name_from_guid(iface_guids):
    iface_names = ['(unknown)' for i in range(len(iface_guids))]
    reg = wr.ConnectRegistry(None, wr.HKEY_LOCAL_MACHINE)
    reg_key = wr.OpenKey(reg, r'SYSTEM\CurrentControlSet\Control\Network\{4d36e972-e325-11ce-bfc1-08002be10318}')
    for i in range(len(iface_guids)):
        try:
            reg_subkey = wr.OpenKey(reg_key, iface_guids[i] + r'\Connection')
            iface_names[i] = wr.QueryValueEx(reg_subkey, 'Name')[0]
        except :
            pass
    return iface_names

def CreateWinUser(login,Password,Groups=['Users']):
    """
    We check if the user exists
    """
    try:
        d = win32net.NetUserGetInfo(None,login, 1)
        return
    except:
        pass
    d = {}
    d['name'] = login
    d['password'] = Password
    d['comment'] = ''
    d['flags'] = win32netcon.UF_NORMAL_ACCOUNT | win32netcon.UF_SCRIPT | win32netcon.UF_PASSWD_CANT_CHANGE | win32netcon.UF_DONT_EXPIRE_PASSWD
    d['priv'] = win32netcon.USER_PRIV_USER
    win32net.NetUserAdd(None, 1, d)
    domain = win32api.GetDomainName()
    d = [{"domainandname" : domain+"\\"+login}]
    for gr in Groups:
        win32net.NetLocalGroupAddMembers(None, gr, 3, d)


def create_Win_user(username, password, full_name=None, comment=None):
    """
    Create a system user account for Rattail.
    """
    try:
        win32net.NetUserGetInfo(None, username, 1)
        return
    except:
        pass
    if not full_name:
        full_name = "{0} User".format(username.capitalize())
    if not comment:
        comment = "System user account for Rattail applications"
    win32net.NetUserAdd(None, 2, {
            'name': username,
            'password': password,
            'priv': win32netcon.USER_PRIV_USER,
            'comment': comment,
            'flags': (win32netcon.UF_NORMAL_ACCOUNT
                      | win32netcon.UF_PASSWD_CANT_CHANGE
                      | win32netcon.UF_DONT_EXPIRE_PASSWD),
            'full_name': full_name,
            'acct_expires': win32netcon.TIMEQ_FOREVER,
            })

    win32net.NetLocalGroupAddMembers(None, 'Users', 3, [
            {'domainandname': r'{0}\{1}'.format(socket.gethostname(), username)}])

    hide_user_account(username)
    return True

def isWinUserAdmin():
    if os.name == 'nt':
        import ctypes
        # WARNING: requires Windows XP SP2 or higher!
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            traceback.print_exc()
            print "Admin check failed, assuming not an admin."
            return False
    elif os.name == 'posix':
        # Check for root on Posix
        return os.getuid() == 0
    else:
        raise RuntimeError, "Unsupported operating system for this module: %s" % (os.name,)

def isMacOsUserAdmin():
    obj=simplecommand("cat /etc/master.passwd")#pour linux "cat /etc/shadow")
    if int(obj['code']) == 0:
        return True
    else:
        return False



#listplugins = ['.'.join(fn.split('.')[:-1]) for fn in os.listdir(getPluginsPath) if fn.endswith(".py") and fn != "__init__.py"]
def getRandomName(nb, pref=""):
    a="abcdefghijklnmopqrstuvwxyz0123456789"
    d=pref
    for t in range(nb):
        d=d+a[random.randint(0,35)]
    return d

def md5(fname):
    hash = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash.hexdigest()

def load_plugin(name):
    mod = __import__("plugin_%s" % name)
    return mod


def call_plugin(name, *args, **kwargs):
    pluginaction = load_plugin(name)
    pluginaction.action(*args, **kwargs)

def getIpListreduite():
    listmacadress={}
    for i in netifaces.interfaces():
        addrs = netifaces.ifaddresses(i)
        try:
            if_mac = reduction_mac(addrs[netifaces.AF_LINK][0]['addr'])
            address = int(if_mac,16)
            if address != 0:
                listmacadress[address]= if_mac
        except :
            pass
    return listmacadress


def getMacAdressList():
    listmacadress = []
    for interfacenet in netifaces.interfaces():
        try:
            macadress = netifaces.ifaddresses(interfacenet)[netifaces.AF_LINK][0]['addr']
            if macadress != "00:00:00:00:00:00":
                listmacadress.append(macadress)
        except:
            pass
    return listmacadress

def getIPAdressList():
    ip_list = []
    for interface in netifaces.interfaces():
        try:
            for link in netifaces.ifaddresses(interface)[netifaces.AF_INET]:
                if link['addr'] != '127.0.0.1':
                    ip_list.append(link['addr'])
        except:
            pass
    return ip_list

def MacAdressToIp(ip):
    'Returns a MAC for interfaces that have given IP, returns None if not found'
    for i in netifaces.interfaces():
        addrs = netifaces.ifaddresses(i)
        try:
            if_mac = addrs[netifaces.AF_LINK][0]['addr']
            if_ip = addrs[netifaces.AF_INET][0]['addr']
        except:# IndexError, KeyError: #ignore ifaces that dont have MAC or IP
            if_mac = if_ip = None
        if if_ip == ip:
            return if_mac
    return None

def name_jid():
    dd = getIpListreduite()
    cc = dd.keys()
    cc.sort()
    return dd[cc[0]]

def reduction_mac(mac):
    mac=mac.lower()
    mac = mac.replace(":","")
    mac = mac.replace("-","")
    mac = mac.replace(" ","")
    #mac = mac.replace("/","")
    return mac

def is_valid_ipv4(ip):
    """Validates IPv4 addresses.
    """
    pattern = re.compile(r"""
        ^
        (?:
          # Dotted variants:
          (?:
            # Decimal 1-255 (no leading 0's)
            [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
          |
            0x0*[0-9a-f]{1,2}  # Hexadecimal 0x0 - 0xFF (possible leading 0's)
          |
            0+[1-3]?[0-7]{0,2} # Octal 0 - 0377 (possible leading 0's)
          )
          (?:                  # Repeat 0-3 times, separated by a dot
            \.
            (?:
              [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
            |
              0x0*[0-9a-f]{1,2}
            |
              0+[1-3]?[0-7]{0,2}
            )
          ){0,3}
        |
          0x0*[0-9a-f]{1,8}    # Hexadecimal notation, 0x0 - 0xffffffff
        |
          0+[0-3]?[0-7]{0,10}  # Octal notation, 0 - 037777777777
        |
          # Decimal notation, 1-4294967295:
          429496729[0-5]|42949672[0-8]\d|4294967[01]\d\d|429496[0-6]\d{3}|
          42949[0-5]\d{4}|4294[0-8]\d{5}|429[0-3]\d{6}|42[0-8]\d{7}|
          4[01]\d{8}|[1-3]\d{0,9}|[4-9]\d{0,8}
        )
        $
    """, re.VERBOSE | re.IGNORECASE)
    return pattern.match(ip) is not None

def is_valid_ipv6(ip):
    """Validates IPv6 addresses.
    """
    pattern = re.compile(r"""
        ^
        \s*                         # Leading whitespace
        (?!.*::.*::)                # Only a single whildcard allowed
        (?:(?!:)|:(?=:))            # Colon iff it would be part of a wildcard
        (?:                         # Repeat 6 times:
            [0-9a-f]{0,4}           #   A group of at most four hexadecimal digits
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
        ){6}                        #
        (?:                         # Either
            [0-9a-f]{0,4}           #   Another group
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
            [0-9a-f]{0,4}           #   Last group
            (?: (?<=::)             #   Colon iff preceeded by exacly one colon
             |  (?<!:)              #
             |  (?<=:) (?<!::) :    #
             )                      # OR
         |                          #   A v4 address with NO leading zeros
            (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            (?: \.
                (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            ){3}
        )
        \s*                         # Trailing whitespace
        $
    """, re.VERBOSE | re.IGNORECASE | re.DOTALL)
    return pattern.match(ip) is not None



#linux systemd ou init
def typelinux():
    p = subprocess.Popen('cat /proc/1/comm',
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    result = p.stdout.readlines()
    code_result= p.wait()
    system=result[0].rstrip('\n')
    """renvoi la liste des ip gateway en fonction de l'interface linux"""
    return system

def isprogramme(name):
    obj={}
    p = subprocess.Popen("which %s"%(name),
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    result = p.stdout.readlines()
    obj['code']=p.wait()
    obj['result']=result
    #print obj['code']
    #print obj['result']
    #print obj
    if obj['result'] != "":
        return True
    else:
        return False

def simplecommand(cmd):
    obj={}
    p = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    result = p.stdout.readlines()
    obj['code']=p.wait()
    obj['result']=result
    return obj

def simplecommandstr(cmd):
    obj={}
    p = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    result = p.stdout.readlines()
    obj['code']=p.wait()
    obj['result']="\n".join(result)
    return obj



class shellcommandtimeout(object):
    def __init__(self, cmd, timeout=15):
        self.process = None
        self.obj = {}
        self.obj['timeout'] = timeout
        self.obj['cmd'] = cmd


    def run(self):
        def target():
            #print 'Thread started'
            self.process = subprocess.Popen(self.obj['cmd'], 
                                            shell=True,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT)
            self.obj['result'] = self.process.stdout.readlines()
            self.process.communicate()
            #print 'Thread finished'
        thread = threading.Thread(target=target)
        thread.start()

        thread.join(self.obj['timeout'])
        if thread.is_alive():
            print 'Terminating process'
            print "timeout %s"%self.obj['timeout']
            #self.codereturn = -255
            #self.result = "error tineour"
            self.process.terminate()
            thread.join()

        #self.result = self.process.stdout.readlines()
        self.obj['codereturn'] = self.process.returncode

        if self.obj['codereturn']==-15:
                self.result = "error tineout"

        return self.obj


def servicelinuxinit(name,action):
    obj={}
    p = subprocess.Popen("/etc/init.d/%s %s"%(name,action),
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    result = p.stdout.readlines()
    obj['code']=p.wait()
    obj['result']=result
    return obj

#restart service
def service(name, action): #start | stop | restart | reload
    obj={}
    if sys.platform.startswith('linux'):
        system=""
        p = subprocess.Popen('cat /proc/1/comm',
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
        result = p.stdout.readlines()
        code_result = p.wait()
        system = result[0].rstrip('\n')
        if system == "init":
            p = subprocess.Popen("/etc/init.d/%s %s"%(name,action),
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
            result = p.stdout.readlines()
            obj['code']=p.wait()
            obj['result']=result
        elif system == "systemd":
            p = subprocess.Popen("systemctl %s %s"%(action, name),
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
            result = p.stdout.readlines()
            obj['code']=p.wait()
            obj['result']=result
    elif sys.platform.startswith('win'):
        pythoncom.CoInitialize ()
        try:
            wmi_obj = wmi.WMI()
            wmi_sql = "select * from Win32_Service Where Name ='%s'"%name
            wmi_out = wmi_obj.query( wmi_sql )
        finally:
            pythoncom.CoUninitialize ()
        for dev in wmi_out:
            print dev.Caption
        pass
    elif sys.platform.startswith('darwin'):
        pass
    return obj


def listservice():
    pythoncom.CoInitialize ()
    try:
        wmi_obj = wmi.WMI()
        wmi_sql = "select * from Win32_Service"# Where Name ='Alerter'"
        wmi_out = wmi_obj.query( wmi_sql )
    finally:
        pythoncom.CoUninitialize ()
    for dev in wmi_out:
        print dev.Caption
        print dev.DisplayName

def joint_compteAD(domain,password,login,group):
    ##https://msdn.microsoft.com/en-us/library/windows/desktop/aa392154%28v=vs.85%29.aspx
    pythoncom.CoInitialize ()
    try:
        c = wmi.WMI()
        for computer in c.Win32_ComputerSystem():
            if computer.PartOfDomain:
                print computer.Domain #DOMCD
                print computer.SystemStartupOptions
                computer.JoinDomainOrWorkGroup(domain,password,login,group,3  )
    finally:
        pythoncom.CoUninitialize ()

def windowsservice(name, action):
    pythoncom.CoInitialize ()
    try:
        wmi_obj = wmi.WMI()
        wmi_sql = "select * from Win32_Service Where Name ='%s'"%name
        print wmi_sql
        wmi_out = wmi_obj.query( wmi_sql )
    finally:
      pythoncom.CoUninitialize ()
    print len(wmi_out)
    for dev in wmi_out:
        print dev.caption
        if action.lower() == "start":
            dev.StartService()
        elif action.lower() == "stop":
            print dev.Name
            dev.StopService()
        elif action.lower() == "restart":
            dev.StopService()
            dev.StartService()
        else:
            pass


def methodservice():
    import pythoncom
    import wmi
    pythoncom.CoInitialize ()
    try:
        c = wmi.WMI ()
        for method in c.Win32_Service._methods:
            print method
    finally:
        pythoncom.CoUninitialize ()

def file_get_content(path):
    inputFile = open(path, 'r')     #Open test.txt file in read mode
    content = inputFile.read()
    inputFile.close()
    return content

def file_put_content(filename, contents,mode="w"):
    fh = open(filename, mode)
    fh.write(contents)
    fh.close()

##windows
#def listusergroup():
    #import wmi
    #c = wmi.WMI()
    #for group in c.Win32_Group():
    #print group.Caption
    #for user in group.associators("Win32_GroupUser"):
        #print "  ", user.Caption

#decorateur pour simplifier les plugins
def pluginprocess(func):
    def wrapper( objetxmpp, action, sessionid, data, message, dataerreur):
        resultaction = "result%s"%action
        result={}
        result['action'] = resultaction
        result['ret'] = 0
        result['sessionid'] = sessionid
        result['base64'] = False
        result['data'] = {}
        dataerreur['action']=resultaction
        dataerreur['data']['msg'] = "ERROR : %s"%action
        dataerreur['sessionid'] = sessionid
        try:
            response = func( objetxmpp, action, sessionid, data, message, dataerreur, result)
            #encode  result['data'] si besoin
            #print result
            if result['base64'] == True:
                result['data'] = base64.b64encode(json.dumps(result['data']))
            print "Send message \n%s"%result
            objetxmpp.send_message( mto = message['from'],
                                    mbody = json.dumps(result),
                                    mtype = 'chat')
        except:
            print "Send error message\n%s"%dataerreur
            objetxmpp.send_message( mto=message['from'],
                                    mbody=json.dumps(dataerreur),
                                    mtype='chat')
            return
        return response
    return wrapper


#decorateur pour simplifier les plugins
def pulgindeploy(func):
    def wrapper( objetxmpp, action, sessionid, data, message, dataerreur):
        resultaction = action
        result={}
        result['action'] = resultaction
        result['ret'] = 0
        result['sessionid'] = sessionid
        result['base64'] = False
        result['data'] = {}
        dataerreur['action']=resultaction
        dataerreur['data']['msg'] = "ERROR : %s"%action
        dataerreur['sessionid'] = sessionid
        try:
            response = func( objetxmpp, action, sessionid, data, message, dataerreur, result)
            if result['data'] != "end":
                if result['base64'] == True:
                    result['data'] = base64.b64encode(json.dumps(result['data']))
                objetxmpp.send_message( mto = message['from'],
                                        mbody = json.dumps(result),
                                        mtype = 'chat')
        except:
            if result['data'] != "end":
                objetxmpp.send_message( mto=message['from'],
                                        mbody=json.dumps(dataerreur),
                                        mtype='chat')
            return
        return response
    return wrapper

#decorateur pour simplifier les plugins
def pulgindeploy1(func):
    def wrapper( objetxmpp, action, sessionid, data, message, dataerreur):
        result={}
        result['action'] = action
        result['ret'] = 0
        result['sessionid'] = sessionid
        result['base64'] = False
        result['data'] = {}
        dataerreur['action']=action
        dataerreur['data']['msg'] = "ERROR : %s"%action
        dataerreur['sessionid'] = sessionid
        try:
            response = func( objetxmpp, action, sessionid, data, message, dataerreur, result)

            if not 'end' in result['data']:
               result['data']['end'] = False

            print "----------------------------------------------------------------"
            print "sent message to %s "%message['from']
            if "Devent" in data:
                print "Devent : %s"%data["Devent"]
            if "Dtypequery" in data:
                print "Dtypequery : %s"%data["Dtypequery"]
            if "Deventindex" in data:
                print "Deventindex : %s"%data["Deventindex"]

            if not result['data']['end']:
                print "Envoi Message"
                print "result",result
                if result['base64'] == True:
                    result['data'] = base64.b64encode(json.dumps(result['data']))
                objetxmpp.send_message( mto = message['from'],
                                        mbody = json.dumps(result),
                                        mtype = 'chat')
            else:
                print "envoi pas de message"
        except:
            if not result['data']['end']:
                print "Send error message"
                print "result",dataerreur
                objetxmpp.send_message( mto=message['from'],
                                        mbody=json.dumps(dataerreur),
                                        mtype='chat')
            else:
                print "Envoi pas de Message erreur"
            return
        print "---------------------------------------------------------------"
        return response
    return wrapper

#determine address ip utiliser pour xmpp
def getIpXmppInterface(ipadress1, Port):
    resultip =  ''
    ipadress = ipfromdns(ipadress1)
    if sys.platform.startswith('linux'):
        logging.log(DEBUGPULSE,"Searching for the XMPP Server IP Adress")
        print "netstat -an |grep %s |grep %s| grep ESTABLISHED | grep -v tcp6"%(Port, ipadress)
        obj = simplecommand("netstat -an |grep %s |grep %s| grep ESTABLISHED | grep -v tcp6"%(Port,ipadress))
        logging.log(DEBUGPULSE,"netstat -an |grep %s |grep %s| grep ESTABLISHED | grep -v tcp6"%(Port,ipadress))
        if len(obj['result']) != 0:
            for i in range(len(obj['result'])):
                obj['result'][i]=obj['result'][i].rstrip('\n')
            a = "\n".join(obj['result'])
            b = [ x for x in a.split(' ') if x!=""]
            if len(b) != 0:
                resultip =  b[3].split(':')[0]
    elif sys.platform.startswith('win'):
        obj = simplecommand("netstat -an | findstr %s | findstr ESTABLISHED"%Port)
        if len(obj['result']) != 0:
            for i in range(len(obj['result'])):
                obj['result'][i]=obj['result'][i].rstrip('\n')
            a = "\n".join(obj['result'])
            b = [ x for x in a.split(' ') if x!=""]

            if len(b) != 0:
                resultip = b[1].split(':')[0]
    else:
        obj = simplecommand("netstat -a | grep %s | grep ESTABLISHED"%Port)
        if len(obj['result']) != 0:
            for i in range(len(obj['result'])):
                obj['result'][i]=obj['result'][i].rstrip('\n')
            a = "\n".join(obj['result'])
            b = [ x for x in a.split(' ') if x!=""]
            if len(b) != 0:
                resultip =  b[1].split(':')[0]
    return resultip

# 3 functions used for subnet network
def ipV4toDecimal(ipv4):
    d = ipv4.split('.')
    return (int(d[0])*256*256*256) + (int(d[1])*256*256) + (int(d[2])*256) +int(d[3])

def decimaltoIpV4(ipdecimal):
    a=float(ipdecimal)/(256*256*256)
    b = (a - int(a))*256
    c = (b - int(b))*256
    d = (c - int(c))*256
    return "%s.%s.%s.%s"%(int(a),int(b),int(c),int(d))

def subnetnetwork(adressmachine, mask):
    adressmachine = adressmachine.split(":")[0]
    reseaumachine = ipV4toDecimal(adressmachine) &  ipV4toDecimal(mask)
    return decimaltoIpV4(reseaumachine)

def searchippublic(site = 1):
    return None
    try:
        if site == 1:
            try:
                page = urllib.urlopen("http://ifconfig.co/json").read()
                objip = json.loads(page)
                return objip['ip']
            except:
                return searchippublic(2)
        else:
            page = urllib.urlopen("http://www.monip.org/").read()
            ip = page.split("IP : ")[1].split("<br>")[0]
            return ip
    except:
        return None


# decorateur pour simplifier les plugins
# verify session exist.
# pas de session end
def pulginmaster(func):
    def wrapper( objetxmpp, action, sessionid, data, message, ret ):
        if action.startswith("result"):
            action = action[:6]
        if objetxmpp.session.isexist(sessionid):
            objsessiondata = objetxmpp.session.sessionfromsessiondata(sessionid)
        else:
            objsessiondata = None
        response = func( objetxmpp, action, sessionid, data, message, ret, objsessiondata)
        return response
    return wrapper

def pulginmastersessionaction( sessionaction, timeminute = 10 ):
    def decorateur(func):
        def wrapper(objetxmpp, action, sessionid, data, message, ret, dataobj):
            #avant
            if action.startswith("result"):
                action = action[6:]
            if objetxmpp.session.isexist(sessionid):
                if sessionaction == "actualise":
                    objetxmpp.session.reactualisesession(sessionid, 10)
                objsessiondata = objetxmpp.session.sessionfromsessiondata(sessionid)
            else:
                objsessiondata = None
            response = func( objetxmpp, action, sessionid, data, message, ret, dataobj, objsessiondata)
            if sessionaction == "clear" and objsessiondata != None:
                objetxmpp.session.clear(sessionid)
            elif sessionaction == "actualise":
                objetxmpp.session.reactualisesession(sessionid, 10)
            return response
        return wrapper
    return decorateur

def merge_dicts(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def portline(result):
    colonne = [x.strip() for x in result.split(' ') if x != ""]
    return colonne[-2:-1][0].split(':')[1]


def protoandport():
    protport={}
    if sys.platform.startswith('linux'):
        vnc = simplecommand( 'lsof -n -P -i -sTCP:LISTEN | grep LISTEN | grep IPv4 | grep vnc' )
        if len(vnc['result']) >0:
            protport['vnc']=portline(vnc['result'][0])
    elif sys.platform.startswith('darwin'):
        vnc = simplecommand( 'lsof -n -P -iTCP:rfb -sTCP:LISTEN' )
        if len(vnc['result']) >=1:
            protport['vnc']=portline(vnc['result'][1])

    if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
        ssh = simplecommand( 'lsof -n -P -iTCP:ssh -sTCP:LISTEN' )
        if len(ssh['result']) >=1:
            protport['ssh']=portline(ssh['result'][1])
            print protport['ssh']
    if sys.platform.startswith('linux'):
        rdp = simplecommand( 'lsof -n -P -i -sTCP:LISTEN | grep LISTEN | grep IPv4 | grep rdp' )
        if len(rdp['result']) > 0:
            protport['rdp']=portline(rdp['result'][0])
            print protport['rdp']
    if sys.platform.startswith('win'):
        pidvnc = None
        pidssh = None
        pidrdp = None
        processwin=simplecommand( 'tasklist /svc' )
        for line in processwin['result']:
            if line.startswith(" "):
                if "TermService" in line:
                    pidrdp = pidrdptmp
            else:
                if line.startswith("sshd"):
                    colonne = [x for x in line.split(' ') if x !=""]
                    pidssh = colonne[1]
                elif line.startswith("tvnserver"):
                    colonne = [x for x in line.split(' ') if x !=""]
                    if colonne[2] == 'tvnserver':
                        pidvnc = colonne[1]
                elif line.startswith("svchost"):
                    colonne = [x for x in line.split(' ') if x !=""]
                    pidrdptmp = colonne[1]

        networkwin = simplecommand( 'netstat -a -n -o | findstr TCP | findstr 0.0.0.0' )
        for line in networkwin['result']:
            if pidvnc != None and pidvnc in line:
                colonne = [x.strip() for x in line.split(' ') if x !=""]
                vncporwin = colonne[1].split(':')[1]
                if not 'vnc' in  protport:
                    protport['vnc']=vncporwin
                else:
                    if vncporwin > protport['vnc']:
                        protport['vnc']=vncporwin
            if not 'ssh' in  protport and pidssh != None and pidssh in line:
                colonne = [x.strip() for x in line.split(' ') if x !=""]
                if colonne[-1] ==  pidssh:
                    protport['ssh']=colonne[1].split(':')[1]
            if not 'rdp' in  protport and pidrdp  != None and pidrdp in line:
                colonne = [x.strip() for x in line.split(' ') if x !=""]
                if colonne[-1] ==  pidrdp:
                    protport['rdp']=colonne[1].split(':')[1]
    return protport

def ipfromdns(ipdata):
    """ This function converts a dns to ipv4
        If not find return ""
        function tester on OS:
        MAcOs, linux (debian, redhat, ubuntu), windows
        eg : print ip("sfr.fr")
        80.125.163.172
    """
    def strnslookup(str):
        adress = False
        for t in str:
            if "Name" in t:
                adress = True
            if adress == True and "Address" in t:
                return t.split(":")[1].strip()
        return ""

    def searchipfromdns(ip):
        re = shellcommandtimeout("nslookup %s"%ip, 10).run()
        result  = [x.strip() for x in re['result'] if x !='']
        if sys.platform.startswith('linux'):
            return strnslookup(result)
        elif sys.platform.startswith('win'):
            return strnslookup(result)
        elif sys.platform.startswith('darwin'):
            return strnslookup(result)

    if ipdata != "" and ipdata != None:
        if ipdata.lower() == "localhost":
            return "127.0.0.1"
        if is_valid_ipv4(ipdata):
            return ipdata
        else:
            return searchipfromdns(ipdata)
    return ""
