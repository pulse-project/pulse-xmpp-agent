#!/usr/bin/python3
# -*- coding: utf-8; -*-
# SPDX-FileCopyrightText: 2016-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

import netifaces
import subprocess

import platform
import logging
import re
import socket
import psutil
import os
import sys
import ipaddress

from lib.utils import simplecommand, powerschellscript1ps1
from . import utils

import traceback

if sys.platform.startswith("win"):
    import wmi

logger = logging.getLogger()


class networkagentinfo:
    def __init__(self, sessionid, action="resultgetinfo", param=[]):
        self.sessionid = sessionid
        self.action = action
        self.messagejson = {}
        self.networkobjet(self.sessionid, self.action)
        for d in self.messagejson["listipinfo"]:
            d["macnotshortened"] = d["macaddress"]
            d["macaddress"] = self.reduction_mac(d["macaddress"])
        if len(param) != 0 and len(self.messagejson["listipinfo"]) != 0:
            dd = []
            param1 = list(map(self.reduction_mac, param))
            for d in self.messagejson["listipinfo"]:
                e = [s for s in param1 if d["macaddress"] == s]
                if e:
                    dd.append(d)
            self.messagejson["listipinfo"] = dd

    def reduction_mac(self, mac):
        """
        Reduce the MAC address to lowercase and remove any separators like colons, dashes, or spaces.

        :param mac: The MAC address to be reduced.
        :type mac: str

        :return: The reduced MAC address.
        :rtype: str
        """
        mac = mac.lower()
        mac = mac.replace(":", "")
        mac = mac.replace("-", "")
        mac = mac.replace(" ", "")
        return mac

    def getuser(self):
        return list({users[0] for users in psutil.users()})

    def networkobjet(self, sessionid, action):
        """
        Collecte des informations sur la configuration réseau de l'ordinateur.

        Args:
            sessionid (str): Identifiant de session pour l'action en cours.
            action (str): Action à entreprendre.

        Returns:
            dict: Un dictionnaire contenant des informations sur la configuration réseau.
                Les clés comprennent :
                - "action": Action spécifiée en argument.
                - "sessionid": Identifiant de session spécifié en argument.
                - "listdns": Liste des serveurs DNS configurés.
                - "listipinfo": Liste des informations sur les adresses IP et les interfaces réseau.
                - "dhcp": Indique si DHCP est activé (True/False).
                - 'dhcpinfo' est présent seulement si dhcp est True
                - "dnshostname": Nom de l'hôte DNS.
                - "msg": Système d'exploitation en cours d'exécution.

        Note:
            Cette fonction collecte des informations sur la configuration réseau de l'ordinateur
            en fonction du système d'exploitation en cours d'exécution.
            - Sur Linux, elle vérifie la configuration DHCP et collecte les informations des interfaces réseau.
            - Sur Windows, elle collecte les informations des interfaces réseau, y compris les adresses IP, les passerelles, etc.
            - Sur macOS, elle appelle la fonction MacOsNetworkInfo pour collecter des informations spécifiques.
            - Pour d'autres systèmes d'exploitation, un message indiquant qu'ils ne sont pas encore gérés est renvoyé.
        """
        self.messagejson = {
            "action": action,
            "sessionid": sessionid,
            "listdns": [],
            "listipinfo": [],
            "dhcp": "False",
            "dnshostname": "",
            "msg": platform.system(),
        }
        try:
            self.messagejson["users"] = self.getuser()
        except BaseException:
            self.messagejson["users"] = ["system"]
        if sys.platform.startswith("linux"):
            p = subprocess.Popen(
                "ip addr show | grep dynamic",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            result = [x.decode("utf-8").strip() for x in p.stdout.readlines()]
            self.messagejson["dhcp"] = "True" if len(result) > 0 else "False"
            if self.messagejson["dhcp"]:
                pattern = (
                    r"inet (\S+)\/(\d+) metric (\d+) brd (\S+) scope global (\S+) (\S+)"
                )
                self.messagejson["dhcpinfo"] = []
                for info_line in result:
                    match = re.match(pattern, info_line)
                    if match:
                        # Créez un dictionnaire avec les informations extraites
                        info_dict = {
                            "ip_address": match.group(1),
                            "subnet_mask": match.group(2),
                            "metric": match.group(3),
                            "broadcast_address": match.group(4),
                            "scope": match.group(5),
                            "interface": match.group(6),
                        }
                        self.messagejson["dhcpinfo"].append(info_dict)

            self.messagejson["listdns"] = self.listdnslinux()
            self.messagejson["listipinfo"] = self.getLocalIipAddress()
            self.messagejson["dnshostname"] = platform.node()
            return self.messagejson

        elif sys.platform.startswith("win"):
            """revoit objet reseau windows"""
            # interface active only
            # pythoncom.CoInitialize()
            # try:
            # wmi_obj = wmi.WMI()
            # wmi_sql = "select * from Win32_NetworkAdapterConfiguration where IPEnabled=TRUE"
            # wmi_out = wmi_obj.query(wmi_sql)
            # finally:
            # pythoncom.CoUninitialize()
            # for dev in wmi_out:
            # objnet = {}
            # objnet['macaddress'] = dev.MACAddress
            # objnet['ipaddress'] = dev.IPAddress[0]
            # try:
            # objnet['gateway'] = dev.DefaultIPGateway[0]
            # except BaseException:
            # objnet['gateway'] = ""
            # objnet['mask'] = dev.IPSubnet[0]
            # objnet['dhcp'] = dev.DHCPEnabled
            # objnet['dhcpserver'] = dev.DHCPServer
            # self.messagejson['listipinfo'].append(objnet)
            # try:
            # self.messagejson['listdns'].append(
            # dev.DNSServerSearchOrder[0])
            # except BaseException:
            # pass
            # self.messagejson['dnshostname'] = dev.DNSHostName
            # self.messagejson['msg'] = platform.system()
            # all interface
            wmi_obj = wmi.WMI()
            wmi_sql = "select * from Win32_NetworkAdapterConfiguration"
            wmi_out = wmi_obj.query(wmi_sql)
            for dev in wmi_out:
                if dev.MACAddress is None:
                    continue
                objnet = {"macaddress": dev.MACAddress, "Description": dev.Description}
                try:
                    objnet["ipaddress"] = dev.IPAddress[0]
                except BaseException:
                    objnet["ipaddress"] = None
                try:
                    objnet["gateway"] = dev.DefaultIPGateway[0]
                except BaseException:
                    objnet["gateway"] = ""
                try:
                    objnet["mask"] = dev.IPSubnet[0]
                except BaseException:
                    objnet["mask"] = None
                try:
                    objnet["dhcp"] = dev.DHCPEnabled
                except BaseException:
                    objnet["dhcp"] = None
                try:
                    objnet["dhcpserver"] = dev.DHCPServer
                except BaseException:
                    objnet["dhcpserver"] = None
                self.messagejson["listipinfo"].append(objnet)
                try:
                    self.messagejson["listdns"].append(dev.DNSServerSearchOrder[0])
                except BaseException:
                    pass
                try:
                    self.messagejson["dnshostname"] = dev.DNSHostName
                except BaseException:
                    pass
            return self.messagejson
        elif sys.platform.startswith("darwin"):
            return self.MacOsNetworkInfo()
        else:
            self.messagejson["msg"] = f"system {sys.platform} : not managed yet"
            return self.messagejson

    def IpDhcp(self):
        """
        This function retrieves the DHCP server IP addresses used on the machine.
        It parses system logs (/var/log/syslog and journal logs) to extract DHCP-related information.
        Returns a dictionary where keys are IP addresses and values are corresponding DHCP server IP addresses.
        """
        obj1 = {}
        system = ""
        p = subprocess.Popen(
            "cat /proc/1/comm",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        result = p.stdout.readlines()
        if sys.version_info[0] == 3:
            system = result[0].decode("utf-8").rstrip("\n")
        else:
            system = result[0].rstrip("\n")

        if system == "init":
            p = subprocess.Popen(
                "grep -e DHCPACK /var/log/syslog | tail -n10 | awk '{print $(NF-2)\"@\" $NF}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            arrayresult = p.stdout.readlines()
            result = [x.decode("utf-8").rstrip("\n") for x in arrayresult]
            for item in result:
                d = item.split("@")
                try:
                    ip = ipaddress.ip_address(d[0])
                    dhcp_ip = ipaddress.ip_address(d[1])
                    obj1[str(ip)] = str(dhcp_ip)
                except ValueError:
                    pass
        elif system == "systemd":
            p = subprocess.Popen(
                "journalctl -t dhclient --no-pager | grep -E 'DHCPACK|bound to'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            arrayresult = p.stdout.readlines()
            result = [x.decode("utf-8").rstrip("\n") for x in arrayresult]
            for line in result:
                match = re.search(
                    r"DHCPACK.*?(\d+\.\d+\.\d+\.\d+|\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4})",
                    line,
                )
                if match:
                    ipdhcp = match.group(1)
                elif "bound to" in line:
                    match_ip = re.search(
                        r"bound to (\d+\.\d+\.\d+\.\d+|\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4}:\w{0,4})",
                        line,
                    )
                    if match_ip:
                        ipadress = match_ip.group(1)
                        try:
                            ip = ipaddress.ip_address(ipadress)
                            dhcp_ip = ipaddress.ip_address(ipdhcp)
                            obj1[str(ip)] = str(dhcp_ip)
                        except ValueError:
                            logging.getLogger().error(
                                f"The IP {ip} is not a valid IP adress"
                            )
                            # The IP is not valid, we do not want to do anything
                            pass

        return obj1

    def get_mac_address(self, ip):
        """
        Récupère l'adresse MAC associée à une adresse IPv4 en utilisant la commande 'ip' sous Linux.
        Args:
            ip (str): Adresse IPv4 pour laquelle vous souhaitez obtenir l'adresse MAC.
        Returns:
            str or None: L'adresse MAC associée à l'adresse IPv4, ou None si l'adresse MAC n'est pas trouvée.
        """
        try:
            # Vérifie le système d'exploitation
            current_os = platform.system().lower()
            if current_os == "linux":
                # Utilise la commande 'ip' pour obtenir le nom de l'interface
                name_command = f"ip addr | grep 'inet {ip}' | awk '{{print($NF)}}'"
                interface_name = subprocess.check_output(
                    name_command, shell=True, text=True
                ).strip()
                # Utilise la commande 'ip' pour obtenir l'adresse MAC de l'interface
                mac_command = f"ip link show dev {interface_name} | grep link/ether | awk '{{print($2)}}'"
                mac_address = subprocess.check_output(
                    mac_command, shell=True, text=True
                ).strip()
                return mac_address
            else:
                # Autres systèmes d'exploitation (non Linux), vous pouvez ajouter ici une autre logique si nécessaire
                return None
        except Exception:
            return None

    @staticmethod
    def get_mac_address_with_netifaces(ip):
        """
        Retrieve the MAC address associated with the given IP address using netifaces library.

        Args:
            ip (str): The IP address for which the MAC address is to be retrieved.

        Returns:
            str or None: The MAC address corresponding to the provided IP address, or None if not found.

        Raises:
            None. Exceptions are caught and logged.
        """
        try:
            for interface in netifaces.interfaces():
                if interface == "":
                    continue
                addrs = netifaces.ifaddresses(interface)

                if netifaces.AF_INET in addrs:
                    ipv4_info = addrs[netifaces.AF_INET][0]
                    if "addr" in ipv4_info and ipv4_info["addr"] == ip:
                        # Si l'adresse IP correspond, obtenir l'adresse MAC
                        if netifaces.AF_LINK in addrs:
                            mac_address = addrs[netifaces.AF_LINK][0]["addr"]
                            return mac_address

        except Exception:
            logger.error("\n%s" % (traceback.format_exc()))
            return None

    def MacAddressToIp(self, ip):
        """
        Converts an IP address to its corresponding MAC address using the networkagentinfo class method and fallback mechanisms.

        Args:
            ip (str): The IP address to be converted to a MAC address.

        Returns:
            str or None: The MAC address corresponding to the provided IP address, or None if not found.

        Raises:
            None. Logs a warning message if network issues are detected or if the MAC address cannot be found.
        """
        mac = networkagentinfo.get_mac_address_with_netifaces(ip)
        if mac is not None:
            return mac
        logger.warning(
            "Network issues detected\nnetifaces could not find the MAC address of an interface with a valid IPv4 address.\nNetwork connectivity issues can impact MAC address resolution.\nPlease check your network configuration."
        )
        # Si la première fonction échoue et que le système est Linux, utilisez la fonction get_mac_address
        current_os = platform.system().lower()
        if current_os == "linux":
            return self.get_mac_address(ip)
        return None

    def MacOsNetworkInfo(self):
        self.messagejson["dnshostname"] = platform.node()
        self.messagejson["listipinfo"] = []
        self.messagejson["dhcp"] = "False"

        for i in netifaces.interfaces():
            try:
                p = subprocess.Popen(
                    f"ipconfig getpacket {i}",
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                result = p.stdout.readlines()
                code_result = p.wait()
                if code_result == 0:
                    partinfo = {
                        "dhcpserver": "",
                        "dhcp": "False",
                        "macaddress": netifaces.ifaddresses(i)[netifaces.AF_LINK][0][
                            "addr"
                        ],
                    }
                    for line in result:
                        line = line.rstrip("\n")
                        colonne = line.split("=")
                        if len(colonne) != 2:
                            colonne = line.split(":")
                        if colonne[0].strip().startswith("yiaddr"):
                            partinfo["ipaddress"] = colonne[1].strip()
                        elif colonne[0].strip().startswith("subnet_mask"):
                            partinfo["mask"] = colonne[1].strip()
                        elif colonne[0].strip().startswith("router"):
                            partinfo["gateway"] = colonne[1].strip(" {}")
                        elif colonne[0].strip().startswith("server_identifier"):
                            partinfo["dhcpserver"] = colonne[1].strip()
                            partinfo["dhcp"] = "True"
                            self.messagejson["dhcp"] = "True"
                        elif colonne[0].strip().startswith("domain_name_server"):
                            self.messagejson["listdns"] = (
                                colonne[1].strip(" {}").split(",")
                            )
                            self.messagejson["listdns"] = [
                                x.strip() for x in self.messagejson["listdns"]
                            ]
                        else:
                            continue
                    try:
                        if partinfo["ipaddress"] != "":
                            self.messagejson["listipinfo"].append(partinfo)
                    except BaseException:
                        pass
            except (
                BaseException
            ):  # IndexError, KeyError: #ignore ifaces that dont have MAC or IP
                pass
        return self.messagejson

    def getLocalIipAddress(self):
        dhcpserver = self.IpDhcp()
        ip_addresses = []
        defaultgateway = {}
        try:
            gws = netifaces.gateways()
            intergw = gws["default"][netifaces.AF_INET]
            defaultgateway[intergw[1]] = intergw[0]
        except Exception:
            pass
        interfaces = netifaces.interfaces()
        for i in interfaces:
            if i == "lo" or i == "":
                continue
            if iface := netifaces.ifaddresses(i).get(netifaces.AF_INET):
                for j in iface:
                    if (
                        j["addr"] != "127.0.0.1"
                        and self.MacAddressToIp(j["addr"]) is not None
                    ):
                        obj = {"ipaddress": j["addr"], "mask": j["netmask"]}
                        try:
                            obj["broadcast"] = j["broadcast"]
                        except BaseException:
                            obj["broadcast"] = "0.0.0.0"
                        try:
                            obj["gateway"] = defaultgateway.get(str(i), "0.0.0.0")
                        except Exception:
                            obj["gateway"] = "0.0.0.0"

                        obj["macaddress"] = self.MacAddressToIp(j["addr"])
                        try:
                            if dhcpserver[j["addr"]] is not None:
                                obj["dhcp"] = "True"
                                obj["dhcpserver"] = dhcpserver[j["addr"]]
                            else:
                                obj["dhcp"] = "False"
                                obj["dhcpserver"] = "0.0.0.0"
                        except BaseException:
                            obj["dhcp"] = "False"
                            obj["dhcpserver"] = "0.0.0.0"
                        ip_addresses.append(obj)
        return ip_addresses

    def get_network_info(self):
        """
        Collecte des informations détaillées sur les interfaces réseau de l'ordinateur sous Linux.

        Returns:
            list: Une liste de dictionnaires, chaque dictionnaire contenant des informations détaillées sur une interface réseau.
                Les informations incluses sont :
                - "interface": Nom de l'interface réseau.
                - "ipv4": Adresse IPv4 de l'interface.
                - "ipv6": Adresse IPv6 de l'interface.
                - "mask": Masque de sous-réseau de l'interface.
                - "broadcast": Adresse de diffusion de l'interface (par défaut 0.0.0.0).
                - "macaddress": Adresse MAC de l'interface.
                - "dhcp": Indique si DHCP est activé sur l'interface (True/False).
                - "scope": Portée de l'interface.
                - "metric": Métrique de l'interface.
                - "gateway": Adresse de passerelle de l'interface.
                - "mtu": MTU (Maximum Transmission Unit) de l'interface.
                - "dns_servers": Liste des serveurs DNS configurés pour l'interface.
                - "status": Statut de l'interface (Up/Down).
                - "speed": Vitesse de l'interface (en Mbps).
                - "duplex": Mode duplex de l'interface.
                - "isp": Nom du fournisseur de services (ISP) si applicable.
                - "network_type": Type de réseau (public, privé, domaine) si applicable.
                - "connection_quality": Informations sur la qualité de la connexion si applicable.
                - "custom_config": Informations de configuration spécifique de l'interface si nécessaire.

        Note:
            Cette fonction collecte des informations détaillées sur les interfaces réseau de l'ordinateur sous Linux,
            notamment les informations sur les adresses IP, les adresses MAC, la configuration DHCP, le MTU, les serveurs DNS,
            le statut de l'interface, la vitesse, le mode duplex, le fournisseur de services (ISP), le type de réseau,
            la qualité de la connexion, et toute information de configuration personnalisée si spécifiée.
        """
        network_info = []
        if platform.system().lower() == "linux":
            # Collecte les informations réseau sous Linux
            interfaces = netifaces.interfaces()
            for interface in interfaces:
                if interface == "":
                    continue
                interface_info = {}
                interface_info["interface"] = interface
                interface_info["ipv4"] = None
                interface_info["ipv6"] = None
                interface_info["mask"] = None
                interface_info["broadcast"] = "0.0.0.0"
                interface_info["macaddress"] = None
                interface_info["dhcp"] = False
                interface_info["scope"] = None
                interface_info["metric"] = None
                interface_info["gateway"] = None
                interface_info["mtu"] = None
                interface_info["dns_servers"] = []
                interface_info["status"] = None
                interface_info["speed"] = None
                interface_info["isp"] = None
                interface_info["network_type"] = None
                interface_info["connection_quality"] = None
                interface_info["custom_config"] = None
                try:
                    # Obtient les informations d'adresse IP pour l'interface
                    addrs = netifaces.ifaddresses(interface)
                    if netifaces.AF_INET in addrs:
                        ipv4_info = addrs[netifaces.AF_INET][0]
                        interface_info["ipv4"] = ipv4_info.get("addr")
                        interface_info["mask"] = ipv4_info.get("netmask")
                        interface_info["broadcast"] = ipv4_info.get(
                            "broadcast", "0.0.0.0"
                        )
                    if netifaces.AF_INET6 in addrs:
                        ipv6_info = addrs[netifaces.AF_INET6][0]
                        interface_info["ipv6"] = ipv6_info.get("addr")
                    # Obtient l'adresse MAC de l'interface
                    mac_info = netifaces.ifaddresses(interface)[netifaces.AF_LINK][0]
                    interface_info["macaddress"] = mac_info.get("addr")
                    # Vérifie si l'interface utilise DHCP
                    result = subprocess.run(
                        ["ip", "addr", "show", "dev", interface],
                        capture_output=True,
                        text=True,
                    )
                    if "inet dynamic" in result.stdout:
                        interface_info["dhcp"] = True
                    # Obtient la métrique et la portée de l'interface
                    result = subprocess.run(
                        ["ip", "route", "show", "dev", interface],
                        capture_output=True,
                        text=True,
                    )
                    for line in result.stdout.splitlines():
                        if "metric" in line:
                            metric_match = re.search(r"metric (\d+)", line)
                            if metric_match:
                                interface_info["metric"] = metric_match.group(1)
                        if "scope" in line:
                            scope_match = re.search(r"scope (\w+)", line)
                            if scope_match:
                                interface_info["scope"] = scope_match.group(1)
                    # Obtient l'information MTU
                    result = subprocess.run(
                        ["ip", "link", "show", "dev", interface],
                        capture_output=True,
                        text=True,
                    )
                    mtu_match = re.search(r"mtu (\d+)", result.stdout)
                    if mtu_match:
                        interface_info["mtu"] = mtu_match.group(1)
                    # Obtient les serveurs DNS
                    dns_result = subprocess.run(
                        ["cat", "/etc/resolv.conf"], capture_output=True, text=True
                    )
                    dns_servers = re.findall(r"nameserver (\S+)", dns_result.stdout)
                    interface_info["dns_servers"] = dns_servers
                    # Obtient l'état de l'interface (Up/Down)
                    status_result = subprocess.run(
                        ["ip", "link", "show", "dev", interface],
                        capture_output=True,
                        text=True,
                    )
                    if "state UP" in status_result.stdout:
                        interface_info["status"] = "Up"
                    else:
                        interface_info["status"] = "Down"
                    # Obtient la vitesse et le mode duplex de l'interface
                    ethtool_result = subprocess.run(
                        ["ethtool", interface], capture_output=True, text=True
                    )
                    speed_match = re.search(r"Speed: (\d+)", ethtool_result.stdout)
                    duplex_match = re.search(r"Duplex: (\w+)", ethtool_result.stdout)
                    if speed_match:
                        interface_info["speed"] = speed_match.group(1)
                    if duplex_match:
                        interface_info["duplex"] = duplex_match.group(1)
                    # Obtient le nom du fournisseur de services (ISP) si possible
                    if "ppp" in interface:
                        ppp_result = subprocess.run(
                            ["pppoe-status"], capture_output=True, text=True
                        )
                        isp_match = re.search(r"ISP Name\s*:\s*(.*)", ppp_result.stdout)
                        if isp_match:
                            interface_info["isp"] = isp_match.group(1)
                except Exception as e:
                    print(
                        f"Erreur lors de la collecte d'informations pour {interface}: {str(e)}"
                    )
                network_info.append(interface_info)
        return network_info

    def listdnslinux(self):
        dns = []
        p = subprocess.Popen(
            "",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        result = p.stdout.readlines()
        return (
            [x.decode("utf-8").strip() for x in result]
            if sys.version_info[0] == 3
            else [x.strip() for x in result]
        )


def powershellfqdnwindowscommand():
    """
    search fqdn for machine windows from activedirectory
    """
    try:
        output = subprocess.check_output(
            [
                "powershell.exe",
                """([adsisearcher]"(&(objectClass=computer)(name=$env:computername))").findone().path""",
            ],
            shell=True,
        )
        output = output.decode("cp850")
        lou = [x.replace("OU=", "") for x in output.split(",") if "OU=" in x]
        outou = [y for y in lou if not re.findall("[éèêëÉÈÊËàâäÀÂÄôöÔÖùÙ\\(\\)]", y)]
        if not outou:
            return ""
        outou.reverse()
        return "@@".join(outou)
    except subprocess.CalledProcessError as e:
        logger.error(f"subproces powershellfqdnwindowscommand.output = {e.output}")
    return ""


def powershellfqdnwindowscommandbyuser(user):
    try:
        output = subprocess.check_output(
            [
                "powershell.exe",
                f"""([adsisearcher]"(&(objectClass=user)(samaccountname={user}))").findone().path""",
            ],
            shell=True,
        )
        output = output.decode("cp850")
        lou = [x.replace("OU=", "") for x in output.split(",") if "OU=" in x]
        outou = [y for y in lou if not re.findall("[éèêëÉÈÊËàâäÀÂÄôöÔÖùÙ\\(\\)]", y)]
        if not outou:
            return ""
        outou.reverse()
        return "@@".join(outou)
    except subprocess.CalledProcessError as e:
        logger.error(
            f"subproces powershellfqdnwindowscommandbyuser.output = {e.output}"
        )
    return ""


def powershellfinduseradgroups(user):
    result = []
    try:
        for line in subprocess.check_output(
            [
                "powershell.exe",
                f"""([adsisearcher]"(&(objectClass=user)(samaccountname={user}))").findone().Properties.memberof""",
            ],
            shell=True,
        ).split("\n"):
            line = line.decode("cp850")
            lcn = [x.replace("CN=", "") for x in line.split(",") if "CN=" in x]
            outcn = [
                y for y in lcn if not re.findall("[éèêëÉÈÊËàâäÀÂÄôöÔÖùÙ\\(\\)]", y)
            ]
            if len(outcn) != 0:
                outcn.reverse()
                result.append("@@".join(outcn))
        logger.debug(f"AD Groups: {result}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"subproces powershellfinduseradgroups.output = {e.output}")
    return ""


def powershellgetlastuser():
    if sys.platform.startswith("win"):
        script = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "..",
                "script",
                "getlastuser.ps1",
            )
        )
        result = powerschellscript1ps1(script)
        try:
            if result["code"] == 0:
                ret = []
                line = [
                    x.replace(os.linesep, "")
                    for x in result["result"]
                    if x.strip().replace(os.linesep, "") != ""
                ]
                namelist = []
                if len(line) > 2:
                    for t in line[2:]:
                        if res := t.split("\\")[1]:
                            if final := res.split(" ")[:-1]:
                                namelist.append(" ".join(final))
                return ",".join(namelist)
        except IndexError:
            logger.warning("detection last name")
            logger.error("\n%s" % (traceback.format_exc()))
            return "system"
    return "system"


def isMachineInDomain():
    """
    returns if the machine is part of an AD domain or not
    """
    try:
        output = subprocess.check_output(
            ["powershell.exe", """(gwmi win32_computersystem).partofdomain"""],
            shell=True,
        )
        return output.strip() == "true"
    except subprocess.CalledProcessError as e:
        logger.error(f"subproces isMachineInDomain.output = {e.output}")
    return False


def organizationbymachine():
    """
    AD information for machine
    search fqdn for machine windows from activedirectory
    """
    if sys.platform.startswith("win"):
        if indomain := isMachineInDomain():
            return powershellfqdnwindowscommand()

    return ""


def organizationbyuser(user):
    """
    AD information for user
    search fqdn for machine windows from activedirectory
    """
    if sys.platform.startswith("win"):
        if indomain := isMachineInDomain():
            return powershellfqdnwindowscommandbyuser(user)

    return ""


def adusergroups(user):
    """
    AD groups of user
    find groups to which user belongs in AD
    This function returns an array containing the groups
    """
    if sys.platform.startswith("win"):
        if indomain := isMachineInDomain():
            return powershellfinduseradgroups(user)

    return ""


def interfacename(mac):
    for interface in netifaces.interfaces():
        if interface == "":
            continue
        if isInterfaceToMacadress(interface, mac):
            return interface
    return ""


def lit_networkconf():
    pass


def isInterfaceToMacadress(interface, mac):
    addrs = netifaces.ifaddresses(interface)
    try:
        if_mac = addrs[netifaces.AF_LINK][0]["addr"]
    except (
        BaseException
    ):  # IndexError, KeyError: #ignore ifaces that dont have MAC or IP
        return False
    return if_mac == mac


def isInterfaceToIpadress(interface, ip):
    addrs = netifaces.ifaddresses(interface)
    try:
        if_ip = addrs[netifaces.AF_INET][0]["addr"]
    except (
        BaseException
    ):  # IndexError, KeyError: #ignore ifaces that dont have MAC or IP
        return False
    return if_ip == ip


def rewriteInterfaceTypeRedhad(configfile, data, interface):
    tab = []
    with open(configfile, "rb") as inputFile:
        filecontent = inputFile.read()
    tab = filecontent.split("\n")
    ll = [
        x
        for x in tab
        if not x.strip().startswith("IPADDR")
        and not x.strip().startswith("NETMASK")
        and not x.strip().startswith("NETWORK")
        and not x.strip().startswith("GATEWAY")
        and not x.strip().startswith("BROADCAST")
        and not x.strip().startswith("BOOTPROTO")
    ]
    try:
        if data["dhcp"]:
            ll.insert(1, "BOOTPROTO=dhcp")
        else:
            ll.insert(1, "BOOTPROTO=static")
            ll.append(f'IPADDR={data["ipaddress"]}')
            ll.append(f'NETMASK={data["mask"]}')
            ll.append(f'GATEWAY={data["gateway"]}')
        strr = "\n".join(ll)
        with open(configfile, "wb") as inputFile:
            inputFile.write(strr)
    except BaseException:
        return False
    return True


def rewriteInterfaceTypeDebian(data, interface):
    tab = []
    z = []
    try:
        with open("/etc/network/interfaces", "rb") as inputFile:
            filecontent = inputFile.read()
        with open("/etc/network/interfacesold", "wb") as inputFile:
            inputFile.write(filecontent)
        b = filecontent.split("\n")
        ll = [
            x.strip()
            for x in b
            if not x.startswith("auto")
            and "auto" not in x
            and not x.startswith("#")
            and x != ""
        ]
        string = "\n".join(ll)
        ll = [x.strip() for x in string.split("iface") if x != ""]
        z.extend(t for t in ll if t.split(" ")[0] != interface)
        if data["dhcp"] is True:
            tab.extend(("\nauto %s\n" % interface, "iface %s inet dhcp\n" % interface))
        else:
            tab.extend(
                (
                    "auto %s\n" % interface,
                    "iface %s inet static\n" % interface,
                    "\taddress %s\n" % data["ipaddress"],
                    "\tnetmask %s\n" % data["mask"],
                    "\tgateway %s\n" % data["gateway"],
                )
            )
        val1 = "".join(tab)
        for t in z:
            val = "\nauto %s\niface " % t.split(" ")[0]
            val = "%s %s\n" % (val, t)
        with open("/etc/network/interfaces", "wb") as inputFile:
            inputFile.write("%s\n%s" % (val, val1))
        return True
    except BaseException:
        return False


def typelinuxfamily():
    debiandist = [
        "astra",
        "canaima",
        "collax",
        "cumulus",
        "damn",
        "debian",
        "doudoulinux",
        "euronode",
        "finnix",
        "grml",
        "kanotix",
        "knoppix",
        "linex",
        "linspire",
        "advanced",
        "lmde",
        "mepis",
        "ocera",
        "ordissimo",
        "parsix",
        "pureos",
        "rays",
        "aptosid",
        "ubuntu",
        "univention",
        "xandros",
    ]
    val = platform.platform().lower()
    return next(("debian" for t in debiandist if t in val), "redhat")


def getsystemressource():
    p = subprocess.Popen(
        "cat /proc/1/comm", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    result = p.stdout.readlines()
    return result[0].rstrip("\n")


def getWindowsNameInterfaceForMacadress(macadress):
    obj = utils.simplecommand("wmic NIC get MACAddress,NetConnectionID")
    for lig in obj["result"]:
        l = lig.lower()
        mac = macadress.lower()
        if l.startswith(mac):
            element = lig.split(" ")
            element[0] = ""
            fin = [x for x in element if x.strip() != ""]
            return " ".join(fin)


def getUserName():
    """
    This function allow to obtain the name of the connected users
    """
    if sys.platform.startswith("linux"):
        connected_users = simplecommand("who | cut -d" "  -f1 | sort | uniq")

    return connected_users
