#!/usr/bin/python3
# -*- coding: utf-8; -*-
# SPDX-FileCopyrightText: 2016-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

# file : pluginsmastersubstitute/plugin_wakeonlangroup.py

import json
from lib.plugins.xmpp import XmppMasterDatabase
import traceback
from lib.utils import name_random
import logging
import os

try:
    import wakeonlan3 as wol
except ModuleNotFoundError:
    import wakeonlan as wol
import configparser
from netifaces import interfaces, ifaddresses, AF_INET

logger = logging.getLogger()
plugin = {"VERSION": "1.3", "NAME": "wakeonlangroup", "TYPE": "substitute"}  # fmt: skip


def action(xmppobject, action, sessionid, data, message, ret):
    logger.debug("=====================================================")
    logger.debug("call %s from %s" % (plugin, message["from"]))
    logger.debug("=====================================================")
    sessionid = name_random(5, "wakeonlangroup")
    try:
        compteurcallplugin = getattr(xmppobject, "num_call%s" % action)
        logger.debug("compteurcallplugin %s" % compteurcallplugin)
        if compteurcallplugin == 0:
            read_conf_wol(xmppobject)
            xmppobject.brodcastwol = []
            for ifaceName in interfaces():
                addrs = ifaddresses(ifaceName)
                if AF_INET not in addrs:
                    continue
                k = addrs[AF_INET]
                for t in k:
                    if "broadcast" not in t:
                        break
                    if "netmask" not in t:
                        break
                    if "addr" not in t:
                        break
                    xmppobject.brodcastwol.append(t["broadcast"])
    except:
        logger.error("plugin %s\n%s" % (plugin["NAME"], traceback.format_exc()))

    try:
        if "macadress" in data:
            if xmppobject.wakeonlangroupremotelan:
                senddataplugin = {
                    "action": "wakeonlangroup",
                    "sessionid": sessionid,
                    "data": {"macaddress": ""},
                }
                serverrelaylist = (
                    XmppMasterDatabase().random_list_ars_relay_one_only_in_cluster()
                )
                senddataplugin["data"]["macaddress"] = data["macadress"]
                for serverrelay in serverrelaylist:
                    xmppobject.send_message(
                        mto=serverrelay["jid"],
                        mbody=json.dumps(senddataplugin, encoding="latin1"),
                        mtype="chat",
                    )
                    msglog = (
                        "A WOL request has been sent from the ARS %s "
                        "to the mac address (10 first adress) %s"
                        % (serverrelay["jid"], data["macadress"][:10])
                    )
                    historymessage(xmppobject, sessionid, msglog)
                    logger.debug(msglog)
            else:
                if xmppobject.wakeonlantargetsubnet:
                    # send magic to broadcast network
                    datamac = XmppMasterDatabase().wolbroadcastadressmacadress(
                        data["macadress"]
                    )
                    for t in datamac:
                        wol.send_magic_packet(
                            *datamac[t],
                            ip_address=t,
                            port=xmppobject.wakeonlangroupport
                        )
                        msglog = (
                            "A WOL request has been sent on broacast subnet %s "
                            "to the mac address %s" % (t, datamac[t])
                        )
                        historymessage(xmppobject, sessionid, msglog)
                        logger.debug(msglog)
                else:
                    dellist = []
                    for z in xmppobject.brodcastwol:
                        try:
                            wol.send_magic_packet(
                                *data["macadress"],
                                ip_address=z,
                                port=xmppobject.wakeonlangroupport
                            )
                        except Exception as e:
                            if "Connection refused" in str(e):
                                logger.debug("WOL impossible on broadcast %s" % z)
                                dellist.append(z)
                    for t in dellist:
                        xmppobject.brodcastwol.remove(t)

                    msglog = (
                        "A local lan WOL request have been sent to the"
                        "(display only for 10 addresses) mac "
                        "address %s and port %s"
                        % (data["macadress"][:10], xmppobject.wakeonlangroupport)
                    )
                    historymessage(xmppobject, sessionid, msglog)
                    logger.debug(msglog)
        else:
            msglog = "macadress key missing for plugin wakeonlangroup"
            historymessage(xmppobject, sessionid, msglog)
            logger.debug(msglog)

    except Exception as error_exception:
        msglog = (
            "An error occurent when loading the plugin plugin_wakeonlangroup %s" % data
        )
        tracebackerror = "\n%s" % (traceback.format_exc())
        logger.error("%s\n%s" % (tracebackerror, msglog))
        logger.error("The exception raised is %s" % error_exception)
        historymessage(
            xmppobject, sessionid, "%s\ndetail error:\n%s" % (msglog, tracebackerror)
        )


def historymessage(xmppobject, sessionid, msg):
    xmppobject.xmpplog(
        msg,
        type="deploy",
        sessionname=sessionid,
        priority=-1,
        action="xmpplog",
        who="",
        how="",
        why=xmppobject.boundjid.bare,
        module="Wol | Start | Creation",
        date=None,
        fromuser=xmppobject.boundjid.bare,
        touser="",
    )


def read_conf_wol(xmppobject):
    """
    This function read the configuration file for the wol plugin.
    The configuration file should be like:
    [parameters]
    remotelan = True
    # wakeonlanport using only for remotelan is False
    wakeonlanport = 9
    """

    conf_filename = plugin["NAME"] + ".ini"
    pathfileconf = os.path.join(xmppobject.config.pathdirconffile, conf_filename)
    logger.debug("The configuration file is %s" % pathfileconf)
    xmppobject.wakeonlangroupremotelan = False
    xmppobject.wakeonlangroupport = 9
    xmppobject.wakeonlantargetsubnet = True

    if not os.path.isfile(pathfileconf):
        logger.error(
            "The configuration file for the plugin %s is missing.\n"
            "It should be located to %s)" % (plugin["NAME"], pathfileconf)
        )
        if not xmppobject.wakeonlangroupremotelan:
            logger.error(
                "The used parameters are\nremotelan %s"
                "\nwakeonlanport %s\ntargetsubnet %s"
                % (
                    xmppobject.wakeonlangroupremotelan,
                    xmppobject.wakeonlangroupport,
                    xmppobject.wakeonlantargetsubnet,
                )
            )
        else:
            logger.error(
                "The default parameters for remotelan is %s"
                % (xmppobject.wakeonlangroupremotelan)
            )
    else:
        Config = configparser.ConfigParser()
        Config.read(pathfileconf)
        if os.path.exists(pathfileconf + ".local"):
            Config.read(pathfileconf + ".local")

        if Config.has_option("parameters", "remotelan"):
            xmppobject.wakeonlangroupremotelan = Config.getboolean(
                "parameters", "remotelan"
            )

        if not xmppobject.wakeonlangroupremotelan:
            if Config.has_option("parameters", "wakeonlanport"):
                xmppobject.wakeonlangroupport = Config.getint(
                    "parameters", "wakeonlanport"
                )

            if Config.has_option("parameters", "targetsubnet"):
                xmppobject.wakeonlantargetsubnet = Config.getboolean(
                    "parameters", "targetsubnet"
                )

        if not xmppobject.wakeonlangroupremotelan:
            logger.debug(
                "The used parameters are\nremotelan %s"
                "\nwakeonlanport %s\ntargetsubnet %s"
                % (
                    xmppobject.wakeonlangroupremotelan,
                    xmppobject.wakeonlangroupport,
                    xmppobject.wakeonlantargetsubnet,
                )
            )
        else:
            logger.info(
                "The default parameters for remotelan is %s"
                % (xmppobject.wakeonlangroupremotelan)
            )
