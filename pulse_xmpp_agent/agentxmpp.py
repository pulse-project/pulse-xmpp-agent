#!/usr/bin/python3
# -*- coding: utf-8; -*-
# SPDX-FileCopyrightText: 2016-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later
import socket

import glob
import sys
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback
import platform
import base64
import json
import time
import threading
import shutil
import subprocess
import psutil
import random
import hashlib
import configparser
from lib.manageresourceplugin import resource_plugin
import zlib
import importlib
import cherrypy
from lib.reverseport import reverse_port_ssh
from lib.agentconffile import (
    conffilename,
    conffilenametmp,
    medullaPath,
    directoryconffile,
    pulseTempDir,
)
from lib.update_remote_agent import Update_Remote_Agent
from lib.xmppiq import dispach_iq_command
from lib.networkinfo import (
    networkagentinfo,
    organizationbymachine,
    organizationbyuser,
    adusergroups,
)
from lib.configuration import (
    confParameter,
    nextalternativeclusterconnection,
    changeconnection,
    nextalternativeclusterconnectioninformation,
    alternativeclusterconnection,
)
from lib.managesession import session
from lib.managefifo import fifodeploy
from lib.managedeployscheduler import ManageDbScheduler
from lib.managedbkiosk import manageskioskdb

from lib.iq_custom import iq_custom_xep, iq_value, Myiq
from lib.utils import (
    DEBUGPULSE,
    getIpXmppInterface,
    NetworkInfoxmpp,
    refreshfingerprint,
    getRandomName,
    load_back_to_deploy,
    cleanbacktodeploy,
    call_plugin,
    call_mon_plugin,
    subnetnetwork,
    createfingerprintnetwork,
    isWinUserAdmin,
    isMacOsUserAdmin,
    check_exist_ip_port,
    ipfromdns,
    shutdown_command,
    reboot_command,
    vnc_set_permission,
    save_count_start,
    unregister_agent,
    unregister_subscribe,
    test_kiosk_presence,
    file_get_contents,
    isBase64,
    connection_established,
    file_put_contents,
    simplecommand,
    testagentconf,
    Setdirectorytempinfo,
    setgetcountcycle,
    setgetrestart,
    protodef,
    geolocalisation_agent,
    Env,
    serialnumbermachine,
    file_put_contents_w_a,
    os_version,
    offline_search_kb,
    file_message_iq,
    call_plugin_sequentially,
    convert,
    DateTimebytesEncoderjson,
    clean_update_directories,
)
from lib.manage_xmppbrowsing import xmppbrowsing
from lib.manage_event import manage_event
from lib.manage_process import mannageprocess, process_on_end_send_message_xmpp
from lib.syncthingapirest import syncthing, syncthingprogram, iddevice, conf_ars_deploy
from lib.manage_scheduler import manage_scheduler
from lib.logcolor import add_coloring_to_emit_ansi, add_coloring_to_emit_windows
from lib.manageRSAsigned import MsgsignedRSA, installpublickey
from lib.managepackage import managepackage
from lib.httpserver import Controller
from lib.grafcetdeploy import grafcet
from zipfile import *
from optparse import OptionParser
from multiprocessing import Queue, Process, Event, Value
from multiprocessing.managers import SyncManager
import multiprocessing
from modulefinder import ModuleFinder
from datetime import datetime, timezone

import zipfile
import tempfile

from slixmpp import ClientXMPP
from slixmpp import jid
from slixmpp.xmlstream import handler, matcher
from slixmpp.exceptions import IqError, IqTimeout
from slixmpp.xmlstream.stanzabase import ET
from slixmpp.xmlstream.handler import Callback, CoroutineCallback
from slixmpp.xmlstream.matcher.xpath import MatchXPath
from slixmpp.xmlstream.matcher.stanzapath import StanzaPath
from slixmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from slixmpp.xmlstream.matcher.many import MatchMany
from slixmpp.xmlstream.matcher.idsender import MatchIDSender
from slixmpp.xmlstream.matcher.id import MatcherId
from slixmpp.xmlstream.matcher.base import MatcherBase
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

if sys.platform.startswith("win"):
    import win32api
    import win32con
    import win32pipe
    import win32file
    import win32com.client
else:
    import signal
    from resource import RLIMIT_NOFILE, RLIM_INFINITY, getrlimit
    import posix_ipc


from lib.server_kiosk import (
    manage_kiosk_message,
)


sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib"))

# Créer un verrou partagé
terminate_lock = multiprocessing.Lock()


# reecriture du fichier cluster


def create_config_file_atomically(file_path, config_data):
    """
    Creates a configuration file in .ini format based on the provided dictionary structure
    and writes it atomically.

    :param file_path: The full path of the file to be created.
    :param config_data: The dictionary containing the configuration data.
    """
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Add the [alternativelist] section
    config["alternativelist"] = {
        "listars": ",".join(config_data["listars"]),
        "nbserver": str(config_data["nbserver"]),
        "nextserver": str(config_data["nextserver"]),
    }

    # Add sections for each server in listars
    for server in config_data["listars"]:
        config[server] = config_data[server]

    # Generate the configuration content as a string
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_file:
        config.write(temp_file)
        temp_file.flush()
        temp_file_path = temp_file.name

    try:
        # Renommer le fichier temporaire pour remplacer le fichier cible
        os.replace(temp_file_path, file_path)
    except Exception as e:
        # En cas d'erreur, supprimer le fichier temporaire
        os.remove(temp_file_path)
        raise e


class TimedCompressedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Extended version of TimedRotatingFileHandler that compress logs on rollover.
    the rotation file is compress in zip
    """

    def __init__(
        self,
        filename,
        when="h",
        interval=1,
        backupCount=0,
        encoding=None,
        delay=False,
        utc=False,
        compress="zip",
    ):
        super(TimedCompressedRotatingFileHandler, self).__init__(
            filename, when, interval, backupCount, encoding, delay, utc
        )
        self.backupCountlocal = backupCount

    def get_files_by_date(self):
        dir_name, base_name = os.path.split(self.baseFilename)
        file_names = os.listdir(dir_name)
        result = []
        result1 = []
        prefix = f"{base_name}"
        for file_name in file_names:
            if file_name.startswith(prefix) and not file_name.endswith(".zip"):
                f = os.path.join(dir_name, file_name)
                result.append((os.stat(f).st_ctime, f))
            if file_name.startswith(prefix) and file_name.endswith(".zip"):
                f = os.path.join(dir_name, file_name)
                result1.append((os.stat(f).st_ctime, f))
        result1.sort()
        result.sort()
        while result1 and len(result1) >= self.backupCountlocal:
            el = result1.pop(0)
            if os.path.exists(el[1]):
                os.remove(el[1])
        return result[1][1]

    def doRollover(self):
        super(TimedCompressedRotatingFileHandler, self).doRollover()
        try:
            dfn = self.get_files_by_date()
        except Exception:
            return
        dfn_zipped = f"{dfn}.zip"
        if os.path.exists(dfn_zipped):
            os.remove(dfn_zipped)
        with zipfile.ZipFile(dfn_zipped, "w") as f:
            f.write(dfn, dfn_zipped, zipfile.ZIP_DEFLATED)
        os.remove(dfn)


if platform.system() == "Windows":
    # Windows does not support ANSI escapes and we are using API calls to
    # set the console color
    logging.StreamHandler.emit = add_coloring_to_emit_windows(
        logging.StreamHandler.emit
    )
else:
    # all non-Windows platforms are supporting ANSI escapes so we use them
    logging.StreamHandler.emit = add_coloring_to_emit_ansi(logging.StreamHandler.emit)


logger = logging.getLogger()

signalint = False


class QueueManager(SyncManager):
    pass


class DateTimebytesEncoderjson(json.JSONEncoder):
    """
    Used to handle datetime in json files.
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode("utf-8")
        else:
            return json.JSONEncoder.default(self, obj)


class MUCBot(ClientXMPP):
    """
    bot xmpp
    """

    def __init__(
        self,
        conf,
        queue_recv_tcp_to_xmpp,
        queueout,
        eventkilltcp,
        eventkillpipe,
        pidprogrammprincipal,
        shared_dict,
        terminate_lock,
        alternatifconnection,
    ):
        logging.debug(
            "initialise agent xmpp  %s Type %s" % (conf.jidagent, conf.agenttype)
        )

        # object partage
        self.alternatifconnection = alternatifconnection
        self.shared_dict = shared_dict
        self.massage_reconection = "ask connect"
        self.demandeRestartBot_bool = False

        # self.connectcount = 0
        self.iq_msg = file_message_iq(dev_mod=True)
        self.pidprogrammprincipal = pidprogrammprincipal

        # self.reconnect_time_wait = 3
        # create mutex
        self.mutex = threading.Lock()
        self.mutexslotquickactioncount = threading.Lock()
        self.eventkilltcp = eventkilltcp
        self.eventkillpipe = eventkillpipe
        self.queue_recv_tcp_to_xmpp = queue_recv_tcp_to_xmpp
        self.queueout = queueout
        self.presencectrlsubscribe = "unavailable"
        self.concurrentquickdeployments = {}

        # create dir for descriptor syncthing deploy
        self.dirsyncthing = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "syncthingdescriptor"
        )
        if not os.path.isdir(self.dirsyncthing):
            os.makedirs(self.dirsyncthing, 0o755)
        ClientXMPP.__init__(self, jid.JID(conf.jidagent), conf.passwordconnection)
        laps_time_update_plugin = 3600
        laps_time_action_extern = 60
        laps_time_handlemanagesession = 20
        laps_time_check_established_connection = 900
        laps_time_send_ping_to_kiosk = 350
        logging.debug(
            "check connexion xmpp %ss" % laps_time_check_established_connection
        )
        self.back_to_deploy = {}
        self.config = conf

        self.ipconnection = self.config.Server

        format = "%(asctime)s - %(levelname)s - (XMPP)%(message)s"
        formatter = logging.Formatter(format)

        # update level log for slixmpp
        handler_slixmpp = logging.getLogger("slixmpp")
        logger.debug("slixmpp log level is %s" % self.config.log_level_slixmpp)
        handler_slixmpp.setLevel(self.config.log_level_slixmpp)

        if handler_slixmpp.handlers:
            hslixmpp = logger.handlers[
                0
            ]  # we assume the first handler is the one we want to configure
            hslixmpp.setFormatter(formatter)

        # _____________ verify network interface _____________
        # verifi si on a changer les interface pendant l'arret de l'agent.
        netfingerprintstart = createfingerprintnetwork()
        filetempinfolibfingerprint = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "lib",
            "INFOSTMP",
            "fingerprintnetwork",
        )
        logging.debug("filename: %s" % filetempinfolibfingerprint)
        if os.path.exists(filetempinfolibfingerprint):
            logging.debug(
                "current fingerprint file %s"
                % file_get_contents(filetempinfolibfingerprint)
            )
            if netfingerprintstart != file_get_contents(filetempinfolibfingerprint):
                logging.warning(
                    "after start : registration must update the information in the xmpp tables."
                )
                self.force_full_registration()
        # ______________________________________________________
        Env.agenttype = self.config.agenttype
        managepackage.agenttype = self.config.agenttype
        # creation object session
        self.session = session(self.config.agenttype)
        self.boolinventory = False  # cette variable permet de faire demander 1
        # inventaire suite a 1 changement de reseau.
        # inventaire sera demander quand l'agent sera dans 1 mode moins transitoire.
        # CREATE MANAGE SCHEDULER
        logging.debug("CREATION MANAGER PLUGINSCHEDULING")
        self.manage_scheduler = manage_scheduler(self)
        # Definition path directory plugin
        namelibplugins = "pluginsmachine"
        if self.config.agenttype in ["relayserver"]:
            namelibplugins = "pluginsrelay"
        self.modulepath = os.path.abspath(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), namelibplugins)
        )
        # totalise les sessions persistence de 10 secondes
        self.sessionaccumulator = {}
        self.charge_apparente_cluster = {}

        self.laps_time_networkMonitor = self.config.detectiontime
        self.pathagent = os.path.join(os.path.dirname(os.path.realpath(__file__)))
        self.img_agent = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "img_agent"
        )
        if os.path.isdir(self.img_agent):
            logging.debug("deleting directory %s" % self.img_agent)
            try:
                shutil.rmtree(self.img_agent)
            except Exception as e:
                logging.error(
                    "Cannot delete the directory %s : %s" % (self.img_agent, str(e))
                )

        self.Update_Remote_Agentlist = Update_Remote_Agent(self.pathagent, True)
        self.descriptorimage = Update_Remote_Agent(self.img_agent)
        self.descriptor_master = None
        if len(self.descriptorimage.get_md5_descriptor_agent()["program_agent"]) == 0:
            # Copy the agent to the img  (backup)
            if sys.platform.startswith("win"):
                program_agent_files = (
                    self.Update_Remote_Agentlist.get_md5_descriptor_agent()[
                        "program_agent"
                    ]
                )
                lib_agent_files = (
                    self.Update_Remote_Agentlist.get_md5_descriptor_agent()["lib_agent"]
                )
                script_agent_files = (
                    self.Update_Remote_Agentlist.get_md5_descriptor_agent()[
                        "script_agent"
                    ]
                )
                self.copy_files(self.pathagent, self.img_agent, program_agent_files)
                self.copy_files(self.pathagent, self.img_agent, ["agentversion"])
                self.copy_files(
                    os.path.join(self.pathagent, "lib"),
                    os.path.join(self.img_agent, "lib"),
                    lib_agent_files,
                )
                self.copy_files(
                    os.path.join(self.pathagent, "script"),
                    os.path.join(self.img_agent, "script"),
                    script_agent_files,
                )
            elif sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                try:
                    base_py_files = glob.glob(os.path.join(self.pathagent, "*.py"))
                    for py_file in base_py_files:
                        shutil.copy(py_file, self.img_agent)

                    source_dir = os.path.join(self.pathagent, "script")
                    destination_dir = os.path.join(self.img_agent, "script")
                    script_py_files = glob.glob(os.path.join(source_dir, "*.py"))
                    for py_file in script_py_files:
                        shutil.copy(py_file, destination_dir)

                    lib_py_files = glob.glob(
                        os.path.join(self.pathagent, "lib", "*.py")
                    )
                    for py_file in lib_py_files:
                        shutil.copy(py_file, os.path.join(self.img_agent, "lib"))

                    shutil.copy(
                        os.path.join(self.pathagent, "agentversion"),
                        os.path.join(self.img_agent, "agentversion"),
                    )
                except Exception as e:
                    logger.error(f"An error occurred while copying files: {str(e)}")
            else:
                logger.error("Your system is not supported.")

        # We update the descriptors now that the image is copied.
        self.descriptorimage = Update_Remote_Agent(self.img_agent)

        if self.config.updating != 1:
            logging.debug("The updating configuration is set to 0")
            logging.debug(
                "We will not update the agent if a new version is available on the server"
            )

        if self.config.updatingplugins != 1:
            logging.debug("The updatingplugins configuration is set to 0")
            logging.debug(
                "We will not update the plugins if new versions are available on the server"
            )

        if (
            self.descriptorimage.get_fingerprint_agent_base()
            != self.Update_Remote_Agentlist.get_fingerprint_agent_base()
        ):
            self.agentupdating = True
            logging.warning("Agent installed is different from agent on master.")

        if self.config.agenttype in ["machine"]:
            # on appelle cette fonction toutes les 30 seconde
            self.schedule(
                "reinjection_deplot_message_box",
                30,
                self.reinjection_deplot_message_box,
                repeat=True,
            )
            # on appelle cette fonction a 200 seconde apres 1 restart.
            self.schedule(
                "reinjection_deploy_protected",
                60,
                self.reinjection_deploy_protected,
                repeat=False,
            )

        # initialise charge relay server
        if self.config.agenttype in ["relayserver"]:
            self.managefifo = fifodeploy()
            self.levelcharge = {}
            self.levelcharge["machinelist"] = []
            self.levelcharge["charge"] = 0
            # supprime les reverses ssh inutile
            self.manage_persistence_reverse_ssh = reverse_port_ssh()
        self.jidclusterlistrelayservers = {}
        self.machinerelayserver = []
        self.nicklistchatroomcommand = {}
        self.jidchatroomcommand = jid.JID(self.config.jidchatroomcommand)
        self.agentcommand = jid.JID(self.config.agentcommand)

        if not testagentconf(self.config.agenttype):
            # We remove the fingerprint file
            pathfingerprint = os.path.join(Setdirectorytempinfo(), "fingerprintconf")
            logger.error("configuration error del fingerprint %s" % pathfingerprint)
            if os.path.isfile(pathfingerprint):
                os.remove(pathfingerprint)
                logger.error("configuration error del fingerprint %s" % pathfingerprint)
        self.agentsiveo = self.config.jidagentsiveo

        self.agentmaster = jid.JID("master@pulse")
        self.sub_subscribe_all = []
        if not hasattr(self.config, "sub_subscribe"):
            self.sub_subscribe = jid.JID("master_subs@pulse")
        else:
            if isinstance(self.config.sub_subscribe, list):
                self.sub_subscribe_all = [jid.JID(x) for x in self.config.sub_subscribe]
            if (
                isinstance(self.config.sub_subscribe, list)
                and len(self.config.sub_subscribe) > 0
            ):
                self.sub_subscribe = jid.JID(self.config.sub_subscribe[0])
            else:
                self.sub_subscribe_all = [jid.JID(self.config.sub_subscribe)]
                self.sub_subscribe = jid.JID(self.config.sub_subscribe)

        if not hasattr(self.config, "sub_logger"):
            self.sub_logger = jid.JID("master_log@pulse")
        else:
            if (
                isinstance(self.config.sub_logger, list)
                and len(self.config.sub_logger) > 0
            ):
                self.sub_logger = jid.JID(self.config.sub_logger[0])
            else:
                self.sub_logger = jid.JID(self.config.sub_logger)

        if self.sub_subscribe.bare == "":
            self.sub_subscribe = jid.JID("master_subs@pulse")

        if not hasattr(self.config, "sub_inventory"):
            self.sub_inventory = jid.JID("master_inv@pulse")
        else:
            if (
                isinstance(self.config.sub_inventory, list)
                and len(self.config.sub_inventory) > 0
            ):
                self.sub_inventory = jid.JID(self.config.sub_inventory[0])
            else:
                self.sub_inventory = jid.JID(self.config.sub_inventory)
        if self.sub_inventory.bare == "":
            self.sub_inventory = jid.JID("master_inv@pulse")

        if not hasattr(self.config, "sub_registration"):
            self.sub_registration = jid.JID("master_reg@pulse")
        else:
            if (
                isinstance(self.config.sub_registration, list)
                and len(self.config.sub_registration) > 0
            ):
                self.sub_registration = jid.JID(self.config.sub_registration[0])
            else:
                self.sub_registration = jid.JID(self.config.sub_registration)
        if self.sub_registration.bare == "":
            self.sub_registration = jid.JID("master_reg@pulse")

        if not hasattr(self.config, "sub_monitoring"):
            self.sub_monitoring = jid.JID("master_mon@pulse")
        else:
            if (
                isinstance(self.config.sub_monitoring, list)
                and len(self.config.sub_monitoring) > 0
            ):
                self.sub_monitoring = jid.JID(self.config.sub_monitoring[0])
            else:
                self.sub_monitoring = jid.JID(self.config.sub_monitoring)
        if self.sub_monitoring.bare == "":
            self.sub_monitoring = jid.JID("master_mon@pulse")

        if not hasattr(self.config, "sub_updates"):
            self.sub_updates = jid.JID("master_upd@pulse")
        else:
            if (
                isinstance(self.config.sub_updates, list)
                and len(self.config.sub_updates) > 0
            ):
                self.sub_updates = jid.JID(self.config.sub_updates[0])
            else:
                self.sub_updates = jid.JID(self.config.sub_updates)
        if self.sub_updates.bare == "":
            self.sub_updates = jid.JID("master_upd@pulse")

        if sys.platform.startswith("linux"):
            if self.config.agenttype in ["relayserver"]:
                self.fichierconfsyncthing = os.path.join(
                    self.config.syncthing_home, "config.xml"
                )
                conf_ars_deploy(
                    self.config.syncthing_port,
                    configfile=self.fichierconfsyncthing,
                    deviceName="pulse",
                )
            else:
                self.fichierconfsyncthing = os.path.join(
                    os.path.expanduser("~pulseuser"),
                    ".config",
                    "syncthing",
                    "config.xml",
                )
            self.tmpfile = "/tmp/confsyncting.txt"
        elif sys.platform.startswith("win"):
            self.fichierconfsyncthing = os.path.join(
                directoryconffile(), "syncthing", "config.xml"
            )
            self.tmpfile = os.path.join(pulseTempDir(), "confsyncting.txt")
        elif sys.platform.startswith("darwin"):
            self.fichierconfsyncthing = os.path.join(
                directoryconffile(), "syncthing", "config.xml"
            )
            self.tmpfile = "/tmp/confsyncting.txt"
        # TODO: Disable this try if synthing is not activated. Prevent backtraces
        if self.config.syncthing_on and os.path.isfile(self.fichierconfsyncthing):
            try:
                hostnameiddevice = None
                if self.boundjid.domain == "pulse":
                    hostnameiddevice = "pulse"
                self.deviceid = iddevice(
                    configfile=self.fichierconfsyncthing, deviceName=hostnameiddevice
                )
            except Exception:
                self.deviceid = ""
                pass
        else:
            self.deviceid = ""

        if self.config.agenttype in ["relayserver"]:
            # We remove the start sessions of the agent.
            # As long as the Relayserver Agent isn't started, the sesion queues
            # where the deploy has failed are not useful
            self.session.clearallfilesession()

        if self.config.agenttype in ["machine"]:
            self.schedule("stabilized_start", 120, self.stabilized_start, repeat=True)

        self.schedule("subscription", 1800, self.unsubscribe_agent, repeat=True)
        self.reversessh = None
        self.reversesshmanage = {}
        self.signalinfo = {}
        self.queue_read_event_from_command = Queue()
        self.xmppbrowsingpath = xmppbrowsing(
            defaultdir=self.config.defaultdir,
            rootfilesystem=self.config.rootfilesystem,
            objectxmpp=self,
        )
        self.ban_deploy_sessionid_list = set()  # List id sessions that are banned
        self.lapstimebansessionid = 900  # ban session id 900 secondes
        self.banterminate = {}  # used for clear id session banned
        if self.config.sched_remove_ban:
            self.schedule(
                "removeban",
                30,
                self.remove_sessionid_in_ban_deploy_sessionid_list,
                repeat=True,
            )
        self.Deploybasesched = ManageDbScheduler()
        self.eventkiosk = manage_kiosk_message(self.queue_recv_tcp_to_xmpp, self)
        self.infolauncherkiook = manageskioskdb()
        self.kiosk_presence = "False"
        self.eventmanage = manage_event(self.queue_read_event_from_command, self)
        self.mannageprocess = mannageprocess(self.queue_read_event_from_command)
        self.process_on_end_send_message_xmpp = process_on_end_send_message_xmpp(
            self.queue_read_event_from_command
        )
        if self.config.sched_check_connection:
            self.schedule(
                "check established connection",
                laps_time_check_established_connection,
                self.established_connection,
                repeat=True,
            )
        if self.config.agenttype in ["relayserver"]:
            # scheduled task that calls the slot plugin for sending the quick
            # deployments that have not been processed.
            if self.config.sched_quick_deployment_load:
                self.schedule(
                    "Quick deployment load", 15, self.QDeployfile, repeat=True
                )

        if not hasattr(self.config, "geolocalisation"):
            self.config.geolocalisation = True
        if not hasattr(self.config, "request_type"):
            self.config.request_type = "public"

        self.geodata = None
        if self.config.geolocalisation:
            self.geodata = geolocalisation_agent(
                typeuser=self.config.request_type,
                geolocalisation=self.config.geolocalisation,
                ip_public=self.config.public_ip,
                strlistgeoserveur=self.config.geoservers,
            )

            self.config.public_ip = self.geodata.get_ip_public()
        if self.config.public_ip == "" or self.config.public_ip is None:
            self.config.public_ip = None

        self.md5reseau = refreshfingerprint()
        if self.config.sched_scheduled_plugins:
            self.schedule("schedulerfunction", 10, self.schedulerfunction, repeat=True)
        if self.config.sched_update_plugin:
            self.schedule(
                "update plugin",
                laps_time_update_plugin,
                self.update_plugin,
                repeat=True,
            )
        self.schedule("check reconf file", 300, self.checkreconf, repeat=True)

        if self.config.netchanging == 1:
            logging.debug("Network Changing enable")
            if self.config.sched_check_network:
                self.schedule(
                    "check network",
                    self.laps_time_networkMonitor,
                    self.networkMonitor,
                    repeat=True,
                )
        else:
            logging.debug("Network Changing disable")

        if self.config.agenttype not in ["relayserver"]:
            self.schedule(
                "check_subscribe",
                self.config.time_before_reinscription,
                self.check_subscribe,
                repeat=True,
            )
            if self.config.sched_send_ping_kiosk:
                self.schedule(
                    "send_ping",
                    laps_time_send_ping_to_kiosk,
                    self.send_ping_to_kiosk,
                    repeat=True,
                )
        if self.config.sched_update_agent:
            self.schedule(
                "check AGENT INSTALL", 350, self.checkinstallagent, repeat=True
            )
        if self.config.sched_manage_session:
            self.schedule(
                "manage session",
                laps_time_handlemanagesession,
                self.handlemanagesession,
                repeat=True,
            )
        if self.config.agenttype in ["relayserver"]:
            if self.config.sched_reload_deployments:
                self.schedule("reloaddeploy", 15, self.reloaddeploy, repeat=True)

            # Update remote agent
            self.diragentbase = os.path.join(
                "/", "var", "lib", "pulse2", "xmpp_baseremoteagent"
            )
            self.Update_Remote_Agentlist = Update_Remote_Agent(self.diragentbase, True)
        # we make sure that the temp for the inventories is greater than or equal to 1 hour.
        # if the time for the inventories is 0, it is left at 0.
        # this deactive cycle inventory
        if self.config.inventory_interval != 0:
            if self.config.inventory_interval < 3600:
                self.config.inventory_interval = 3600
                logging.warning("change minimun time cyclic inventory : 3600")
                logging.warning(
                    "we make sure that the time for "
                    " the inventories is greater than or equal to 1 hour."
                )
            if self.config.sched_check_inventory:
                self.schedule(
                    "event inventory",
                    self.config.inventory_interval,
                    self.handleinventory,
                    repeat=True,
                )
        else:
            logging.debug("The cyclic inventory feature is disabled")

        if self.config.agenttype not in ["relayserver"]:
            if self.config.sched_session_reload:
                self.schedule("session reload", 15, self.reloadsesssion, repeat=False)
        if self.config.sched_check_events:
            self.schedule(
                "reprise_evenement", 10, self.handlereprise_evenement, repeat=True
            )

        # Parameters for the agent connexion
        self.add_event_handler("register", self.register)
        self.add_event_handler("connecting", self.handle_connecting)
        self.add_event_handler("connected", self.handle_connected)
        self.add_event_handler("connection_failed", self.handle_connection_failed)
        self.add_event_handler("disconnected", self.handle_disconnected)

        self.add_event_handler("session_start", self.start)
        self.add_event_handler("message", self.message)
        self.add_event_handler(
            "signalsessioneventrestart", self.signalsessioneventrestart
        )
        self.add_event_handler("loginfotomaster", self.loginfotomaster)

        self.add_event_handler("changed_status", self.changed_status)

        self.add_event_handler("presence_unavailable", self.presence_unavailable)
        self.add_event_handler("presence_available", self.presence_available)

        self.add_event_handler("presence_subscribe", self.presence_subscribe)
        self.add_event_handler("presence_subscribed", self.presence_subscribed)

        self.add_event_handler("presence_unsubscribe", self.presence_unsubscribe)
        self.add_event_handler("presence_unsubscribed", self.presence_unsubscribed)

        self.add_event_handler("changed_subscription", self.changed_subscription)

        self.RSA = MsgsignedRSA(self.boundjid.user)
        logger.info("The version of the agent is %s" % self.version_agent())

        if sys.platform.startswith("win"):
            result = win32api.SetConsoleCtrlHandler(self._CtrlHandler, 1)
            if result == 0:
                logger.debug(
                    "Could not SetConsoleCtrlHandler (error %r)"
                    % win32api.GetLastError()
                )
            else:
                logger.debug("Set handler for console events.")
                self.is_set = True
        elif sys.platform.startswith("linux"):
            signal.signal(signal.SIGINT, self.signal_handler)
        elif sys.platform.startswith("darwin"):
            signal.signal(signal.SIGINT, self.signal_handler)

        if self.config.sched_check_cmd_file:
            self.schedule(
                "execcmdfile", laps_time_action_extern, self.execcmdfile, repeat=True
            )
        if self.config.sched_init_syncthing:
            self.schedule("initsyncthing", 15, self.initialise_syncthing, repeat=False)

        # Alternatf Configuration agent Machine
        self.brestartbot = False
        if self.config.agenttype in ["relayserver"]:
            self.startmode = (
                True  # si connection echoue on charge alternative configuration
            )
            self.alternatifconnection = {}  # alternative connection for machine

        self.register_handler(
            Callback(
                "Handle_custom_xep",
                MatchXPath("{%s}iq/{custom_xep}query" % (self.default_ns)),
                self._custom_xep_iq,
            )
        )

        self.register_handler(
            CoroutineCallback(
                "iq_error_Handle",
                StanzaPath("/iq@type=error"),
                self._iq_error_Handle,
            )
        )

    def sendbigdatatoagent(self, jid_receiver, data_utf8_json, segment_size=65535):
        """
        Envoie de gros volumes de données à un agent XMPP en plusieurs segments.

        Args:
            jid_receiver (str): Le JID du destinataire.
            data_utf8_json (str): Les données JSON à envoyer, en format UTF-8.
            segment_size (int, optional): La taille maximale de chaque segment (par défaut: 65535).

        Returns:
            None
        """
        # Vérification si le message est assez gros pour nécessiter un découpage en segments
        if len(data_utf8_json) > segment_size:
            # Génération d'un identifiant de session
            sessionid = getRandomName(6, "big_data")
            # Compression et encodage en base64
            data_compressed = zlib.compress(data_utf8_json.encode("utf-8"))
            data_base64 = base64.b64encode(data_compressed).decode("utf-8")

            # Calcul du nombre total de segments nécessaires
            nb_segments_total = (len(data_base64) + segment_size - 1) // segment_size

            # Envoi des segments
            for i in range(nb_segments_total):
                # Découpage des données en segments de taille segment_size
                segment = data_base64[i * segment_size : (i + 1) * segment_size]
                # Construction du message
                message = {
                    "action": "big_data",  # Action spécifiée pour le plugin à appeler
                    "sessionid": sessionid,  # Identifiant de session
                    "data": {
                        "segment": segment,  # Données de ce segment
                        "nb_segment": i + 1,  # Numéro du segment actuel
                        "nb_segment_total": nb_segments_total,  # Nombre total de segments
                        "from": self.boundjid.full,
                    },  # JID de l'expéditeur
                }
                # Envoi du message à jid_receiver
                self.send_message(
                    mto=jid_receiver, mbody=json.dumps(message), mtype="chat"
                )
        else:
            # Envoi direct du message sans découpage
            self.send_message(mto=jid_receiver, mbody=data_utf8_json, mtype="chat")

    def copy_files(self, source_dir, target_dir, file_list):
        """
        Copy files from the source directory to the target directory.

        Args:
            source_dir (str): The path to the source directory.
            target_dir (str): The path to the target directory.
            file_list (list): A list of file names to copy.

        Returns:
            None

        Raises:
        IOError: If an error occurs while copying files.
        """
        for file_agent in file_list:
            source_file = os.path.join(source_dir, file_agent)
            target_file = os.path.join(target_dir, file_agent)
            if not os.path.isfile(target_file):
                try:
                    shutil.copy(source_file, target_file)
                except Exception as e:
                    logger.error(f"An error occurred while copying files: {str(e)}")

    def iqsendpulse(self, to, datain, timeout=900, sessionid=None):
        """
        attention cette appelle ne peut ce faire que dans 1 plugin appeller par call_plugin
        pour la gestion des iq dans la boucle asyncio principal,
        utiliser la fonction slixmpp
        """
        thread = Myiq(self, to, datain, timeout=timeout, sessionid=None)
        result = thread.iqsend()
        return result

    async def _iq_error_Handle(self, iq):
        if iq["type"] == "error":
            errortext = iq["error"]["text"]
            if "User already exists" in errortext:
                # ce n'est pas 1 erreur iq
                logger.warning("User already exists")
                return
            ret = '{"err" : "%s"}' % errortext
            logger.error("IQ error : %s" % ret)

    def _custom_xep_get(self, iq):
        def reply_result(iq, result_str_base64):
            # warning encodage et decodage base64 se fait sur du bytes.
            # ici convertit en str pour etre inclut au message.
            logger.debug("reply iq")
            iq["type"] = "result"
            iq["to"] = iq["from"]
            iq["from"] = self.boundjid.bare
            for childresult in iq.xml:
                if childresult.tag.endswith("query"):
                    for z in childresult:
                        # composition message
                        z.tag = "{%s}data" % result
                iq.send()

        def reply_error(iq, str_message_erreur):
            # warning encodage et decodage base64 se fait sur du bytes.
            # ici convertit en str pour etre inclut au message.
            logger.debug("reply error")
            iq["type"] = "error"
            iq["to"] = iq["from"]
            iq["from"] = self.boundjid.bare
            iq["error"]["text"] = str_message_erreur
            messageerror = """{ "error" : "%s" }""" % str_message_erreur
            for childresult in iq.xml:
                if childresult.tag.endswith("query"):
                    for z in childresult:
                        # composition message
                        z.tag = "{%s}data" % messageerror
                iq.send()

        logger.debug("traitement de iq get custom_xep")
        for child in iq.xml:
            if child.tag.endswith("query"):
                # select data element query
                for z in child:
                    # recuperation (bytes data) encode en base64
                    data = z.tag[1:-5]
                    try:
                        data = base64.b64decode(data)
                    except Exception as e:
                        logger.error("_custom_xep_get :decodage erreur : %s" % str(e))
                        logger.error("\n%s" % (traceback.format_exc()))
                        logger.error("xml recv : %s " % str(e))
                        return
                    try:
                        # traitement de la function
                        # result json str
                        logger.debug(" traitement iq get [session %s]" % iq["id"])
                        result = dispach_iq_command(self, data)
                        try:
                            result = convert.encode_to_string_base64(result)
                            reply_result(iq, result)
                        except Exception as e:
                            logger.error(
                                "_custom_xep_get : encode base64 : %s" % str(e)
                            )
                            logger.error("\n%s" % (traceback.format_exc()))
                            reply_error(iq, str(e))
                            return ""
                    except Exception as e:
                        logger.error("_custom_xep_get : error command : %s" % str(e))
                        logger.error("\n%s" % (traceback.format_exc()))
                        self.reply_error(iq, str(e))
                        return

    def terminateprogram(self):
        """
        End the program according to.
        If the platform is Windows the agent is necessarily of type machine only, the function uses the 'TASKKILL' command to forcefully terminate the process if it remains blocked.
            sets the specified process id (self.pidprogrammprincipal).
        If the platform is not Windows, the function constructs a command to find and kill the process based on the
        machine or relayserver configuration agent type.

        the function constructs a command to find and kill a process with 'machine' or 'agentxmpp' in its name.
        Feedback:
            None
        """
        if sys.platform.startswith("win"):
            cmd = "TASKKILL /F /PID %s /T" % self.pidprogrammprincipal
            os.system(cmd)
        else:
            if self.config.agenttype in ["relayserver"]:
                cmd = "ps -ef | grep 'relayserver' | grep 'agentxmpp' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
            else:
                cmd = "ps -ef | grep 'machine' | grep 'agentxmpp' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
            os.system(cmd)

    def _custom_xep_iq(self, iq):
        """
        Handle a custom XEP IQ message recu.
        Traitement de la reception iq xmlns espace de nom custom_xep
        filtre xml : MatchXPath('{%s}iq/{custom_xep}query' % (self.default_ns)),
        exclusion des iq venant de l'agent directement.
        If the IQ message is a "get" type, the function logs a debug message and calls the _custom_xep_get
        function to process the IQ.

        Parameters:
            self (object): The instance of the class containing the function.
            iq (Stanza): The IQ message to be handled.

        Returns:
            None
        """
        # master@pulse/MASTER dev-deb12-2.zb0@pulse
        if str(iq["from"]) == self.boundjid.bare or iq["query"] != "custom_xep":
            logger.debug("PAs de traitement de cet handler")
            return
        # traitement de iq
        if iq["type"] == "get":
            logger.debug("GET Traitement iq custom siveo")
            self._custom_xep_get(iq)

    def _custom_xep_get(self, iq):
        """
        Processes a "get" request of a custom IQ message (custom XEP).

           This function handles custom IQ messages of type "get" by extracting the data from the message.
           It calls the appropriate processing function (dispach_iq_command) after formatting the request to obtain the result.
           Note: dispach_iq_command only handles a predefined set of permitted commands.

           The result is then encoded in base64 and returned.

           If an error occurs during data retrieval or processing, an "error" response is sent with the appropriate error message.

           Parameters:
           self (object): The instance of the class containing the function.
           iq (Stanza): The IQ message to be processed.

           Returns:
           None
        """

        def reply_result(iq, result_str_base64):
            # warning encodage et decodage base64 se fait sur du bytes.
            # ici convertit en str pour etre inclut au message.
            logger.debug("reply iq")
            iq["type"] = "result"
            iq["to"] = iq["from"]
            iq["from"] = self.boundjid.bare
            for childresult in iq.xml:
                if childresult.tag.endswith("query"):
                    for z in childresult:
                        # composition message
                        z.tag = "{%s}data" % result
                iq.send()

        def reply_error(iq, str_message_erreur):
            # warning encodage et decodage base64 se fait sur du bytes.
            # ici convertit en str pour etre inclut au message.
            logger.debug("reply error")
            iq["type"] = "error"
            iq["to"] = iq["from"]
            iq["from"] = self.boundjid.bare
            iq["error"]["text"] = str_message_erreur
            messageerror = """{ "error" : "%s" }""" % str_message_erreur
            for childresult in iq.xml:
                if childresult.tag.endswith("query"):
                    for z in childresult:
                        # composition message
                        z.tag = "{%s}data" % messageerror
                iq.send()

        logger.debug("processing of iq get custom xep")
        for child in iq.xml:
            if child.tag.endswith("query"):
                # select data element query
                for z in child:
                    # recuperation (bytes data) encode en base64
                    data = z.tag[1:-5]
                    try:
                        data = base64.b64decode(data)
                    except Exception as e:
                        logger.error("_custom_xep_get : decoding error : %s" % str(e))
                        logger.error("\n%s" % (traceback.format_exc()))
                        logger.error("xml recv : %s " % str(e))
                        return
                    try:
                        # traitement de la function
                        # result json str
                        logger.debug("iq get treatment [session %s]" % iq["id"])
                        result = dispach_iq_command(self, data)
                        try:
                            result = convert.encode_to_string_base64(result)
                            reply_result(iq, result)
                        except Exception as e:
                            logger.error(
                                "_custom_xep_get : encode base64 : %s" % str(e)
                            )
                            logger.error("\n%s" % (traceback.format_exc()))
                            reply_error(iq, str(e))
                            return ""
                    except Exception as e:
                        logger.error("_custom_xep_get : error command : %s" % str(e))
                        logger.error("\n%s" % (traceback.format_exc()))
                        self.reply_error(iq, str(e))
                        return

    def restartBot(self, wait=10):
        """
        on relance xmpp dans agent
        il relit la conf...
        """
        self.brestartbot = True  # boucle reinitialise.
        logger.debug(
            "We restart the medulla agent for the machine %s" % self.boundjid.user
        )
        # self.demandeRestartBot_bool = True
        self.disconnect(wait=wait)

    def handle_disconnected(self, data):
        logger.debug(f"handle_disconnected {self.server_address}")
        with terminate_lock:
            if self.shared_dict.get("terminate"):
                logger.debug(f"handle_disconnected TERMINATE")
                if sys.platform.startswith("win"):
                    try:
                        # on debloque le pipe
                        fileHandle = win32file.CreateFile(
                            "\\\\.\\pipe\\interfacechang",
                            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                            0,
                            None,
                            win32file.OPEN_EXISTING,
                            0,
                            None,
                        )
                        win32file.WriteFile(fileHandle, "terminate")
                        fileHandle.Close()
                        time.sleep(2)
                    except Exception as e:
                        logger.error("\n%s" % (traceback.format_exc()))
                        pass
                self.loop.stop()
                return
        #
        # if self.demandeRestartBot_bool:
        #     logger.debug(f"Restart Bot")
        #     self.reconnect(0.0, self.massage_reconection)
        #     return

        if self.config.agenttype in ["relayserver"]:
            self.reconnect(0.0, self.massage_reconection)
            return

        # if self.shared_dict.get("alternative"):
        # reconnecte alternative JFK
        if (
            self.alternatifconnection["nextserver"]
            > self.alternatifconnection["nbserver"]
        ):
            self.alternatifconnection["nextserver"] = 1

        index_server_list = self.alternatifconnection["nextserver"] - 1
        arsconnection = self.alternatifconnection["listars"][index_server_list]
        self.config.Port = self.alternatifconnection[arsconnection]["port"]
        self.config.Server = self.alternatifconnection[arsconnection]["server"]
        self.config.guacamole_baseurl = self.alternatifconnection[arsconnection][
            "guacamole_baseurl"
        ]
        serverjid = str(arsconnection)
        try:
            self.config.confdomain = str(arsconnection).split("@")[1].split("/")[0]
        except BaseException:
            self.config.confdomain = str(serverjid)
        logger.debug(
            f"Rewriting cluster.ini file: {self.config.Port} {ipfromdns(self.config.Server)} {arsconnection} {self.config.guacamole_baseurl}"
        )
        changeconnection(
            conffilename(self.config.agenttype),
            self.config.Port,
            ipfromdns(self.config.Server),
            arsconnection,
            self.config.guacamole_baseurl,
        )

        self.alternatifconnection["nextserver"] = (
            self.alternatifconnection["nextserver"]
        ) + 1
        if (
            self.alternatifconnection["nextserver"]
            > self.alternatifconnection["nbserver"]
        ):
            self.alternatifconnection["nextserver"] = 1

        # write alternative configuration
        create_config_file_atomically(
            conffilenametmp("cluster"), self.alternatifconnection
        )
        create_config_file_atomically(
            conffilename("cluster"), self.alternatifconnection
        )
        self.address = (
            ipfromdns(self.config.Server),
            int(self.config.Port),
        )
        self.server_address = self.address
        self.shared_dict["new_connection"] = True
        self.loop.stop()

    def handle_connection_failed(self, data):
        logger.debug(f"handle_connection_failed {self.server_address}")
        self.disconnect()

    def handle_connecting(self, data):
        """
        success connecting agent
        """
        pass

    def handle_connected(self, data):
        self.demandeRestartBot_bool = False
        logger.info(f"connected {self.server_address}")
        with terminate_lock:
            if self.shared_dict["reconnect"] == True:
                pass
            self.shared_dict["compteur"] = 0
            self.shared_dict["reconnect"] = False

    def reconnect_agent(self, msg="deconection", delay=0):
        self.massage_reconection = msg
        self._connect_loop_wait = 0
        if delay:
            self.disconnect(delay)
        else:
            self.disconnect()

    def reconnect_alternative(self, alternative=True, delay=0):
        self.alternative = alternative
        if delay:
            self.disconnect(delay)
        else:
            self.disconnect()
        loop1 = asyncio.get_event_loop()
        loop1.stop()

    def quit_application(self, wait=2):
        """
        Quit the application gracefully.

        This function logs a debug message indicating the intention to quit the application.
        It sets the restart flag to 0, indicating that the application should not restart.
        Then, it disconnects from the server, allowing for a graceful shutdown.

        Parameters:
            self (object): The instance of the class containing the function.
            wait (int): The number of seconds to wait before disconnecting (default: 2).

        Returns:
            None
        """
        logger.debug("Quit Application")

        self.queue_read_event_from_command.put("quit")
        # termine server kiosk
        self.eventkiosk.quit()
        self.eventkilltcp.set()
        self.eventkillpipe.set()
        time.sleep(1)
        if sys.platform.startswith("win"):
            try:
                # on debloque le pipe
                fileHandle = win32file.CreateFile(
                    "\\\\.\\pipe\\interfacechang",
                    win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                    0,
                    None,
                    win32file.OPEN_EXISTING,
                    0,
                    None,
                )
                win32file.WriteFile(fileHandle, "terminate")
                fileHandle.Close()
                time.sleep(2)
            except Exception as e:
                # logger.error("\n%s" % (traceback.format_exc()))
                pass
        with terminate_lock:
            self.shared_dict["terminate"] = True
        self.startdata = -1
        logger.debug("byby session xmpp")
        loop1 = asyncio.get_event_loop()
        loop1.stop()

    async def register(self, iq):
        """
        Fill out and submit a registration form.

        The form may be composed of basic registration fields, a data form,
        an out-of-band link, or any combination thereof. Data forms and OOB
        links can be checked for as so:

        if iq.match('iq/register/form'):
            # do stuff with data form
            # iq['register']['form']['fields']
        if iq.match('iq/register/oob'):
            # do stuff with OOB URL
            # iq['register']['oob']['url']

        To get the list of basic registration fields, you can use:
            iq['register']['fields']
        """
        resp = self.Iq()
        resp["type"] = "set"
        resp["register"]["username"] = self.boundjid.user
        resp["register"]["password"] = self.password
        try:
            await resp.send()
            logger.info("Account created for %s!" % self.boundjid)
        except IqError as e:
            logger.debug("Could not register account: %s" % e.iq["error"]["text"])
        except IqTimeout:
            logger.error("Could not register account No response from server.")
            self.disconnect()

    def check_subscribe(self):
        if self.presencectrlsubscribe != "available":
            logger.debug(
                "no subscribe change received. we do not start 1 recording [%s]"
                % (self.sub_subscribe)
            )
        else:
            self.presencectrlsubscribe = "unavailable"
            self.update_plugin()

    def stabilized_start(self):
        """
        It creates a file called BOOL_FILE_CONTROL_WATCH_DOG with
        inside the pid and a date when it has been created.
        It is used to see if the program runs correctly.
        """
        directory_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "INFOSTMP"
        )
        BOOL_FILE_CONTROL_WATCH_DOG = os.path.join(
            directory_file, "BOOL_FILE_CONTROL_WATCH_DOG"
        )
        pidprocess = "process %s :(%s)" % (os.getpid(), str(datetime.now()))
        logger.debug("creation %s [pid %s]" % (BOOL_FILE_CONTROL_WATCH_DOG, pidprocess))
        file_put_contents(BOOL_FILE_CONTROL_WATCH_DOG, pidprocess)

        logger.debug(
            "creation BOOL_FILE_CONTROL_WATCH_DOG in %s" % BOOL_FILE_CONTROL_WATCH_DOG
        )

    def QDeployfile(self):
        """
        Cette fonction déploie un fichier.

        :return: None
        """
        sessioniddata = getRandomName(6, "Qdeployfile")
        dataerreur = {
            "action": "resultqdeploy",
            "sessionid": sessioniddata,
            "ret": 255,
            "base64": False,
            "data": {"msg": "Deployment error"},
        }
        transfertdeploy = {
            "action": "slot_quickdeploy_count",
            "sessionid": sessioniddata,
            "data": {"subaction": "deployfile"},
            "ret": 0,
            "base64": False,
        }

        msg = {"from": self.boundjid.bare, "to": self.boundjid.bare, "type": "chat"}
        call_plugin(
            transfertdeploy["action"],
            self,
            transfertdeploy["action"],
            transfertdeploy["sessionid"],
            transfertdeploy["data"],
            msg,
            dataerreur,
        )

    def __dirsessionreprise(self):
        dir_reprise_session = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "lib", "INFOSTMP", "REPRISE"
        )
        if not os.path.exists(dir_reprise_session):
            os.makedirs(dir_reprise_session, mode=0o007)
        return dir_reprise_session

    def __clean_message_box(self):
        # creation repertoire si probleme
        dir_reprise_session = self.__dirsessionreprise()
        # lit repertoire de fichier
        filelist = [
            x
            for x in os.listdir(dir_reprise_session)
            if os.path.isfile(os.path.join(dir_reprise_session, x))
            and x.startswith("medulla_messagebox")
        ]
        for t in filelist:
            filenamejson = os.path.join(dir_reprise_session, t)
            detection = t.split("@_@")
            try:
                with open(filenamejson, "r") as f:
                    data = json.load(f)
                    # signal error timeout reluanch
                    data["data"][
                        "repriseerror"
                    ] = "ABORT DEPLOYMENT SHUTDOWN [USER NO CHOICE]"
                    grafcet(self, data)
            except:
                logger.error("\n%s" % (traceback.format_exc()))
            finally:
                os.remove(filenamejson)
        return

    def reinjection_deploy_protected(self):
        # creation repertoire si probleme
        dir_reprise_session = self.__dirsessionreprise()
        timecurrent = int(time.mktime(time.localtime()))
        timecurentdatetime = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(int(timecurrent))
        )
        # lit repertoire de fichier
        filelist = [
            x
            for x in os.listdir(dir_reprise_session)
            if os.path.isfile(os.path.join(dir_reprise_session, x))
            and x.startswith("medulla_protected")
        ]
        for t in filelist:
            try:
                filenamejson = os.path.join(dir_reprise_session, t)
                detection = t.split("@_@")
                if (
                    len(detection) == 5
                    and detection[0] == "medulla_protected"
                    and (timecurrent + 60)
                    >= int(time.mktime(time.localtime(float(detection[1]))))
                ):
                    with open(filenamejson, "r") as f:
                        data = json.load(f)
                    datainfo = data["data"]
                    slotdep = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.gmtime(int(datainfo["stardate"]))
                    )
                    slotend = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.gmtime(int(datainfo["enddate"]))
                    )
                    if (timecurrent + 60) >= int(
                        time.mktime(time.gmtime(datainfo["stardate"]))
                    ) and (timecurrent + 60) <= int(
                        time.mktime(time.gmtime(datainfo["enddate"]))
                    ):
                        # on est toujours dans le temps de deployements on relance la tache protegee apres 1 shutdown
                        try:
                            data["data"]["repriseok"] = (
                                "<span class='log_warn'>Resumption deploy session id %s on"
                                " step %s restart machine (timeloacal %s)in slot deploy from server[ %s -> %s ]</span>"
                                % (
                                    detection[4],
                                    detection[3],
                                    timecurentdatetime,
                                    slotdep,
                                    slotend,
                                )
                            )
                            grafcet(self, data)
                            os.remove(filenamejson)
                        except:
                            logger.error(
                                "\nResumption deploy %s" % (traceback.format_exc())
                            )
                    else:
                        try:
                            data["data"]["repriseerror"] = (
                                "<span class='log_err'>ABORT DEPLOYMENT SHUTDOWN session id %s"
                                " step %s out slot deploy [ %s -> %s ]  (timeloacal machine %s)</span>"
                                % (
                                    detection[4],
                                    detection[3],
                                    slotdep,
                                    slotend,
                                    timecurentdatetime,
                                )
                            )
                            # reinjection for terminate deploy error correctement
                            grafcet(self, data)
                            os.remove(filenamejson)
                        except:
                            logger.error(
                                "\nABORT DEPLOYMENT SHUTDOWN %s"
                                % (traceback.format_exc())
                            )
                else:
                    logger.error("We are not in the interval yet")
                    break
            except:
                logger.error(
                    "reinjection deploy protected\n%s" % (traceback.format_exc())
                )
                os.rename(
                    filenamejson,
                    os.path.join(
                        os.path.dirname(filenamejson),
                        "error_session_grafcet" + os.path.basename(filenamejson),
                    ),
                )
        return

    def reinjection_deplot_message_box(self):
        # creation repertoire si probleme
        dir_reprise_session = self.__dirsessionreprise()
        # lit repertoire de fichier
        timecurrent_utc = datetime.now(timezone.utc)
        timecurrent_local = timecurrent_utc.astimezone()
        offset_seconds = timecurrent_local.utcoffset().total_seconds()
        timecurrent = timecurrent_utc.timestamp() + offset_seconds

        filelist = [
            x
            for x in os.listdir(dir_reprise_session)
            if os.path.isfile(os.path.join(dir_reprise_session, x))
            and x.startswith("medulla_messagebox")
        ]
        for t in filelist:
            try:
                filenamejson = os.path.join(dir_reprise_session, t)
                creation_time = os.path.getctime(filenamejson)
                time_elapsed = time.time() - creation_time

                with open(filenamejson, "r") as f:
                    data = json.load(f)
                datainfo = data["data"]

                stardate = float(datainfo.get("stardate", timecurrent))
                enddate = float(datainfo.get("enddate", timecurrent + 3600))

                if stardate <= timecurrent <= enddate:
                    logging.debug("Current time is within the deployment window.")

                    # Timeloop verification before relaunching the grafcet
                    sequence = datainfo.get("descriptor", {}).get("sequence", [])
                    for step in sequence:
                        if "timeloop" in step:
                            timeloop = step["timeloop"]
                            logging.debug(f"Timeloop duration: {timeloop} seconds")

                            if time_elapsed >= timeloop:
                                grafcet(self, data)
                                os.remove(filenamejson)
                            else:
                                remaining_time = timeloop - time_elapsed
                                logging.debug(
                                    f"Remaining time before relaunching grafcet: {remaining_time} seconds. Skipping to next."
                                )
                                continue
                        else:
                            logging.debug(
                                "No timeloop found, relaunching grafcet immediately."
                            )
                else:
                    logging.error("We are not in the interval yet.")
                    break
            except Exception as e:
                logging.error(f"Error processing file {e}: {traceback.format_exc()}")
                os.rename(
                    filenamejson,
                    os.path.join(
                        os.path.dirname(filenamejson),
                        "error_session_grafcet_" + os.path.basename(filenamejson),
                    ),
                )
        return

    # syncthing function
    def is_exist_folder_id(self, idfolder, config):
        for folder in config["folders"]:
            if folder["id"] == idfolder:
                return True
        return False

    def add_folder_dict_if_not_exist_id(self, dictaddfolder, config):
        if not self.is_exist_folder_id(dictaddfolder["id"], config):
            config["folders"].append(dictaddfolder)
            return True
        return False

    def add_device_in_folder_if_not_exist(
        self, folderid, keydevice, config, introducedBy=""
    ):
        result = False
        for folder in config["folders"]:
            if folderid == folder["id"]:
                # folder trouve
                for device in folder["devices"]:
                    if device["deviceID"] == keydevice:
                        # device existe
                        result = False
                new_device = {"deviceID": keydevice, "introducedBy": introducedBy}
                folder["devices"].append(new_device)
                result = True
        return result

    def is_exist_device_in_config(self, keydevicesyncthing, config):
        for device in config["devices"]:
            if device["deviceID"] == keydevicesyncthing:
                return True
        return False

    def add_device_syncthing(
        self,
        keydevicesyncthing,
        namerelay,
        config,
        introducer=False,
        autoAcceptFolders=False,
        address=["dynamic"],
    ):
        # test si device existe
        for device in config["devices"]:
            if device["deviceID"] == keydevicesyncthing:
                result = False
        logger.debug("add device syncthing %s" % keydevicesyncthing)
        dsyncthing_tmp = self.syncthing.create_template_struct_device(
            namerelay,
            str(keydevicesyncthing),
            introducer=introducer,
            autoAcceptFolders=autoAcceptFolders,
            address=address,
        )

        logger.debug(
            "add device [%s]syncthing to ars %s\n%s"
            % (keydevicesyncthing, namerelay, json.dumps(dsyncthing_tmp, indent=4))
        )

        config["devices"].append(dsyncthing_tmp)
        return dsyncthing_tmp

    def clean_pendingFolders_ignoredFolders_in_devices(self, config):
        for device in config["devices"]:
            if "pendingFolders" in device:
                del device["pendingFolders"]
            if "ignoredFolders" in device:
                del device["ignoredFolders"]

    def pendingdevice_accept(self, config):
        modif = False
        if "pendingDevices" in config and len(config["pendingDevices"]) != 0:
            # print "device trouve"
            for pendingdevice in config["pendingDevices"]:
                logger.info("pendingdevice %s" % pendingdevice)
                # exist device?
                if not self.is_exist_device_in_config(
                    pendingdevice["deviceID"], config
                ):
                    # add device
                    if pendingdevice["name"] == "":
                        continue
                    self.add_device_syncthing(
                        pendingdevice["deviceID"],
                        pendingdevice["name"],
                        config,
                        introducer=False,
                        autoAcceptFolders=False,
                        address=["dynamic"],
                    )
                    modif = True
                else:
                    pass
        # self.clean_pending(config)
        return modif

    def synchro_synthing(self):
        if not self.config.syncthing_on:
            return
        self.syncthingreconfigure = False
        logger.debug("synchro_synthing")
        # update syncthing
        if self.config.agenttype in ["relayserver"]:
            self.clean_old_partage_syncting()
        try:
            config = self.syncthing.get_config()  # content all config
        except Exception:
            return
        if len(config) == 0:
            return
        if "pendingDevices" in config and len(config["pendingDevices"]) > 0:
            if self.pendingdevice_accept(config):
                self.syncthingreconfigure = True
            config["pendingDevices"] = []
        if "remoteIgnoredDevices" in config:
            config["remoteIgnoredDevices"] = []
        if "defaultFolderPath" in config["options"]:
            # pas de pathfolder definie. warning.
            defaultFolderPath = config["options"]["defaultFolderPath"]
            for de in config["devices"]:
                if "pendingFolders" in de and len(de["pendingFolders"]) > 0:
                    # add folder
                    for devicefolder in de["pendingFolders"]:
                        path_folder = os.path.join(
                            defaultFolderPath, devicefolder["id"]
                        )
                        newfolder = self.syncthing.create_template_struct_folder(
                            devicefolder["label"], path_folder, id=devicefolder["id"]
                        )
                        logging.debug("add shared folder %s" % path_folder)
                        logger.info("add device in folder %s" % devicefolder["id"])
                        self.add_folder_dict_if_not_exist_id(newfolder, config)
                        self.add_device_in_folder_if_not_exist(
                            devicefolder["id"], de["deviceID"], config
                        )
                        self.syncthingreconfigure = True
            if self.syncthingreconfigure:
                self.syncthing.post_config(config)
                time.sleep(3)
                self.syncthing.post_restart()
                time.sleep(1)
                self.syncthing.reload_config()
            else:
                self.syncthing.validate_chang_config()

    def clean_old_descriptor_syncting(self, pathdescriptor):
        duration = 3
        onlyfiles = [
            os.path.join(pathdescriptor, f)
            for f in os.listdir(pathdescriptor)
            if os.path.isfile(os.path.join(pathdescriptor, f))
        ]
        timestampnew = time.time()
        for f in onlyfiles:
            if ((timestampnew - os.stat(f).st_mtime) / 3600) > duration:
                os.remove(f)

    def clean_old_partage_syncting(self):
        """
        This function helps to clean old syncthing shares.
        A share ends after 3 hours ( duration variable )
        """
        try:
            self.syncthing
        except Exception:
            return
        duration = 3.0
        syncthingroot = self.getsyncthingroot()
        if not os.path.exists(syncthingroot):
            os.makedirs(syncthingroot)
        sharefolder = [x for x in os.listdir(syncthingroot)]
        listflo = []
        for folder in sharefolder:
            folderpart = os.path.join(syncthingroot, folder)
            exist = self.syncthing.is_exist_folder_id(folder)
            if not exist:
                # If there is no shared folder, we remove the useless files from the share
                # listflo.append(folderpart)
                pass
            if ((time.time() - os.stat(folderpart).st_mtime) / 3600) > duration:
                if exist:
                    # Shares older from 3 hours must be deleted
                    # self.syncthing.del_folder(folder)
                    self.syncthing.delete_folder_pulse_deploy(folder, reload=False)
                    listflo.append(folderpart)
        self.syncthing.validate_chang_config()
        for deletedfolder in listflo:
            if os.path.isdir(deletedfolder):
                try:
                    logger.debug("Removing the shared folder %s" % deletedfolder)
                    shutil.rmtree(deletedfolder)
                except OSError:
                    logger.error(
                        "Error while removing the shared folder %s" % (deletedfolder)
                    )
                    logger.error("\n%s" % (traceback.format_exc()))

    def getsyncthingroot(self):
        syncthingroot = ""
        if self.config.agenttype in ["relayserver"]:
            return self.config.syncthing_share
        if sys.platform.startswith("win"):
            syncthingroot = os.path.join(medullaPath(), "var", "syncthing")
        elif sys.platform.startswith("linux"):
            syncthingroot = os.path.join(os.path.expanduser("~pulseuser"), "syncthing")
        elif sys.platform.startswith("darwin"):
            syncthingroot = os.path.join(medullaPath(), "var", "syncthing")

        return syncthingroot

    def scan_syncthing_deploy(self):
        if not self.config.syncthing_on:
            return
        self.clean_old_partage_syncting()
        self.clean_old_descriptor_syncting(self.dirsyncthing)
        listfilearssyncthing = [
            os.path.join(self.dirsyncthing, x)
            for x in os.listdir(self.dirsyncthing)
            if x.endswith("ars")
        ]

        # get the root for the sync folders
        syncthingroot = self.getsyncthingroot()

        for filears in listfilearssyncthing:
            try:
                syncthingtojson = managepackage.loadjsonfile(filears)
            except Exception:
                syncthingtojson = None

            if syncthingtojson is not None:
                namesearch = os.path.join(
                    syncthingroot, syncthingtojson["objpartage"]["repertoiredeploy"]
                )
                # verify le contenue de namesearch
                if os.path.isdir(namesearch):
                    logging.debug("deploy transfert syncthing : %s" % namesearch)
                    # Get the deploy json
                    filedeploy = os.path.join("%s.descriptor" % filears[:-4])
                    deploytojson = managepackage.loadjsonfile(filedeploy)
                    # Now we have :
                    #   - the .ars file root in filears
                    #   - it's json in syncthingtojson
                    #   - the .descriptor file root in filedeploy
                    #   - it's json in deploytojson
                    #
                    # We need to copy the content of namesearch into the tmp
                    # package dirl
                    packagedir = managepackage.packagedir()
                    logging.warning(packagedir)
                    for dirname in os.listdir(namesearch):
                        if dirname != ".stfolder":
                            # clean the dest package to be sure
                            try:
                                shutil.rmtree(os.path.join(packagedir, dirname))
                                logging.debug(
                                    "clean package before copy %s"
                                    % (os.path.join(packagedir, dirname))
                                )
                            except OSError:
                                pass
                            try:
                                self.xmpplog(
                                    "Transfer complete on machine %s\n "
                                    "Start Deployement" % self.boundjid.bare,
                                    type="deploy",
                                    sessionname=syncthingtojson["sessionid"],
                                    priority=-1,
                                    action="xmpplog",
                                    who="",
                                    how="",
                                    why=self.boundjid.bare,
                                    module="Deployment | Syncthing",
                                    date=None,
                                    fromuser="",
                                    touser="",
                                )
                                shutil.copytree(
                                    os.path.join(namesearch, dirname),
                                    os.path.join(packagedir, dirname),
                                )

                                logging.debug(
                                    "copy %s to %s"
                                    % (
                                        os.path.join(namesearch, dirname),
                                        os.path.join(packagedir, dirname),
                                    )
                                )
                                try:
                                    logging.debug("Delete %s" % filears)
                                    os.remove(filears)
                                except Exception:
                                    logging.warning("%s no exist" % filears)
                                try:
                                    logging.debug("delete %s" % filedeploy)
                                    os.remove(filedeploy)
                                except Exception:
                                    logging.warning("%s does no exist" % filedeploy)

                                dataerreur = {
                                    "action": "resultapplicationdeploymentjson",
                                    "sessionid": syncthingtojson["sessionid"],
                                    "ret": 255,
                                    "base64": False,
                                    "data": {"msg": "error deployement"},
                                }

                                transfertdeploy = {
                                    "action": "applicationdeploymentjson",
                                    "sessionid": syncthingtojson["sessionid"],
                                    "data": deploytojson,
                                    "ret": 0,
                                    "base64": False,
                                }
                                msg = {
                                    "from": syncthingtojson["objpartage"]["cluster"][
                                        "elected"
                                    ],
                                    "to": self.boundjid.bare,
                                    "type": "chat",
                                }
                                logging.debug("call  applicationdeploymentjson")
                                logging.debug(
                                    "%s " % json.dumps(transfertdeploy, indent=4)
                                )
                                call_plugin(
                                    transfertdeploy["action"],
                                    self,
                                    transfertdeploy["action"],
                                    transfertdeploy["sessionid"],
                                    transfertdeploy["data"],
                                    msg,
                                    dataerreur,
                                )
                                logging.warning("SEND MASTER")
                                datasend = {
                                    "action": "deploysyncthing",
                                    "sessionid": syncthingtojson["sessionid"],
                                    "data": {
                                        "subaction": "counttransfertterminate",
                                        "iddeploybase": syncthingtojson["objpartage"][
                                            "syncthing_deploy_group"
                                        ],
                                    },
                                    "ret": 0,
                                    "base64": False,
                                }
                                strr = json.dumps(datasend)
                                logging.warning("SEND MASTER %s : " % strr)
                                logging.error("send to master")
                                logging.error("%s " % strr)

                                self.send_message(
                                    mto=self.agentmaster, mbody=strr, mtype="chat"
                                )
                            except Exception:
                                logging.error(
                                    "The package's copy %s to %s failed"
                                    % (dirname, packagedir)
                                )
                                logger.error("\n%s" % (traceback.format_exc()))
                else:
                    # we look if we have informations about the transfert
                    # print
                    # self.syncthing.get_db_status(syncthingtojson['id_deploy'])
                    logging.debug(
                        "Recherche la completion de transfert %s" % namesearch
                    )
                    result = self.syncthing.get_db_completion(
                        syncthingtojson["objpartage"]["repertoiredeploy"],
                        self.syncthing.device_id,
                    )
                    if (
                        "syncthing_deploy_group" in syncthingtojson["objpartage"]
                        and len(self.syncthing.device_id) > 40
                    ):
                        if "completion" in result and result["completion"] != 0:
                            datasend = {
                                "action": "deploysyncthing",
                                "sessionid": syncthingtojson["sessionid"],
                                "data": {
                                    "subaction": "completion",
                                    "iddeploybase": syncthingtojson["objpartage"][
                                        "syncthing_deploy_group"
                                    ],
                                    "completion": result["completion"],
                                    "jidfull": self.boundjid.full,
                                },
                                "ret": 0,
                                "base64": False,
                            }
                            strr = json.dumps(datasend)
                            self.send_message(
                                mto=syncthingtojson["objpartage"]["agentdeploy"],
                                mbody=strr,
                                mtype="chat",
                            )
            else:
                # todo supprimer le fichier ars et descriptor.
                # signaler l'erreur de decodage du fichier json.
                logger.error("\n%s" % (traceback.format_exc()))
                pass

    # end syncthing function

    def execcmdfile(self):
        """
        lit fichier avec demande de commande
        """
        fileextern = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "cmdexterne"
        )
        if os.path.isfile(fileextern):
            aa = file_get_contents(fileextern).strip()
            logging.info("cmd externe : %s " % aa)
            if aa.startswith("inventory"):
                logging.info("send inventory")
                self.handleinventory()
            os.remove(fileextern)

    def version_agent(self):
        pathversion = os.path.join(self.pathagent, "agentversion")
        if os.path.isfile(pathversion):
            self.versionagent = (
                file_get_contents(pathversion)
                .replace("\n", "")
                .replace("\r", "")
                .strip()
            )
        else:
            self.versionagent = 0.0
        return self.versionagent

    def send_ping_to_kiosk(self):
        """Send a ping to the kiosk  to ask it's presence"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("localhost", 8766)
        try:
            sock.connect(server_address)
            try:
                msg = '{"action":"presence","type":"ping"}'
                sock.sendall(msg.encode("ascii"))
                self.kiosk_presence = "True"
            except:
                self.kiosk_presence = "False"
        except:
            self.kiosk_presence = "False"
        finally:
            sock.close()
        datasend = {
            "action": "resultkiosk",
            "sessionid": getRandomName(6, "kioskGrub"),
            "ret": 0,
            "base64": False,
            "data": {},
        }

        datasend["data"]["subaction"] = "presence"
        datasend["data"]["value"] = self.kiosk_presence
        self.send_message_to_master(datasend)

    def send_pong_to_kiosk(self):
        """Send a pong to the kiosk  to answer to ping presence"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("localhost", 8766)
        try:
            sock.connect(server_address)
            try:
                msg = '{"action":"presence","type":"pong"}'
                sock.sendall(msg.encode("ascii"))
                self.kiosk_presence = "True"
            except:
                self.kiosk_presence = "False"
        except:
            self.kiosk_presence = "False"
        finally:
            sock.close()

        datasend = {
            "action": "resultkiosk",
            "sessionid": getRandomName(6, "kioskGrub"),
            "ret": 0,
            "base64": False,
            "data": {},
        }
        datasend["data"]["subaction"] = "presence"
        datasend["data"]["value"] = self.kiosk_presence
        self.send_message_to_master(datasend)

    def handle_client_connection(self, client_socket):
        """
        this function handles the message received from kiosk or watching syncting service
        the function must provide a response to an acknowledgment kiosk or a result
        Args:
            client_socket: socket for exchanges between AM and Kiosk

        Returns:
            no return value
        """
        try:
            # request the recv message
            recv_msg_from_kiosk = client_socket.recv(4096)
            if len(recv_msg_from_kiosk) != 0:
                print("Received {}".format(recv_msg_from_kiosk))
                datasend = {
                    "action": "resultkiosk",
                    "sessionid": getRandomName(6, "kioskGrub"),
                    "ret": 0,
                    "base64": False,
                    "data": {},
                }
                msg = str(recv_msg_from_kiosk.decode("utf-8", "ignore"))
                ##############
                if isBase64(msg):
                    msg = base64.b64decode(msg)
                try:
                    result = json.loads(msg)
                except ValueError as e:
                    logger.error("Message socket is not json correct : %s" % (str(e)))
                    return
                if "uuid" in result:
                    datasend["data"]["uuid"] = result["uuid"]
                if "utcdatetime" in result:
                    datasend["data"]["utcdatetime"] = result["utcdatetime"]
                if "action" in result:
                    if result["action"] == "kioskinterface":
                        # start kiosk ask initialization
                        datasend["data"]["subaction"] = result["subaction"]
                        datasend["data"]["userlist"] = list(
                            set([users[0] for users in psutil.users()])
                        )
                        datasend["data"]["ouuser"] = organizationbyuser(
                            datasend["data"]["userlist"][0]
                        )
                        datasend["data"]["oumachine"] = organizationbymachine()
                    elif result["action"] == "kioskinterfaceInstall":
                        datasend["data"]["subaction"] = "install"
                    elif result["action"] == "kioskinterfaceLaunch":
                        datasend["data"]["subaction"] = "launch"
                    elif result["action"] == "kioskinterfaceDelete":
                        datasend["data"]["subaction"] = "delete"
                    elif result["action"] == "kioskinterfaceUpdate":
                        datasend["data"]["subaction"] = "update"
                    elif result["action"] == "presence":
                        if result["type"] == "ping":
                            # Send pong message
                            self.kiosk_presence = "True"
                            self.send_pong_to_kiosk()
                            logging.getLogger().debug("Sendback pong message to kiosk")
                        elif result["type"] == "pong":
                            # Set the kiosk_presence variable to True
                            logging.getLogger().debug("Receive pong message from kiosk")
                            self.kiosk_presence = "True"

                        else:
                            # Ignore the others messages
                            pass
                        datasend["data"]["subaction"] = "presence"
                        datasend["data"]["value"] = self.kiosk_presence
                    elif result["action"] == "inventory":
                        datasend["data"]["subaction"] = "inventory"

                    elif result["action"] == "kioskLog":
                        if "message" in result and result["message"] != "":
                            self.xmpplog(
                                result["message"],
                                type="noset",
                                sessionname="",
                                priority=0,
                                action="xmpplog",
                                who=self.boundjid.bare,
                                how="Planned",
                                why="",
                                module="Kiosk | Notify",
                                fromuser="",
                                touser="",
                            )
                            if "type" in result:
                                if result["type"] == "info":
                                    logging.getLogger().info(result["message"])
                                elif result["type"] == "warning":
                                    logging.getLogger().warning(result["message"])
                    elif result["action"] == "notifysyncthing":
                        datasend["action"] = "notifysyncthing"
                        datasend["sessionid"] = getRandomName(6, "syncthing")
                        datasend["data"] = result["data"]
                    else:
                        # bad action
                        logging.getLogger().warning(
                            "this action is not taken into account : %s"
                            % result["action"]
                        )
                        return
                    # call plugin on master
                    self.send_message_to_master(datasend)
        except Exception as e:
            logging.error("message to kiosk server : %s" % str(e))
            logger.error("\n%s" % (traceback.format_exc()))
        finally:
            client_socket.close()

    def established_connection(self):
        """check connection xmppmaster"""
        if not connection_established(self.config.Port):
            logger.info("RESTART AGENT lost Connection")
            self.restartBot()

    def reloaddeploy(self):
        for sessionidban in self.ban_deploy_sessionid_list:
            self.managefifo.delsessionfifo(sessionidban)

        list_session_terminate_fifo = self.managefifo.checking_deploy_slot_outdoor()

        for sessionid in list_session_terminate_fifo:
            # on supprime cette session des fifo
            # le deploiement est treminée pour cette session.
            self.managefifo.delsessionfifo(sessionid)
            logging.warning(
                "stop deploy session %s " "(deployment slot has passed)" % sessionid
            )
            self.xmpplog(
                '<span class="log_err">Deployment error in fifo : '
                "timed out (sessionid %s)</span>" % (sessionid),
                type="deploy",
                sessionname=sessionid,
                priority=-1,
                action="xmpplog",
                who=self.boundjid.bare,
                how="",
                why="",
                module="Deployment | Download | Transfert | Notify | Error",
                date=None,
                fromuser=self.boundjid.bare,
                touser="",
            )
            self.xmpplog(
                "DEPLOYMENT TERMINATE",
                type="deploy",
                sessionname=sessionid,
                priority=-1,
                action="xmpplog",
                who=self.boundjid.bare,
                how="",
                why="",
                module="Deployment | Error | Terminate | Notify",
                date=None,
                fromuser=self.boundjid.bare,
                touser="",
            )
        if len(list_session_terminate_fifo) > 0:
            dataerreur = {
                "action": "resultcluster",
                "data": {"msg": "error plugin : plugin"},
                "sessionid": list_session_terminate_fifo[0],
                "ret": 255,
                "base64": False,
            }
            # send "envoi message pour signaler ressource level"
            msg = {"from": self.boundjid.bare, "to": self.boundjid.bare, "type": "chat"}
            call_plugin(
                "cluster",
                self,
                "cluster",
                list_session_terminate_fifo[0],
                {"subaction": "refresh"},
                msg,
                dataerreur,
            )

        if self.managefifo.getcount() != 0:
            logger.debug(
                "FIFO DEPLOY %s level charge %s"
                " concurent deploy max %s"
                % (
                    self.managefifo.getcount(),
                    self.levelcharge["charge"],
                    self.config.concurrentdeployments,
                )
            )

            if self.levelcharge["charge"] < self.config.concurrentdeployments:
                nbresource = (
                    self.config.concurrentdeployments - self.levelcharge["charge"]
                )
                logger.debug("Possible Slot deploy %s" % nbresource)
                for Slot in range(nbresource):
                    if self.managefifo.getcount() != 0:
                        data = self.managefifo.getfifo()
                        datasend = {
                            "action": data["action"],
                            "sessionid": data["sessionid"],
                            "ret": 0,
                            "base64": False,
                        }
                        del data["action"]
                        del data["sessionid"]
                        datasend["data"] = data
                        self.send_message(
                            mto=self.boundjid.bare,
                            mbody=json.dumps(datasend),
                            mtype="chat",
                        )

    def checklevelcharge(self, ressource=0):
        self.levelcharge["charge"] = self.levelcharge["charge"] + ressource
        if self.levelcharge["charge"] < 0:
            self.levelcharge["charge"] = 0
        return self.levelcharge["charge"]

    def getlevelmachinelist(self, jidmachine=""):
        return self.levelcharge["machinelist"]

    def addmachineinlevelmachinelist(self, jidmachine):
        self.levelcharge["machinelist"].append(jidmachine)
        self.levelcharge["charge"] = len(self.levelcharge["machinelist"])

    def delmachineinlevelmachinelist(self, jidmachine):
        for index, elt in enumerate(self.levelcharge["machinelist"][:]):
            if elt == jidmachine:
                del self.levelcharge["machinelist"][index]
        self.levelcharge["charge"] = len(self.levelcharge["machinelist"])

    def signal_handler(self, signal, frame):
        logger.debug("CTRL-C EVENT")
        global signalint
        signalint = True
        msgevt = {
            "action": "evtfrommachine",
            "sessionid": getRandomName(6, "eventwin"),
            "ret": 0,
            "base64": False,
            "data": {"machine": self.boundjid.jid, "event": "CTRL_C_EVENT"},
        }
        self.send_message_subcripted_agent(msgevt)
        time.sleep(2)
        self.quit_application(wait=3)

    def send_message_subcripted_agent(self, msg):
        self.send_message(mbody=json.dumps(msg), mto=self.sub_subscribe, mtype="chat")

    def send_message_to_master(self, msg):
        self.send_message(
            mbody=json.dumps(msg), mto="%s/MASTER" % self.agentmaster, mtype="chat"
        )

    def _CtrlHandler(self, evt):
        """## todo intercep message in console program
        win32con.WM_QUERYENDSESSION win32con.WM_POWERBROADCAS(PBT_APMSUSPEND
        """
        global signalint
        if sys.platform.startswith("win"):
            with terminate_lock:
                self.shared_dict["terminate"] = True
            msgevt = {
                "action": "evtfrommachine",
                "sessionid": getRandomName(6, "eventwin"),
                "ret": 0,
                "base64": False,
                "data": {"machine": self.boundjid.jid},
            }
            if evt == win32con.CTRL_SHUTDOWN_EVENT:
                msgevt["data"]["event"] = "SHUTDOWN_EVENT"
                self.send_message_to_master(msgevt)
                logger.debug("CTRL_SHUTDOWN EVENT")
                signalint = True
                return True
            if evt == win32con.CTRL_LOGOFF_EVENT:
                msgevt["data"]["event"] = "LOGOFF_EVENT"
                self.send_message_to_master(msgevt)
                logger.debug("CTRL_LOGOFF EVENT")
                return True
            if evt == win32con.CTRL_BREAK_EVENT:
                msgevt["data"]["event"] = "BREAK_EVENT"
                self.send_message_to_master(msgevt)
                logger.debug("CTRL_BREAK EVENT")
                return True
            if evt == win32con.CTRL_CLOSE_EVENT:
                msgevt["data"]["event"] = "CLOSE_EVENT"
                self.send_message_to_master(msgevt)
                logger.debug("CTRL_CLOSE EVENT")
                return True
            if evt == win32con.CTRL_C_EVENT:
                msgevt["data"]["event"] = "CTRL_C_EVENT"
                self.send_message_to_master(msgevt)
                logger.debug("CTRL-C EVENT")
                signalint = True
                self.quit_application(wait=3)
                return True
            return False
        else:
            pass

    def __sizeout(self, q):
        return q.qsize()

    def sizeoutARS(self):
        return self.__sizeout(self.qoutARS)

    def __getout(self, timeq, q):
        try:
            valeur = q.get(True, timeq)
        except Exception:
            valeur = ""
        return valeur

    def getoutARS(self, timeq=10):
        return self.__getout(timeq, self.qoutARS)

    def gestioneventconsole(self, event, q):
        try:
            dataobj = json.loads(event)
        except Exception as e:
            logging.error("bad struct jsopn Message console %s : %s " % (event, str(e)))
            q.put("bad struct jsopn Message console %s : %s " % (event, str(e)))
        # cette liste contient les function directement appelable depuis
        # console.
        listaction = []
        # check action in message
        if "action" in dataobj:
            if "sessionid" not in dataobj:
                dataobj["sessionid"] = getRandomName(6, dataobj["action"])
            if dataobj["action"] in listaction:
                # call fubnction agent direct
                func = getattr(self, dataobj["action"])
                if "params_by_val" in dataobj and "params_by_name" not in dataobj:
                    func(*dataobj["params_by_val"])
                elif "params_by_val" in dataobj and "params_by_name" in dataobj:
                    func(*dataobj["params_by_val"], **dataobj["params_by_name"])
                elif "params_by_name" in dataobj and "params_by_val" not in dataobj:
                    func(**dataobj["params_by_name"])
                else:
                    func()
            else:
                # call plugin
                dataerreur = {
                    "action": "result" + dataobj["action"],
                    "data": {"msg": "error plugin : " + dataobj["action"]},
                    "sessionid": dataobj["sessionid"],
                    "ret": 255,
                    "base64": False,
                }
                msg = {"from": "console", "to": self.boundjid.bare, "type": "chat"}
                if "data" not in dataobj:
                    dataobj["data"] = {}
                call_plugin(
                    dataobj["action"],
                    self,
                    dataobj["action"],
                    dataobj["sessionid"],
                    dataobj["data"],
                    msg,
                    dataerreur,
                )
        else:
            logging.error("action missing in json Message console %s" % (dataobj))
            q.put("action missing in jsopn Message console %s" % (dataobj))
            return

    def remove_sessionid_in_ban_deploy_sessionid_list(self):
        """
        this function remove sessionid banned
        """
        # renove if timestamp is 10000 millis seconds.
        d = time.time()
        for sessionidban, timeban in list(self.banterminate.items()):
            if (d - self.banterminate[sessionidban]) > 60:
                del self.banterminate[sessionidban]
                try:
                    self.ban_deploy_sessionid_list.remove(sessionidban)
                except Exception as e:
                    logger.warning(str(e))

    def schedulerfunction(self):
        self.manage_scheduler.process_on_event()

    def presence_subscribe(self, presence):
        if presence["from"].bare != self.boundjid.bare:
            logger.info(
                "********** presence_subscribe %s %s"
                % (presence["from"], presence["type"])
            )

    def presence_subscribed(self, presence):
        if presence["from"].bare != self.boundjid.bare:
            logger.info(
                "********** presence_subscribed %s %s"
                % (presence["from"], presence["type"])
            )

    def changed_subscription(self, presence):
        if presence["from"].bare != self.boundjid.bare:
            logger.info(
                "********** changed_subscription %s %s"
                % (presence["from"], presence["type"])
            )

    def presence_unavailable(self, presence):
        if presence["from"].bare != self.boundjid.bare:
            logger.info(
                "********** presence_unavailable %s %s"
                % (presence["from"], presence["type"])
            )

    def presence_available(self, presence):
        if presence["from"].bare != self.boundjid.bare:
            logger.info(
                "********** presence_available %s %s"
                % (presence["from"], presence["type"])
            )
            self.unsubscribe_agent()

    def presence_unsubscribe(self, presence):
        logger.info(
            "**********   presence_unsubscribe %s %s"
            % (presence["from"], presence["type"])
        )

    def presence_unsubscribed(self, presence):
        logger.info(
            "**********   presence_unsubscribed %s %s"
            % (presence["from"], presence["type"])
        )
        self.get_roster()

    async def changed_status(self, presence):
        """
        This function is a xmpp handler used to follow the signal
        from ejabberd when the state of an affiliated agent changes.
        """
        frommsg = jid.JID(presence["from"])
        logger.info(
            "**********   changed_status %s %s" % (presence["from"], presence["type"])
        )
        if frommsg.bare == self.boundjid.bare and presence["type"] == "available":
            logger.debug("Machine available for registration")
            self.update_plugin()
            self.presencectrlsubscribe = "available"
            logger.debug("Machine available for registration")
            self.subscribe_initialisation()
        elif frommsg.bare == self.sub_subscribe:
            if (
                self.presencectrlsubscribe != presence["type"]
                and presence["type"] != "available"
            ):
                logger.warning("Subscription [%s] ON to OFF" % self.sub_subscribe)
            self.presencectrlsubscribe = presence["type"]

    def unsubscribe_agent(self):
        try:
            for t in self.client_roster:
                if t == self.boundjid.bare or t in [self.sub_subscribe]:
                    continue
                logger.info("Unsubscribe agent %s" % t)
                self.send_presence(pto=t, ptype="unsubscribe")
                self.update_roster(t, subscription="remove")
        except Exception:
            logger.error("\n%s" % (traceback.format_exc()))

        if self.sub_subscribe not in self.client_roster:
            self.send_presence(pto=self.sub_subscribe, ptype="subscribe")
            self.get_roster()

    def subscribe_initialisation(self):
        logger.debug("subscribe_initialisation agent %s" % self.sub_subscribe)
        self.unsubscribe_agent()
        if self.sub_subscribe not in list(self.client_roster.keys()):
            logger.warning(
                "Subscription [%s] is not yet in the roster %s"
                % (self.sub_subscribe, list(self.client_roster.keys()))
            )
        logger.info(
            "%s roster is %s configured substitute is %s"
            % (
                self.config.agenttype,
                list(self.client_roster.keys()),
                self.sub_subscribe,
            )
        )
        self.xmpplog(
            "%s roster is %s configured substitute is %s"
            % (
                self.config.agenttype,
                list(self.client_roster.keys()),
                self.sub_subscribe,
            ),
            type="info",
            sessionname="",
            priority=-1,
            action="xmpplog",
            who=self.boundjid.bare,
            how="",
            why="",
            date=None,
            fromuser=self.boundjid.bare,
            touser="",
        )

    async def start(self, event):
        self.send_presence()
        await self.get_roster()
        # send iq to subscribe
        self.send_presence(pto=self.sub_subscribe, ptype="subscribe")
        # machine connected
        self.config.ipxmpp = getIpXmppInterface(self.config)
        # nettoy les fichiers de mise a jour windows
        clean_update_directories()
        self.__clean_message_box()
        if self.config.agenttype in ["relayserver"]:
            try:
                if self.config.public_ip_relayserver != "":
                    logging.log(
                        DEBUGPULSE,
                        "Attribution ip public by configuration for ipconnexion: [%s]"
                        % self.config.public_ip_relayserver,
                    )
                    self.ipconnection = self.config.public_ip_relayserver
            except Exception:
                pass
        else:
            result, jid_struct = unregister_agent(
                self.boundjid.user, self.boundjid.domain, self.boundjid.resource
            )
            if result:
                # We need to unregistrer jid_struct
                # send unregistered user to ars old domain
                ars = "rs%s@%s" % (
                    jid_struct["domain"].strip(),
                    jid_struct["domain"].strip(),
                )
                datasend = {
                    "action": "unregister_agent",
                    "sessionid": getRandomName(6, "unregister_agent"),
                    "data": jid_struct,
                    "ret": 0,
                    "base64": False,
                }
                self.send_message(mbody=json.dumps(datasend), mto=ars, mtype="chat")
        self.agentrelayserverrefdeploy = self.config.jidchatroomcommand.split("@")[0]

        self.xmpplog(
            "Starting %s agent -> subscription agent is %s"
            % (self.config.agenttype, self.sub_subscribe),
            type="info",
            sessionname="",
            priority=-1,
            action="xmpplog",
            who=self.boundjid.bare,
            how="",
            why="",
            date=None,
            fromuser=self.boundjid.bare,
            touser="",
        )
        # notify master conf error in AM
        dataerrornotify = {
            "to": self.boundjid.bare,
            "action": "notify",
            "sessionid": getRandomName(6, "notify"),
            "data": {"msg": "", "type": "error"},
            "ret": 0,
            "base64": False,
        }
        try:
            self.send_ping_to_kiosk()
        except Exception:
            pass

        if not os.path.isdir(self.config.defaultdir):
            dataerrornotify["data"]["msg"] = (
                "An error occured while configuring the browserfile. The default dir %s does not exist on %s."
                % (self.boundjid.bare, self.config.defaultdir)
            )
            self.send_message(
                mto=self.agentmaster, mbody=json.dumps(dataerrornotify), mtype="chat"
            )

        if not os.path.isdir(self.config.rootfilesystem):
            dataerrornotify["data"]["msg"] = (
                "An error occured while configuring the browserfile. The rootfilesystem dir %s does not exist on %s."
                % (self.boundjid.bare, self.config.rootfilesystem)
            )
        if dataerrornotify["data"]["msg"] != "":
            self.send_message(
                mto=self.agentmaster, mbody=json.dumps(dataerrornotify), mtype="chat"
            )
        startparameter = {
            "action": "start",
            "sessionid": getRandomName(6, "start"),
            "ret": 0,
            "base64": False,
            "data": {},
        }
        dataerreur = {
            "action": "result" + startparameter["action"],
            "data": {"msg": "error plugin : " + startparameter["action"]},
            "sessionid": startparameter["sessionid"],
            "ret": 255,
            "base64": False,
        }
        msg = {"from": self.boundjid.bare, "to": self.boundjid.bare, "type": "chat"}
        if "data" not in startparameter:
            startparameter["data"] = {}
        call_plugin_sequentially(
            startparameter["action"],
            self,
            startparameter["action"],
            startparameter["sessionid"],
            startparameter["data"],
            msg,
            dataerreur,
        )

    def call_plugin_differed(self, time_differed=5):
        try:
            for pluginname in self.paramsdict:
                self.schedule(
                    pluginname["descriptor"]["action"],
                    time_differed,
                    self.call_plugin_deffered_mode,
                    repeat=False,
                    kwargs={},
                    args=(),
                )
        except Exception:
            logger.error(
                "An error occured while calling the function call_plugin_differed."
            )
            logger.error(
                "We encountered the backtrace: \n%s" % (traceback.format_exc())
            )

    def call_plugin_deffered_mode(self, *args, **kwargs):
        try:
            newparams = self.paramsdict.pop(0)
            call_plugin(
                newparams["descriptor"]["action"],
                self,
                newparams["descriptor"]["action"],
                newparams["descriptor"]["sessionid"],
                newparams["descriptor"]["data"],
                newparams["msg"],
                newparams["errordescriptor"],
            )
        except Exception:
            logger.error(
                "An error occured whild calling the function call_plugin_deffered_mode."
            )
            logger.error(
                "We encountered the backtrace: \n%s" % (traceback.format_exc())
            )

    def initialise_syncthing(self):
        try:
            self.config.syncthing_on
        except NameError:
            self.config.syncthing_on = False

        if self.config.syncthing_on:
            logger.info("Initialisation of syncthing in progress.")
            if self.config.agenttype not in ["relayserver"]:
                if self.config.sched_check_syncthing_deployment:
                    self.schedule(
                        "scan_syncthing_deploy",
                        55,
                        self.scan_syncthing_deploy,
                        repeat=True,
                    )
            if self.config.sched_check_synthing_config:
                self.schedule(
                    "synchro_synthing", 60, self.synchro_synthing, repeat=True
                )
            if logger.level <= 10:
                console = False
                browser = True
            self.Ctrlsyncthingprogram = syncthingprogram(
                agenttype=self.config.agenttype
            )
            self.Ctrlsyncthingprogram.restart_syncthing()

            try:
                self.syncthing = syncthing(
                    configfile=self.fichierconfsyncthing,
                    port=self.config.syncthing_gui_port,
                )
                if logger.level <= 10:
                    self.syncthing.save_conf_to_file(self.tmpfile)
                else:
                    if os.path.isfile(self.tmpfile):
                        try:
                            os.remove(self.tmpfile)
                        except OSError:
                            logging.error(
                                "We failed to remove the file %s" % self.tmpfile
                            )
                            pass

            except KeyError as keyerror:
                logging.error(
                    "The %s key is missing in your syncthing config file" % keyerror
                )

            except Exception as e:
                logging.error(
                    "The initialisation of syncthing failed with the error %s: "
                    % str(e)
                )
                logger.error(
                    "We hit the following backtrace: \n%s" % traceback.format_exc()
                )
                logger.error("Syncthing is not functionnal. Using the degraded mode")

            logger.info("Initialisation of syncthing finished.")

        else:
            logger.warning("Syncthing is disabled, we won't initialise it.")

    def send_message_agent(
        self, mto, mbody, msubject=None, mtype=None, mhtml=None, mfrom=None, mnick=None
    ):
        if mto != "console":
            self.send_message(
                mto, json.dumps(mbody), msubject, mtype, mhtml, mfrom, mnick
            )
        else:
            if self.config.agenttype in ["relayserver"]:
                q = self.qoutARS
            else:
                q = self.qoutAM
            if q.full():
                while not q.empty():
                    q.get()
            else:
                try:
                    q.put(json.dumps(mbody), True, 10)
                except Exception:
                    logger.error("Impossible to add in the queue")

    def logtopulse(self, text, type="noset", sessionname="", priority=0, who=""):
        if who == "":
            who = self.boundjid.bare
        msgbody = {}
        data = {
            "log": "xmpplog",
            "action": "xmpplog",
            "text": text,
            "type": type,
            "sessionid": sessionname,
            "session": sessionname,
            "priority": priority,
            "who": who,
        }
        msgbody["data"] = data
        msgbody["action"] = "xmpplog"
        msgbody["sessionid"] = sessionname
        msgbody["session"] = sessionname
        self.send_message(mto=self.sub_logger, mbody=json.dumps(msgbody), mtype="chat")

    def xmpplog(
        self,
        text,
        type="noset",
        sessionname="",
        priority=0,
        action="xmpplog",
        who="",
        how="",
        why="",
        module="",
        date=None,
        fromuser="",
        touser="",
    ):
        if sessionname == "":
            sessionname = getRandomName(6, "logagent")
        if who == "":
            who = self.boundjid.bare
        if touser == "":
            touser = self.boundjid.bare
        msgbody = {}
        data = {
            "log": "xmpplog",
            "text": text,
            "type": type,
            "sessionid": sessionname,
            "priority": priority,
            "action": action,
            "who": who,
            "how": how,
            "why": why,
            "module": module,
            "date": None,
            "fromuser": fromuser,
            "touser": touser,
        }
        msgbody["data"] = data
        msgbody["action"] = "xmpplog"
        msgbody["sessionid"] = sessionname
        self.send_message(mto=self.sub_logger, mbody=json.dumps(msgbody), mtype="chat")

    def handleinventory(self, forced="forced", sessionid=None):
        msg = {"from": "master@pulse/MASTER", "to": self.boundjid.bare}
        datasend = {"forced": "forced"}
        if forced == "forced" or forced is True:
            datasend = {"forced": "forced"}
        else:
            datasend = {"forced": "noforced"}
        if sessionid is None:
            sessionid = getRandomName(6, "inventory")
        dataerreur = {}
        dataerreur["action"] = "resultinventory"
        dataerreur["data"] = datasend
        dataerreur["data"]["msg"] = "ERROR : inventory"
        dataerreur["sessionid"] = sessionid
        dataerreur["ret"] = 255
        dataerreur["base64"] = False

        self.xmpplog(
            "Sending inventory from agent"
            " %s (Interval : %s)"
            % (self.boundjid.bare, self.config.inventory_interval),
            type="noset",
            sessionname="",
            priority=0,
            action="xmpplog",
            who=self.boundjid.bare,
            how="Planned",
            why="",
            module="Inventory | Inventory reception | Planned",
            fromuser="",
            touser="",
        )
        call_plugin(
            "inventory", self, "inventory", sessionid, datasend, msg, dataerreur
        )

    def update_plugin(self):
        """
        Updates the plugin by sending plugin and machine information to the Master.

        This function attempts to gather machine information using the `searchInfoMachine` method,
        then logs the registration information and sends it to the specified registration address using XMPP.

        If successful, it logs the registration details and sends the information as a chat message.
        If an exception occurs during the process, it logs the error details.

        Raises:
            Exception: If an error occurs during the update process.

        Note:
            The update process involves obtaining machine information, logging registration details,
            and sending the information as a chat message using XMPP.

        Usage:
            To update the plugin, call this method on an instance of the class.

        Example:
            instance.update_plugin()
        """
        # Send plugin and machine informations to Master
        try:
            dataobj = self.searchInfoMachine()
            if dataobj is None:
                logger.error(
                    "IMPOSSIBLE SEND REGISTRATION to %s" % self.sub_registration
                )
                return

            logging.log(
                DEBUGPULSE,
                "SEND REGISTRATION XMPP to %s \n%s"
                % (self.sub_registration, json.dumps(dataobj, indent=4)),
            )

            setgetcountcycle()
            self.send_message(
                mto=self.sub_registration, mbody=json.dumps(dataobj), mtype="chat"
            )
        except Exception:
            logger.error("\n%s" % (traceback.format_exc()))

    def call_asynchrome_function_plugin(
        self, nameplugin, differed=0, data=None, sessionid=None
    ):
        """
        Call a plugin function in parallel or deferred mode.

        This method schedules the execution of the asynchronous plugin function, allowing for parallel
        or deferred calling. It generates a random event name and, if not provided, a session ID for the
        asynchronous operation.

        Args:
            nameplugin (str): The name of the plugin function to be called.
            differed (int, optional): Time delay (in seconds) before the function execution (default is 0).
            data (dict, optional): Additional data to be passed to the asynchronous function (default is an empty dictionary).
            sessionid (str, optional): Session ID for the asynchronous operation (default is generated randomly).

        Note:
            The function schedules the asynchronous plugin function using the provided parameters and
            does not wait for its completion.

        Example:
            To call an asynchronous plugin function named 'example_plugin' with a 5-second delay:
            ```
            instance.call_asynchrome_function_plugin("example_plugin", differed=5)
            ```
        """
        nameevenement = getRandomName(6, nameplugin)
        if sessionid is None:
            sessionid = getRandomName(6, "asynchrone")
        if data is None:
            data = {}
        argv = [nameplugin, sessionid]
        self.schedule(
            nameevenement,
            differed,
            self.__asynchrome_function_plugin,
            argv,
            data,
            repeat=False,
        )

    def __asynchrome_function_plugin(self, *argv, **kargv):
        """
        "data" : { "msg" : "error plugin : "+ dataobj["action"]
        """
        # structure execution
        nameplugin = argv[0]
        datasend = {
            "action": argv[0],
            "sessionid": argv[1],
            "ret": 0,
            "base64": False,
            "data": kargv,
        }
        datasenderror = datasend.copy()
        datasenderror["action"] = "result" + datasend["action"]
        datasenderror["data"] = {"msg": "error plugin : " + datasend["action"]}
        msg = {"from": self.boundjid.bare, "to": self.boundjid.bare, "type": "chat"}
        call_plugin(
            datasend["action"],
            self,
            datasend["action"],
            argv[1],
            datasend["data"],
            msg,
            datasenderror,
        )

    def reloadsesssion(self):
        # reloadsesssion only for machine
        # retrieve existing sessions
        if not self.session.loadsessions():
            return
        try:
            # load back to deploy after read session
            self.back_to_deploy = load_back_to_deploy()
        except IOError:
            self.back_to_deploy = {}
        cleanbacktodeploy(self)
        for i in self.session.sessiondata:
            logger.debug("DEPLOYMENT AFTER RESTART OU RESTART BOT")
            msg = {"from": self.boundjid.bare, "to": self.boundjid.bare}
            call_plugin(
                i.datasession["action"],
                self,
                i.datasession["action"],
                i.datasession["sessionid"],
                i.datasession["data"],
                msg,
                {},
            )

    def loginfotomaster(self, msgdata):
        logstruct = {
            "action": "infolog",
            "sessionid": getRandomName(6, "xmpplog"),
            "ret": 0,
            "base64": False,
            "msg": msgdata,
        }
        try:
            self.send_message(
                mbody=json.dumps(logstruct),
                mto="%s/MASTER" % self.agentmaster,
                mtype="chat",
            )
        except Exception as e:
            logging.error(
                "message log to '%s/MASTER': %s " % (self.agentmaster, str(e))
            )
            logger.error("\n%s" % (traceback.format_exc()))
            return

    def handlereprise_evenement(self):
        self.eventmanage.manage_event_loop()

    def signalsessioneventrestart(self, result):
        pass

    def handlemanagesession(self):
        # on verifie si on doit faire 1 reconnection
        with terminate_lock:
            if self.shared_dict.get("reconnect"):
                self.shared_dict["reconnect"] = False
                self.reconnect(0.0, "new configuration recu")
        # on gere les compteurs de session
        self.session.decrementesessiondatainfo()

    def force_full_registration(self):
        BOOLFILECOMPLETREGISTRATION = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "BOOLFILECOMPLETREGISTRATION"
        )
        BOOLFILEINVENTORYONCHANGINTERFACE = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "BOOLFILEINVENTORYONCHANGINTERFACE",
        )
        file_put_contents(
            BOOLFILECOMPLETREGISTRATION,
            "Do not erase.\n"
            "when re-recording, it will be of type 2. full recording.\n from networkMonitor",
        )

        file_put_contents(
            BOOLFILEINVENTORYONCHANGINTERFACE,
            "Do not erase.\n"
            "this file allows you to request 1 inventory following 1 change of network.\n"
            "The inventory is sent when the agent is no longer in transient mode\n"
            "following changes of interfaces.",
        )
        force_reconfiguration = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "action_force_reconfiguration"
        )
        if os.path.isfile(force_reconfiguration):
            os.remove(force_reconfiguration)

    def reconfagent(self, restartbot=True, force_full_registration=True):
        namefilebool = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "BOOLCONNECTOR"
        )
        nameprogconnection = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "connectionagent.py"
        )
        if os.path.isfile(namefilebool):
            os.remove(namefilebool)

        connectionagentArgs = ["py", nameprogconnection, "-t", "machine"]
        subprocess.call(connectionagentArgs)

        for i in range(15):
            if os.path.isfile(namefilebool):
                break
            time.sleep(2)
        logging.log(
            DEBUGPULSE,
            "A new configuration has been detected on %s. We will reconfigure it."
            % self.boundjid.user,
        )

        if force_full_registration:
            self.force_full_registration()

        if restartbot:
            self.restartBot()

    def checkreconf(self):
        force_reconfiguration = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "action_force_reconfiguration"
        )
        if os.path.isfile(force_reconfiguration):
            self.reconfagent()

    def networkMonitor(self):
        try:
            logging.debug(
                "network monitor time  "
                "%ss %s!" % (self.laps_time_networkMonitor, self.boundjid.user)
            )
            md5ctl = createfingerprintnetwork()
            force_reconfiguration = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "action_force_reconfiguration",
            )
            if self.md5reseau != md5ctl or os.path.isfile(force_reconfiguration):
                self.force_full_registration()
                # il y a 1 changement dans le reseau
                # on verify si on connecte
                if self.state.ensure("connected"):
                    logging.log(
                        DEBUGPULSE, "AGENT MACHINE ALWAY CONNECTED ON CHANG RESEAU"
                    )
                    # toujours connected.
                    self.md5reseau = refreshfingerprint()
                    # il y a changement d interface. il faut remettre a jour la table pour network.
                    # remarque cela declenchera 1 inventaire glpi apres
                    # reengeristrement
                    self.update_plugin()
                    return
                if not os.path.isfile(force_reconfiguration):
                    refreshfingerprint()
                    logging.log(
                        DEBUGPULSE,
                        "by network changed. The reconfiguration of the agent [%s] will be executed."
                        % self.boundjid.user,
                    )
                else:
                    logging.log(
                        DEBUGPULSE,
                        "by request. The reconfiguration of the agent [%s] will be executed."
                        % self.boundjid.user,
                    )
                    os.remove(force_reconfiguration)
                # execution de convigurateur.
                # timeout 5 minutes.
                self.reconfagent()
            else:
                BOOLFILEINVENTORYONCHANGINTERFACE = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "BOOLFILEINVENTORYONCHANGINTERFACE",
                )
                if os.path.isfile(BOOLFILEINVENTORYONCHANGINTERFACE):
                    # if on a ce fichier alors on genere 1 nouveau inventaire
                    os.remove(BOOLFILEINVENTORYONCHANGINTERFACE)
                    logging.log(
                        DEBUGPULSE, "The network changed. We will send a new inventory"
                    )
                    self.handleinventory()
        except Exception as e:
            logging.error(" %s " % (str(e)))
            logger.error("\n%s" % (traceback.format_exc()))

    def programfilepath(self, pathwindows):
        """
        Normalise le chemin de fichier `pathwindows` pour le système d'exploitation Windows.

        Arguments:
            pathwindows (str): Le chemin de fichier à normaliser.

        Returns:
            str: Le chemin de fichier normalisé pour Windows.

        Remarque:
            La fonction prend un chemin de fichier `pathwindows` en entrée et le normalise pour
            le système d'exploitation Windows en remplaçant les barres obliques inverses par des
            doubles barres obliques et en encadrant les noms de répertoires contenant des espaces par des guillemets.

            Cette normalisation est particulièrement utile lorsque vous devez manipuler des chemins de fichiers avec
            des espaces dans les noms de répertoires sur Windows.

        Exemple:
            >>> programfilepath("C:/chemin avec des espaces/fichier.txt")
            'C:\\chemin avec des espaces\\fichier.txt'
            >>> programfilepath("C:/chemin/sans/espaces/fichier.txt")
            'C:\\chemin\\sans\\espaces\\fichier.txt'
            >>> programfilepath("D:/répertoire avec espace/")
            'D:\\répertoire avec espace'
            >>> programfilepath("D:/répertoire_sans_espaces/")
            'D:\\répertoire_sans_espaces'
        """
        if not sys.platform.startswith("win"):
            return pathwindows
        pathwindows = os.path.normpath(pathwindows)
        disk_path = pathwindows.split(":")
        if len(disk_path) < 2:
            return pathwindows
        disk = f"{disk_path.pop(0)}:" + "\\\\"
        pathdir = "".join(disk_path)
        t = [x.strip('" ') for x in pathdir.split("\\") if x.strip('" ') != ""]
        result = []
        for x in t:
            if " " in x:
                result.append(f'"{x}"')
            else:
                result.append(x)
        return disk + "\\\\".join(result)

    def reinstall_agent(self):
        BOOL_DISABLE_IMG = os.path.join(self.pathagent, "BOOL_DISABLE_IMG")
        if os.path.exists(BOOL_DISABLE_IMG):
            return
        file_put_contents(
            os.path.join(self.pathagent, "BOOL_UPDATE_AGENT"),
            "use file boolean update. enable verify update.",
        )
        logger.debug(
            "We will update Medulla agent from version %s to %s"
            % (
                file_get_contents(os.path.join(self.img_agent, "agentversion")),
                self.boundjid.bare,
            )
        )
        agentversion = os.path.join(self.pathagent, "agentversion")
        versiondata = (
            file_get_contents(os.path.join(self.img_agent, "agentversion"))
            .replace("\n", "")
            .replace("\r", "")
            .strip()
        )

        try:
            os.remove(os.path.join(self.pathagent, "BOOL_UPDATE_AGENT"))
        except OSError as remove_error:
            logger.error(
                "An error occured while trying to remove the %s file. \n We obtained the error %s"
                % (os.path.join(self.pathagent, "BOOL_UPDATE_AGENT"), remove_error)
            )
            pass
        pythonexec = self.programfilepath(psutil.Process().exe())
        replicatorfunction = os.path.join(self.pathagent, "replicator.py")
        if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
            logger.debug(f"Replicator for os system  {pythonexec} {replicatorfunction}")
        else:
            logger.debug(
                f"Replicator for os windows system {pythonexec} {replicatorfunction}"
            )
        replicatorcmd = f'"{pythonexec}" "{replicatorfunction}"'
        logger.debug("cmd : %s" % (replicatorcmd))
        result = simplecommand(replicatorcmd)
        if result["code"] == 0:
            logger.warning(
                "the agent is already installed for version  %s" % (versiondata)
            )
        elif result["code"] == 1:
            logger.info("installed success agent version %s" % (versiondata))
        elif result["code"] == 120:
            logger.error(
                "installed default agent version %s (rollback previous version.). We will not switch to new agent."
                % (versiondata)
            )
        elif result["code"] == 121:
            logger.warning(
                "installed success agent version %s (unable to update the version in the registry.)"
                % (versiondata)
            )
        elif result["code"] == 122:
            logger.warning(
                "Some python modules needed for running lib are missing. We will not switch to new agent)"
            )
        elif result["code"] == 5:
            logger.warning(
                "mode replicator non permit dans pluging, ni installation agent. We will not switch to new agent."
            )
        else:
            logger.error(
                "installed agent version %s (indefinie operation). We will not switch to new agent."
                % (versiondata)
            )
            logger.error("return code is : %s" % (result["code"]))

    def checkinstallagent(self):
        if self.config.updating == 1:
            if os.path.isfile(os.path.join(self.pathagent, "BOOL_UPDATE_AGENT")):
                if self.descriptor_master is not None:
                    Update_Remote_Agenttest = Update_Remote_Agent(self.pathagent, True)
                    Update_Remote_Img = Update_Remote_Agent(self.img_agent, True)
                    logger.debug(
                        "Fingerprint of Remote Agenttest: %s"
                        % Update_Remote_Agenttest.get_fingerprint_agent_base()
                    )
                    logger.debug(
                        "Fingerprint of Remote Image: %s"
                        % Update_Remote_Img.get_fingerprint_agent_base()
                    )
                    logger.debug(
                        "Fingerprint of Master Image: %s"
                        % self.descriptor_master["fingerprint"]
                    )
                    if (
                        Update_Remote_Agenttest.get_fingerprint_agent_base()
                        != Update_Remote_Img.get_fingerprint_agent_base()
                        and Update_Remote_Img.get_fingerprint_agent_base()
                        == self.descriptor_master["fingerprint"]
                    ):
                        self.reinstall_agent()
                else:
                    logger.warning(
                        "We have been asked for an update but we are missing the descriptor"
                    )
                    os.remove(os.path.join(self.pathagent, "BOOL_UPDATE_AGENT"))

    def filtre_message(self, msg):
        pass

    async def message(self, msg):
        possibleclient = [
            "master",
            self.agentcommand.user,
            self.boundjid.user,
            "log",
            self.jidchatroomcommand.user,
        ] + self.agentsiveo
        possibleclient = [str(x) for x in possibleclient]
        if not msg["type"] == "chat":
            return
        try:
            dataobj = json.loads(msg["body"])

        except Exception as e:
            logging.error("bad struct Message %s %s " % (msg, str(e)))
            dataerreur = {
                "action": "resultmsginfoerror",
                "sessionid": "",
                "ret": 255,
                "base64": False,
                "data": {"msg": "ERROR : Message structure"},
            }
            self.send_message(
                mto=msg["from"], mbody=json.dumps(dataerreur), mtype="chat"
            )
            logger.error("\n%s" % (traceback.format_exc()))
            return

        if not str(msg["from"].user) in possibleclient:
            if not (
                "sessionid" in dataobj and self.session.isexist(dataobj["sessionid"])
            ):
                # les messages venant d'une machine sont filtré sauf si une
                # session message existe dans le gestionnaire de session.
                if self.config.ordreallagent:
                    logging.warning(
                        "filtre message from %s eg possible client" % (msg["from"].bare)
                    )
                    return

        dataerreur = {
            "action": "resultmsginfoerror",
            "sessionid": "",
            "ret": 255,
            "base64": False,
            "data": {"msg": ""},
        }

        if "action" not in dataobj:
            logging.error("warning message action missing %s" % (msg))
            return

        if dataobj["action"] == "restarfrommaster":
            reboot_command()
            return

        if dataobj["action"] == "shutdownfrommaster":
            msg = '"Shutdown from administrator"'
            time = 15  # default 15 seconde
            if "time" in dataobj["data"] and dataobj["data"]["time"] != 0:
                time = dataobj["data"]["time"]
            if "msg" in dataobj["data"] and dataobj["data"]["msg"] != "":
                msg = '"' + dataobj["data"]["msg"] + '"'

            shutdown_command(time, msg)
            return

        if dataobj["action"] == "vncchangepermsfrommaster":
            askpermission = 1
            if (
                "askpermission" in dataobj["data"]
                and dataobj["data"]["askpermission"] == "0"
            ):
                askpermission = 0

            vnc_set_permission(askpermission)
            return

        if dataobj["action"] == "installkeymaster":
            # note install publickeymaster
            self.masterpublickey = installpublickey(
                "master", dataobj["keypublicbase64"]
            )
            return

        if dataobj["action"] == "resultmsginfoerror":
            logging.warning(
                "filtre message from %s for action %s"
                % (msg["from"].bare, dataobj["action"])
            )
            return

        try:
            if "action" in dataobj and dataobj["action"] != "" and "data" in dataobj:
                if "base64" in dataobj and (
                    (isinstance(dataobj["base64"], bool) and dataobj["base64"] is True)
                    or (
                        isinstance(dataobj["base64"], str)
                        and dataobj["base64"].lower() == "true"
                    )
                ):
                    # data in base 64
                    mydata = json.loads(base64.b64decode(dataobj["data"]))
                else:
                    mydata = dataobj["data"]

                if "sessionid" not in dataobj:
                    dataobj["sessionid"] = getRandomName(6, "xmpp")
                    logging.warning(
                        "sessionid missing in message from %s : attributed sessionid %s "
                        % (msg["from"], dataobj["sessionid"])
                    )
                else:
                    if dataobj["sessionid"] in self.ban_deploy_sessionid_list:
                        # abort deploy if msg session id is banny
                        logging.info(
                            "ABORT DEPLOYMENT CANCELLED BY USER Sesion %s"
                            % dataobj["sessionid"]
                        )
                        self.xmpplog(
                            "<span class='log_err'>ABORT DEPLOYMENT CANCELLED BY USER</span>",
                            type="deploy",
                            sessionname=dataobj["sessionid"],
                            priority=-1,
                            action="xmpplog",
                            who=self.boundjid.bare,
                            how="",
                            why="",
                            module="Deployment | Banned",
                            date=None,
                            fromuser="MASTER",
                            touser="",
                        )
                        return

                del dataobj["data"]
                # traitement TEVENT
                # TEVENT event sended by remote machine ou RS
                # message adresse au gestionnaire evenement
                if (
                    "Dtypequery" in mydata
                    and mydata["Dtypequery"] == "TEVENT"
                    and self.session.isexist(dataobj["sessionid"])
                ):
                    mydata["Dtypequery"] = "TR"
                    datacontinue = {
                        "to": self.boundjid.bare,
                        "action": dataobj["action"],
                        "sessionid": dataobj["sessionid"],
                        "data": dict(
                            list(
                                self.session.sessionfromsessiondata(
                                    dataobj["sessionid"]
                                ).datasession.items()
                            )
                            + list(mydata.items())
                        ),
                        "ret": 0,
                        "base64": False,
                    }
                    # add Tevent gestion event
                    self.eventmanage.addevent(datacontinue)
                    return
                try:
                    msg["body"] = dataobj
                    logging.info(
                        "call plugin %s from %s" % (dataobj["action"], msg["from"].user)
                    )

                    call_plugin(
                        dataobj["action"],
                        self,
                        dataobj["action"],
                        dataobj["sessionid"],
                        mydata,
                        msg,
                        dataerreur,
                    )
                except TypeError:
                    if dataobj["action"] != "resultmsginfoerror":
                        dataerreur["data"]["msg"] = (
                            "ERROR : plugin %s Missing" % dataobj["action"]
                        )
                        dataerreur["action"] = "result%s" % dataobj["action"]
                        self.send_message(
                            mto=msg["from"], mbody=json.dumps(dataerreur), mtype="chat"
                        )
                    logging.error(
                        "TypeError execution plugin %s : [ERROR : plugin Missing] %s"
                        % (dataobj["action"], sys.exc_info()[0])
                    )
                    logger.error("\n%s" % (traceback.format_exc()))

                except Exception as e:
                    logging.error(
                        "execution plugin [%s]  : %s " % (dataobj["action"], str(e))
                    )
                    if dataobj["action"].startswith("result"):
                        return
                    if dataobj["action"] != "resultmsginfoerror":
                        dataerreur["data"]["msg"] = (
                            "ERROR : plugin execution %s" % dataobj["action"]
                        )
                        dataerreur["action"] = "result%s" % dataobj["action"]
                        self.send_message(
                            mto=msg["from"], mbody=json.dumps(dataerreur), mtype="chat"
                        )
                    logger.error("\n%s" % (traceback.format_exc()))
            else:
                if "data" not in dataobj:
                    msgerr = "data section missing;  msg : %s" % (msg["body"])
                if "action" in dataobj:
                    act = dataobj["action"]
                else:
                    act = ""
                dataerreur["data"]["msg"] = (
                    "ERROR : Action ignored : %s\n " "structure msg\n%s" % (act, msgerr)
                )
                self.send_message(
                    mto=msg["from"], mbody=json.dumps(dataerreur), mtype="chat"
                )
        except Exception as e:
            logging.error("bad struct Message %s %s " % (msg, str(e)))
            dataerreur["data"]["msg"] = "ERROR : Message structure"
            self.send_message(
                mto=msg["from"], mbody=json.dumps(dataerreur), mtype="chat"
            )
            logger.error("\n%s" % (traceback.format_exc()))

    def searchInfoMachine(self):
        xmppmask = ""
        xmppdhcp = ""
        xmppdhcpserver = ""
        xmppgateway = ""
        xmppmacaddress = ""
        xmppmacnotshortened = ""
        xmppbroadcast = ""

        er = networkagentinfo("master", "infomachine")
        er.messagejson["info"] = self.config.information

        er.messagejson["publickey"] = self.RSA.get_key_public()
        er.messagejson["publickeyname"] = self.RSA.get_name_key()[0]
        er.messagejson["privatekeyname"] = self.RSA.get_name_key()[1]
        # send if master public key public is missing
        er.messagejson["is_masterpublickey"] = self.RSA.isPublicKey("master")

        self.config.ipxmpp = getIpXmppInterface(self.config)
        NetworkInfos = NetworkInfoxmpp(self.config.Port)
        portconnection = self.config.Port
        if NetworkInfos.ip_address and NetworkInfos.details:
            if not self.config.ipxmpp:
                self.config.ipxmpp = NetworkInfos.ip_address
            xmppmask = (
                ""
                if NetworkInfos.details["netmask"] is None
                else NetworkInfos.details["netmask"]
            )
            xmppdhcp = (
                ""
                if NetworkInfos.details["dhcp_client"] is None
                else NetworkInfos.details["dhcp_client"]
            )
            xmppdhcpserver = (
                ""
                if NetworkInfos.details["dhcp_server"] is None
                else NetworkInfos.details["dhcp_server"]
            )
            xmppgateway = (
                ""
                if NetworkInfos.details["gateway"] is None
                else NetworkInfos.details["gateway"]
            )
            xmppmacaddress = (
                ""
                if NetworkInfos.details["macaddress"] is None
                else NetworkInfos.details["macaddress"]
            )
            xmppmacnotshortened = (
                ""
                if NetworkInfos.details["macnotshortened"] is None
                else NetworkInfos.details["macnotshortened"]
            )
            xmppbroadcast = (
                ""
                if NetworkInfos.details["broadcast"] is None
                else NetworkInfos.details["broadcast"]
            )
        else:
            return None

        if not xmppmask:
            logger.error(
                "We could not find a suitable network interface."
                + "\n\t '''''''' Please check your network interfaces ''''''''"
            )
            logreception = """
Imposible calculate subnetnetwork verify the configuration of %s [%s]
Check if ip [%s] is correct:
check if interface exist with ip %s

Warning Configuration machine %s
[connection]
server = It must be expressed in ip notation.

server = 127.0.0.1  correct
server = localhost in not correct
AGENT %s ERROR """ % (
                self.boundjid.bare,
                er.messagejson["info"]["hostname"],
                self.config.ipxmpp,
                self.config.ipxmpp,
                er.messagejson["info"]["hostname"],
                self.boundjid.bare,
            )
            logger.error(logreception)
            return None
        try:
            subnetreseauxmpp = subnetnetwork(self.config.ipxmpp, xmppmask)
        except Exception:
            logger.error(
                "We failed to calculate the subnetnetwork, we hit this backtrace\n"
            )
            logger.error(f"\n {traceback.format_exc()}")
            logger.error(
                f"The variable xmppmask comes from {er.messagejson['listipinfo']}"
            )
            logreception = """
Imposible calculate subnetnetwork verify the configuration of %s [%s]
Check if ip [%s] is correct:
check if interface exist with ip %s

Warning Configuration machine %s
[connection]
server = It must be expressed in ip notation.

server = 127.0.0.1  correct
server = localhost in not correct
AGENT %s ERROR TERMINATE""" % (
                self.boundjid.bare,
                er.messagejson["info"]["hostname"],
                self.config.ipxmpp,
                self.config.ipxmpp,
                er.messagejson["info"]["hostname"],
                self.boundjid.bare,
            )
            self.loginfotomaster(logreception)
            sys.exit(0)

        if self.config.public_ip is None:
            self.config.public_ip = self.config.ipxmpp
        remoteservice = protodef()
        # || condition de reconf complet
        self.FullRegistration = remoteservice.boolchangerproto
        # on search if exist fileboolreconfcomple
        BOOLFILECOMPLETREGISTRATION = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "BOOLFILECOMPLETREGISTRATION"
        )
        if os.path.exists(BOOLFILECOMPLETREGISTRATION):
            self.FullRegistration = True
            os.remove(BOOLFILECOMPLETREGISTRATION)
        dataobj = {
            "action": "infomachine",
            "from": self.config.jidagent,
            "compress": False,
            "deployment": self.config.jidchatroomcommand,
            "who": "%s/%s" % (self.config.jidchatroomcommand, self.config.NickName),
            "machine": self.config.NickName,
            "platform": os_version(),
            "completedatamachine": base64.b64encode(
                json.dumps(er.messagejson).encode("utf-8")
            ).decode("utf-8"),
            "plugin": {},
            "pluginscheduled": {},
            "versionagent": self.version_agent(),
            "portxmpp": self.config.Port,
            "serverxmpp": self.config.Server,
            "agenttype": self.config.agenttype,
            "baseurlguacamole": self.config.baseurlguacamole,
            "subnetxmpp": subnetreseauxmpp,
            "xmppip": self.config.ipxmpp,
            "xmppmask": xmppmask,
            "xmppbroadcast": xmppbroadcast,
            "xmppdhcp": xmppdhcp,
            "xmppdhcpserver": xmppdhcpserver,
            "xmppgateway": xmppgateway,
            "xmppmacaddress": xmppmacaddress,
            "xmppmacnotshortened": xmppmacnotshortened,
            "ipconnection": self.ipconnection,
            "portconnection": portconnection,
            "classutil": self.config.classutil,
            "ippublic": self.config.public_ip,
            "geolocalisation": {},
            "remoteservice": remoteservice.proto,
            "regcomplet": self.FullRegistration,
            "packageserver": self.config.packageserver,
            "adorgbymachine": base64.b64encode(
                organizationbymachine().encode("utf-8")
            ).decode("utf-8"),
            "adorgbyuser": "",
            "kiosk_presence": test_kiosk_presence(),
            "countstart": save_count_start(),
            "keysyncthing": self.deviceid,
            "uuid_serial_machine": serialnumbermachine(),
            "updatingagent": self.config.updating,
            "system_info": offline_search_kb().get(),
        }
        try:
            dataobj["md5_conf_monitoring"] = ""
            # self.monitoring_agent_config_file
            if (
                self.config.agenttype not in ["relayserver"]
                and hasattr(self.config, "monitoring_agent_config_file")
                and self.config.monitoring_agent_config_file != ""
                and os.path.exists(self.config.monitoring_agent_config_file)
            ):
                dataobj["md5_conf_monitoring"] = hashlib.md5(
                    file_get_contents(self.config.monitoring_agent_config_file)
                ).hexdigest()
        except AttributeError:
            logging.debug("The monitoring configuration file is missing")
        except Exception as e:
            logging.error("%s error on file config monitoring" % str(e))

        if self.config.agenttype in ["relayserver"]:
            try:
                dataobj["syncthing_port"] = self.config.syncthing_port
            except Exception:
                pass
        if self.geodata is not None:
            dataobj["geolocalisation"] = self.geodata.localisation
        else:
            logging.debug("The geolocalisation is disabled")
        try:
            if self.config.agenttype in ["relayserver"]:
                dataobj["moderelayserver"] = self.config.moderelayserver
                if dataobj["moderelayserver"] == "dynamic":
                    dataobj["packageserver"]["public_ip"] = self.config.ipxmpp
        except Exception:
            dataobj["moderelayserver"] = "static"
        md5agentversion = Update_Remote_Agent(
            self.pathagent, True
        ).get_fingerprint_agent_base()
        dataobj["md5agentversion"] = md5agentversion
        if self.config.updating == 1:
            dataobj["md5agent"] = md5agentversion
        # todo determination lastusersession to review
        lastusersession = ""
        userlist = list({users[0] for users in psutil.users()})
        if len(userlist) > 0:
            lastusersession = userlist[0]
        if lastusersession != "":
            dataobj["adorgbyuser"] = base64.b64encode(
                organizationbyuser(lastusersession).encode("utf-8")
            ).decode("utf-8")
            dataobj["adusergroups"] = base64.b64encode(
                adusergroups(lastusersession).encode("utf-8")
            ).decode("utf-8")

        dataobj["lastusersession"] = lastusersession
        sys.path.append(self.config.pathplugins)
        for element in os.listdir(self.config.pathplugins):
            if element.endswith(".py") and element.startswith("plugin_"):
                try:
                    mod = __import__(element[:-3])
                    importlib.reload(mod)
                    module = getattr(mod, "plugin")
                    dataobj["plugin"][module["NAME"]] = module["VERSION"]
                except Exception as e:
                    logger.error(
                        "error loading plugin %s : %s verify plugin %s and import"
                        % (element, str(e), element)
                    )
        # add list scheduler plugins
        dataobj["pluginscheduled"] = self.loadPluginschedulerList()
        # persistence info machine
        self.infomain = dataobj
        self.dataplugininstall = {
            "plu": dataobj["plugin"],
            "schedule": dataobj["pluginscheduled"],
        }
        return dataobj

    def loadPluginschedulerList(self):
        logger.debug("Verify base plugin scheduler")
        plugindatasearch = {}
        for element in os.listdir(self.config.pathpluginsscheduled):
            if element.endswith(".py") and element.startswith("scheduling_"):
                f = open(os.path.join(self.config.pathpluginsscheduled, element), "r")
                lignes = f.readlines()
                f.close()
                for ligne in lignes:
                    if "VERSION" in ligne and "NAME" in ligne:
                        l = ligne.split("=")
                        plugin = eval(l[1])
                        plugindatasearch[plugin["NAME"]] = plugin["VERSION"]
                        break
        return plugindatasearch

    def module_needed(self):
        finder = ModuleFinder()
        newdescriptorimage = Update_Remote_Agent(self.img_agent)
        for file in newdescriptorimage.get_md5_descriptor_agent()["program_agent"]:
            finder.run_script(os.path.join(self.img_agent, file))
            for name, mod in list(finder.modules.items()):
                try:
                    __import__(name.split(".", 1)[0])
                except ImportError:
                    logging.warning(
                        "The following python module needs to be installed first: %s"
                        % (name)
                    )
                    return True
        for file in newdescriptorimage.get_md5_descriptor_agent()["lib_agent"]:
            finder.run_script(os.path.join(self.img_agent, "lib", file))
            for name, mod in list(finder.modules.items()):
                try:
                    __import__(name.split(".", 1)[0])
                except ImportError:
                    logging.warning(
                        "The following python module needs to be installed first: %s"
                        % (name)
                    )
                    return True
        return False


def createDaemon(
    optstypemachine,
    optsconsoledebug,
    optsdeamon,
    tgfichierconf,
    tglevellog,
    tglogfile,
    shared_dict,
    terminate_lock,
):
    """
    This function create a service/Daemon that will execute a det. task
    """
    try:
        if sys.platform.startswith("win"):
            while True:
                p = multiprocessing.Process(
                    name="xmppagent",
                    target=doTask,
                    args=(
                        optstypemachine,
                        optsconsoledebug,
                        optsdeamon,
                        tgfichierconf,
                        tglevellog,
                        tglogfile,
                        shared_dict,
                        terminate_lock,
                    ),
                )
                p.daemon = True
                p.start()
                while p.is_alive():
                    with terminate_lock:
                        if shared_dict.get("terminate"):
                            logging.info("Terminating daemon process...")
                            p.terminate()
                            p.join()
                            break
                time.sleep(1)
        else:
            pid = os.getpid()
            pidfile_path = "/var/run/xmpp_agent_pulse_%s.pid" % optstypemachine
            with open(pidfile_path, "w") as f:
                f.write("%s\n" % pid)
            try:
                while True:
                    doTask(
                        optstypemachine,
                        optsconsoledebug,
                        optsdeamon,
                        tgfichierconf,
                        tglevellog,
                        tglogfile,
                        shared_dict,
                        terminate_lock,
                    )
                    with terminate_lock:
                        if shared_dict.get("terminate"):
                            logging.info("Terminating daemon process...")
                            break
                    time.sleep(1)  # Pause pour éviter une boucle excessive
            except KeyboardInterrupt:
                logging.info("Daemon interrupted by CTRL-C.")
            finally:
                # Nettoyage final
                os.remove(pidfile_path)
                logging.info("Daemon process terminated and PID file removed.")
    except OSError as error:
        logging.error("Unable to fork. Error: %d (%s)" % (error.errno, error.strerror))
        logging.error("\n%s" % (traceback.format_exc()))


def tgconf(optstypemachine):
    tg = confParameter(optstypemachine)

    if optstypemachine.lower() in ["machine"]:
        tg.pathplugins = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "pluginsmachine"
        )
        tg.pathpluginsscheduled = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "descriptor_scheduler_machine"
        )
    else:
        tg.pathplugins = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "pluginsrelay"
        )
        tg.pathpluginsscheduled = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "descriptor_scheduler_relay"
        )

    while True:
        if tg.Server == "" or tg.Port == "":
            logger.error("Error config ; Parameter Connection missing")
            sys.exit(1)
        if ipfromdns(tg.Server) != "" and check_exist_ip_port(
            ipfromdns(tg.Server), tg.Port
        ):
            break
        logging.log(
            DEBUGPULSE,
            "Unable to connect. (%s : %s) on xmpp server."
            " Check that %s can be resolved" % (tg.Server, tg.Port, tg.Server),
        )
        logger.debug("verify a information ip or dns for connection AM")
        if ipfromdns(tg.Server) == "":
            logger.debug("not resolution adresse : %s " % tg.Server)
        time.sleep(2)
    return tg


# ==========================
# = cherrypy server config =
# ==========================


def servercherrypy(
    optstypemachine,
    optsconsoledebug,
    optsdeamon,
    tgnamefileconfig,
    tglevellog,
    tglogfile,
    shared_dict,
    terminate_lock,
):
    config = confParameter(optstypemachine)
    if config.agenttype in ["machine"]:
        port = 52044
        root_path = os.path.dirname(os.path.realpath(__file__))
        server_path = os.path.join(root_path, "lib")
        server_ressources_path = os.path.join(root_path, "lib", "ressources")

        Controller.config = config
        # Generate cherrypy server conf
        server_conf = {
            # Root access
            "global": {
                "server.socket_host": config.fv_host,
                "server.socket_port": config.fv_port,
            },
            "/": {
                # 'tools.staticdir.on': True,
                "tools.staticdir.dir": server_path
            },
            # Sharing css ...
            "/css": {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": os.path.join(
                    server_ressources_path, "fileviewer", "css"
                ),
            },
            # Sharing js ...
            "/js": {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": os.path.join(
                    server_ressources_path, "fileviewer", "js"
                ),
            },
            # Sharing images ...
            "/images": {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": os.path.join(
                    server_ressources_path, "fileviewer", "images"
                ),
            },
            # Alias to images for datatables js lib
            "/DataTables-1.10.21/images": {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": os.path.join(
                    server_ressources_path, "fileviewer", "images"
                ),
            },
            # Sharing fonts
            "/fonts": {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": os.path.join(
                    server_ressources_path, "fileviewer", "fonts"
                ),
            },
        }
        count = 0
        for path in config.paths:
            name = config.names[count]
            # Here we know the name and the path, we can add the access for
            # each folders
            server_conf["/%s" % str(name)] = {
                "tools.staticdir.on": True,
                "tools.staticdir.dir": str(path),
            }
            count += 1
        cherrypy.tree.mount(Controller(), "/", server_conf)
        # We will create our own server so we don't need the
        # default one

        cherrypy.server.unsubscribe()
        server1 = cherrypy._cpserver.Server()
        server1.socket_port = config.fv_port
        server1._socket_host = config.fv_host

        # ===
        # Do not remove the following lines
        # They can be usefull to configure the server
        # ===

        server1.subscribe()
        cherrypy.engine.start()


def shuffle_listars(config):
    """
    Réorganise la liste 'listars' dans le dictionnaire de configuration.

    Cette fonction conserve le premier élément de la liste 'listars' à sa position initiale
    et réorganise les autres éléments de manière aléatoire. La liste 'listars' contient les serveurs
    (cluster) qui peuvent satisfaire une connexion pour cette machine. En mélangeant les éléments
    de la liste, on introduit une certaine entropie, ce qui permet de répartir les connexions des
    machines de manière aléatoire sur le cluster. Cela évite que toutes les machines d'un ARS tombé
    se connectent au même serveur alternatif.

    Args:
        config (dict): Le dictionnaire de configuration contenant la liste 'listars'.

    Returns:
        None
    """
    # Vérifier si la liste 'listars' contient plus d'un élément
    if len(config["listars"]) > 1:
        # Extraire le premier élément de la liste
        first_element = config["listars"][0]
        # Extraire les autres éléments de la liste
        other_elements = config["listars"][1:]
        # Mélanger les autres éléments de manière aléatoire
        random.shuffle(other_elements)
        # Reconstruire la liste en plaçant le premier élément à sa position initiale
        # suivi des éléments mélangés
        config["listars"] = [first_element] + other_elements


def initialize_alternatifconnection(optstypemachine):
    """
    Initialise la variable alternatifconnection en fonction des informations de configuration.

    :param optstypemachine: Type de machine (par exemple, "relay").
    :return: La variable alternatifconnection initialisée.
    """
    # Chemin du fichier alternative
    namefilealternatifconnection = conffilename("cluster")
    if os.path.isfile(namefilealternatifconnection):
        alternatifconnection = nextalternativeclusterconnectioninformation(
            namefilealternatifconnection
        )
        if (
            "nbserver" not in alternatifconnection
            or "listars" not in alternatifconnection
            or "nextserver" not in alternatifconnection
        ):
            alternatifconnection = (
                None  # json cluster.ini pas correct pas d'alternative.
            )

        if optstypemachine.lower() not in ["relay"]:
            shuffle_listars(alternatifconnection)
            logger.debug(
                "list server connection possible (alternative cluster) %s"
                % alternatifconnection["listars"]
            )
        else:
            alternatifconnection = {}  # 1 seul serveur pas d'alternative.
    else:
        alternatifconnection = None  # Fichier de configuration non trouvé

    return alternatifconnection


def read_alternatifconnection():
    """
    Lecture et vérification des informations d'une connexion alternative
    pour un cluster, basée sur le fichier de configuration.
    """
    alternatifconnection = None

    # Récupération du nom du fichier de configuration
    namefilealternatifconnection = conffilename("cluster")

    if os.path.isfile(namefilealternatifconnection):
        # Lecture des informations de connexion alternative
        alternatifconnection = nextalternativeclusterconnectioninformation(
            namefilealternatifconnection
        )

        logger.info(f"Composition alternative : {namefilealternatifconnection}")

        # Validation des informations obtenues
        if (
            "nbserver" not in alternatifconnection
            or "listars" not in alternatifconnection
            or "nextserver" not in alternatifconnection
        ):
            # Si le fichier est mal formé, alternative impossible
            return None
        return alternatifconnection
    else:
        logger.warning(f"Fichier {namefilealternatifconnection} introuvable.")
        return None


def is_process_running(pid):
    """
    Vérifie si un processus avec le PID spécifié est en cours d'exécution.

    Args:
        pid (int): Le PID du processus à vérifier.

    Returns:
        bool: True si le processus est en cours d'exécution, False sinon.
    """
    try:
        process = psutil.Process(pid)
        return process.is_running()
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        print(f"Accès refusé pour le PID {pid}")
        return False
    except psutil.ZombieProcess:
        print(f"Le processus avec le PID {pid} est un processus zombie")
        return False


def launch_standalone_program(
    pathagent=None, program_name="connectionagent.py", options="-t machine"
):
    """
    Lance un programme standalone Python sans attendre de résultat.

    Args:
        pathagent (str, optional): Répertoire contenant le programme à exécuter.
                                   Si None, utilise le répertoire courant.
        program_name (str, optional): Nom du programme à exécuter. Par défaut, "connectionagent.py".
        options (str, optional): Options à passer au programme. Par défaut, "-t relayserver".
    """
    if pathagent is None:
        # Le programme à lancer dans le même répertoire
        pathagent = os.path.dirname(
            os.path.abspath(__file__)
        )  # Répertoire courant du programme

    try:
        # Obtenir le chemin complet du programme à lancer
        program_path = os.path.join(pathagent, program_name)

        # Obtenir l'exécutable Python utilisé par le processus actuel
        pythonexec = psutil.Process().exe()

        # Vérifier si le fichier existe
        if not os.path.isfile(program_path):
            raise FileNotFoundError(
                f"Le programme {pythonexec} {program_path} est introuvable."
            )

        # Construire la commande à exécuter
        command = [pythonexec, program_path] + options.split()

        # Lancer le programme de manière asynchrone
        process = subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        logger.info(
            f"Programme lancé : {pythonexec} {program_path} avec les options : {options}"
        )

        # Retourner le PID du processus lancé
        return process.pid

    except FileNotFoundError as fnf_error:
        logger.error(f"Erreur : {fnf_error}")
    except Exception as e:
        logger.error(f"Erreur lors du lancement du programme : {e}")
    return None


def doTask(
    optstypemachine,
    optsconsoledebug,
    optsdeamon,
    tgnamefileconfig,
    tglevellog,
    tglogfile,
    shared_dict,
    terminate_lock,
):
    processes = []
    listpid = []
    listpid.append(os.getpid())
    queue_recv_tcp_to_xmpp = Queue()
    queueout = Queue()
    # event inter process
    eventkilltcp = Event()
    eventkillpipe = Event()
    pidfile = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "INFOSTMP", "pidagent"
    )
    pid_connecteur = None
    alternatifconnection = None
    file_put_contents(pidfile, "%s" % os.getpid())
    tg = confParameter(optstypemachine)
    if sys.platform.startswith("win"):
        try:
            result = subprocess.check_output(
                [
                    "icacls",
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "INFOSTMP",
                        "pidagent",
                    ),
                    "/setowner",
                    "pulse",
                    "/t",
                ],
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            pass
    global signalint

    if optstypemachine.lower() in ["machine"]:
        sys.path.append(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "pluginsmachine")
        )
    else:
        sys.path.append(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "pluginsrelay")
        )
    # ==========================
    # = cherrypy server config =
    # ==========================
    # pcherry = Process(
    # target=servercherrypy,
    # name="cherrypy",
    # args=(
    # optstypemachine,
    # optsconsoledebug,
    # optsdeamon,
    # tgnamefileconfig,
    # tglevellog,
    # tglogfile,
    # shared_dict,
    # terminate_lock,
    # ),
    # )
    # processes.append(pcherry)
    # listpid.append(pcherry.pid)
    # pcherry.start()

    pid_file_path_xmppagent_win = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "INFOSTMP", "pidagentwintreename"
    )

    # on lit les alternatives si elle existe. 1 fois suivant alternatifconnection initialise ou pas
    if not alternatifconnection:
        logger.debug("composition  alternative")
        alternatifconnection = initialize_alternatifconnection(optstypemachine)

    if not alternatifconnection:
        with terminate_lock:
            shared_dict["alternative"] = False
    logger.debug(
        "list server connection possible (alternative cluster) %s"
        % alternatifconnection
    )

    with terminate_lock:
        shared_dict["new_connection"] = False

    while True:
        # Lit le fichier de configuration alternative
        alternatifconnection = read_alternatifconnection()

        # Incrémente la valeur de "compteur" dans le dictionnaire partagé
        with terminate_lock:
            shared_dict["compteur"] += 1

        # Enregistre la valeur actuelle du compteur dans le logger
        logger.debug("shared_dict_compteur : %s" % shared_dict["compteur"])

        # Définit le nombre d'essais pour vérifier le compteur et lanceer 1 reconf des fichiers.
        tous_les_n_essais = 10

        # Vérifie si le compteur est un multiple de 10 et n'est pas égal à 0
        if (
            shared_dict["compteur"] % tous_les_n_essais == 0
            and shared_dict["compteur"] != 0
        ):

            # Si le compteur est un multiple de 10 et n'est pas égal à 0, lance connectionagent.py
            if pid_connecteur is None:
                # Si pid_connecteur est None, lance le programme standalone
                pid_connecteur = launch_standalone_program()
                with terminate_lock:
                    shared_dict["reconnect"] = True
                    # Construire le chemin du fichier
                    # force_reconfiguration = os.path.join(
                    # os.path.dirname(os.path.realpath(__file__)),
                    # "action_force_reconfiguration" )
                    # # Créer le fichier vide
                    # with open(force_reconfiguration, 'w') as f:
                    # pass  # Le fichier est créé vide

            else:
                # Si pid_connecteur n'est pas None, vérifie si le processus est en cours d'exécution
                if not is_process_running(pid_connecteur):
                    # Si le processus n'est pas en cours d'exécution, réinitialise pid_connecteur à None
                    pid_connecteur = None
                    # # reconnect demande
                    # with terminate_lock:
                    # shared_dict["reconnect"]= True

        with terminate_lock:
            if shared_dict.get("terminate"):
                logging.info(" termine boucle process")
                break
        p = Process(
            target=process_xmpp_agent,
            name="xmppagent",
            args=(
                optstypemachine,
                optsconsoledebug,
                optsdeamon,
                tglevellog,
                tglogfile,
                queue_recv_tcp_to_xmpp,
                queueout,
                eventkilltcp,
                eventkillpipe,
                os.getpid(),
                shared_dict,
                terminate_lock,
                alternatifconnection,
            ),
        )
        processes.append(p)
        listpid.append(p.pid)
        p.start()
        file_put_contents(
            pid_file_path_xmppagent_win,
            "from %s : %s %s" % (os.getpid(), p.name, p.pid),
        )
        logger.info(
            "%s -> %s : [Process Alive %s (%s)]" % (os.getpid(), p.pid, p.name, p.pid)
        )
        while p.is_alive():
            with terminate_lock:
                if shared_dict.get("terminate"):
                    logger.debug(" TERMINATE %s" % alternatifconnection)
                    # pcherry.terminate()
                    p.terminate()
                    # pcherry.join()
                    p.join()
                    processes.remove(p)  # Supprimer le processus terminé
                    # processes.remove(pcherry)  # Supprimer le processus terminé
                    # listpid.remove(pcherry.pid)
                    listpid.remove(p.pid)
                    if p.pid in listpid:
                        listpid.remove(p.pid)
                    else:
                        logger.warning(f"PID {p.pid} not found in listpid")
                    break
                if shared_dict.get("alternative") and shared_dict.get("new_connection"):
                    logger.info("NEW CONECTION %s" % alternatifconnection)
                    # pcherry.terminate()
                    p.terminate()
                    # pcherry.join()
                    p.join()
                    processes.remove(p)  # Supprimer le processus terminé
                    # processes.remove(pcherry)  # Supprimer le processus terminé
                    # listpid.remove(pcherry.pid)
                    if p.pid in listpid:
                        listpid.remove(p.pid)
                    else:
                        logger.warning(f"PID {p.pid} not found in listpid")
                    break
            time.sleep(1)
        logger.warning("QUIT PROCESS")
        with terminate_lock:
            if not shared_dict.get("new_connection"):
                logger.debug("nouveau process process")
                break
            shared_dict["new_connection"] = False

    with terminate_lock:
        shared_dict["terminate"] = True
    for proc in processes:
        if proc.is_alive():
            proc.terminate()
            proc.join()
    logger.debug("The Pulse Xmpp Agent Relay is now stopped")


class process_xmpp_agent:
    def __init__(
        self,
        optstypemachine,
        optsconsoledebug,
        optsdeamon,
        tglevellog,
        tglogfile,
        queue_recv_tcp_to_xmpp,
        queueout,
        eventkilltcp,
        eventkillpipe,
        pidprogrammprincipal,
        shared_dict,
        terminate_lock,
        alternatifconnection,
    ):
        # parameter permet arret programme complet  ICI PASSER PARAMETRE DANS XMPPBOT
        self.alternatifconnection = alternatifconnection
        self.pidprogrammprincipal = pidprogrammprincipal
        self.shared_dict = shared_dict
        # self.terminate_lock =  terminate_lock
        if sys.platform.startswith("win"):
            format = "%(asctime)s - %(levelname)s - (AG_EVENT)%(message)s"
            formatter = logging.Formatter(format)

        logger = logging.getLogger()  # either the given logger or the root logger
        logger.setLevel(tglevellog)
        # If the logger has handlers, we configure the first one. Otherwise we add a handler and configure it
        if sys.platform.startswith("win"):
            if logger.handlers:
                console = logger.handlers[
                    0
                ]  # we assume the first handler is the one we want to configure
            else:
                console = logging.StreamHandler()
                logger.addHandler(console)
            console.setFormatter(formatter)
            console.setLevel(tglevellog)

            file_handler = logging.FileHandler(tglogfile)
            file_handler.setLevel(tglevellog)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        self.logger = logging.getLogger()
        # while self.process_restartbot:
        self.process_restartbot = False
        self.logger.debug("/---------------------------------\\")
        self.logger.debug("|--- INITIALISATION XMPP AGENT ---|")
        self.logger.debug("\---------------------------------/")

        setgetcountcycle()

        setgetrestart()
        while True:
            tg = tgconf(optstypemachine)
            self.logger.debug(f"{tg.Server}{ int(tg.Port)}")
            xmpp = MUCBot(
                tg,
                queue_recv_tcp_to_xmpp,
                queueout,
                eventkilltcp,
                eventkillpipe,
                pidprogrammprincipal,
                shared_dict,
                terminate_lock,
                alternatifconnection,
            )
            xmpp.auto_reconnect = False
            xmpp.register_plugin("xep_0030")  # Service Discovery
            xmpp.register_plugin("xep_0045")  # Multi-User Chat
            xmpp.register_plugin("xep_0004")  # Data Forms
            xmpp.register_plugin("xep_0050")  # Adhoc Commands
            xmpp.register_plugin(
                "xep_0199",
                {"keepalive": True, "frequency": 600, "interval": 600, "timeout": 500},
            )
            xmpp.register_plugin("xep_0077")  # In-band Registration
            xmpp["xep_0077"].force_registration = True
            xmpp.server_address = (ipfromdns(tg.Server), int(tg.Port))
            time.sleep(0.2)
            try:
                self.readconfig_Marche_Arret
            except:
                self.readconfig_Marche_Arret = True

            self.logger.debug("/---------------------------------\\")
            self.logger.debug("|----- CONNECTION XMPP AGENT -----|")
            self.logger.debug("\---------------------------------/")
            try:
                xmpp.connect(address=xmpp.server_address)
            except Exception as e:
                self.logger.error("Connection failed: %s. Retrying..." % e)
            try:
                loop = asyncio.get_event_loop()
                loop.run_forever()
            except RuntimeError:
                self.logger.error("RuntimeError during connection")
            finally:
                # loop.close()
                pass

            with terminate_lock:
                if shared_dict.get("terminate"):
                    self.logger.info("CONNECTION XMPP AGENT")
                    self.logger.info("termine application")
                    loop.close()
                    break

                if shared_dict.get("alternative"):

                    self.logger.info("CONNECTION XMPP AGENT")
                    self.logger.info("cherche alternative")
                    # loop.close()

            self.logger.info("REBOUCLE")
        terminateserver(xmpp)


def terminateserver(xmpp):
    # event for quit loop server tcpserver for kiosk
    logger.debug("terminateserver")
    # if xmpp.config.agenttype in ["relayserver"]:
    #     xmpp.qin.put("quit")
    xmpp.queue_read_event_from_command.put("quit")

    # if xmpp.config.agenttype in ["relayserver"]:
    #     xmpp.managerQueue.shutdown()
    # termine server kiosk
    xmpp.eventkiosk.quit()
    xmpp.eventkilltcp.set()
    xmpp.eventkillpipe.set()
    if sys.platform.startswith("win"):
        try:
            # on debloque le pipe
            fileHandle = win32file.CreateFile(
                "\\\\.\\pipe\\interfacechang",
                win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                0,
                None,
                win32file.OPEN_EXISTING,
                0,
                None,
            )
            win32file.WriteFile(fileHandle, "terminate")
            fileHandle.Close()
        except Exception as e:
            logger.error("\n%s" % (traceback.format_exc()))
            pass
    logger.debug("wait 2s end thread event loop")
    logger.debug("terminate manage data sharing")
    time.sleep(2)
    logger.debug("terminate scheduler")
    logger.debug("Waiting to stop kiosk server")
    logger.debug("QUIT")
    logger.debug("bye bye Agent")
    if sys.platform.startswith("win"):
        windowfilepid = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "INFOSTMP", "pidagentwintree"
        )
        with open(windowfilepid) as json_data:
            data_dict = json.load(json_data)
        pythonmainproces = ""

        for pidprocess in data_dict:
            if "pythonmainproces" in data_dict[pidprocess]:
                pythonmainproces = pidprocess
        if pythonmainproces != "":
            logger.debug("TERMINE process pid %s" % pythonmainproces)
            pidfile = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "INFOSTMP", "pidagent"
            )
            aa = file_get_contents(pidfile).strip()
            logger.debug("process pid file pidagent is %s" % aa)
            cmd = "TASKKILL /F /PID %s /T" % pythonmainproces
            os.system(cmd)
    os._exit(0)


if __name__ == "__main__":
    if sys.platform.startswith("linux") and os.getuid() != 0:
        print("Agent must be running as root")
        sys.exit(0)
    elif sys.platform.startswith("win") and isWinUserAdmin() == 0:
        print("Medulla agent must be running as Administrator")
        sys.exit(0)
    elif sys.platform.startswith("darwin") and not isMacOsUserAdmin():
        print("Medulla agent must be running as root")
        sys.exit(0)
    optp = OptionParser()
    optp.add_option(
        "-d",
        "--deamon",
        action="store_true",
        dest="deamon",
        default=False,
        help="deamonize process",
    )
    optp.add_option(
        "-t",
        "--type",
        dest="typemachine",
        default=False,
        help="Type machine : machine or relayserver",
    )
    optp.add_option(
        "-c",
        "--consoledebug",
        action="store_true",
        dest="consoledebug",
        default=False,
        help="console debug",
    )
    # Ce dict est partage entre tout les processs
    manager = multiprocessing.Manager()
    shared_dict = manager.dict()
    shared_dict["terminate"] = False
    shared_dict["reconnect"] = False
    shared_dict["compteur"] = 0
    shared_dict["alternative"] = True
    shared_dict["new_connection"] = False

    opts, args = optp.parse_args()

    try:
        tg = confParameter(opts.typemachine)
    except configparser.NoSectionError:
        if opts.typemachine.lower() in ["machine"] and os.path.exists(
            conffilename(opts.typemachine.lower()) + ".tpl"
        ):
            logger.error(
                "The agentconf.ini file does not exist. We add the template file"
            )
            shutil.copy(
                conffilename(opts.typemachine.lower()) + ".tpl",
                conffilename(opts.typemachine.lower()),
            )
            sys.exit(0)
    except Exception as e:
        logger.error(str(e))

    # termine ssh reverse
    if sys.platform.startswith("win"):
        searchreversesshprocess = os.path.join(medullaPath(), "bin")
        for f in [
            os.path.join(medullaPath(), "bin", x)
            for x in os.listdir(searchreversesshprocess)
            if x[-4:] == ".pid"
        ]:
            pid = file_get_contents(f).strip(" \n\r\t")
            cmd = "taskkill /F /PID %s" % str(pid)
            logger.info(cmd)
            simplecommand(cmd)
            os.remove(f)
    mfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), "DEBUG_AGENT")
    if opts.consoledebug or os.path.isfile(mfile) or os.path.isfile(f"{mfile}.txt"):
        tg.levellog = logging.DEBUG

    format = "%(asctime)s - %(levelname)s - (AGENT)%(message)s"
    formatter = logging.Formatter(format)
    logging.basicConfig(level=tg.levellog, format=format)
    logger = logging.getLogger()  # either the given logger or the root logger
    logger.setLevel(tg.levellog)
    # If the logger has handlers, we configure the first one. Otherwise we add a handler and configure it
    if logger.handlers:
        console = logger.handlers[
            0
        ]  # we assume the first handler is the one we want to configure
    else:
        console = logging.StreamHandler()
        logger.addHandler(console)
    console.setFormatter(formatter)
    console.setLevel(tg.levellog)
    file_handler = logging.FileHandler(tg.logfile)
    file_handler.setLevel(tg.levellog)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if not opts.deamon:
        doTask(
            opts.typemachine,
            opts.consoledebug,
            opts.deamon,
            tg.namefileconfig,
            tg.levellog,
            tg.logfile,
            shared_dict,
            terminate_lock,
        )
    else:
        createDaemon(
            opts.typemachine,
            opts.consoledebug,
            opts.deamon,
            tg.namefileconfig,
            tg.levellog,
            tg.logfile,
            shared_dict,
            terminate_lock,
        )
    logger.info("Ferme Connection manager")
    # Fermer proprement le manager
    manager.shutdown()
