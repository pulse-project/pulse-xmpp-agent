# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: 2020-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from distutils.version import StrictVersion
import logging
import platform
import tempfile
import os
import winreg
import traceback
from lib import utils

TIGHTVNC = "2.8.84"

logger = logging.getLogger()

plugin = {"VERSION": "1.71", "NAME": "updatetightvnc", "TYPE": "machine"}  # fmt: skip


@utils.set_logging_level
def action(xmppobject, action, sessionid, data, message, dataerreur):
    logger.debug("###################################################")
    logger.debug(f"PL-TIGHT call {plugin} from {message['from']}")
    logger.debug("###################################################")
    try:
        # Update if version is lower
        installed_version = checktightvncversion()
        if StrictVersion(installed_version) < StrictVersion(TIGHTVNC):
            updatetightvnc(xmppobject)
        check_tightvnc_configuration(xmppobject)
    except Exception as error_plugin:
        logger.debug(f"PL-TIGHT failed with the error {error_plugin}")
        logger.error(f"PL_TIGHT failed with the backtrace \n {traceback.format_exc()}")
        pass


def check_tightvnc_configuration(xmppobject):
    """
    Check and modify TightVNC Server registry keys as necessary
    and restart the service if any changes are made.
    """
    if sys.platform.startswith("win"):
        configurations = [
            {
                "key": "AllowLoopback",
                "type": winreg.REG_DWORD,
                "value": 0x0,
                "set_value": 0,
            },
            {
                "key": "LoopbackOnly",
                "type": winreg.REG_DWORD,
                "value": 0x0,
                "set_value": 0,
            },
            {
                "key": "AcceptHttpConnections",
                "type": winreg.REG_DWORD,
                "value": 0x0,
                "set_value": 0,
            },
            {
                "key": "UseVncAuthentication",
                "type": winreg.REG_DWORD,
                "value": 0x1,
                "set_value": 1,
            },
            {
                "key": "Password",
                "type": winreg.REG_BINARY,
                "value": bin(xmppobject.config.password_rw),
                "set_value": bin(xmppobject.config.password_rw),
            },
        ]
        need_restart = False
        # Open the registry key and assign it to a variable
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\TightVNC\Server",
            0,
            winreg.KEY_ALL_ACCESS,
        )
        for config in configurations:
            try:
                # Query the current value of the key
                value, _ = winreg.QueryValueEx(key, config["key"])
                if value != config["value"]:
                    # Modify the key
                    winreg.SetValueEx(
                        key, config["key"], 0, config["type"], config["set_value"]
                    )
                    logger.debug(
                        f"PL-TIGHT TightVNCServer registry {config['key']} with value {value} is reconfigured."
                    )
                    need_restart = True
                else:
                    logger.debug(
                        f"PL-TIGHT TightVNC Server registry key {config['key']} is correctly configured."
                    )
            except FileNotFoundError:
                # Key not found, add it
                if config["key"] == "Password":
                    winreg.SetValueEx(
                        key, config["key"], 0, config["type"], config["set_value"]
                    )
                    logger.debug(
                        f"PL-TIGHT TightVNC Server registry key {config['key']} added."
                    )
                else:
                    logger.debug(
                        f"PL-TIGHT TightVNC Server registry key {config['key']} not found."
                    )
        if need_restart:
            try:
                # Restart the TightVNC Server service
                import subprocess

                subprocess.check_call(
                    "powershell Restart-Service -Name tvnserver", shell=True
                )
                logger.debug("PL-TIGHT TightVNCServer is reconfigured and restarted.")
            except subprocess.CalledProcessError:
                logger.debug(
                    "PL-TIGHT We failed to reinitialize the registry entry for TightVNCServer."
                )


def checktightvncversion():
    tightvncversion = "0.1"
    key = None

    if sys.platform.startswith("win"):
        try:
            # Open the registry key for TightVNC
            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{5AE9C1FB-F4F8-44A7-8550-F0592F56A1F2}",
                0,
                winreg.KEY_READ,
            )
            # Query the DisplayVersion value
            value, _ = winreg.QueryValueEx(key, "DisplayVersion")
            tightvncversion = value
        except FileNotFoundError:
            logger.debug(
                "PL-TIGHT TightVNCServer is not installed or not the corresponding version."
            )

    return tightvncversion


def updatetightvnc(xmppobject):
    logger.info(f"PL-TIGHT Updating TightVNC Agent to version {TIGHTVNC}")

    windows_tempdir = os.path.join("c:\\", "Windows", "Temp")
    install_tempdir = tempfile.mkdtemp(dir=windows_tempdir)

    Used_rfb_port = 5900
    if hasattr(xmppobject.config, "rfbport"):
        Used_rfb_port = xmppobject.config.rfbport

    if sys.platform.startswith("win"):
        if platform.architecture()[0] == "64bit":
            architecture = "64bit"
        else:
            architecture = "32bit"
        filename = "tightvnc-%s-gpl-setup-%s.msi" % (TIGHTVNC, architecture)
        dl_url = "http://%s/downloads/win/downloads/%s" % (
            xmppobject.config.Server,
            filename,
        )
        logger.debug(f"PL-TIGHT Downloading {dl_url}")
        result, txtmsg = utils.downloadfile(
            dl_url, os.path.join(install_tempdir, filename)
        ).downloadurl()
        if result:
            # Download success
            logger.info("PL-TIGHT {txtmsg}")
            current_dir = os.getcwd()
            os.chdir(install_tempdir)
            install_options = "/quiet /qn /norestart"
            install_options = (
                install_options
                + " ADDLOCAL=Server SERVER_REGISTER_AS_SERVICE=1 SERVER_ADD_FIREWALL_EXCEPTION=1 SERVER_ALLOW_SAS=1"
            )
            # Disable embedded Java WebSrv on port 5800
            install_options = (
                install_options
                + " SET_ACCEPTHTTPCONNECTIONS=1 VALUE_OF_ACCEPTHTTPCONNECTIONS=0"
            )
            # Enable RFB on port 5900
            install_options = (
                install_options
                + " SET_ACCEPTRFBCONNECTIONS=1 VALUE_OF_ACCEPTRFBCONNECTIONS=1"
            )
            # Enable loopback connection
            install_options = (
                install_options + " SET_ALLOWLOOPBACK=1 VALUE_OF_ALLOWLOOPBACK=0"
            )
            # Allow on all interfaces
            install_options = (
                install_options + " SET_LOOPBACKONLY=1 VALUE_OF_LOOPBACKONLY=0"
            )
            # Only allow from 127.0.0.1 and query user
            install_options = (
                install_options
                + " SET_IPACCESSCONTROL=1 VALUE_OF_IPACCESSCONTROL=0.0.0.0-255.255.255.255:2"
            )
            # Default answser on timeout is reject
            install_options = (
                install_options
                + " SET_QUERYACCEPTONTIMEOUT=1 VALUE_OF_QUERYACCEPTONTIMEOUT=0"
            )
            # Timeout is 20s
            install_options = (
                install_options + " SET_QUERYTIMEOUT=1 VALUE_OF_QUERYTIMEOUT=20"
            )
            # Show service icon
            install_options = (
                install_options
                + " SET_RUNCONTROLINTERFACE=1 VALUE_OF_RUNCONTROLINTERFACE=1"
            )
            # Hide wallpaper
            install_options = (
                install_options + " SET_REMOVEWALLPAPER=1 VALUE_OF_REMOVEWALLPAPER=1"
            )
            # Share between multiple connection
            install_options = (
                install_options
                + " SET_ALWASHARED=1 SET_NEVERSHARED=1 VALUE_OF_ALWASHARED=1 VALUE_OF_NEVERSHARED=0"
            )
            # Disable authentication
            install_options = (
                install_options
                + " SET_USEVNCAUTHENTICATION=1 VALUE_OF_USEVNCAUTHENTICATION=0"
            )
            # Ensure remote inputs are enabled
            install_options = (
                install_options + " SET_BLOCKREMOTEINPUT=1 VALUE_OF_BLOCKREMOTEINPUT=0"
            )
            # Don't do anything when terminating VNC session
            install_options = (
                install_options + " SET_DISCONNECTACTION=1 VALUE_OF_DISCONNECTACTION=0"
            )
            # Set the server listening port
            install_options = (
                install_options + " SET_RFBPORT=1 VALUE_OF_RFBPORT=%s" % Used_rfb_port
            )

            # Run installer
            cmd = "msiexec /i %s %s REBOOT=R" % (filename, install_options)
            cmd_result = utils.simplecommand(cmd)
            if cmd_result["code"] == 0:
                logger.info(
                    f"PL-TIGHT {filename} installed successfully to version {TIGHTVNC}"
                )
            else:
                logger.error(
                    f"PL-TIGHT Error installing {filename}: {cmd_result['result']}"
                )

            utils.simplecommand(
                'netsh advfirewall firewall add rule name="Remote Desktop for Pulse VNC" dir=in action=allow protocol=TCP localport=%s'
                % Used_rfb_port
            )
        else:
            # Download error
            logger.error(f"PL-TIGHT {txtmsg}")
