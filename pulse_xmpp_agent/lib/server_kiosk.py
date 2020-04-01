#!/usr/bin/env python
# -*- coding: utf-8; -*-
#
# (c) 2016-2017 siveo, http://www.siveo.net
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

# file : pulse_xmpp_agent/lib/server_kiosk.py

import sys
import os
import logging
import traceback
import sleekxmpp
import platform
import base64
import json
import time
import socket
import select
import threading
import shutil
import subprocess
import random
from multiprocessing import Process, Queue, Lock, current_process, TimeoutError
import psutil
import json
import threading
from utils import getRandomName, call_plugin, isBase64
from sleekxmpp import jid
from configuration import confParameter
from logcolor import  add_coloring_to_emit_ansi, add_coloring_to_emit_windows

from networkinfo import networkagentinfo,\
                        organizationbymachine,\
                        organizationbyuser



class process_tcp_serveur():
    def __init__(self,
                 port,
                 optstypemachine,
                 optsconsoledebug,
                 optsdeamon,
                 tglevellog,
                 tglogfile,
                 queue_recv_tcp_to_xmpp,
                 queueout,
                 eventkilltcp):

        if platform.system()=='Windows':
            # Windows does not support ANSI escapes and we are using API calls to set the console color
            logging.StreamHandler.emit = add_coloring_to_emit_windows(logging.StreamHandler.emit)
        else:
            # all non-Windows platforms are supporting ANSI escapes so we use them
            logging.StreamHandler.emit = add_coloring_to_emit_ansi(logging.StreamHandler.emit)
        # format log more informations
        format = '%(asctime)s - %(levelname)s - %(message)s'
        # more information log
        # format ='[%(name)s : %(funcName)s : %(lineno)d] - %(levelname)s - %(message)s'
        if not optsdeamon :
            if optsconsoledebug :
                logging.basicConfig(level = logging.DEBUG, format=format)
            else:
                logging.basicConfig( level = tglevellog,
                                     format = format,
                                     filename = tglogfile,
                                     filemode = 'a')
        else:
            logging.basicConfig( level = tglevellog,
                                 format = format,
                                 filename = tglogfile,
                                 filemode = 'a')
        self.logger = logging.getLogger()
        self.logger.debug("____________________________________________________________")
        self.logger.debug("_______________ INITIALISATION SERVER KIOSK ________________")
        self.logger.debug("____________________________________________________________")

        tg = confParameter(optstypemachine)

        #using event eventkill for signal stop thread
        self.eventkill = eventkilltcp
        #multiprocessing.Event
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.queue_recv_tcp_to_xmpp = queue_recv_tcp_to_xmpp
        self.queueout = queueout
        self.port = tg.am_local_port
        self.optstypemachine =  optstypemachine
        self.optsconsoledebug = optsconsoledebug
        self.optsdeamon = optsdeamon
        self.tglevellog = tglevellog
        self.tglogfile = tglogfile
        # Bind the socket to the port
        server_address = ('localhost',  self.port)
        for t in range(20):
            try:
                self.logger.info("Binding to kiosk server %s" % str(server_address))
                self.sock.bind(server_address)
                break
            except Exception as e:
                self.logger.error("bind adress %s"%str(e))
                time.sleep(40)
        # Listen for incoming connections
        self.sock.listen(5)
        self.logger.debug("_______________________________________________")
        self.logger.debug("_____________ START SERVER KIOSK ______________")
        self.logger.debug("_______________________________________________")
        while not self.eventkill.wait(1):
            self.logger.debug("SERVER KIOSK ON")
            try:
                rr, rw, err = select.select([self.sock],[],[self.sock], 5)
            except Exception as e:
                self.logger.error("kiosk server : %s" % str(e))
                #self.sock.shutdown(2)    # 0 = done receiving, 1 = done sending, 2 = both
                self.sock.close()
                # connection error event here, maybe reconnect
                self.logger.error('Quit connection kiosk')
                break
            except KeyboardInterrupt:
                self.logger.error("INTERRUPTED SERVER KIOSK CTRL+C")
                break
            if self.sock in rr:
                try:
                    clientsocket, client_address = self.sock.accept()
                except Exception as e:
                    break
                if client_address[0] == "127.0.0.1":
                    self.logger.debug("creation thread")
                    client_handler = threading.Thread( target=self.handle_client_connection,
                                                       args=(clientsocket,)).start()
                else:
                    self.logger.info("Connection refused from : %s" % client_address)
                    clientsocket.close()
            if self.sock in err:
                self.sock.close()
                self.logger.error('Quit connection kiosk')
                break;
        self.quitserverkiosk = True
        self.logger.debug("Stopping Kiosk")
        self.sock.close()

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
                msg = str(recv_msg_from_kiosk.decode("utf-8", 'ignore'))
                self.queue_recv_tcp_to_xmpp.put(msg)
        except Exception as e:
            self.logger.error("message to kiosk server : %s" % str(e))
            self.logger.error("\n%s"%(traceback.format_exc()))
        finally:
            client_socket.close()

class manage_kiosk_message:
    def __init__(self, queue_in, objectxmpp, key_quit="quit_server_kiosk"):
        self.logger = logging.getLogger()
        self.queue_in = queue_in
        self.objectxmpp = objectxmpp
        self.key_quit = key_quit
        self.threadevent = threading.Thread(name="thread_read_queue",
                                            target=self.manage_event_kiosk)
        self.running = True
        self.threadevent.start()

    def quit(self):
        self.queue_in.put(self.key_quit)
        self.running = False

    def send_message(self, msg):
        self.queue_in.put(msg)

    def manage_event_kiosk(self):
        self.logger.info('loop event wait start')
        while self.running:
            try:
                # lit event
                event = self.queue_in.get(5)
                self.logger.info('loop event wait start')
                if event == self.key_quit:
                    break
                self.handle_client_connection(str(event))
            except Queue.Empty:
                self.logger.debug("VIDE")
            except KeyboardInterrupt:
                pass
            finally:
                self.logger.info('loop event wait stop')

    def handle_client_connection(self, recv_msg_from_kiosk):
        try:
            print 'Received {}'.format(recv_msg_from_kiosk)
            datasend = { 'action' : "resultkiosk",
                        "sessionid" : getRandomName(6, "kioskGrub"),
                        "ret" : 0,
                        "base64" : False,
                        'data': {}}
            msg = str(recv_msg_from_kiosk.decode("utf-8", 'ignore'))
            ##############
            if isBase64(msg):
                msg = base64.b64decode(msg)
            try:
                result = json.loads(msg)
            except ValueError as e:
                self.logger.error('Message socket is not json correct : %s'%(str(e)))
                return
            if 'uuid' in result:
                datasend['data']['uuid'] = result['uuid']
            if 'utcdatetime' in result:
                datasend['data']['utcdatetime'] = result['utcdatetime']
            if 'action' in result:
                if result['action'] == "kioskinterface":
                    #start kiosk ask initialization
                    datasend['data']['subaction'] =  result['subaction']
                    datasend['data']['userlist'] = list(set([users[0]  for users in psutil.users()]))
                    datasend['data']['ouuser'] = organizationbyuser(datasend['data']['userlist'])
                    datasend['data']['oumachine'] = organizationbymachine()
                elif result['action'] == 'kioskinterfaceInstall':
                    datasend['data']['subaction'] =  'install'
                elif result['action'] == 'kioskinterfaceLaunch':
                    datasend['data']['subaction'] =  'launch'
                elif result['action'] == 'kioskinterfaceDelete':
                    datasend['data']['subaction'] =  'delete'
                elif result['action'] == 'kioskinterfaceUpdate':
                    datasend['data']['subaction'] =  'update'
                elif result['action'] == 'kioskLog':
                    self.logger.error("kkkkkkkk")
                    if 'message' in result and result['message'] != "":
                        self.objectxmpp.xmpplog(
                                    result['message'],
                                    type = 'noset',
                                    sessionname = '',
                                    priority = 0,
                                    action = "xmpplog",
                                    who = self.objectxmpp.boundjid.bare,
                                    how = "Planned",
                                    why = "",
                                    module = "Kiosk | Notify",
                                    fromuser = "",
                                    touser = "")
                        if 'type' in result:
                            if result['type'] == "info":
                                self.logger.getself.logger().info(result['message'])
                            elif result['type'] == "warning":
                                self.logger.warning(result['message'])
                elif result['action'] == "notifysyncthing":
                    datasend['action'] = "notifysyncthing"
                    datasend['sessionid'] = getRandomName(6, "syncthing")
                    datasend['data'] = result['data']
                else:
                    #bad action
                    self.logger.getLogger().warning("this action is not taken into account : %s"%result['action'])
                    return
                #call plugin on master
                self.objectxmpp.send_message_to_master(datasend)
        except Exception as e:
            self.logger.error("message to kiosk server : %s" % str(e))
            self.logger.error("\n%s"%(traceback.format_exc()))
