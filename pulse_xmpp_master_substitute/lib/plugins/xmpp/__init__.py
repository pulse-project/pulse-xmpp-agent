# -*- coding: utf-8; -*-
# SPDX-FileCopyrightText: 2018-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

"""
xmppmaster database handler
"""

# SqlAlchemy
from sqlalchemy import (
    create_engine,
    MetaData,
    select,
    func,
    and_,
    desc,
    or_,
    distinct,
    not_,
    delete,
)
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy.exc import DBAPIError, NoSuchTableError, IntegrityError
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql.expression import literal
from datetime import date, datetime, timedelta
import pprint

from sqlalchemy import Boolean
from sqlalchemy import TypeDecorator

# PULSE2 modules
from lib.plugins.xmpp.schema import (
    Network,
    Machines,
    RelayServer,
    Users,
    Regles,
    Has_machinesusers,
    Has_relayserverrules,
    Has_guacamole,
    Base,
    UserLog,
    Deploy,
    Has_login_command,
    Logs,
    ParametersDeploy,
    Organization,
    Packages_list,
    Qa_custom_command,
    Cluster_ars,
    Has_cluster_ars,
    Command_action,
    Command_qa,
    Organization_ad,
    Cluster_resources,
    Syncthing_machine,
    Substituteconf,
    Agentsubscription,
    Subscription,
    Syncthing_deploy_group,
    Syncthing_ars_cluster,
    Def_remote_deploy_status,
    Uptime_machine,
    Mon_machine,
    Mon_devices,
    Mon_device_service,
    Mon_rules,
    Mon_event,
    Mon_panels_template,
    Glpi_entity,
    Glpi_location,
    Glpi_Register_Keys,
    Up_machine_windows,
    Update_data,
    Up_black_list,
    Up_white_list,
    Up_gray_list,
    Up_action_update_packages,
    Up_history,
    Users_adgroups,
)

# Imported last
import logging
import json
import time
import copy

# topology
import os
import pwd
import traceback
import sys
import re
import uuid

import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# without this iqsendpulse can't work.

from lib.configuration import confParameter
from lib.utils import (
    getRandomName,
    simplecommandstr,
)
import subprocess
import functools
import base64
import zlib
from netaddr import *

try:
    from sqlalchemy.orm.util import _entity_descriptor
except ImportError:
    from sqlalchemy.orm.base import _entity_descriptor

from sqlalchemy.orm import scoped_session
import random

if sys.version_info >= (3, 0, 0):
    basestring = (str, bytes)


logger = logging.getLogger()


class Error(Exception):
    """Base class for exceptions in this module."""

    pass


class DomaineTypeDeviceError(Error):
    """
        type is not in domaine 'thermalprinter', 'nfcReader', 'opticalReader',\
        'cpu', 'memory', 'storage', 'network'
    """

    def __str__(self):
        return "{0} {1}".format(self.__doc__, Exception.__str__(self))


class DomainestatusDeviceError(Error):
    """
    status is not in domaine 'ready', 'busy', 'warning', 'error'
    """

    def __str__(self):
        return "{0} {1}".format(self.__doc__, Exception.__str__(self))


class Singleton(object):
    def __new__(type, *args):
        if "_the_instance" not in type.__dict__:
            type._the_instance = object.__new__(type)
        return type._the_instance


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            encoded_object = obj.isoformat()
        else:
            encoded_object = json.JSONEncoder.default(self, obj)
        return encoded_object


class LiberalBoolean(TypeDecorator):
    impl = Boolean

    def process_bind_param(self, value, dialect):
        if value is not None:
            if isinstance(value, tuple):
                value = value[0]
            if isinstance(value, bool):
                return value
            value = bool(int(value))
        return value


class DatabaseHelper(Singleton):
    # Session decorator to create and close session automatically
    @classmethod
    def _sessionxmpp(self, func):
        @functools.wraps(func)
        def __session(self, *args, **kw):
            created = False
            if not self.sessionxmpp:
                self.sessionxmpp = sessionmaker(bind=self.engine_xmppmmaster_base)
                created = True
            result = func(self, self.session, *args, **kw)
            if created:
                self.sessionxmpp.close()
                self.sessionxmpp = None
            return result

        return __session

    # Session decorator to create and close session automatically
    @classmethod
    def _sessionm(self, func):
        @functools.wraps(func)
        def __sessionm(self, *args, **kw):
            session_factory = sessionmaker(bind=self.engine_xmppmmaster_base)
            sessionmultithread = scoped_session(session_factory)
            result = func(self, sessionmultithread, *args, **kw)
            sessionmultithread.remove()
            return result

        return __sessionm


class XmppMasterDatabase(DatabaseHelper):
    """
    Singleton Class to query the xmppmaster database.
    """

    is_activated = False

    def activate(self):
        if self.is_activated:
            return None
        self.logger = logging.getLogger()
        self.logger.debug("Xmpp activation")
        self.engine = None
        self.sessionxmpp = None
        self.sessionglpi = None
        self.config = confParameter()
        # utilisation xmppmaster
        # dbpoolrecycle & dbpoolsize global conf
        # si sizepool et recycle  parametres sont definies pour xmpp, ils sont utilises
        try:
            self.config.xmpp_dbpoolrecycle
            self.poolrecycle = self.config.xmpp_dbpoolrecycle
        except Exception:
            self.poolrecycle = self.config.dbpoolrecycle

        try:
            self.config.xmpp_dbpoolsize
            self.poolsize = self.config.xmpp_dbpoolsize
        except Exception:
            self.poolsize = self.config.dbpoolsize
        self.logger.info(
            "Xmpp parameters connections is "
            " user = %s,host = %s, port = %s, schema = %s,"
            " poolrecycle = %s, poolsize = %s"
            % (
                self.config.xmpp_dbuser,
                self.config.xmpp_dbhost,
                self.config.xmpp_dbport,
                self.config.xmpp_dbname,
                self.poolrecycle,
                self.poolsize,
            )
        )
        try:
            echodata = False
            self.engine_xmppmmaster_base = create_engine(
                "mysql://%s:%s@%s:%s/%s"
                % (
                    self.config.xmpp_dbuser,
                    self.config.xmpp_dbpasswd,
                    self.config.xmpp_dbhost,
                    self.config.xmpp_dbport,
                    self.config.xmpp_dbname,
                ),
                pool_recycle=self.poolrecycle,
                pool_size=self.poolsize,
                echo=echodata,
                convert_unicode=True,
            )
            self.Sessionxmpp = sessionmaker(bind=self.engine_xmppmmaster_base)

            Base = automap_base()
            Base.prepare(self.engine_xmppmmaster_base, reflect=True)

            # Only federated tables (beginning by local_) are automatically mapped
            # If needed, excludes tables from this list
            exclude_table = []
            # Dynamically add attributes to the object for each mapped class
            for table_name, mapped_class in Base.classes.items():
                if table_name in exclude_table:
                    continue
                if table_name.startswith("local"):
                    setattr(self, table_name.capitalize(), mapped_class)

            self.is_activated = True
            self.logger.debug("Xmpp activation done.")
            return True
        except Exception as e:
            self.logger.error("We failed to connect to the Xmpp database.")
            self.logger.error("Please verify your configuration")
            self.is_activated = False
            return False

    @DatabaseHelper._sessionm
    def setagentsubscription(self, session, name):
        """
        this functions addition a log line in table log xmpp.
        """
        try:
            new_agentsubscription = Agentsubscription()
            new_agentsubscription.name = name
            session.add(new_agentsubscription)
            session.commit()
            session.flush()
            return new_agentsubscription.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return None

    @DatabaseHelper._sessionm
    def deAgentsubscription(self, session, name):
        """
        del organization name
        """
        session.query(Agentsubscription).filter(Agentsubscription.name == name).delete()
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def setupagentsubscription(self, session, name):
        """
        this functions addition ou update table in table log xmpp.
        """
        try:
            q = session.query(Agentsubscription)
            q = q.filter(Agentsubscription.name == name)
            record = q.first()
            if record:
                record.name = name
                session.commit()
                session.flush()
                return record.id
            else:
                return self.setagentsubscription(name)
        except Exception as e:
            logging.getLogger().error(str(e))
            return None

    @DatabaseHelper._sessionm
    def setSubscription(self, session, macadress, idagentsubscription):
        """
        this functions addition a log line in table log xmpp.
        """
        try:
            new_subscription = Subscription()
            new_subscription.macadress = macadress
            new_subscription.idagentsubscription = idagentsubscription
            session.add(new_subscription)
            session.commit()
            session.flush()
            return new_subscription.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return None

    @DatabaseHelper._sessionm
    def setupSubscription(self, session, macadress, idagentsubscription):
        """
        this functions addition a log line in table log xmpp.
        """
        try:
            q = session.query(Subscription)
            q = q.filter(Subscription.macadress == macadress)
            record = q.first()
            if record:
                record.macadress = macadress
                record.idagentsubscription = idagentsubscription
                session.commit()
                session.flush()
                return record.id
            else:
                return self.setSubscription(macadress, idagentsubscription)
        except Exception as e:
            logging.getLogger().error(str(e))
            return None

    @DatabaseHelper._sessionm
    def setuplistSubscription(self, session, listmacadress, agentsubscription):
        try:
            id = self.setupagentsubscription(agentsubscription)
            if id is not None:
                for macadress in listmacadress:
                    self.setupSubscription(macadress, id)
                return id
            else:
                logging.getLogger().error(
                    "setup or create record for agent subscription%s"
                    % agentsubscription
                )
                return None
        except Exception as e:
            logging.getLogger().error(str(e))
            return None

    @DatabaseHelper._sessionm
    def delSubscriptionmacadress(self, session, macadress):
        """
        this functions addition a log line in table log xmpp.
        """
        try:
            q = session.query(Subscription)
            q = q.filter(Subscription.macadress == macadress).delete()
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def update_count_subscription(self, session, agentsubtitutename, countroster):
        logging.getLogger().debug("update_count_subscription %s" % agentsubtitutename)
        try:
            result = (
                session.query(Substituteconf)
                .filter(Substituteconf.jidsubtitute == agentsubtitutename)
                .all()
            )
            first_value = True
            for t in result:
                logging.getLogger().debug(
                    "The ARS id: %s contains %s machines on the substitute %s"
                    % (t.relayserver_id, t.countsub, t.jidsubtitute)
                )

                if first_value:
                    first_value = False
                    t.countsub = countroster
                else:
                    t.countsub = 0
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error(
                "An error occured on update_count_subscription function."
            )
            logging.getLogger().error("We obtained the error: \n %s" % str(e))
            return False

    @DatabaseHelper._sessionm
    def update_enable_for_agent_subscription(
        self, session, agentsubtitutename, status="0", agenttype="machine"
    ):
        try:
            sql = """
            UPDATE `xmppmaster`.`machines`
                    INNER JOIN
                `xmppmaster`.`subscription` ON `xmppmaster`.`machines`.`macaddress` = `xmppmaster`.`subscription`.`macadress`
                    INNER JOIN
                `xmppmaster`.`agent_subscription` ON `xmppmaster`.`subscription`.`idagentsubscription` = `xmppmaster`.`agent_subscription`.`id`
            SET
                `xmppmaster`.`machines`.`enabled` = '%s'
            WHERE
                `xmppmaster`.`machines`.agenttype = '%s'
                    AND `xmppmaster`.`agent_subscription`.`name` = '%s';""" % (
                status,
                agenttype,
                agentsubtitutename,
            )
            machines = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            self.logger.error("\n%s" % (traceback.format_exc()))

    @DatabaseHelper._sessionm
    def setlogxmpp(
        self,
        session,
        text,
        type="noset",
        sessionname="",
        priority=0,
        who="",
        how="",
        why="",
        module="",
        action="",
        touser="",
        fromuser="",
    ):
        """
        this functions addition a log line in table log xmpp.
        """
        try:
            new_log = Logs()
            new_log.text = text
            new_log.type = type
            new_log.sessionname = sessionname
            new_log.priority = priority
            new_log.who = who
            new_log.how = how
            new_log.why = why
            new_log.module = module
            new_log.action = action
            new_log.touser = touser
            new_log.fromuser = fromuser
            session.add(new_log)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def search_machines_from_state(self, session, state):
        dateend = datetime.now()
        sql = """SELECT
                    *
                 FROM
                    xmppmaster.deploy
                 WHERE
                    state LIKE '%s%%' AND
                    '%s' BETWEEN startcmd AND
                    endcmd;""" % (
            state,
            dateend,
        )
        machines = session.execute(sql)
        session.commit()
        session.flush()
        result = [x for x in machines]
        resultlist = []
        for t in result:
            listresult = {
                "id": t[0],
                "title": t[1],
                "jidmachine": t[2],
                "jid_relay": t[3],
                "pathpackage": t[4],
                "state": t[5],
                "sessionid": t[6],
                "start": str(t[7]),
                "startcmd": str(t[8]),
                "endcmd": str(t[9]),
                "inventoryuuid": t[10],
                "host": t[11],
                "user": t[12],
                "command": t[13],
                "group_uuid": t[14],
                "login": t[15],
                "macadress": t[16],
                "syncthing": t[17],
                "result": t[18],
            }
            resultlist.append(listresult)
        return resultlist

    @DatabaseHelper._sessionm
    def Timeouterrordeploy(self, session):
        # test les evenements states qui ne sont plus valides sur intervalle de
        # deployement.
        Stateforupdateontimeout = [
            "'WOL 1'",
            "'WOL 2'",
            "'WOL 3'",
            "'WAITING MACHINE ONLINE'",
            "'DEPLOYMENT START'",
            "'WAITING REBOOT'",
            "'DEPLOYMENT PENDING (REBOOT/SHUTDOWN/...)'",
            "'Offline'",
        ]
        nowdate = datetime.now()
        set_search = ",".join(Stateforupdateontimeout)

        # reprise code ici
        try:
            sql = """SELECT
                         *
                     FROM
                         xmppmaster.deploy
                     WHERE
                         state in (%s) AND
                         '%s' > endcmd;""" % (
                set_search,
                nowdate,
            )
            machines = session.execute(sql)
            session.commit()
            session.flush()
            result = [x for x in machines]
            resultlist = []
            for t in result:
                self.update_state_deploy(t[0], "ABORT ON TIMEOUT")
                listresult = {
                    "id": t[0],
                    "title": t[1],
                    "jidmachine": t[2],
                    "jid_relay": t[3],
                    "pathpackage": t[4],
                    "state": t[5],
                    "sessionid": t[6],
                    "start": str(t[7]),
                    "startcmd": str(t[8]),
                    "endcmd": str(t[9]),
                    "inventoryuuid": t[10],
                    "host": t[11],
                    "user": t[12],
                    "command": t[13],
                    "group_uuid": t[14],
                    "login": t[15],
                    "macadress": t[16],
                    "syncthing": t[17],
                    "result": t[18],
                }
                resultlist.append(listresult)
            return resultlist
        except Exception as e:
            logging.getLogger().error(str(e))
            logging.getLogger().error("fn Timeouterrordeploy on sql %s" % sql)

            return resultlist

    @DatabaseHelper._sessionm
    def update_state_deploy(self, session, sql_id, state):
        """
        Reset the state of the deploiement to `state` for the
        `sql_id` deploiements
        Args:
            session: The SQL Alchemy session
            sql_id: The id of the deploiement that need to be reset
            state: The new state of the deploiement
        """
        try:
            sql = """UPDATE `xmppmaster`.`deploy`
                     SET `state`='%s'
                     WHERE `id`='%s';""" % (
                state,
                sql_id,
            )
            session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    def replaydeploysessionid(self, sessionid, force_redeploy=0, reschedule=0):
        """
        Call the mmc_restart_deploy_sessionid stored procedure
        Args:
            session: The SQL Alchemy session
            sessionid: The sessionid of the deploiement
            force_redeploy: Tells if we force to redeploy ALL.
            reschedule: Tell if we reschedule the deploiements
        """

        connection = self.engine_xmppmmaster_base.raw_connection()
        try:
            self.logger.info(
                "Call the mmc_restart_deploy_sessionid stored procedure for the sessionid: %s"
                "force_redeploy is set to %s and reschedule is set to %s"
                % (sessionid, force_redeploy, reschedule)
            )
            cursor = connection.cursor()
            cursor.callproc(
                "mmc_restart_deploy_sessionid", [sessionid, force_redeploy, reschedule]
            )
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()

    def restart_blocked_deployments(self, nb_reload=50):
        """
        Call the mmc_restart_blocked_deployments stored procedure
        It plans with blocked deployments again
        """
        self.restart_blocked_deployments_on_status_transfer_failed(nb_reload)
        connection = self.engine_xmppmmaster_base.raw_connection()
        results = None
        try:
            cursor = connection.cursor()
            cursor.callproc("mmc_restart_blocked_deployments", [nb_reload])
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()

        if results:
            results = "%s" % results[0]
            self.logger.info(
                "Calling the mmc_restart_deploy_sessionid stored procedure with %s"
                % nb_reload
            )
            self.logger.info("Restarting %s deployements" % results)
        return results

    def restart_blocked_deployments_on_status_transfer_failed(self, nb_reload=50):
        """
        Call the mmc_restart_blocked_deployments_transfer_error stored procedure
        It plans with transfert failed blocked deployments again
        """
        connection = self.engine_xmppmmaster_base.raw_connection()
        results = None
        try:
            cursor = connection.cursor()
            cursor.callproc(
                "mmc_restart_blocked_deployments_transfer_error", [nb_reload]
            )
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()
        if results:
            results = "%s" % results[0]
            self.logger.info(
                "Calling the mmc_restart_blocked_deployments_transfer_error stored procedure with %s"
                % nb_reload
            )
            self.logger.info("Restarting %s deployements" % results)
        return results

    @DatabaseHelper._sessionm
    def updatedeploytosessionid(self, session, status, sessionid):
        try:
            sql = """UPDATE `xmppmaster`.`deploy`
                     SET `state`='%s'
                     WHERE `sessionid`='%s';""" % (
                status,
                sessionid,
            )
            session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def updatedeploytosyncthing(self, session, sessionid, syncthing=1):
        try:
            sql = """UPDATE `xmppmaster`.`deploy`
                     SET `syncthing`='%s'
                     WHERE `sessionid`='%s';""" % (
                syncthing,
                sessionid,
            )
            print(sql)
            session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def nbsyncthingdeploy(self, session, grp, cmd):
        try:
            sql = """SELECT
                        COUNT(*) as nb
                    FROM
                        deploy
                    WHERE
                        group_uuid = %s AND command = %s
                            AND syncthing > 1;""" % (
                grp,
                cmd,
            )
            req = session.execute(sql)
            session.commit()
            session.flush()
            ret = [elt for elt in req]
            return ret[0][0]
        except Exception as e:
            logging.getLogger().error(str(e))
            return 0

    @DatabaseHelper._sessionm
    def getQAforMachine(self, session, cmd_id, uuidmachine):
        try:
            command_action = session.query(Command_action).filter(
                and_(
                    Command_action.command_id == cmd_id,
                    Command_action.target == uuidmachine,
                )
            )
            print(command_action)
            print(cmd_id)
            print(uuidmachine)
            command_action = command_action.all()
            listcommand = []
            for command in command_action:
                action = []
                action.append(command.command_id)
                action.append(str(command.date))
                action.append(command.session_id)
                action.append(command.typemessage)
                action.append(command.command_result)
                listcommand.append(action)
            return listcommand
        except Exception as e:
            logging.getLogger().error(str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return []

    @DatabaseHelper._sessionm
    def getCommand_action_time(
        self, session, during_the_last_seconds, start, stop, filter=None
    ):
        try:
            command_qa = session.query(
                distinct(Command_qa.id).label("id"),
                Command_qa.command_name.label("command_name"),
                Command_qa.command_login.label("command_login"),
                Command_qa.command_os.label("command_os"),
                Command_qa.command_start.label("command_start"),
                Command_qa.command_grp.label("command_grp"),
                Command_qa.command_machine.label("command_machine"),
                Command_action.target.label("target"),
            ).join(Command_action, Command_qa.id == Command_action.command_id)
            # si on veut passer par les groupe avant d'aller sur les machine.
            # command_qa = command_qa.group_by(Command_qa.id)
            command_qa = command_qa.order_by(desc(Command_qa.id))
            if during_the_last_seconds:
                command_qa = command_qa.filter(
                    Command_qa.command_start
                    >= (datetime.now() - timedelta(seconds=during_the_last_seconds))
                )

            nbtotal = self.get_count(command_qa)
            if start != "" and stop != "":
                command_qa = command_qa.offset(int(start)).limit(int(stop) - int(start))
            command_qa = command_qa.all()
            session.commit()
            session.flush()
            # creation des list pour affichage web organiser par colone
            result_list = []
            command_id_list = []
            command_name_list = []
            command_login_list = []
            command_os_list = []
            command_start_list = []
            command_grp_list = []
            command_machine_list = []
            command_target_list = []
            for command in command_qa:
                command_id_list.append(command.id)
                command_name_list.append(command.command_name)
                command_login_list.append(command.command_login)
                command_os_list.append(command.command_os)
                command_start_list.append(command.command_start)
                command_grp_list.append(command.command_grp)
                command_machine_list.append(command.command_machine)
                command_target_list.append(command.target)
            result_list.append(command_id_list)
            result_list.append(command_name_list)
            result_list.append(command_login_list)
            result_list.append(command_os_list)
            result_list.append(command_start_list)
            result_list.append(command_grp_list)
            result_list.append(command_machine_list)
            result_list.append(command_target_list)
            return {"nbtotal": nbtotal, "result": result_list}
        except Exception as e:
            logging.getLogger().debug("getCommand_action_time error %s->" % str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return {"nbtotal": 0, "result": result_list}

    @DatabaseHelper._sessionm
    def setCommand_qa(
        self,
        session,
        command_name,
        command_action,
        command_login,
        command_grp="",
        command_machine="",
        command_os="",
    ):
        try:
            new_Command_qa = Command_qa()
            new_Command_qa.command_name = command_name
            new_Command_qa.command_action = command_action
            new_Command_qa.command_login = command_login
            new_Command_qa.command_grp = command_grp
            new_Command_qa.command_machine = command_machine
            new_Command_qa.command_os = command_os
            session.add(new_Command_qa)
            session.commit()
            session.flush()
            return new_Command_qa.id
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def getCommand_qa_by_cmdid(self, session, cmdid):
        try:
            command_qa = session.query(Command_qa).filter(Command_qa.id == cmdid)
            command_qa = command_qa.first()
            session.commit()
            session.flush()
            return {
                "id": command_qa.id,
                "command_name": command_qa.command_name,
                "command_action": command_qa.command_action,
                "command_login": command_qa.command_login,
                "command_os": command_qa.command_os,
                "command_start": str(command_qa.command_start),
                "command_grp": command_qa.command_grp,
                "command_machine": command_qa.command_machine,
            }
        except Exception as e:
            logging.getLogger().error("getCommand_qa_by_cmdid error %s->" % str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return {
                "id": "",
                "command_name": "",
                "command_action": "",
                "command_login": "",
                "command_os": "",
                "command_start": "",
                "command_grp": "",
                "command_machine": "",
            }

    @DatabaseHelper._sessionm
    def setCommand_action(
        self,
        session,
        target,
        command_id,
        sessionid,
        command_result="",
        typemessage="log",
    ):
        try:
            new_Command_action = Command_action()
            new_Command_action.session_id = sessionid
            new_Command_action.command_id = command_id
            new_Command_action.typemessage = typemessage
            new_Command_action.command_result = command_result
            new_Command_action.target = target
            session.add(new_Command_action)
            session.commit()
            session.flush()
            return new_Command_action.id
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def updateaddCommand_action(
        self, session, command_result, sessionid, typemessage="result"
    ):
        try:
            sql = """UPDATE `xmppmaster`.`command_action`
                    SET
                        `typemessage` = '%s',
                        `command_result` = CONCAT(`command_result`, ' ', '%s')
                    WHERE
                        (`session_id` = '%s');""" % (
                typemessage,
                command_result,
                sessionid,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error(str(e))
            return False

    @DatabaseHelper._sessionm
    def logtext(self, session, text, sessionname="", type="noset", priority=0, who=""):
        try:
            new_log = Logs()
            new_log.text = text
            new_log.sessionname = sessionname
            new_log.type = type
            new_log.priority = priority
            new_log.who = who
            session.add(new_log)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def log(self, session, msg, type="info"):
        try:
            new_log = UserLog()
            new_log.msg = msg
            new_log.type = type
            session.add(new_log)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    #
    @DatabaseHelper._sessionm
    def getlistpackagefromorganization(
        self, session, organization_name=None, organization_id=None
    ):
        """
        return list package an organization
        eg call function example:
        XmppMasterDatabase().getlistpackagefromorganization( organization_id = 1)
        or
        XmppMasterDatabase().getlistpackagefromorganization( organization_name = "name")
        """
        # recupere id organization
        idorganization = -1
        try:
            if organization_id is not None:
                try:
                    result_organization = session.query(Organization).filter(
                        Organization.id == organization_id
                    )
                    result_organization = result_organization.one()
                    session.commit()
                    session.flush()
                    idorganization = result_organization.id

                except Exception as e:
                    logging.getLogger().debug(
                        "organization id : %s is not exist" % organization_id
                    )
                    return -1
            elif organization_name is not None:
                idorganization = self.getIdOrganization(organization_name)
                if idorganization == -1:
                    return {"nb": 0, "packageslist": []}
            else:
                return {"nb": 0, "packageslist": []}
            result = (
                session.query(
                    Packages_list.id.label("id"),
                    Packages_list.packageuuid.label("packageuuid"),
                    Packages_list.organization_id.label("idorganization"),
                    Organization.name.label("name"),
                )
                .join(Organization, Packages_list.organization_id == Organization.id)
                .filter(Organization.id == idorganization)
            )
            nb = self.get_count(result)
            result = result.all()

            list_result = [
                {
                    "id": x.id,
                    "packageuuid": x.packageuuid,
                    "idorganization": x.idorganization,
                    "name": x.name,
                }
                for x in result
            ]
            return {"nb": nb, "packageslist": list_result}
        except Exception as e:
            logging.getLogger().debug(
                "load packages for organization id : %s is error : %s"
                % (organization_id, str(e))
            )
            return {"nb": 0, "packageslist": []}

    @DatabaseHelper._sessionm
    def getIdOrganization(self, session, name_organization):
        """
        return id organization suivant le Name
        On error return -1
        """
        try:
            result_organization = session.query(Organization).filter(
                Organization.name == name_organization
            )
            result_organization = result_organization.one()
            session.commit()
            session.flush()
            return result_organization.id
        except Exception as e:
            logging.getLogger().error(str(e))
            logging.getLogger().debug(
                "organization name : %s is not exist" % name_organization
            )
            return -1

    @DatabaseHelper._sessionm
    def addOrganization(self, session, name_organization):
        """
        creation d'une organization
        """
        id = self.getIdOrganization(name_organization)
        if id == -1:
            organization = Organization()
            organization.name = name_organization
            session.add(organization)
            session.commit()
            session.flush()
            return organization.id
        else:
            return id

    @DatabaseHelper._sessionm
    def delOrganization(self, session, name_organization):
        """
        del organization name
        """
        idorganization = self.getIdOrganization(name_organization)
        if idorganization != -1:
            session.query(Organization).filter(
                Organization.name == name_organization
            ).delete()
            session.commit()
            session.flush()
            q = session.query(Packages_list).filter(
                Packages_list.organization_id == idorganization
            )
            q.delete()
            session.commit()
            session.flush()

    # Custom Command Quick Action
    @DatabaseHelper._sessionm
    def create_Qa_custom_command(
        self, session, user, osname, namecmd, customcmd, description=""
    ):
        """
        create Qa_custom_command
        """
        try:
            qa_custom_command = Qa_custom_command()
            qa_custom_command.namecmd = namecmd
            qa_custom_command.user = user
            qa_custom_command.os = osname
            qa_custom_command.customcmd = customcmd
            qa_custom_command.description = description
            session.add(qa_custom_command)
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().error(str(e))
            logging.getLogger().debug("qa_custom_command error")
            return -1

    @DatabaseHelper._sessionm
    def update_Glpi_entity(self, session, glpi_id, complete_name=None, name=None):
        try:
            result_entity = (
                session.query(Glpi_entity)
                .filter(Glpi_entity.glpi_id == glpi_id)
                .first()
            )
            if result_entity:
                if complete_name is not None:
                    result_entity.complete_name = complete_name
                if name is not None:
                    result_entity.name = name
                session.commit()
                session.flush()
                return result_entity.get_data()
            else:
                logging.getLogger().debug("id entity no exist for update")
        except NoResultFound:
            logging.getLogger().debug("id entity %s no exist for update " % glpi_id)
        except Exception:
            logging.getLogger().error("update Glpi_entity ")
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def update_Glpi_location(self, session, glpi_id, complete_name=None, name=None):
        try:
            result_location = (
                session.query(Glpi_location)
                .filter(Glpi_location.glpi_id == glpi_id)
                .first()
            )
            if result_location:
                if complete_name is not None:
                    result_location.complete_name = complete_name
                if name is not None:
                    result_location.name = name
                session.commit()
                session.flush()
                return result_location.get_data()
            else:
                logging.getLogger().debug("id location no exist for update")
        except NoResultFound:
            logging.getLogger().debug("id location %s no exist for update " % glpi_id)
        except Exception:
            logging.getLogger().error("update Glpi_location ")
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def update_Glpi_register_key(self, session, machines_id, name, value, comment=""):
        try:
            if name is not None and name != "":
                result_register_key = (
                    session.query(Glpi_Register_Keys)
                    .filter(
                        or_(
                            Glpi_Register_Keys.machines_id == machines_id,
                            Glpi_Register_Keys.name == name,
                        )
                    )
                    .one()
                )
                session.commit()
                session.flush()
                if result_register_key:
                    return result_register_key.get_data()
                else:
                    logging.getLogger().debug("id registration no exist for update")
        except NoResultFound:
            logging.getLogger().error(
                "update Glpi_Register_Keys  : %s for machine %s is not exist"
                % (name, machines_id)
            )
        except Exception:
            logging.getLogger().error(
                "update Glpi_Register_Keys  : %s for machine %s is not exist"
                % (name, machines_id)
            )
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def get_Glpi_entity(self, session, glpi_id):
        """
        get Glpi_entity by glpi id machine
        """
        try:
            result_entity = (
                session.query(Glpi_entity)
                .filter(Glpi_entity.glpi_id == glpi_id)
                .first()
            )
            session.commit()
            session.flush()
            if result_entity:
                return result_entity.get_data()
            else:
                logging.getLogger().debug("Glpi_entity id : %s is not exist" % glpi_id)
        except NoResultFound:
            logging.getLogger().debug("Glpi_entity id : %s is not exist" % glpi_id)
        except Exception as e:
            logging.getLogger().error("Glpi_entity id : %s is not exist" % glpi_id)
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def get_Glpi_location(self, session, glpi_id):
        """
        get Glpi_location by glpi id machine
        """
        try:
            result_location = (
                session.query(Glpi_location)
                .filter(Glpi_location.glpi_id == glpi_id)
                .first()
            )
            session.commit()
            session.flush()
            if result_location:
                return result_location.get_data()
            else:
                logging.getLogger().debug(
                    "Glpi_location id : %s is not exist" % glpi_id
                )
        except NoResultFound:
            logging.getLogger().debug("Glpi_location id : %s is not exist" % glpi_id)
        except Exception as e:
            logging.getLogger().error("Glpi_location id : %s is not exist" % glpi_id)
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def get_Glpi_register_key(self, session, machines_id, name):
        """
        get Glpi_register_key by glpi id machine and name key reg
        """
        try:
            result_register_key = (
                session.query(Glpi_Register_Keys)
                .filter(
                    and_(
                        Glpi_Register_Keys.machines_id == machines_id,
                        Glpi_Register_Keys.name == name,
                    )
                )
                .one()
            )
            result_register_key = result_register_key
            session.commit()
            session.flush()
            if result_register_key:
                return result_register_key.get_data()
            else:
                logging.getLogger().debug(
                    "Glpi_Register_Keys  : %s"
                    " for machine %s is not exist" % (name, machines_id)
                )
        except NoResultFound:
            logging.getLogger().debug(
                "Glpi_Register_Keys  : %s "
                "for machine %s is not exist" % (name, machines_id)
            )
        except Exception as e:
            logging.getLogger().error(
                "Glpi_Register_Keys  : %s "
                "for machine %s is not exist(%s)" % (name, machines_id, str(e))
            )
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return None

    @DatabaseHelper._sessionm
    def create_Glpi_entity(self, session, complete_name, name, glpi_id):
        """
        create Glpi_entity
        """
        if glpi_id is None or glpi_id == "":
            logging.getLogger().warning("create_Glpi_entity glpi_id missing")
            return None
        ret = self.get_Glpi_entity(glpi_id)
        if ret is None:
            # creation de cette entity
            try:
                # creation si cette entite n'existe pas.
                new_glpi_entity = Glpi_entity()
                new_glpi_entity.complete_name = complete_name
                new_glpi_entity.name = name
                new_glpi_entity.glpi_id = glpi_id
                session.add(new_glpi_entity)
                session.commit()
                session.flush()
                return new_glpi_entity.get_data()
            except Exception as e:
                logging.getLogger().error(str(e))
                logging.getLogger().error("glpi_entity error")
        else:
            # verify coherence
            if ret["name"] == name and ret["complete_name"] == complete_name:
                return ret
            else:
                # update entity
                logging.getLogger().warning("update entity exist")
                return self.update_Glpi_entity(glpi_id, complete_name, name)
        return None

    @DatabaseHelper._sessionm
    def create_Glpi_location(self, session, complete_name, name, glpi_id):
        """
        create Glpi_location
        """
        if glpi_id is None or glpi_id == "":
            logging.getLogger().warning("create_Glpi_location glpi_id missing")
            return None
        ret = self.get_Glpi_location(glpi_id)
        if ret is None:
            # creation de cette location
            try:
                # creation si cette entite n'existe pas.
                new_glpi_location = Glpi_location()
                new_glpi_location.complete_name = complete_name
                new_glpi_location.name = name
                new_glpi_location.glpi_id = glpi_id
                session.add(new_glpi_location)
                session.commit()
                session.flush()
                return new_glpi_location.get_data()
            except Exception as e:
                logging.getLogger().error(str(e))
                logging.getLogger().error("create_Glpi_location error")
        else:
            # verify coherence
            if ret["name"] == name and ret["complete_name"] == complete_name:
                return ret
            else:
                # update location
                logging.getLogger().warning("update location exist")
                return self.update_Glpi_location(glpi_id, complete_name, name)
        return None

    @DatabaseHelper._sessionm
    def create_Glpi_register_keys(
        self, session, machines_id, name, value=0, comment=""
    ):
        """
        create Glpi_Register_Keys
        """

        if machines_id is None or machines_id == "" or name is None or name == "":
            return None
        ret = self.get_Glpi_register_key(machines_id, name)
        if ret is None:
            # creation de cette register_keys
            try:
                # creation si cette entite n'existe pas.
                new_glpi_register_keys = Glpi_Register_Keys()
                new_glpi_register_keys.name = name
                new_glpi_register_keys.value = value
                new_glpi_register_keys.machines_id = machines_id
                new_glpi_register_keys.comment = comment
                session.add(new_glpi_register_keys)
                session.commit()
                session.flush()
                return new_glpi_register_keys.get_data()
            except Exception as e:
                logging.getLogger().error(str(e))
                logging.getLogger().error("Glpi_register_keys error")
        else:
            # verify coherence
            if ret["name"] == name and ret["value"] == value:
                return ret
            else:
                # update register_keys
                logging.getLogger().warning("update register_keys exist")
                return self.update_Glpi_register_key(machines_id, name, value, comment)
        return None

    @DatabaseHelper._sessionm
    def updateMachineGlpiInformationInventory(
        self, session, glpiinformation, idmachine, data
    ):
        retentity = self.create_Glpi_entity(
            glpiinformation["data"]["complete_entity"][0],
            glpiinformation["data"]["entity"][0],
            glpiinformation["data"]["entity_glpi_id"][0],
        )
        if retentity is None:
            entity_id_xmpp = "NULL"
        else:
            entity_id_xmpp = retentity["id"]

        retlocation = self.create_Glpi_location(
            glpiinformation["data"]["complete_location"][0],
            glpiinformation["data"]["location"][0],
            glpiinformation["data"]["location_glpi_id"][0],
        )
        if retlocation is None:
            location_id_xmpp = "NULL"
        else:
            location_id_xmpp = retlocation["id"]
        if (
            "win" in data["information"]["info"]["platform"].lower()
            and "reg" in glpiinformation["data"]
        ):
            for regwindokey in glpiinformation["data"]["reg"]:
                if glpiinformation["data"]["reg"][regwindokey][0] is not None:
                    self.create_Glpi_register_keys(
                        idmachine,
                        regwindokey,
                        value=glpiinformation["data"]["reg"][regwindokey][0],
                    )
        return self.updateGLPI_information_machine(
            idmachine,
            "UUID%s" % glpiinformation["data"]["uuidglpicomputer"][0],
            glpiinformation["data"]["description"][0],
            glpiinformation["data"]["owner_firstname"][0],
            glpiinformation["data"]["owner_realname"][0],
            glpiinformation["data"]["owner"][0],
            glpiinformation["data"]["model"][0],
            glpiinformation["data"]["manufacturer"][0],
            entity_id_xmpp,
            location_id_xmpp,
        )

    @DatabaseHelper._sessionm
    def updateGLPI_information_machine(
        self,
        session,
        id,
        uuid_inventory,
        description_machine,
        owner_firstname,
        owner_realname,
        owner,
        model,
        manufacturer,
        entity_id_xmpp,
        location_id_xmpp,
    ):
        """
        update table Machine with informations obtained from GLPI
        """

        try:
            entity_id_xmpp = None if entity_id_xmpp in ["NULL", ""] else entity_id_xmpp
            location_id_xmpp = (
                None if location_id_xmpp in ["NULL", ""] else location_id_xmpp
            )
            obj = {
                Machines.uuid_inventorymachine: uuid_inventory,
                Machines.glpi_description: description_machine,
                Machines.glpi_owner_firstname: owner_firstname,
                Machines.glpi_owner_realname: owner_realname,
                Machines.glpi_owner: owner,
                Machines.model: model,
                Machines.manufacturer: manufacturer,
                Machines.glpi_entity_id: entity_id_xmpp,
                Machines.glpi_location_id: location_id_xmpp,
            }
            session.query(Machines).filter(Machines.id == id).update(obj)
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().debug("updateMachines error %s->" % str(e))
            return -1

    @DatabaseHelper._sessionm
    def updateName_Qa_custom_command(
        self, session, user, osname, namecmd, customcmd, description
    ):
        """
        update updateName_Qa_custom_command
        """

        try:
            session.query(Qa_custom_command).filter(
                Qa_custom_command.namecmd == namecmd
            ).update(
                {
                    Qa_custom_command.customcmd: customcmd,
                    Qa_custom_command.description: description,
                    Qa_custom_command.os: osname,
                }
            )
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().debug(
                "updateName_Qa_custom_command error %s->" % str(e)
            )
            return -1

    @DatabaseHelper._sessionm
    def delQa_custom_command(self, session, user, osname, namecmd):
        """
        del Qa_custom_command
        """
        try:
            session.query(Qa_custom_command).filter(
                and_(
                    Qa_custom_command.user == user,
                    Qa_custom_command.os == osname,
                    Qa_custom_command.namecmd == namecmd,
                )
            ).delete()
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().debug("delQa_custom_command error %s ->" % str(e))
            return -1

    @DatabaseHelper._sessionm
    def get_list_of_users_for_shared_qa(self, session, namecmd):
        """
        Return the list of users who are owning the specified QA.
        Param:
            str: namecmd the name of the quickaction
        Returns :
            list of users
        """

        query = session.query(Qa_custom_command.user).filter(
            Qa_custom_command.namecmd == namecmd
        )

        if query is not None:
            user_list = [user[0] for user in query]
            return user_list
        else:
            return []

    @DatabaseHelper._sessionm
    def getlistcommandforuserbyos(
        self, session, user, osname=None, min=None, max=None, filt=None, edit=None
    ):
        ret = {
            "len": 0,
            "nb": 0,
            "limit": 0,
            "max": 0,
            "min": 0,
            "filt": "",
            "command": [],
        }
        try:
            if edit is not None:
                # We are in the edition view
                result = session.query(Qa_custom_command).filter(
                    and_(Qa_custom_command.user == user)
                )
            elif osname is None:
                # We are displaying the list of QAs for use where OS is not
                # defined (view list of QAs)
                result = session.query(Qa_custom_command).filter(
                    or_(
                        Qa_custom_command.user == user,
                        Qa_custom_command.user == "allusers",
                    )
                )
            else:
                # We are displaying the list of QAs for use where OS is defined
                # (list QAs for specific machine)
                result = session.query(Qa_custom_command).filter(
                    and_(
                        or_(
                            Qa_custom_command.user == user,
                            Qa_custom_command.user == "allusers",
                        ),
                        Qa_custom_command.os == osname,
                    )
                )

            total = self.get_count(result)
            # TODO: filter
            if filt is not None:
                result = result.filter(
                    or_(
                        result.namecmd.like("%%%s%%" % (filt)),
                        result.os.like("%%%s%%" % (filt)),
                        result.description.like("%%%s%%" % (filt)),
                    )
                )

            nbfilter = self.get_count(result)

            if min is not None and max is not None:
                result = result.offset(int(min)).limit(int(max) - int(min))
                ret["limit"] = int(max) - int(min)
            if min:
                ret["min"] = min
            if max:
                ret["max"] = max
            if filt:
                ret["filt"] = filt
            result = result.all()
            session.commit()
            session.flush()
            ret["len"] = total
            ret["nb"] = nbfilter

            arraylist = []
            for t in result:
                obj = {}
                obj["user"] = t.user
                obj["os"] = t.os
                obj["namecmd"] = t.namecmd
                obj["customcmd"] = t.customcmd
                obj["description"] = t.description
                arraylist.append(obj)
            ret["command"] = arraylist
            return ret
        except Exception as e:
            logging.getLogger().debug("getlistcommandforuserbyos error %s->" % str(e))
            return ret

    @DatabaseHelper._sessionm
    def addPackageByOrganization(
        self, session, packageuuid, organization_name=None, organization_id=None
    ):
        """
        addition reference package in packages table for organization id
            the organization input parameter is either organization name or either organization id
            return -1 if not created
        """
        # recupere id organization
        idorganization = -1
        try:
            if organization_id is not None:
                try:
                    result_organization = session.query(Organization).filter(
                        Organization.id == organization_id
                    )
                    result_organization = result_organization.one()
                    session.commit()
                    session.flush()
                    idorganization = result_organization.id
                except Exception as e:
                    logging.getLogger().debug(
                        "organization id : %s is not exist" % organization_id
                    )
                    return -1
            elif organization_name is not None:
                idorganization = self.getIdOrganization(organization_name)
                if idorganization == -1:
                    return -1
            else:
                return -1

            # addition reference package in listpackages for attribut
            # organization id.
            packageslist = Packages_list()
            packageslist.organization_id = idorganization
            packageslist.packageuuid = packageuuid
            session.add(packageslist)
            session.commit()
            session.flush()
            return packageslist.id
        except Exception as e:
            logging.getLogger().error(str(e))
            logging.getLogger().debug(
                "add Package [%s] for Organization : %s%s is not exist"
                % (
                    packageuuid,
                    self.__returntextisNone__(organization_name),
                    self.__returntextisNone__(organization_id),
                )
            )
            return -1

    def __returntextisNone__(para, text=""):
        if para is None:
            return text
        else:
            return para

    # gestion packages
    @DatabaseHelper._sessionm
    def resetPresenceMachine(self, session):
        session.query(Machines).update({Machines.enabled: "0"})
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def getIdMachineFromMacaddress(self, session, macaddress):
        presence = (
            session.query(Machines.id)
            .filter(Machines.macaddress.like(macaddress + "%"))
            .first()
        )
        session.commit()
        session.flush()
        return presence

    @DatabaseHelper._sessionm
    def getMachinefrommacadress(self, session, macaddress, agenttype=None):
        """information machine"""
        if agenttype is None:
            machine = (
                session.query(Machines)
                .filter(Machines.macaddress.like(macaddress))
                .first()
            )
        elif agenttype == "machine":
            machine = (
                session.query(Machines)
                .filter(
                    and_(
                        Machines.macaddress.like(macaddress),
                        Machines.agenttype.like("machine"),
                    )
                )
                .first()
            )
        elif agenttype == "relayserver":
            machine = (
                session.query(Machines)
                .filter(
                    and_(
                        Machines.macaddress.like(macaddress),
                        Machines.agenttype.like("relayserver"),
                    )
                )
                .first()
            )
        session.commit()
        session.flush()
        result = {}
        if machine:
            result = {
                "id": machine.id,
                "jid": machine.jid,
                "platform": machine.platform,
                "archi": machine.archi,
                "hostname": machine.hostname,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "ippublic": machine.ippublic,
                "macaddress": machine.macaddress,
                "subnetxmpp": machine.subnetxmpp,
                "agenttype": machine.agenttype,
                "classutil": machine.classutil,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "picklekeypublic": machine.picklekeypublic,
                "ad_ou_user": machine.ad_ou_user,
                "ad_ou_machine": machine.ad_ou_machine,
                "kiosk_presence": machine.kiosk_presence,
                "lastuser": machine.lastuser,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
                "uuid_serial_machine": machine.uuid_serial_machine,
            }
        return result

    @DatabaseHelper._sessionm
    def getMachinefromuuidsetup(self, session, uuid_serial_machine, agenttype=None):
        """information machine"""
        if agenttype is None:
            machine = (
                session.query(Machines)
                .filter(Machines.uuid_serial_machine.like(uuid_serial_machine))
                .first()
            )
        elif agenttype == "machine":
            machine = (
                session.query(Machines)
                .filter(
                    and_(
                        Machines.uuid_serial_machine.like(uuid_serial_machine),
                        Machines.agenttype.like("machine"),
                    )
                )
                .first()
            )
        elif agenttype == "relayserver":
            machine = (
                session.query(Machines)
                .filter(
                    and_(
                        Machines.uuid_serial_machine.like(uuid_serial_machine),
                        Machines.agenttype.like("relayserver"),
                    )
                )
                .first()
            )
        session.commit()
        session.flush()
        result = {}
        if machine:
            result = {
                "id": machine.id,
                "jid": machine.jid,
                "platform": machine.platform,
                "archi": machine.archi,
                "hostname": machine.hostname,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "ippublic": machine.ippublic,
                "macaddress": machine.macaddress,
                "subnetxmpp": machine.subnetxmpp,
                "agenttype": machine.agenttype,
                "classutil": machine.classutil,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "picklekeypublic": machine.picklekeypublic,
                "ad_ou_user": machine.ad_ou_user,
                "ad_ou_machine": machine.ad_ou_machine,
                "kiosk_presence": machine.kiosk_presence,
                "lastuser": machine.lastuser,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
                "uuid_serial_machine": machine.uuid_serial_machine,
            }
        return result

    @DatabaseHelper._sessionm
    def addPresenceMachine(
        self,
        session,
        jid,
        platform,
        hostname,
        archi,
        uuid_inventorymachine,
        ip_xmpp,
        subnetxmpp,
        macaddress,
        agenttype,
        classutil="private",
        urlguacamole="",
        groupdeploy="",
        objkeypublic=None,
        ippublic=None,
        ad_ou_user="",
        ad_ou_machine="",
        kiosk_presence="False",
        lastuser="",
        keysyncthing="",
        uuid_serial_machine="",
        glpi_description="",
        glpi_owner_firstname="",
        glpi_owner_realname="",
        glpi_owner="",
        model="",
        manufacturer="",
        glpi_entity_id=1,
        glpi_location_id=None,
    ):
        if uuid_inventorymachine is None:
            uuid_inventorymachine = ""
        msg = "Create Machine"
        pe = -1
        if uuid_serial_machine != "":
            machineforupdate = self.getMachinefromuuidsetup(
                uuid_serial_machine, agenttype=agenttype
            )
        else:
            machineforupdate = self.getMachinefrommacadress(
                macaddress, agenttype=agenttype
            )
        if machineforupdate:
            pe = machineforupdate["id"]
        if pe != -1:
            # update
            maxlenhostname = max([len(machineforupdate["hostname"]), len(hostname)])
            maxlenjid = max([len(machineforupdate["jid"]), len(jid)])
            maxmacadress = max([len(machineforupdate["macaddress"]), len(macaddress)])
            maxip_xmpp = max(
                [len(machineforupdate["ip_xmpp"]), len(ip_xmpp), len("ip_xmpp")]
            )
            maxsubnetxmpp = max(
                [
                    len(machineforupdate["subnetxmpp"]),
                    len(subnetxmpp),
                    len("subnetxmpp"),
                ]
            )
            maxonoff = 6
            uuidold = str(machineforupdate["uuid_inventorymachine"])
            if uuid_inventorymachine is None:
                uuidnew = "None"
            else:
                uuidnew = str(uuid_inventorymachine)
            if lastuser is None or lastuser == "":
                lastuser = str(machineforupdate["lastuser"])
            maxuuid = max([len(uuidold), len(uuidnew)])
            msg = (
                "Update Machine %8s (%s)\n"
                "|%*s|%*s|%*s|%*s|%*s|%*s|%*s|\n"
                "|%*s|%*s|%*s|%*s|%*s|%*s|%*s|\n"
                "by\n"
                "|%*s|%*s|%*s|%*s|%*s|%*s|%*s|"
                % (
                    machineforupdate["id"],
                    uuid_serial_machine,
                    maxlenhostname,
                    "hostname",
                    maxlenjid,
                    "jid",
                    maxmacadress,
                    "macaddress",
                    maxip_xmpp,
                    "ip_xmpp",
                    maxsubnetxmpp,
                    "subnetxmpp",
                    maxonoff,
                    "On/OFF",
                    maxuuid,
                    "UUID",
                    maxlenhostname,
                    machineforupdate["hostname"],
                    maxlenjid,
                    machineforupdate["jid"],
                    maxmacadress,
                    machineforupdate["macaddress"],
                    maxip_xmpp,
                    machineforupdate["ip_xmpp"],
                    maxsubnetxmpp,
                    machineforupdate["subnetxmpp"],
                    maxonoff,
                    machineforupdate["enabled"],
                    maxuuid,
                    uuidold,
                    maxlenhostname,
                    hostname,
                    maxlenjid,
                    jid,
                    maxmacadress,
                    macaddress,
                    maxip_xmpp,
                    ip_xmpp,
                    maxsubnetxmpp,
                    subnetxmpp,
                    maxonoff,
                    "1",
                    6,
                    uuidnew,
                )
            )
            self.logger.warning(msg)
            session.query(Machines).filter(Machines.id == pe).update(
                {
                    Machines.jid: jid,
                    Machines.platform: platform,
                    Machines.hostname: hostname,
                    Machines.archi: archi,
                    Machines.uuid_inventorymachine: uuid_inventorymachine,
                    Machines.ippublic: ippublic,
                    Machines.ip_xmpp: ip_xmpp,
                    Machines.subnetxmpp: subnetxmpp,
                    Machines.macaddress: macaddress,
                    Machines.agenttype: agenttype,
                    Machines.classutil: classutil,
                    Machines.urlguacamole: urlguacamole,
                    Machines.groupdeploy: groupdeploy,
                    Machines.picklekeypublic: objkeypublic,
                    Machines.ad_ou_user: ad_ou_user,
                    Machines.ad_ou_machine: ad_ou_machine,
                    Machines.kiosk_presence: kiosk_presence,
                    Machines.lastuser: lastuser,
                    Machines.keysyncthing: keysyncthing,
                    Machines.enabled: 1,
                    Machines.uuid_serial_machine: uuid_serial_machine,
                }
            )
            session.commit()
            session.flush()
            return pe, msg
        else:
            # create
            lenhostname = len(hostname)
            lenjid = len(jid)
            lenmacadress = len(macaddress)
            lenip_xmpp = len(ip_xmpp)
            lensubnetxmpp = len(subnetxmpp)
            lenonoff = 6
            msg = (
                "creat Machine (%s)\n"
                "|%*s|%*s|%*s|%*s|%*s|%*s|\n"
                "|%*s|%*s|%*s|%*s|%*s|%*s|\n"
                % (
                    uuid_serial_machine,
                    lenhostname,
                    "hostname",
                    lenjid,
                    "jid",
                    lenmacadress,
                    "macaddress",
                    lenip_xmpp,
                    "ip_xmpp",
                    lensubnetxmpp,
                    "subnetxmpp",
                    lenonoff,
                    "On/OFF",
                    lenhostname,
                    hostname,
                    lenjid,
                    jid,
                    lenmacadress,
                    macaddress,
                    lenip_xmpp,
                    ip_xmpp,
                    lensubnetxmpp,
                    subnetxmpp,
                    lenonoff,
                    "1",
                )
            )
            self.logger.debug(msg)
            try:
                new_machine = Machines()
                new_machine.jid = jid
                new_machine.platform = platform
                new_machine.hostname = hostname
                new_machine.archi = archi
                new_machine.uuid_inventorymachine = uuid_inventorymachine
                new_machine.ippublic = ippublic
                new_machine.ip_xmpp = ip_xmpp
                new_machine.subnetxmpp = subnetxmpp
                new_machine.macaddress = macaddress
                new_machine.agenttype = agenttype
                new_machine.classutil = classutil
                new_machine.urlguacamole = urlguacamole
                new_machine.groupdeploy = groupdeploy
                new_machine.picklekeypublic = objkeypublic
                new_machine.ad_ou_user = ad_ou_user
                new_machine.ad_ou_machine = ad_ou_machine
                new_machine.kiosk_presence = kiosk_presence
                new_machine.lastuser = lastuser
                new_machine.keysyncthing = keysyncthing
                new_machine.glpi_description = glpi_description
                new_machine.glpi_owner_firstname = glpi_owner_firstname
                new_machine.glpi_owner_realname = glpi_owner_realname
                new_machine.glpi_owner = glpi_owner
                new_machine.model = model
                new_machine.manufacturer = manufacturer
                new_machine.glpi_entity_id = glpi_entity_id
                new_machine.glpi_location_id = glpi_location_id
                new_machine.enabled = 1
                new_machine.uuid_serial_machine = uuid_serial_machine
                session.add(new_machine)
                session.commit()
                session.flush()
                if agenttype == "relayserver":
                    sql = (
                        "UPDATE `xmppmaster`.`relayserver` \
                                SET `enabled`=1 \
                                WHERE `xmppmaster`.`relayserver`.`nameserver`='%s';"
                        % hostname
                    )
                    session.execute(sql)
                    session.commit()
                    session.flush()
                else:
                    sql = """DELETE FROM xmppmaster.machines
                        WHERE
                        hostname LIKE '%s' and
                            id < %s;
                            """ % (
                        hostname,
                        new_machine.id,
                    )
                    self.logger.debug(sql)
                    session.execute(sql)
                    session.commit()
                    session.flush()
            except Exception as e:
                logging.getLogger().error(str(e))
                msg = str(e)
                return -1, msg
            return new_machine.id, msg

    @DatabaseHelper._sessionm
    def is_jiduser_organization_ad(self, session, jiduser):
        """
        if user exist return True
        """
        sql = """SELECT COUNT(jiduser) AS nb
            FROM
                 xmppmaster.organization_ad
             WHERE
              jiduser LIKE ('%s');""" % (
            jiduser
        )
        req = session.execute(sql)
        session.commit()
        session.flush()
        ret = [m[0] for m in req]
        if ret[0] == 0:
            return False
        return True

    def uuidtoid(self, uuid):
        if uuid.strip().lower().startswith("uuid"):
            return uuid[4:]
        else:
            return uuid

    @DatabaseHelper._sessionm
    def is_id_inventory_organization_ad(self, session, id_inventory):
        """if id_inventory exist return True"""
        sql = """SELECT COUNT(id_inventory) AS nb
            FROM
                 xmppmaster.organization_ad
             WHERE
              jiduser LIKE ('%s');""" % (
            self.uuidtoid(id_inventory)
        )
        req = session.execute(sql)
        session.commit()
        session.flush()
        ret = [m[0] for m in req]
        if ret[0] == 0:
            return False
        return True

    @DatabaseHelper._sessionm
    def is_id_inventory_jiduser_organization_ad(self, session, id_inventory, jiduser):
        """if id_inventory exist return True"""
        sql = """SELECT COUNT(id_inventory) AS nb
            FROM
                 xmppmaster.organization_ad
             WHERE
              jiduser LIKE ('%s')
              and
              id_inventory LIKE ('%s')
              ;""" % (
            jiduser,
            self.uuidtoid(id_inventory),
        )
        req = session.execute(sql)
        session.commit()
        session.flush()
        ret = [m[0] for m in req]
        if ret[0] == 0:
            return False
        return True

    @DatabaseHelper._sessionm
    def getAllOUuser(self, session, ctx, filt=""):
        """
        @return: all ou defined in the xmpp database
        """
        query = session.query(Organization_ad)
        if filter != "":
            query = query.filter(Organization_ad.ouuser.like("%" + filt + "%"))
        ret = query.all()
        session.close()
        return ret

    @DatabaseHelper._sessionm
    def getAllOUmachine(self, session, ctx, filt=""):
        """
        @return: all ou defined in the xmpp database
        """
        query = session.query(Organization_ad)
        if filter != "":
            query = query.filter(Organization_ad.oumachine.like("%" + filt + "%"))
        ret = query.all()
        session.close()
        return ret

    @DatabaseHelper._sessionm
    def replace_Organization_ad_id_inventory(
        self, session, old_id_inventory, new_id_inventory
    ):
        if old_id_inventory is None:
            logging.getLogger().warning("Organization AD id inventory is not exits")
            return -1
        try:
            session.query(Organization_ad).filter(
                Organization_ad.id_inventory == self.uuidtoid(old_id_inventory)
            ).update({Organization_ad.id_inventory: self.uuidtoid(new_id_inventory)})
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def updateOrganization_ad_id_inventory(
        self,
        session,
        id_inventory,
        jiduser,
        ouuser="",
        oumachine="",
        hostname="",
        username="",
    ):
        """
        update Organization_ad table in base xmppmaster
        """
        try:
            session.query(Organization_ad).filter(
                Organization_ad.id_inventory == self.uuidtoid(id_inventory)
            ).update(
                {
                    Organization_ad.jiduser: jiduser,
                    Organization_ad.id_inventory: self.uuidtoid(id_inventory),
                    Organization_ad.ouuser: ouuser,
                    Organization_ad.oumachine: oumachine,
                    Organization_ad.hostname: hostname,
                    Organization_ad.username: username,
                }
            )
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def updateOrganization_ad_jiduser(
        self,
        session,
        id_inventory,
        jiduser,
        ouuser="",
        oumachine="",
        hostname="",
        username="",
    ):
        """
        update Organization_ad table in base xmppmaster
        """
        try:
            session.query(Organization_ad).filter(
                Organization_ad.jiduser == jiduser
            ).update(
                {
                    Organization_ad.jiduser: jiduser,
                    Organization_ad.id_inventory: self.uuidtoid(id_inventory),
                    Organization_ad.ouuser: ouuser,
                    Organization_ad.oumachine: oumachine,
                    Organization_ad.hostname: hostname,
                    Organization_ad.username: username,
                }
            )
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def addOrganization_ad(
        self,
        session,
        id_inventory,
        jiduser,
        ouuser="",
        oumachine="",
        hostname="",
        username="",
    ):
        id = self.uuidtoid(id_inventory)
        new_Organization = Organization_ad()
        new_Organization.id_inventory = id
        new_Organization.jiduser = jiduser
        new_Organization.ouuser = ouuser
        new_Organization.oumachine = oumachine
        new_Organization.hostname = hostname
        new_Organization.username = username
        boolexistuserjid = self.is_jiduser_organization_ad(jiduser)
        if not boolexistuserjid:
            # Creation de organization for machine jiduser
            if self.is_id_inventory_organization_ad(id):
                # Delete for uuid
                self.delOrganization_ad(id_inventory=id)
            try:
                session.add(new_Organization)
                session.commit()
                session.flush()
            except Exception as e:
                logging.getLogger().error(
                    "creation Organisation_ad for jid user %s inventory uuid : %s"
                    % (jiduser, id)
                )
                logging.getLogger().error(
                    "ouuser=%s\noumachine = %s\nhostname=%s\nusername=%s"
                    % (ouuser, oumachine, hostname, username)
                )
                logging.getLogger().error(str(e))
                return -1
            return new_Organization.id_inventory
        else:
            # Update fiche
            self.updateOrganization_ad_jiduser(
                id_inventory,
                jiduser,
                ouuser=ouuser,
                oumachine=oumachine,
                hostname=hostname,
                username=username,
            )
        return new_Organization.id_inventory

    @DatabaseHelper._sessionm
    def delOrganization_ad(self, session, id_inventory=None, jiduser=None):
        """
        supp organization ad
        """
        req = session.query(Organization_ad)
        if id_inventory is not None and jiduser is not None:
            req = req.filter(
                and_(
                    Organization_ad.id_inventory == id_inventory,
                    Organization_ad.jiduser == jiduser,
                )
            )
        elif id_inventory is not None and jiduser is None:
            req = req.filter(Organization_ad.id_inventory == id_inventory)
        elif jiduser is not None and id_inventory is None:
            req = req.filter(Organization_ad.jiduser == jiduser)
        else:
            return False
        try:
            req.delete()
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error("delOrganization_ad : %s " % str(e))
            return False

    @DatabaseHelper._sessionm
    def loginbycommand(self, session, idcommand):
        sql = (
            """SELECT
                    login
                FROM
                    xmppmaster.has_login_command
                WHERE
                    command = %s
                    LIMIT 1 ;"""
            % idcommand
        )
        try:
            result = session.execute(sql)
            session.commit()
            session.flush()
            # result = [x for x in result]
            # print result.__dict__
            l = [x[0] for x in result][0]
            return l
        except Exception as e:
            logging.getLogger().error(str(e))
            return ""

    @DatabaseHelper._sessionm
    def updatedeployinfo(self, session, idcommand):
        """
        this function allows to update the counter of deployments in pause
        """
        try:
            session.query(Has_login_command).filter(
                and_(Has_login_command.command == idcommand)
            ).update(
                {
                    Has_login_command.count_deploy_progress: Has_login_command.count_deploy_progress
                    + 1
                }
            )
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            return -1

    @DatabaseHelper._sessionm
    def wolbroadcastadressmacadress(self, session, listmacaddress):
        """
        We monitor the mac addresses to check.

        Args:
            session: The SQL Alchemy session
            listmacaddress: The mac addressesses to follow

        Return:
            We return those mac addresses grouped by the broadcast address.
        """
        grp_wol_broadcast_adress = {}
        result = (
            session.query(Network.broadcast, Network.mac)
            .distinct(Network.mac)
            .filter(
                and_(
                    Network.broadcast != "",
                    Network.broadcast.isnot(None),
                    Network.macaddress.in_(listmacaddress),
                )
            )
            .all()
        )

        if not bool(result):
            logger.error("An error occured while checking the broadcast address.")
            logger.error(
                "Please check that the broadcast information exists for the following mac addresses: %s"
                % listmacaddress
            )

        for t in result:
            if t.broadcast not in grp_wol_broadcast_adress:
                grp_wol_broadcast_adress[t.broadcast] = []
            grp_wol_broadcast_adress[t.broadcast].append(t.mac)
        return grp_wol_broadcast_adress

    def convertTimestampToSQLDateTime(self, value):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))

    def convertSQLDateTimeToTimestamp(self, value):
        return time.mktime(time.strptime(value, "%Y-%m-%d %H:%M:%S"))

    @DatabaseHelper._sessionm
    def checkstatusdeploy(self, session, idcommand):
        """
        Détermine l'état du déploiement lorsque le déploiement est planifié et le planificateur est actif.

        Paramètres :
            - session : Session SQLAlchemy. Une session SQLAlchemy préexistante pour la base de données.
            - idcommand : str. ID de la commande de déploiement pour laquelle on souhaite déterminer l'état.

        Remarques :
            - Cette fonction est utilisée pour déterminer l'état du déploiement lorsque le déploiement est planifié et que le
            planificateur est actif.
            - La fonction vérifie si le déploiement est dans la plage de temps prévue par la colonne "startcmd" et "endcmd" du modèle
            "Deploy". Si le déploiement n'est pas dans cette plage de temps, la fonction met à jour l'état des sessions de déploiement
            associées avec "ERROR UNKNOWN ERROR" et renvoie 'abandonmentdeploy'.
            - Ensuite, la fonction vérifie si le déploiement est conditionné par la colonne "start_exec_on_time" ou
            "start_exec_on_nb_deploy" du modèle "Has_login_command". Si c'est le cas, elle compare la date et l'heure actuelles avec
            la valeur de "start_exec_on_time" et le nombre de déploiements "start_exec_on_nb_deploy" pour déterminer si le déploiement
            peut s'exécuter (renvoie 'run') ou s'il doit être mis en pause (renvoie 'pause').
            - Si le déploiement ne répond à aucune des conditions ci-dessus, la fonction renvoie 'pause'.

        Retour :
            - 'abandonmentdeploy' : Le déploiement est hors de la plage de temps prévue ou la commande de déploiement associée a été
            supprimée de la base de données. L'état des sessions de déploiement est mis à jour avec "ERROR UNKNOWN ERROR".
            - 'run' : Le déploiement est conditionné par une heure de début ou un nombre de déploiements spécifié, et les conditions sont
            remplies pour exécuter le déploiement.
            - 'pause' : Le déploiement ne répond à aucune des conditions ci-dessus. Il doit être mis en pause.
        """
        nowtime = datetime.now()
        try:
            result = (
                session.query(Has_login_command)
                .filter(and_(Has_login_command.command == idcommand))
                .order_by(desc(Has_login_command.id))
                .limit(1)
                .one()
            )
            deployresult = (
                session.query(Deploy)
                .filter(and_(Deploy.command == idcommand))
                .order_by(desc(Deploy.id))
                .limit(1)
                .one()
            )
        except BaseException:
            # error case command supp base nunualy
            return "abandonmentdeploy"

        if not (deployresult.startcmd <= nowtime and deployresult.endcmd >= nowtime):
            # we are more in the range of deployments.
            # abandonmentdeploy
            for id in self.sessionidforidcommand(idcommand):
                self.updatedeploystate(id, "ERROR UNKNOWN ERROR")
            return "abandonmentdeploy"

        if not (
            result.start_exec_on_time is None
            or str(result.start_exec_on_time) == ""
            or str(result.start_exec_on_time) == "None"
        ):
            # time processing
            if nowtime > result.start_exec_on_time:
                return "run"
        if not (
            result.start_exec_on_nb_deploy is None
            or result.start_exec_on_nb_deploy == ""
        ):
            if result.start_exec_on_nb_deploy <= result.count_deploy_progress:
                return "run"
        for id in self.sessionidforidcommand(idcommand):
            self.updatedeploystate(id, "DEPLOYMENT DELAYED")
        return "pause"

    @DatabaseHelper._sessionm
    def update_status_deploy_end(self, session):
        """this function schedued by xmppmaster"""
        datenow = datetime.now()
        result = (
            session.query(Deploy)
            .filter(
                and_(Deploy.endcmd < datenow, Deploy.state.like("DEPLOYMENT START%%"))
            )
            .all()
        )
        session.flush()
        session.close()
        for t in result:
            try:
                sql = (
                    """UPDATE `xmppmaster`.`deploy`
                                SET `state`='ERROR UNKNOWN ERROR'
                                WHERE `id`='%s';"""
                    % t.id
                )
                session.execute(sql)
                session.commit()
                session.flush()
            except Exception as e:
                logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def sessionidforidcommand(self, session, idcommand):
        result = (
            session.query(Deploy.sessionid).filter(Deploy.command == idcommand).all()
        )
        if result:
            a = [m[0] for m in result]
            return a
        else:
            return []

    @DatabaseHelper._sessionm
    def datacmddeploy(self, session, idcommand):
        try:
            result = (
                session.query(Has_login_command)
                .filter(and_(Has_login_command.command == idcommand))
                .order_by(desc(Has_login_command.id))
                .limit(1)
            )
            result = result.one()
            session.commit()
            session.flush()
            obj = {"countnb": 0, "exec": True}
            if result.login != "":
                obj["login"] = result.login
            obj["idcmd"] = result.command

            if not (
                result.start_exec_on_time is None
                or str(result.start_exec_on_time) == ""
                or str(result.start_exec_on_time) == "None"
            ):
                obj["exectime"] = str(result.start_exec_on_time)
                obj["exec"] = False

            if result.grpid != "":
                obj["grp"] = result.grpid

            if result.nb_machine_for_deploy != "":
                obj["nbtotal"] = result.nb_machine_for_deploy
            if not (
                result.start_exec_on_nb_deploy is None
                or result.start_exec_on_nb_deploy == ""
            ):
                obj["consignnb"] = result.start_exec_on_nb_deploy
                obj["exec"] = False

            obj["rebootrequired"] = result.rebootrequired
            obj["shutdownrequired"] = result.shutdownrequired
            obj["limit_rate_ko"] = result.bandwidth
            obj["syncthing"] = result.syncthing
            if result.params_json is not None:
                try:
                    params_json = json.loads(result.params_json)
                    if "spooling" in params_json:
                        obj["spooling"] = params_json["spooling"]
                except Exception as e:
                    logging.getLogger().error(
                        "[the avanced parameters from msc] : " + str(e)
                    )

            if result.parameters_deploy is not None:
                try:
                    params = str(result.parameters_deploy)
                    if params == "":
                        return obj
                    if not params.startswith("{"):
                        params = "{" + params
                    if not params.endswith("}"):
                        params = params + "}"
                    obj["paramdeploy"] = json.loads(params)
                except Exception as e:
                    logging.getLogger().error(
                        "[the avanced parameters must be"
                        " declared in a json dictionary] : " + str(e)
                    )
            return obj
        except Exception as e:
            logging.getLogger().error("[ obj commandid missing] : " + str(e))
            return {}

    @DatabaseHelper._sessionm
    def adddeploy(
        self,
        session,
        idcommand,
        jidmachine,
        jidrelay,
        host,
        inventoryuuid,
        uuidpackage,
        state,
        sessionid,
        user="",
        login="",
        title="",
        group_uuid=None,
        startcmd=None,
        endcmd=None,
        macadress=None,
        result=None,
        syncthing=None,
    ):
        """
        parameters
        startcmd and endcmd  int(timestamp) either str(datetime)
        """
        createcommand = datetime.now()
        try:
            start = int(startcmd)
            end = int(endcmd)
            print(start)
            print(end)
            startcmd = datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
            endcmd = datetime.fromtimestamp(end).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            pass
        # del doublon macadess
        if macadress is not None:
            adressemac = str(macadress).split("||")
            adressemac = list(set(adressemac))
            macadress = "||".join(adressemac)
        # recupere login command
        if login == "":
            login = self.loginbycommand(idcommand)[0]
        try:
            new_deploy = Deploy()
            new_deploy.group_uuid = group_uuid
            new_deploy.jidmachine = jidmachine
            new_deploy.jid_relay = jidrelay
            new_deploy.host = host
            new_deploy.inventoryuuid = inventoryuuid
            new_deploy.pathpackage = uuidpackage
            new_deploy.state = state
            new_deploy.sessionid = sessionid
            new_deploy.user = user
            new_deploy.command = idcommand
            new_deploy.login = login
            new_deploy.startcmd = startcmd
            new_deploy.endcmd = endcmd
            new_deploy.start = createcommand
            new_deploy.macadress = macadress
            new_deploy.title = title
            if result is not None:
                new_deploy.result = result
            if syncthing is not None:
                new_deploy.syncthing = syncthing
            session.add(new_deploy)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        return new_deploy.id

    @DatabaseHelper._sessionm
    def deploysyncthingxmpp(self, session):
        """
        analyse the deploy table and creates the sharing syncthing
        """
        # todo: get ARS device
        datenow = datetime.now()
        result = (
            session.query(Deploy)
            .filter(and_(Deploy.startcmd <= datenow, Deploy.syncthing == 1))
            .all()
        )
        id_deploylist = set()
        # TODO: search keysyncthing in table machine.
        session.commit()
        session.flush()
        if len(result) == 0:
            return list(id_deploylist)
        list_id_ars = {}
        list_ars = set()
        list_cluster = set()
        # syncthing and set stat to 2
        self.chang_status_deploy_syncthing(datenow)
        cluster = self.clusterlistars()
        cluster_pris_encharge = []
        gr_pris_en_charge = -1
        command_pris_en_charge = -1

        for t in result:
            if t.group_uuid == "":
                # The machine MUST be in a group
                continue
            # if command_pris_en_charge == -1:
            # on deploy qu'une commande sur 1 group a la fois en syncthing
            # command_pris_en_charge = t.command
            # gr_pris_en_charge = t.group_uuid
            # if t.command != command_pris_en_charge or \
            # t.group_uuid != gr_pris_en_charge:
            # continue
            # if t.inventoryuuid.startswith("UUID"):
            # inventoryid = int(t.inventoryuuid[4:])
            # else:
            # inventoryid = int(t.inventoryuuid)

            e = json.loads(t.result)
            package = os.path.basename(e["path"])
            # creation du partage si celui ci n'existe pas.
            id_deploy = self.setSyncthing_deploy_group(
                t.title,
                uuid.uuid4(),  # namepartage
                package,
                t.command,
                t.group_uuid,
                dateend=t.endcmd,
            )
            id_deploylist.add(id_deploy)
            clu = self.clusternum(t.jid_relay)
            ars_cluster_id = self.setSyncthing_ars_cluster(
                clu["numcluster"],
                clu["namecluster"],
                t.jid_relay,
                clu["choose"],
                id_deploy,
                type_partage="cluster",
                evivesyncthing="",
                keypartage="",
            )
            cluster = self.clusterlistars()
            clusterdata = {}
            for z in cluster:
                if t.jid_relay in cluster[z]["listarscluster"]:
                    # on trouve le cluster qui possede ars
                    clusterdata = cluster[z]
            self.setSyncthing_machine(
                t.jidmachine,
                t.jid_relay,
                json.dumps(clusterdata),
                package,
                t.sessionid,
                t.start,
                t.startcmd,
                t.endcmd,
                t.command,
                t.group_uuid,
                t.result,
                ars_cluster_id,
                syncthing=t.syncthing,
                state=t.state,
                user=t.user,
                type_partage="",
                title=t.title,
                inventoryuuid=t.inventoryuuid,
                login=t.login,
                macadress=t.macadress,
                comment="%s_%s"
                % (
                    t.command,
                    t.group_uuid,
                ),
            )

        return list(id_deploylist)

    # =====================================================================
    # xmppmaster verification jid for deploy
    # =====================================================================
    @DatabaseHelper._sessionm
    def update_jid_if_changed(self, session, jidmachine):
        try:
            sql = """SELECT
                        xmppmaster.machines.jid,
                        xmppmaster.machines.groupdeploy
                    FROM
                        xmppmaster.machines
                    WHERE
                        xmppmaster.machines.hostname = xmppmaster.FS_JIDUSERTRUE('%s')
                            limit 1;""" % (
                jidmachine
            )
            resultproxy = session.execute(sql)
            session.commit()
            session.flush()
            if not resultproxy:
                return []
            else:
                ret = self._return_dict_from_dataset_mysql(resultproxy)
                return ret
        except Exception as e:
            logging.getLogger().error(str(e))
            return False
        return []

    @DatabaseHelper._sessionm
    def replace_jid_mach_ars_in_deploy(self, session, jidmachine, jidrelay, title):
        """
        Cette fonction est utilise pour mettre a jour les jid dans deploy quand 1 machine reenregistre change de jid.

        Args:
            jidmachine : new JID
            jidrelay : new jidrelay
            title : title du deploy
        """
        try:
            sql = """
                    UPDATE `xmppmaster`.`deploy`
                    SET
                        `jidmachine` = '%s',
                        `jid_relay` = '%s'
                    WHERE
                        (`title` = '%s');""" % (
                jidmachine,
                jidrelay,
                title,
            )
            session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    # =====================================================================
    # xmppmaster FUNCTIONS deploy syncthing
    # =====================================================================
    @DatabaseHelper._sessionm
    def setSyncthing_deploy_group(
        self,
        session,
        namepartage,
        directory_tmp,
        packagename,
        cmd,
        grp_parent,
        status="C",
        dateend=None,
        deltatime=60,
    ):
        try:
            idpartage = self.search_partage_for_package(packagename)
            if idpartage == -1:
                print("add partage")
                # il faut cree le partage.
                new_Syncthing_deploy_group = Syncthing_deploy_group()
                new_Syncthing_deploy_group.namepartage = namepartage
                new_Syncthing_deploy_group.directory_tmp = directory_tmp
                new_Syncthing_deploy_group.cmd = cmd
                new_Syncthing_deploy_group.status = status
                new_Syncthing_deploy_group.package = packagename
                new_Syncthing_deploy_group.grp_parent = grp_parent
                if dateend is None:
                    dateend = datetime.now() + timedelta(minutes=deltatime)
                else:
                    new_Syncthing_deploy_group.dateend = dateend + timedelta(
                        minutes=deltatime
                    )
                session.add(new_Syncthing_deploy_group)
                session.commit()
                session.flush()
                return new_Syncthing_deploy_group.id
            else:
                return idpartage
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def incr_count_transfert_terminate(self, session, iddeploy):
        sql = """UPDATE xmppmaster.syncthing_deploy_group
                SET
                    nbtransfert = nbtransfert + 1
                WHERE
                    id = %s;""" % (
            iddeploy
        )
        # print "incr_count_transfert_terminate", sql
        result = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def update_transfert_progress(self, session, progress, iddeploy, jidmachine):
        """this function update this level progress"""
        sql = """
                UPDATE xmppmaster.syncthing_machine
                        INNER JOIN
                    syncthing_ars_cluster
                      ON xmppmaster.syncthing_ars_cluster.id =
                             xmppmaster.syncthing_machine.fk_arscluster
                        INNER JOIN
                    xmppmaster.syncthing_deploy_group
                      ON xmppmaster.syncthing_deploy_group.id =
                      xmppmaster.syncthing_ars_cluster.fk_deploy
                SET
                    xmppmaster.syncthing_machine.progress = IF(%s>=xmppmaster.syncthing_machine.progress,%s,xmppmaster.syncthing_machine.progress)
                WHERE
                    xmppmaster.syncthing_deploy_group.id = %s
                        AND xmppmaster.syncthing_machine.jidmachine LIKE '%s';""" % (
            progress,
            progress,
            iddeploy,
            jidmachine,
        )
        # print "update_transfert_progress", sql
        result = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def get_ars_for_pausing_syncthing(self, session, nbtransfert=2):
        sql = """SELECT
                    xmppmaster.syncthing_deploy_group.id,
                    xmppmaster.syncthing_ars_cluster.liststrcluster,
                    xmppmaster.syncthing_deploy_group.directory_tmp,
                    xmppmaster.syncthing_deploy_group.nbtransfert,
                    xmppmaster.syncthing_ars_cluster.id
                FROM
                    xmppmaster.syncthing_deploy_group
                        INNER JOIN
                    xmppmaster.syncthing_ars_cluster
                      ON
                         xmppmaster.syncthing_deploy_group.id =
                         xmppmaster.syncthing_ars_cluster.fk_deploy
                WHERE
                    xmppmaster.syncthing_deploy_group.nbtransfert >= %s
                    and
                    xmppmaster.syncthing_ars_cluster.keypartage != "pausing";""" % (
            nbtransfert
        )
        # print "get_ars_for_pausing_syncthing"#, sql
        result = session.execute(sql)
        session.commit()
        session.flush()
        if result is None:
            return -1
        else:
            re = [y for y in [x for x in result]]
            for arssyncthing in re:
                self.update_ars_status(arssyncthing[4], "pausing")
        return re

    @DatabaseHelper._sessionm
    def update_ars_status(self, session, idars, keystatus="pausing"):
        sql = """UPDATE
                    xmppmaster.syncthing_ars_cluster
                SET
                    xmppmaster.syncthing_ars_cluster.keypartage = '%s'
                WHERE
                    xmppmaster.syncthing_ars_cluster.id = '%s';""" % (
            keystatus,
            idars,
        )
        # print "update_ars_status", sql
        result = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def search_partage_for_package(self, session, packagename):
        result = -1
        sql = """ SELECT
                    xmppmaster.syncthing_deploy_group.id
                FROM
                    xmppmaster.syncthing_deploy_group
                WHERE
                    xmppmaster.syncthing_deploy_group.package LIKE '%s'
                        AND xmppmaster.syncthing_deploy_group.dateend > DATE_SUB(NOW(), INTERVAL 1 HOUR)
                limit 1;""" % (
            packagename
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        resultat = [x for x in result]
        if len(resultat) == 0:
            return -1
        else:
            return resultat[0][0]

    @DatabaseHelper._sessionm
    def search_ars_cluster_for_package(self, session, idpartage, ars):
        result = -1
        sql = """SELECT
                xmppmaster.syncthing_ars_cluster.id
                FROM
                    xmppmaster.syncthing_ars_cluster
                where xmppmaster.syncthing_ars_cluster.fk_deploy = %s and
                xmppmaster.syncthing_ars_cluster.liststrcluster like '%s'
                LIMIT 1;""" % (
            idpartage,
            ars,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        resultat = [x for x in result]
        if len(resultat) == 0:
            return -1
        else:
            return resultat[0][0]

    @DatabaseHelper._sessionm
    def search_ars_master_cluster_(self, session, idpartage, numcluster):
        result = -1
        sql = """SELECT DISTINCT xmppmaster.syncthing_ars_cluster.arsmastercluster
                FROM
                    xmppmaster.syncthing_ars_cluster
                where
                    xmppmaster.syncthing_ars_cluster.fk_deploy = %s
                      and
                    xmppmaster.syncthing_ars_cluster.numcluster = %s limit 1;""" % (
            idpartage,
            numcluster,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        resultat = [x for x in result]
        countresult = len(resultat)

        if countresult == 0:
            return ""
        elif countresult == 1:
            return resultat[0][0]
        else:
            # il y a plusieurs cluster dans le deployement.
            # il faut donc choisir celui correspondant au cluster
            ljidars = [x[0] for x in resultat]
            for jidars in ljidars:
                # print jidars
                if self.ars_in_num_cluster(jidars, numcluster):
                    return jidars
        return ""

    @DatabaseHelper._sessionm
    def ars_in_num_cluster(self, session, jidars, numcluster):
        """
        test si jidars est dans le cluster number.
        """
        sql = """SELECT
                    id_ars
                FROM
                    xmppmaster.has_cluster_ars
                INNER JOIN
                    xmppmaster.relayserver
                        ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                where xmppmaster.relayserver.jid like '%s'
                  and
                  xmppmaster.has_cluster_ars.id_cluster= %s;""" % (
            jidars,
            numcluster,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        resultat = [x for x in result]
        if len(resultat) != 0:
            return True
        else:
            return False

    @DatabaseHelper._sessionm
    def setSyncthing_ars_cluster(
        self,
        session,
        numcluster,
        namecluster,
        liststrcluster,
        arsmastercluster,
        fk_deploy,
        type_partage="",
        devivesyncthing="",
        keypartage="",
    ):
        try:
            # search ars elu if exist for partage
            arsmasterclusterexist = self.search_ars_master_cluster_(
                fk_deploy, numcluster
            )
            ars_cluster_id = self.search_ars_cluster_for_package(
                fk_deploy, liststrcluster
            )
            if ars_cluster_id == -1:
                new_Syncthing_ars_cluster = Syncthing_ars_cluster()
                new_Syncthing_ars_cluster.numcluster = numcluster
                new_Syncthing_ars_cluster.namecluster = namecluster
                new_Syncthing_ars_cluster.liststrcluster = liststrcluster
                if arsmasterclusterexist == "":
                    new_Syncthing_ars_cluster.arsmastercluster = arsmastercluster
                else:
                    new_Syncthing_ars_cluster.arsmastercluster = arsmasterclusterexist
                new_Syncthing_ars_cluster.keypartage = keypartage
                new_Syncthing_ars_cluster.fk_deploy = fk_deploy
                new_Syncthing_ars_cluster.type_partage = type_partage
                new_Syncthing_ars_cluster.devivesyncthing = devivesyncthing
                session.add(new_Syncthing_ars_cluster)
                session.commit()
                session.flush()
                return new_Syncthing_ars_cluster.id
            else:
                return ars_cluster_id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def setSyncthing_machine(
        self,
        session,
        jidmachine,
        jid_relay,
        cluster,
        pathpackage,
        sessionid,
        start,
        startcmd,
        endcmd,
        command,
        group_uuid,
        result,
        fk_arscluster,
        syncthing=1,
        state="",
        user="",
        type_partage="",
        title="",
        inventoryuuid=None,
        login=None,
        macadress=None,
        comment="",
    ):
        try:
            new_Syncthing_machine = Syncthing_machine()
            new_Syncthing_machine.jidmachine = jidmachine
            new_Syncthing_machine.cluster = cluster
            new_Syncthing_machine.jid_relay = jid_relay
            new_Syncthing_machine.pathpackage = pathpackage
            new_Syncthing_machine.state = state
            new_Syncthing_machine.sessionid = sessionid
            new_Syncthing_machine.start = start
            new_Syncthing_machine.startcmd = startcmd
            new_Syncthing_machine.endcmd = endcmd
            new_Syncthing_machine.user = user
            new_Syncthing_machine.command = command
            new_Syncthing_machine.group_uuid = group_uuid
            new_Syncthing_machine.result = result
            new_Syncthing_machine.syncthing = syncthing
            new_Syncthing_machine.type_partage = type_partage
            new_Syncthing_machine.title = title
            new_Syncthing_machine.inventoryuuid = inventoryuuid
            new_Syncthing_machine.login = login
            new_Syncthing_machine.macadress = macadress
            new_Syncthing_machine.comment = comment
            new_Syncthing_machine.fk_arscluster = fk_arscluster
            session.add(new_Syncthing_machine)
            session.commit()
            session.flush()
            return new_Syncthing_machine.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def stat_syncthing_distributon(self, session, idgrp, idcmd, valuecount=[0, 100]):
        setvalues = " "
        if len(valuecount) != 0:
            setvalues = "AND xmppmaster.syncthing_machine.progress in (%s)" % ",".join(
                [str(x) for x in valuecount]
            )
        sql = """SELECT DISTINCT progress, COUNT(progress)
                    FROM
                        xmppmaster.syncthing_machine
                    WHERE
                        xmppmaster.syncthing_machine.group_uuid = %s
                        AND xmppmaster.syncthing_machine.command = %s
                        """ % (
            idgrp,
            idcmd,
        )
        sql = sql + setvalues + "\nGROUP BY progress ;"

        # print sql
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [(x[0], x[1]) for x in result]

    @DatabaseHelper._sessionm
    def stat_syncthing_transfert(self, session, idgrp, idcmd):
        ddistribution = self.stat_syncthing_distributon(idgrp, idcmd)
        distibution = {"nbvalue": len(ddistribution), "data_dist": ddistribution}

        sql = """SELECT
                    pathpackage,
                    COUNT(*) AS nb,
                    CAST((SUM(xmppmaster.syncthing_machine.progress) / COUNT(*)) AS CHAR) AS progress
                FROM
                    xmppmaster.syncthing_machine
                WHERE
                    xmppmaster.syncthing_machine.group_uuid = %s
                        AND xmppmaster.syncthing_machine.command = %s;
                        """ % (
            idgrp,
            idcmd,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        re = [x for x in result]
        re = re[0]
        if re[0] is None:
            return {
                "package": "",
                "nbmachine": 0,
                "progresstransfert": 0,
                "distibution": distibution,
            }
        try:
            progress = int(float(re[2]))
        except ValueError:
            progress = 0

        return {
            "package": re[0],
            "nbmachine": re[1],
            "progresstransfert": progress,
            "distibution": distibution,
        }

    @DatabaseHelper._sessionm
    def getnumcluster_for_ars(self, session, jidrelay):
        sql = (
            """SELECT
                    xmppmaster.has_cluster_ars.id_cluster
                FROM
                    xmppmaster.relayserver
                        INNER JOIN
                    xmppmaster.has_cluster_ars
                      ON `has_cluster_ars`.`id_ars` = xmppmaster.relayserver.id
                WHERE
                    `relayserver`.`jid` LIKE '%s'
                LIMIT 1;"""
            % jidrelay
        )
        # print "getnumclusterforars", sql
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result][0]

    @DatabaseHelper._sessionm
    def getCluster_deploy_syncthing(self, session, iddeploy):
        sql = (
            """SELECT
                    xmppmaster.syncthing_deploy_group.namepartage,
                    xmppmaster.syncthing_deploy_group.directory_tmp,
                    xmppmaster.syncthing_deploy_group.package,
                    xmppmaster.syncthing_ars_cluster.namecluster,
                    xmppmaster.syncthing_ars_cluster.arsmastercluster,
                    xmppmaster.syncthing_ars_cluster.numcluster,
                    xmppmaster.syncthing_machine.cluster,
                    xmppmaster.syncthing_deploy_group.grp_parent,
                    xmppmaster.syncthing_deploy_group.cmd,
                    xmppmaster.syncthing_deploy_group.id
                FROM
                    xmppmaster.syncthing_deploy_group
                        INNER JOIN
                    xmppmaster.syncthing_ars_cluster ON xmppmaster.syncthing_deploy_group.id = xmppmaster.syncthing_ars_cluster.fk_deploy
                        INNER JOIN
                    xmppmaster.syncthing_machine ON xmppmaster.syncthing_ars_cluster.id = xmppmaster.syncthing_machine.fk_arscluster
                WHERE
                    xmppmaster.syncthing_deploy_group.id = %s ;"""
            % iddeploy
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [y for y in [x for x in result]]

    @DatabaseHelper._sessionm
    def updateMachine_deploy_Syncthing(
        self, session, listidmachine, statusold=2, statusnew=3
    ):
        if isinstance(listidmachine, (int, str)):
            listidmachine = [listidmachine]
        if len(listidmachine) == 0:
            return
        listidmachine = ",".join([str(x) for x in listidmachine])

        sql = """UPDATE
                    xmppmaster.syncthing_machine
                SET
                    xmppmaster.syncthing_machine.syncthing = %s
                where
                    syncthing = %s
                    and
                    id in (%s);""" % (
            statusnew,
            statusold,
            listidmachine,
        )
        print(sql)
        result = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def getMachine_deploy_Syncthing(self, session, iddeploy, ars=None, status=None):
        sql = (
            """SELECT
                    xmppmaster.syncthing_machine.sessionid,
                    xmppmaster.syncthing_machine.jid_relay,
                    xmppmaster.syncthing_machine.jidmachine,
                    xmppmaster.machines.keysyncthing,
                    xmppmaster.syncthing_machine.result,
                    xmppmaster.syncthing_machine.id
                FROM
                    xmppmaster.syncthing_deploy_group
                        INNER JOIN
                    xmppmaster.syncthing_ars_cluster
                            ON xmppmaster.syncthing_deploy_group.id =
                                xmppmaster.syncthing_ars_cluster.fk_deploy
                        INNER JOIN
                    xmppmaster.syncthing_machine
                            ON xmppmaster.syncthing_ars_cluster.id =
                                xmppmaster.syncthing_machine.fk_arscluster
                        INNER JOIN
                    xmppmaster.machines
                            ON xmppmaster.machines.uuid_inventorymachine =
                                xmppmaster.syncthing_machine.inventoryuuid
                WHERE
                    xmppmaster.syncthing_deploy_group.id=%s """
            % iddeploy
        )
        if ars is not None:
            sql = (
                sql
                + """
            and
            xmppmaster.syncthing_machine.jid_relay like '%s' """
                % ars
            )
        if status is not None:
            sql = (
                sql
                + """
            and
            xmppmaster.syncthing_machine.syncthing = %s """
                % status
            )
        sql = sql + ";"
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    # =====================================================================
    # xmppmaster END FUNCTIONS deploy syncthing
    # =====================================================================

    @DatabaseHelper._sessionm
    def clusternum(self, session, jidars):
        jidars = jidars.split("/")[0]
        sql = (
            """SELECT
                    relayserver.jid,
                    xmppmaster.has_cluster_ars.id_cluster,
                    xmppmaster.cluster_ars.name
                FROM
                    xmppmaster.relayserver
                        INNER JOIN
                    xmppmaster.has_cluster_ars ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                        INNER JOIN
                    xmppmaster.cluster_ars ON xmppmaster.cluster_ars.id = xmppmaster.has_cluster_ars.id_cluster
                WHERE
                    xmppmaster.has_cluster_ars.id_cluster = (SELECT
                            has_cluster_ars.id_cluster
                        FROM
                            xmppmaster.relayserver
                                INNER JOIN
                            xmppmaster.has_cluster_ars ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                                INNER JOIN
                            xmppmaster.cluster_ars ON xmppmaster.cluster_ars.id = xmppmaster.has_cluster_ars.id_cluster
                        WHERE
                            relayserver.jid like '%s%%'
                            AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
                            LIMIT 1);"""
            % jidars
        )
        listars = session.execute(sql)
        session.commit()
        session.flush()
        cluster = {"ars": [], "numcluster": -1, "namecluster": "", "choose": ""}
        n = 0
        for z in listars:
            cluster["ars"].append(z[0])
            cluster["numcluster"] = z[1]
            cluster["namecluster"] = z[2]
            n = n + 1
            print("nb ars %s" % n)
        if n != 0:
            nb = random.randint(0, n - 1)
            cluster["choose"] = cluster["ars"][nb]
        return cluster

    @DatabaseHelper._sessionm
    def clusterlistars(self, session, enabled=1):
        sql = """SELECT
            GROUP_CONCAT(`jid`) AS 'listarsincluster',
            cluster_ars.name AS 'namecluster',
            cluster_ars.id AS 'numcluster',
            GROUP_CONCAT(`keysyncthing`) AS 'ksync'
        FROM
            xmppmaster.relayserver
                INNER JOIN
            xmppmaster.has_cluster_ars ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                INNER JOIN
            xmppmaster.cluster_ars ON xmppmaster.cluster_ars.id = xmppmaster.has_cluster_ars.id_cluster"""

        if enabled is not None:
            sql = """%s WHERE
            `relayserver`.`enabled` = %s
            AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)""" % (
                sql,
                enabled,
            )

        sql = sql + " GROUP BY xmppmaster.has_cluster_ars.id_cluster;"
        listars = session.execute(sql)
        session.commit()
        session.flush()
        cluster = {}
        for z in listars:
            if z[3] is None:
                za = ""
            else:
                za = z[3]
            cluster[z[2]] = {
                "listarscluster": z[0].split(","),
                "namecluster": z[1],
                "numcluster": z[2],
                "keysyncthing": za.split(","),
            }
        return cluster

    @DatabaseHelper._sessionm
    def chang_status_deploy_syncthing(self, session, datenow=None):
        if datenow is None:
            datenow = datetime.now()
        sql = (
            """ UPDATE `xmppmaster`.`deploy` SET `syncthing`='2'
                WHERE `startcmd`<= "%s" and syncthing = 1;"""
            % datenow
        )
        session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def change_end_deploy_syncthing(self, session, iddeploy, offsettime=60):
        dateend = datetime.now() + timedelta(minutes=offsettime)
        sql = """ UPDATE `xmppmaster`.`syncthing_deploy_group` SET `dateend`=%s
                WHERE `id`= "%s";""" % (
            dateend,
            iddeploy,
        )

        session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def deploy_machine_partage_exist(self, session, jidmachine, uidpackage):
        sql = """SELECT
                    *
                FROM
                    xmppmaster.syncthing_machine
                        INNER JOIN
                    xmppmaster.syncthing_ars_cluster ON xmppmaster.syncthing_ars_cluster.id = xmppmaster.syncthing_machine.fk_arscluster
                        INNER JOIN
                    xmppmaster.syncthing_deploy_group ON xmppmaster.syncthing_deploy_group.id = xmppmaster.syncthing_ars_cluster.fk_deploy
                WHERE
                    xmppmaster.syncthing_machine.jidmachine LIKE '%s'
                        AND xmppmaster.syncthing_deploy_group.package LIKE '%s'
                LIMIT 1;""" % (
            jidmachine,
            uidpackage,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def deploy_mach_exist_in_deploy(self, session, jidmachine, namepackage):
        pass

    @DatabaseHelper._sessionm
    def addcluster_resources(
        self,
        session,
        jidmachine,
        jidrelay,
        hostname,
        sessionid,
        login="",
        startcmd=None,
        endcmd=None,
    ):
        """
        add ressource for cluster ressource
        """
        try:
            new_cluster_resources = Cluster_resources()
            new_cluster_resources.jidmachine = jidmachine
            new_cluster_resources.jidrelay = jidrelay
            new_cluster_resources.hostname = hostname
            new_cluster_resources.sessionid = sessionid
            new_cluster_resources.login = login
            new_cluster_resources.startcmd = startcmd
            new_cluster_resources.endcmd = endcmd
            session.add(new_cluster_resources)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        return new_cluster_resources.id

    @DatabaseHelper._sessionm
    def getcluster_resources(self, session, jidmachine):
        clusterresources = (
            session.query(Cluster_resources)
            .filter(Cluster_resources.jidmachine == str(jidmachine))
            .all()
        )
        session.commit()
        session.flush()
        ret = {"len": len(clusterresources)}
        arraylist = []
        for t in clusterresources:
            obj = {}
            obj["jidmachine"] = t.jidmachine
            obj["jidrelay"] = t.jidrelay
            obj["hostname"] = t.hostname
            obj["sessionid"] = t.sessionid
            obj["login"] = t.login
            obj["startcmd"] = str(t.startcmd)
            obj["endcmd"] = str(t.endcmd)
            arraylist.append(obj)
        ret["resource"] = arraylist
        self.clean_resources(jidmachine)
        return ret

    @DatabaseHelper._sessionm
    def clean_resources(self, session, jidmachine):
        session.query(Cluster_resources).filter(
            Cluster_resources.jidmachine == str(jidmachine)
        ).delete()
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def delete_resources(self, session, sessionid):
        session.query(Cluster_resources).filter(
            Cluster_resources.sessionid == str(sessionid)
        ).delete()
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def getlinelogswolcmd(self, session, idcommand, uuid):
        log = (
            session.query(Logs)
            .filter(
                and_(
                    Logs.sessionname == str(idcommand),
                    Logs.type == "wol",
                    Logs.who == uuid,
                )
            )
            .order_by(Logs.id)
        )
        log = log.all()
        session.commit()
        session.flush()
        ret = {}
        ret["len"] = len(log)
        arraylist = []
        for t in log:
            obj = {}
            obj["type"] = t.type
            obj["date"] = t.date
            obj["text"] = t.text
            obj["sessionname"] = t.sessionname
            obj["priority"] = t.priority
            obj["who"] = t.who
            arraylist.append(obj)
        ret["log"] = arraylist
        return ret

    @DatabaseHelper._sessionm
    def get_machine_stop_deploy(self, session, cmdid, inventoryuuid):
        """
        this function return the machines list for  1 command_id and 1 uuid
        """
        query = session.query(Deploy).filter(
            and_(Deploy.inventoryuuid == inventoryuuid, Deploy.command == cmdid)
        )
        query = query.one()
        session.commit()
        session.flush()
        machine = {}
        machine["len"] = 0
        try:
            machine["len"] = 1
            machine["title"] = query.title
            machine["pathpackage"] = query.pathpackage
            machine["jid_relay"] = query.jid_relay
            machine["inventoryuuid"] = query.inventoryuuid
            machine["jidmachine"] = query.jidmachine
            machine["state"] = query.state
            machine["sessionid"] = query.sessionid
            machine["start"] = query.start
            machine["startcmd"] = query.startcmd
            machine["endcmd"] = query.endcmd
            machine["host"] = query.host
            machine["user"] = query.user
            machine["login"] = str(query.login)
            machine["command"] = query.command
            machine["group_uuid"] = query.group_uuid
            machine["macadress"] = query.macadress
            machine["syncthing"] = query.syncthing
        except Exception as e:
            logging.getLogger().error(str(e))
        return machine

    @DatabaseHelper._sessionm
    def get_group_stop_deploy(self, session, grpid, cmdid):
        """
        this function return the machines list for 1 group id and 1 command id
        """
        machine = session.query(Deploy).filter(
            and_(
                Deploy.group_uuid == grpid,
                Deploy.command == cmdid,
                not_(Deploy.sessionid.like("missingagent%")),
            )
        )
        machine = machine.all()
        session.commit()
        session.flush()
        ret = {}
        ret["len"] = len(machine)
        arraylist = []
        for t in machine:
            obj = {}
            obj["title"] = t.title
            obj["pathpackage"] = t.pathpackage
            obj["jid_relay"] = t.jid_relay
            obj["inventoryuuid"] = t.inventoryuuid
            obj["jidmachine"] = t.jidmachine
            obj["state"] = t.state
            obj["sessionid"] = t.sessionid
            obj["start"] = t.start
            obj["startcmd"] = t.startcmd
            obj["endcmd"] = t.endcmd
            obj["host"] = t.host
            obj["user"] = t.user
            obj["login"] = str(t.login)
            obj["command"] = t.command
            obj["group_uuid"] = t.group_uuid
            obj["macadress"] = t.macadress
            obj["syncthing"] = t.syncthing
            arraylist.append(obj)
        ret["objectdeploy"] = arraylist
        return ret

    @DatabaseHelper._sessionm
    def getstatdeployfromcommandidstartdate(self, session, command_id, datestart):
        try:
            machinedeploy = session.query(Deploy).filter(
                and_(Deploy.command == command_id, Deploy.startcmd == datestart)
            )
            totalmachinedeploy = self.get_count(machinedeploy)
            # count success deploy
            machinesuccessdeploy = self.get_count(
                machinedeploy.filter(and_(Deploy.state == "DEPLOYMENT SUCCESS"))
            )
            # count error deploy
            machineerrordeploy = self.get_count(
                machinedeploy.filter(and_(Deploy.state.startswith("ERROR")))
            )
            # count process deploy
            machineprocessdeploy = self.get_count(
                machinedeploy.filter(or_(Deploy.state.like("DEPLOYMENT START%%")))
            )
            # count abort deploy
            machineabortdeploy = self.get_count(
                machinedeploy.filter(and_(Deploy.state.startswith("ABORT")))
            )
            return {
                "totalmachinedeploy": totalmachinedeploy,
                "machinesuccessdeploy": machinesuccessdeploy,
                "machineerrordeploy": machineerrordeploy,
                "machineprocessdeploy": machineprocessdeploy,
                "machineabortdeploy": machineabortdeploy,
            }
        except Exception:
            return {
                "totalmachinedeploy": 0,
                "machinesuccessdeploy": 0,
                "machineerrordeploy": 0,
                "machineprocessdeploy": 0,
                "machineabortdeploy": 0,
            }

    @DatabaseHelper._sessionm
    def getdeployfromcommandid(self, session, command_id, uuid):
        if uuid == "UUID_NONE":
            relayserver = session.query(Deploy).filter(
                and_(Deploy.command == command_id)
            )
        else:
            relayserver = session.query(Deploy).filter(
                and_(Deploy.inventoryuuid == uuid, Deploy.command == command_id)
            )
        relayserver = relayserver.all()
        session.commit()
        session.flush()
        ret = {}
        ret["len"] = len(relayserver)
        arraylist = []
        for t in relayserver:
            obj = {}
            obj["pathpackage"] = t.pathpackage
            obj["jid_relay"] = t.jid_relay
            obj["inventoryuuid"] = t.inventoryuuid
            obj["jidmachine"] = t.jidmachine
            obj["state"] = t.state
            obj["sessionid"] = t.sessionid
            obj["start"] = t.start
            if t.result is None:
                obj["result"] = ""
            else:
                obj["result"] = t.result
            obj["host"] = t.host
            obj["user"] = t.user
            obj["login"] = str(t.login)
            obj["command"] = t.command
            arraylist.append(obj)
        ret["objectdeploy"] = arraylist
        return ret

    @DatabaseHelper._sessionm
    def getlinelogssession(self, session, sessionnamexmpp):
        log_type = "deploy"
        if re.search("update", sessionnamexmpp) is not None:
            log_type = "update"
        log = (
            session.query(Logs)
            .filter(and_(Logs.sessionname == sessionnamexmpp, Logs.type == log_type))
            .order_by(Logs.id)
        )
        log = log.all()
        session.commit()
        session.flush()
        ret = {}
        ret["len"] = len(log)
        arraylist = []
        for t in log:
            obj = {}
            obj["type"] = t.type
            obj["date"] = t.date
            obj["text"] = t.text
            obj["sessionname"] = t.sessionname
            obj["priority"] = t.priority
            obj["who"] = t.who
            arraylist.append(obj)
        ret["log"] = arraylist
        return ret

    @DatabaseHelper._sessionm
    def addlogincommand(
        self,
        session,
        login,
        commandid,
        grpid,
        nb_machine_in_grp,
        instructions_nb_machine_for_exec,
        instructions_datetime_for_exec,
        parameterspackage,
        rebootrequired,
        shutdownrequired,
        bandwidth,
        syncthing,
        params,
    ):
        try:
            new_logincommand = Has_login_command()
            try:
                new_logincommand.login = login
            except Exception:
                new_logincommand.login = "unknown"
            new_logincommand.command = commandid
            new_logincommand.count_deploy_progress = 0
            try:
                new_logincommand.bandwidth = int(bandwidth)
            except Exception:
                new_logincommand.bandwidth = 0
            if grpid != "":
                new_logincommand.grpid = grpid
            if instructions_datetime_for_exec != "":
                new_logincommand.start_exec_on_time = instructions_datetime_for_exec
            if nb_machine_in_grp != "":
                new_logincommand.nb_machine_for_deploy = nb_machine_in_grp
            if instructions_nb_machine_for_exec != "":
                new_logincommand.start_exec_on_nb_deploy = (
                    instructions_nb_machine_for_exec
                )
            if parameterspackage != "":
                new_logincommand.parameters_deploy = parameterspackage
            if rebootrequired == 0:
                new_logincommand.rebootrequired = False
            else:
                new_logincommand.rebootrequired = True
            if shutdownrequired == 0:
                new_logincommand.shutdownrequired = False
            else:
                new_logincommand.shutdownrequired = True
            if syncthing == 0:
                new_logincommand.syncthing = False
            else:
                new_logincommand.syncthing = True
            try:
                if isinstance(params, (list, dict)) and len(params) != 0:
                    new_logincommand.params_json = json.dumps(params)
            except Exception as e:
                logging.getLogger().error(
                    "We encountered an error. The error message is %s" % str(e)
                )
                logging.getLogger().error("Please, verify the parameters %s" % params)
            session.add(new_logincommand)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        return new_logincommand.id

    @DatabaseHelper._sessionm
    def getListPresenceRelay(self, session):
        sql = """SELECT
                    jid, agenttype, hostname
                FROM
                    xmppmaster.machines
                WHERE
                    `machines`.`agenttype` = 'relayserver';"""
        presencelist = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = []
            for t in presencelist:
                a.append({"jid": t[0], "type": t[1], "hostname": t[2]})
                logging.getLogger().debug("t %s" % t)
            # a = {"jid": x, for x, y ,z in presencelist}
            logging.getLogger().debug("a %s" % a)
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def deploylog(self, session, nblastline):
        """return les machines en fonction du RS"""
        sql = (
            """SELECT
                    *
                FROM
                    xmppmaster.deploy
                ORDER BY id DESC
                LIMIT %s;"""
            % nblastline
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def updatedeploystate1(self, session, sessionid, state):
        """
        Met à jour l'état de déploiement pour une session donnée en exécutant une requête SQL personnalisée.

        Paramètres :
            - session : Session SQLAlchemy. Une session SQLAlchemy préexistante pour la base de données.
            - sessionid : str. ID de la session de déploiement à mettre à jour.
            - state : str. Nouvel état du déploiement à enregistrer dans la base de données.

        Remarques :
            - Cette fonction met à jour l'état d'une session de déploiement spécifiée par "sessionid" avec le nouvel état "state".
            - Elle exécute une requête SQL personnalisée pour mettre à jour l'état, en utilisant les valeurs fournies par les
            paramètres "state" et "sessionid".
            - L'état ne sera mis à jour que si l'état précédent n'est pas "DEPLOYMENT SUCCESS", "ABORT DEPLOYMENT CANCELLED BY USER"
            ou s'il ne commence pas par "ERROR", "SUCCESS" ou "ABORT".
            - En cas de succès de la mise à jour, la fonction renvoie None. En cas d'erreur ou d'exception lors de l'exécution de la
            requête SQL, la fonction renverra -1 avec un message d'erreur approprié.

        Retour :
            - None : Mise à jour réussie de l'état du déploiement.
            - -1 : Erreur ou exception lors de la mise à jour de l'état du déploiement. Consultez les logs pour plus de détails.
        """

        try:
            sql = """UPDATE `xmppmaster`.`deploy`
                SET
                    `state` = '%s'
                WHERE
                    (deploy.sessionid = '%s'
                        AND ( `state` NOT IN ('DEPLOYMENT SUCCESS' ,
                                              'ABORT DEPLOYMENT CANCELLED BY USER')
                                OR
                              `state` REGEXP '^(?!ERROR)^(?!SUCCESS)^(?!ABORT)'));
                """ % (
                state,
                sessionid,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def updatemachineAD(self, session, idmachine, lastuser, ou_machine, ou_user):
        """
        update Machine table in base xmppmaster
        data AD
        """
        try:
            session.query(Machines).filter(Machines.id == idmachine).update(
                {
                    Machines.ad_ou_machine: ou_machine,
                    Machines.ad_ou_user: ou_user,
                    Machines.lastuser: lastuser,
                }
            )
            session.commit()
            session.flush()
            return 1
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def updatedeploystate(self, session, sessionid, state):
        """
        Met à jour l'état de déploiement pour une session donnée.

        Paramètres :
            - session : Session SQLAlchemy. Une session SQLAlchemy préexistante pour la base de données. (session fournie par le decorateur)
            - sessionid : str. ID de la session de déploiement à mettre à jour.
            - state : str. Nouvel état du déploiement à enregistrer dans la base de données.

        Remarques :
            - Cette fonction met à jour l'état d'une session de déploiement spécifiée par "sessionid" avec le nouvel état "state".
            - Elle vérifie que l'état ne commence pas par "abort", "success" ou "error", car ces états ne peuvent pas être modifiés
            ultérieurement.
            - Si l'état est "DEPLOYMENT PENDING (REBOOT/SHUTDOWN/...)", la fonction vérifie si l'état précédent était l'un des états
            ["WOL 1", "WOL 2", "WOL 3", "WAITING MACHINE ONLINE"]. Si c'est le cas, l'état ne change pas.
            - Si la requête SQLAlchemy ne retourne pas de résultat, la fonction renverra -1 avec un message d'erreur approprié.
            - Si la requête SQLAlchemy retourne plusieurs résultats, la fonction renverra -1 avec un message d'erreur approprié.

        Retour :
            - 1 : Mise à jour réussie de l'état du déploiement.
            - 0 : Aucun changement d'état nécessaire car l'état est "DEPLOYMENT PENDING (REBOOT/SHUTDOWN/...)" et l'état précédent
                était l'un des états ["WOL 1", "WOL 2", "WOL 3", "WAITING MACHINE ONLINE"].
            - -1 : Erreur ou exception lors de la mise à jour de l'état du déploiement. Consultez les logs pour plus de détails.
        """

        try:
            # on peut lever 2 exceptions
            deploysession = (
                session.query(Deploy).filter(Deploy.sessionid == sessionid).one()
            )
            if deploysession:
                # les status commençant par error, success, abort ne peuvent plus être modifiés.
                # L'expression régulière ^(?!abort) est une expression régulière en utilisant la syntaxe des assertions (lookahead) négatives pour rechercher des lignes
                #    qui ne commencent pas par le mot "abort".
                # Explication :
                #  ^ : C'est un ancrage qui signifie que l'expression régulière doit rechercher le début de la ligne.
                #  (?!abort) : C'est une assertion négative (negative lookahead). L'expression régulière suivante ne doit pas être "abort" pour qu'il y ait correspondance.
                # | ou entre les preposition de l'expression complete.
                regexpexlusion = re.compile(
                    "^(?!abort)^(?!success)^(?!error)", re.IGNORECASE
                )
                if regexpexlusion.match(state) is None:
                    return
                if state == "DEPLOYMENT PENDING (REBOOT/SHUTDOWN/...)":
                    if deploysession.state in [
                        "WOL 1",
                        "WOL 2",
                        "WOL 3",
                        "WAITING MACHINE ONLINE",
                    ]:
                        # STATUS NE CHANGE PAS
                        return 0
                # update status
                deploysession.state = state
                session.commit()
                session.flush()
                return 1
        except MultipleResultsFound:
            logging.getLogger().error(
                "Several deployments have the same sessionid %s" % sessionid
            )
            return -1
        except NoResultFound:
            logging.getLogger().error(
                "No deployment found having session %s" % sessionid
            )
            return -1
        except Exception:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
            return -1
        return -1

    @DatabaseHelper._sessionm
    def delNetwork_for_machines_id(self, session, machines_id):
        sql = (
            """DELETE FROM `xmppmaster`.`network`
                WHERE
                    (`machines_id` = '%s');"""
            % machines_id
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return result

    @DatabaseHelper._sessionm
    def addPresenceNetwork(
        self, session, macaddress, ipaddress, broadcast, gateway, mask, mac, id_machine
    ):
        # self.delNetwork_for_machines_id(id_machine)
        try:
            new_network = Network()
            if broadcast is None or broadcast == "None":
                broadcast = ""
            if isinstance(mask, str):
                mask = mask.strip()
            if isinstance(ipaddress, str):
                ipaddress = ipaddress.strip()
            if isinstance(broadcast, str):
                broadcast = broadcast.strip()
            if isinstance(macaddress, str):
                macaddress = macaddress.strip()
            if isinstance(gateway, str):
                gateway = gateway.strip()
            new_network.macaddress = macaddress
            new_network.ipaddress = ipaddress
            if not broadcast and mask and ipaddress:
                netmask_bits = IPAddress(mask).netmask_bits()
                CIDR = IPNetwork("%s/%s" % (ipaddress, netmask_bits))
                broadcast = CIDR.broadcast
                if broadcast is None or broadcast == "None":
                    broadcast = ""
            new_network.broadcast = broadcast
            new_network.gateway = gateway
            new_network.mask = mask
            new_network.mac = mac
            new_network.machines_id = id_machine
            session.add(new_network)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error("\n%s" % (traceback.format_exc()))
            logging.getLogger().error("add Presence Network : %s " % str(e))

    @DatabaseHelper._sessionm
    def addServerRelay(
        self,
        session,
        urlguacamole,
        subnet,
        nameserver,
        groupdeploy,
        ipserver,
        ipconnection,
        portconnection,
        port,
        mask,
        jid,
        longitude="",
        latitude="",
        enabled=False,
        classutil="private",
        packageserverip="",
        packageserverport="",
        moderelayserver="static",
        keysyncthing="",
        syncthing_port=23000,
    ):
        sql = (
            "SELECT count(*) as nb FROM xmppmaster.relayserver where "
            "`relayserver`.`nameserver`='%s';" % nameserver
        )
        nb = session.execute(sql)
        session.commit()
        session.flush()
        result = [x for x in nb][0][0]
        if result == 0:
            try:
                new_relayserver = RelayServer()
                new_relayserver.urlguacamole = urlguacamole
                new_relayserver.subnet = subnet
                new_relayserver.nameserver = nameserver
                new_relayserver.groupdeploy = groupdeploy
                new_relayserver.ipserver = ipserver
                new_relayserver.port = port
                new_relayserver.mask = mask
                new_relayserver.jid = jid
                new_relayserver.ipconnection = ipconnection
                new_relayserver.portconnection = portconnection
                new_relayserver.longitude = longitude
                new_relayserver.latitude = latitude
                new_relayserver.enabled = enabled
                new_relayserver.classutil = classutil
                new_relayserver.package_server_ip = packageserverip
                new_relayserver.package_server_port = packageserverport
                new_relayserver.moderelayserver = moderelayserver
                new_relayserver.keysyncthing = keysyncthing
                new_relayserver.syncthing_port = syncthing_port
                session.add(new_relayserver)
                session.commit()
                session.flush()
            except Exception as e:
                logging.getLogger().error(str(e))
        else:
            try:
                sql = (
                    "UPDATE `xmppmaster`.`relayserver`\
                        SET `enabled`=%s, `classutil`='%s'\
                      WHERE `xmppmaster`.`relayserver`.`nameserver`='%s';"
                    % (enabled, classutil, nameserver)
                )
                session.execute(sql)
                session.commit()
                session.flush()
            except Exception as e:
                logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def getCountPresenceMachine(self, session):
        return session.query(func.count(Machines.id)).scalar()

    def _iso_8859_1__to__utf8(self, strdata):
        try:
            strdata = bytes(strdata, "iso-8859-1").decode("utf8")
        except Exception:
            return strdata

    @DatabaseHelper._sessionm
    def adduser(
        self,
        session,
        namesession,
        hostname,
        city="",
        region_name="",
        time_zone="",
        longitude="",
        latitude="",
        postal_code="",
        country_code="",
        country_name="",
        creation_user="",
        last_modif="",
    ):
        sql = (
            "SELECT count(*) as nb FROM xmppmaster.users where "
            "`users`.`namesession`='%s' and `users`.`hostname`='%s';"
            % (namesession, hostname)
        )
        city = self._iso_8859_1__to__utf8(city)
        region_name = self._iso_8859_1__to__utf8(region_name)
        time_zone = self._iso_8859_1__to__utf8(time_zone)
        postal_code = self._iso_8859_1__to__utf8(postal_code)
        country_code = self._iso_8859_1__to__utf8(country_code)
        country_name = self._iso_8859_1__to__utf8(country_name)
        createuser = datetime.now()
        try:
            nb = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        result = [x for x in nb][0][0]
        if result == 0:
            try:
                new_user = Users()
                new_user.namesession = namesession
                new_user.hostname = hostname
                new_user.city = city
                new_user.region_name = region_name
                new_user.time_zone = time_zone
                new_user.longitude = longitude
                new_user.latitude = latitude
                new_user.postal_code = postal_code
                new_user.country_code = country_code
                new_user.country_name = country_name
                new_user.creation_user = createuser
                new_user.last_modif = createuser
                session.add(new_user)
                session.commit()
                session.flush()
                return new_user.id
            except Exception as e:
                logging.getLogger().error(str(e))
                return -1
        else:
            try:
                session.query(Users).filter(Users.hostname == hostname).update(
                    {
                        Users.city: city,
                        Users.region_name: region_name,
                        Users.time_zone: time_zone,
                        Users.longitude: longitude,
                        Users.latitude: latitude,
                        Users.postal_code: postal_code,
                        Users.country_code: country_code,
                        Users.country_name: country_name,
                        Users.last_modif: createuser,
                    }
                )
                session.commit()
                session.flush()
                sql = (
                    "select id from `xmppmaster`.`users` WHERE `xmppmaster`.`users`.`hostname`='%s';"
                    % hostname
                )
                result = session.execute(sql)
                result = [x for x in result][0]
                session.commit()
                session.flush()
                return result
            except Exception as e:
                logging.getLogger().error(str(e))
        return -1

    def get_count(self, q):
        count_q = q.statement.with_only_columns([func.count()]).order_by(None)
        count = q.session.execute(count_q).scalar()
        return count

    def get_count1(self, q):
        return q.with_entities(func.count()).scalar()

    @DatabaseHelper._sessionm
    def getdeploybyuserlen(self, session, login=None, typedeploy="command"):
        deploybyuserlen = session.query(Deploy).filter(
            Deploy.sessionid.like("%s%%" % (typedeploy))
        )
        if login is not None:
            return self.get_count(deploybyuserlen.filter(Deploy.login == login))
        else:
            return self.get_count(deploybyuserlen)

    @DatabaseHelper._sessionm
    def getLogxmpp(
        self,
        session,
        start_date,
        end_date,
        typelog,
        action,
        module,
        user,
        how,
        who,
        why,
        headercolumn,
    ):
        logs = session.query(Logs)
        if headercolumn == "":
            headercolumn = "date@fromuser@who@text"

        if start_date != "":
            logs = logs.filter(Logs.date > start_date)
        if end_date != "":
            logs = logs.filter(Logs.date < end_date)
        if not (typelog == "None" or typelog == ""):
            logs = logs.filter(Logs.type == typelog)
        if not (action == "None" or action == ""):
            logs = logs.filter(Logs.action == action)
        if not (module == "None" or module == ""):
            # plusieurs criteres peuvent se trouver dans ce parametre.
            criterformodule = [
                x.strip() for x in module.split("|") if x.strip() != "" and x != "None"
            ]
            for x in criterformodule:
                stringsearchinmodule = "%" + x + "%"
                logs = logs.filter(Logs.module.like(stringsearchinmodule))
        if not (user == "None" or user == ""):
            logs = logs.filter(func.lower(Logs.fromuser).like(func.lower(user)))
        if not (how == "None" or how == ""):
            logs = logs.filter(Logs.how == how)
        if not (who == "None" or who == ""):
            logs = logs.filter(Logs.who == who)
        if not (why == "None" or why == ""):
            logs = logs.filter(Logs.why == why)
        logs = logs.order_by(desc(Logs.id)).limit(1000)
        result = logs.all()
        session.commit()
        session.flush()
        ret = {"data": []}
        index = 0
        for linelogs in result:
            listchamp = []
            # listchamp.append(index)
            if headercolumn != "" and "date" in headercolumn:
                listchamp.append(str(linelogs.date))
            if headercolumn != "" and "fromuser" in headercolumn:
                listchamp.append(linelogs.fromuser)
            if headercolumn != "" and "type" in headercolumn:
                listchamp.append(linelogs.type)
            if headercolumn != "" and "action" in headercolumn:
                listchamp.append(linelogs.action)
            if headercolumn != "" and "module" in headercolumn:
                listchamp.append(linelogs.module)
            if headercolumn != "" and "how" in headercolumn:
                listchamp.append(linelogs.how)
            if headercolumn != "" and "who" in headercolumn:
                listchamp.append(linelogs.who)
            if headercolumn != "" and "why" in headercolumn:
                listchamp.append(linelogs.why)
            if headercolumn != "" and "priority" in headercolumn:
                listchamp.append(linelogs.priority)
            if headercolumn != "" and "touser" in headercolumn:
                listchamp.append(linelogs.touser)
            if headercolumn != "" and "sessionname" in headercolumn:
                listchamp.append(linelogs.sessionname)
            if headercolumn != "" and "text" in headercolumn:
                listchamp.append(linelogs.text)
            ret["data"].append(listchamp)
        return ret

    @DatabaseHelper._sessionm
    def getdeploybymachinegrprecent(
        self, session, group_uuid, state, duree, min, max, filt
    ):
        deploylog = session.query(Deploy)
        if group_uuid:
            deploylog = deploylog.filter(Deploy.group_uuid == group_uuid)
        if duree:
            deploylog = deploylog.filter(
                Deploy.start >= (datetime.now() - timedelta(seconds=duree))
            )
        if state:
            deploylog = deploylog.filter(Deploy.state == state)

        nb = self.get_count(deploylog)
        lentaillerequette = session.query(func.count(distinct(Deploy.title)))[0]
        deploylog = deploylog.order_by(desc(Deploy.id))

        nb = self.get_count(deploylog)
        if min and max:
            deploylog = deploylog.offset(int(min)).limit(int(max) - int(min))

        result = deploylog.all()
        session.commit()
        session.flush()
        ret = {
            "lentotal": 0,
            "lenquery": 0,
            "tabdeploy": {
                "len": [],
                "state": [],
                "pathpackage": [],
                "sessionid": [],
                "start": [],
                "inventoryuuid": [],
                "command": [],
                "login": [],
                "host": [],
                "macadress": [],
                "group_uuid": [],
                "startcmd": [],
                "endcmd": [],
                "jidmachine": [],
                "jid_relay": [],
                "title": [],
            },
        }
        ret["lentotal"] = lentaillerequette[0]
        ret["lenquery"] = nb
        for linedeploy in result:
            ret["tabdeploy"]["state"].append(linedeploy.state)
            ret["tabdeploy"]["pathpackage"].append(
                linedeploy.pathpackage.split("/")[-1]
            )
            ret["tabdeploy"]["sessionid"].append(linedeploy.sessionid)
            ret["tabdeploy"]["start"].append(str(linedeploy.start))
            ret["tabdeploy"]["inventoryuuid"].append(linedeploy.inventoryuuid)
            ret["tabdeploy"]["command"].append(linedeploy.command)
            ret["tabdeploy"]["login"].append(linedeploy.login)
            ret["tabdeploy"]["host"].append(linedeploy.host.split("@")[0][:-4])
            ret["tabdeploy"]["macadress"].append(linedeploy.macadress)
            if linedeploy.group_uuid is None:
                linedeploy.group_uuid = ""
            ret["tabdeploy"]["group_uuid"].append(linedeploy.group_uuid)
            ret["tabdeploy"]["startcmd"].append(linedeploy.startcmd)
            ret["tabdeploy"]["endcmd"].append(linedeploy.endcmd)
            ret["tabdeploy"]["jidmachine"].append(linedeploy.jidmachine)
            ret["tabdeploy"]["jid_relay"].append(linedeploy.jid_relay)
            ret["tabdeploy"]["title"].append(linedeploy.title)
        return ret

    @DatabaseHelper._sessionm
    def getdeploybymachinerecent(
        self, session, uuidinventory, state, duree, min, max, filt
    ):
        deploylog = session.query(Deploy)
        if uuidinventory:
            deploylog = deploylog.filter(Deploy.inventoryuuid == uuidinventory)
        if duree:
            deploylog = deploylog.filter(
                Deploy.start >= (datetime.now() - timedelta(seconds=duree))
            )
        if state:
            deploylog = deploylog.filter(Deploy.state == state)

        nb = self.get_count(deploylog)

        lentaillerequette = session.query(func.count(distinct(Deploy.title)))[0]
        deploylog = deploylog.order_by(desc(Deploy.id))

        nb = self.get_count(deploylog)
        if min and max:
            deploylog = deploylog.offset(int(min)).limit(int(max) - int(min))
        result = deploylog.all()
        session.commit()
        session.flush()
        ret = {
            "lentotal": 0,
            "lenquery": 0,
            "tabdeploy": {
                "len": [],
                "state": [],
                "pathpackage": [],
                "sessionid": [],
                "start": [],
                "inventoryuuid": [],
                "command": [],
                "login": [],
                "host": [],
                "macadress": [],
                "group_uuid": [],
                "startcmd": [],
                "endcmd": [],
                "jidmachine": [],
                "jid_relay": [],
                "title": [],
            },
        }
        ret["lentotal"] = lentaillerequette[0]
        ret["lenquery"] = nb
        for linedeploy in result:
            ret["tabdeploy"]["state"].append(linedeploy.state)
            ret["tabdeploy"]["pathpackage"].append(
                linedeploy.pathpackage.split("/")[-1]
            )
            ret["tabdeploy"]["sessionid"].append(linedeploy.sessionid)
            ret["tabdeploy"]["start"].append(str(linedeploy.start))
            ret["tabdeploy"]["inventoryuuid"].append(linedeploy.inventoryuuid)
            ret["tabdeploy"]["command"].append(linedeploy.command)
            ret["tabdeploy"]["login"].append(linedeploy.login)
            ret["tabdeploy"]["host"].append(linedeploy.host.split("/")[-1])
            ret["tabdeploy"]["macadress"].append(linedeploy.macadress)
            ret["tabdeploy"]["group_uuid"].append(linedeploy.group_uuid)
            ret["tabdeploy"]["startcmd"].append(linedeploy.startcmd)
            ret["tabdeploy"]["endcmd"].append(linedeploy.endcmd)
            ret["tabdeploy"]["jidmachine"].append(linedeploy.jidmachine)
            ret["tabdeploy"]["jid_relay"].append(linedeploy.jid_relay)
            ret["tabdeploy"]["title"].append(linedeploy.title)
        return ret

    @DatabaseHelper._sessionm
    def delDeploybygroup(self, session, numgrp):
        """
        creation d'une organization
        """
        session.query(Deploy).filter(Deploy.group_uuid == numgrp).delete()
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def getdeploybyuserrecent(
        self,
        session,
        login,
        state,
        duree,
        min=None,
        max=None,
        filt=None,
        typedeploy="command",
    ):
        deploylog = session.query(Deploy).filter(
            Deploy.sessionid.like("%s%%" % (typedeploy))
        )
        if login:
            deploylog = deploylog.filter(Deploy.login == login)
        if state:
            deploylog = deploylog.filter(Deploy.state == state)

        if duree:
            deploylog = deploylog.filter(
                Deploy.start >= (datetime.now() - timedelta(seconds=duree))
            )

        count = """select count(*) as nb from (
        select count(id) as nb
        from deploy
        where
            sessionid like "%s%%" AND
            start >= DATE_SUB(NOW(),INTERVAL 24 HOUR)
        group by title
        ) as x;""" % (
            typedeploy
        )

        if filt is not None:
            deploylog = deploylog.filter(
                or_(
                    Deploy.state.like("%%%s%%" % (filt)),
                    Deploy.pathpackage.like("%%%s%%" % (filt)),
                    Deploy.start.like("%%%s%%" % (filt)),
                    Deploy.login.like("%%%s%%" % (filt)),
                    Deploy.host.like("%%%s%%" % (filt)),
                )
            )
            count = """select count(*) as nb from (
              select count(id) as nb
              from deploy
              where
                    sessionid like "%s%%" AND
                    start >= DATE_SUB(NOW(),INTERVAL 24 HOUR)
              AND (state LIKE "%%%s%%"
              or pathpackage LIKE "%%%s%%"
              or start LIKE "%%%s%%"
              or login LIKE "%%%s%%"
              or host LIKE "%%%s%%"
              )
              group by title
              ) as x;""" % (
                typedeploy,
                filt,
                filt,
                filt,
                filt,
                filt,
            )

        lentaillerequette = self.get_count(deploylog)

        result = session.execute(count)
        session.commit()
        session.flush()
        lenrequest = [x for x in result]

        deploylog = deploylog.group_by(Deploy.title)

        deploylog = deploylog.order_by(desc(Deploy.id))

        if min is not None and max is not None:
            deploylog = deploylog.offset(int(min)).limit(int(max) - int(min))
        result = deploylog.all()
        session.commit()
        session.flush()
        ret = {
            "total_of_rows": 0,
            "lentotal": 0,
            "tabdeploy": {
                "state": [],
                "pathpackage": [],
                "sessionid": [],
                "start": [],
                "inventoryuuid": [],
                "command": [],
                "login": [],
                "host": [],
                "macadress": [],
                "group_uuid": [],
                "startcmd": [],
                "endcmd": [],
                "jidmachine": [],
                "jid_relay": [],
                "title": [],
            },
        }

        ret["lentotal"] = lentaillerequette  # [0]
        ret["total_of_rows"] = lenrequest[0][0]
        for linedeploy in result:
            ret["tabdeploy"]["state"].append(linedeploy.state)
            ret["tabdeploy"]["pathpackage"].append(
                linedeploy.pathpackage.split("/")[-1]
            )
            ret["tabdeploy"]["sessionid"].append(linedeploy.sessionid)
            ret["tabdeploy"]["start"].append(str(linedeploy.start))
            ret["tabdeploy"]["inventoryuuid"].append(linedeploy.inventoryuuid)
            ret["tabdeploy"]["command"].append(linedeploy.command)
            ret["tabdeploy"]["login"].append(linedeploy.login)
            ret["tabdeploy"]["host"].append(linedeploy.host.split("@")[0][:-4])
            ret["tabdeploy"]["macadress"].append(linedeploy.macadress)
            ret["tabdeploy"]["group_uuid"].append(linedeploy.group_uuid)
            ret["tabdeploy"]["startcmd"].append(linedeploy.startcmd)
            ret["tabdeploy"]["endcmd"].append(linedeploy.endcmd)
            ret["tabdeploy"]["jidmachine"].append(linedeploy.jidmachine)
            ret["tabdeploy"]["jid_relay"].append(linedeploy.jid_relay)
            ret["tabdeploy"]["title"].append(linedeploy.title)
        return ret

    @DatabaseHelper._sessionm
    def getRelayServerfromjiddomain(self, session, jiddomain):
        relayserver = session.query(RelayServer).filter(
            RelayServer.jid.like("%%@%s/%%" % jiddomain)
        )
        relayserver = relayserver.first()
        session.commit()
        session.flush()
        try:
            result = {
                "id": relayserver.id,
                "urlguacamole": relayserver.urlguacamole,
                "subnet": relayserver.subnet,
                "nameserver": relayserver.nameserver,
                "ipserver": relayserver.ipserver,
                "ipconnection": relayserver.ipconnection,
                "port": relayserver.port,
                "portconnection": relayserver.portconnection,
                "mask": relayserver.mask,
                "jid": relayserver.jid,
                "longitude": relayserver.longitude,
                "latitude": relayserver.latitude,
                "enabled": relayserver.enabled,
                "switchonoff": relayserver.switchonoff,
                "mandatory": relayserver.mandatory,
                "classutil": relayserver.classutil,
                "groupdeploy": relayserver.groupdeploy,
                "package_server_ip": relayserver.package_server_ip,
                "package_server_port": relayserver.package_server_port,
                "moderelayserver": relayserver.moderelayserver,
                "keysyncthing": relayserver.keysyncthing,
                "syncthing_port": relayserver.syncthing_port,
            }
        except Exception:
            result = {}
        return result

    @DatabaseHelper._sessionm
    def getdeploybyuserpast(
        self, session, login, duree, min=None, max=None, filt=None, typedeploy="command"
    ):
        deploylog = session.query(Deploy).filter(
            Deploy.sessionid.like("%s%%" % (typedeploy))
        )
        if login:
            deploylog = deploylog.filter(Deploy.login == login)

        if duree:
            deploylog = deploylog.filter(
                Deploy.start >= (datetime.now() - timedelta(seconds=duree))
            )

        if filt is not None:
            deploylog = deploylog.filter(
                or_(
                    Deploy.state.like("%%%s%%" % (filt)),
                    Deploy.pathpackage.like("%%%s%%" % (filt)),
                    Deploy.start.like("%%%s%%" % (filt)),
                    Deploy.login.like("%%%s%%" % (filt)),
                    Deploy.host.like("%%%s%%" % (filt)),
                )
            )

        deploylog = deploylog.filter(
            or_(
                Deploy.state == "DEPLOYMENT SUCCESS",
                Deploy.state.startswith("ERROR"),
                Deploy.state.startswith("ABORT"),
            )
        )

        lentaillerequette = session.query(func.count(distinct(Deploy.title)))[0]
        deploylog = deploylog.group_by(Deploy.title)

        deploylog = deploylog.order_by(desc(Deploy.id))

        nbfilter = self.get_count(deploylog)

        if min is not None and max is not None:
            deploylog = deploylog.offset(int(min)).limit(int(max) - int(min))
        result = deploylog.all()
        session.commit()
        session.flush()
        ret = {
            "lentotal": 0,
            "tabdeploy": {
                "len": [],
                "state": [],
                "pathpackage": [],
                "sessionid": [],
                "start": [],
                "inventoryuuid": [],
                "command": [],
                "login": [],
                "host": [],
                "macadress": [],
                "group_uuid": [],
                "startcmd": [],
                "endcmd": [],
                "jidmachine": [],
                "jid_relay": [],
                "title": [],
            },
        }

        ret["lentotal"] = lentaillerequette[0]
        for linedeploy in result:
            ret["tabdeploy"]["state"].append(linedeploy.state)
            ret["tabdeploy"]["pathpackage"].append(
                linedeploy.pathpackage.split("/")[-1]
            )
            ret["tabdeploy"]["sessionid"].append(linedeploy.sessionid)
            ret["tabdeploy"]["start"].append(str(linedeploy.start))
            ret["tabdeploy"]["inventoryuuid"].append(linedeploy.inventoryuuid)
            ret["tabdeploy"]["command"].append(linedeploy.command)
            ret["tabdeploy"]["login"].append(linedeploy.login)
            ret["tabdeploy"]["host"].append(linedeploy.host.split("/")[-1])
            ret["tabdeploy"]["macadress"].append(linedeploy.macadress)
            ret["tabdeploy"]["group_uuid"].append(linedeploy.group_uuid)
            ret["tabdeploy"]["startcmd"].append(linedeploy.startcmd)
            ret["tabdeploy"]["endcmd"].append(linedeploy.endcmd)
            ret["tabdeploy"]["jidmachine"].append(linedeploy.jidmachine)
            ret["tabdeploy"]["jid_relay"].append(linedeploy.jid_relay)
            ret["tabdeploy"]["title"].append(linedeploy.title)
        return ret

    @DatabaseHelper._sessionm
    def getdeploybyuser(
        self, session, login=None, numrow=None, offset=None, typedeploy="command"
    ):
        deploylog = session.query(Deploy).filter(
            Deploy.sessionid.like("%s%%" % (typedeploy))
        )
        if login is not None:
            deploylog = deploylog.filter(Deploy.login == login).order_by(
                desc(Deploy.id)
            )
        else:
            deploylog = deploylog.order_by(desc(Deploy.id))
        if numrow is not None:
            deploylog = deploylog.limit(numrow)
            if offset is not None:
                deploylog = deploylog.offset(offset)
        deploylog = deploylog.all()
        session.commit()
        session.flush()
        ret = {
            "len": len(deploylog),
            "tabdeploy": {
                "state": [],
                "pathpackage": [],
                "sessionid": [],
                "start": [],
                "inventoryuuid": [],
                "command": [],
                "login": [],
                "host": [],
            },
        }
        for linedeploy in deploylog:
            ret["tabdeploy"]["state"].append(linedeploy.state)
            ret["tabdeploy"]["pathpackage"].append(
                linedeploy.pathpackage.split("/")[-1]
            )
            ret["tabdeploy"]["sessionid"].append(linedeploy.sessionid)
            d = linedeploy.start.strftime("%Y-%m-%d %H:%M")
            dd = str(linedeploy.start.strftime("%Y-%m-%d %H:%M"))
            ret["tabdeploy"]["start"].append(dd)
            ret["tabdeploy"]["inventoryuuid"].append(linedeploy.inventoryuuid)
            ret["tabdeploy"]["command"].append(linedeploy.command)
            ret["tabdeploy"]["login"].append(linedeploy.login)
            ret["tabdeploy"]["start"].append(linedeploy.start)
            ret["tabdeploy"]["host"].append(linedeploy.host.split("/")[-1])
        return ret

    @DatabaseHelper._sessionm
    def showmachinegrouprelayserver(self, session):
        """return les machines en fonction du RS"""
        sql = """SELECT
                `jid`, `agenttype`, `platform`, `groupdeploy`, `hostname`, `uuid_inventorymachine`, `ip_xmpp`, `subnetxmpp`
            FROM
                xmppmaster.machines
            order BY `groupdeploy` ASC, `agenttype` DESC;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def get_qaction(self, session, namecmd, user, grp):
        """
        return quick actions informations
        """
        if grp == 0:
            qa_custom_command = session.query(Qa_custom_command).filter(
                and_(
                    Qa_custom_command.namecmd == namecmd, Qa_custom_command.user == user
                )
            )
            qa_custom_command = qa_custom_command.first()
        else:
            qa_custom_command = session.query(Qa_custom_command).filter(
                and_(
                    Qa_custom_command.customcmd == namecmd,
                    or_(
                        Qa_custom_command.user == user,
                        Qa_custom_command.user == "allusers",
                    ),
                )
            )
            qa_custom_command = qa_custom_command.first()
        if qa_custom_command:
            result = {
                "user": qa_custom_command.user,
                "os": qa_custom_command.os,
                "namecmd": qa_custom_command.namecmd,
                "customcmd": qa_custom_command.customcmd,
                "description": qa_custom_command.description,
            }
            return result
        else:
            result = {}

    @DatabaseHelper._sessionm
    def listjidRSdeploy(self, session):
        """return les RS pour le deploiement"""
        sql = """SELECT
                    groupdeploy
                FROM
                    xmppmaster.machines
                WHERE
                    machines.agenttype = 'relayserver';"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def listmachinesfromRSdeploy(self, session, groupdeploy):
        """return les machine suivie par un RS"""
        sql = (
            """SELECT
                    *
                FROM
                    xmppmaster.machines
                WHERE
                    machines.agenttype = 'machine'
                        AND machines.groupdeploy = '%s';"""
            % groupdeploy
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def listmachinesfromdeploy(self, session, groupdeploy):
        """return toutes les machines pour un deploy"""
        sql = (
            """SELECT
                        *
                    FROM
                        xmppmaster.machines
                    WHERE
                    machines.groupdeploy = '%s'
                    order BY  `agenttype` DESC;"""
            % groupdeploy
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def ipfromjid(self, session, jid, enable=1):
        """return ip xmpp for JID"""
        user = str(jid).split("@")[0]
        if enable is None:
            sql = (
                """SELECT
                        ip_xmpp
                    FROM
                        xmppmaster.machines
                    WHERE
                        jid LIKE ('%s%%')
                                    LIMIT 1;"""
                % user
            )
        else:
            sql = """SELECT
                        ip_xmpp
                    FROM
                        xmppmaster.machines
                    WHERE
                        enabled = '%s' and
                        jid LIKE ('%s%%')
                                    LIMIT 1;""" % (
                enable,
                user,
            )

        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def groupdeployfromjid(self, session, jid):
        """return groupdeploy xmpp for JID"""
        user = str(jid).split("@")[0]
        sql = (
            """SELECT
                    groupdeploy
                FROM
                    xmppmaster.machines
                WHERE
                    jid LIKE ('%s%%')
                                LIMIT 1;"""
            % user
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def ippackageserver(self, session, jid):
        """return ip xmpp for JID"""
        user = str(jid).split("@")[0]
        sql = (
            """SELECT
                    package_server_ip
                FROM
                    xmppmaster.relayserver
                WHERE
                    jid LIKE ('%s@%%')
                                LIMIT 1;"""
            % user
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def portpackageserver(self, session, jid):
        """return ip xmpp for JID"""
        user = str(jid).split("@")[0]
        sql = (
            """SELECT
                    package_server_port
                FROM
                    xmppmaster.relayserver
                WHERE
                    jid LIKE ('%s%%')
                                LIMIT 1;"""
            % user
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def ipserverARS(self, session, jid):
        """return ip xmpp for JID"""
        user = str(jid).split("@")[0]
        sql = (
            """SELECT
                    ipserver
                FROM
                    xmppmaster.relayserver
                WHERE
                    jid LIKE ('%s%%')
                                LIMIT 1;"""
            % user
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def getUuidFromJid(self, session, jid):
        """return machine uuid for JID"""
        uuid_inventorymachine = (
            session.query(Machines).filter_by(jid=jid).first().uuid_inventorymachine
        )
        if uuid_inventorymachine:
            return uuid_inventorymachine.strip("UUID")
        else:
            return False

    @DatabaseHelper._sessionm
    def algoruleadorganisedbyusers(
        self, session, userou, classutilMachine="both", rule=8, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search only to the rule pointed.
                          rule_id = 8 by organization users
        Field "subject" is used to define the organisation by user OU eg Computers/HeadQuarter/Locations
        Field "relayserver_id" is used to define the Relayserver associe a ce name user
        enabled = 1 Only on active relayserver.
        If classutilMachine is deprived then the choice of relayserver will be in the relayserver
        reserve to a use of the private machine.
        """

        if classutilMachine == "private":
            sql = (
                session.query(RelayServer.id)
                .filter(
                    and_(
                        Has_relayserverrules.rules_id == rule,
                        literal(userou).op("regexp")(Has_relayserverrules.subject),
                        RelayServer.enabled == enabled,
                        RelayServer.classutil == classutilMachine,
                        RelayServer.moderelayserver == "static",
                        or_(RelayServer.switchonoff, RelayServer.mandatory),
                    )
                )
                .join(
                    Has_relayserverrules,
                    RelayServer.id == Has_relayserverrules.relayserver_id,
                )
                .limit(1)
            )
        else:
            sql = (
                session.query(RelayServer.id)
                .filter(
                    and_(
                        Has_relayserverrules.rules_id == rule,
                        literal(userou).op("regexp")(Has_relayserverrules.subject),
                        RelayServer.enabled == enabled,
                        RelayServer.moderelayserver == "static",
                        or_(RelayServer.switchonoff, RelayServer.mandatory),
                    )
                )
                .join(
                    Has_relayserverrules,
                    RelayServer.id == Has_relayserverrules.relayserver_id,
                )
                .limit(1)
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algoruleadorganisedbymachines(
        self, session, machineou, classutilMachine="both", rule=7, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search only to the rule pointed.
                          rule_id = 7 by organization machine
        Field "subject" is used to define the organisation by machine OU eg Computers/HeadQuarter/Locations
        Field "relayserver_id" is used to define the Relayserver associe a this organization
        enabled = 1 Only on active relayserver.
        If classutilMachine is deprived then the choice of relayserver will be in the relayserver reserve
           to a use of the private machine.
        """
        if classutilMachine == "private":
            sql = (
                session.query(RelayServer.id)
                .filter(
                    and_(
                        Has_relayserverrules.rules_id == rule,
                        literal(machineou).op("regexp")(Has_relayserverrules.subject),
                        RelayServer.enabled == enabled,
                        RelayServer.moderelayserver == "static",
                        RelayServer.classutil == classutilMachine,
                        or_(RelayServer.switchonoff, RelayServer.mandatory),
                    )
                )
                .join(
                    Has_relayserverrules,
                    RelayServer.id == Has_relayserverrules.relayserver_id,
                )
                .limit(1)
            )
        else:
            sql = (
                session.query(RelayServer.id)
                .filter(
                    and_(
                        Has_relayserverrules.rules_id == rule,
                        literal(machineou).op("regexp")(Has_relayserverrules.subject),
                        RelayServer.enabled == enabled,
                        RelayServer.moderelayserver == "static",
                        or_(RelayServer.switchonoff, RelayServer.mandatory),
                    )
                )
                .join(
                    Has_relayserverrules,
                    RelayServer.id == Has_relayserverrules.relayserver_id,
                )
                .limit(1)
            )
        result = sql.all()
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algoruleuser(
        self, session, username, classutilMachine="both", rule=1, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search only
                          to the rule pointed. rule_id = 1 for user name
        Field "subject" is used to define the name of the user in this rule
        Field "relayserver_id" is used to define the Relayserver associe a ce name user
        enabled = 1 Only on active relayserver.
        If classutilMachine is deprived then the choice of relayserver will be
           in the relayserver reserve to a use of the private machine.
        """
        if classutilMachine == "private":
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND `relayserver`.`classutil` = '%s'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                rule,
                re.escape(username),
                enabled,
                classutilMachine,
            )
        else:
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                rule,
                re.escape(username),
                enabled,
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algorulehostname(
        self, session, hostname, classutilMachine="both", rule=2, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search
                          only to the rule designated. rule_id = 2 for hostname
        Field "subject" is used to define the hostname in this rule
        enabled = 1 Only on active relayserver.
        If classutilMachine is private then the choice of relayserver will be
          in the relayservers reserved for machines where [global].agent_space
          configuration is set to private.
        # hostname regex
            #hostname matches subject of has_relayserverrules table
            #-- subject is the regex.
            #-- eg : ^machine_win_.*1$
            #-- eg : ^machine_win_.*[2-9]{1,3}$
            Tip: For cheching the regex using Mysql use
                select "hostname_for_test" REGEXP "^hostname.*";  => result  1
                select "hostname_for_test" REGEXP "^(?!hostname).*"; => result 0
        """
        if classutilMachine == "private":
            sql = """select `relayserver`.`id` , `has_relayserverrules`.`subject`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND `relayserver`.`classutil` = '%s'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            order by `has_relayserverrules`.`order`
            limit 1;""" % (
                rule,
                hostname,
                enabled,
                classutilMachine,
            )
        else:
            sql = """select `relayserver`.`id` , `has_relayserverrules`.`subject`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            order by `has_relayserverrules`.`order`
            limit 1;""" % (
                rule,
                hostname,
                enabled,
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        ret = [y for y in result]
        if len(ret) > 0:
            logging.getLogger().debug(
                "Matched hostname rule with "
                'hostname "%s\\# by regex \\#%s"' % (hostname, ret[0].subject)
            )
        return ret

    @DatabaseHelper._sessionm
    def algoruleloadbalancer(self, session):
        sql = """
            SELECT
                COUNT(*) AS nb, `machines`.`groupdeploy`, `relayserver`.`id`
            FROM
                xmppmaster.machines
                    INNER JOIN
                xmppmaster.`relayserver` ON `relayserver`.`groupdeploy` = `machines`.`groupdeploy`
            WHERE
                agenttype = 'machine'
                AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            GROUP BY `machines`.`groupdeploy`
            ORDER BY nb DESC
            LIMIT 1;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algorulesubnet(
        self, session, subnetmachine, classutilMachine="both", enabled=1
    ):
        """
        To associate relay server that is on same networks...
        """
        if classutilMachine == "private":
            sql = """select `relayserver`.`id`
            from `relayserver`
            where
                        `relayserver`.`enabled` = %d
                    AND `relayserver`.`subnet` ='%s'
                    AND `relayserver`.`classutil` = '%s'
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                enabled,
                subnetmachine,
                classutilMachine,
            )
        else:
            sql = """select `relayserver`.`id`
            from `relayserver`
            where
                        `relayserver`.`enabled` = %d
                    AND `relayserver`.`subnet` ='%s'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                enabled,
                subnetmachine,
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algorulebynetmaskaddress(
        self, session, netmaskaddress, classutilMachine="both", rule=10, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search only to the rule pointed. rule_id = 10 by network mask
        Field "netmaskaddress" is used to define the net mask address for association
        Field "relayserver_id" is used to define the Relayserver to be assigned to the machines matching that rule
        enabled = 1 Only on active relayserver.
        If classutilMachine is deprived then the choice of relayserver
            will be in the relayserver reserve to a use of the private machine.
        """
        if classutilMachine == "private":
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND `has_relayserverrules`.`subject` = '%s'
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND `relayserver`.`classutil` = '%s'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                rule,
                netmaskaddress,
                enabled,
                classutilMachine,
            )
        else:
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND `has_relayserverrules`.`subject` = '%s'
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            limit 1;""" % (
                rule,
                netmaskaddress,
                enabled,
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def algorulebynetworkaddress(
        self, session, subnetmachine, classutilMachine="both", rule=9, enabled=1
    ):
        """
        Field "rule_id" : This information allows you to apply the search
                          only to the rule pointed. rule_id = 9 by network address
        Field "subject" is used to define the subnet for association
        Field "relayserver_id" is used to define the Relayserver to be assigned to
                               the machines matching that rule
        enabled = 1 Only on active relayserver.
        If classutilMachine is private then the choice of relayserver will be in the relayserver reserved to a use of the private machine.
        subnetmachine CIDR machine.
            CIDR matching with suject of table has_relayserverrules
            -- subject is the regex.
            -- eg : ^55\\.171\\.[5-6]{1}\\.[0-9]{1,3}/24$
            -- eg : ^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}/24$ all address mask 255.255.255.255
        """
        if classutilMachine == "private":
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND `relayserver`.`classutil` = '%s'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            order by `has_relayserverrules`.`order`
            limit 1;""" % (
                rule,
                subnetmachine,
                enabled,
                classutilMachine,
            )
        else:
            sql = """select `relayserver`.`id`
            from `relayserver`
                inner join
                    `has_relayserverrules` ON  `relayserver`.`id` = `has_relayserverrules`.`relayserver_id`
            where
                `has_relayserverrules`.`rules_id` = %d
                    AND '%s' REGEXP `has_relayserverrules`.`subject`
                    AND `relayserver`.`enabled` = %d
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`)
            order by `has_relayserverrules`.`order`
            limit 1;""" % (
                rule,
                subnetmachine,
                enabled,
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def IpAndPortConnectionFromServerRelay(self, session, id):
        """return ip et port server relay for connection"""
        sql = (
            """SELECT
                    ipconnection, port, jid, urlguacamole
                 FROM
                    xmppmaster.relayserver
                 WHERE
                    id = %s;"""
            % id
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return list([x for x in result][0])

    @DatabaseHelper._sessionm
    def jidrelayserverforip(self, session, ip):
        """return jid server relay for connection"""
        sql = (
            """SELECT
                    ipconnection, port, jid, urlguacamole
                FROM
                    xmppmaster.relayserver
                WHERE
                    ipconnection = '%s';"""
            % ip
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = list([x for x in result][0])
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def IdlonglatServerRelay(self, session, classutilMachine="both", enabled=1):
        """return long and lat server relay"""
        if classutilMachine == "private":
            sql = """SELECT
                        id, longitude, latitude
                    FROM
                        xmppmaster.relayserver
                    WHERE
                            `relayserver`.`enabled` = %d
                        AND `relayserver`.`classutil` = '%s'
                    AND `relayserver`.`moderelayserver` = 'static'
                    AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`);""" % (
                enabled,
                classutilMachine,
            )
        else:
            sql = """SELECT
                        id,longitude,latitude
                    FROM
                        xmppmaster.relayserver
                    WHERE
                        `relayserver`.`enabled` = %d
                        AND (`relayserver`.`switchonoff` OR `relayserver`.`mandatory`);""" % (
                enabled
            )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def Orderrules(self, session):
        sql = """SELECT
                    *
                FROM
                    xmppmaster.rules
                ORDER BY level;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def hasmachineusers(self, session, machines_id, users_id):
        result = (
            session.query(Has_machinesusers.machines_id)
            .filter(
                and_(
                    Has_machinesusers.machines_id == machines_id,
                    Has_machinesusers.users_id == users_id,
                )
            )
            .first()
        )
        session.commit()
        session.flush()
        if result is None:
            new_machineuser = Has_relayserverrules()
            new_machineuser.machines_id = machines_id
            new_machineuser.users_id = users_id
            session.commit()
            session.flush()
            return True
        return False

    @DatabaseHelper._sessionm
    def addguacamoleidformachineid(self, session, machine_id, idguacamole):
        try:
            hasguacamole = Has_guacamole()
            hasguacamole.idguacamole = idguacamole
            hasguacamole.machine_id = machine_id
            session.add(hasguacamole)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def addlistguacamoleidformachineid(self, session, machine_id, connection):
        # objet connection: {u'VNC': 60, u'RDP': 58, u'SSH': 59}}
        if len(connection) == 0:
            # on ajoute 1 protocole inexistant pour signaler que guacamle est
            # configure.
            connection["INF"] = 0

        sql = (
            """DELETE FROM `xmppmaster`.`has_guacamole`
                    WHERE
                        `xmppmaster`.`has_guacamole`.`machine_id` = '%s';"""
            % machine_id
        )
        session.execute(sql)
        session.commit()
        session.flush()

        for idguacamole in connection:
            try:
                hasguacamole = Has_guacamole()
                hasguacamole.idguacamole = connection[idguacamole]
                hasguacamole.machine_id = machine_id
                hasguacamole.protocol = idguacamole
                session.add(hasguacamole)
                session.commit()
                session.flush()
            except Exception as e:
                logging.getLogger().error(str(e))

    @DatabaseHelper._sessionm
    def listserverrelay(self, session, moderelayserver="static"):
        sql = (
            """SELECT
                    jid
                FROM
                    xmppmaster.relayserver
                WHERE
                    `xmppmaster`.`relayserver`.`moderelayserver` = '%s'
                    ;"""
            % moderelayserver
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def column_list_table(self, session, tablename, basename="xmppmaster"):
        """
        This function returns the list of column titles in the table,
        where the name of this table is passed as a parameter.
        """
        try:
            sql = """SELECT
                        column_name
                    FROM
                        information_schema.columns WHERE table_name = '%s'
                        AND
                        table_schema='%s';""" % (
                tablename,
                basename,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            return [x[0] for x in result]
        except Exception as e:
            logging.getLogger().error(str(e))
            logging.getLogger().error("\n%s" % (traceback.format_exc()))

    @DatabaseHelper._sessionm
    def random_list_ars_relay_one_only_in_cluster(
        self, session, sessiontype_return="dict"
    ):
        """
        this function search 1 list ars.
        1 only ars by cluster.
        the ars of cluster is randomly selected

        return object is 1 list organize per row found.
            following the sessiontype_return parameter:
                - sessiontype_return is "dict"
                    the rows are expressed in the form of dictionary (column name, value column)
                - sessiontype_return is "list"
                    the rows are expressed as a list of values.
        """
        sql = """SELECT
                    *
                FROM
                    xmppmaster.relayserver
                WHERE
                    `xmppmaster`.`relayserver`.`id` IN (
                        SELECT
                            id
                        FROM
                            (SELECT
                                id
                            FROM
                                (SELECT
                                    xmppmaster.relayserver.id AS id,
                                    xmppmaster.has_cluster_ars.id_cluster AS cluster
                                FROM
                                    xmppmaster.relayserver
                                INNER JOIN xmppmaster.has_cluster_ars
                                        ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                                ORDER BY RAND()) selectrandonlistars
                            GROUP BY cluster) selectcluster);"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        a = []
        if result:
            if sessiontype_return == "dict":
                columnlist = self.column_list_table("relayserver")
                for ligneresult in [x for x in result]:
                    obj = {}
                    for index, value in enumerate(columnlist):
                        obj[value] = ligneresult[index]
                    a.append(obj)
                return a
            else:
                return [x[0] for x in result]
        else:
            return []

    @DatabaseHelper._sessionm
    def listmachines(self, session):
        sql = """SELECT
                    jid
                FROM
                    xmppmaster.machines;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def clearMachine(self, session):
        session.execute("TRUNCATE TABLE xmppmaster.machines;")
        session.execute("TRUNCATE TABLE xmppmaster.network;")
        session.execute("TRUNCATE TABLE xmppmaster.has_machinesusers;")
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def listMacAdressforMachine(self, session, id_machine, infomac=False):
        try:
            sql = """SELECT
                        GROUP_CONCAT(DISTINCT mac ORDER BY mac ASC  SEPARATOR ',') AS listmac
                    FROM
                        xmppmaster.network
                    WHERE
                        machines_id = '%s';""" % (
                id_machine
            )
            if infomac:
                logging.getLogger().debug(
                    "SQL request to get the mac addresses list "
                    "for the presence machine #%s" % id_machine
                )
            listMacAdress = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        result = [x for x in listMacAdress][0]
        if infomac:
            logging.getLogger().debug(
                "Result list MacAdress for Machine : %s" % result[0]
            )
        return result

    @DatabaseHelper._sessionm
    def getjidMachinefromuuid(self, session, uuid):
        try:
            sql = (
                """SELECT
                        jid
                    FROM
                        xmppmaster.machines
                    WHERE
                        uuid_inventorymachine = '%s'
                        LIMIT 1;"""
                % uuid
            )
            jidmachine = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
            return ""
        try:
            result = [x for x in jidmachine][0]
        except BaseException:
            return ""
        return result[0]

    @DatabaseHelper._sessionm
    def updateMachineidinventory(self, session, id_machineinventory, idmachine):
        updatedb = -1
        try:
            sql = """UPDATE `machines`
                    SET
                        `uuid_inventorymachine` = '%s'
                    WHERE
                        `id` = '%s';""" % (
                id_machineinventory,
                idmachine,
            )
            updatedb = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        return updatedb

    @DatabaseHelper._sessionm
    def updateMachinejidGuacamoleGroupdeploy(
        self, session, jid, urlguacamole, groupdeploy, idmachine
    ):
        updatedb = -1
        try:
            sql = """UPDATE machines
                        SET
                            jid = '%s', urlguacamole = '%s', groupdeploy = '%s'
                        WHERE
                            id = '%s';""" % (
                jid,
                urlguacamole,
                groupdeploy,
                idmachine,
            )
            updatedb = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
        return updatedb

    @DatabaseHelper._sessionm
    def getPresenceuuidenabled(self, session, uuid, enabled=0):
        return session.query(
            exists().where(
                and_(
                    Machines.uuid_inventorymachine == uuid, Machines.enabled == enabled
                )
            )
        ).scalar()

    @DatabaseHelper._sessionm
    def getPresenceuuid(self, session, uuid):
        machinespresente = (
            session.query(Machines.uuid_inventorymachine)
            .filter(
                and_(Machines.uuid_inventorymachine == uuid, Machines.enabled == "1")
            )
            .first()
        )
        session.commit()
        session.flush()
        if machinespresente:
            return True
        return False

    @DatabaseHelper._sessionm
    def getPresenceuuids(self, session, uuids):
        if isinstance(uuids, str):
            uuids = [uuids]
        result = {}
        for uuidmachine in uuids:
            result[uuidmachine] = False
        machinespresente = (
            session.query(Machines.uuid_inventorymachine)
            .filter(
                and_(Machines.uuid_inventorymachine.in_(uuids), Machines.enabled == "1")
            )
            .all()
        )
        session.commit()
        session.flush()
        for linemachine in machinespresente:
            result[linemachine.uuid_inventorymachine] = True
        return result

    @DatabaseHelper._sessionm
    def getPresenceExistuuids(self, session, uuids):
        """
        This function is used to obtain the presence and the GLPI uuid
        of machines based on the uuids.
        Args:
            session: SQLAlchemy session
            uuids: uuid of the machine we are searching
        Return: This fonction return a dictionnary:
                {'UUID_GLPI': [presence of the machine, initialised glpi uuid]}
        """
        if isinstance(uuids, str):
            uuids = [uuids]
        result = {}
        for uuidmachine in uuids:
            result[uuidmachine] = [0, 0]
        machinespresente = (
            session.query(Machines.uuid_inventorymachine, Machines.enabled)
            .filter(Machines.uuid_inventorymachine.in_(uuids))
            .all()
        )
        session.commit()
        session.flush()
        for linemachine in machinespresente:
            out = 0
            if linemachine.enabled is True:
                out = 1
            result[linemachine.uuid_inventorymachine] = [out, 1]

        return result

    @DatabaseHelper._sessionm
    def update_uuid_inventory(self, session, sql_id, uuid):
        """
        This function is used to update the uuid_inventorymachine value
        in the database for a specific machine.
        Args:
            session: The SQLAlchemy session
            sql_id: the id of the machine in the SQL database
            uuid: The uuid_inventorymachine of the machine
        Return:
           It returns None if it failed to update the machine uuid_inventorymachine.
        """
        try:
            sql = """UPDATE `xmppmaster`.`machines`
                    SET
                        `uuid_inventorymachine` = '%s'
                    WHERE
                        `id`  = %s;""" % (
                uuid,
                sql_id,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            return result
        except Exception as e:
            logging.getLogger().error("Function update_uuid_inventory")
            logging.getLogger().error("We got the error: %s" % str(e))
            return None

    # Topology
    @DatabaseHelper._sessionm
    def listRS(self, session):
        """return les RS pour le deploiement"""
        sql = """SELECT DISTINCT
                    groupdeploy
                FROM
                    xmppmaster.machines;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        listrs = [x for x in result]
        return [i[0] for i in listrs]

    # Topology
    @DatabaseHelper._sessionm
    def topologypulse(self, session):
        # listrs = self.listRS()
        # select liste des RS
        # list des machines pour un relayserver

        sql = """SELECT groupdeploy,
                    GROUP_CONCAT(jid)
                FROM
                    xmppmaster.machines
                WHERE
                    xmppmaster.machines.agenttype = 'machine'
                GROUP BY
                    groupdeploy;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        listmachinebyRS = [x for x in result]
        resulttopologie = {}
        for i in listmachinebyRS:
            listmachines = i[1].split(",")
            resulttopologie[i[0]] = listmachines
        self.write_topologyfile(resulttopologie)
        return [resulttopologie]

    # Topology
    def write_topologyfile(self, topology):
        directoryjson = os.path.join("/", "usr", "share", "mmc", "datatopology")
        if not os.path.isdir(directoryjson):
            # creation repertoire de json topology
            os.makedirs(directoryjson)
            os.chmod(directoryjson, 0o777)  # for example
            uid, gid = pwd.getpwnam("root").pw_uid, pwd.getpwnam("root").pw_gid
            # set user:group as root:www-data
            os.chown(directoryjson, uid, gid)
        # creation topology file.
        filename = "topology.json"
        pathfile = os.path.join(directoryjson, filename)
        builddatajson = {"name": "Pulse", "type": "AMR", "parent": None, "children": []}
        for i in topology:
            listmachines = topology[i]

            ARS = {}
            ARS["name"] = i
            ARS["display_name"] = i.split("/")[1]
            ARS["type"] = "ARS"
            ARS["parent"] = "Pulse"
            ARS["children"] = []

            listmachinesstring = []
            for mach in listmachines:
                ARS["children"].append(
                    {
                        "name": mach,
                        "display_name": mach.split("/")[1],
                        "type": "AM",
                        "parent": i,
                    }
                )
            # builddatajson[i] = listmachinesstring
            # ARS['children'] = builddatajson
            # print listmachinesstring
            builddatajson["children"].append(ARS)

        with open(pathfile, "w") as outfile:
            json.dump(builddatajson, outfile, indent=4)
        os.chmod(pathfile, 0o777)
        uid, gid = pwd.getpwnam("root").pw_uid, pwd.getpwnam("root").pw_gid
        os.chown(pathfile, uid, gid)

    @DatabaseHelper._sessionm
    def getstepdeployinsession(self, session, sessiondeploy):
        sql = """
                SELECT
            date, text
        FROM
            xmppmaster.logs
        WHERE
            type = 'deploy'
                AND sessionname = '%s'
        ORDER BY id;""" % (
            sessiondeploy
        )
        step = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = []
            for t in step:
                a.append({"date": t[0], "text": t[1]})
            return a
        except BaseException:
            return []

    @DatabaseHelper._sessionm
    def getlistPresenceMachineid(self, session, format=False):
        sql = """SELECT
                    uuid_inventorymachine
                 FROM
                    xmppmaster.machines
                 WHERE
                    enabled = '1' and
                    agenttype = 'machine' and uuid_inventorymachine IS NOT NULL AND uuid_inventorymachine!='';"""

        presencelist = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = []
            for t in presencelist:
                a.append(t[0])
            return a
        except BaseException:
            return a

    @DatabaseHelper._sessionm
    def getidlistPresenceMachine(self, session, presence=None):
        """
        This function is used to retrieve the list of the machines based on the 'presence' argument.

        Args:
            session: The SQLAlchemy session
            presence: if True, it returns the list of the machine with an agent up.
                      if False, it returns the list of the machine with an agent down.
                      if None, it returns the list with all the machines.
        Returns:
            It returns the list of the machine based on the 'presence' argument.
        """
        strpresence = ""
        try:
            if presence is not None:
                if presence:
                    strpresence = " and enabled = 1"
                else:
                    strpresence = " and enabled = 0"
            sql = (
                """SELECT
                        SUBSTR(uuid_inventorymachine, 5)
                    FROM
                        xmppmaster.machines
                    WHERE
                        agenttype = 'machine'
                    and
                        uuid_inventorymachine IS NOT NULL %s;"""
                % strpresence
            )
            presencelist = session.execute(sql)
            session.commit()
            session.flush()
            return [x[0] for x in presencelist]
        except Exception as e:
            logging.getLogger().error(
                "Error debug for the getidlistPresenceMachine function!"
            )
            logging.getLogger().error("The presence of the machine is:  %s" % presence)
            logging.getLogger().error("The sql error is: %s" % sql)
            logging.getLogger().error("the Exception catched is %s" % str(e))
            return []

    @DatabaseHelper._sessionm
    def getxmppmasterfilterforglpi(self, session, listqueryxmppmaster=None):
        listqueryxmppmaster[2] = listqueryxmppmaster[2].lower()
        fl = listqueryxmppmaster[3].replace("*", "%")
        if listqueryxmppmaster[2] == "ou user":
            machineid = session.query(Machines.uuid_inventorymachine).filter(
                Machines.uuid_inventorymachine.isnot(None)
            )
            machineid = machineid.filter(Machines.ad_ou_user.like(fl))
        elif listqueryxmppmaster[2] == "ou machine":
            machineid = session.query(Machines.uuid_inventorymachine).filter(
                Machines.uuid_inventorymachine.isnot(None)
            )
            machineid = machineid.filter(Machines.ad_ou_machine.like(fl))
        elif listqueryxmppmaster[2] == "online computer":
            d = XmppMasterDatabase().getlistPresenceMachineid()
            listid = [x.replace("UUID", "") for x in d]
            return listid
        machineid = machineid.all()
        session.commit()
        session.flush()
        ret = [str(m.uuid_inventorymachine).replace("UUID", "") for m in machineid]
        return ret

    @DatabaseHelper._sessionm
    def getListPresenceMachine(self, session):
        sql = """SELECT
                    jid, agenttype, hostname, uuid_inventorymachine
                 FROM
                    xmppmaster.machines
                 WHERE
                    agenttype='machine' and uuid_inventorymachine IS NOT NULL;"""

        presencelist = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = []
            for t in presencelist:
                a.append(
                    {
                        "jid": t[0],
                        "type": t[1],
                        "hostname": t[2],
                        "uuid_inventorymachine": t[3],
                    }
                )
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def getListPresenceMachineWithKiosk(self, session):
        sql = """SELECT
                    *
                 FROM
                    xmppmaster.machines
                 WHERE
                    agenttype='machine' and uuid_inventorymachine IS NOT NULL ;"""

        presencelist = session.execute(sql)
        session.commit()
        session.flush()
        try:
            a = []
            for t in presencelist:
                a.append(
                    {
                        "id": t[0],
                        "jid": t[1],
                        "platform": t[2],
                        "hostname": t[4],
                        "uuid_inventorymachine": t[5],
                        "agenttype": t[10],
                        "classutil": t[11],
                    }
                )
            return a
        except BaseException:
            return -1

    @DatabaseHelper._sessionm
    def update_Presence_Relay(self, session, jid, presence=0):
        """
        Update the presence in the relay and machine SQL Tables
        Args:
            session: The SQL Alchemy session
            jid: jid of the relay to update
            presence: Availability of the relay
                      0: Set the relay as offline
                      1: Set the relay as online
        """
        try:
            user = str(jid).split("@")[0]
            sql = """UPDATE
                        `xmppmaster`.`machines`
                    SET
                        `enabled` = '%s'
                    WHERE
                        `xmppmaster`.`machines`.`jid` like('%s@%%') limit 1;""" % (
                presence,
                user,
            )
            session.execute(sql)
            sql = """UPDATE
                        `xmppmaster`.`relayserver`
                    SET
                        `enabled` = '%s'
                    WHERE
                        `xmppmaster`.`relayserver`.`jid` like('%s@%%') limit 1;""" % (
                presence,
                user,
            )
            session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(
                "Function : update_Presence_Relay, we got the error: " % str(e)
            )
            logging.getLogger().error(
                "We encountered the backtrace: \n%s" % traceback.format_exc()
            )

    @DatabaseHelper._sessionm
    def is_machine_reconf_needed(self, session, jid, reconf=1):
        """
        Tell if we need to start a reconfiguration of the machines assigned to a relay.
        Args:
            session: The SQL Alchemy session
            jid: jid of the relay to update
            reconf: Tell if we need to reconfigure the machines.
                    0: No reconf needed
                    1: A reconfigurtion is needed
        """
        try:
            user = str(jid).split("@")[0]
            set_reconf = """UPDATE
                        `xmppmaster`.`machines`
                     SET
                        `need_reconf` = '%s'
                     WHERE
                        `xmppmaster`.`machines`.`agenttype` like ("machine")
                        AND
                        `xmppmaster`.`machines`.`groupdeploy` like('%s@%%');""" % (
                reconf,
                user,
            )
            session.execute(set_reconf)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(
                "Function : is_machine_reconf_needed, we got the error: %s " % str(e)
            )
            logging.getLogger().error(
                "We encountered the backtrace: \n%s" % traceback.format_exc()
            )

    @DatabaseHelper._sessionm
    def delPresenceMachine(self, session, jid):
        result = ["-1"]
        typemachine = "machine"
        try:
            sql = (
                """SELECT
                        id, hostname, agenttype
                    FROM
                        xmppmaster.machines
                    WHERE
                        xmppmaster.machines.jid = '%s';"""
                % jid
            )
            id = session.execute(sql)
            session.commit()
            session.flush()
            result = [x for x in id][0]
            sql = (
                """DELETE FROM `xmppmaster`.`machines`
                    WHERE
                        `xmppmaster`.`machines`.`id` = '%s';"""
                % result[0]
            )

            sql3 = (
                """DELETE FROM `xmppmaster`.`has_machinesusers`
                    WHERE
                        `has_machinesusers`.`machines_id` = '%s';"""
                % result[0]
            )
            if result[2] == "relayserver":
                typemachine = "relayserver"
                sql2 = (
                    """UPDATE `xmppmaster`.`relayserver`
                            SET
                                `enabled` = '0'
                            WHERE
                                `xmppmaster`.`relayserver`.`nameserver` = '%s';"""
                    % result[1]
                )
                session.execute(sql2)
            session.execute(sql)
            session.execute(sql3)
            session.commit()
            session.flush()
        except IndexError:
            logging.getLogger().warning(
                "Configuration agent machine jid [%s]. "
                "no jid in base for configuration" % jid
            )
            return {}
        except Exception as e:
            logging.getLogger().error(str(e))
            return {}
        resulttypemachine = {"type": typemachine}
        return resulttypemachine

    @DatabaseHelper._sessionm
    def getPresencejiduser(self, session, userjid):
        user = str(userjid).split("@")[0]
        sql = """SELECT COUNT(jid) AS nb
            FROM
                 xmppmaster.machines
             WHERE
              jid LIKE ('%s%%');""" % (
            user
        )
        presencejid = session.execute(sql)
        session.commit()
        session.flush()
        ret = [m[0] for m in presencejid]
        if ret[0] == 0:
            return False
        return True

    @DatabaseHelper._sessionm
    def delPresenceMachinebyjiduser(self, session, jiduser):
        result = ["-1"]
        typemachine = "machine"
        try:
            sql = (
                """SELECT
                        id, hostname, agenttype
                    FROM
                        xmppmaster.machines
                    WHERE
                        xmppmaster.machines.jid like('%s@%%');"""
                % jiduser
            )
            id = session.execute(sql)
            session.commit()
            session.flush()
            result = [x for x in id][0]
            sql = (
                """DELETE FROM `xmppmaster`.`machines`
                    WHERE
                        `xmppmaster`.`machines`.`id` = '%s';"""
                % result[0]
            )
            sql3 = (
                """DELETE FROM `xmppmaster`.`has_machinesusers`
                    WHERE
                        `has_machinesusers`.`machines_id` = '%s';"""
                % result[0]
            )
            if result[2] == "relayserver":
                typemachine = "relayserver"
                sql2 = (
                    """UPDATE `xmppmaster`.`relayserver`
                            SET
                                `enabled` = '0'
                            WHERE
                                `xmppmaster`.`relayserver`.`nameserver` = '%s';"""
                    % result[1]
                )
                session.execute(sql2)
            session.execute(sql)
            session.execute(sql3)
            session.commit()
            session.flush()
        except IndexError:
            logging.getLogger().warning(
                "Configuration agent machine "
                "jid [%s]. no jid in base for configuration" % jiduser
            )
            return {}
        except Exception as e:
            logging.getLogger().error(str(e))
            return {}
        resulttypemachine = {"type": typemachine}
        return resulttypemachine

    @DatabaseHelper._sessionm
    def get_machine_with_dupplicate_uuidinventory(self, session, uuid, enable=1):
        """
        This function is used to retrieve computers with dupplicate uuids.
        Args:
            session: The SQL Alchemy session
            uuid: The uuid we are looking for
            enable: Used to search for enabled or disabled only machines

        Returns:
            It return machines with dupplicate UUIDs.
            We can search for enabled/disabled or all machines.
        """

        try:
            querymachine = session.query(Machines)
            if enable is None:
                querymachine = querymachine.filter(
                    Machines.uuid_inventorymachine == uuid
                )
            else:
                querymachine = querymachine.filter(
                    and_(
                        Machines.uuid_inventorymachine == uuid,
                        Machines.enabled == enable,
                    )
                )
            machine = querymachine.all()
            resultdata = []
            if machine:
                for t in machine:
                    result = {
                        "uuid": uuid,
                        "jid": t.jid,
                        "groupdeploy": t.groupdeploy,
                        "urlguacamole": t.urlguacamole,
                        "subnetxmpp": t.subnetxmpp,
                        "hostname": t.hostname,
                        "platform": t.platform,
                        "macaddress": t.macaddress,
                        "archi": t.archi,
                        "uuid_inventorymachine": t.uuid_inventorymachine,
                        "ip_xmpp": t.ip_xmpp,
                        "agenttype": t.agenttype,
                        "keysyncthing": t.keysyncthing,
                        "enabled": t.enabled,
                    }
                    for i in result:
                        if result[i] is None:
                            result[i] = ""
                    resultdata.append(result)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(
                "We failed to search the computers having %s as uuid" % uuid
            )
            logging.getLogger().error("The backtrace we trapped is: \n %s" % str(e))

        return resultdata

    @DatabaseHelper._sessionm
    def getGuacamoleRelayServerMachineUuid(self, session, uuid, enable=1):
        result = {
            "error": "noresult",
            "uuid": uuid,
            "jid": "",
            "groupdeploy": "",
            "urlguacamole": "",
            "subnetxmpp": "",
            "hostname": "",
            "platform": "",
            "macaddress": "",
            "archi": "",
            "uuid_inventorymachine": "",
            "ip_xmpp": "",
            "agenttype": "",
            "keysyncthing": "",
            "enabled": enable,
        }
        try:
            querymachine = session.query(Machines)
            if enable is None:
                querymachine = querymachine.filter(
                    Machines.uuid_inventorymachine == uuid
                )
            else:
                querymachine = querymachine.filter(
                    and_(
                        Machines.uuid_inventorymachine == uuid,
                        Machines.enabled == enable,
                    )
                )
            machine = querymachine.one()

            session.commit()
            session.flush()

            result = {
                "error": "noerror",
                "uuid": uuid,
                "jid": machine.jid,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "subnetxmpp": machine.subnetxmpp,
                "hostname": machine.hostname,
                "platform": machine.platform,
                "macaddress": machine.macaddress,
                "archi": machine.archi,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "agenttype": machine.agenttype,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
            }
            for i in result:
                if result[i] is None:
                    result[i] = ""

        except NoResultFound as e:
            result["error"] = "NoResultFound"
            if enable is None:
                logging.getLogger().error(
                    "We found no machines with the UUID %s" % uuid
                )
            else:
                logging.getLogger().error(
                    "We found no machines with the UUID %s, and with enabled: %s"
                    % uuid,
                    enable,
                )

            logging.getLogger().error(
                "We encountered the following error:\n %s" % str(e)
            )
        except MultipleResultsFound as e:
            result["error"] = "MultipleResultsFound"
            if enable is None:
                logging.getLogger().error(
                    "We found multiple machines with the UUID %s" % uuid
                )
            else:
                logging.getLogger().error(
                    "We found multiple machines with the UUID %s, and with enabled: %s"
                    % uuid,
                    enable,
                )

            logging.getLogger().error(
                "We encountered the following error:\n %s" % str(e)
            )

        except Exception as e:
            result["error"] = str(e)
            if enable is None:
                logging.getLogger().error(
                    "We were searching for machines with the UUID %s" % uuid
                )
            else:
                logging.getLogger().error(
                    "We were searching for machines with the UUID %s, and with enabled: %s"
                    % uuid,
                    enable,
                )

            logging.getLogger().error(
                "We encountered the following error:\n %s" % str(e)
            )

        return result

    @DatabaseHelper._sessionm
    def getMachinedeployexistonHostname(self, session, hostname):
        machinesexits = []
        try:
            sql = (
                """SELECT
                    machines.id AS id,
                    machines.uuid_inventorymachine AS uuid,
                    machines.uuid_serial_machine AS serial,
                    GROUP_CONCAT(network.mac) AS macs
                FROM
                    xmppmaster.machines
                        JOIN
                    xmppmaster.network ON machines.id = network.machines_id
                WHERE
                    machines.agenttype = 'machine'
                        AND machines.hostname LIKE '%s'
                GROUP BY machines.id;"""
                % hostname.strip()
            )
            machines = session.execute(sql)
        except Exception as e:
            logging.getLogger().error(
                "function getMachinedeployexistonHostname %s" % str(e)
            )
            return machinesexits
        for machine in machines:
            mach = {
                "id": machine.id,
                "uuid": machine.uuid,
                "macs": machine.macs,
                "serial": machine.serial,
            }
            machinesexits.append(mach)
        return machinesexits

    @DatabaseHelper._sessionm
    def getMachineHostname(self, session, hostname):
        try:
            machine = (
                session.query(Machines.id, Machines.uuid_inventorymachine)
                .filter(Machines.hostname == hostname)
                .first()
            )
            session.commit()
            session.flush()
            if machine:
                return {
                    "id": machine.id,
                    "uuid_inventorymachine": machine.uuid_inventorymachine,
                }
        except Exception as e:
            logging.getLogger().error("function getMachineHostname %s" % str(e))

        return {}

    @DatabaseHelper._sessionm
    def getGuacamoleRelayServerMachineHostname(
        self, session, hostname, enable=1, agenttype="machine"
    ):
        querymachine = session.query(Machines)
        if enable is None:
            querymachine = querymachine.filter(Machines.hostname == hostname)
        else:
            querymachine = querymachine.filter(
                and_(
                    Machines.hostname == hostname,
                    Machines.enabled == enable,
                    Machines.agenttype == agenttype,
                )
            )
        machine = querymachine.one()
        session.commit()
        session.flush()
        try:
            result = {
                "uuid": machine.uuid_inventorymachine,
                "jid": machine.jid,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "subnetxmpp": machine.subnetxmpp,
                "hostname": machine.hostname,
                "platform": machine.platform,
                "macaddress": machine.macaddress,
                "archi": machine.archi,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "agenttype": machine.agenttype,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
            }
            for i in result:
                if result[i] is None:
                    result[i] = ""
        except Exception:
            result = {
                "uuid": -1,
                "jid": "",
                "groupdeploy": "",
                "urlguacamole": "",
                "subnetxmpp": "",
                "hostname": "",
                "platform": "",
                "macaddress": "",
                "archi": "",
                "uuid_inventorymachine": "",
                "ip_xmpp": "",
                "agenttype": "",
                "keysyncthing": "",
                "enabled": 0,
            }
        return result

    @DatabaseHelper._sessionm
    def getGuacamoleRelayServerMachineJiduser(self, session, userjid, enable=1):
        user = str(userjid).split("@")[0]
        querymachine = session.query(Machines)
        if enable is None:
            querymachine = querymachine.filter(Machines.jid.like("%s%%" % user))
        else:
            querymachine = querymachine.filter(
                and_(Machines.jid.like("%s%%" % user), Machines.enabled == enable)
            )
        machine = querymachine.one()
        session.commit()
        session.flush()
        try:
            result = {
                "uuid": uuid,
                "jid": machine.jid,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "subnetxmpp": machine.subnetxmpp,
                "hostname": machine.hostname,
                "platform": machine.platform,
                "macaddress": machine.macaddress,
                "archi": machine.archi,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "agenttype": machine.agenttype,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
            }
            for i in result:
                if result[i] is None:
                    result[i] = ""
        except Exception:
            result = {
                "uuid": uuid,
                "jid": "",
                "groupdeploy": "",
                "urlguacamole": "",
                "subnetxmpp": "",
                "hostname": "",
                "platform": "",
                "macaddress": "",
                "archi": "",
                "uuid_inventorymachine": "",
                "ip_xmpp": "",
                "agenttype": "",
                "keysyncthing": "",
                "enabled": 0,
            }
        return result

    @DatabaseHelper._sessionm
    def getGuacamoleidforUuid(self, session, uuid, existtest=None):
        """
        if existtest is None
         this function return the list of protocole for 1 machine
         if existtest is not None:
         this function return True if guacamole is configured
         or false si guacamole is not configued.
        """
        if existtest is None:
            ret = (
                session.query(Has_guacamole.idguacamole, Has_guacamole.protocol)
                .filter(
                    and_(
                        Has_guacamole.idinventory == uuid.replace("UUID", ""),
                        Has_guacamole.protocol != "INF",
                    )
                )
                .all()
            )
            session.commit()
            session.flush()
            if ret:
                return [(m[1], m[0]) for m in ret]
            else:
                return []
        else:
            ret = (
                session.query(Has_guacamole.idguacamole)
                .filter(Has_guacamole.idinventory == uuid.replace("UUID", ""))
                .first()
            )
            if ret:
                return True
            return False

    @DatabaseHelper._sessionm
    def getGuacamoleIdForHostname(self, session, host, existtest=None):
        """
        if existtest is None
         this function return the list of protocole for 1 machine
         if existtest is not None:
         this function return True if guacamole is configured
         or false si guacamole is not configued.
        """
        if existtest is None:
            protocole = session.query(
                Has_guacamole.idguacamole, Has_guacamole.protocol
            ).join(Machines, Machines.id == Has_guacamole.machine_id)

            protocole = protocole.filter(
                and_(Has_guacamole.protocol != "INF", Machines.hostname == host)
            )
            protocole = protocole.all()
            session.commit()
            session.flush()
            if protocole:
                return [(m[1], m[0]) for m in protocole]
            else:
                return []
        else:
            protocole = session.query(Has_guacamole.idguacamole).join(
                Machines, Machines.id == Has_guacamole.machine_id
            )
            protocole = protocole.filter(Machines.hostname == host)

            protocole = protocole.first()
            if protocole:
                return True
            return False

    @DatabaseHelper._sessionm
    def getPresencejid(self, session, jid):
        user = str(jid).split("@")[0]
        sql = """SELECT COUNT(jid) AS nb
            FROM
                 xmppmaster.machines
             WHERE
              jid LIKE ('%s%%');""" % (
            user
        )
        presencejid = session.execute(sql)
        session.commit()
        session.flush()
        ret = [m[0] for m in presencejid]
        if ret[0] == 0:
            return False
        return True

    @DatabaseHelper._sessionm
    def getMachinefromjid(self, session, jid):
        """information machine"""
        user = str(jid).split("@")[0]
        machine = (
            session.query(Machines).filter(Machines.jid.like("%s%%" % user)).first()
        )
        session.commit()
        session.flush()
        result = {}
        if machine:
            result = {
                "id": machine.id,
                "jid": machine.jid,
                "platform": machine.platform,
                "archi": machine.archi,
                "hostname": machine.hostname,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "ippublic": machine.ippublic,
                "macaddress": machine.macaddress,
                "subnetxmpp": machine.subnetxmpp,
                "agenttype": machine.agenttype,
                "classutil": machine.classutil,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "picklekeypublic": machine.picklekeypublic,
                "ad_ou_user": machine.ad_ou_user,
                "ad_ou_machine": machine.ad_ou_machine,
                "kiosk_presence": machine.kiosk_presence,
                "lastuser": machine.lastuser,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
                "uuid_serial_machine": machine.uuid_serial_machine,
            }
        return result

    @DatabaseHelper._sessionm
    def getMachinefromuuid(self, session, uuid):
        """information machine"""
        machine = (
            session.query(Machines)
            .filter(Machines.uuid_inventorymachine == uuid)
            .first()
        )
        session.commit()
        session.flush()
        result = {}
        if machine:
            result = {
                "id": machine.id,
                "jid": machine.jid,
                "platform": machine.platform,
                "archi": machine.archi,
                "hostname": machine.hostname,
                "uuid_inventorymachine": machine.uuid_inventorymachine,
                "ip_xmpp": machine.ip_xmpp,
                "ippublic": machine.ippublic,
                "macaddress": machine.macaddress,
                "subnetxmpp": machine.subnetxmpp,
                "agenttype": machine.agenttype,
                "classutil": machine.classutil,
                "groupdeploy": machine.groupdeploy,
                "urlguacamole": machine.urlguacamole,
                "picklekeypublic": machine.picklekeypublic,
                "ad_ou_user": machine.ad_ou_user,
                "ad_ou_machine": machine.ad_ou_machine,
                "kiosk_presence": machine.kiosk_presence,
                "lastuser": machine.lastuser,
                "keysyncthing": machine.keysyncthing,
                "enabled": machine.enabled,
                "uuid_serial_machine": machine.uuid_serial_machine,
            }
        return result

    @DatabaseHelper._sessionm
    def getRelayServerfromjid(self, session, jid):
        relayserver = session.query(RelayServer).filter(
            RelayServer.jid.like("%s%%" % jid)
        )
        relayserver = relayserver.first()
        session.commit()
        session.flush()
        try:
            result = {
                "id": relayserver.id,
                "urlguacamole": relayserver.urlguacamole,
                "subnet": relayserver.subnet,
                "nameserver": relayserver.nameserver,
                "ipserver": relayserver.ipserver,
                "ipconnection": relayserver.ipconnection,
                "port": relayserver.port,
                "portconnection": relayserver.portconnection,
                "mask": relayserver.mask,
                "jid": relayserver.jid,
                "longitude": relayserver.longitude,
                "latitude": relayserver.latitude,
                "enabled": relayserver.enabled,
                "switchonoff": relayserver.switchonoff,
                "mandatory": relayserver.mandatory,
                "classutil": relayserver.classutil,
                "groupdeploy": relayserver.groupdeploy,
                "package_server_ip": relayserver.package_server_ip,
                "package_server_port": relayserver.package_server_port,
                "moderelayserver": relayserver.moderelayserver,
                "keysyncthing": relayserver.keysyncthing,
                "syncthing_port": relayserver.syncthing_port,
            }
        except Exception:
            result = {}
        return result

    @DatabaseHelper._sessionm
    def getRelayServerForMachineUuid(self, session, uuid):
        relayserver = (
            session.query(Machines).filter(Machines.uuid_inventorymachine == uuid).one()
        )
        session.commit()
        session.flush()
        try:
            result = {"uuid": uuid, "jid": relayserver.groupdeploy}
            for i in result:
                if result[i] is None:
                    result[i] = ""
        except Exception:
            result = {"uuid": uuid, "jid": ""}
        return result

    @DatabaseHelper._sessionm
    def getCountOnlineMachine(self, session):
        return (
            session.query(func.count(Machines.id))
            .filter(Machines.agenttype == "machine")
            .scalar()
        )

    @DatabaseHelper._sessionm
    def getRelayServerofclusterFromjidars(
        self, session, jid, moderelayserver=None, enablears=1
    ):
        # determine ARS id from jid
        relayserver = session.query(RelayServer).filter(RelayServer.jid == jid)
        relayserver = relayserver.first()
        session.commit()
        session.flush()
        if relayserver:
            notconfars = {
                relayserver.jid: [
                    relayserver.ipconnection,
                    relayserver.port,
                    relayserver.jid,
                    relayserver.urlguacamole,
                    0,
                    relayserver.syncthing_port,
                ]
            }
            # search for clusters where ARS is
            clustersid = session.query(Has_cluster_ars).filter(
                Has_cluster_ars.id_ars == relayserver.id
            )
            clustersid = clustersid.all()
            session.commit()
            session.flush()
            # search the ARS in the same cluster that ARS finds
            if clustersid:
                listcluster_id = [m.id_cluster for m in clustersid]
                ars = (
                    session.query(RelayServer)
                    .join(Has_cluster_ars, Has_cluster_ars.id_ars == RelayServer.id)
                    .join(Cluster_ars, Has_cluster_ars.id_cluster == Cluster_ars.id)
                )
                ars = ars.filter(Has_cluster_ars.id_cluster.in_(listcluster_id))
                if moderelayserver is not None:
                    ars = ars.filter(RelayServer.moderelayserver == moderelayserver)
                if enablears is not None:
                    ars = ars.filter(RelayServer.enabled == enablears)
                ars = ars.all()
                session.commit()
                session.flush()
                if ars:
                    try:
                        result2 = {
                            m.jid: [
                                m.ipconnection,
                                m.port,
                                m.jid,
                                m.urlguacamole,
                                0,
                                m.keysyncthing,
                                m.syncthing_port,
                            ]
                            for m in ars
                        }
                    except Exception:
                        result2 = {
                            m.jid: [
                                m.ipconnection,
                                m.port,
                                m.jid,
                                m.urlguacamole,
                                0,
                                "",
                                0,
                            ]
                            for m in ars
                        }
                    countarsclient = self.algoloadbalancerforcluster()
                    if len(countarsclient) != 0:
                        for i in countarsclient:
                            try:
                                if result2[i[1]]:
                                    result2[i[1]][4] = i[0]
                            except KeyError:
                                pass
                    return result2
            else:
                # there are no clusters configured for this ARS.
                logging.getLogger().warning(
                    "Cluster ARS [%s] no configured" % relayserver.jid
                )
                return notconfars
        else:
            logging.getLogger().warning("Relay server no present")
            logging.getLogger().warning("ARS not known for machine")
        return {}

    @DatabaseHelper._sessionm
    def algoloadbalancerforcluster(self, session):
        sql = """
            SELECT 
                COUNT(*) - 1 AS nb, `machines`.`groupdeploy`
            FROM
                xmppmaster.machines
            GROUP BY `machines`.`groupdeploy`
            HAVING nb != 0
                AND COALESCE(`machines`.`groupdeploy`, '') <> ''
            ORDER BY nb DESC;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [x for x in result]

    @DatabaseHelper._sessionm
    def get_machine_ad_infos(self, session, uuid_inventory):
        """
        Select the founded OUs of the logged machine.
        Param:
            uuid_inventory: str. This param is the uuid of the inventory
                                 of the machine received by xmpp.

        Returns:
            List of tuple. The tuple contains all the ou_machine and ou_user founded.
        """

        sql = """
        SELECT
            ad_ou_machine, ad_ou_user
        FROM
            machines
        WHERE
            uuid_inventorymachine = '%s';""" % (
            uuid_inventory
        )

        result = session.execute(sql)
        session.commit()
        session.flush()
        return [element for element in result]

    @DatabaseHelper._sessionm
    def get_machines_with_kiosk(self, session):
        """
        Select the machines with the kiosk installed.
        Returns:
            List of tuple. The tuple contains all the machines founded.
        """

        sql = """
        SELECT
            *
        FROM
            machines
        WHERE
            kiosk_presence = 'True';"""
        result = session.execute(sql)
        session.commit()
        session.flush()

        return [element for element in result]

    @DatabaseHelper._sessionm
    def substituteinfo(self, session, listconfsubstitute, arsname):
        """
        This function creates sorted lists of substitutes to configure machines.
        It uses the sum of every substitute and attribute the one with the less machines in. It is used for the load balancing.
        The calculation is done taking into consideration all the substitutes associated to the relay to which the machine is connected.

        Args:
            session: The SQL Alchemy session
            listconfsubstitute: The list of the substitutes in the machine configuration
            arsname: The ars where the machine is connected to.
        Returns:
        """
        incrementeiscount = []
        try:
            try:
                sql = """SELECT
                            `substituteconf`.`id` AS `id`,
                            `substituteconf`.`jidsubtitute` AS `jidsubtitute`,
                            `substituteconf`.`type` AS `type`,
                            SUM(`substituteconf`.`countsub`) AS `totsub`
                        FROM
                            `substituteconf`
                        WHERE
                            `substituteconf`.`jidsubtitute` IN (SELECT DISTINCT
                                    `substituteconf`.`jidsubtitute`
                                FROM
                                    `substituteconf`
                                WHERE
                                    `substituteconf`.`relayserver_id` IN (SELECT
                                            id
                                        FROM
                                            xmppmaster.relayserver
                                        WHERE
                                            jid LIKE ('%s')))
                        GROUP BY `substituteconf`.`jidsubtitute` , type
                        ORDER BY type , totsub;""" % (
                    arsname
                )
                resultproxy = session.execute(sql)
                session.commit()
                session.flush()
                for listconfsubstituteitem in listconfsubstitute["conflist"]:
                    # reinitialise les lists
                    listconfsubstitute[listconfsubstituteitem] = []
                for x in resultproxy:
                    if str(x[2]).startswith("master@pulse"):
                        continue
                    if x[2] not in listconfsubstitute:
                        listconfsubstitute["conflist"].append(x[2])
                        listconfsubstitute[x[2]] = []
                    listconfsubstitute[x[2]].append(x[1])
                    incrementeiscount.append(x[0])
                self.logger.debug("listconfsubstitute %s" % listconfsubstitute)
                self.logger.debug("incrementeiscount %s" % incrementeiscount)
            except Exception as e:
                self.logger.error(
                    "An error occured while fetching the ordered list of subsitutes."
                )
                self.logger.error(
                    "We hit the backtrace: \n%s" % (traceback.format_exc())
                )

            if incrementeiscount:
                sql = """UPDATE `xmppmaster`.`substituteconf`
                    SET
                        `countsub` = `countsub` + '1'
                    WHERE
                        `id` IN (%s);""" % ",".join(
                    [str(x) for x in incrementeiscount]
                )
                result = session.execute(sql)
                session.commit()
                session.flush()
        except Exception as e:
            logging.getLogger().error("substituteinfo : %s" % str(e))
            logging.getLogger().debug("substitute list : %s" % listconfsubstitute)
        return listconfsubstitute

    @DatabaseHelper._sessionm
    def get_public_key_of_ars(self, session, jids):
        try:
            query = select(RelayServer.jid, RelayServer.ssh_public_key).where(RelayServer.jid.in_(jids))
            result = session.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Voilà mon erreur {e}")
            return []
        return {row.jid: row.ssh_public_key for row in result}

    @DatabaseHelper._sessionm
    def GetMachine(self, session, jid):
        """
        Initialize boolean presence in table machines
        This function tells if the machine is present of not.
        Args:
            session: The SQL Alchemy session
            jid: The JID of the machine
        Returns:
            It returns None in case of error.
        """
        user = str(jid).split("@")[0]
        try:
            sql = (
                """SELECT
                        id, hostname, agenttype, need_reconf
                    FROM
                        `xmppmaster`.`machines`
                    WHERE
                        `xmppmaster`.`machines`.jid like('%s@%%')
                    LIMIT 1;"""
                % user
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            re = [x for x in result]
            if re:
                return re[0]
        except IndexError as index_error:
            logging.getLogger().error(
                "An index error occured while trying to set up online/offline machine: %s"
                % str(index_error)
            )
            return None
        except Exception as e:
            logging.getLogger().error(
                "An error occured while trying to set up online/offline machine: %s"
                % str(e)
            )
            return None

    @DatabaseHelper._sessionm
    def updateMachinereconf(self, session, jid, status=0):
        """
        update boolean need_reconf in table machines
        """
        user = self.jid_to_hostname(jid)
        if not user:
            logging.getLogger().error("SetPresenceMachine jid error : %s" % jid)
            return False
        try:
            sql = """UPDATE `xmppmaster`.`machines`
                         SET `need_reconf` = %s
                     WHERE
                         `xmppmaster`.`machines`.hostname like '%s' limit 1;""" % (
                status,
                user,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error("updateMachinereconf : %s" % str(e))
            return False

    @DatabaseHelper._sessionm
    def initialisePresenceMachine(self, session, jid, presence=0):
        """
        Initialize presence in table machines and relay
        """
        mach = self.GetMachine(jid)
        if mach is not None:
            self.SetPresenceMachine(jid, presence)
            if mach[2] != "machine":
                try:
                    sql = """UPDATE
                                `xmppmaster`.`relayserver`
                            SET
                                `xmppmaster`.`relayserver`.`enabled` = '%s'
                            WHERE
                                `xmppmaster`.`relayserver`.`nameserver` = '%s';""" % (
                        presence,
                        mach[1],
                    )
                    session.execute(sql)
                    session.commit()
                    session.flush()
                except Exception as e:
                    logging.getLogger().error("initialisePresenceMachine : %s" % str(e))
                finally:
                    return {"type": "relayserver", "reconf": mach[3]}
            else:
                return {"type": "machine", "reconf": mach[3]}
        else:
            self.logger.warning("absent %s Mach table" % (jid))
            return {}

    def jid_to_hostname(self, jid):
        try:
            user = jid.split("@")[0].split(".")
            if len(user) > 1:
                user = user[:-1]
        except Exception:
            return None
        user = ".".join(user)
        if not user:
            return None
        return user

    @DatabaseHelper._sessionm
    def SetPresenceMachine(self, session, jid, presence=0):
        """
        Change the presence in the machine table.
        Args:
            session: The SQL Alchemy session
            jid: The jid of the machine where we want to change the presence
            presence: The new presence state/
                      0: The machine is offline
                      1: The machine is online
        """
        user = self.jid_to_hostname(jid)
        if not user:
            logging.getLogger().error("SetPresenceMachine jid error : %s" % jid)
            return False
        try:
            sql = """UPDATE
                        `xmppmaster`.`machines`
                    SET
                        `xmppmaster`.`machines`.`enabled` = '%s'
                    WHERE
                        `xmppmaster`.`machines`.hostname like '%s' limit 1;""" % (
                presence,
                user,
            )
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception as error_presence:
            logging.getLogger().error(
                "An error occured while setting the new presence."
            )
            logging.getLogger().error("We got the error:\n %s" % str(error_presence))
            return False

    @DatabaseHelper._sessionm
    def updatedeployresultandstate(self, session, sessionid, state, result):
        try:
            jsonresult = json.loads(result)
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error("We failed to convert the result into a json format")
            self.logger.error("The string we failed to convert is: %s" % result)
            return -1

        if "descriptor" not in jsonresult:
            jsonresult["descriptor"] = {}

        if "sequence" not in jsonresult["descriptor"]:
            jsonresult["descriptor"]["sequence"] = {}

        if "info" not in jsonresult["descriptor"]:
            jsonresult["descriptor"]["info"] = {}

        jsonautre = copy.deepcopy(jsonresult)
        try:
            del jsonautre["descriptor"]
        except KeyError:
            pass
        try:
            del jsonautre["packagefile"]
        except KeyError:
            pass
        # DEPLOYMENT START
        try:
            deploysession = (
                session.query(Deploy).filter(Deploy.sessionid == sessionid).one()
            )
            if deploysession:
                if (
                    deploysession.result is None
                    or ("wol" in jsonresult and jsonresult["wol"] >= 1)
                    or (
                        "advanced" in jsonresult
                        and "syncthing" in jsonresult["advanced"]
                        and jsonresult["advanced"]["syncthing"] == 1
                    )
                ):
                    jsonbase = {
                        "infoslist": [jsonresult["descriptor"]["info"]],
                        "descriptorslist": [jsonresult["descriptor"]["sequence"]],
                        "otherinfos": [jsonautre],
                        "title": deploysession.title,
                        "session": deploysession.sessionid,
                        "macadress": deploysession.macadress,
                        "user": deploysession.login,
                    }
                else:
                    need_info = False
                    jsonbase = json.loads(deploysession.result)

                    if "infoslist" not in jsonbase:
                        jsonbase["infoslist"] = []
                        need_info = True

                    if "descriptorslist" not in jsonbase:
                        jsonbase["descriptorslist"] = []
                        need_info = True

                    if "otherinfos" not in jsonbase:
                        jsonbase["otherinfos"] = []
                        need_info = True

                    if need_info:
                        self.logger.info(
                            "The content of the uncomplete json file is \n %s"
                            % deploysession.result
                        )

                    jsonbase["infoslist"].append(jsonresult["descriptor"]["info"])
                    jsonbase["descriptorslist"].append(
                        jsonresult["descriptor"]["sequence"]
                    )
                    jsonbase["otherinfos"].append(jsonautre)
                deploysession.result = json.dumps(jsonbase, indent=3)
                if (
                    "infoslist" in jsonbase
                    and "otherinfos" in jsonbase
                    and len(jsonbase["otherinfos"]) > 0
                    and "plan" in jsonbase["otherinfos"][0]
                    and len(jsonbase["infoslist"])
                    != len(jsonbase["otherinfos"][0]["plan"])
                    and state == "DEPLOYMENT SUCCESS"
                ):
                    state = "DEPLOYMENT PARTIAL SUCCESS"
                regexpexlusion = re.compile(
                    "^(?!abort)^(?!success)^(?!error)", re.IGNORECASE
                )
                if regexpexlusion.match(state) is not None:
                    deploysession.state = state
            session.commit()
            session.flush()
            session.close()
            return 1
        except Exception as e:
            self.logger.error(str(e))
            self.logger.error(
                "function updatedeployresultandstate parameter debug error"
            )
            self.logger.error(
                "params debug :\nsession id : "
                "%s\nstate : %s\nresult deployement: %s\n"
                % (sessionid, state, pprint.pformat(jsonresult, indent=4))
            )
            self.logger.error("\n%s" % (traceback.format_exc()))
            return -1

    @DatabaseHelper._sessionm
    def get_syncthing_deploy_to_clean(self, session):
        sql = """
    SELECT
        distinct xmppmaster.syncthing_deploy_group.id,
        GROUP_CONCAT(xmppmaster.syncthing_machine.jidmachine) AS jidmachines,
        GROUP_CONCAT(xmppmaster.syncthing_machine.jid_relay) AS jidrelays,
        xmppmaster.syncthing_ars_cluster.numcluster,
        syncthing_deploy_group.directory_tmp
    FROM
        xmppmaster.syncthing_deploy_group
            INNER JOIN
        xmppmaster.syncthing_ars_cluster
            ON xmppmaster.syncthing_deploy_group.id = xmppmaster.syncthing_ars_cluster.fk_deploy
            INNER JOIN
        xmppmaster.syncthing_machine
            ON xmppmaster.syncthing_ars_cluster.fk_deploy = xmppmaster.syncthing_deploy_group.id
    WHERE
        xmppmaster.syncthing_deploy_group.dateend < NOW()
    GROUP BY xmppmaster.syncthing_ars_cluster.numcluster; """
        result = session.execute(sql)
        session.commit()
        session.flush()
        ret = [
            {
                "id": x[0],
                "jidmachines": x[1],
                "jidrelays": x[2],
                "numcluster": x[3],
                "directory_tmp": x[4],
            }
            for x in result
        ]
        return ret

    @DatabaseHelper._sessionm
    def get_ensemble_ars_idem_cluster(self, session, ars_id):
        sql = (
            """SELECT
                    jid, nameserver, keysyncthing
                FROM
                    xmppmaster.has_cluster_ars
                        INNER JOIN
                    xmppmaster.relayserver ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                WHERE
                    id_cluster = (SELECT
                            id_cluster
                        FROM
                            xmppmaster.has_cluster_ars
                        WHERE
                            id_ars = %s);"""
            % ars_id
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [
            {"jid": element[0], "name": element[1], "keysyncthing": element[2]}
            for element in result
        ]

    @DatabaseHelper._sessionm
    def get_list_ars_from_cluster(self, session, cluster=0):
        sql = (
            """SELECT jid, nameserver, keysyncthing  FROM xmppmaster.has_cluster_ars
                INNER JOIN
                xmppmaster.relayserver
                    ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                WHERE id_cluster = %s;"""
            % cluster
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [
            {"jid": element[0], "name": element[1], "keysyncthing": element[2]}
            for element in result
        ]

    @DatabaseHelper._sessionm
    def refresh_syncthing_deploy_clean(self, session, iddeploy):
        sql = (
            """DELETE FROM `xmppmaster`.`syncthing_deploy_group` WHERE  id= %s;"""
            % iddeploy
        )
        result = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def getRelayServer(self, session, enable=None):
        listrelayserver = []
        if enable is not None:
            relayservers = (
                session.query(RelayServer)
                .filter(and_(RelayServer.enabled == enable))
                .all()
            )
        else:
            relayservers = session.query(RelayServer).all()
        session.commit()
        session.flush()
        try:
            for relayserver in relayservers:
                res = {
                    "id": relayserver.id,
                    "urlguacamole": relayserver.urlguacamole,
                    "subnet": relayserver.subnet,
                    "nameserver": relayserver.nameserver,
                    "ipserver": relayserver.ipserver,
                    "ipconnection": relayserver.ipconnection,
                    "port": relayserver.port,
                    "portconnection": relayserver.portconnection,
                    "mask": relayserver.mask,
                    "jid": relayserver.jid,
                    "longitude": relayserver.longitude,
                    "latitude": relayserver.latitude,
                    "enabled": relayserver.enabled,
                    "switchonoff": relayserver.switchonoff,
                    "mandatory": relayserver.mandatory,
                    "classutil": relayserver.classutil,
                    "groupdeploy": relayserver.groupdeploy,
                    "package_server_ip": relayserver.package_server_ip,
                    "package_server_port": relayserver.package_server_port,
                    "moderelayserver": relayserver.moderelayserver,
                }
                listrelayserver.append(res)
            return listrelayserver
        except Exception as e:
            logging.getLogger().error(str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return listrelayserver

    @DatabaseHelper._sessionm
    def get_log_status(self, session):
        """
        get complete table
        """
        result = []
        try:
            ret = session.query(Def_remote_deploy_status).all()
            session.commit()
            session.flush()
            if ret is None:
                result = []
            else:
                result = [
                    {
                        "index": id,
                        "id": regle.id,
                        "regexplog": regle.regex_logmessage,
                        "status": regle.status,
                    }
                    for id, regle in enumerate(ret)
                ]
            return result
        except Exception as e:
            self.logger.error("\n%s" % (traceback.format_exc()))
            return result

    @DatabaseHelper._sessionm
    def call_reconfiguration_machine(self, session, limit=None, typemachine="machine"):
        if typemachine in ["machine", "relay"]:
            res = session.query(Machines.id, Machines.jid).filter(
                and_(
                    Machines.need_reconf == "1",
                    Machines.enabled == "1",
                    Machines.agenttype.like(typemachine),
                )
            )
        elif typemachine is None or typemachine == "all":
            res = session.query(Machines.id, Machines.jid).filter(
                and_(Machines.need_reconf == "1", Machines.enabled == "1")
            )
        if limit is not None:
            res = res.limit(int(limit))
        res = res.all()
        listjid = []
        if res is not None:
            for machine in res:
                listjid.append([machine.id, machine.jid])
        session.commit()
        session.flush()
        return listjid

    @DatabaseHelper._sessionm
    def call_acknowledged_reconficuration(self, session, listmachine=[]):
        listjid = []
        if len(listmachine) == 0:
            return listjid
        res = (
            session.query(Machines.id, Machines.need_reconf)
            .filter(and_(Machines.need_reconf == "0", Machines.id.in_(listmachine)))
            .all()
        )
        if res is not None:
            for machine in res:
                listjid.append(machine.id)
        session.commit()
        session.flush()
        return listjid

    @DatabaseHelper._sessionm
    def call_set_list_machine(self, session, listmachine=[], valueset=0):
        """
        initialise presence on list id machine
        """
        if len(listmachine) == 0:
            return False
        try:
            liststr = ",".join(["'%s'" % x for x in listmachine])

            sql = """UPDATE `xmppmaster`.`machines`
                    SET
                        `enabled` = '%s'
                    WHERE
                        `id` IN (%s);""" % (
                valueset,
                liststr,
            )
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error("call_set_list_machine: %s" % str(e))
            return False

    @DatabaseHelper._sessionm
    def setUptime_machine(
        self, session, hostname, jid, status=0, updowntime=0, date=None
    ):
        """
        This function allow to know the uptime of a machine
        Args:
            session: The sqlalchemy session
            hostname: The hostname of the machine
            jid: The jid of the machine
            status: The current status of the machine
                    Can be 1 or 0
                    0: The machine is offline
                    1: The machine is online
            uptime: The current uptime of the machine
        Returns:
            It returns the id of the machine
        """

        try:
            new_Uptime_machine = Uptime_machine()
            new_Uptime_machine.hostname = hostname
            new_Uptime_machine.jid = jid
            new_Uptime_machine.status = status
            new_Uptime_machine.updowntime = updowntime
            if date is not None:
                new_Uptime_machine.date = date
            session.add(new_Uptime_machine)
            session.commit()
            session.flush()
            return new_Uptime_machine.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def Update_version_agent_machine_md5(self, session, hostname, md5, version):
        """
        This function updates the md5 and the version of the agent in the uptime_machine
        table.
        Args:
            session: The sqlalchemy session
            hostname: The hostname of the machine
            md5: The md5 fingerprint of the agent.
            version: The version of the agent
        """

        try:
            sql = """
                UPDATE
                    `xmppmaster`.`uptime_machine`
                SET
                    `md5agentversion` = '%s',
                    `version` = '%s'
                WHERE
                    (id = (SELECT
                            id
                        FROM
                            xmppmaster.uptime_machine
                        WHERE
                            hostname LIKE '%s' AND status = 1
                        ORDER BY id DESC
                        LIMIT 1));""" % (
                md5,
                version,
                hostname,
            )
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception as e:
            logging.getLogger().error(
                "We failed to update the md5 and the version of the running agent for %s"
                % hostname
            )
            logging.getLogger().error("we encounterd the error: %s" % str(e))
            return False

    @DatabaseHelper._sessionm
    def last_event_presence_xmpp(self, session, jid, nb=1):
        """
        This function allow to obtain the last presence.
            Args:
                session: The sqlalchemy session
                jid: The jid of the machine
                nb: Number of evenements we look at

            Returns:
                It returns a dictionnary with:
                    id: The id of the machine
                    hostname: The hostname of the machine
                    status: The current status of the machine
                        Can be 1 or 0:
                            0: The machine is offline
                            1: The machine is online
                    updowntime:
                            The uptime if status is set to 0
                            The downtime if status is set to 1
                    date: The date we checked the informations
                    time: Unix time
        """
        try:
            sql = """SELECT
                    *,
                    UNIX_TIMESTAMP(date)
                FROM
                    xmppmaster.uptime_machine
                WHERE
                    jid LIKE '%s'
                ORDER BY id DESC
                LIMIT %s;""" % (
                jid,
                nb,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            return [
                {
                    "id": element[0],
                    "hostname": element[1],
                    "jid": element[2],
                    "status": element[3],
                    "updowntime": element[4],
                    "date": element[5].strftime("%Y/%m/%d/ %H:%M:%S"),
                    "time": element[6],
                }
                for element in result
            ]
        except Exception as e:
            logging.getLogger().error(str(e))
            return []

    # TODO: Add this function for hours too.
    #      Add in QA too.
    @DatabaseHelper._sessionm
    def stat_up_down_time_by_last_day(self, session, jid, day=1):
        """
        This function is used to know how long a machine is online/offline.
        It allow to know the number of start of this machine too.

        Args:
            session: The Sqlalchemy session
            jid: The jid of the machine
            day: The number of days for the count
        Returns:
            It returns a dictonary with :
                jid: The jid of the machine
                downtime: The time the machine has been down
                uptime: The time the machine has been running the agent
                nbstart: The number of start of the agent
                totaltime: The interval (in seconds) on which we count
        """
        statdict = {}
        statdict["machine"] = jid
        statdict["downtime"] = 0
        statdict["uptime"] = 0
        statdict["nbstart"] = 0
        statdict["totaltime"] = day * 86400
        try:
            sql = """SELECT
                    id, status, updowntime, date
                FROM
                    xmppmaster.uptime_machine
                WHERE
                        jid LIKE '%s'
                    AND
                        date > CURDATE() - INTERVAL %s DAY;""" % (
                jid,
                day,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
            # We set nb to false to not use the last informations
            # This would lead to errors.
            nb = False
            if result:
                for el in result:
                    if el.status == 0:
                        if statdict["nbstart"] > 0:
                            if nb:
                                statdict["uptime"] = statdict["uptime"] + el[2]
                            else:
                                nb = True
                    else:
                        statdict["nbstart"] = statdict["nbstart"] + 1
                        if nb:
                            statdict["downtime"] = statdict["downtime"] + el[2]
                        else:
                            nb = True
            return statdict
        except Exception as e:
            self.logger.error("\n%s" % (traceback.format_exc()))
            logging.getLogger().error(str(e))
            return statdict

    @DatabaseHelper._sessionm
    def setMonitoring_machine(
        self, session, machines_id, hostname, statusmsg="", date=None
    ):
        try:
            new_Monitoring_machine = Mon_machine()
            new_Monitoring_machine.machines_id = machines_id
            if date is not None:
                date = date.replace("T", " ").replace("Z", "")[:19]
                new_Monitoring_machine.date = date
            new_Monitoring_machine.hostname = hostname
            new_Monitoring_machine.statusmsg = statusmsg
            session.add(new_Monitoring_machine)
            session.commit()
            session.flush()
            return new_Monitoring_machine.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def setMonitoring_device(
        self,
        session,
        hostname,
        mon_machine_id,
        device_type,
        serial,
        firmware,
        status,
        alarm_msg,
        doc,
    ):
        try:
            # if device_type not in ['thermalPrinter',
            #'nfcReader',
            #'opticalReader',
            #'cpu',
            #'memory',
            #'storage',
            #'network',
            #'system']:
            # raise DomaineTypeDeviceError()
            if status not in ["ready", "busy", "warning", "error", "disable"]:
                raise DomainestatusDeviceError()
            new_Monitoring_device = Mon_devices()
            new_Monitoring_device.mon_machine_id = mon_machine_id
            new_Monitoring_device.device_type = device_type
            new_Monitoring_device.serial = serial
            new_Monitoring_device.firmware = firmware
            new_Monitoring_device.status = status
            new_Monitoring_device.alarm_msg = alarm_msg
            new_Monitoring_device.doc = doc
            session.add(new_Monitoring_device)
            session.commit()
            session.flush()
            return new_Monitoring_device.id
        except Exception as e:
            logging.getLogger().error(str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return -1

    @DatabaseHelper._sessionm
    def setMonitoring_device_reg(
        self,
        session,
        hostname,
        id_machine,
        platform,
        agenttype,
        statusmsg,
        xmppobject,
        msg_from,
        sessionid,
        mon_machine_id,
        device_type,
        serial,
        firmware,
        status,
        alarm_msg,
        doc,
    ):
        machine_hostname = msg_from.split("@")[0]
        result = None
        try:
            id_device_reg = self.setMonitoring_device(
                msg_from,
                mon_machine_id,
                device_type,
                serial,
                firmware,
                status,
                alarm_msg,
                doc,
            )
            # creation event on rule
            objectlist_local_rule = self._rule_monitoring(
                machine_hostname,
                hostname,
                id_machine,
                platform,
                agenttype,
                mon_machine_id,
                device_type,
                serial,
                firmware,
                status,
                alarm_msg,
                doc,
                localrule=True,
            )
            if objectlist_local_rule:
                # A rule is defined for this device on this machine
                result = self._action_new_event(
                    objectlist_local_rule,
                    statusmsg,
                    xmppobject,
                    msg_from,
                    sessionid,
                    mon_machine_id,
                    id_device_reg,
                    doc,
                    status_event=1,
                    hostname=hostname,
                )
            if result and result == -1:
                logging.getLogger().warning(
                    "treatment stop : alarm from  %s:" % msg_from
                )
                return -1
            logging.getLogger().debug("==================================")
            return id_device_reg
        except Exception as e:
            logging.getLogger().error(str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return -1

    @DatabaseHelper._sessionm
    def setMonitoring_event(
        self,
        session,
        machines_id,
        id_device,
        id_rule,
        cmd,
        type_event="log",
        status_event=1,
        parameter_other=None,
        ack_user=None,
        ack_date=None,
    ):
        try:
            new_Monitoring_event = Mon_event()
            new_Monitoring_event.machines_id = machines_id
            new_Monitoring_event.id_rule = id_rule
            new_Monitoring_event.id_device = id_device
            new_Monitoring_event.type_event = type_event
            new_Monitoring_event.cmd = cmd
            new_Monitoring_event.parameter_other = parameter_other
            new_Monitoring_event.ack_user = ack_user
            new_Monitoring_event.ack_date = ack_date
            session.add(new_Monitoring_event)
            session.commit()
            session.flush()
            return new_Monitoring_event.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def get_machine_information_id_device(self, session, id_mon_machine):
        sql = (
            """SELECT
                    statusmsg as mon_machine_statusmsg
                FROM
                    xmppmaster.mon_machine
                WHERE
                    id = %s limit 1;"""
            % id_mon_machine
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        if not result:
            return False
        resultmon_machine = [rowproxy._asdict() for rowproxy in result]
        resultmon_machine = resultmon_machine[0]

        if "mon_machine_statusmsg" in resultmon_machine and isinstance(
            resultmon_machine["mon_machine_statusmsg"], basestring
        ):
            try:
                resultmon_machine["mon_machine_statusmsg"] = json.loads(
                    resultmon_machine["mon_machine_statusmsg"]
                )
            except ValueError:
                return False
        return resultmon_machine

    @DatabaseHelper._sessionm
    def get_event_information_id_device(self, session, id_device):
        sql = """
            SELECT
            machines.id ,
            machines.jid as jid,
            machines.uuid_serial_machine as uuid_serial_machine,
            machines.platform as platform,
            machines.archi as archi,
            machines.hostname as machine_hostname,
            machines.uuid_inventorymachine,
            machines.ippublic as ippublic,
            machines.ip_xmpp as ip_xmpp,
            machines.macaddress as macaddress,
            machines.subnetxmpp as subnetxmpp,
            machines.agenttype as agenttype,
            machines.groupdeploy as groupdeploy,
            machines.urlguacamole as urlguacamole,
            machines.ad_ou_machine as ad_ou_machine,
            machines.ad_ou_user as ad_ou_user,
            machines.lastuser as lastuser,
            machines.glpi_description as glpi_description,
            machines.glpi_owner_firstname as glpi_owner_firstname,
            machines.glpi_owner_realname as glpi_owner_realname,
            machines.glpi_owner as glpi_owner,
            machines.model as model,
            machines.manufacturer as manufacturer,
            mon_event.id as mon_event_id,
            mon_event.status_event as mon_event_status_event,
            mon_event.type_event as mon_event_type_event,
            mon_event.cmd as mon_event_cmd,
            mon_event.id_rule as mon_event_id_rule ,
            mon_event.machines_id as mon_event_machines_id,
            mon_event.id_device as mon_event_id_device,
            mon_event.parameter_other as mon_event_parameter_other,
            mon_event.ack_user as mon_event_ack_user,
            mon_event.ack_date as mon_event_ack_date,
            mon_rules.id as mon_rules_id ,
            mon_rules.hostname as mon_rules_hostname,
            mon_rules.device_type as mon_rules_device_type,
            mon_rules.binding as mon_rules_binding,
            mon_rules.succes_binding_cmd as mon_rules_succes_binding_cmd,
            mon_rules.no_success_binding_cmd as mon_rules_no_success_binding_cmd,
            mon_rules.error_on_binding as mon_rules_error_on_binding,
            mon_rules.type_event as mon_rules_type_event,
            mon_rules.user as mon_rules_user,
            mon_rules.comment as mon_rules_comment,
            mon_machine.id as mon_machine_id,
            mon_machine.machines_id as mon_machine_machines_id,
            mon_machine.date as mon_machine_date,
            mon_machine.hostname as mon_machine_hostname,
            mon_machine.statusmsg as mon_machine_statusmsg,
            mon_devices.id as mon_devices_id,
            mon_devices.mon_machine_id as mon_devices_mon_machine_id ,
            mon_devices.device_type as mon_devices_device_type,
            mon_devices.serial as mon_devices_serial,
            mon_devices.firmware as mon_devices_firmware,
            mon_devices.status asmon_devices_status,
            mon_devices.alarm_msg as mon_devices_alarm_msg,
            mon_devices.doc as mon_devices_doc,
            machines.hostname as machine_hostname
            FROM
                xmppmaster.mon_event
                    JOIN
                xmppmaster.mon_rules ON xmppmaster.mon_rules.id = xmppmaster.mon_event.id_rule
                    JOIN
                xmppmaster.mon_machine ON xmppmaster.mon_machine.id = xmppmaster.mon_event.machines_id
                    JOIN
                xmppmaster.mon_devices ON xmppmaster.mon_devices.id = xmppmaster.mon_event.id_device
                JOIN
                xmppmaster.machines ON xmppmaster.machines.id = xmppmaster.mon_machine.machines_id
            WHERE
                xmppmaster.mon_event.id = %s;""" % (
            id_device
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        if not result:
            return {}
        resultproxy = [rowproxy._asdict() for rowproxy in result]
        resultproxy = resultproxy[0]
        resultproxy["mon_param0"] = ""
        resultproxy["mon_subject"] = ""
        resultproxy["mon_status"] = ""
        if "mon_machine_statusmsg" in resultproxy and isinstance(
            resultproxy["mon_machine_statusmsg"], basestring
        ):
            try:
                resultproxy["mon_machine_statusmsg"] = json.loads(
                    resultproxy["mon_machine_statusmsg"]
                )
                if "mon_param0" in resultproxy["mon_machine_statusmsg"]:
                    resultproxy["mon_param0"] = resultproxy["mon_machine_statusmsg"][
                        "mon_param0"
                    ]
                if "mon_subject" in resultproxy["mon_machine_statusmsg"]:
                    resultproxy["mon_subject"] = resultproxy["mon_machine_statusmsg"][
                        "mon_subject"
                    ]
                if "mon_status" in resultproxy["mon_machine_statusmsg"]:
                    resultproxy["mon_status"] = resultproxy["mon_machine_statusmsg"][
                        "mon_status"
                    ]
            except ValueError:
                pass

        if "mon_devices_doc" in resultproxy and isinstance(
            resultproxy["mon_devices_doc"], basestring
        ):
            try:
                resultproxy["mon_devices_doc"] = json.loads(
                    resultproxy["mon_devices_doc"]
                )
            except ValueError:
                pass
        if "mon_rules_comment" in resultproxy and isinstance(
            resultproxy["mon_rules_comment"], basestring
        ):
            try:
                resultproxy["mon_rules_comment"] = (
                    resultproxy["mon_rules_comment"]
                    .replace("\\t", "")
                    .replace("\\n", "")
                    .replace('\\"', '"')
                )

                resultproxy["mon_rules_comment"] = json.loads(
                    resultproxy["mon_rules_comment"]
                )
            except ValueError:
                pass

        return resultproxy

    @DatabaseHelper._sessionm
    def get_info_event(self, session, id_device, outformat=None):
        def is_number_string(s):
            """Returns True is string is a number."""
            try:
                float(s)
                return True
            except ValueError:
                return False

        def is_integer_string(s):
            if is_number_string(s):
                try:
                    int(s)
                    return True
                except ValueError:
                    return False
            else:
                return False

        def is_float_string(s):
            if is_number_string(s):
                try:
                    int(s)
                    return False
                except ValueError:
                    return True
            else:
                return False

        keys = [
            "mon_event_id",
            "mon_event_status_event",
            "mon_event_type_event",
            "mon_event_cmd",
            "mon_event_id_rule",
            "mon_event_machines_id",
            "mon_event_id_device",
            "mon_event_parameter_other",
            "mon_event_ack_user",
            "mon_event_ack_date",
            "mon_rules_id",
            "mon_rules_hostname",
            "mon_rules_device_type",
            "mon_rules_binding",
            "mon_rules_succes_binding_cmd",
            "mon_rules_no_success_binding_cmd",
            "mon_rules_error_on_binding",
            "mon_rules_type_event",
            "mon_rules_user",
            "mon_rules_comment",
            "mon_machine_id",
            "mon_machine_machines_id",
            "mon_machine_date",
            "mon_machine_hostname",
            "mon_machine_statusmsg",
            "mon_devices_id",
            "mon_devices_mon_machine_id",
            "mon_devices_device_type",
            "mon_devices_serial",
            "mon_devices_firmware",
            "mon_devices_status",
            "mon_devices_alarm_msg",
            "mon_devices_doc",
        ]
        resultproxy = self.get_event_information_id_device(id_device)
        resultjsonstr = json.dumps(resultproxy, indent=4, cls=DateTimeEncoder)

        python_dict = resultproxy
        if outformat is None:
            return python_dict
        # serialization for remplace in script
        if outformat == "json_string":
            return resultjsonstr
        elif outformat == "pickle_string":
            import pickle

            return pickle.dumps(python_dict)
        elif outformat == "cgi_string":
            import urllib

            return urllib.urlencode(python_dict)
        elif outformat == "bash_string":
            # creation string parameter for bash script.
            return self._template_bash_string_event(python_dict)
        elif outformat == "python_string":
            # creation string parameter for bash script.
            return self._template_python_string_event(python_dict)
        elif outformat == "html_string":
            # return string html format event
            return self._template_html_event(python_dict)
        else:
            return ""

    def replace_in_file_exist_template(self, srcfile, destfile, oldvalue, newvalue):
        fin = open(srcfile, "rt")
        data = fin.read()
        data = data.replace(oldvalue, newvalue)
        fin.close()
        fin = open(srcfile, "wt")
        fin.write(data)
        fin.close()

    def replace_in_file_template(self, srcfile, destfile, oldvalue, newvalue):
        fin = open(srcfile, "rt")
        fout = open(destfile, "wt")
        # for each line in the input file
        for line in fin:
            # read replace the string and write to output file
            fout.write(line.replace(oldvalue, newvalue))
        # close input and output files
        fin.close()
        fout.close()

    def replace_in_file_template1(self, srcfile, destfile, oldvalue, newvalue):
        fin = open(srcfile, "rt")
        completfile = fin.read()
        fin.close()
        completfile.replace(oldvalue, newvalue)
        fout = open(destfile, "wt")
        fout.write(completfile)
        fout.close()
        return completfile

    def _template_bash_string_event(self, python_dict):
        bash_string = ""
        for t in python_dict:
            bash_string = bash_string + "%s=%s\n" % (t, python_dict[t])
        return bash_string

    def _template_python_string_event(self, python_dict):
        # creation string parameter for bash script.
        python_string = ""

        def is_number_string(s):
            """Returns True is string is a number."""
            try:
                float(s)
                return True
            except ValueError:
                return False

        for t in python_dict:
            valor = python_dict[t]
            if isinstance(valor, basestring):
                if is_number_string(valor):
                    python_string = python_string + "%s = %s \n" % (t, valor)
                else:
                    valor = python_dict[t].replace('"', '\\"')
                    python_string = python_string + '%s = "%s" \n' % (t, valor)
            else:
                python_string = python_string + "%s = %s \n" % (t, valor)
            python_string = python_string.replace('"None"', "None")
            python_string = python_string.replace('"false"', "False")
            python_string = python_string.replace('"true"', "True")
            python_string = python_string.replace('"null"', "None")
            python_string = python_string.replace('"NULL"', "None")
        return python_string

    def _template_html_event(self, dictresult):
        templateevent = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title></title>
<style type="text/css">
table {
border:3px solid #6495ed;
border-collapse:collapse;
width:90%;
margin:auto;
}
thead, tfoot {
background-color:#D0E3FA;
background-image:url(sky.jpg);
border:1px solid #6495ed;
}
tbody {
background-color:#FFFFFF;
border:1px solid #6495ed;
}
th {
font-family:monospace;
border:1px dotted #6495ed;
padding:5px;
background-color:#EFF6FF;
width:25%;
}
td {
font-family:sans-serif;
font-size:80%;
border:1px solid #6495ed;
padding:5px;
text-align:left;
}
caption {
font-family:sans-serif;
}

</style>
</head>
<body>

<h1>ALERT @mon_devices_device_type@ : e.
</h1>
<h2>MAchine @mon_machine_hostname@</h2>

<!-- DEVICE INFORMATION -->
<!-- mon_devices_mon_machine_id = @mon_devices_mon_machine_id@ -->
<!-- mon_devices_doc = @mon_devices_doc@ -->
<!-- mon_devices_status = @mon_devices_status@ -->
<!-- mon_devices_device_type = @mon_devices_device_type@ -->
<!-- mon_devices_firmware = @mon_devices_firmware@ -->
<!-- mon_devices_alarm_msg = @mon_devices_alarm_msg@ -->
<!-- mon_devices_serial = @mon_devices_serial@ -->
<!-- mon_devices_id = @mon_devices_id@ -->
<table>
  <!-- <caption>Device information</caption> -->
   <thead>
        <tr>
            <th colspan="5">DEVICE</th>
        </tr>
    </thead>
  <tbody>
    <tr>
      <th scope="col">status</th>
      <th scope="col">firmware</th>
      <th scope="col">serial</th>
      <th scope="col">alarm_msg</th>
      <th scope="col">retour</th>
    </tr>
    <tr>
      <td>@mon_devices_status@</td>
      <td>@mon_devices_firmware@</td>
      <td>@mon_devices_serial@</td>
      <td>@mon_devices_alarm_msg@</td>
      <td><code>@mon_devices_doc@</code></td>
    </tr>
  </tbody>
</table>

<!-- MACHINES INFORMATION -->
<!-- mon_machine_hostname = @mon_machine_hostname@
mon_machine_statusmsg =@mon_machine_statusmsg@
mon_machine_date = @mon_machine_date@
mon_machine_id = @mon_machine_id@
mon_machine_machines_id = @mon_machine_machines_id@ -->

<table>
  <!-- <caption>Device information</caption> -->
   <thead>
        <tr>
            <th colspan="2">MACHINE</th>
        </tr>
    </thead>
  <tbody>
    <tr>
      <th scope="col">host</th>
      <th scope="col">date</th>
    </tr>
    <tr>

      <td>@mon_machine_hostname@</td>
      <td>@mon_machine_date@</td>
    </tr>
  </tbody>
</table>

<!-- EVENT INFORMATION -->
<!-- mon_event_type_event = @mon_event_type_event@
mon_event_id = @mon_event_id@
mon_event_cmd = @mon_event_cmd@
mon_event_status_event = @mon_event_status_event@
mon_event_machines_id = @mon_event_machines_id@
mon_event_id_device = @mon_event_id_device@
mon_event_id_rule = @mon_event_id_rule@
mon_event_ack_date = @mon_event_ack_date@
mon_event_parameter_other = @mon_event_parameter_other@
mon_event_ack_user = @mon_event_ack_user@ -->

<!-- RULES INFORMATION -->
<!-- mon_rules_user = @mon_rules_user@
mon_rules_error_on_binding = @mon_rules_error_on_binding@
mon_rules_id = @mon_rules_id@
mon_rules_hostname = @mon_rules_hostname@
mon_rules_succes_binding_cmd = @mon_rules_succes_binding_cmd@
mon_rules_comment = @mon_rules_comment@
mon_rules_binding = @mon_rules_binding@
mon_rules_type_event = @mon_rules_type_event@
mon_rules_device_type = @mon_rules_device_type@
mon_rules_no_success_binding_cmd = @mon_rules_no_success_binding_cmd@ -->

<table>
  <!-- <caption>Device information</caption> -->
   <thead>
        <tr>
            <th colspan="4">RULES</th>
        </tr>
    </thead>
  <tbody>
    <tr>
      <th scope="col">Type</th>
      <th scope="col">comments</th>
      <th scope="col">BINDING</th>
    </tr>
    <tr>
      <td>@mon_rules_type_event@</td>
      <td>@mon_rules_comment@</td>
      <td><code>@mon_rules_binding@</code></td>
    </tr>
  </tbody>
</table>

</body>
</html>"""
        for t in dictresult:
            search = "@%s@" % t
            templateevent = templateevent.replace(search, str(dictresult[t]))
        return templateevent

    def _load_file_to_datas(self, path_file):
        try:
            if os.path.exists(path_file):
                with open(path_file, "rb") as f:
                    data = f.read()
                return base64.b64encode(zlib.compress(data, 9))

            return None
        except:
            return None

    def _action_new_event(
        self,
        objectlist_local_rule,
        statusmsg,
        xmppobject,
        msg_from,
        sessionid,
        id_machine,
        id_device,
        doc,
        status_event=1,
        hostname=None,
    ):
        keysreplace = statusmsg.keys()
        if "mon_param0" in keysreplace:
            mon_statusmsg_param0 = statusmsg["mon_param0"]
        if "mon_subject" in keysreplace:
            mon_statusmsg_subject = statusmsg["mon_subject"]
        other_data = None
        if "other_data" in keysreplace:
            if isinstance(statusmsg["other_data"], basestring):
                other_data = statusmsg["other_data"]
            else:
                other_data = json.dumps(statusmsg["other_data"])
        if objectlist_local_rule:
            # apply binding to find out if an alert or event is defined
            # resultproxy = self.get_event_information_id_device(idevent)
            index = 1
            for z in objectlist_local_rule:
                index = index + 1
                for rep in keysreplace:
                    keyre = "@%s@" % rep
                    if statusmsg[rep]:
                        if isinstance(statusmsg[rep], basestring):
                            z["binding"] = z["binding"].replace(keyre, statusmsg[rep])
                            # Replace the cmd if it exists
                            if isinstance(z["no_success_binding_cmd"], basestring):
                                z["no_success_binding_cmd"] = z[
                                    "no_success_binding_cmd"
                                ].replace(keyre, statusmsg[rep])
                            if isinstance(z["succes_binding_cmd"], basestring):
                                z["succes_binding_cmd"] = z[
                                    "succes_binding_cmd"
                                ].replace(keyre, statusmsg[rep])
                            if isinstance(z["error_on_binding"], basestring):
                                z["error_on_binding"] = z["error_on_binding"].replace(
                                    keyre, statusmsg[rep]
                                )
                        else:
                            stringreplace = json.dumps(statusmsg[rep])
                            z["binding"] = z["binding"].replace(keyre, stringreplace)
                            if isinstance(z["no_success_binding_cmd"], basestring):
                                z["no_success_binding_cmd"] = z[
                                    "no_success_binding_cmd"
                                ].replace(keyre, stringreplace)
                            if isinstance(z["succes_binding_cmd"], basestring):
                                z["succes_binding_cmd"] = z[
                                    "succes_binding_cmd"
                                ].replace(keyre, stringreplace)
                            if isinstance(z["error_on_binding"], basestring):
                                z["error_on_binding"] = z["error_on_binding"].replace(
                                    keyre, stringreplace
                                )
                # Verify if the binding is not a template
                testkeytemplate = []
                for rep in keysreplace:
                    keyre = "@%s@" % rep
                    if keyre in z["binding"]:
                        testkeytemplate.append(keyre)
                if testkeytemplate:
                    self.logger.warning(
                        "No treatment resolution template binding impossible on key %s"
                        % testkeytemplate
                    )
                    self.logger.warning(
                        "rule %s : event type : [%s] on device '%s'"
                        % (z["id"], str(z["type_event"]), str(z["device_type"]))
                    )
                    self.logger.warning(
                        "machine [%s] mon_machine id [%s] id_device [%s]"
                        % (msg_from, id_machine, id_device)
                    )
                    continue
                self.logger.debug(
                    "rule %s : event type : %s on device %s"
                    % (z["id"], str(z["type_event"]), str(z["device_type"]))
                )
                bindingcmd = ""
                msg, result = self.__binding_application_check(
                    doc, z["binding"], z["device_type"]
                )
                if result == -1:
                    if (
                        z["error_on_binding"] is None
                        or z["error_on_binding"].strip() == ""
                    ):
                        # There is not treatment done on errors.
                        continue
                    else:
                        self.logger.warning(
                            "We failed to process the binding. We got the error:  %s "
                            % msg
                        )
                        self.logger.debug("The content of the binding is: %s " % z)

                        bindingcmd = z["error_on_binding"]
                        continue
                elif result == 1:
                    # alert True
                    # create event if action associated to true
                    if (
                        z["succes_binding_cmd"] is None
                        or z["succes_binding_cmd"].strip() == ""
                    ):
                        # There is not treatment done on success.
                        continue
                    else:
                        # 1 event to handle
                        self.logger.debug(
                            "The treatment of the binding succeeded with the message:  %s "
                            % msg
                        )
                        self.logger.debug(
                            "The content of the sucessful binding is: %s " % z
                        )

                        bindingcmd = z["succes_binding_cmd"]
                elif result == 0:
                    # alert False
                    # create event if action associated to False
                    if (
                        z["no_success_binding_cmd"] is None
                        or z["no_success_binding_cmd"].strip() == ""
                    ):
                        self.logger.warning(
                            "No treatment on" " expected no success  %s " % (z)
                        )
                        continue
                    else:
                        self.logger.debug("no_success_binding_cmd  %s " % msg)
                        self.logger.debug(
                            "The content of the 'expecting to fail binding' is: %s " % z
                        )

                        bindingcmd = z["no_success_binding_cmd"]
                else:
                    # This case is not yet handled
                    self.logger.warning(
                        "No treatment on" "missing on def binding action%s " % (z)
                    )
                    continue

                idevent = self.setMonitoring_event(
                    id_machine,
                    id_device,
                    z["id"],
                    bindingcmd,
                    type_event=z["type_event"],
                    status_event=1,
                    parameter_other=other_data,
                )
                self.logger.debug(
                    "%s create event %s [%s]"
                    % (z["device_type"], z["type_event"], idevent)
                )
                # traitement event
                script_monitoring = os.path.join(
                    "/", "var", "lib", "pulse2", "script_monitoring"
                )
                if not os.path.exists(script_monitoring):
                    os.makedirs(script_monitoring)
                tmpprocessmonitoring = os.path.join(
                    "/", "var", "lib", "pulse2", "tmpprocessmonitoring"
                )
                if not os.path.exists(tmpprocessmonitoring):
                    os.makedirs(tmpprocessmonitoring)
                namescript = "%s_%s_%s_%s" % (
                    id_device,
                    z["type_event"],
                    getRandomName(5, pref=datetime.now().strftime("%a_%d%b%Y_%Hh%M")),
                    bindingcmd,
                )
                dest_script = os.path.join(tmpprocessmonitoring, namescript)
                if bindingcmd != "":
                    paramsubs = copy.deepcopy(vars(self.config))
                    listkeyconf = paramsubs.keys()
                    src_script = os.path.join(script_monitoring, bindingcmd)
                    resultproxy = self.get_event_information_id_device(idevent)
                    try:
                        resultproxy["mon_statusmsg_param0"] = mon_statusmsg_param0
                    except:
                        pass
                    try:
                        resultproxy["mon_statusmsg_subject"] = mon_statusmsg_subject
                    except:
                        pass
                    resultproxy["conf_submon"] = {}
                    for keyparam in listkeyconf:
                        resultproxy["conf_submon"][keyparam] = paramsubs[keyparam]
                    resultproxy["msg_from"] = msg_from
                    resultproxy["session_id"] = sessionid
                    resultproxy["hostname"] = hostname
                    resultproxy["status_event"] = status_event
                    resultproxy["submon"] = xmppobject.boundjid.bare
                    resultproxy["src_script"] = src_script
                    resultproxy["dest_script"] = dest_script
                    resultproxy["mysqlxmpp_dbuser"] = self.config.xmpp_dbuser
                    resultproxy["mysqlxmpp_dbhost"] = self.config.xmpp_dbhost
                    resultproxy["mysqlxmpp_dbport"] = self.config.xmpp_dbport
                    resultproxy["mysqlxmpp_dbname"] = self.config.xmpp_dbname
                    resultproxy["mysqlxmpp_dbpoolrecycle"] = (
                        self.config.xmpp_dbpoolrecycle
                    )
                    resultproxy["mysqlxmpp_dbpoolsize"] = self.config.xmpp_dbpoolsize
                    resultproxy["mysqlxmpp_dbpooltimeout"] = (
                        self.config.xmpp_dbpooltimeout
                    )
                    resultproxy["start_script"] = datetime.now().strftime(
                        "%a_%d%b%Y_%Hh%M"
                    )
                    if z["type_event"] == "ack":
                        self.logger.debug("ack event %s" % idevent)
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "ack_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- ACK --------\n"
                                "evenement id : %s\n" % (idevent)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")
                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "log":
                        self.logger.debug("log event %s" % idevent)
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "log_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- log --------\n"
                                "evenement id : %s\n" % (idevent)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")
                        msglog = "from %s log  %s" % (str(msg_from), z)
                        self.logger.info(msglog)
                        xmppobject.xmpplog(
                            msglog,
                            type="noset",
                            sessionname="",
                            priority=0,
                            action="xmpplog",
                            who=str(msg_from),
                            how="Remote",
                            why="",
                            module="Monitoring | Notify",
                            fromuser="",
                            touser="",
                        )
                        continue
                    elif (
                        z["type_event"] == "script_python"
                        and os.path.isfile(src_script)
                        and bindingcmd.endswith("py")
                    ):
                        self.logger.debug("script_python event %s" % idevent)
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "script_python_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        self.replace_in_file_template(
                            src_script,
                            dest_script,
                            "@@@@@event@@@@@",
                            serializeinformationjson,
                        )
                        self.replace_in_file_exist_template(
                            dest_script, dest_script, "@@@@@msgfrom@@@@@", str(msg_from)
                        )
                        self.replace_in_file_exist_template(
                            dest_script,
                            dest_script,
                            "@@@@@binding@@@@@",
                            str(bindingcmd),
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- script  python --------\n"
                                "evenement id : %s \n"
                                "script name  : %s\n" % (idevent, dest_script)
                            )
                            out.write("\n--------- pid cmd ---------\n")
                            pid = subprocess.Popen(
                                ["python", dest_script],
                                stdin=None,
                                stdout=out,
                                stderr=out,
                            ).pid
                            out.write("pid : %s\n" % pid)
                            self.logger.debug(
                                "call script pid %s : %s " % (pid, bindingcmd)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")

                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "script_remote" and os.path.isfile(
                        src_script
                    ):
                        self.logger.debug("script_remote %s" % idevent)

                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "script_remote_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        self.replace_in_file_template(
                            src_script,
                            dest_script,
                            "@@@@@event@@@@@",
                            serializeinformationjson,
                        )
                        type_script = z["user"].strip()
                        if z["user"].strip() == "":
                            type_script = "python"
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- script %s--------\n"
                                "evenement id : %s \n"
                                "script name  : %s\n"
                                % (type_script, idevent, dest_script)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write(
                                "\n--------- send "
                                "script remote machine %s"
                                "---------\n"
                                % datetime.now().strftime("%a_%d%b%Y_%Hh%M")
                            )
                            out.write("\nsend script %s:" % dest_script)
                            script_sending = self._load_file_to_datas(dest_script)

                            if script_sending is not None:
                                message_to_send = {
                                    "action": "remote_script_monitoring",
                                    "sessionid": sessionid,
                                    "base64": False,
                                    "ret": 0,
                                    "data": {
                                        "file_result": namefileout,
                                        "script_data": script_sending,
                                        "name_script": os.path.basename(dest_script),
                                        "type_script": type_script,
                                    },
                                }
                                out.write(
                                    "\n--------- Waiting Result from %s ------------\n"
                                    % str(msg_from)
                                )
                                xmppobject.send_message(
                                    mto=str(msg_from),
                                    mbody=json.dumps(
                                        message_to_send, cls=DateTimeEncoder
                                    ),
                                    mtype="chat",
                                )
                        self.update_status_event(idevent)
                        continue
                    elif (
                        z["type_event"] == "email"
                        and os.path.isfile(src_script)
                        and bindingcmd.endswith("py")
                    ):
                        self.logger.debug("email event %s" % idevent)
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        toemail = ""
                        if "mon_rules_user" in resultproxy:
                            nameto = resultproxy["mon_rules_user"].split("@")[0]
                            toemail = resultproxy["mon_rules_user"]

                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "email_%s_%s_%s_to_%s.txt"
                            % (rd, idevent, msgfrom, toemail),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        # We copy the python script in tmpprocessmonitoring for this event.
                        self.replace_in_file_template(
                            src_script,
                            dest_script,
                            "@@@@@event@@@@@",
                            serializeinformationjson,
                        )
                        self.replace_in_file_exist_template(
                            dest_script,
                            dest_script,
                            "@@@@@to_addrs_string@@@@@",
                            z["user"],
                        )
                        self.replace_in_file_exist_template(
                            dest_script,
                            dest_script,
                            "@@@@@paramcompte@@@@@",
                            z["comment"],
                        )
                        self.replace_in_file_exist_template(
                            dest_script, dest_script, "@@@@@msgfrom@@@@@", str(msg_from)
                        )
                        self.replace_in_file_exist_template(
                            dest_script,
                            dest_script,
                            "@@@@@binding@@@@@",
                            str(bindingcmd),
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- email  python --------\n"
                                "to : %s \n"
                                "evenement id : %s \n"
                                "script email  : %s\n"
                                % (resultproxy["mon_rules_user"], idevent, dest_script)
                            )
                            out.write("\n--------- pid cmd ---------\n")
                            pid = subprocess.Popen(
                                ["python", dest_script],
                                stdin=None,
                                stdout=out,
                                stderr=out,
                            ).pid
                            out.write("pid : %s\n" % pid)
                            self.logger.debug(
                                "call script  pid %s : %s " % (pid, bindingcmd)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")

                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "json_bash" and os.path.isfile(src_script):
                        self.logger.debug("json_bash event%s" % idevent)
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "json_bash_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        serializeinformationjsonsh = serializeinformationjson.replace(
                            "'", "'"
                        )
                        self.replace_in_file_template(
                            src_script,
                            dest_script,
                            "@@@@@event@@@@@",
                            serializeinformationjson,
                        )
                        self.replace_in_file_exist_template(
                            dest_script, dest_script, "@@@@@msgfrom@@@@@", str(msg_from)
                        )
                        self.replace_in_file_exist_template(
                            dest_script,
                            dest_script,
                            "@@@@@binding@@@@@",
                            str(bindingcmd),
                        )

                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- json_bash --------\n"
                                "evenement id : %s \n"
                                "script name  : %s\n" % (idevent, dest_script)
                            )
                            out.write("\n--------- pid cmd ---------\n")
                            pid = subprocess.Popen(
                                ["/bin/bash", dest_script],
                                stdin=None,
                                stdout=out,
                                stderr=out,
                            ).pid
                            out.write("pid : %s\n" % pid)
                            self.logger.debug(
                                "call script  pid %s : %s " % (pid, bindingcmd)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")

                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "xmppmsg":
                        self.logger.debug("xmppmsg event%s" % idevent)
                        resultproxy["program"] = resultproxy["src_script"]
                        del resultproxy["src_script"]
                        del resultproxy["dest_script"]
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "xmppmsg_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- xmppmsg --------\n"
                                "evenement id : %s\n" % (idevent)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write(
                                "\n--------- sent message to %s ---------\n"
                                % resultproxy["jid"]
                            )

                        progran = "python3 %s" % resultproxy["program"]
                        param = base64.b64encode(serializeinformationjson)
                        cmd = "python3 %s '%s'" % (resultproxy["program"], param)
                        message_to_send = simplecommandstr(cmd)["result"].replace(
                            "\n\n", "\n"
                        )
                        if "ERROR_MESSAGE_XMPP" not in message_to_send:
                            self.logger.debug(
                                "send message to send  : %s " % (str(msg_from))
                            )
                            xmppobject.send_message(
                                mto=str(msg_from), mbody=message_to_send, mtype="chat"
                            )
                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "cmd terminal":
                        self.logger.debug("cmd terminal event %s" % idevent)
                        cmd = bindingcmd
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "cmd_terminal_%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout

                        for t in resultproxy:
                            # We replace in the command if we find a value for @namevariable@
                            if isinstance(t, basestring):
                                search = "@%s@" % t
                                cmd = cmd.replace(search, str(resultproxy[t]))
                        if z["user"] is None or z["user"].strip() == "":
                            z["user"] = "root"
                        if z["user"] != "root":
                            cmd = bindingcmd.replace('"', '\\"')
                            cmd = """/bin/su - %s -c "%s" """ % (z["user"], cmd)
                        self.logger.debug("command %s" % (cmd))
                        resultproxy["command"] = cmd
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n-------- cmd terminal --------\n"
                                "evenement id : %s \n"
                                "command  : %s\n" % (idevent, cmd)
                            )
                            out.write("\n--------- pid cmd ---------\n")
                            pid = subprocess.Popen(
                                [cmd], shell=True, stdin=None, stdout=out, stderr=out
                            ).pid
                            out.write("pid : %s\n" % pid)
                            self.logger.debug(
                                "call script  pid %s : %s " % (pid, bindingcmd)
                            )
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- out cmd ------------\n")
                        self.update_status_event(idevent)
                        continue
                    elif z["type_event"] == "cmd remote terminal":
                        self.logger.debug("cmd remote terminal %s" % idevent)
                        cmd = bindingcmd
                        rd = "%s" % time.time()
                        msgfrom = "%s" % msg_from.split("/")[0]
                        namefileout = os.path.join(
                            tmpprocessmonitoring,
                            "cmd_remote_terminal%s_%s_%s.txt" % (rd, idevent, msgfrom),
                        )
                        resultproxy["namefileout"] = namefileout

                        for t in resultproxy:
                            # We replace in the command if we find a value for @namevariable@
                            if isinstance(t, basestring):
                                search = "@%s@" % t
                                cmd = cmd.replace(search, str(resultproxy[t]))
                        self.logger.debug("command %s" % (cmd))
                        namefilelog = resultproxy["jid"]
                        resultproxy["command_remote"] = cmd
                        serializeinformationjson = json.dumps(
                            resultproxy, indent=4, cls=DateTimeEncoder
                        )
                        with open(namefileout, "ab") as out:
                            out.write(
                                "\n------- cmd remote terminal -------\n"
                                "cmd on %s \n"
                                "evenement id%s \n"
                                "command : %s\n" % (resultproxy["jid"], idevent, cmd)
                            )
                            try:
                                result1 = xmppobject.iqsendpulse(
                                    resultproxy["jid"],
                                    {
                                        "action": "remotecommandshell",
                                        "data": cmd,
                                        "timeout": 1,
                                    },
                                    1,
                                )
                                outcmd = json.loads(result1)
                                outcmd = json.dumps(
                                    outcmd, indent=4, cls=DateTimeEncoder
                                )
                                out.write("\n--------- out cmd ---------\n")
                                out.write("\n%s" % outcmd)
                            except:
                                msgerror = "\n%s" % (traceback.format_exc())
                                self.logger.error("%s" % msgerror)
                                out.write("\n--------- out error cmd ---------\n")
                                self.logger.error("result1 %s" % msgerror)
                            out.write("\n--------- information event ------------\n")
                            out.write("%s" % serializeinformationjson)
                            out.write("\n--------- end ------------\n")
                        self.update_status_event(idevent)
                        continue
                    else:
                        # No type found
                        self.logger.warning(
                            "Event type not processes  %s" % (z["type_event"])
                        )
                        self.update_status_event(idevent, 2)
                        continue
                return True

    @DatabaseHelper._sessionm
    def update_status_event(self, session, id_event, value_status=0):
        """
        this function update status event
        1 event for process
        0 event terminate.
        """
        try:
            sql = """ UPDATE `xmppmaster`.`mon_event`
                    SET
                        `status_event` = '%d'
                    WHERE
                        (`id` = '%d');""" % (
                value_status,
                id_event,
            )
            result = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def remise_status_event(self, session, id_rule, status_event, hostname):
        """
        this function update status event
        1 event for process
        0 event terminate.
        """
        try:
            sql = """UPDATE `xmppmaster`.`mon_event`
                        JOIN
                    xmppmaster.mon_machine ON xmppmaster.mon_machine.id = xmppmaster.mon_event.machines_id
                SET
                    `xmppmaster`.`mon_event`.`status_event` = '%s'
                WHERE
                        xmppmaster.mon_machine.hostname LIKE '%s'
                    AND
                        xmppmaster.mon_event.id_rule = %s;""" % (
                status_event,
                hostname,
                id_rule,
            )

            result = session.execute(sql)
            session.commit()
            session.flush()
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    def __binding_application_check(self, datastring, bindingstring, device_type):
        resultbinding = None

        d = re.search(r"\[\'\@.*\@\'\]", bindingstring)
        try:
            d.group
            logging.getLogger().warning(
                "template binding no replace %s" % bindingstring
            )
        except AttributeError:
            pass
        except Exception as e:
            logging.getLogger().error("__binding_application_check %s" % str(e))

        try:
            logging.getLogger().debug("data for binding is %s" % datastring)
            data = json.loads(datastring)
        except Exception as e:
            msg = (
                "[binding error device rule %s] : data from message"
                " monitoring format json error %s" % (device_type, str(e))
            )
            return (msg, -1)

        try:
            logging.getLogger().debug("compile")
            code = compile(bindingstring, "<string>", "exec")
            exec(code)
        except KeyError as e:
            msg = (
                "[binding error device rule %s] : key %s in "
                "binding:\n%s\nis missing. Check your binding on data\n%s"
                % (device_type, str(e), bindingstring, json.dumps(data, indent=4))
            )
            return (msg, -1)
        except Exception as e:
            msg = (
                "[binding device rule %s error %s] in binding:\n%s\ "
                "on data\n%s"
                % (device_type, str(e), bindingstring, json.dumps(data, indent=4))
            )
            return (msg, -1)
        msg = "[ %s : result binding %s for binding:\n%s\ " "on data\n%s" % (
            device_type,
            resultbinding,
            bindingstring,
            json.dumps(data, indent=4),
        )
        return (msg, resultbinding)

    def __binding_application(self, datastring, bindingstring, device_type):
        resultbinding = None
        try:
            data = json.loads(datastring)
        except Exception as e:
            return (
                "[binding error device rule %s] : data from message"
                " monitoring format json error %s" % (device_type, str(e))
            )

        try:
            code = compile(bindingstring, "<string>", "exec")
            exec(code)
        except KeyError as e:
            resultbinding = (
                "[binding error device rule %s] : key %s in "
                "binding:\n%s\nis missing. Check your binding on data\n%s"
                % (device_type, str(e), bindingstring, json.dumps(data, indent=4))
            )
        except Exception as e:
            resultbinding = (
                "[binding device rule %s error %s] in binding:\n%s\\ "
                "on data\n%s"
                % (device_type, str(e), bindingstring, json.dumps(data, indent=4))
            )
        return resultbinding

    @DatabaseHelper._sessionm
    def getlistMonitoring_devices_type(self, session, enable=1):
        sql = """ SELECT DISTINCT
                    LOWER(device_type)
                FROM
                    xmppmaster.mon_device_service
                WHERE
                    enable = 1;"""
        result = session.execute(sql)
        session.commit()
        session.flush()
        return [i[0].lower() for i in result]

    @DatabaseHelper._sessionm
    def _rule_monitoring(
        self,
        session,
        machine_hostname,
        hostname,
        id_machine,
        platform,
        agenttype,
        mon_machine_id,
        device_type,
        serial,
        firmware,
        status,
        alarm_msg,
        doc,
        localrule=True,
    ):
        result = None
        sql = """ SELECT
                    *
                FROM
                    xmppmaster.mon_rules
                WHERE
                    enable = 1 AND
                    ('%s' REGEXP hostname or NULLIF(hostname, "") is null) AND
                    ('%s' REGEXP os or NULLIF(os, "") is null) AND
                    (type_machine like '%s' or NULLIF(type_machine, "") is Null ) AND
                    device_type LIKE '%s';""" % (
            machine_hostname,
            platform,
            agenttype,
            device_type,
        )
        result = session.execute(sql)
        session.commit()
        session.flush()
        if result:
            return [
                {
                    "id": i[0],
                    "hostname": i[2],
                    "device_type": i[3],
                    "binding": i[4],
                    "succes_binding_cmd": i[5],
                    "no_success_binding_cmd": i[6],
                    "error_on_binding": i[7],
                    "type_event": i[8],
                    "user": i[9],
                    "comment": i[10],
                }
                for i in result
            ]
        else:
            return []

    @DatabaseHelper._sessionm
    def analyse_mon_rules(
        self,
        session,
        mon_machine_id,
        device_type,
        serial,
        firmware,
        status,
        alarm_msg,
        doc,
    ):
        # search rule for device and machine
        pass

    @DatabaseHelper._sessionm
    def setMonitoring_panels_template(
        self,
        session,
        name_graphe,
        template_json,
        type_graphe,
        parameters="{}",
        status=True,
        comment="",
    ):
        """
        This function allows to record panel graph template
        Args:
            session: The sqlalchemy session
            name_graphe: The name of graph
            template_json: The panel template in json format
            type_graphe: The type of graph
            parameters: The optional parameters json string  { "key":"value",...}
            status: Can be True, False or None
            comment:
        Returns:
            It returns the id of the machine
        """
        try:
            new_Monitoring_panels_template = Mon_panels_template()
            new_Monitoring_panels_template.name_graphe = name_graphe
            new_Monitoring_panels_template.template_json = template_json
            new_Monitoring_panels_template.type_graphe = type_graphe
            new_Monitoring_panels_template.parameters = parameters
            new_Monitoring_panels_template.status = status
            new_Monitoring_panels_template.comment = comment
            session.add(new_Monitoring_panels_template)
            session.commit()
            session.flush()
            return new_Monitoring_panels_template.id
        except Exception as e:
            logging.getLogger().error(str(e))
            return -1

    @DatabaseHelper._sessionm
    def getMonitoring_panels_template(self, session, status=True):
        """
        This function allows to get panel graph template
        Args:
            session: The sqlalchemy session
            status: The default value is True
                    Can be 1, 0 or None
                    False : list of template panels status False
                    True : list of template panels status True
                    None: list of all template panels
        Returns:
            It returns the list of template panels
        """
        try:
            list_panels_template = []
            if status:
                result_panels_template = (
                    session.query(Mon_panels_template)
                    .filter(and_(Mon_panels_template.status == 1))
                    .all()
                )
            elif status is False:
                result_panels_template = (
                    session.query(Mon_panels_template)
                    .filter(and_(Mon_panels_template.status == 0))
                    .all()
                )
            else:
                result_panels_template = session.query(Mon_panels_template).all()
            session.commit()
            session.flush()
            for graphe_template in result_panels_template:
                res = {
                    "id": graphe_template.id,
                    "name_graphe": graphe_template.name_graphe,
                    "template_json": graphe_template.template_json,
                    "type_graphe": graphe_template.type_graphe,
                    "parameters": graphe_template.parameters,
                    "status": graphe_template.status,
                    "comment": graphe_template.comment,
                }
                list_panels_template.append(res)
        except Exception as e:
            logging.getLogger().error(str(e))
        return list_panels_template

    @DatabaseHelper._sessionm
    def get_ars_group_in_list_clusterid(self, session, clusterid, enabled=None):
        """cherche les ars en appartenant a 1 ou plusieurs cluster.
        params : clusterid id d'un cluster ou [list id de clusteur]
                 enable  prend ou pas en compte les ars enable.
        """
        setsearch = clusterid
        if isinstance(clusterid, list):
            # Cluster's list
            listidcluster = [x for x in set(clusterid)]
            if listidcluster:
                setsearch = ("%s" % listidcluster)[1:-1]
            else:
                raise
        searchclusterars = "(%s)" % setsearch

        sql = """SELECT
                    relayserver.id AS ars_id,
                    relayserver.urlguacamole AS urlguacamole,
                    relayserver.subnet AS subnet,
                    relayserver.nameserver AS nameserver,
                    relayserver.ipserver AS ipserver,
                    relayserver.ipconnection AS ipconnection,
                    relayserver.port AS port,
                    relayserver.portconnection AS portconnection,
                    relayserver.mask AS mask,
                    relayserver.jid AS jid,
                    relayserver.longitude AS longitude,
                    relayserver.latitude AS latitude,
                    relayserver.enabled AS enabled,
                    relayserver.mandatory AS mandatory,
                    relayserver.switchonoff AS switchonoff,
                    relayserver.classutil AS classutil,
                    relayserver.groupdeploy AS groupdeploy,
                    relayserver.package_server_ip AS package_server_ip,
                    relayserver.package_server_port AS package_server_port,
                    relayserver.moderelayserver AS moderelayserver,
                    relayserver.keysyncthing AS keysyncthing,
                    relayserver.syncthing_port AS syncthing_port,
                    has_cluster_ars.id_cluster AS id_cluster,
                    cluster_ars.name AS name_cluster
                FROM
                    xmppmaster.relayserver
                        INNER JOIN
                    xmppmaster.has_cluster_ars ON xmppmaster.has_cluster_ars.id_ars = xmppmaster.relayserver.id
                        INNER JOIN
                    xmppmaster.cluster_ars ON xmppmaster.cluster_ars.id = xmppmaster.has_cluster_ars.id_cluster
                WHERE
                    id_cluster IN %s """ % (
            searchclusterars
        )
        if enabled is not None:
            sql += (
                """AND
                            `relayserver`.`enabled` = %s"""
                % enabled
            )
        sql += ";"
        logging.getLogger().error(sql)
        result = session.execute(sql)
        session.commit()
        session.flush()
        resultlist = []
        for t in result:
            tmpdict = {
                "ars_id": t[0],
                "urlguacamole": t[1],
                "subnet": t[2],
                "nameserver": t[3],
                "ipserver": t[4],
                "ipconnection": t[5],
                "port": t[6],
                "portconnection": t[7],
                "mask": t[8],
                "jid": t[9],
                "longitude": t[10],
                "latitude": t[11],
                "enabled": t[12],
                "mandatory": t[13],
                "switchonoff": t[14],
                "classutil": t[15],
                "groupdeploy": t[16],
                "package_server_ip": t[17],
                "package_server_port": t[18],
                "moderelayserver": t[19],
                "keysyncthing": t[20],
                "syncthing_port": t[21],
                "id_cluster": t[22],
                "name_cluster": t[23],
            }
            resultlist.append(tmpdict)
        logging.getLogger().error(resultlist)
        return resultlist

    # Update machine scheduling

    def __updatemachine(self, object_update_machine):
        """
        This function create a dictionnary with the informations of the
        machine that need to be updated.

        Args:
            object_update_machine: An object with the informations of the machine.
        Returns:
            A dicth with the informations of the machine.
        """
        try:
            ret = {
                "id": object_update_machine.id,
                "jid": object_update_machine.jid,
                "ars": object_update_machine.ars,
                "status": object_update_machine.status,
                "descriptor": object_update_machine.descriptor,
                "md5": object_update_machine.md5,
                "date_creation": object_update_machine.date_creation,
            }
            return ret
        except Exception as error_creating:
            logging.getLogger().error(
                "We failed to retrieve the informations of the machine to update"
            )
            logging.getLogger().error("We got the error \n : %s" % str(error_creating))
            return None

    @DatabaseHelper._sessionm
    def update_update_machine(
        self,
        session,
        hostname,
        jid,
        ars="",
        status="ready",
        descriptor="",
        md5="",
        date_creation=None,
    ):
        """
        We create the informations of the machines in the update SQL table
        Args:
            session: The SQL Alchemy session
            hostname: The hostname of the machine to update
            jid: The jid of the machine to update
            ars: The ARS on which the machine is connected
            status: The status of the update (ready, updating, ... )
                    ready: Machines that need an update. Those kind of machines
                           won't be updated automatically.
                    updating: Machines that will be updated automatically.
            descriptor: All the md5sum of files that needs to be updated.
            md5: md5 of the md5 of files ( that helps to see quickly if an update is needed )
            date_creation: Date when it has been added on the update table.
        """
        try:
            query = (
                session.query(self.Update_machine)
                .filter(self.Update_machine.jid.like(jid))
                .one()
            )

            query.hostname = hostname
            query.ars = ars
            query.status = status
            query.descriptor = descriptor
            query.md5 = md5
            session.commit()
            session.flush()
            return self.__updatemachine(query)
        except Exception as e:
            logging.getLogger().error(
                "We failed to update the informations on the SQL Table"
            )
            logging.getLogger().error("We got the error %s " % str(e))
            self.logger.error("We hit the backtrace \n%s" % (traceback.format_exc()))
            return None

    @DatabaseHelper._sessionm
    def setUpdate_machine(
        self,
        session,
        hostname,
        jid,
        ars="",
        status="ready",
        descriptor="",
        md5="",
        date_creation=None,
    ):
        """
        We update the informations of the machines in the update SQL table
        Args:
            session: The SQL Alchemy session
            hostname: The hostname of the machine to update
            jid: The jid of the machine to update
            ars: The ARS on which the machine is connected
            status: The status of the update (ready, updating, ... )
                    ready: Machines that need an update. Those kind of machines
                           won't be updated automatically.
                    updating: Machines that will be updated automatically.
            descriptor: All the md5sum of files that needs to be updated.
            md5: md5 of the md5 of files ( that helps to see quickly if an update is needed )
            date_creation: Date when it has been added on the update table.
        """

        try:
            new_Update_machine = self.Update_machine()
            new_Update_machine.hostname = hostname
            new_Update_machine.jid = jid
            new_Update_machine.ars = ars
            new_Update_machine.status = status
            new_Update_machine.descriptor = descriptor
            new_Update_machine.md5 = md5
            if date_creation is not None:
                new_Update_machine.date_creation = date_creation
            session.add(new_Update_machine)
            session.commit()
            session.flush()
            return self.__updatemachine(new_Update_machine)
        except IntegrityError as e:
            reason = e.message
            if "Duplicate entry" in reason:
                self.logger.info("%s already in table." % e.params[0])
                return self.update_update_machine(
                    hostname, jid, ars, status, descriptor, md5
                )
            else:
                self.logger.info("setUpdate_machine : %s" % str(e))
                return None
        except Exception as e:
            logging.getLogger().error(str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
            return None

    @DatabaseHelper._sessionm
    def getUpdate_machine(self, session, status="ready", nblimit=1000):
        """
        This function is used to retrieve the machines in the pending list
        for update.

        Args:
            session: The SQL Alchemy session
            status: The status of the machine in the database ( ready, updating, ... )
            nblimit: Number maximum of machines allowed to be updated at once.
        """

        sql = """SELECT
                    MIN(id) AS minid , MAX(id) AS maxid
                FROM
                    (SELECT id
                        FROM
                            update_machine
                        WHERE
                            status LIKE '%s'
                        LIMIT %s) AS dt;""" % (
            status,
            nblimit,
        )
        machines_jid_for_updating = []
        borne = session.execute(sql)

        result = [x for x in borne][0]
        minid = result[0]
        maxid = result[0]
        if minid is not None:
            sql = """ SELECT
                        jid, ars
                    FROM
                        update_machine
                    WHERE
                        id >= %s and id <= %s and
                            status LIKE '%s';""" % (
                minid,
                maxid,
                status,
            )
            resultquery = session.execute(sql)

            for record_updating_machine in resultquery:
                machines_jid_for_updating.append(
                    (record_updating_machine.jid, record_updating_machine.ars)
                )

            sql = """ delete

                    FROM
                        update_machine
                    WHERE
                        id >= %s and id <= %s and
                            status LIKE '%s';""" % (
                minid,
                maxid,
                status,
            )
            resultquery = session.execute(sql)

            session.commit()
            session.flush()
        return machines_jid_for_updating

    # ----------------------------- Update windows ---------------------------------

    def _colonne_name_update(self):
        """
        return colonne fo result
        """
        return [
            "updateid",
            "revisionid",
            "creationdate",
            "company",
            "product",
            "productfamily",
            "updateclassification",
            "prerequisite",
            "title",
            "description",
            "msrcseverity",
            "msrcnumber",
            "kb",
            "languages",
            "category",
            "supersededby",
            "supersedes",
            "payloadfiles",
            "revisionnumber",
            "bundledby_revision",
            "isleaf",
            "issoftware",
            "deploymentaction",
            "title_short",
        ]

    def _colonne_name_update_product(self):
        """
        return colonne fo result
        """
        return [
            "updateid",
            "kb",
            "revisionid",
            "title",
            "description",
            "updateid_package",
            "payloadfiles",
            "supersededby",
            "creationdate",
            "title_short",
            "",
            "msrcseverity",
        ]

    @DatabaseHelper._sessionm
    def setUp_action_update_packages(self, session, action, packages, option):
        """
        creation 1 update pour 1 machine
        """
        try:
            new_Up_action_update_packages = Up_action_update_packages()
            new_Up_action_update_packages.action = action
            new_Up_action_update_packages.packages = packages
            new_Up_action_update_packages.option = option
            session.add(new_Up_action_update_packages)
            session.commit()
            session.flush()
            return {
                "id": new_Up_action_update_packages.id,
                "action": new_Up_action_update_packages.action,
                "date": new_Up_action_update_packages.date.isoformat(),
                "in_process": new_Up_action_update_packages.in_process,
                "packages": new_Up_action_update_packages.packages,
                "option": new_Up_action_update_packages.option,
            }
        except IntegrityError as e:
            self.logger.info(
                "IntegrityError setUp_action_update_packages : %s" % str(e)
            )
        except Exception as e:
            self.logger.info("Except setUp_action_update_packages : %s" % str(e))
            self.logger.error("\n%s" % (traceback.format_exc()))
        return {}

    @DatabaseHelper._sessionm
    def del_Up_action_update_packages(self, session, packages):
        """
        del tout les updates de la machines
        """
        session.query(Up_action_update_packages).filter(
            Up_action_update_packages.packages == packages
        ).delete()
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def del_Up_action_update_packages_id(self, session, idlist=[]):
        """
        del tout les updates de la machines array id
        """
        if isinstance(idlist, (str, int)):
            idlist = [int(idlist)]
        if idlist:
            sql = delete(Up_action_update_packages).where(
                Up_action_update_packages.id.in_(idlist)
            )
            resultquery = session.execute(sql)
            session.commit()
            session.flush()

    @DatabaseHelper._sessionm
    def get_all_Up_action_update_packages(self, session):
        """
        return tout les updates de la machines
        et positionne in_process a 1 pour les commande a executer.
        """
        result = []
        res = (
            session.query(Up_action_update_packages)
            .filter(Up_action_update_packages.in_process == False)
            .all()
        )
        if res is not None:
            for update_package in res:
                update_package.in_process = True
                result.append(
                    {
                        "id": update_package.id,
                        "action": update_package.action,
                        "date": update_package.date.isoformat(),
                        "in_process": update_package.in_process,
                        "packages": update_package.packages,
                        "option": update_package.option,
                    }
                )

        session.commit()
        session.flush()
        return result

    @DatabaseHelper._sessionm
    def get_pid_list_all_Up_action_update_packages(self, session):
        """
        get list de tout les pid des process de package en cour sur le serveur
        """
        result = []
        res = (
            session.query(
                Up_action_update_packages.id, Up_action_update_packages.pid_run
            )
            .filter(
                and_(
                    Up_action_update_packages.in_process == True,
                    Up_action_update_packages.pid_run is not None,
                )
            )
            .all()
        )
        if res is not None:
            for t in res:
                result.append({"id": t.id, "pid_run": t.pid_run})
        return result

    @DatabaseHelper._sessionm
    def update_pid_all_Up_action_update_packages(self, session, id, pid_run):
        """
        update pid_run du process lancer
        les process sont supprimés
        """
        res = (
            session.query(Up_action_update_packages)
            .filter(Up_action_update_packages.id == id)
            .update({Up_action_update_packages.pid_run: pid_run})
        )
        session.commit()
        session.flush()
        return

    @DatabaseHelper._sessionm
    def delete_pid_all_Up_action_update_packages(self, session, id, pid_run):
        """
        update pid_run du process lancer
        les process sont supprimés
        """
        res = (
            session.query(Up_action_update_packages)
            .filter(Up_action_update_packages.id == id)
            .update({Up_action_update_packages.pid_run: pid_run})
        )
        session.commit()
        session.flush()
        return

    # appel procedure stocke
    @DatabaseHelper._sessionm
    def search_kb_windows(self, session, filter, kb_list):
        """
        Cette fonction renvoi les updates pour 1 machine en fonctions des kb installer et de os present sur la machine
        sous forme d'un list de dict
         Parameters :
            filter filter on title field obligatoire value
                rq: la value % doit commencer et terminer le filtre
            kblist "string list separator ," de tous les kb remontés depuis la machine

        eg : search_kb_windows("%Windows 10 Version 21H2 for x64-based%",
                                "5007289,5003791" );
        """
        result = []
        colonnename = self._colonne_name_update()
        try:
            connection = self.engine_xmppmmaster_base.raw_connection()
            results = None
            cursor = connection.cursor()
            cursor.callproc("up_search_kb_windows", [filter, kb_list])
            results = list(cursor.fetchall())
            for lineresult in results:
                dictline = {}
                for index, value in enumerate(colonnename):
                    lr = lineresult[index]
                    if isinstance(lineresult[index], datetime):
                        lr = lineresult[index].isoformat()
                    dictline[value] = lr
                result.append(dictline)
            cursor.close()
            connection.commit()
        except Exception as e:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        finally:
            connection.close()
        return result

    @DatabaseHelper._sessionm
    def search_kb_windows1(
        self,
        session,
        filter="",
        product="Windows 10",
        version="21H2",
        sevrity="Critical",
        archi="x64",
        kb_list="",
    ):
        """
        Cette fonction renvoi les updates pour 1 machine en fonctions des kb installer et de os present sur la machine
        sous forme d'un list de dict
        Parameters :
            filter filter on title field "" par default
            product windows default "Windows 10"
            version produit default "21H2"
            severity de update default "Critical"
            architecture de la machine default x64
            kblist "string list separator ," de tous les kb remontés depuis la machine

        eg : search_kb_windows1("", "Windows 10", "21H2", "Critical", "x64", "5007289, 5003791");
        """

        result = []

        colonnename = self._colonne_name_update()
        try:
            connection = self.engine_xmppmmaster_base.raw_connection()
            results = None
            cursor = connection.cursor()
            cursor.callproc(
                "up_search_kb_windows1",
                [filter, product, version, sevrity, archi, kb_list],
            )
            results = list(cursor.fetchall())
            for lineresult in results:
                dictline = {}
                for index, value in enumerate(colonnename):
                    lr = lineresult[index]
                    if isinstance(lineresult[index], datetime):
                        lr = lineresult[index].isoformat()
                    dictline[value] = lr
                result.append(dictline)
            cursor.close()
            connection.commit()
        except Exception as e:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        finally:
            connection.close()
        return result

    @DatabaseHelper._sessionm
    def search_update_windows_malicious_software_tool(
        self, session, product="Windows 10", archi="x64", major=5, minor=104
    ):
        """
        cette fonction renvoi update for windows malicious software tool
        sous forme d'un list de dict
        Parameters :
            product windows default "Windows 10"
            architecture de la machine default "x64"
            major la valeur major de la version installer
            minor la valeur mineur de la version installer
        eg : search_update_windows_malicious_software_tool(product="Windows 10",
                                                            archi="x64",
                                                            major= 5,
                                                            minor=105);
        """
        result = []
        colonnename = self._colonne_name_update()
        try:
            connection = self.engine_xmppmmaster_base.raw_connection()
            results = None
            cursor = connection.cursor()
            cursor.callproc(
                "up_windows_malicious_software_tool", [product, archi, major, minor]
            )
            results = list(cursor.fetchall())
            for lineresult in results:
                dictline = {}
                for index, value in enumerate(colonnename):
                    lr = lineresult[index]
                    if isinstance(lineresult[index], datetime):
                        lr = lineresult[index].isoformat()
                    dictline[value] = lr
                    dictline["tableproduct"] = "up_packages_Win_Malicious_X64"
                result.append(dictline)
            cursor.close()
            connection.commit()
        except Exception as e:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        finally:
            connection.close()
        return result

    @DatabaseHelper._sessionm
    def history_list_kb(self, session, list_updateid):
        """
        Cette onction renvoi les numero de kb en fonction d'une list python des updateid des update
        Parameters:
            list updateid ["00009be5-c940-498d-b04d-620a572436df","00009be5-c940-498d-b04d-620a572436df"]
        return
            listt kb
        """
        ret = []
        try:
            if list_updateid:
                indata = ['"%s"' % x for x in list_updateid]
                sql = """SELECT
                            kb
                        FROM
                            xmppmaster.update_data
                        WHERE
                            updateid IN (%s); """ % ",".join(
                    indata
                )
                req = session.execute(sql)
                session.commit()
                session.flush()
                ret = [elt[0] for elt in req]
        except Exception:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return ret

    @DatabaseHelper._sessionm
    def history_list_base(self, session, list_updateid):
        """
        Cette fonction renvoi update en fonction d'une list python des updateid
        return 1 tableau de dict
        Parameters:
            list updateid ["00009be5-c940-498d-b04d-620a572436df","00009be5-c940-498d-b04d-620a572436df"]
        return
            listt kb
        """
        ret = []
        try:
            if list_updateid:
                indata = ['"%s"' % x for x in list_updateid]
                sql = """SELECT
                            *
                        FROM
                            xmppmaster.update_data
                        WHERE
                            updateid IN (%s); """ % ",".join(
                    indata
                )
                req = session.execute(sql)
                session.commit()
                session.flush()
                ret = self._return_dict_from_dataset_mysql(req)
        except Exception:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        return ret

    @DatabaseHelper._sessionm
    def getId_UuidFromJid(self, session, jid):
        """return machine uuid and id for machines"""
        user = str(jid).split("@")[0]
        uuid_inventorymachine = (
            session.query(Machines.id, Machines.uuid_inventorymachine)
            .filter(Machines.jid.like(user + "%"))
            .first()
        )
        session.commit()
        session.flush()
        result = {}
        if uuid_inventorymachine:
            result = {
                "id": uuid_inventorymachine.id,
                "uuid_inv": uuid_inventorymachine.uuid_inventorymachine,
            }
        return result

    def __Up_machine_windows(self, object_Up_machine_windows):
        """
        This function create a dictionnary with  update kb
        Args:
            object_Up_machine_windows: dataset line Up_machine_windows
        Returns:
            A dicth with the informations of the update.
        """
        try:
            ret = {
                "id_machine": object_Up_machine_windows.id_machine,
                "update_id": object_Up_machine_windows.update_id,
                "kb": object_Up_machine_windows.kb,
            }
            return ret
        except Exception as error_creating:
            logging.getLogger().error(
                "We failed to retrieve the informations of the Up_machine_windows"
            )
            logging.getLogger().error("We got the error \n : %s" % str(error_creating))
            return None

    @DatabaseHelper._sessionm
    def test_black_list(self, session, jid):
        """
        This function renvoi la liste des tables produits a prendre en compte pour les updates.
        """
        ret = {}
        up = []
        kb = []
        try:
            user = "".join(str(jid).split("@")[0].split(".")[:-1])
            sql = (
                """SELECT DISTINCT
                    updateid_or_kb, type_rule
                FROM
                    xmppmaster.up_black_list
                WHERE
                    enable_rule = 1
                        AND "%s" REGEXP userjid_regexp;"""
                % user
            )
            req = session.execute(sql)
            session.commit()
            session.flush()
            if req:
                for x in req:
                    if x[1].lower() == "kb":
                        kb.append(x[0])
                    elif x[1].lower() == "id":
                        up.append(x[0])
            ret = {"update_id": up, "kb": kb}
        except Exception:
            logging.getLogger().error(
                "sql test_black_list : %s" % traceback.format_exc()
            )
        return ret

    @DatabaseHelper._sessionm
    def setUp_machine_windows(
        self,
        session,
        id_machine,
        update_id,
        kb="",
        deployment_intervals="",
        msrcseverity="Corrective",
    ):
        """
        creation 1 update pour 1 machine
        """
        if msrcseverity.strip() == "":
            msrcseverity = "Corrective"
        objet_existant = (
            session.query(Up_machine_windows)
            .filter(
                and_(
                    Up_machine_windows.id_machine == id_machine,
                    or_(
                        Up_machine_windows.update_id == update_id,
                        Up_machine_windows.kb == kb,
                    ),
                )
            )
            .count()
        )
        # Si l'objet n'existe pas, l'ajouter à la base de données
        if objet_existant == 0:
            try:
                new_Up_machine_windows = Up_machine_windows()
                new_Up_machine_windows.id_machine = id_machine
                new_Up_machine_windows.update_id = update_id
                new_Up_machine_windows.kb = kb
                new_Up_machine_windows.intervals = deployment_intervals
                new_Up_machine_windows.msrcseverity = msrcseverity
                session.add(new_Up_machine_windows)
                session.commit()
                session.flush()
                return self.__Up_machine_windows(new_Up_machine_windows)
            except IntegrityError as e:
                self.logger.info("IntegrityError setUp_machine_windows : %s" % str(e))
            except Exception as e:
                self.logger.info("Except setUp_machine_windows : %s" % str(e))
                self.logger.error("\n%s" % (traceback.format_exc()))
        return None

    @DatabaseHelper._sessionm
    def del_all_Up_machine_windows(self, session, id_machine, listupdatiddesire=[]):
        """
        Supprime les enregistrements de la table 'Up_machine_windows' qui sont maintenant installé.

        Args:
            session (Session): Session SQLAlchemy active.
                La session active fournie par le décorateur @DatabaseHelper._sessionm.
            id_machine (int): L'ID de la machine cible pour laquelle les enregistrements doivent être supprimés.
            listupdatiddesire (list): Liste des mises necessaire a la machine.

        Note:
            requête de suppression sur la table 'Up_machine_windows', en fonction des conditions fournies. Les conditions incluent :
            - L'ID de la machine correspondant à 'id_machine'.
            - La date de fin est soit nulle, soit antérieure à la date et à l'heure actuelles.
            - L'ID de mise à jour n'est pas présent dans la liste 'listupdatiddesire'.
        """
        logging.getLogger().debug("id_machine : %s" % id_machine)
        logging.getLogger().debug("listupdatiddesire : %s" % listupdatiddesire)
        if listupdatiddesire:
            sql = """DELETE
                FROM
                    `xmppmaster`.`up_machine_windows`
                WHERE
                    (`id_machine` = '%s')
                        AND (`update_id` NOT IN (%s)
                        OR up_machine_windows.end_date IS NULL
                        OR up_machine_windows.end_date < NOW());""" % (
                id_machine,
                ",".join(["'%s'" % x for x in listupdatiddesire]),
            )
        else:
            sql = """DELETE
                FROM
                    `xmppmaster`.`up_machine_windows`
                WHERE
                    (`id_machine` = '%s');""" % (
                id_machine
            )
        logging.getLogger().debug("sql : %s" % sql)
        req = session.execute(sql)
        session.commit()
        session.flush()

    @DatabaseHelper._sessionm
    def list_produits(self, session):
        """
        This function renvoi la liste des table produitss a prendre en compte pour les updates.
        """
        ret = {}
        try:
            sql = """SELECT
                    name_procedure
                FROM
                    xmppmaster.up_list_produit
                WHERE
                    enable = 1; """
            req = session.execute(sql)
            session.commit()
            session.flush()
            ret = self._return_dict_from_dataset_mysql(req)
        except Exception:
            logging.getLogger().error("sql list_produits : %s" % traceback.format_exc())
        return ret

    def search_update_by_products(self, tableproduct="", str_kb_list=""):
        """
        cette fonction renvoi update en fonction des produits
        Parameters :
            tableproduct voir table produits dans la table list_produits
            str_kb_list list des kb installer sur la machine
        """
        result = []
        colonnename = self._colonne_name_update_product()
        try:
            connection = self.engine_xmppmmaster_base.raw_connection()
            results = None
            cursor = connection.cursor()
            cursor.callproc(
                "up_search_kb_update",
                [str(tableproduct["name_procedure"]), str(str_kb_list).strip('"() ')],
            )
            results = list(cursor.fetchall())

            if results is None:
                return result

            for lineresult in results:
                dictline = {}
                dictline["tableproduct"] = tableproduct["name_procedure"]
                for index, value in enumerate(colonnename):
                    if value == "":
                        continue
                    lr = lineresult[index]
                    if isinstance(lr, datetime):
                        lr = lr.isoformat()
                    dictline[value] = lr
                result.append(dictline)
            cursor.close()
            connection.commit()
        except Exception as e:
            logging.getLogger().error("sql : %s" % traceback.format_exc())
        finally:
            connection.close()
        return result

    @DatabaseHelper._sessionm
    def is_exist_value_in_table(
        self, session, valchamp, namefield="updateid", tablename="up_gray_list"
    ):
        """
        test si il existe dans 1 table 1 enregistrement avec la valeur pour le nom du champ passe
        """
        try:
            sql = """SELECT
                    COUNT(%s)
                FROM
                    %s
                WHERE
                    updateid LIKE '%s'
                LIMIT 1;""" % (
                namefield,
                tablename,
                valchamp,
            )
            rest = session.execute(sql)
            session.commit()
            session.flush()
            if rest:
                ret = [elt for elt in rest][0]
                return bool(ret[0] == 1)
        except Exception:
            logging.getLogger().error(
                "sql is_exist_value_in_table: %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def setUp_machine_windows_gray_list(
        self, session, updateid, tableproduct="", validity_day=10
    ):
        """
        cette fonction insert dans la table gray list 1 update
        Si l update existe. Il update seulement la date de validity
        Parameters :
            tableproduct voir table produits dans la table list_produits
            str_kb_list list des kb installer sur la machine
        """
        # if le update existe dans la table up_white_list
        # on ne fait rien
        # if le update existe dans la table up_gray_list_flop
        # on supprime dans la stock_table up_gray_list_flop
        # ce qui fera que l'update sera reinitialiser
        # auterment on insert ou update

        try:
            if self.is_exist_value_in_table(
                updateid, namefield="updateid", tablename="up_white_list"
            ):
                return False

            if self.is_exist_value_in_table(
                updateid, namefield="updateid", tablename="up_gray_list_flop"
            ):
                # si l'update existe dans la flip flop, la supprimer de la table flip flop la reinitialise dans la table gray list.
                # On met a jour la date avant que l'enregistrement change de table.
                self.update_in_grays_list_validity(
                    updateid, flipflop=True, validity_day=validity_day
                )
                sql = (
                    """DELETE FROM `up_gray_list_flop` WHERE (`updateid` = '%s');"""
                    % (updateid)
                )
                session.execute(sql)
                session.commit()
                session.flush()
                return True
            if self.is_exist_value_in_table(
                updateid, namefield="updateid", tablename="up_gray_list"
            ):
                # update date validity date
                self.update_in_grays_list_validity(
                    updateid, flipflop=False, validity_day=validity_day
                )
                return True

            # insertion
            sql = """INSERT INTO `xmppmaster`.`up_gray_list` (updateid,
                                                            kb,
                                                            revisionid,
                                                            title,
                                                            description,
                                                            updateid_package,
                                                            payloadfiles,
                                                            supersededby,
                                                            title_short,
                                                            validity_date)
                        ( SELECT updateid,
                                kb,
                                revisionid,
                                title,
                                description,
                                updateid_package,
                                payloadfiles,
                                supersededby,
                                title_short,
                                now() + INTERVAL %s day
                        FROM
                            xmppmaster.%s
                        WHERE
                            updateid LIKE '%s')
                        ON DUPLICATE KEY UPDATE validity_date = now() + INTERVAL %s day;""" % (
                validity_day,
                tableproduct,
                updateid,
                validity_day,
            )
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception:
            logging.getLogger().error("sql list_produits : %s" % traceback.format_exc())
        return False

    @DatabaseHelper._sessionm
    def update_in_grays_list_validity(
        self, session, updateid, flipflop=True, validity_day=10
    ):
        """
        Met à jour la date de validité d'un enregistrement dans la table
        # up_gray_list_flop ou "up_gray_list"

        Parameters:
            session: Session de base de données.
            updateid: updateID de la mise à jour.
            flipflop: Indicateur pour déterminer la (table up_gray_list_flop ou up_gray_list (par défaut : True).
            validity_day: Nombre de jours de validité (par défaut : 10 jours).
        Returns:
            True si l'opération réussit, False sinon.
        """
        # Sélectionne la table en fonction de l'indicateur flipflop.
        if flipflop:
            table = "up_gray_list_flop"
        else:
            table = "up_gray_list"

        try:
            # Met à jour la date de validité de l'enregistrement.
            sql = """
            UPDATE `xmppmaster`.`%s`
            SET validity_date = DATE_ADD(NOW(), INTERVAL %s DAY)
            WHERE updateid LIKE '%s';
            """ % (
                table,
                validity_day,
                updateid,
            )

            # Exécute la requête SQL.
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception:
            logging.getLogger().error(
                "update_in_grays_list_validity : %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def delete_in_gray_list(self, session, updateid):
        """
        cettte fonction supprime 1 update completement depuis les grays list
        update est supprime du flip flop (up_gray_list_flop/up_gray_list)
        le principe on renome le updateid en "a_efface"
        updateid < 36 caracteres il est donc directement supprimable sans effet flip flop
        """
        try:
            updateidreduit = updateid[-12:]
            sql = """ UPDATE `up_gray_list_flop` SET `updateid` = '%s' WHERE (`updateid` = '%s');
                    UPDATE `up_gray_list` SET `updateid` = '%s' WHERE (`updateid` = '%s');
                    DELETE FROM `up_gray_list_flop` WHERE (`updateid` = '%s');
                    DELETE FROM `up_gray_list` WHERE (`updateid` = '%s');
            """ % (
                updateidreduit,
                updateid,
                updateidreduit,
                updateid,
                updateidreduit,
                updateidreduit,
            )
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception:
            logging.getLogger().error(
                "sql delete_in_gray_list : %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def delete_in_white_list(self, session, updateid):
        """
        cettte fonction supprime 1 update completement depuis les grays list
        update est supprime du flip flop (up_gray_list_flop/up_gray_list)
        le principe on renome le updateid en "a_efface"
        updateid < 36 caracteres il est donc directement supprimable sans effet flip flop
        """
        try:
            sql = """ DELETE FROM `up_white_list` WHERE (`updateid` = '%s');""" % (
                updateid
            )
            self.logger.info("delete_in_white_list : %s" % sql)
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception:
            logging.getLogger().error(
                "sql delete_in_white_list : %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def get_all_update_in_gray_list(self, session, updateid=None):
        """cette function renvoi tout les update de la list gray"""
        try:
            sql = """
                select * from
                    (SELECT
                        *
                    FROM
                        xmppmaster.up_gray_list
                    UNION
                    SELECT
                        *
                    FROM
                        xmppmaster.up_gray_list_flop) as e"""
            if updateid:
                filter = """ WHERE
                            updateid = '%s'""" % (
                    updateid
                )
                sql = sql + filter
            sql += ";"
            resultproxy = session.execute(sql)
            session.commit()
            session.flush()
            return [rowproxy._asdict() for rowproxy in resultproxy]
        except Exception:
            logging.getLogger().error(
                "sql get_all_update_in_gray_list : %s" % traceback.format_exc()
            )
        return []

    @DatabaseHelper._sessionm
    def delete_in_white_list(self, session, updateid):
        """
        cettte fonction supprime 1 update completement depuis les grays list
        update est supprime du flip flop (up_gray_list_flop/up_gray_list)
        le principe on renome le updateid en "a_efface"
        updateid < 36 caracteres il est donc directement supprimable sans effet flip flop
        """
        try:
            sql = """ DELETE FROM `up_white_list` WHERE (`updateid` = '%s');""" % (
                updateid
            )
            self.logger.info("delete_in_white_list : %s" % sql)
            session.execute(sql)
            session.commit()
            session.flush()
            return True
        except Exception:
            logging.getLogger().error(
                "sql delete_in_white_list : %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def get_all_update_in_gray_list(self, session, updateid=None):
        """
        cettte fonction display tout les update dans gray list. du flip flop complet
        """
        try:
            sql = """
                select * from
                    (SELECT
                        *
                    FROM
                        xmppmaster.up_gray_list
                    UNION
                    SELECT
                        *
                    FROM
                        xmppmaster.up_gray_list_flop) as e"""
            if updateid:
                filter = """ WHERE
                            updateid = '%s'""" % (
                    updateid
                )
                sql = sql + filter
            sql += ";"
            resultproxy = session.execute(sql)
            session.commit()
            session.flush()
            return [rowproxy._asdict() for rowproxy in resultproxy]
        except Exception:
            logging.getLogger().error(
                "sql delete_in_gray_list : %s" % traceback.format_exc()
            )
        return []

    @DatabaseHelper._sessionm
    def delete_in_gray_and_white_list(self, session, updateid):
        """
        cettte fonction supprime 1 update completement depuis les grays list
        et white list
        """
        try:
            self.delete_in_gray_list(updateid)
            self.delete_in_white_list(updateid)
            return True
        except Exception:
            logging.getLogger().error(
                "sql delete_in_gray_and_white_list : %s" % traceback.format_exc()
            )
        return False

    @DatabaseHelper._sessionm
    def remove_expired_updates(self, session):
        date_now = datetime.now()

        # Select "availables" updates which are already in history and done (deleted_date)
        query1 = (
            session.query(Up_machine_windows)
            .join(
                Up_history,
                and_(
                    Up_history.id_machine == Up_machine_windows.id_machine,
                    Up_history.update_id == Up_machine_windows.update_id,
                ),
            )
            .filter(Up_history.delete_date != None)
        )
        count1 = query1.count()
        query1 = query1.all()

        # if some rows are present, we will delete them from the pool (up_machine_windows)
        if count1 > 0:
            history_updateid = [element.update_id for element in query1]
            history_id_machine = [element.id_machine for element in query1]

            session.query(Up_machine_windows).filter(
                and_(
                    Up_machine_windows.id_machine.in_(history_id_machine),
                    Up_machine_windows.update_id.in_(history_updateid),
                )
            ).delete()
            session.commit()
            session.flush()

        query = session.query(Up_machine_windows).filter(
            and_(
                Up_machine_windows.end_date is not None,
                Up_machine_windows.end_date < date_now,
            )
        )
        count = query.count()
        query.delete()

        session.commit()
        session.flush()
        return count1 + count

    @DatabaseHelper._sessionm
    def pending_up_machine_windows_white(self, session):
        query = (
            session.query(Up_machine_windows, Up_white_list, Machines)
            .filter(
                and_(
                    or_(
                        Up_machine_windows.curent_deploy == None,
                        Up_machine_windows.curent_deploy == 0,
                    ),
                    or_(
                        Up_machine_windows.required_deploy == None,
                        Up_machine_windows.required_deploy == 0,
                    ),
                )
            )
            .join(Up_white_list, Up_machine_windows.update_id == Up_white_list.updateid)
            .join(Machines, Up_machine_windows.id_machine == Machines.id)
            .all()
        )

        result = []
        start_date = datetime.now()
        end_date = start_date + timedelta(days=7)

        exclude_name_package = ["sharing", ".stfolder", ".stignore"]

        for element, white, machine in query:
            # Add entry to history

            deployName = "%s -@upd@- %s" % (white.title, start_date)

            history = Up_history()
            history.update_id = element.update_id
            history.id_machine = element.id_machine
            history.jid = machine.jid
            history.update_list = "white"
            history.required_date = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")
            history.deploy_title = deployName
            session.add(history)

            element.required_deploy = 1
            element.curent_deploy = 0
            element.start_date = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")
            element.end_date = datetime.strftime(end_date, "%Y-%m-%d %H:%M:%S")

            folderpackage = os.path.join(
                "/", "var", "lib", "pulse2", "packages", element.update_id
            )
            files = []

            if os.path.isdir(folderpackage):
                for root, dir, file in os.walk(folderpackage):
                    if root != folderpackage:
                        continue
                    for _file in file:
                        if _file not in exclude_name_package:
                            files.append(
                                {
                                    "path": os.path.basename(os.path.dirname(root)),
                                    "name": _file,
                                    "id": str(uuid.uuid4()),
                                    "size": str(
                                        os.path.getsize(os.path.join(root, _file))
                                    ),
                                }
                            )
            else:
                files = []

            files_str = "\n".join(
                [
                    file["id"] + "##" + file["path"] + "/" + file["name"]
                    for file in files
                ]
            )
            result.append(
                {
                    "id_machine": element.id_machine,
                    "update_id": element.update_id,
                    "kb": element.kb,
                    "curent_deploy": element.curent_deploy,
                    "required_deploy": element.required_deploy,
                    "start_date": element.start_date,
                    "end_date": element.end_date,
                    "intervals": element.intervals,
                    "title": deployName,
                    "jidmachine": machine.jid,
                    "groupdeploy": machine.groupdeploy,
                    "uuidmachine": machine.uuid_inventorymachine,
                    "hostname": machine.hostname,
                    "files_str": files_str,
                }
            )

        session.commit()
        session.flush()

        return result

    @DatabaseHelper._sessionm
    def pending_up_machine_windows(self, session, to_deploy):
        try:
            query = (
                session.query(Up_machine_windows, Update_data, Machines)
                .filter(
                    and_(
                        Up_machine_windows.update_id == to_deploy["updateid"],
                        Up_machine_windows.id_machine == to_deploy["idmachine"],
                    )
                )
                .join(Update_data, Up_machine_windows.update_id == Update_data.updateid)
                .join(Machines, Up_machine_windows.id_machine == Machines.id)
                .all()
            )

            exclude_name_package = ["sharing", ".stfolder", ".stignore"]
            result = {}

            start_date = datetime.now()
            end_date = start_date + timedelta(days=7)

            for element, updata, machine in query:
                deployName = "%s -@upd@- %s" % (updata.title, start_date)
                element.required_deploy = 0
                element.curent_deploy = 1
                try:
                    history = (
                        session.query(Up_history)
                        .filter(
                            and_(
                                element.update_id == Up_history.update_id,
                                element.id_machine == Up_history.id_machine,
                            )
                        )
                        .first()
                    )
                    history.curent_date = datetime.strftime(
                        start_date, "%Y-%m-%d %H:%M:%S"
                    )
                    history.deploy_title = deployName
                except Exception as e:
                    self.logger.error(e)
                folderpackage = os.path.join(
                    "/", "var", "lib", "pulse2", "packages", element.update_id
                )
                files = []

                if os.path.isdir(folderpackage):
                    for root, dir, file in os.walk(folderpackage):
                        if root != folderpackage:
                            continue
                        for _file in file:
                            if _file not in exclude_name_package:
                                files.append(
                                    {
                                        "path": os.path.basename(os.path.dirname(root)),
                                        "name": _file,
                                        "id": str(uuid.uuid4()),
                                        "size": str(
                                            os.path.getsize(os.path.join(root, _file))
                                        ),
                                    }
                                )
                else:
                    files = []

                files_str = "\n".join(
                    [
                        file["id"] + "##" + file["path"] + "/" + file["name"]
                        for file in files
                    ]
                )
                result = {
                    "id_machine": element.id_machine,
                    "update_id": element.update_id,
                    "kb": element.kb,
                    "curent_deploy": element.curent_deploy,
                    "required_deploy": element.required_deploy,
                    "start_date": element.start_date,
                    "end_date": element.end_date,
                    "intervals": element.intervals,
                    "title": deployName,
                    "jidmachine": machine.jid,
                    "groupdeploy": machine.groupdeploy,
                    "uuidmachine": machine.uuid_inventorymachine,
                    "hostname": machine.hostname,
                    "files_str": files_str,
                }

            session.commit()
            session.flush()
            return result
        except Exception as e:
            self.logger.error(e)
            return False

    @DatabaseHelper._sessionm
    def get_updates_in_required_deploy_state(self, session):
        query = (
            session.query(Up_machine_windows)
            .join(Machines, Machines.id == Up_machine_windows.id_machine)
            .filter(Up_machine_windows.required_deploy == 1)
        )

        count = query.count()
        query = query.all()

        result = {"total": count, "datas": query}
        return result

    @DatabaseHelper._sessionm
    def get_updates_in_curent_deploy_state(self, session):
        query = (
            session.query(Up_machine_windows)
            .join(Machines, Machines.id == Up_machine_windows.id_machine)
            .filter(Up_machine_windows.curent_deploy == 1)
        )

        count = query.count()
        query = query.all()

        result = {"total": count, "datas": query}
        return result

    @DatabaseHelper._sessionm
    def get_updates_in_deploy_state(self, session):
        date_now = datetime.now()
        try:
            query = (
                session.query(Up_machine_windows, Machines)
                .add_column(Update_data.kb.label("kb"))
                .join(Machines, Machines.id == Up_machine_windows.id_machine)
                .join(Update_data, Up_machine_windows.update_id == Update_data.updateid)
                .filter(
                    and_(
                        or_(
                            Up_machine_windows.required_deploy == 1,
                            Up_machine_windows.curent_deploy == 1,
                        ),
                        Up_machine_windows.start_date < date_now,
                        Up_machine_windows.end_date > date_now,
                    )
                )
            )

            count = query.count()
            query = query.all()

            result = {
                "total": count,
                "current": {"total": 0, "datas": []},
                "required": {"total": 0, "datas": []},
            }

            for update, machine, kb in query:
                tmp = {
                    "idmachine": machine.id,
                    "jidmachine": machine.jid,
                    "uuid_inventorymachine": (
                        machine.uuid_inventorymachine
                        if machine.uuid_inventorymachine is not None
                        else ""
                    ),
                    "groupdeploy": machine.groupdeploy,
                    "updateid": update.update_id,
                    "kb": kb,
                    "deployment_intervals": update.intervals,
                    "start_date": update.start_date,
                    "end_date": update.end_date,
                }

                switch_list = "current" if update.curent_deploy == 1 else "required"
                result[switch_list]["total"] += 1
                result[switch_list]["datas"].append(tmp)

            return result
        except Exception as e:
            self.logger.error(e)

    @DatabaseHelper._sessionm
    def deployment_is_running_on_machine(self, session, jid):
        date_now = datetime.now()

        query = session.query(Deploy).filter(
            and_(
                Deploy.jidmachine == jid,
                Deploy.startcmd < date_now,
                Deploy.endcmd > date_now,
                Deploy.state.op("not regexp")("(SUCCESS)|(ABORT)|(ERROR)"),
            )
        )

        query = query.count()

        return bool(query is not None and query != 0)

    @DatabaseHelper._sessionm
    def delete_all_done_updates(self, session):
        date_now = datetime.now()

        try:
            query = (
                session.query(Up_machine_windows, Machines, Update_data, Up_history)
                .join(Machines, Up_machine_windows.id_machine == Machines.id)
                .join(Update_data, Up_machine_windows.update_id == Update_data.updateid)
                .join(
                    Up_history,
                    and_(
                        Up_machine_windows.update_id == Up_history.update_id,
                        Up_machine_windows.id_machine == Up_history.id_machine,
                    ),
                )
                .filter(and_(Up_machine_windows.curent_deploy == 1))
            )
            count = query.count()
            query = query.all()

            self.logger.info("%s updates in current_deploy state" % count)
        except Exception as e:
            self.logger.error("delete_all_done_updates : %s" % e)
            return False

        if count == 0:
            self.logger.info("No update in current_deploy state, nothing to remove")
            return False

        result = []
        for update, machine, data, history in query:
            result.append(
                {
                    "update_id": update.update_id,
                    "id_machine": machine.id,
                    "jid": machine.jid,
                    "hostname": machine.hostname,
                    "kb": data.kb,
                    "command": history.command,
                }
            )

            try:
                deploy_done = []
                deploy_not_done = []
                query2 = session.query(Deploy).filter(
                    and_(
                        Deploy.jidmachine == machine.jid,
                        Deploy.title == history.deploy_title,
                        Deploy.command == history.command,
                        Deploy.start > update.start_date,
                        Deploy.start < update.end_date,
                    )
                )
                query2 = query2.all()
                for deploy in query2:
                    if history.id_deploy is None:
                        history.id_deploy = deploy.id
                        history.deploy_date = deploy.start
                    if re.search("(SUCCESS)|(ABORT)|(ERROR)", deploy.state):
                        deploy_done.append(deploy)
                    else:
                        deploy_not_done.append(deploy)
                self.logger.info(
                    "%s deploy done for machine %s and update %s"
                    % (len(deploy_done), machine.jid, update.update_id)
                )
            except Exception as e:
                self.logger.error(e)
                return False

            for deploy in deploy_done:
                self.logger.info(
                    "Removing update %s for machine %s : done"
                    % (update.update_id, machine.hostname)
                )
                history.delete_date = datetime.strftime(date_now, "%Y-%m-%d %H:%M:%S")
                query_del = (
                    session.query(Up_machine_windows)
                    .filter(
                        and_(
                            Up_machine_windows.update_id == update.update_id,
                            Up_machine_windows.id_machine == machine.id,
                        )
                    )
                    .delete()
                )
                session.commit()
                session.flush()
        return True

    @DatabaseHelper._sessionm
    def insert_command_into_up_history(self, session, updateid, jidmachine, commandid):
        query = (
            session.query(Up_history)
            .filter(
                and_(Up_history.update_id == updateid, Up_history.jid == jidmachine)
            )
            .order_by(desc(Up_history.curent_date))
            .first()
        )

        try:
            query.command = commandid
            session.commit()
            session.flush()
        except Exception as e:
            self.logger.error(e)

    @DatabaseHelper._sessionm
    def getmachineentityfromjid(self, session, jid):
        query = (
            session.query(Glpi_entity)
            .join(Machines, Glpi_entity.id == Machines.glpi_entity_id)
            .filter(Machines.jid == jid)
            .first()
        )
        return query

    @DatabaseHelper._sessionm
    def get_ad_group_for_lastuser(self, session, login):
        query = (
            session.query(Users_adgroups).filter(Users_adgroups.lastuser == login).all()
        )

        result = [element.adname for element in query] if query != None else []
        return result

    @DatabaseHelper._sessionm
    def get_all_ad_groups(self, session, start, limit, filter):
        query = session.query(Users_adgroups).group_by(Users_adgroups.adname).all()

        result = [element.adname for element in query] if query != None else []
        return result

    @DatabaseHelper._sessionm
    def get_all_ad_groups_team(self, session, logins):
        query = (
            session.query(Users_adgroups)
            .filter(Users_adgroups.lastuser.in_(logins))
            .group_by(Users_adgroups.adname)
            .all()
        )

        result = [element.adname for element in query] if query != None else []
        return result

    @DatabaseHelper._sessionm
    def addadusergroups(self, session, lastuser, ous):
        query = (
            session.query(Users_adgroups)
            .filter(Users_adgroups.lastuser == lastuser)
            .delete()
        )
        session.commit()
        if ous != []:
            for ou in ous:
                toAdd = Users_adgroups()
                toAdd.lastuser = lastuser
                toAdd.adname = ou
                session.add(toAdd)
            session.commit()

    # -------------------------------------------------------------------------------
    def _return_dict_from_dataset_mysql(self, resultproxy):
        return [rowproxy._asdict() for rowproxy in resultproxy]
