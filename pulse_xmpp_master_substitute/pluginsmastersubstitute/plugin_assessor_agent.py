# file : /pluginsmastersubstitute/plugin_assessor_agent.py

import base64
import json
import os
import logging
import time
from lib.utils import ipfromdns, AESCipher, subnetnetwork, call_plugin
from lib.localisation import Point
from lib.plugins.xmpp import XmppMasterDatabase
from lib.manageADorganization import manage_fqdn_window_activedirectory

from random import randint
import operator
import traceback
import configparser
import netaddr
from math import cos, sin, atan2, sqrt

try:
    from lib.stat import statcallplugin

    statfuncton = True
except:
    statfuncton = False

logger = logging.getLogger()
DEBUGPULSEPLUGIN = 25

# this plugin calling to starting agent

# connectionconf et le nom du plugin appeler.

plugin = {"VERSION": "1.3", "NAME": "assessor_agent", "TYPE": "substitute", "FEATURE": "assessor"}  # fmt: skip

params = {"duration": 300}
# The parameter named duration is the time after which a configuration request is considered as expired.
# The connection agent re-sends a configuration request after 300 seconds, thus making the previous one
# obsolete as it will not be processed


def action(objectxmpp, action, sessionid, data, msg, ret, dataobj):
    logger.debug("=====================================================")
    logger.debug("call %s from %s" % (plugin, msg["from"]))
    logger.debug("=====================================================")
    msgq = {"to": str(msg["to"]), "from": str(msg["from"])}
    try:
        timeact = int(time.time())
        compteurcallplugin = getattr(objectxmpp, "num_call%s" % action)
        if compteurcallplugin == 0:
            objectxmpp.compteur_de_traitement = {}
            objectxmpp.listconfiguration = []
            objectxmpp.simultaneous_processing = 50
            objectxmpp.show_queue_status = False
            objectxmpp.assessor_agent_errorconf = False
            if statfuncton:
                objectxmpp.stat_assessor_agent = statcallplugin(
                    objectxmpp, plugin["NAME"]
                )
            try:
                read_conf_assessor(objectxmpp)
            except:
                logger.error("\n%s" % (traceback.format_exc()))
        else:
            if statfuncton:
                objectxmpp.stat_assessor_agent.statutility()
        # ______________________________________
        # error configuration on quitte et signale erreur au configurateur distant
        if objectxmpp.assessor_agent_errorconf:
            logger.error(
                "error configuration no process action %s for machine %s"
                % (action, msg["from"])
            )
            sendErrorConnectionConf(objectxmpp, sessionid, msg)
            return
        # ______________________________________
        # delete compteur expire
        deletelist = []
        for sessionid_save in list(objectxmpp.compteur_de_traitement.keys()):
            if (
                timeact - int(objectxmpp.compteur_de_traitement[sessionid_save][0])
            ) > params["duration"]:
                deletelist.append(sessionid_save)
        for t in deletelist:
            del objectxmpp.compteur_de_traitement[t]
        # ______________________________________

        # ______________________________________
        # add report dans la liste
        if len(objectxmpp.compteur_de_traitement) >= objectxmpp.simultaneous_processing:
            objectxmpp.listconfiguration.append(
                [
                    int(time.time()),
                    {
                        "action": action,
                        "sessionid": sessionid,
                        "data": data,
                        "msg": msgq,
                    },
                ]
            )

            if bool(objectxmpp.show_queue_status):
                logger.info(
                    "add (%s) : Pending pool counter = %s"
                    % (
                        msgq["from"].split("/")[1],
                        len(objectxmpp.compteur_de_traitement),
                    )
                )
            return
        # ______________________________________

        # ______________________________________
        # execute configuration
        try:
            objectxmpp.compteur_de_traitement[sessionid] = [timeact, msgq]
            if bool(objectxmpp.show_queue_status):
                logger.info(
                    "Pending pool counter = %s"
                    % (len(objectxmpp.compteur_de_traitement))
                )

            Algorithm_Rule_Attribution_Agent_Relay_Server(
                objectxmpp, action, sessionid, data, msg
            )
        except Exception:
            logger.error("\n%s" % (traceback.format_exc()))
        finally:
            if sessionid in objectxmpp.compteur_de_traitement:
                del objectxmpp.compteur_de_traitement[sessionid]
        # ______________________________________

        # ______________________________________
        # relance ou suprime depuis la liste
        while (
            objectxmpp.listconfiguration
            and len(objectxmpp.compteur_de_traitement)
            < objectxmpp.simultaneous_processing
        ):
            ## call plugin
            report = objectxmpp.listconfiguration.pop(0)
            if len(report) == 2 and (timeact - report[0]) < params["duration"]:
                dataerreur = {
                    "action": "result" + plugin["NAME"],
                    "data": {"msg": "error plugin : " + plugin["NAME"]},
                    "sessionid": report[1]["sessionid"],
                    "ret": 255,
                    "base64": False,
                }
                if bool(objectxmpp.show_queue_status):
                    logger.info("Re-call plugin %s" % (plugin["NAME"]))
                call_plugin(
                    __file__,
                    objectxmpp,
                    action,
                    report[1]["sessionid"],
                    report[1]["data"],
                    report[1]["msg"],
                    0,
                    dataerreur,
                )
            else:
                if bool(objectxmpp.show_queue_status):
                    if "from" in report[1]["data"]:
                        mach = report[1]["data"]["from"].split("/")[1]
                        logger.info(
                            "Timeout re-calling plugin %s on machine %s"
                            % (plugin["NAME"], mach)
                        )
        # ______________________________________
    except Exception as e:
        sendErrorConnectionConf(objectxmpp, sessionid, msg)
        logger.error("\n%s" % (traceback.format_exc()))


def testsignaturecodechaine(objectxmpp, data, sessionid, msg):
    codechaine = "%s" % (msg["from"])
    result = False
    for t in objectxmpp.assessor_agent_keyAES32:
        cipher = AESCipher(t)
        decrypted = cipher.decrypt(data["codechaine"])
        if str(decrypted) == str(codechaine):
            result = True
            break
    if not result:
        logger.warning("authentification False %s" % (codechaine))

        sendErrorConnectionConf(objectxmpp, sessionid, msg)
    return result


def distHaversine(p1, p2):
    """
    Calculate the distance (in km) between 2 points specified by their
    latitude/longitude using Haversine formula

     de : Haversine formula - R. W. Sinnott, "Virtues of the Haversine",
          Sky and Telescope, vol 68, no 2, 1984
          http://www.census.gov/cgi-bin/geo/gisfaq?Q5.1

    """
    rt = 6371  # Mean Earth radius in km
    # We consider latitude at rt whatever the point
    dLat = p2.lat - p1.lat
    dLong = p2.lon - p1.lon
    a = sin(dLat / 2) * sin(dLat / 2) + cos(p1.lat) * cos(p2.lat) * sin(
        dLong / 2
    ) * sin(dLong / 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = rt * c
    return d


def Algorithm_Rule_Attribution_Agent_Relay_Server(
    objectxmpp, action, sessionid, data, msg
):
    """
    Allocate the Relay server of the machine and reconfigure the agent.

    Args:
        objectxmpp: Reference object to the xmpp server
        action: Name of the plugin
        sessionid: The SQL Alchemy session
        data: The xmpp message.
        msg: A dictionnary with informations like from and to where are going the messages.
    """
    codechaine = "%s" % (msg["from"])

    try:
        host = codechaine.split("/")[1]
    except Exception:
        host = msg["from"]

    logger.info("Configuring machine %s" % host)
    if data["machine"].split(".")[0] in objectxmpp.assessor_agent_showinfomachine:
        showinfomachine = True
        logger.info("showinfomachine is enabled for the machine %s" % (host))
    else:
        showinfomachine = False
        logger.debug("showinfomachine option is not enabled for the machine %s " % host)
        logger.debug(
            "If you want to enable it, add the information in the assessor_agent.ini(.local) file"
        )

    if data["adorgbymachine"] is not None and data["adorgbymachine"] != "":
        data["adorgbymachine"] = base64.b64decode(data["adorgbymachine"])
    if data["adorgbyuser"] is not None and data["adorgbyuser"] != "":
        data["adorgbyuser"] = base64.b64decode(data["adorgbyuser"])
    try:
        data["information"] = json.loads(base64.b64decode(data["completedatamachine"]))
        del data["completedatamachine"]
    except Exception:
        logger.error("decode msg error from %s" % (codechaine))
        sendErrorConnectionConf(objectxmpp, sessionid, msg)
        return
    if data["agenttype"] == "relayserver":
        objectxmpp.sendErrorConnectionConf(objectxmpp, sessionid, msg)
        return
    if "codechaine" not in data:
        logger.error("missing authentification from %s" % (codechaine))
        sendErrorConnectionConf(objectxmpp, sessionid, msg)
        return

    if not testsignaturecodechaine(objectxmpp, data, sessionid, msg):
        return

    displayData(objectxmpp, data)

    if not (
        "information" in data
        and "users" in data["information"]
        and len(data["information"]["users"]) > 0
    ):
        data["information"]["users"].append("system")
    else:
        XmppMasterDatabase().log(
            "Warning no user determinated for the machine : %s "
            % (data["information"]["info"]["hostname"])
        )

    msgstr = "Search Relay Server for connection from user %s hostname %s" % (
        data["information"]["users"][0],
        data["information"]["info"]["hostname"],
    )
    if showinfomachine:
        logger.info(msgstr)
    XmppMasterDatabase().log(msgstr)

    adorgbymachinebool = False
    if "adorgbymachine" in data and data["adorgbymachine"] != "":
        adorgbymachinebool = True

    adorgbyuserbool = False
    if "adorgbyuser" in data and data["adorgbyuser"] != "":
        adorgbyuserbool = True

    # Defining relay server for connection
    # Order of rules to be applied
    ordre = XmppMasterDatabase().Orderrules()
    odr = [int(x[0]) for x in ordre]
    if showinfomachine:
        logger.info("Rule order : %s " % odr)
    result = []
    for x in ordre:
        # User Rule : 1
        if x[0] == 1:
            if showinfomachine:
                logger.info("Analysis the 1st rule : select the relay server by User")
            result1 = XmppMasterDatabase().algoruleuser(data["information"]["users"][0])
            if len(result1) > 0:
                if showinfomachine:
                    logger.info("Applied : Associate the relay server based on user.")
                result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                    result1[0].id
                )
                msg_log(
                    "The User",
                    data["information"]["info"]["hostname"],
                    data["information"]["users"][0],
                    result,
                    objectxmpp,
                    data,
                )
                break
        # Hostname Rule : 2
        elif x[0] == 2:
            if showinfomachine:
                logger.info(
                    "Analysis the 2nd rule : select the relay server by Hostname"
                )
            result1 = XmppMasterDatabase().algorulehostname(
                data["information"]["info"]["hostname"]
            )
            if len(result1) > 0:
                if showinfomachine:
                    logger.info("applied rule Associate relay server based on hostname")
                result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                    result1[0].id
                )
                msg_log(
                    "The Hostname",
                    data["information"]["info"]["hostname"],
                    data["information"]["users"][0],
                    result,
                    objectxmpp,
                    data,
                )
                break
        # Location Rule : 3
        elif x[0] == 3:
            if showinfomachine:
                logger.info(
                    "Analysis the 3rd rule : select the relay server by Geolocalisation"
                )
            if (
                "geolocalisation" in data
                and data["geolocalisation"] is not None
                and len(data["geolocalisation"]) > 0
            ):
                # initialization parameter geolocalisation
                tabinformation = {
                    "longitude": "unknown",
                    "latitude": "unknown",
                    "city": "unknown",
                    "region_name": "unknown",
                    "time_zone": "unknown",
                    "zip_code": "unknown",
                    "country_iso": "",
                    "country": "unknown",
                }
                for geovariable in tabinformation:
                    try:
                        tabinformation[geovariable] = str(
                            data["geolocalisation"][geovariable]
                        )
                    except Exception:
                        logger.error("\n%s" % (traceback.format_exc()))
                        pass

                if showinfomachine:
                    logger.info(
                        "Geoposition of machine %s %s " % (codechaine, tabinformation)
                    )
                if (
                    tabinformation["longitude"] != "unknown"
                    and tabinformation["latitude"] != "unknown"
                    and tabinformation["longitude"] != ""
                    and tabinformation["latitude"] != ""
                ):
                    if showinfomachine:
                        logger.info(
                            "Analysis the 3rd rule : select the relay server by Geolocalisation"
                        )
                    if showinfomachine:
                        logger.info(
                            "Geoposition of machine %s [ %s : %s]"
                            % (
                                data["information"]["info"]["hostname"],
                                tabinformation["latitude"],
                                tabinformation["longitude"],
                            )
                        )

                    pointmachine = Point(
                        float(tabinformation["latitude"]),
                        float(tabinformation["longitude"]),
                    )
                    distance = 40000000000
                    listeserver = set()
                    relayserver = -1
                    result = []
                    try:
                        result1 = XmppMasterDatabase().IdlonglatServerRelay(
                            data["classutil"]
                        )
                        for x in result1:
                            # pour tout les relay on clacule la distance a vol oiseau.
                            if x[1] not in ["", "unknown"] and x[2] not in [
                                "",
                                "unknown",
                            ]:
                                try:
                                    xpoint = float(x[2])
                                    ypoint = float(x[1])
                                except Exception:
                                    continue
                                pointrelay = Point(xpoint, ypoint)
                                if str(pointrelay.lat) == str(pointmachine.lat) and str(
                                    pointrelay.lon
                                ) == str(pointmachine.lon):
                                    distancecalculated = 0
                                else:
                                    distancecalculated = distHaversine(
                                        pointrelay, pointmachine
                                    )
                                if showinfomachine:
                                    logger.info(
                                        "Geoposition ars id %s: long %s lat %s dist %s km"
                                        % (x[0], x[1], x[2], distancecalculated)
                                    )
                                if distancecalculated < distance:
                                    listeserver = {x[0]}
                                    distance = distancecalculated
                                    relayserver = x[0]  # x[0] id du relayserver
                                if distancecalculated == distance:
                                    # il peut y avoir plusieurs ars a la meme distance.
                                    listeserver.add(x[0])
                        listeserver = list(listeserver)
                        nbserver = len(listeserver)
                        if nbserver > 1:
                            index = randint(0, nbserver - 1)
                            # on choisi 1 ARS dans la liste
                            logger.warning(
                                "Geoposition Rule returned %d "
                                "relay servers for machine"
                                "%s user %s \nPossible relay servers"
                                " : id list %s "
                                % (
                                    nbserver,
                                    data["information"]["info"]["hostname"],
                                    data["information"]["users"][0],
                                    listeserver,
                                )
                            )
                            logger.warning(
                                "ARS Random choice : %s" % listeserver[index]
                            )
                            result = (
                                XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                                    listeserver[index]
                                )
                            )
                            msg_log(
                                "The Geoposition",
                                data["information"]["info"]["hostname"],
                                data["information"]["users"][0],
                                result,
                                objectxmpp,
                                data,
                            )
                            break
                        elif nbserver == 1:
                            # il n y a 1 seul relay server de trouve
                            result = (
                                XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                                    relayserver
                                )
                            )
                            msg_log(
                                "The Geoposition",
                                data["information"]["info"]["hostname"],
                                data["information"]["users"][0],
                                result,
                                objectxmpp,
                                data,
                            )
                            break
                        else:
                            logger.warning(
                                "no ARS found by Geoposition for machine %s"
                                % msg["from"]
                            )
                            continue
                    except KeyError:
                        logger.error("Error algo rule 3")
                        logger.error("\n%s" % (traceback.format_exc()))
                        continue
            else:
                # regle trois aucun ars trouve
                logger.warning(
                    "On Remote Machine %s, geolocalisation misssing" % msg["from"]
                )
                continue
        # Subnet Rule : 4
        elif x[0] == 4:
            if showinfomachine:
                logger.info(
                    "Analysis the 4th rule : select the relay server in same subnet"
                )
                logger.info("rule subnet : Test if network are identical")
            subnetexist = False
            for z in data["information"]["listipinfo"]:
                if z["ipaddress"] is None or z["mask"] is None:
                    continue

                result1 = XmppMasterDatabase().algorulesubnet(
                    subnetnetwork(z["ipaddress"], z["mask"]), data["classutil"]
                )
                if len(result1) > 0:
                    if showinfomachine:
                        logger.info(
                            "Applied rule : select the relay server in same subnet"
                        )
                    subnetexist = True
                    result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                        result1[0].id
                    )
                    msg_log(
                        "same subnet",
                        data["information"]["info"]["hostname"],
                        data["information"]["users"][0],
                        result,
                        objectxmpp,
                        data,
                    )
                    break
            if subnetexist:
                break

        # Default Rule : 5
        elif x[0] == 5:
            if showinfomachine:
                logger.info(
                    "analysis the 5th rule : "
                    "use default relay server %s" % objectxmpp.assessor_agent_serverip
                )
            result = XmppMasterDatabase().jidrelayserverforip(
                objectxmpp.assessor_agent_serverip
            )
            msg_log(
                "use default relay server",
                data["information"]["info"]["hostname"],
                data["information"]["users"][0],
                result,
                objectxmpp,
                data,
            )
            break

        # Load Balancer Rule : 6
        elif x[0] == 6:
            if showinfomachine:
                logger.info(
                    "Analysis the 6th rule : select the relay on less "
                    "requested ARS (load balancer)"
                )
            result1 = XmppMasterDatabase().algoruleloadbalancer()
            if len(result1) > 0:
                if showinfomachine:
                    logger.info("Applied : Rule Chooses the less requested ARS.")
                result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                    result1[0].id
                )
                msg_log(
                    "Load Balancer",
                    data["information"]["info"]["hostname"],
                    data["information"]["users"][0],
                    result,
                    objectxmpp,
                    data,
                )
                break

        # OU Machine Rule : 7
        elif x[0] == 7:
            # "AD organised by machines "
            if showinfomachine:
                logger.info("Analysis the 7th rule : AD organized by machines")
            if adorgbymachinebool:
                result1 = XmppMasterDatabase().algoruleadorganisedbymachines(
                    manage_fqdn_window_activedirectory.getOrganizationADmachineOU(
                        data["adorgbymachine"]
                    )
                )
                if len(result1) > 0:
                    if showinfomachine:
                        logger.info("Applied rule : AD organized by machines")
                        logger.info("We matched the relayserver ID: %s" % result1)

                    result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                        result1[0].id
                    )
                    msg_log(
                        "AD organized by machine",
                        data["information"]["info"]["hostname"],
                        data["information"]["users"][0],
                        result,
                        objectxmpp,
                        data,
                    )
                    break

        # OU User Rule : 8
        elif x[0] == 8:
            # "AD organised by users"
            if showinfomachine:
                logger.info("Analysis the 8th rule : AD organized by users")
            if adorgbyuserbool:
                result1 = XmppMasterDatabase().algoruleadorganisedbyusers(
                    manage_fqdn_window_activedirectory.getOrganizationADuserOU(
                        data["adorgbyuser"]
                    )
                )
                if len(result1) > 0:
                    if showinfomachine:
                        logger.info("Applied rule : AD organized by users")
                    result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                        result1[0].id
                    )
                    msg_log(
                        "AD organized by User",
                        data["information"]["info"]["hostname"],
                        data["information"]["users"][0],
                        result,
                        objectxmpp,
                        data,
                    )
                    break

        # Network Rule : 9
        elif x[0] == 9:
            # Associates relay server based on network address
            if showinfomachine:
                logger.info(
                    "Analysis the 9th rule : Associate relay server based on network address"
                )
            networkaddress = netaddr.IPNetwork(
                data["xmppip"] + "/" + data["xmppmask"]
            ).cidr
            if showinfomachine:
                logger.info("Network address: %s" % networkaddress)
            result1 = XmppMasterDatabase().algorulebynetworkaddress(
                networkaddress, data["classutil"]
            )
            if len(result1) > 0:
                if showinfomachine:
                    logger.info(
                        "Applied Rule : Associate relay server based on network address"
                    )
                result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                    result1[0].id
                )
                msg_log(
                    "network address",
                    data["information"]["info"]["hostname"],
                    data["information"]["users"][0],
                    result,
                    objectxmpp,
                    data,
                )
                break
                # Network Rule : 10
        elif x[0] == 10:
            # Associates relay server based on network address
            if showinfomachine:
                logger.info(
                    "Analysis the 10th rule : Associate relay server based on netmask address"
                )
                logger.info("Net mask address: %s" % data["xmppmask"])
            result1 = XmppMasterDatabase().algorulebynetmaskaddress(
                data["xmppmask"], data["classutil"]
            )
            if len(result1) > 0:
                logger.info(
                    "Applied Rule : Associate relay server based on net Mask address"
                )
                result = XmppMasterDatabase().IpAndPortConnectionFromServerRelay(
                    result1[0].id
                )
                msg_log(
                    "net mask address",
                    data["information"]["info"]["hostname"],
                    data["information"]["users"][0],
                    result,
                    objectxmpp,
                    data,
                )
                break

    try:
        msg_string = (
            "[user: %s / hostname: %s] : "
            "Relay server assigned: "
            "%s:%s"
            % (
                data["information"]["users"][0],
                data["information"]["info"]["hostname"],
                result[0],
                result[1],
            )
        )
        if showinfomachine:
            logger.info(msg_string)
        XmppMasterDatabase().setlogxmpp(
            msg_string,
            "conf",
            sessionid,
            -1,
            data["information"]["info"]["hostname"],
            "",
            "",
            "Configuration | Assessor",
            "",
            objectxmpp.boundjid.bare,
            objectxmpp.boundjid.bare,
        )

    except Exception:
        logger.warning("Relay server attributed by default")
        XmppMasterDatabase().setlogxmpp(
            "Relay server attributed by default",
            "conf",
            sessionid,
            -1,
            data["information"]["info"]["hostname"],
            "",
            "",
            "Configuration | Assessor",
            "",
            objectxmpp.boundjid.bare,
            objectxmpp.boundjid.bare,
        )
        try:
            result = XmppMasterDatabase().jidrelayserverforip(
                objectxmpp.assessor_agent_serverip
            )
        except Exception:
            logger.warning("Unable to assign the relay server : missing")
            result = XmppMasterDatabase().jidrelayserverforip(
                objectxmpp.assessor_agent_serverip
            )
            msg_log(
                "error use default relay server",
                data["information"]["info"]["hostname"],
                data["information"]["users"][0],
                result,
                objectxmpp,
                data,
            )
            XmppMasterDatabase().setlogxmpp(
                "Unable to use default relay server: missing",
                "conf",
                sessionid,
                -1,
                data["information"]["info"]["hostname"],
                "",
                "",
                "Configuration | Notify | Assessor",
                "",
                objectxmpp.boundjid.bare,
                objectxmpp.boundjid.bare,
            )
    try:
        try:
            listars = XmppMasterDatabase().getRelayServerofclusterFromjidars(
                result[2], moderelayserver="static"
            )
            if len(listars) == 0:
                listars = XmppMasterDatabase().getRelayServerofclusterFromjidars(
                    result[2], moderelayserver="static", enablears=None
                )
                logger.warning(
                    "agent %s. ARS %s is found but it is "
                    "stopped." % (data["information"]["info"]["hostname"], result[2])
                )
                logger.warning("ACTION: Re-start the ARS on %s." % (result[2]))
                logger.warning(
                    "try to apply the configuration on agent "
                    "%s of down default ars."
                    % (data["information"]["info"]["hostname"])
                )
        except (RuntimeError, TypeError, NameError):
            msglog = (
                "Verify configuration assessor file name assessor_agent.ini.local."
                "\n\terror parameter serverip "
                "\nsearch ipconnection in table relayserver for default ARS"
                "\nserverip = %s "
                "configuration error" % (objectxmpp.assessor_agent_serverip)
            )
            msglog1 = (
                "ERROR: Unable to assign a relay server "
                "to an agent %s" % data["information"]["info"]["hostname"]
            )
            logger.error(msglog1)
            logger.error(msglog)
            sendErrorConnectionConf(objectxmpp, sessionid, msg)
            XmppMasterDatabase().setlogxmpp(
                msglog1,
                "conf",
                sessionid,
                -1,
                data["information"]["info"]["hostname"],
                "",
                "",
                "Configuration | Notify | Assessor",
                "",
                objectxmpp.boundjid.bare,
                objectxmpp.boundjid.bare,
            )
            XmppMasterDatabase().setlogxmpp(
                msglog,
                "conf",
                sessionid,
                -1,
                data["information"]["info"]["hostname"],
                "",
                "",
                "Configuration | Notify | Assessor",
                "",
                objectxmpp.boundjid.bare,
                objectxmpp.boundjid.bare,
            )
            return
        z = [listars[x] for x in listars]
        z1 = sorted(z, key=operator.itemgetter(4))
        arsjid = XmppMasterDatabase().getRelayServerfromjid("rspulse@pulse")
        # Start relay server agent configuration
        # we order the ARS from the least used to the most used.
        response = {
            "action": "resultconnectionconf",
            "sessionid": data["sessionid"],
            "data": z1,
            "syncthing": objectxmpp.assessor_agent_announce_server,
            "ret": 0,
        }
        if len(listars) == 0:
            logger.warning(
                "No configuration sent to machine "
                "agent %s. ARS %s is found but it is stopped."
                % (data["information"]["info"]["hostname"], result[2])
            )
            logger.warning(
                "ACTION: Re-start the ARS on %s, and wait for"
                " the agent to run its reconfiguration." % (result[2])
            )
            objectxmpp.xmpplog(
                "No configuration sent to machine "
                "agent %s. ARS %s is found but it is stopped."
                % (result[2], data["information"]["info"]["hostname"]),
                type="conf",
                sessionname=sessionid,
                priority=-1,
                action="xmpplog",
                who=data["information"]["info"]["hostname"],
                module="Configuration | Notify | Assessor",
                date=None,
                fromuser=objectxmpp.boundjid.bare,
            )
            sendErrorConnectionConf(objectxmpp, sessionid, msg)
            return

        agentsubscription = "master@pulse"
        if (
            "substitute" in data
            and "conflist" in data["substitute"]
            and len(data["substitute"]["conflist"]) > 0
        ):
            response["substitute"] = XmppMasterDatabase().substituteinfo(
                data["substitute"], z1[0][2]
            )
            response["substitute"]["ars_chooose_for_substitute"] = z1[0][2]
            if showinfomachine:
                logger.info(
                    "substitute resend to agent : %s"
                    % json.dumps(response["substitute"], indent=4)
                )
                logger.info(
                    "substitute resend to agent : %s"
                    % json.dumps(response["substitute"]["subscription"], indent=4)
                )
            if "subscription" in response["substitute"]:
                agentsubscription = response["substitute"]["subscription"][0]
                listmacadress = []
                for mac in data["information"]["listipinfo"]:
                    listmacadress.append(mac["macaddress"])
                XmppMasterDatabase().setuplistSubscription(
                    listmacadress, agentsubscription
                )
        if showinfomachine:
            logger.info("msg['from'] %s" % data["agent_machine_name"])
        XmppMasterDatabase().updateMachinereconf(data["agent_machine_name"])
        if showinfomachine:
            logger.info("updateMachinereconf %s " % data["agent_machine_name"])

        response["ssh_public_key"] = XmppMasterDatabase().get_public_key_of_ars(
            listars.keys()
        )

        objectxmpp.send_message(
            mto=msg["from"], mbody=json.dumps(response), mtype="chat"
        )
    except IndexError as indexError:
        sendErrorConnectionConf(objectxmpp, sessionid, msg)
        logger.error(
            "It seems that the relay server is not correctly "
            "configured.\n"
            "Please verify the substituteconf mysql table"
        )

        logger.error("If this is a new install please replay the ansible")

    except Exception:
        sendErrorConnectionConf(objectxmpp, sessionid, msg)
        logger.error("Unable to assign a relay server to an agent")
        logger.error("\n%s" % (traceback.format_exc()))
        XmppMasterDatabase().setlogxmpp(
            "Unable to assign a relay server to an agent",
            "conf",
            sessionid,
            -1,
            data["information"]["info"]["hostname"],
            "",
            "",
            "Configuration | Notify | Assessor",
            "",
            objectxmpp.boundjid.bare,
            objectxmpp.boundjid.bare,
        )
        XmppMasterDatabase().setlogxmpp(
            "Error: %s" % (traceback.format_exc()),
            "conf",
            sessionid,
            -1,
            data["information"]["info"]["hostname"],
            "",
            "",
            "Configuration | Notify | Assessor",
            "",
            objectxmpp.boundjid.bare,
            objectxmpp.boundjid.bare,
        )


def msg_log(msg_header, hostname, user, result, objectxmpp, data):
    if data["machine"].split(".")[0] in objectxmpp.assessor_agent_showinfomachine:
        logger.info(
            "%s Rule selects "
            "the relay server for machine "
            "%s user %s \n %s" % (msg_header, hostname, user, result)
        )
        pass


def displayData(objectxmpp, data):
    if data["machine"].split(".")[0] in objectxmpp.assessor_agent_showinfomachine:
        logger.info("--------------------------")
        if "action" in data and data["action"] == "assessor_agent":
            logger.info(
                "** INFORMATION FROM CONFIGURATION AGENT FOR %s"
                % data["agenttype"].upper()
            )
        else:
            logger.info(
                "** INFORMATION FROM AGENT %s %s"
                % (data["agenttype"].upper(), data["action"])
            )
        logger.info("__________________________")
        logger.info("MACHINE INFORMATION")
        logger.info("Deployment name : %s" % data["deployment"])
        logger.info("From : %s" % data["who"])
        logger.info("Jid from : %s" % data["from"])
        logger.info("Machine : %s" % data["machine"])
        logger.info("Platform : %s" % data["platform"])
        if "versionagent" in data:
            logger.info("Version agent : %s" % data["versionagent"])
        if "win" in data["platform"].lower():
            logger.info("__________________________")
            logger.info("ACTIVE DIRECTORY")
            logger.info("OU Active directory")
            logger.info("OU by machine : %s" % data["adorgbymachine"])
            logger.info("OU by user : %s" % data["adorgbyuser"])
            if "lastusersession" in data:
                logger.info("last user session: %s" % data["lastusersession"])
        logger.info("--------------------------------")
        logger.info("----MACHINE XMPP INFORMATION----")
        logger.info("portxmpp : %s" % data["portxmpp"])
        logger.info("serverxmpp : %s" % data["serverxmpp"])
        logger.info("xmppip : %s" % data["xmppip"])
        logger.info("agenttype : %s" % data["agenttype"])
        if "moderelayserver" in data:
            logger.info("mode relay server : %s" % data["moderelayserver"])
        logger.info("baseurlguacamole : %s" % data["baseurlguacamole"])
        logger.info("xmppmask : %s" % data["xmppmask"])
        logger.info("subnetxmpp : %s" % data["subnetxmpp"])
        logger.info("xmppbroadcast : %s" % data["xmppbroadcast"])
        logger.info("xmppdhcp : %s" % data["xmppdhcp"])
        logger.info("xmppdhcpserver : %s" % data["xmppdhcpserver"])
        logger.info("xmppgateway : %s" % data["xmppgateway"])
        logger.info("xmppmacaddress : %s" % data["xmppmacaddress"])
        logger.info("xmppmacnotshortened : %s" % data["xmppmacnotshortened"])
        if data["agenttype"] == "relayserver":
            logger.info("package server : %s" % data["packageserver"])

        if "ipconnection" in data:
            logger.info("ipconnection : %s" % data["ipconnection"])
        if "portconnection" in data:
            logger.info("portconnection : %s" % data["portconnection"])
        if "classutil" in data:
            logger.info("classutil : %s" % data["classutil"])
        if "ippublic" in data:
            logger.info("ippublic : %s" % data["ippublic"])
        if "geolocalisation" in data:
            logger.info("------------LOCALISATION-----------")
            logger.info(
                "geolocalisation : %s" % json.dumps(data["geolocalisation"], indent=4)
            )
        if "win" in data["platform"].lower():
            if "adorgbymachine" in data and data["adorgbymachine"]:
                logger.info("AD found for the Machine : %s" % data["adorgbymachine"])
            else:
                logger.info("No AD found for the Machine")
            if "adorgbyuser" in data and data["adorgbyuser"]:
                logger.info("AD found for the User : %s" % data["adorgbyuser"])
            else:
                logger.info("No AD found for the User")
        logger.info("-----------------------------------")

        logger.info("DETAILED INFORMATION")
        if "information" in data:
            logger.info(
                "%s" % json.dumps(data["information"], indent=4, sort_keys=True)
            )


def sendErrorConnectionConf(objectxmpp, session, msg):
    response = {
        "action": "resultconnectionconf",
        "sessionid": session,
        "data": [],
        "syncthing": "",
        "ret": 255,
    }
    objectxmpp.send_message(mto=msg["from"], mbody=json.dumps(response), mtype="chat")


def read_conf_assessor(objectxmpp):
    """
    lit la configuration du plugin
    le repertoire ou doit se trouver le fichier de configuration est dans la variable objectxmpp.config.pathdirconffile
    """
    objectxmpp.assessor_agent_showinfomachine = []
    objectxmpp.simultaneous_processing = 50
    objectxmpp.assessor_agent_errorconf = False
    objectxmpp.show_queue_status = False
    namefichierconf = plugin["NAME"] + ".ini"
    objectxmpp.pathfileconf = os.path.join(
        objectxmpp.config.pathdirconffile, namefichierconf
    )
    if not os.path.isfile(objectxmpp.pathfileconf):
        logger.error(
            "plugin %s\nConfiguration file  missing\n  %s"
            % (plugin["NAME"], objectxmpp.pathfileconf)
        )
        message_config(plugin["NAME"], objectxmpp.pathfileconf)
        objectxmpp.assessor_agent_errorconf = True
        if statfuncton:
            objectxmpp.stat_assessor_agent.display_param_config(msg="DEFAULT")
        return False
    else:
        logger.info(
            "Configuration file plugin %s is %s"
            % (plugin["NAME"], objectxmpp.pathfileconf)
        )
        objectxmpp.assessor_agent_errorconf = False
        Config = configparser.ConfigParser()
        Config.read(objectxmpp.pathfileconf)
        if os.path.exists(objectxmpp.pathfileconf + ".local"):
            Config.read(objectxmpp.pathfileconf + ".local")
        if Config.has_section("parameters"):
            if Config.has_option("parameters", "showinfomachine"):
                paramshowinfomachine = Config.get("parameters", "showinfomachine")
                objectxmpp.assessor_agent_showinfomachine = [
                    str(x.strip())
                    for x in paramshowinfomachine.split(",")
                    if x.strip() != ""
                ]
            else:
                # default configuration
                objectxmpp.assessor_agent_showinfomachine = []
                logger.warning("showinfomachine default value is []")

            if Config.has_option("parameters", "keyAES32"):
                paramkeyAES32 = Config.get("parameters", "keyAES32")
                objectxmpp.assessor_agent_keyAES32 = [
                    str(x.strip()) for x in paramkeyAES32.split(",") if x.strip() != ""
                ]

                if len(objectxmpp.assessor_agent_keyAES32) > 0:
                    for keyAES32items in objectxmpp.assessor_agent_keyAES32:
                        if len(keyAES32items) != 32:
                            logger.warning(
                                "parameter taille keyAES32 %s" % keyAES32items
                            )
                            objectxmpp.assessor_agent_errorconf = True
            else:
                logger.error("keyAES32 Key AES 32 char [Mandatory parameter keyAES32]")
                objectxmpp.assessor_agent_errorconf = True

            if Config.has_option("parameters", "announce_server"):
                objectxmpp.assessor_agent_announce_server = Config.get(
                    "parameters", "announce_server"
                )
            else:
                objectxmpp.assessor_agent_announce_server = "default"
                logger.warning("announce_server for syncthing default value is default")

            if Config.has_option("parameters", "simultaneous_processing"):
                objectxmpp.simultaneous_processing = Config.getint(
                    "parameters", "simultaneous_processing"
                )
            else:
                objectxmpp.simultaneous_processing = 50

            if Config.has_option("parameters", "show_queue_status"):
                objectxmpp.show_queue_status = Config.getboolean(
                    "parameters", "show_queue_status"
                )
            else:
                objectxmpp.show_queue_status = False

            # default connection ##########################
            # Connection server parameters if no relay server is available ####

            if Config.has_option("parameters", "serverip"):
                objectxmpp.assessor_agent_serverip = ipfromdns(
                    Config.get("parameters", "serverip")
                )
                if objectxmpp.assessor_agent_serverip == "":
                    logger.error(
                        "see parameter [serverip] in file : %s "
                        "if Connection server parameters "
                        "if no relay server is available" % objectxmpp.pathfileconf
                    )
                    objectxmpp.assessor_agent_errorconf = True
                ipdatadown = ["localhost", "127.0.0.1"]
                for ip in ipdatadown:
                    if objectxmpp.assessor_agent_serverip == ip:
                        logger.error(
                            "see parameter [serverip] in file : %s "
                            "if Connection server parameters "
                            "if no relay server is available" % objectxmpp.pathfileconf
                        )
                        logging.getLogger().error(
                            'parameter section "parameters" '
                            "serverip must not be %s" % ip
                        )
                        objectxmpp.assessor_agent_errorconf = True
            else:
                logger.error(
                    "see parameter [serverip] "
                    "missing in file : %s " % objectxmpp.pathfileconf
                )
                objectxmpp.assessor_agent_errorconf = True

            if Config.has_option("parameters", "port"):
                objectxmpp.assessor_agent_port = Config.get("parameters", "port")
                if objectxmpp.assessor_agent_port == "":
                    logger.error(
                        "see parameter [port] in file : %s "
                        "if Connection server parameters "
                        "if no relay server is available" % objectxmpp.pathfileconf
                    )
                    objectxmpp.assessor_agent_errorconf = True
            else:
                logger.error("default value parameter [port] 5222")
                objectxmpp.assessor_agent_port = "5222"

            if Config.has_option("parameters", "guacamole_baseurl"):
                objectxmpp.assessor_agent_baseurlguacamole = Config.get(
                    "parameters", "guacamole_baseurl"
                )
                if objectxmpp.assessor_agent_baseurlguacamole == "":
                    logger.error(
                        "see parameter [guacamole_baseurl] in file : %s "
                        "if Connection server parameters "
                        "if no relay server is available" % objectxmpp.pathfileconf
                    )
                    objectxmpp.assessor_agent_errorconf = True
            else:
                logger.error(
                    "see parameter [guacamole_baseurl] "
                    "missing in file : %s " % objectxmpp.pathfileconf
                )
                objectxmpp.assessor_agent_errorconf = True
            if statfuncton:
                objectxmpp.stat_assessor_agent.load_param_lap_time_stat_(Config)
                objectxmpp.stat_assessor_agent.display_param_config("CONFIG")
        else:
            logger.error(
                "see SECTION [parameters] mising in file : %s "
                % objectxmpp.pathfileconf
            )
            objectxmpp.assessor_agent_errorconf = True
            if statfuncton:
                objectxmpp.stat_assessor_agent.display_param_config("DEFAULT")
        if objectxmpp.assessor_agent_errorconf:
            message_config(plugin["NAME"], objectxmpp.pathfileconf)
            return False
    return True


def message_config(nameplugin, pathfileconf):
    msg = (
        """=========configuration assessor plugin master==========="
        check MASTER assessor plugin config file "%s"

        The following parameters must be defined:

        serverip : IP of default ARS (or its FQDN if it can be resolved by the clients)
        port : Port of default ARS
        guacamole_baseurl : Guacamole base url of default ARS
        announce_server : Syncthing announce server url of default ARS
        showinfomachine : list of names of the machines whose information we want displayed
        keyAES32 : List of AES keys for checking the agent"""
        % pathfileconf
    )
    logger.error("%s" % msg)
    logger.error(
        "For the [%s] plugin, it is mandatory "
        "to define the following parameters." % (nameplugin)
    )
    logger.error("in files [%s] and file [%s.local]" % (pathfileconf, pathfileconf))
    logger.error("# Connection server parameters if no relay server is available")
    logger.error("[parameters]")
    logger.error("# XMPP server")
    logger.error("serverip = 192.168.56.2 #Mandatory parameter")
    logger.error("# XMPP port")
    logger.error("port = 5222 #Mandatory parameter")
    logger.error("# The location of the guacamole server.")
    logger.error(
        "guacamole_baseurl = http://192.168.56.2/guacamole/ #Mandatory parameter"
    )
    logger.error("#for syncthing")
    logger.error(
        "announce_server = https://192.168.56.2:8443/?id=IGQIW2T"
        "-OHEFK3P-JHSB6KH-OHHYABS-YEWJRVC-M6F4NLZ-D6U55ES-VXIVMA3"
    )
    logger.error("#for authentification key AES 32 chars")
    logger.error("keyAES32 = abcdefghijklnmopqrstuvwxyz012345")
    logger.error("#maximum number of simultaneous processing")
    logger.error("simultaneous_processing = 50")
    logger.error("showinfomachine = infra_jfk_deb1, infra_jfk_deb2")
