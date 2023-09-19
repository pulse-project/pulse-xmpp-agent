# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: 2016-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

import logging
from lib.utils import set_logging_level
logger = logging.getLogger()
DEBUGPULSEPLUGIN = 25
plugin = {"VERSION": "1.306", "NAME": "resultapplicationdeploymentjson", "TYPE": "all"}  # fmt: skip

@set_logging_level
def action(objectxmpp, action, sessionid, data, message, dataerreur):
    logger.debug("###################################################")
    logger.debug("call %s from %s" % (plugin, message["from"]))
    logger.debug("###################################################")

    if objectxmpp.session.isexist(sessionid):
        logging.getLogger().debug(
            "clear sessionid %s from %s" % (sessionid, message["from"])
        )
        objectxmpp.session.clearnoevent(sessionid)
