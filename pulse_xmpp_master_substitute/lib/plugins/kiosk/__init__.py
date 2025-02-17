# -*- coding: utf-8; -*-
# SPDX-FileCopyrightText: 2018-2023 Siveo <support@siveo.net>
# SPDX-License-Identifier: GPL-3.0-or-later

"""
kiosk database handler
"""
# SqlAlchemy
from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError

# PULSE2 modules
# from mmc.database.database_helper import DatabaseHelper
# from mmc.plugins.pkgs import get_xmpp_package, xmpp_packages_list, package_exists
from lib.plugins.kiosk.schema import (
    Profiles,
    Packages,
    Profile_has_package,
    Profile_has_ou,
    Acknowledgements,
)

# Imported last
import logging
import time
from lib.configuration import confParameter
import functools
from datetime import datetime

try:
    from sqlalchemy.orm.util import _entity_descriptor
except ImportError:
    from sqlalchemy.orm.base import _entity_descriptor

from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.automap import automap_base

Session = sessionmaker()


class Singleton(object):
    def __new__(type, *args):
        if "_the_instance" not in type.__dict__:
            type._the_instance = object.__new__(type)
        return type._the_instance


class DatabaseHelper(Singleton):
    # Session decorator to create and close session automatically
    @classmethod
    def _sessionkiosk(self, func):
        @functools.wraps(func)
        def __session(self, *args, **kw):
            created = False
            if not self.sessionkiosk:
                self.sessionkiosk = sessionmaker(bind=self.engine_kiosk_base)
                created = True
            result = func(self, self.session, *args, **kw)
            if created:
                self.sessionkiosk.close()
                self.sessionkiosk = None
            return result

        return __session

    # Session decorator to create and close session automatically
    @classmethod
    def _sessionm(self, func):
        @functools.wraps(func)
        def __sessionm(self, *args, **kw):
            session_factory = sessionmaker(bind=self.engine_kiosk_base)
            sessionmultithread = scoped_session(session_factory)
            result = func(self, sessionmultithread, *args, **kw)
            sessionmultithread.remove()
            return result

        return __sessionm


class KioskDatabase(DatabaseHelper):
    """
    Singleton Class to query the kioskmaster database.

    """

    is_activated = False

    def activate(self):  # jid, password, room, nick):
        if self.is_activated:
            return None
        self.logger = logging.getLogger()
        self.logger.debug("kiosk activation")
        self.engine = None
        self.sessionxmpp = None
        self.sessionglpi = None
        self.sessionkiosk = None
        self.config = confParameter()
        self.logger.info(
            "kiosk parameters connections is "
            " user = %s,host = %s, port = %s, schema = %s,"
            " poolrecycle = %s, poolsize = %s, pooltimeout %s"
            % (
                self.config.kiosk_dbuser,
                self.config.kiosk_dbhost,
                self.config.kiosk_dbport,
                self.config.kiosk_dbname,
                self.config.kiosk_dbpoolrecycle,
                self.config.kiosk_dbpoolsize,
                self.config.kiosk_dbpooltimeout,
            )
        )
        try:
            self.engine_kiosk_base = create_engine(
                "mysql://%s:%s@%s:%s/%s?charset=%s"
                % (
                    self.config.kiosk_dbuser,
                    self.config.kiosk_dbpasswd,
                    self.config.kiosk_dbhost,
                    self.config.kiosk_dbport,
                    self.config.kiosk_dbname,
                    self.config.charset,
                ),
                pool_recycle=self.config.kiosk_dbpoolrecycle,
                pool_size=self.config.kiosk_dbpoolsize,
                pool_timeout=self.config.kiosk_dbpooltimeout,
                convert_unicode=True,
            )
            self.Sessionkiosk = sessionmaker(bind=self.engine_kiosk_base)

            Base = automap_base()
            Base.prepare(self.engine_kiosk_base, reflect=True)

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
            self.logger.debug("kiosk finish activation")
            return True
        except Exception as e:
            self.logger.error("We failed to connect to the Kiosk database.")
            self.logger.error("Please verify your configuration")
            self.is_activated = False
            return False

    def initMappers(self):
        """
        Initialize all SQLalchemy mappers needed for the kioskmaster database
        """
        # No mapping is needed, all is done on schema file
        return

    def getDbConnection(self):
        NB_DB_CONN_TRY = 2
        ret = None
        for i in range(NB_DB_CONN_TRY):
            try:
                ret = self.db.connect()
            except DBAPIError as e:
                logging.getLogger().error(e)
            except Exception as e:
                logging.getLogger().error(e)
            if ret:
                break
        if not ret:
            raise Exception("Database kiosk connection error")
        return ret

    # =====================================================================
    # kiosk FUNCTIONS
    # =====================================================================

    @DatabaseHelper._sessionm
    def get_kiosk_version(self, session):
        """
        return version kiosk table
        """
        return session.execute("SELECT * FROM kiosk.version limit 1;")

    @DatabaseHelper._sessionm
    def get_profiles_list(self, session):
        """
        Return a list of all the existing profiles.
        The list contains all the elements of the profile.

        Returns:
            A list of all the founded entities.
        """
        ret = session.query(Profiles).all()
        lines = []
        for row in ret:
            lines.append(row.toDict())

        return lines

    @DatabaseHelper._sessionm
    def get_profile_list_for_OUList(self, session, OU):
        if len(OU) == 0:
            # return le profils par default
            return
        listou = "('" + "','".join(OU) + "')"
        sql = (
            """
            SELECT
                distinct
                kiosk.package.name as 'name_package',
                kiosk.profiles.name as 'name_profile',
                kiosk.package.description,
                kiosk.package.version_package,
                kiosk.package.software,
                kiosk.package.version_software,
                kiosk.package.package_uuid,
                kiosk.package.os,
                kiosk.package_has_profil.package_status
            FROM
                kiosk.package
                  inner join
                kiosk.package_has_profil on kiosk.package.id = kiosk.package_has_profil.package_uuid
                  inner join
                kiosk.profiles on profiles.id = kiosk.package_has_profil.profil_id

            WHERE
                kiosk.profiles.id in
                        (SELECT DISTINCT
                                profile_id
                            FROM
                                kiosk.profile_has_ous
                            WHERE
                                ou IN %s)
                    AND kiosk.profiles.active = 1;
                    """
            % listou
        )
        try:
            result = session.execute(sql)
            session.commit()
            session.flush()
            l = [x for x in result]
            return l
        except Exception as e:
            logging.getLogger().error("get_profile_list_for_OUList")
            logging.getLogger().error(str(e))
            return ""

    @DatabaseHelper._sessionm
    def get_profiles_name_list(self, session):
        """
        Return a list of all the existing profiles.
        The list is a shortcut of the method get_profiles_list.

        Returns:
            A list of the names for all the founded entities.
        """
        ret = session.query(Profiles.name).all()
        lines = []
        for row in ret:
            lines.append(row[0])
        return lines

    @DatabaseHelper._sessionm
    def create_profile(self, session, name, ous, active, packages):
        """
        Create a new profile for kiosk with the elements send.

        name:
            String which contains the name of the new profile
        ous:
            List of the selected OUs for this profile
        active:
            Int indicates if the profile is active (active = 1) or inactive (active = 0)
        packages:
            Dict which contains the packages associated with the profile and has the following form.
            {
                'allowed': [
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    },
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    }
                ],
                'restricted': [
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    },
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    }
                ]
            }

        return:
            The value returned is the id of the new profile
        """

        # refresh the packages in the database
        self.refresh_package_list()

        # Creation of the new profile
        now = time.strftime("%Y-%m-%d %H:%M:%S")

        sql = """INSERT INTO `kiosk`.`profiles` VALUES('%s','%s', '%s', '%s');""" % (
            "0",
            name,
            active,
            now,
        )

        session.execute(sql)
        session.commit()
        session.flush()

        # Search the id of this new profile
        result = session.query(Profiles.id).filter(Profiles.name == name)
        result = result.first()
        id = 0
        for row in result:
            id = str(row)

        # Remove all packages associations concerning this profile
        session.query(Profile_has_package).filter(
            Profile_has_package.profil_id == id
        ).delete()

        # The profile is now created, but the packages are not linked to it nor added into database.
        # If the package list is not empty, then firstly we get the status and
        # the uuid for each packages
        if len(packages) > 0:
            for status in list(packages.keys()):
                for uuid in packages[status]:
                    # get the package id and link it with the profile
                    result = session.query(Packages.id).filter(
                        Packages.package_uuid == uuid
                    )
                    result = result.first()
                    id_package = 0
                    for row in result:
                        id_package = str(row)

                    profile = Profile_has_package()
                    profile.profil_id = id
                    profile.package_id = id_package
                    profile.package_status = status

                    session.add(profile)
                    session.commit()
                    session.flush()
            # Finally we associate the OUs with the profile.
            if isinstance(ous, str) and ous == "":
                profile_ou = Profile_has_ou()
                profile_ou.profile_id = id
                profile_ou.ou = ous

                session.add(profile_ou)
                session.commit()
                session.flush()

            else:
                for ou in ous:
                    profile_ou = Profile_has_ou()
                    profile_ou.profile_id = id
                    profile_ou.ou = ou

                    session.add(profile_ou)
                    session.commit()
                    session.flush()
        return id

    # Get the real list of packages
    # package_list = xmpp_packages_list()

    # For each package in this list, add if not exists or update existing packages rows
    # for ref_pkg in package_list:
    # result = session.query(Packages.id).filter(Packages.package_uuid == ref_pkg['uuid']).all()

    # Create a Package object to interact with the database
    # package = get_xmpp_package(ref_pkg['uuid'])
    # os = json.loads(package).keys()[1]

    # Prepare a package object for the transaction with the database
    # pkg = Packages()
    # pkg.name = ref_pkg['software']
    # pkg.version_package = ref_pkg['version']
    # pkg.software = ref_pkg['software']
    # pkg.description = ref_pkg['description']
    # pkg.version_software = 0
    # pkg.package_uuid = ref_pkg['uuid']
    # pkg.os = os
    # If the package is not registered into database, it is added. Else it is updated
    # if len(result) == 0:
    # session.add(pkg)
    # session.commit()
    # session.flush()
    # else:
    # sql = """UPDATE `package` set name='%s', version_package='%s', software='%s',\
    # description='%s', package_uuid='%s', os='%s' WHERE package_uuid='%s';""" % (
    # ref_pkg['software'], ref_pkg['version'], ref_pkg['software'],
    # ref_pkg['description'], ref_pkg['uuid'], os, ref_pkg['uuid'])

    # Now we need to verify if all the registered packages are still existing into the server
    # sql = """SELECT id, package_uuid FROM package;"""
    # result = session.execute(sql)
    # session.commit()
    # session.flush()
    # packages_in_db = [element for element in result]

    # for package in packages_in_db:
    # if package_exists(package[1]):
    # pass
    # else:
    # session.query(Packages).filter(Packages.id == package[0]).delete()
    # session.commit()
    # session.flush()

    @DatabaseHelper._sessionm
    def delete_profile(self, session, id):
        """
        Delete the named profile from the table profiles.
        This method delete the profiles which have the specified name.

        Args:
            id: the id of the profile

        Returns:
            Boolean: True if success, else False
        """
        try:
            session.query(Profile_has_package).filter(
                Profile_has_package.profil_id == id
            ).delete()
            session.query(Profile_has_ou).filter(
                Profile_has_ou.profile_id == id
            ).delete()

            session.query(Profiles).filter(Profiles.id == id).delete()
            session.commit()
            session.flush()
            return True

        except Exception as e:
            return False

    @DatabaseHelper._sessionm
    def get_profile_by_id(self, session, id):
        """
        Return the profile datas and it's associated packages. This function create a view of the profile.
        id:
            Int it is the id of the wanted package
        return:
             Dict which contains the datas of the profile. The dict has this structure :
             {
                'active': '1',
                'creation_date': '2018-04-10 14:13:19',
                'id': '16',
                'ous': ['root/son/grand_son1', 'root/son/grand_son2']
                'packages': [
                    {
                        'status': 'restricted',
                        'uuid': 'df98c684-25ff-11e8-a488-0800271cd5f6',
                        'name': 'Notepad++'
                    },
                    {
                        'status': 'restricted',
                        'uuid': 'd2d143fa-3792-11e8-8364-0800278d719b',
                        'name': 'myPackage'
                    },
                    {
                        'status': 'allowed',
                        'uuid': '82c5996e-25ff-11e8-a488-0800271cd5f6',
                        'name': 'vlc'
                    },
                    {
                        'status': 'allowed',
                        'uuid': 'a6e11f44-25ff-11e8-a488-0800271cd5f6',
                        'name': 'Firefox'
                    }
                ],
                'name': 'qq'
             }
        """
        self.refresh_package_list()

        # get the profile row

        profile = session.query(Profiles).filter(Profiles.id == id).first()

        sql = """select \
        package.name as package_name,
        package.package_uuid,
        package_status
        from package \
        left join package_has_profil on package.id = package_has_profil.package_uuid \
        left join profiles on profiles.id = package_has_profil.profil_id\
        WHERE profiles.id = '%s';""" % (
            id
        )

        sql_ou = """SELECT ou FROM profile_has_ous WHERE profile_id = %s""" % (id)

        response = session.execute(sql)
        result = [
            {
                "uuid": element.package_uuid,
                "name": element.package_name,
                "status": element.package_status,
            }
            for element in response
        ]

        response_ou = session.execute(sql_ou)
        dict = {}

        for column in profile.__table__.columns:
            dict[column.name] = str(getattr(profile, column.name))
        dict["packages"] = result
        # generate a list for the OUs and it's added to the returned result
        dict["ous"] = [element.ou for element in response_ou]
        return dict

    @DatabaseHelper._sessionm
    def update_profile(self, session, id, name, ous, active, packages):
        """
        Update the specified profile
        id:
            Int is the id of the profile which will be updated
        name:
            String which contains the name of updated profile
        ous:
            List of the selected ous
        active:
            Int indicates if the profile is active (active = 1) or inactive (active = 0)
        packages:
            Dict which contains the packages associated with the profile and has the following form.
            {
                'allowed': [
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    },
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    }
                ],
                'restricted': [
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    },
                    {
                        'uuid': 'the-package-uuid',
                        'name': 'the-package-name'
                    }
                ]
            }
        """

        # Update the profile
        now = time.strftime("%Y-%m-%d %H:%M:%S")

        sql = """UPDATE profiles SET name='%s',active='%s' WHERE id='%s';""" % (
            name,
            active,
            id,
        )

        session.execute(sql)
        session.commit()
        session.flush()

        # Remove all packages associations concerning this profile
        session.query(Profile_has_package).filter(
            Profile_has_package.profil_id == id
        ).delete()
        session.query(Profile_has_ou).filter(Profile_has_ou.profile_id == id).delete()

        session.commit()
        session.flush()

        # Finally we associate the OUs with the profile.
        if isinstance(ous, str) and ous == "":
            profile_ou = Profile_has_ou()
            profile_ou.profile_id = id
            profile_ou.ou = ous

            session.add(profile_ou)
            session.commit()
            session.flush()

        else:
            for ou in ous:
                profile_ou = Profile_has_ou()
                profile_ou.profile_id = id
                profile_ou.ou = ou

                session.add(profile_ou)
                session.commit()
                session.flush()

        # The profile is now created, but the packages are not linked to it nor added into database.
        # If the package list is not empty, then firstly we get the status and
        # the uuid for each packages
        if len(packages) > 0:
            for status in list(packages.keys()):
                for uuid in packages[status]:
                    # get the package id and link it with the profile
                    result = session.query(Packages.id).filter(
                        Packages.package_uuid == uuid
                    )
                    result = result.first()
                    id_package = 0
                    for row in result:
                        id_package = str(row)

                    profile = Profile_has_package()
                    profile.profil_id = id
                    profile.package_id = id_package
                    profile.package_status = status

                    session.add(profile)
                    session.commit()
                    session.flush()

    @DatabaseHelper._sessionm
    def get_profiles_by_sources(self, session, sources):
        """get the list of profiles concerned by the specified sources
        - params: sources dict with the form {"source_name": [list of ous]}
        """
        profiles = []
        profile_ids = []

        for source in sources:
            try:
                query = (
                    session.query(Profiles)
                    .join(Profile_has_ou, Profile_has_ou.profile_id == Profiles.id)
                    .filter(
                        and_(
                            Profiles.source == source,
                            Profiles.active == 1,
                            Profile_has_ou.ou.like("%s%%" % sources[source]),
                        )
                    )
                    .group_by(Profiles.id)
                )
                query = query.all()
            except Exception as e:
                logging.getLogger().error("Error during profile selection : %s" % e)

            if query is not None:
                for row in query:
                    profiles.append(
                        {
                            "id": row.id,
                            "name": row.name,
                            "owner": row.owner,
                            "source": row.source,
                            "active": row.active if row.active is not None else False,
                        }
                    )
        return profiles

    @DatabaseHelper._sessionm
    def get_profile_list_for_profiles_list(self, session, profiles):
        profiles_ids = [profile["id"] for profile in profiles]
        if len(profiles_ids) == 0:
            # return le profils par default
            return []

        profiles_ids_str = ",".join(["%s" % profile["id"] for profile in profiles])
        sql = (
            """
            SELECT
                distinct
                pkgs.packages.label as 'name_package',
                kiosk.profiles.name as 'name_profile',
                pkgs.packages.description,
                pkgs.packages.version version_package,
                pkgs.packages.Qsoftware as software,
                pkgs.packages.Qversion version_software,
                pkgs.packages.uuid as package_uuid,
                pkgs.packages.os,
                kiosk.package_has_profil.package_status,
                kiosk.package_has_profil.id as id_package_has_profil
            FROM
                pkgs.packages
                  inner join
                kiosk.package_has_profil on pkgs.packages.uuid = kiosk.package_has_profil.package_uuid
                  inner join
                kiosk.profiles on profiles.id = kiosk.package_has_profil.profil_id
            WHERE
                kiosk.package_has_profil.profil_id in (%s)
                    """
            % profiles_ids_str
        )
        try:
            result = session.execute(sql)
            session.commit()
            session.flush()
            l = [x.decode("utf-8") if isinstance(x, bytes) else x for x in result]
            return l
        except Exception as e:
            logging.getLogger().error("get_profile_list_for_profiles_list")
            logging.getLogger().error(str(e))
            return []

    @DatabaseHelper._sessionm
    def get_acknowledges_for_package_profile(
        self, session, id_package_profil, uuid_package, user
    ):
        today = datetime.now()
        try:
            query = (
                session.query(Acknowledgements)
                .add_column(Profile_has_package.package_uuid)
                .filter(
                    and_(
                        Acknowledgements.id_package_has_profil == id_package_profil,
                        Acknowledgements.askuser == user,
                    ),
                    Acknowledgements.startdate <= today.strftime("%Y-%m-%d %H:%M:%S"),
                    or_(
                        Acknowledgements.enddate > today.strftime("%Y-%m-%d %H:%M:%S"),
                        Acknowledgements.enddate == None,
                        Acknowledgements.enddate == "",
                    ),
                )
                .join(
                    Profile_has_package,
                    Profile_has_package.id == Acknowledgements.id_package_has_profil,
                )
            )
            query = query.all()
        except Exception as e:
            self.logger.error(e)
        result = []

        if query is not None:
            for element, package_uuid in query:
                self.logger.error(23)
                askdate = ""
                startdate = ""
                enddate = ""
                if element.askdate is not None:
                    askdate = element.askdate.strftime("%Y-%m-%d %H:%M:%S")
                if element.startdate is not None:
                    startdate = element.startdate.strftime("%Y-%m-%d %H:%M:%S")
                if element.enddate is not None:
                    enddate = element.enddate.strftime("%Y-%m-%d %H:%M:%S")

                result.append(
                    {
                        "askuser": (
                            element.askuser if element.askuser is not None else ""
                        ),
                        "askdate": askdate,
                        "acknowledgedbyuser": (
                            element.acknowledgedbyuser
                            if element.acknowledgedbyuser is not None
                            else ""
                        ),
                        "startdate": startdate,
                        "enddate": enddate,
                        "status": element.status if element.status is not None else "",
                        "id": element.id if element.id is not None else "",
                        "id_package_has_profil": element.id_package_has_profil,
                        "package_uuid": package_uuid,
                    }
                )
        return result
