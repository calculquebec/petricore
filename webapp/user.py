import subprocess
import json
from pwd import getpwnam
from socket import gethostname
import external_access
import pymysql
import os
import ldap

# GLOBAL constants
# LOCALHOST = gethostname()
# LOCALHOST = LOCALHOST.split(".")[0]
SLURM_DB_HOST = "mgmt1.int." + external_access.get_domain_name()
SLURM_DB_USER = "petricore"
SLURM_DB_PASS = "yourPassword"
SLURM_DB_HOST = SLURM_DB_HOST.rstrip()
SLURM_DB_PORT = 3306
SLURM_ACCT_DB = "slurm_acct_db"
LDAP_HOST = "ldap://mgmt1"
FILE_LIMITS = {"scratch": 1000000, "home": 500000, "projects": 5000000}


class User:
    def __init__(self, username):
        # Declare and initalize
        self.__username = username
        self.__uid = getpwnam(username).pw_uid
        self.__storage_info = {}
        self.__storage_info["username"] = self.__username
        self.__storage_info["uid"] = self.__uid
        self.__jobs = []
        self.__projects_dict = {}
        self.__usage_dict = {}
        self.__files = {}
        self.__files["scratch"] = {}
        self.__files["projects"] = {}
        self.__files["home"] = {}

        # Create the connections to Slurm's acct db and LDAP
        self.SLURM_DB_CONNECTION = external_access.create_slurm_db_connection(
            SLURM_DB_HOST, SLURM_DB_PORT, SLURM_DB_USER, SLURM_DB_PASS, SLURM_ACCT_DB
        )
        self.LDAP_CONNECTION = external_access.create_ldap_connection(LDAP_HOST)

        self.__jobs = self.retrieve_job_map()
        self.__projects_dict = self.retrieve_user_projects()
        (
            self.__usage_dict,
            self.__files["projects"]["file_count"],
            self.__files["projects"]["percentage"],
        ) = self.get_projects_usage(self.__projects_dict)
        self.__usage_dict["unit"] = "B"

        for filesystem in ["scratch", "home"]:
            (
                self.__files[filesystem]["file_count"],
                self.__files[filesystem]["percentage"],
            ) = self.get_file_usage(filesystem)

    def __del__(self):
        # Close connections
        self.SLURM_DB_CONNECTION.close()
        self.LDAP_CONNECTION.close()

    def retrieve_user_projects(self):
        """
        Retrieves the user's projects

        Returns
        -------
        dictionnary
            dictionnary containing as key the projects' names and as values the projects' paths

        """
        projects = {}
        ldap_host = SLURM_DB_HOST.split(".")
        dc_string = "dc={},dc={},dc={},dc={}".format(
            ldap_host[1], ldap_host[2], ldap_host[3], ldap_host[4]
        )

        # To X-reference with found groups for `username`
        path = "/home/" + self.__username + "/projects/"

        # Find groups where `username` is a member (search returns list of dictionnaries)
        groups = self.LDAP_CONNECTION.search_s(
            dc_string, ldap.SCOPE_SUBTREE, "memberUid=" + self.__username, ["cn"],
        )[0][1]["cn"]

        groups = [g.decode("ascii") for g in groups]

        for project in os.listdir(path):
            # Fully qualified name of the project (/home/user/projects/def-X)
            fqn = path + project
            if project in groups and os.path.islink(fqn):
                projects[project] = fqn + "/" + self.__username
        return projects

    def get_projects_usage(self, paths):
        """
        Retrieves the file and storage usage of /projects
        
        Parameters
        ----------
        paths : array
            list of user's projects paths
            
        Returns
        -------
        Tuple
            (usage_dict, file_count)
            usage_dict : Dictionnary of all projects' disk usage in Bytes
            file_count : Total file count of all projects"""
        usage_dict = {}
        file_count = 0
        for project, path in paths.items():
            total_size = 0
            for dp, dn, fn in os.walk(path):
                file_count += len(fn) + 1  # +1 for directory which counts as a file
                for f in fn:
                    fp = os.path.join(dp, f)
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
            usage_dict[project] = total_size
            # usage_dict[project] = subprocess.check_output(["du", "-s", path]).rstrip().decode("ascii").split("\t")[0]
        return usage_dict, file_count, file_count / FILE_LIMITS["projects"]

    def get_file_usage(self, filesystem):
        """
        Retrieves the file usage of a given filesystem
        
        Parameters
        ----------
        filesystem : string
            filesystem to scrape
            
        Returns
        -------
        Tuple
            (file_count, percent) :
                file_count : File count for the file system
                percent : percentage of files used compared to limit imposed on the filesystem
        """
        if filesystem != "home":
            path = "/home/" + self.__username + "/" + filesystem
            is_home = False
        else:
            path = "/home/" + self.__username
            is_home = True
        file_count = 0
        percent = 0
        for dp, dn, filenames in os.walk(path):
            if is_home:
                if (
                    path + "/scratch" in dp
                    or path + "/projects" in dp
                    or path + "/nearline" in dp
                ):
                    continue
            file_count += len(filenames) + 1  # + 1 for directory which counts as a file
        percent = file_count / FILE_LIMITS[filesystem]
        return file_count, percent

    def retrieve_job_map(self):
        """
        Retrieves the mapping of jobs for the user

        Returns
        -------
        list
            the list of jobs that the user ran on the cluster
        """
        job_list = []
        sql_parameterized_query = (
            """SELECT id_job FROM user_job_view WHERE id_user = %s"""
        )

        with self.SLURM_DB_CONNECTION.cursor() as cursor:
            cursor.execute(sql_parameterized_query, (self.__uid,))
            result = cursor.fetchall()
            job_list = list(
                [element[0] for element in result]
            )  # Convert the tuple of single element tuples to a list of elements
        return job_list

    def get_info(self):
        """
        Retrieves the info for exposition to the REST API and returns JSON formatted dictionnary

        Returns:
        --------
        JSON-style dictionnary
            Contains all the user's data
        """
        output = {}
        output["user"] = self.__storage_info
        output["jobs"] = self.__jobs
        output["projects"] = self.__projects_dict
        output["project_usages"] = self.__usage_dict
        output["file_usages"] = self.__files
        return output

