import subprocess
import json
from pwd import getpwnam
from socket import gethostname
import external_access
import pymysql
import os
import ldap

# GLOBAL constants
SLURM_DB_HOST = "mgmt1.int." + external_access.get_domain_name()
SLURM_DB_USER = "petricore"
SLURM_DB_PASS = external_access.get_db_password()
SLURM_DB_HOST = SLURM_DB_HOST.rstrip()
SLURM_DB_PORT = 3306
SLURM_ACCT_DB = "slurm_acct_db"
LDAP_HOST = "ldap://mgmt1"
FILE_LIMIT = 500000

# Create the connections to Slurm's acct db and LDAP
SLURM_DB_CONNECTION = external_access.create_slurm_db_connection(
    SLURM_DB_HOST, SLURM_DB_PORT, SLURM_DB_USER, SLURM_DB_PASS, SLURM_ACCT_DB
)
LDAP_CONNECTION = external_access.create_ldap_connection(LDAP_HOST)


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
        self.__scratch_files = {}

        self.__jobs = self.retrieve_job_map(self.__uid)
        self.__projects_dict = self.retrieve_user_projects(self.__username)
        self.__usage_dict = self.get_disk_usage(self.__projects_dict)
        self.__usage_dict["unit"] = "B"
        (
            self.__scratch_files["file_count"],
            self.__scratch_files["percentage"],
        ) = self.get_scratch_file_usage(self.__username)

    def get_storage_info(self):
        """Get the self.__storage_info attribute"""
        return self.__storage_info

    def get_jobs(self):
        return self.__jobs

    def retrieve_user_projects(self, username):
        projects = {}
        ldap_host = SLURM_DB_HOST.split(".")
        dc_string = "dc={},dc={},dc={},dc={}".format(
            ldap_host[1], ldap_host[2], ldap_host[3], ldap_host[4]
        )

        # To X-reference with found groups for `username`
        path = "/home/" + self.__username + "/projects/"

        # Find groups where `username` is a member (search returns list of dictionnaries)
        groups = LDAP_CONNECTION.search_s(
            dc_string, ldap.SCOPE_SUBTREE, "memberUid=" + username, ["cn"],
        )[0][1]["cn"]

        groups = [g.decode("ascii") for g in groups]

        for project in os.listdir(path):
            # Fully qualified name of the project (/home/user/projects/def-X)
            fqn = path + project
            if project in groups and os.path.islink(fqn):
                projects[project] = fqn + "/" + username
        return projects

    def get_disk_usage(self, paths):
        usage_dict = {}
        for project, path in paths.items():
            total_size = 0
            for dp, dn, fn in os.walk(path):
                for f in fn:
                    print(f)
                    fp = os.path.join(dp, f)
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
            usage_dict[project] = total_size
            # usage_dict[project] = subprocess.check_output(["du", "-s", path]).rstrip().decode("ascii").split("\t")[0]
        return usage_dict

    def get_scratch_file_usage(self, username):
        scratch_path = "/home/" + username + "/scratch/"
        file_count = 0
        percent = 0
        for dp, dn, filenames in os.walk(scratch_path):
            file_count += len(filenames) + 1  # + 1 for directory which counts as a file
        percent = file_count / FILE_LIMIT
        return file_count, percent

    def retrieve_job_map(self, id_user):
        job_list = []
        sql_parameterized_query = (
            """SELECT id_job FROM user_job_view WHERE id_user = %s"""
        )

        with SLURM_DB_CONNECTION.cursor() as cursor:
            cursor.execute(sql_parameterized_query, (id_user,))
            result = cursor.fetchall()
            job_list = list(
                [element[0] for element in result]
            )  # Convert the tuple of single element tuples to a list of elements
        return job_list

    def get_info(self):
        output = {}
        output["user"] = self.__storage_info
        output["jobs"] = self.__jobs
        output["projects"] = self.__projects_dict
        output["project_usages"] = self.__usage_dict
        output["scratch_file_usage"] = self.__scratch_files
        return output

