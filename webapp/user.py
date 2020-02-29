import subprocess
import json
from pwd import getpwnam
from socket import gethostname
import external_access
import pymysql
from os import listdir, readlink
import ldap

#GLOBAL constants
# LOCALHOST = gethostname()
# LOCALHOST = LOCALHOST.split(".")[0]
SLURM_DB_HOST = "mgmt1.int." + external_access.get_domain_name()
SLURM_DB_USER = "petricore"
SLURM_DB_PASS = "yourPassword"
SLURM_DB_HOST = SLURM_DB_HOST.rstrip()
SLURM_DB_PORT = 3306
SLURM_ACCT_DB = "slurm_acct_db"
LDAP_HOST = "ldap://mgmt1"

# Create the connections to Slurm's acct db and LDAP
SLURM_DB_CONNECTION = external_access.create_slurm_db_connection(SLURM_DB_HOST, SLURM_DB_PORT, SLURM_DB_USER, SLURM_DB_PASS, SLURM_ACCT_DB)
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
        self.__projects = []

        # Retrieve the actual storage info
        # self.retrieve_storage_info()
        self.__jobs = self.retrieve_job_map(self.__uid)
        self.__projects = self.retrieve_user_projects(self.__username)

    def get_storage_info(self):
        """Get the self.__storage_info attribute"""
        return self.__storage_info

    def get_jobs(self):
        return self.__jobs

    # def retrieve_storage_info(self):
    #     """Function that retrieves storage data for the user, queries lfs quota on the cluster 
    #     (Except Graham, see retrieve_storage_info_graham())"""
    #     self.__storage_info["storage"] = []
    #     # Get /home and /scratch partition since they're based off of users.
    #     partitions = ("/home", "/scratch")

    #     for partition in partitions:

    #         output = subprocess.check_output(
    #             ["/usr/bin/lfs", "quota", "-u", self.__username, partition]
    #         ).decode("ascii")

    #         titles = output.split("\n")[1].split()
    #         data = output.split("\n")[2].split()
    #         json_dict = {}

    #         # Here, 8 covers all the fields we need to expose in the json
    #         for i in range(8):
    #             try:
    #                 actual_data = int(data[i])
    #             except:
    #                 actual_data = data[i]
    #                 pass

    #             if titles[i] == "quota":
    #                 titles[i] = "available_" + titles[i - 1]

    #             if titles[i] == "limit":
    #                 continue
    #             if data[i] != "-":
    #                 json_dict[titles[i].lower()] = actual_data

    #         self.__storage_info["storage"].append(json_dict)
    #     print(json.dumps(self.__storage_info))

    # def retrieve_storage_info_graham(self):
    #     # TODO
    #     self.__storage_info = []
    #     output = subprocess.check_output(
    #         "/cvmfs/soft.computecanada.ca/custom/bin/diskusage_report"
    #     ).decode("ascii")

    def retrieve_user_projects(self, username):
        projects = []

        #To X-reference with found groups for `username`
        path = "/home/" + self.__username + "/projects/"

        # Find groups where `username` is a member
        groups = LDAP_CONNECTION.search_s(
            "dc=int,dc=tango,dc=calculquebec,dc=cloud",
            ldap.SCOPE_SUBTREE,
            "memberUid=" + username,
            ["cn"],
        )

        for project in listdir(path):
            #Fully qualified name of the project (/home/user/projects/def-X)
            fqn = path + project
            if project in groups:
                try:
                #Verifies if dir with the name of a group is a symbolic link, since all project dirs are symlinks
                    readlink(fqn)
                    projects.append(fqn)
                except:
                    #Just skip it. If it's not a symlink, it's not a project directory.
                    pass
        return projects

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
        output["storage"] = self.__storage_info
        output["jobs"] = self.__jobs
        output["projects"] = self.__projects
        return output
