import pymysql.cursors

# SLURM_DB_HOST_IP = "192.168.159.15"


def get_domain_name():
    """Returns the domain name of the current configuration from a config file"""
    with open("/var/www/logic_webapp/webapp_config") as file:
        line = file.readline()
        domain = line.split("=")[1].rstrip()  # Take right hand side of = and remove \n
        return domain


SLURM_DB_HOST = "mgmt1.int." + get_domain_name()
SLURM_DB_HOST = SLURM_DB_HOST.rstrip()


def create_db_connection():
    """Creates the connection to the database (MySQL) so we can query it"""
    # password = ""
    # with open(".password") as file:
    #     password = file.readline

    connection = pymysql.connect(
        host=SLURM_DB_HOST,
        port=3306,
        user="petricore",
        password="yourPassword",  # TODO Change to an obfuscated file or something
        db="slurm_acct_db",
    )
    print("[+] DB connection is up! [+]")
    return connection
