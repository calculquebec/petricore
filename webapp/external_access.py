import pymysql.cursors
import ldap

def get_domain_name():
    """Returns the domain name of the current configuration from a config file"""
    with open("/var/www/logic_webapp/webapp_config") as file:
        line = file.readline()
        domain = line.split("=")[1].rstrip()  # Take right hand side of = and remove \n
        return domain


def create_slurm_db_connection(host, port, user, password, db):
    """Creates the connection to the database (MySQL) so we can query it"""
    # password = ""
    # with open(".password") as file:
    #     password = file.readline

    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,  # TODO Change to an obfuscated file or something
        db=db
    )
    print("[+] Slurm accounting DB connection is up! [+]")
    return connection

def create_ldap_connection(host):
    connect = ldap.initialize(host)
    connect.set_option(ldap.OPT_REFERRALS, 0)
    connect.simple_bind_s()
    return connect