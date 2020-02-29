import subprocess
import os
import argparse
import ldap

# connect = ldap.initialize("ldap://192.168.126.20")
# connect.set_option(ldap.OPT_REFERRALS, 0)
# connect.simple_bind_s()
# result = connect.search_s('dc=int,dc=tango,dc=calculquebec,dc=cloud', ldap.SCOPE_SUBTREE, 'cn=def-sponsor00', ['memberUid']) -> trouve les utilisateurs a partir du groupe
# result = connect.search_s('dc=int,dc=tango,dc=calculquebec,dc=cloud', ldap.SCOPE_SUBTREE, 'memberUid=user01', ['cn']) -> trouve le groupe a partir de l'utilisateur
# print(result)


MAX_CAP = 500000  # Max capacity of amount of files in scratch for

# Structure dirs -> /home/[user]/dirs/[nomProjet]/[tous les utilisateurs]
# Ou bien /project/[nomProjet ou numeroProjet]

# Structure nearline -> /home/[user]/nearline/def-jfaure/[tous les utilisateurs]

# Structure scratch -> /home/[user]/scratch/[tous les FICHIERS]
# Ou bien /scratch/[user] ou bien encore /lustre[0X]/scratch/[user]


def create_connection_object(host):
    connect = ldap.initialize(host)
    connect.set_option(ldap.OPT_REFERRALS, 0)
    connect.simple_bind_s()
    return connect


def get_disk_usage(path):
    """Retrieves disk usage of directories on a given path (given as argument to the program)"""
    result_list = []  # list which contains the result of the parsing, line by line.

    # Find the amount of megabytes the folder contains
    for f in os.listdir(path):
        dir_path = path + "/" + f
        result_list.append(
            subprocess.check_output(["du", "-s", dir_path]).rstrip().decode("ascii")
        )

    # Outputs result
    for i in range(len(result_list)):
        print(result_list[i])


def get_file_usage(path):
    """Retrieves the file usage on a given path (e.g. number of files out of the limit on scratch). Shows number of files as well as percentage of quota"""
    result_list = []  # list which contains the result of the parsing, line by line.
    # Find number of files in a folder
    for f in os.listdir(path):
        dir_path = path + f
        cmd = "ls -1 " + dir_path + " | wc -result_list"
        ps = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        output = ps.communicate()[0]
        result_list.append(output.rstrip().decode("ascii"))

    # Outputs results
    for i in range(len(result_list)):
        chunk = 100 * (int(result_list[i]) / MAX_CAP)
        print(str(chunk) + "% (" + result_list[i] + " files)")


def get_FS(filesystem, user, connect):
    projects = []
    path = "/home/" + user + "/" + filesystem + "/"

    groups = connect.search_s(
        "dc=int,dc=tango,dc=calculquebec,dc=cloud",
        ldap.SCOPE_SUBTREE,
        "memberUid=" + user,
        ["cn"],
    )  # -> trouve le groupe a partir de l'utilisateur

    # print(groups)
    dirs = os.listdir(path)
    for p in dirs:
        # Fully qualified name -> fqn
        fqn = path + p

        # check if dir is a symlink since we know projects are all symlinks to /project or /lustre04/project
        if p in groups:
            try:
                os.readlink(fqn)
                projects.append(fqn)

            except:
                pass
    return projects


def get_stats(filesystem):
    for fs in filesystem:
        get_disk_usage(fs)


def create_argument_parser():
    parser = argparse.ArgumentParser()

    # required=True
    parser.add_argument(
        "-o",
        "--operation",
        choices=["scratch_files", "disk"],
        help="Set operation to do",
    )
    parser.add_argument(
        "-u", "--user", help="Set user to lookup",
    )

    parser.add_argument(
        "-h", "--host", help="Set LDAP host",
    )

    # add choices - [scratch, project, nearline]
    parser.add_argument("-f", "--filesystem", help="Set filesystem to lookup")

    return parser


if __name__ == "__main__":
    print("=======================================================================")
    parser = create_argument_parser()
    args = parser.parse_args()
    connect = create_connection_object(args.host)
    fs = get_FS(args.filesystem, args.user, connect)
    get_stats(fs)
