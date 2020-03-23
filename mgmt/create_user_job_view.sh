#! /bin/bash
#Must be run as root.
newUser='petricore'
newDbPassword=$(cat /opt/petricore_db/db_config)

host="$(hostname)" #Output of hostname command

IFS='.' # = is set as delimiter
read -ra ADDR <<< "$host" # str is read into an array as tokens separated by IFS
domain=${ADDR[2]} #Find the specific domain name (i.e. kappa from mgmt01.int.kappa.calculquebec.cloud)

loginhost="login1.int.${domain}.calculquebec.cloud"

commands="CREATE USER '${newUser}'@'${loginhost}' IDENTIFIED BY '${newDbPassword}';USE slurm_acct_db; CREATE VIEW user_job_view AS SELECT id_user, id_job, job_name FROM ${domain}_job_table; GRANT SELECT ON user_job_view TO '${newUser}'@'${loginhost}'; FLUSH PRIVILEGES;"

echo "${commands}" | /usr/bin/mysql
