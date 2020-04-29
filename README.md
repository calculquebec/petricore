# Petricore
## jobs_exporter
A Slurm jobs metrics (using cgroups and /proc via cross-reference) data exporter for Prometheus that should be easily adaptable to other schedulers.

## What does it export ?
- CPU Usage (%) of job (jobs_cpu_percent)
- CPU Usage (%) per core of job (jobs_cpu_percent_per_core)
- Resident set size of job in megabytes (jobs_rss)
- Number of spawned processes by the job (jobs_spawned_processes)
- Number of opened file by the job (jobs_opened_files)
- Number of threads per processes spawned by the job (jobs_thread_count)
- Amount of time spent in user mode (jobs_user_time)
- Amount of time spent in system mode (jobs_system_time)
- Amount of cpu time spent in total (jobs_cpu_time_total)
- Amount of cpu time spent per cpu (jobs_cpu_time_core)
- If the job used the scratch filesystem. (jobs_uses_scratch)
- Amount of megabytes read (jobs_read_mb)
- Amount of megabytes written (jobs_write_mb)
- Write count (jobs_write_count)
- Read count (jobs_read_count)
- GPUs used (jobs_gpus_used)

Every class of exported data starts with the prefix `jobs_` to be easily recognizable as part of this exporter.

## Requirements

## jobs_exporter
### Dependencies
#### Python
- psutil
- prometheus_client

#### System
- libcgroup-tools
- Use the jobacct_gather/cgroup plugin for Slurm (can be added in slurm.conf as JobAcctGatherType=jobacct_gather/cgroup)
- Prometheus pushgateway up and running

If it is your first time using cgroups with Slurm, you might want to consider adding these lines to the slurm epilog on the management node : 

```
#Clears the job cgroup in case of cancel
cgdelete -r cpuacct:/slurm/uid_$SLURM_JOB_UID/job_$SLURM_JOBID
cgdelete -r memory:/slurm/uid_$SLURM_JOB_UID/job_$SLURM_JOBID

#Clears the uid cgroup in case it is empty after the scancel. If not empty, then the active cgroups should remain intact.
cgdelete  cpuacct:/slurm/uid_$SLURM_JOB_UID
cgdelete  memory:/slurm/uid_$SLURM_JOB_UID
```
These lines are used in order to not leave any trailing cgroups on unsuccessful jobs (CANCEL, TIMEOUT, ...)


## webapp
### Dependencies
#### Python
- flask
- requests
- pylatex
- matplotlib
- pymysql
- python-ldap

#### System
- mysql-devel
- texlive
- texlive-lastpage

## How it works
### jobs_exporter
![alt text](https://docs.google.com/drawings/d/e/2PACX-1vSOLM2Q9AZYmsRYqsevTvpWUysPeAhbdIre1CnQ-ti78A6XBHMxWXbJhZLqp7bg7RAEwhHoROTnqX0S/pub?w=1315&h=704 "Jobs Exporter Diagram")

jobs_exporter runs as a daemon in the background on the compute nodes and expose metrics to the Prometheus pushgateway exposed on every node in order to be scraped by Prometheus.

All you have to do in order for this daemon to work as expected is to have a Prometheus pushgateway on the node getting scraped and configure Prometheus to scrape the pushgateway on the node on which you are running the daemon (On Magic_Castle, this is handled via Consul's autodiscovery feature) and everything should work!

### Cgroups data
- Time spent in user mode
- Time spent in system mode
- Number of processes spawned
- Amount of CPU time spent per core for one job
- Amount of CPU time spent total for one job

### psutil or /proc data
- Number of threads
- Number of opened files
- Read count
- Write count
- Read amount (MB)
- Write amount (MB)
- RSS (Resident Set Size)
- CPU usage per core on average
- CPU usage total for the job (May be over 100% if multiple CPUs are used)
- Job uses the Scratch filesystem
- GPUs used (via environ)

As a reminder, even though the gathering of the data is split in two different areas, cgroups are still required since they contain a mapping for a job and its process IDs.


### Web App
![alt text](https://docs.google.com/drawings/d/e/2PACX-1vRgZzeBaogtesA9l_xBIsGIpIaiCBhWDK-T8EDSs72Kp9HEpKcYPwR01ENmOnSGvugmN_4_DQ9Fdo5S/pub?w=1315&h=704 "Web app Diagram")

The web app uses the Prometheus REST API in order to retrieve data from the database. It also parses the output of `sacct` locally in order to retrieve additionnal data to show the users. It proceeds to compute and verify if there were some problematic behaviors associated with the job and outputs those warnings (if there are any) in the email (for now, future work may involve more ways of exposing data to the user).

It has multiple endpoints, all accessible via HTTP GET - 
- `/api/v1/users/<username>` : Source of truth for a user
- `/api/v1/jobs/<jobid>/usage` : Source of truth for a job
- `/pdf/<jobid>` : Makes a pdf with various plots and pie charts to visualize the usage of ressources
- `/pie/<jobid>/` : Makes pie charts for a jobid on metrics {"jobs_system_time", "jobs_user_time"} (one pie, 2 components)
- `/plot/<jobid>/<metric>` : Makes a plot for a given job and metric
- `/mail/<jobid>` : Retrieves the content of the email that would be sent to a user after the job is completed
- `/` : Shows examples of paths that can be used and their purpose. (Hostname is not up-to-date)

The web app also connects to Slurm's accounting database in order to retrieve a mapping of users and jobs (user:[jobs]).

### mgmt
mgmt contains a script which creates the petricore user in the mysql / mariadb database. It also makes a restricted view of the user->job mapping on Slurm's accounting database. Petricore only has access the SELECT on this view.

## Why do you do this the way you do ?
I use the Cgroups in order to find out which processes were started by the Slurm job. In doing so, I can then access additionnal data with what is stored in /proc for the pids shown in the cgroup. This was seemingly the only way I could access the number of files / file descriptors opened by a process at any given time. At the same time, the cgroups give information as to how many processes were spawned by one job.

There is probably a way to do this without using the pushgateway, however, at the time it seemed like the most logical thing to use since I needed to delete data frequently off of the scraping endpoint for Prometheus.

## What's the point ?
Having metrics will help the CC and their regional partners to steer users away from bad behaviors. This serves as a tool to shed light on poor resource usage which
was veiled by a lack of tools like this one in the past.


