#!/usr/bin/env python3

"""jobs_exporter.py: Scheduler jobs exporter daemon (for slurm, but can be modified to work with other schedulers I'm sure...) for Prometheus"""

__author__ = "Alexandre Larouche"

import os
import fnmatch
import re
import time
import socket
import argparse
import collections
from prometheus_client import (
    Gauge,
    start_http_server,
    CollectorRegistry,
    push_to_gateway,
    delete_from_gateway,
)
import signal
import sys
import psutil

# Global Constants
HOST = socket.gethostname().split(".")[0]  # Get node name
REGISTRY = CollectorRegistry()
BLACKLIST = []
CPUACCT_DIR = "/sys/fs/cgroup/cpuacct/slurm/"
CPUSET_DIR = "/sys/fs/cgroup/cpuset/slurm/"
REGEX = "task_*"


sp = Gauge(
    "jobs_spawned_processes",
    "Amount of spawned processes by job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
of = Gauge(
    "jobs_opened_files",
    "Amount of opened files by job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
tc = Gauge(
    "jobs_thread_count",
    "Amount of started thread by job",
    ["instance", "slurm_job", "proc_name"],
    registry=REGISTRY,
)
st = Gauge(
    "jobs_system_time",
    "Amount of time spent in system mode by job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
ut = Gauge(
    "jobs_user_time",
    "Amount of time spent in user mode by job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
us = Gauge(
    "jobs_uses_scratch",
    "Boolean value, tells if the job uses the scratch fs",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
cuc = Gauge(
    "jobs_cpu_time_core",
    "Amount of cpu time per cpu allocated to the job (s)",
    ["instance", "slurm_job", "core"],
    registry=REGISTRY,
)
cut = Gauge(
    "jobs_cpu_time_total",
    "Amount of cpu time total for the job (s)",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
read = Gauge(
    "jobs_read_mb",
    "Amount of bytes read by the job (MB)",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
write = Gauge(
    "jobs_write_mb",
    "Amount of bytes written by the job (MB)",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
read_count = Gauge(
    "jobs_read_count",
    "Amount of reads done by the job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
write_count = Gauge(
    "jobs_write_count",
    "Amount of writes done by the job",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
rss = Gauge(
    "jobs_rss",
    "Resident set size of job (MB)",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
cpu_percent = Gauge(
    "jobs_cpu_percent", "CPU usage of job", ["instance", "slurm_job"], registry=REGISTRY
)
cpu_percent_per_core = Gauge(
    "jobs_cpu_percent_per_core",
    "CPU usage per core of job on average",
    ["instance", "slurm_job"],
    registry=REGISTRY,
)
gpus_gauge = Gauge(
    "jobs_gpus_used",
    "Boolean values representing if the IDs of the gpus are used in the job",
    ["instance", "slurm_job", "gpuid"],
    registry=REGISTRY,
)

# Mappings for jobs
job_cpus_map = {}
job_proc_name_map_current = {}
# For comparing if a process in a job has terminated and therefore needs to be removed from the collectors.
job_proc_name_map_last = {}

# Handles SIGINT


def sigint_handler(sig, frame):
    delete_from_gateway("localhost:9091", job="jobs_exporter")
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


def load_blacklist(filename):
    """
    Loads a user list that the program cannot scrape during its process

    Parameters
    ----------
    filename : string
        name of the file containing the blacklist

    """
    global BLACKLIST
    with open(filename) as blacklist:
        BLACKLIST = blacklist.read().split("\n")


def remove_old_procs(cur_map, last_map, jobid):
    """Remove a PROCESS (not job) which was there in the last iteration but isn't appearing in the newest iteration of the scraping

    Parameters
    ----------
    cur_map : dictionnary
        job -> proc mapping for the current iteration
    last_map : dictionnary
        job -> proc mapping for the last iteration (just before cur_map)
    jobid : integer
        id of the job that is being verified
    """
    for proc in last_map:
        if proc not in cur_map:
            tc.remove(HOST, jobid, proc)


def remove_inactive_jobs_from_collectors(jobid):
    """Removes jobs that are done from the collectors so we don't keep pushing them

    Parameters
    ----------
    jobid: integer
        id of the job we want to delete from the collectors
    """
    sp.remove(HOST, jobid)
    of.remove(HOST, jobid)
    st.remove(HOST, jobid)
    ut.remove(HOST, jobid)
    us.remove(HOST, jobid)
    cut.remove(HOST, jobid)
    read.remove(HOST, jobid)
    write.remove(HOST, jobid)
    read_count.remove(HOST, jobid)
    write_count.remove(HOST, jobid)
    rss.remove(HOST, jobid)
    cpu_percent.remove(HOST, jobid)
    cpu_percent_per_core.remove(HOST, jobid)

    # Remove current processes from the collector for this job
    for proc_name in job_proc_name_map_current[jobid]:
        tc.remove(HOST, jobid, proc_name)

    # Remove allocated CPUs from the collector for this job
    for cpu in job_cpus_map[jobid]:
        cuc.remove(HOST, jobid, cpu)


def get_proc_data(pids, numcpus, jobid):
    """
    Retrieves processes data for a given job id

    Parameters
    ----------
    pids : array
        list of process ids that needs checking for data
    numcpus : integer
        number of cpus allocated to the job
    jobid : integer
        id of the job that possesses the processes
    """
    # Set jobs_uses_scratch to false here so in case no fildes links to scratch fs, it is already handled.
    us.labels(instance=HOST, slurm_job=jobid).set(0)

    # Aggregators for processes
    read_cnt = 0
    write_cnt = 0
    read_mbytes = 0
    write_mbytes = 0
    opened_files = set()
    res_set_size = 0
    cpu_usage = 0
    cpu_usage_per_core = 0  # On average
    proc_names = []
    gpus = set()

    for pid in pids:
        p = psutil.Process(pid)
        name = p.name()
        proc_names.append(name)
        env = p.environ()

        if "SLURM_JOB_GPUS" in env.keys():
            gpus.update(env["SLURM_JOB_GPUS"])
        # Request cpu percent out of the "oneshot" below so it's queried properly
        cpu_usage += p.cpu_percent(interval=0.1)

        cpu_usage_per_core += cpu_usage / numcpus

        with p.oneshot():
            # Get data from the process with psutil
            read_cnt += p.io_counters()[0]
            write_cnt += p.io_counters()[1]
            read_mbytes += p.io_counters()[2] / 1048576  # In MB
            write_mbytes += p.io_counters()[3] / 1048576  # In MB
            res_set_size += p.memory_info()[0] / 1048576  # In MB
            opened_files.update(p.open_files())
            threads = p.num_threads()

            tc.labels(instance=HOST, slurm_job=jobid,
                      proc_name=name).set(threads)

            # Looks for scratch usage in the opened files
            for file in opened_files:
                if re.search(".*scratch.*", file[0]):
                    us.labels(instance=HOST, slurm_job=jobid).set(
                        1
                    )  # Sets the state to true, the user is confirmed to be using scratch fs to write.
                    break

            # Remove already encountered pids as threads from the pid list
            for p in p.threads():
                pids.remove(p[0])

    # Keep the job -> proc_name map up to date. Useful to remove old jobs from the pushgateway.
    job_proc_name_map_current[jobid] = proc_names

    # Insures there has been at least one iteration before comparing if a process has terminated (in order not to crash the program)
    if jobid in job_proc_name_map_last:
        remove_old_procs(
            job_proc_name_map_current[jobid], job_proc_name_map_last[jobid], jobid
        )
    job_proc_name_map_last[jobid] = proc_names

    # Put data in collectors
    of.labels(instance=HOST, slurm_job=jobid).set(len(set(opened_files)))
    read.labels(instance=HOST, slurm_job=jobid).set(read_mbytes)
    write.labels(instance=HOST, slurm_job=jobid).set(write_mbytes)
    read_count.labels(instance=HOST, slurm_job=jobid).set(read_cnt)
    write_count.labels(instance=HOST, slurm_job=jobid).set(write_cnt)
    rss.labels(instance=HOST, slurm_job=jobid).set(res_set_size)
    cpu_percent_per_core.labels(
        instance=HOST, slurm_job=jobid).set(cpu_usage_per_core)
    cpu_percent.labels(instance=HOST, slurm_job=jobid).set(cpu_usage)

    for gpu in gpus:
        gpus_gauge.labels(instance=HOST, slurm_job=jobid, gpuid=gpu).set(1)


def retrieve_file_data(job, jobid, user, dirname):
    """Retrieves the data stored in different files within the cgroups. If tasks
    are found, retrieve the process data (cpu%, cputime, R/W counts,...) associated
    with said tasks.

    Parameters
    ----------
    job: string 
        job name to find cgroup
    jobid: integer 
        jobid for collector labels
    user: string 
        username to find the right cgroup
    dirname: string 
        full name of the path to find tasks

    """
    # Declarations
    tasks = []
    times = []
    cpus = []

    # Semi-static paths which change depending on user and job. Control groups.
    cpuset_path = CPUSET_DIR + user + "/" + job + "/cpuset.cpus"
    usage_percpu_path = CPUACCT_DIR + user + "/" + job + "/cpuacct.usage_percpu"
    usage_total_path = CPUACCT_DIR + user + "/" + job + "/cpuacct.usage"
    stat_path = CPUACCT_DIR + user + "/" + job + "/cpuacct.stat"
    task_path = dirname + "/tasks"

    # Look for which CPUs got allocated to this job
    if os.path.isfile(cpuset_path):
        with open(cpuset_path) as cpuset_file:
            data = cpuset_file.readline().rstrip().split(",")
            for alloc in data:
                if "-" in alloc:
                    alloc = alloc.split("-")
                    for i in range(int(alloc[0]), int(alloc[1]) + 1):
                        cpus.append(i)
                else:
                    cpus.append(int(alloc))

    # Keep the job -> cpus map up to date with what was found. Useful to remove terminated jobs from the pushgateway
    job_cpus_map[jobid] = cpus

    # Cross-reference the allocated CPUs with their individual usages in nanoseconds
    if os.path.isfile(usage_percpu_path):
        with open(usage_percpu_path) as cpuacct_file:
            data = cpuacct_file.readline().rstrip().split(" ")
            for cpu in cpus:
                usage = (
                    int(data[cpu]) / 10 ** 9
                )  # Divide the usage by 10 ** 9 because it's in nanoseconds (to send them to Prometheus is seconds).
                cuc.labels(instance=HOST, slurm_job=jobid, core=cpu).set(usage)

    # Gets total cpu time spent for this job. Will be used to compare loads on each cpu to the total time spent (load balancing)
    if os.path.isfile(usage_total_path):
        with open(usage_total_path) as cpuacct_file:
            usage = (
                int(cpuacct_file.readline().rstrip()) / 10 ** 9
            )  # Divide usage by 10**9 because it's in nanoseconds (to send them to Prometheus in seconds).
            cut.labels(instance=HOST, slurm_job=jobid).set(usage)

    # Try to open the file
    if os.path.isfile(task_path):
        with open(task_path) as task_file:
            # Add all the pids to the list 'tasks'
            tasks = task_file.read().rstrip().split("\n")
            if "" in tasks:
                tasks.remove(
                    ""
                )  # In order to prevent a crash if it reads the task file but it's empty

    # Get user and system times from the stat file in the cpuacct cgroup for the job
    if os.path.isfile(stat_path):
        with open(stat_path) as stat_file:
            for line in stat_file:
                times.append(
                    line.rstrip().split()[1]
                )  # Could change for a for i in range() and remove the append...

    # Put data in collectors
    ut.labels(instance=HOST, slurm_job=jobid).set(times[0])
    st.labels(instance=HOST, slurm_job=jobid).set(times[1])
    sp.labels(instance=HOST, slurm_job=jobid).set(len(tasks))

    # Get process-specific data with psutil if tasks list isn't empty
    if tasks:
        tasks = list(map(int, tasks))
        get_proc_data(tasks, len(cpus), jobid)


def retrieve_and_expose(timer):
    """
    Loop that retrieves and exposes the scraped data

    Parameters
    ----------
    timer : integer
        number of seconds to wait before next loop iteration
    """
    # Prevents from sending delete to the pushgateway if no job was pushed two times in a row (i.e. all the jobs are done and accounted for for now)
    iter_empty = 0
    last_iter_found = []
    while True:
        # List for found jobs
        found = []
        empty = True

        # These `for loops` count the number of spawned processes by a task.
        for path, dirs, files in os.walk(CPUACCT_DIR):
            # Look for the REGEX (global constant, change this for other schedulers perhaps...), default is "task_*"
            for f in fnmatch.filter(dirs, REGEX):
                empty = False
                # Find full name of the path
                fullname = os.path.abspath(os.path.join(path, f))
                # Find the job ID
                job = re.search("(job_)[0-9]+", fullname).group(
                    0
                )  # Used for file names
                # Used to push metrics at the right job
                jobid = job.split("_")[1]
                user = re.search("(uid_)[0-9]+", fullname).group(
                    0
                )  # Used for file names
                uid = user.split("_")[1]

                # Avoid finding the same job twice in the same iteration and skips blacklist users.
                if jobid not in found and uid not in BLACKLIST:
                    found.append(jobid)
                    retrieve_file_data(job, jobid, user, fullname)

        if empty:
            iter_empty += 1
        else:
            iter_empty = 0

        # Find the list of newly terminated jobs
        diff = list(set(last_iter_found) - set(found))
        for jobid in diff:
            remove_inactive_jobs_from_collectors(jobid)

        if not empty:
            # Send data to the pushgateway
            push_to_gateway("localhost:9091",
                            job="jobs_exporter", registry=REGISTRY)

        # Wait the set amount of time before re-retrieving and exposing the next set of data.
        time.sleep(timer)

        # Delete from Pushgateway, else it creates flat lines for jobs that don't exist anymore.
        if iter_empty <= 1:
            delete_from_gateway("localhost:9091", job="jobs_exporter")

        last_iter_found = found.copy()


if __name__ == "__main__":
    # Retrieve args passed to the program.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--timer",
        help="Set the timer in seconds on the exposition to Prometheus, by default it is set to 15s",
        type=int,
    )
    parser.add_argument(
        "-b",
        "--blacklist",
        help="Set the path (fully qualified name) of the blacklist to load, by default, no blacklist is loaded",
        type=str,
    )
    args = parser.parse_args()

    # Load blacklist
    if args.blacklist:
        print("[+] Loading blacklist" + args.blacklist + " [+]")
        load_blacklist(args.blacklist)
    else:
        print("[+] No blacklist specified, no blacklist loaded [+]")

    # Retrieve and expose data to Prometheus
    if args.timer:
        print(
            "[+] Started the exporter with an interval of " +
            str(args.timer) + "s [+]"
        )
        try:
            retrieve_and_expose(args.timer)
        except Exception as e:
            print("[-] Program crashed, printing caught exception... [-]")
            print(str(e))
        finally:
            delete_from_gateway("localhost:9091", job="jobs_exporter")
    else:
        print("[+] Started the exporter with an interval of 15s [+]")
        try:
            retrieve_and_expose(15)
        except Exception as e:
            print("[-] Program crashed, printing caught exception... [-]")
            print(str(e))
        finally:
            delete_from_gateway("localhost:9091", job="jobs_exporter")
