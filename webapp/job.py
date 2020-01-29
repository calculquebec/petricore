import subprocess
import datetime
import requests
import json
import os
import matplotlib.pyplot as plt
from pylatex import Document, Section, Figure, NoEscape, NewPage, Command
from user import User
from socket import gethostname
import db_access

CWD = "/var/www/logic_webapp/"
PROM_HOST = "http://mgmt1.int." + db_access.get_domain_name() + ":9090"
API_URL = PROM_HOST + "/api/v1/query"
LOCALHOST = gethostname().split(".")[0]  # charlie, sigma, ... [name].calculquebec.cloud
# LOCALHOST = LOCALHOST.split(".")[0]
SACCT = "/opt/software/slurm/bin/sacct"
FORMAT = "--format=Account,User,Start,End,AllocCPUs,AllocTres,NodeList,Elapsed"

Y_LABELS = {
    "jobs_rss": "Resident set size (MB)",
    "jobs_cpu_percent": "CPU Usage (%)",
    "jobs_read_mb": "Amount of megabytes read (MB)",
    "jobs_write_mb": "Amount of megabytes written (MB)",
    "jobs_user_time": "Time in user mode",
    "jobs_system_time": "Time in system mode",
    "jobs_cpu_time_core": "CPU Time",
}


class Job:
    def __init__(self, jobid):
        # Initialize all variables
        self.__jobid = jobid
        self.__sponsor = ""
        self.__username = ""
        self.__start_time = 0
        self.__end_time = 0
        self.__alloc_cpu = 0
        self.__alloc_tres = 0
        self.__nodes = 0
        self.__step = 0
        self.__runtime = 0
        self.__alloc_mem = 0
        self.__billing = 0
        self.__results = []
        self.__threads = {}
        self.__warning = False
        self.__out_string = ""
        self.__uses_scratch = False
        self.__cpu_time_core = {}
        self.__cpu_time_total = 0
        self.__read_mb = 0
        self.__write_mb = 0
        self.__read_count = 0
        self.__write_count = 0
        self.__opened_files = 0
        self.__max_cpu_usage = 0
        self.__avg_cpu_usage = 0
        self.__max_rss = 0
        self.__count_used_cpus = 0

        # Retrieve actual data
        self.get_sacct_data()
        self.pull_prometheus()
        self.get_num_used_cpus(80)
        # self.__user = User(username)

    def get_num_used_cpus(self, treshold):
        """Gets the number of cpus effectively used by the job by measuring cpu time for each\n
        Args:
        - self
        - treshold: lowest acceptable percentage of cpu time used in percentage
        """
        # Time shares
        max_time_per_core = self.__cpu_time_total / self.__alloc_cpu
        treshold_time_per_core = max_time_per_core * (treshold / 100)

        for usage in self.__cpu_time_core.values():
            if usage >= treshold_time_per_core:
                self.__count_used_cpus += 1

    def get_sacct_data(self):
        """
        Parses the output of sacct for the job and fills the associated object attributes
        """
        out = subprocess.check_output(
            [SACCT, "--units=M", "-X", "-n", "-p", FORMAT, "-j", str(self.__jobid)]
        )

        out = out.decode("ascii")
        out = out.split("\n")
        out = out[0]
        out = out.split("|")

        self.__sponsor = out[0]
        self.__username = out[1]

        self.__start_time = int(
            subprocess.check_output(["/usr/bin/date", "+%s", "-d", out[2]])
            .decode()
            .rstrip()
        )

        self.__end_time = int(
            subprocess.check_output(["/usr/bin/date", "+%s", "-d", out[3]])
            .decode()
            .rstrip()
        )

        self.__alloc_cpu = int(out[4])
        self.__alloc_tres = out[5]
        self.__nodes = out[6]
        self.__runtime = out[7]
        self.__step = self.__end_time - self.__start_time

        self.__billing = self.__alloc_tres.split(",")[0]
        self.__billing = int(self.__billing.split("=")[1])

        self.__alloc_mem = self.__alloc_tres.split(",")[2]
        self.__alloc_mem = self.__alloc_mem.split("=")[1]

    def pull_prometheus(self):
        """
        Pull data from Prometheus HTTP API with a hardcoded list of metrics and fills the associated object attributes
        """

        metrics = ("jobs_cpu_percent", "jobs_rss", "jobs_opened_files")
        modifiers = [("avg", "max"), ("max",), ("avg",)]
        # Request the METRICS array since they all have the same form (sum max and avg) over the length of the job and are series OVER TIME
        for i in range(len(metrics)):
            for modifier in modifiers[i]:
                tmp_list = []
                query_string = (
                    modifier
                    + "_over_time("
                    + metrics[i]
                    + '{slurm_job="'
                    + str(self.__jobid)
                    + '"}['
                    + str(self.__step)
                    + "s])"
                )

                params = {"query": query_string, "time": self.__end_time}
                response = requests.get(API_URL, params=params)
                # print(API_URL)
                # print(params)
                json = response.json()["data"]["result"]
                # print(json)
                for item in json:
                    tmp_list.append(float(item["value"][1]))
                self.__results.append(tmp_list)

        self.__avg_cpu_usage = self.__results[0]
        self.__max_cpu_usage = self.__results[1]
        self.__max_rss = self.__results[2]
        self.__opened_files = int(sum(self.__results[3]))

        # I/O Data
        params = {
            "query": 'jobs_uses_scratch{slurm_job="' + str(self.__jobid) + '"}',
            "time": self.__end_time,
        }
        response = requests.get(API_URL, params=params)

        for item in response.json()["data"]["result"]:
            if item["value"][1] == "1":
                self.__uses_scratch = True
                break

        metrics = (
            "jobs_read_mb",
            "jobs_write_mb",
            "jobs_read_count",
            "jobs_write_count",
        )

        for metric in metrics:
            params = {
                "query": metric + '{slurm_job="' + str(self.__jobid) + '"}',
                "time": self.__end_time,
            }
            response = requests.get(API_URL, params=params)
            json = response.json()["data"]["result"]

            for item in json:
                if metric == "jobs_read_mb":
                    self.__read_mb += float(item["value"][1])
                elif metric == "jobs_read_count":
                    self.__read_count += float(item["value"][1])
                elif metric == "jobs_write_count":
                    self.__write_count += float(item["value"][1])
                else:
                    self.__write_mb += float(item["value"][1])

        # Threads counts (i.e. How many threads did you spawn ?)
        params = {
            "query": 'max_over_time(jobs_thread_count{slurm_job="'
            + str(self.__jobid)
            + '"}['
            + str(self.__step)
            + "s])",
            "time": self.__end_time,
        }
        response = requests.get(API_URL, params=params)
        json = response.json()["data"]["result"]

        # Iterate through each process and collect their threads
        # Use a dict because the structure is more suitable
        # than a list (self.__results[])
        for item in json:
            self.__threads[item["metric"]["proc_name"]] = int(
                item["value"][1]
            )  # Multiple nodes coud be a PROBLEM, overwriting existing data ?

        # CPU Times
        params = {
            "query": 'jobs_cpu_time_core{slurm_job="' + str(self.__jobid) + '"}',
            "time": self.__end_time,
        }

        response = requests.get(API_URL, params=params)
        json = response.json()["data"]["result"]
        # print(json)

        for item in json:
            self.__cpu_time_core[
                item["metric"]["instance"] + "_core_" + item["metric"]["core"]
            ] = float(item["value"][1])

        self.__cpu_time_core = self.__cpu_time_core

        params = {
            "query": 'jobs_cpu_time_total{slurm_job="' + str(self.__jobid) + '"}',
            "time": self.__end_time,
        }

        response = requests.get(API_URL, params=params)
        for item in response.json()["data"]["result"]:
            self.__cpu_time_total += float(item["value"][1])

    def verify_data(self):
        """
        Verifies if CPU Util, core usage, jobs_rss, threads, I/O are withing CC's acceptable usage boundaries
        """
        # Declarations
        usage_avg_per_cpu = sum(self.__avg_cpu_usage) / self.__alloc_cpu
        usage_max_per_cpu = (
            sum(self.__max_cpu_usage) / self.__alloc_cpu
        )  # Assuming balanced workload
        usage_ratio = usage_avg_per_cpu / usage_max_per_cpu
        usage_rss = (sum(self.__max_rss) / int(self.__alloc_mem[:-1])) * 100
        expected_time_usage_core = 0.9 * (
            self.__cpu_time_total / len(self.__cpu_time_core.keys())
        )  # To check for good parallelization (assumes we want to stay withing 10% of perfect load balancing) (Lower threshold)
        core_warning = False
        total_io_per_file = (
            self.__read_mb + self.__write_mb
        ) / self.__opened_files  # 1048576 if to change bytes into MB

        # To add the URL to the pdf right before the last line (which is WARNING=boolean)
        PDF_URL = (
            "petricore." + LOCALHOST + ".calculquebec.cloud/pdf/" + str(self.__jobid)
        )

        self.__out_string = self.__out_string + "----------I/O Data----------\n"
        # IF not using Scratch
        if not self.__uses_scratch:
            self.__warning = True
            self.__out_string = (
                self.__out_string
                + "You should consider using the Scratch fs for your I/O! -- WARNING Related resource: https://docs.computecanada.ca/wiki/Storage_and_file_management/en\n"
            )
        else:
            self.__out_string = (
                self.__out_string + "Congratulations, you've used the Scratch fs!\n"
            )

        if total_io_per_file < 5 and self.__opened_files >= 1000:
            self.__warning = True
            self.__out_string = (
                self.__out_string
                + "You seem to writing very little to a lot of files (R/W per file: "
                + str(total_io_per_file)
                + ", number of opened file descriptors: "
                + str(self.__opened_files)
                + ") -- WARNING Related resource: https://en.wikipedia.org/wiki/Asynchronous_I/O\n"
            )
        else:
            self.__out_string = (
                self.__out_string + "Congratulations, your I/O throughput is great!\n"
            )

        self.__out_string = self.__out_string + "----------Usage Data----------\n"

        # Verify every potential bad behavior in order to flag them and put up a warning with a possible resources to fix the problem
        if usage_avg_per_cpu < 80:
            self.__warning = True
            self.__out_string = (
                self.__out_string
                + "Your average CPU Utilization is: "
                + str(usage_avg_per_cpu)
                + "% -- WARNING (high idle time) Related resource: https://en.wikipedia.org/wiki/CPU_time\n"
            )
        else:
            self.__out_string = (
                self.__out_string
                + "Your average CPU Utilization per core is: "
                + str(usage_avg_per_cpu)
                + "%\n"
            )

        if usage_ratio < 0.75:
            self.__warning = True
            self.__out_string = (
                self.__out_string
                + "Your CPU Utilization ratio (average / maximum) per core is: "
                + str(usage_ratio)
                + " -- WARNING\n"
            )
        else:
            self.__out_string = (
                self.__out_string
                + "Your CPU Utilization ratio (average / maximum) per core is: "
                + str(usage_ratio)
                + "\n"
            )

        if usage_rss < 0.95:
            self.__warning = True
            self.__out_string = (
                self.__out_string
                + "Your RAM usage ratio (average/allocated) per core is: "
                + str(usage_rss)
                + "% -- WARNING (Aim for 90%)\n"
            )
        else:
            self.__out_string = (
                self.__out_string
                + "Your RAM usage ratio (average/allocated) per core is: "
                + str(usage_rss)
                + "%\n"
            )

        # Checks for CPU usage per core and compares it with the lower bound of what is acceptable
        for usage in self.__cpu_time_core.values():
            if usage < expected_time_usage_core and not core_warning:
                self.__warning = True
                self.__out_string = (
                    self.__out_string
                    + "WARNING -- Some of your cores are under utilized! -- WARNING Related resource: https://en.wikipedia.org/wiki/Parallel_programming_model\n"
                )
                core_warning = True
                break
        if not core_warning:
            self.__out_string = (
                self.__out_string
                + "Congratulations, you utilized all of your cores properly!\n"
            )

        self.__out_string = self.__out_string + "----------Thread Data----------\n"

        for proc in self.__threads.keys():
            if self.__threads[proc] > (2 * self.__alloc_cpu):
                self.__warning = True
                self.__out_string = (
                    self.__out_string
                    + proc
                    + ": "
                    + str(self.__threads[proc])
                    + " thread(s) -- WARNING Related resource: https://www.jstorimer.com/blogs/workingwithcode/7970125-how-many-threads-is-too-many\n"
                )
            else:
                self.__out_string = (
                    self.__out_string
                    + proc
                    + ": "
                    + str(self.__threads[proc])
                    + " thread(s)\n"
                )

        self.__out_string = (
            self.__out_string
            + "\nFor more information on the job, plots are available at "
            + PDF_URL
            + "\n"
        )
        if self.__warning:
            self.__out_string = self.__out_string + "WARNING=true"
        else:
            self.__out_string = self.__out_string + "WARNING=false"

    def get_out_string(self):
        """Retrieves the self.__out_string attribute"""
        return self.__out_string

    def fill_out_string(self):
        """Fills the self.__out_string object attribute in order to pipe in to the email"""
        self.__out_string = self.__out_string + "----------General Data----------\n"

        self.__out_string = self.__out_string + "Sponsor: " + self.__sponsor + "\n"
        self.__out_string = self.__out_string + "User: " + self.__username + "\n"
        self.__out_string = (
            self.__out_string + "Allocated CPUS: " + str(self.__alloc_cpu) + "\n"
        )
        self.__out_string = (
            self.__out_string + "Allocated TRES: " + str(self.__alloc_tres) + "\n"
        )
        self.__out_string = self.__out_string + "Node(s): " + self.__nodes + "\n"
        self.__out_string = self.__out_string + "Elapsed Time: " + self.__runtime + "\n"

        self.verify_data()

    def make_plot(self, metric, filename, dirname, forpdf=False):
        """
        Makes a plot with a given metric\n
        Args:
        - self
        - metric: string, metric wanted to make plot
        - forpdf: boolean, which tells the function if the calling function was make_pdf()
        """
        # Constants
        plt.figure()
        step_size = "15s"
        URL = PROM_HOST + "/api/v1/query_range"

        if not os.path.exists(dirname):
            os.mkdir(dirname)

        params = {
            "query": metric + '{slurm_job="' + str(self.__jobid) + '"}',
            "start": self.__start_time,
            "end": self.__end_time,
            "step": step_size,
        }

        response = requests.get(URL, params=params)
        json = response.json()["data"]["result"]

        # Iterates thrrough each result given by the JSON returned by the HTTP API
        for item in json:
            values = []
            timestamps = []
            instance = item["metric"]["instance"]

            # Insures we have proc_name only if we're looking for threads
            if "proc_name" in item["metric"]:
                proc_name = item["metric"]["proc_name"]
            else:
                proc_name = False

            # Insures we have core numbers only if we're looking for cpu_time_core
            if "core" in item["metric"]:
                core = item["metric"]["core"]
            else:
                core = False

            # Iterates through each value, gathers it and the associated timestamp
            for value in item["values"]:
                values.append(float(value[1]))
                timestamps.append(value[0])

            # If proc_name or core were defined , then fix the label to be representative of what we're displaying (threads per proc_name or cpu_time per core)
            if proc_name:
                plt.plot(timestamps, values, label=proc_name)

            elif core:
                plt.plot(
                    timestamps, values, label="core: " + core + " node: " + instance
                )

            else:
                plt.plot(timestamps, values, label=instance)

            # Plotting
            plt.xlabel("Time (in seconds since Unix Epoch)")
            plt.ylabel(Y_LABELS[metric])
            plt.title(Y_LABELS[metric] + " of job " + str(self.__jobid))

        # Add legend at the end
        plt.legend()

        # If we're measuring CPU Util, show a threshold (dahsed line) to show where the expected CPU usage is at. (80% per core hardcoded)
        if metric == "jobs_cpu_percent":
            plt.axhline(
                y=80 * (self.__alloc_cpu / len(json)), linestyle="dashed", color="black"
            )

        # If the file already exists in order to save the newer plot. CHANGE it to create plot only if doesn't exist ???
        # if os.path.isfile(filename):
        # os.remove(filename)

        # Function to save the plot
        plt.savefig(dirname + filename)

        # Added bool forpdf to make sure the figure is not closed before it can be put inside the pdf.
        # If the function make_pdf(...) calls make_plot(...), the figure won't close
        if not forpdf:
            plt.close()

    def make_pie(self, metrics, filename, dirname, forpdf=False):
        """
        Makes a pie chart for a list/tuple of metrics\n
        Args:
        - self
        - metrics: list/tuple of metrics needed for pie chart
        - forpdf: boolean which tells the function if the calling function was make_pdf()
        """
        # Constants
        URL = PROM_HOST + "/api/v1/query"

        # Variables
        labels = []
        data = []

        plt.figure()

        if not os.path.exists(dirname):
            os.mkdir(dirname)

        for metric in metrics:
            params = {
                "query": metric + '{slurm_job="' + str(self.__jobid) + '"}',
                "time": self.__end_time,
            }
            response = requests.get(URL, params=params)
            # print(URL)
            # print(params)
            json = response.json()["data"]["result"]
            for item in json:
                # Insures we have core numbers only if we're looking for cpu_time_core
                if "core" in item["metric"]:
                    core = item["metric"]["core"]
                    # print(core)
                else:
                    core = False

                label = (
                    Y_LABELS[item["metric"]["__name__"]]
                    + " "
                    + item["metric"]["instance"]
                )
                # If proc_name or core were defined , then fix the label to be representative of what we're displaying (threads per proc_name or cpu_time per core)
                if core:
                    labels.append(label + " core: " + core)
                else:
                    labels.append(label)

                data.append(float(item["value"][1]))

        total = sum(data)
        data = [(item / total) * 100 for item in data]

        plt.pie(data, labels=labels, autopct="%3.2f%%", startangle=45)

        # Add percentage to labels here so only the legend has them.
        for i in range(len(labels)):
            value = "{0:.2f}".format(data[i])
            labels[i] += " (" + str(value) + "%)"

        plt.legend(labels=labels, loc="lower left", fontsize="x-small")

        # Function to save the plot
        plt.savefig(dirname + filename)

        if not forpdf:
            plt.close()

    def make_pdf(self, jobid, filename, dirname):
        """
        Makes a PDF for a given job id. It retrieves data from Prometheus HTTP API and calls make_plot() as well as make_pie() in order to make the graphs
        and display resource usage to users.\n
        Args:
        - self
        - jobid: integer, Slurm job's ID
        """
        geometry_options = {"right": "2cm", "left": "2cm"}
        fname = CWD + "pdf/" + str(jobid) + "_summary"
        doc = Document(fname, geometry_options=geometry_options)

        metrics = ("jobs_cpu_percent", "jobs_rss", "jobs_read_mb", "jobs_write_mb")

        doc.preamble.append(Command("title", "Plots for job " + str(self.__jobid)))
        doc.preamble.append(Command("date", NoEscape(r"\today")))
        doc.append(NoEscape(r"\maketitle"))

        doc.append(
            "This pdf contains plots/pie charts which are representative of your job's usage of HPC resources on CC clusters"
        )
        doc.append(NewPage())

        for item in metrics:
            self.make_plot(item, filename, dirname, True)

            with doc.create(Section("Plot for " + Y_LABELS[item])):

                with doc.create(Figure(position="htbp")) as plot:
                    plot.add_plot(width=NoEscape(r"1\textwidth"))
                    plot.add_caption(Y_LABELS[item] + " variation with time")
                if item == "jobs_cpu_percent":
                    doc.append(
                        "The dashed line on this plot shows the lowest acceptable bound for CPU usage for a job with as many cores as yours"
                    )
            doc.append(NewPage())
            plt.close()

        metrics = ("jobs_user_time", "jobs_system_time")
        title = [Y_LABELS[metric] for metric in metrics]

        self.make_pie(metrics, filename, dirname, True)
        with doc.create(Section("Pie chart for " + " vs  ".join(title))):

            with doc.create(Figure(position="htbp")) as plot:
                plot.add_plot(width=NoEscape(r"1\textwidth"))
                plot.add_caption(", ".join(title) + " proportions")
        doc.append(NewPage())

        plt.close()

        metrics = ("jobs_cpu_time_core",)

        self.make_pie(metrics, filename, dirname, True)

        with doc.create(Section("Pie chart for " + Y_LABELS[metrics[0]] + " per core")):
            with doc.create(Figure(position="htbp")) as plot:
                plot.add_plot(width=NoEscape(r"1\textwidth"))
                plot.add_caption(Y_LABELS[metrics[0]] + " proportions")
        doc.append(NewPage())

        plt.close()

        doc.generate_pdf(clean_tex=False)

    def expose_json(self):
        """Expose data in a json format in order to make a SOURCE OF TRUTH (API)"""
        data = {}
        data["jobid"] = int(self.__jobid)
        data["sponsor"] = self.__sponsor
        data["username"] = self.__username
        data["runtime"] = self.__runtime
        data["uses_scratch"] = str(self.__uses_scratch)

        data["alloc_tres"] = {
            "billing": self.__billing,
            "alloc_cpu": self.__alloc_cpu,
            "alloc_mem": self.__alloc_mem,
            "alloc_nodes": self.__nodes,
        }

        data["threads"] = self.__threads
        data["cpu"] = {}
        data["cpu"]["available"] = {"amount": self.__alloc_cpu, "unit": "cores"}
        data["cpu"]["used"] = {"amount": self.__count_used_cpus, "unit": "cores"}
        data["cpu"]["time_core"] = []

        for core in self.__cpu_time_core.keys():
            tmp_dict = {
                "core": core,
                "time": self.__cpu_time_core[core],
                "unit": "s",
                "percent_of_total": (self.__cpu_time_core[core] / self.__cpu_time_total)
                * 100,
            }

            data["cpu"]["time_core"].append(tmp_dict)

        data["cpu"]["time_total"] = {"time": self.__cpu_time_total, "unit": "s"}

        data["cpu"]["avg_usage_job"] = {
            "usage": sum(self.__avg_cpu_usage),
            "lowest_expected_usage": 80 * self.__alloc_cpu,
            "unit": "%",
        }

        data["cpu"]["max_usage_job"] = {"usage": sum(self.__max_cpu_usage), "unit": "%"}

        data["ram"] = {}
        data["ram"]["used"] = {
            "amount": sum(self.__max_rss),
            "unit": "MB",
            "usage": sum(self.__max_rss) / int(self.__alloc_mem[:-1]),
        }

        data["ram"]["available"] = {
            "amount": float(self.__alloc_mem[:-1]),
            "unit": "MB",
        }

        data["io"] = {}
        data["io"]["opened_files"] = self.__opened_files

        data["io"]["read"] = {
            "amount": self.__read_mb,
            "unit": "MB",
            "count": self.__read_count,
        }

        data["io"]["write"] = {
            "amount": self.__write_mb,
            "unit": "MB",
            "count": self.__write_count,
        }

        return data
