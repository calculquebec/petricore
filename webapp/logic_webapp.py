#!/usr/bin/env python3

from flask import Flask, send_file, redirect, url_for
import os
from job import Job
from user import User
from subprocess import CalledProcessError

CWD = "/var/www/logic_webapp/"

app = Flask(__name__)


@app.route("/")
def index():
    return """
    <h1>Logic Web App</h1>
    <p>This is the Webapp which serves as a the implementation of logic for the my.cc portal as well as the smail script
    <br>
        Multiple paths are available from here:
        <ul>
        <li>logic/pdf/&lt;jobid&gt; will give you a pdf with plots for your a given job id</li>
        <li>logic/plot/&lt;jobid&gt;/&lt;metric&gt; will give you a plot for a given metric for a given job id</li>
        <li>logic/mail/&lt;jobid&gt; will give you the contents of the email sent after completion for a given job id</li>
        </ul>

        Examples
        <ul>
        <li><a href="https://goldman.calculquebec.cloud/pdf/243">A pdf for job 243</a></li>
        <li><a href="https://goldman.calculquebec.cloud/plot/317/jobs_cpu_percent">A plot for job 243's CPU usage</a></li>
        <li><a href="https://goldman.calculquebec.cloud/mail/242">The contents of the email sent after job 243's completion</a></li>
        </ul>
    </p>
    """


@app.route("/mail/<jobid>")
def job_info(jobid):

    try:
        assert type(jobid) == int
        job = Job(jobid)
        job.fill_out_string()
    except Exception as e:
        return {"error": e}, 404
    return job.get_out_string()


@app.route("/plot/<jobid>/<metric>")
def job_plot(jobid, metric):
    assert type(jobid) == int
    job = Job(jobid)
    filename = metric + ".png"
    dirname = CWD + "plots/" + str(jobid) + "/"

    if not os.path.isfile(dirname + filename):
        try:
            job.make_plot(metric, filename, dirname)
        except Exception as e:
            return {"error": e}, 404

    try:
        return send_file(dirname + filename, attachment_filename=str(jobid) + filename)
    except Exception as e:
        return {"error": e}, 404


@app.route("/pie/<jobid>/")
def job_pie(jobid):
    try:
        assert type(jobid) == int
        job = Job(jobid)
    except Exception as e:
        return {"error": e}, 404

    metrics = ("jobs_system_time", "jobs_user_time")
    filename = str(jobid)
    dirname = CWD + "pies/" + str(jobid) + "/"

    for metric in metrics:
        filename += metric + "_"
    filename += ".png"

    if not os.path.isfile(dirname + filename):
        try:
            job.make_pie(metrics, filename, dirname)
        except Exception as e:
            return {"error": e}, 404

    try:
        return send_file(dirname + filename, attachment_filename=filename)
    except Exception as e:
        return {"error": e}, 404


@app.route("/pdf/<jobid>")
def job_pdf(jobid):
    try:
        assert type(jobid) == int
        job = Job(jobid)
    except Exception as e:
        return {"error": e}, 404

    filename = str(jobid) + "_summary.pdf"
    dirname = CWD + "pdf/"
    if not os.path.isfile(dirname + filename):
        try:
            job.make_pdf(jobid, filename, dirname)
        except Exception as e:
            return {"error": e}, 404

    try:
        return send_file(dirname + filename, attachment_filename=filename)
    except Exception as e:
        return {"error": e}, 404


@app.route("/api/v1/jobs/<jobid>/usage")
def job_truth(jobid):
    try:
        assert type(jobid) == int
        job = Job(jobid)
        retval = job.expose_json()
    except IndexError:
        retval = {"error": "Job " + jobid + " does not exist"}, 404
    except CalledProcessError:
        retval = {"error": "Job " + jobid + " is not finished"}, 404
    except Exception as e:
        retval = {"error": str(e)}

    return retval


@app.route("/api/v1/users/<username>")
def user_truth(username):
    try:
        user = User(username)
        retval = user.get_info()
    except KeyError:
        retval = {"error": "User " + username + " does not exist"}, 404
    except Exception as e:
        retval = {"error": str(e)}
    return retval


if __name__ == "__main__":
    app.run()
