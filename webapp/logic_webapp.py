#!/usr/bin/env python3

from flask import Flask, send_file, redirect, url_for
import os
from job import Job
from user import User

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
    job = Job(jobid)
    try:
        job.fill_out_string()
    except Exception as e:
        print(str(e))
        print("This is normal if the job was cancelled, it pulls an empty json")
        return redirect(url_for("index"))
        # pass
    job.expose_json()
    return job.get_out_string()


@app.route("/plot/<jobid>/<metric>")
def job_plot(jobid, metric):
    job = Job(jobid)
    filename = metric + ".png"
    dirname = CWD + "plots/" + str(jobid) + "/"

    if not os.path.isfile(dirname + filename):
        try:
            job.make_plot(metric, filename, dirname)
        except Exception as e:
            print(str(e))
            return redirect(
                url_for("index")
            )  # Avant on avait un pass ici au lieu d'un return...

    try:
        return send_file(dirname + filename, attachment_filename=str(jobid) + filename)
    except Exception as e:
        print(str(e))
        return redirect(url_for("index"))


@app.route("/pie/<jobid>/")
def job_pie(jobid):
    job = Job(jobid)
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
            print(str(e))
            return redirect(url_for("index"))

    try:
        return send_file(dirname + filename, attachment_filename=filename)
    except Exception as e:
        print(str(e))
        return redirect(url_for("index"))


@app.route("/pdf/<jobid>")
def job_pdf(jobid):
    job = Job(jobid)
    filename = str(jobid) + "_summary.pdf"
    dirname = CWD + "pdf/"
    if not os.path.isfile(dirname + filename):
        try:
            job.make_pdf(jobid, filename, dirname)
        except Exception as e:
            print(str(e))
            return redirect(url_for("index"))

    try:
        return send_file(dirname + filename, attachment_filename=filename)
    except Exception as e:
        print(str(e))
        return redirect(url_for("index"))


@app.route("/api/v1/jobs/<jobid>/usage")
def job_truth(jobid):
    job = Job(jobid)
    return job.expose_json()


@app.route("/api/v1/users/<username>")
def user_truth(username):
    user = User(username)
    return user.get_info()


if __name__ == "__main__":
    app.run(debug=True)
