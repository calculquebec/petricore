#!/bin/bash

install jobs_exporter.service /etc/systemd/system/jobs_exporter.service
install jobs_exporter.py /usr/sbin/jobs_exporter