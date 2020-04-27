#!/bin/bash

install -m 644 jobs_exporter.service /etc/systemd/system/jobs_exporter.service
install -m 644 nvidia_smi_exporter.service /etc/systemd/system/nvidia_smi_exporter.service
install -m 600 jobs_exporter.py /usr/sbin/jobs_exporter