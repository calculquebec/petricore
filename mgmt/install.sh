#!/bin/bash

mkdir /opt/petricore_db
install -o root -g root -m 0700 create_user_job_view.sh /opt/petricore_db/create_user_job_view.sh
