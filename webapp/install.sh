#!/bin/bash

install -D *.py /var/www/logic_webapp

mkdir /var/www/logic_webapp/pdf
mkdir /var/www/logic_webapp/plots
mkdir /var/www/logic_webapp/pies

install logic_webapp.service /etc/systemd/system/logic_webapp.service