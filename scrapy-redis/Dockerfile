FROM ubuntu:14.04

ADD sources.list /etc/apt/sources.list
ADD get-pip.py ./get-pip.py
ADD pip.conf ~/.pip/pip.conf
RUN apt-get update && apt-get install -y python3-dev python3-lxml libssl-dev libffi-dev wget
RUN python3 get-pip.py
RUN pip3 install scrapy scrapy-redis
