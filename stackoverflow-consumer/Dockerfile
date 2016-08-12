FROM python:3.4

ADD . /code
RUN pip3 install pymongo redis

WORKDIR /code
CMD ["python3", "storing_data.py"]
