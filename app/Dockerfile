FROM ubuntu:latest

WORKDIR /usr/app/src

RUN apt update
RUN apt install python3 -y
RUN apt-get -y install python3-pip
RUN apt-get update && apt-get -y install libpq-dev gcc && pip install psycopg2

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . ./

#CMD ["python3", "./run.py"]