FROM python:3.9-slim
ENV AWS_DEFAULT_REGION "us-east-2"
RUN apt-get update

WORKDIR /app

COPY ./requirements.txt /app

RUN pip install -U pip \
    && pip --no-cache-dir install -r ./requirements.txt

COPY . .
COPY profiles.yml /root/.dbt/profiles.yml

RUN dbt deps