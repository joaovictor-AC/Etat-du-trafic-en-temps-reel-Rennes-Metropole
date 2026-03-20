FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

USER root
WORKDIR /producer/app
COPY ./requirements.txt .

RUN apt-get update && pip install -r requirements.txt

COPY ./app ./app

CMD [ "bash" ]