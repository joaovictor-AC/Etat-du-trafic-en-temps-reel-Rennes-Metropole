FROM python:3.11-alpine

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

USER root
RUN apk update
RUN apk add --no-cache curl
RUN apk add --no-cache bash

WORKDIR /producer/app

# Install dependencies
COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY ./app ./app

CMD [ "bash" ]