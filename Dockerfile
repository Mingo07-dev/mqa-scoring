FROM python:3.8-slim-bullseye

# INSTALL DEPENDECIES

RUN apt-get update \
&& apt-get upgrade -y \
&& apt-get install -y \
&& apt-get -y install apt-utils gcc libpq-dev libsndfile-dev \
&& apt-get install python-tk -y \
&& pip install aiofiles \
&& pip install python-multipart
RUN pip install Pillow

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /app/

EXPOSE 8000
ENV PYTHONPATH=$PWD

ENV PORT=8000


CMD [ "python" , "mqa-scoring.py" ]