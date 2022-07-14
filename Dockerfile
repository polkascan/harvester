# base image
FROM python:3.8-buster
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH "${PYTHONPATH}:/usr/src"

## set working directory
RUN mkdir -p /usr/src
WORKDIR /usr/src

# Upgrade pip
RUN pip install --upgrade pip

## add requirements
COPY ./requirements.txt /usr/src/requirements.txt
#
## install requirements
RUN pip3 install -r requirements.txt

# add app
COPY . /usr/src

ENTRYPOINT ["python",  "app/harvester.py"]
#CMD ["app/harvester.py"]
