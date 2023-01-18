FROM continuumio/miniconda3:4.10.3p0-alpine as base

ARG CONDA_ENV_BASE=pyenv

RUN apk update
RUN apk upgrade
RUN apk add --no-cache gcc cmake git gfortran sqlite sqlite-dev pcre-dev linux-headers libc-dev libffi-dev
RUN pip install -U pip

COPY environment.yml /tmp/environment.yml

RUN conda config --add channels conda-forge
RUN conda create -n $CONDA_ENV_BASE python=3.10
RUN conda env update -n $CONDA_ENV_BASE --file /tmp/environment.yml

RUN conda run -n $CONDA_ENV_BASE --no-capture-output conda clean -acfy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    find /opt/conda -follow -type f -name '*.js.map' -delete

FROM continuumio/miniconda3:4.10.3p0-alpine as prime

ENV APP_USER=www-data
ENV CONDA_ENV=/opt/conda/envs/pyenv

RUN adduser -S $APP_USER -G $APP_USER -G root

RUN apk update
RUN apk upgrade
RUN apk add --no-cache sqlite wget curl

COPY . /src/
WORKDIR /src/

COPY --from=base /opt/conda/envs/pyenv $CONDA_ENV

ENV PYTHONPATH /src:$CONDA_ENV:$PYTHONPATH
ENV PATH /src:$CONDA_ENV:$PATH

#ENTRYPOINT ["tail", "-f", "/dev/null"]