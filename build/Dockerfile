FROM python:3.11.0-slim-bullseye

RUN mkdir -p /whl/chronicle
COPY setup.py /whl/setup.py
COPY __init__.py /whl/chronicle/__init__.py
COPY chronicle.py /whl/chronicle/chronicle.py

WORKDIR /whl
RUN python setup.py bdist_wheel
