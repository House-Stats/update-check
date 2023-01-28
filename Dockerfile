FROM python:3.9.7

WORKDIR /app

COPY ./checker .
COPY ./requirements.txt .


RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r requirements.txt

CMD ["python3", "__init__.py"]