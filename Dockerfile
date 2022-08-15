FROM python:3.8

WORKDIR /app


ENV ES_USER=admin
ENV ES_PASS=tMwQHCgwPHWcyvQ9XXwwMc38


COPY . .

RUN pip install -r requirements.txt


CMD ["python3", "./new_zakupki.py"]