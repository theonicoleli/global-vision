FROM python:3.9

WORKDIR /app

RUN apt-get update && apt-get install -y sqlite3

RUN pip install numpy==1.21.5

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY load_json_into_sqlite.py .
COPY accounts_anonymized.json .
COPY support_cases_anonymized.json .

CMD ["python", "load_json_into_sqlite.py"]
