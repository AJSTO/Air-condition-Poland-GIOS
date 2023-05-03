FROM python:3.10.9-buster

COPY gios_measurements.py requirements.txt credentials.json config.yaml ./

RUN pip install --no-cache-dir -r requirements.txt

CMD python3 gios_measurements.py