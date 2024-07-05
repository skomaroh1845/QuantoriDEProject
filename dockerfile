FROM apache/airflow:2.8.1

# update pip
RUN python3 -m pip install --upgrade pip


COPY requirements.txt .
RUN pip install apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

# COPY dags /opt/airflow/dags
