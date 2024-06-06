FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

WORKDIR /app
USER root

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH="${PYTHONPATH}:/app"


RUN ln -sf $(which python3) /usr/bin/python && \
    ln -sf $(which pip3) /usr/bin/pip

RUN apt-get update
RUN apt-get install -y gcc python3-dev 
RUN pip install --upgrade pip setuptools

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /tmp/spark-events
RUN mkdir -p /tmp/spark-history-server-logs

#RUN bash /opt/spark/sbin/start-history-server.sh

EXPOSE 4041
EXPOSE 4040
EXPOSE 18080
EXPOSE 7890

CMD bash /opt/spark/sbin/start-history-server.sh && uvicorn src.run:app --host 0.0.0.0 --port 7890 