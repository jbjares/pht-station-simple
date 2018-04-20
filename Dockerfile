FROM python:alpine
LABEL maintainer="lukas.zimmermann@uni-tuebingen.de"
COPY app /app
WORKDIR /app
RUN mkdir /data && pip install -r requirements.txt

# Config needs to mounted at runtime
ENV TRAIN_SIMPLE_STATION_CONFIG_FILE /app/config.cfg
ENTRYPOINT [ "python", "app.py" ]
