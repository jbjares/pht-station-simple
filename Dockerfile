FROM python:alpine
LABEL maintainer="lukas.zimmermann@uni-tuebingen.de"
COPY app /app
WORKDIR /app
RUN mkdir /data && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /tmp/*
ENTRYPOINT [ "python", "app.py" ]

