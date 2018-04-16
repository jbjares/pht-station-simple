FROM python:alpine
LABEL maintainer="lukas.zimmermann@uni-tuebingen.de"
COPY app /app
WORKDIR /app
RUN mkdir /data && pip install -r requirements.txt
ENTRYPOINT [ "python", "app.py" ]
EXPOSE 6005
