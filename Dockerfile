FROM python:2
RUN pip install scrapy websocket-client | apt-get update && apt-get install zip -yq
# && apt-get upgrade -y
ADD ./ ./
CMD [ "python", "jobdir_handler.py"]
