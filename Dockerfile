FROM python:2
RUN pip install scrapy websocket
RUN apt-get update && apt-get install zip -yq
ADD ./ ./
CMD [ "python", "jobdir_handler.py" ]
