FROM python:2
RUN pip install scrapy
RUN apt-get install zip
ADD ./ ./
CMD [ "scrapy", "crawl", "toscrape-css", "--set", "JOBDIR=cache" ]
