FROM python:2
RUN pip install scrapy
ADD ./ ./
CMD [ "scrapy", "crawl", "toscrape-css", "--set", "JOBDIR=cache" ]
