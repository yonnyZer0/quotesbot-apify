FROM yonny/scrapy-poc
ADD ./ ./
CMD [ "python", "jobdir_handler.py"]
