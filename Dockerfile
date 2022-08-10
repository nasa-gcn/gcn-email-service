FROM python:3.10 
RUN mkdir /src
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt
CMD ["python", "main.py"]
