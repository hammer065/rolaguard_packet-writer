FROM python:3.6.10-slim-buster

# Set the working directory to /app
WORKDIR /root/app
ENV PYTHONPATH="/root/app"

# Copy the current directory contents into the container at /app
COPY . /root/app/

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Redirect python command to python3
#RUN rm /usr/bin/python
#RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /root/app

ENTRYPOINT python3 MQWriter.py