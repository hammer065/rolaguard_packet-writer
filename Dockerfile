FROM python:3.6-slim-buster

# Set the working directory to /app
WORKDIR /root/app

# Copy the current directory contents into the container at /app
COPY ./requirements.txt ./

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip \
  && pip install --upgrade --trusted-host pypi.python.org --no-cache-dir --timeout 1900 -r requirements.txt \
  && find /usr/local/ \( -type d -a -name test -o -name tests \) -o \( -type f -a -name '*.pyc' -o -name '*.pyo' \) -delete \
  && apt-get clean autoclean \
  && apt-get autopurge -y \
  && rm -rf /var/lib/apt/lists/*

COPY . .

ENV PYTHONPATH="/root/app"

ENTRYPOINT python3 MQWriter.py
