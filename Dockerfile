FROM python:3.8.2-slim-buster
WORKDIR /app
COPY requirements.txt /app
COPY src/ /app

RUN apt-get update && apt-get install -y \
  default-libmysqlclient-dev \
  python3-dev \
  gcc \
  g++
RUN pip install -r requirements.txt
RUN pip install jupyterlab
CMD ["sh", "-c", "jupyter lab --ip=0.0.0.0 --no-browser --NotebookApp.token=dst --allow-root"]
#CMD ["sh", "-c", "python main.py"]