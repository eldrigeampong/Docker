FROM continuumio/miniconda3
RUN pip install --no-cache-dir numpy pandas s3fs pyarrow fastparquet
WORKDIR /Datagov
COPY . .
CMD [ "python", "crime/main.py" ]