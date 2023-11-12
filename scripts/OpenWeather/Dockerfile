FROM continuumio/miniconda3
RUN pip install --no-cache-dir pandas
WORKDIR /OpenWeather
COPY . .
CMD [ "python", "main.py" ]