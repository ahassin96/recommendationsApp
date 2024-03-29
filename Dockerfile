FROM python:3.9-slim


WORKDIR /app


COPY . /app


RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 9092


ENV NAME World


CMD ["python", "recommendationsApp.py"]

