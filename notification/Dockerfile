FROM python:3.11

WORKDIR /app

COPY ./notification/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python utils/other/hotreload.py "notification/src/app.py"
