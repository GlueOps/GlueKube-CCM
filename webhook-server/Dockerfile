
FROM python:3.14@sha256:ffebef43892dd36262fa2b042eddd3320d5510a21f8440dce0a650a3c124b51d


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]