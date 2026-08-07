
FROM python:3.14@sha256:3a9d2dd3f18e5c7a9d8de7b3659418a4ab848ccd409fb9e91ef9d7a6a3520ba7


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]