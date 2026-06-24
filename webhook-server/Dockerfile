
FROM python:3.14@sha256:1a644e11f327643c58d47b3aaf4632ba602da0da95d3e5845d85bd50d4be30e8


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]