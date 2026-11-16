
FROM python:3.15@sha256:61c06383336ec271f3ff69ceae4a4f011faa6f78638fefad00a1d82e807e5c39


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]