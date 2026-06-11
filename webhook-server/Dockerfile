
FROM python:3.14@sha256:90d5306545870da68636de2a12ba9132388714fb5f7077cb914c0e30a96d7de4


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]