
FROM python:3.14@sha256:7e23f81f7111ddeac2d4d056cfda841cda5dd0297401cdbc271feea4f3f1a11e


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]