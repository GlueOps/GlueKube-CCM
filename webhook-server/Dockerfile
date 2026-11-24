
FROM python:3.15@sha256:c3f4160f1cceaf4daa49e2b0b693cae9bb2405efe9e8f8f519dde1ea6c831248


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]