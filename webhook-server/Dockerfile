
FROM python:3.12@sha256:f742af2ba32cd619634370fddbb39ae8539ad84163da52d63a822a04ac2732a1


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]