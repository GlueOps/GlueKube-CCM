
FROM python:3.15@sha256:fa4f927f66f3ff00a0bd02ef9c8a9afce66b79be6c84741eba258691756803e4


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]