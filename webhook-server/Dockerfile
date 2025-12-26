
FROM python:3.13@sha256:ef87b1f3653cb3dca0227569a6c0b3883a39f80de17755f299a60405524bbfed


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]