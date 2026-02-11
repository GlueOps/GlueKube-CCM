
FROM python:3.13@sha256:498320f325ad70645e99ff676347987ca9117728784b8273fb6d25cc735ad9c0


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]