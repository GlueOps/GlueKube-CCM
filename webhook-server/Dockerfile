
FROM python:3.14@sha256:b0a637ba74875a5f9ce0a7844cd8e2b76e97650072d0f455410182a1b7d6d62f


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]