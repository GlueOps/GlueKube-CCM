
FROM python:3.14@sha256:004e5a107ef25cc55a2a126ca2cc3a814f4d4cfcb81438972107fbd6fbdd13e0


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]