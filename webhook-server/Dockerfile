
FROM python:3.15@sha256:5acd826ebc4a7bb4cc50ff2dd0dc479043fab0ae40c4f1661018980374df00b3


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]