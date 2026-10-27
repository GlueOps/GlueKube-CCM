
FROM python:3.15@sha256:3d35e8d5e482ef4f6151426e59208ecde73b7a80e88b38c1b098f60662e5fb91


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]