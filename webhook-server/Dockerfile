
FROM python:3.15@sha256:f6f18f3fb3bb7894ccb1879c93a328f5df2bee04f7bf0d76ec79ac822fd1312a


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]