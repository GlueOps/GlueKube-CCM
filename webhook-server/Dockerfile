
FROM python:3.15@sha256:640956cce7e0dee7998246e103e830404e8677b8c2cdff603fff3962aa9a526d


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]