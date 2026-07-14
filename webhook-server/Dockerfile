
FROM python:3.14@sha256:d158a97d809b7678f4383cabbaf4d8f0974579ebad98f30ca6aa1bf3d850cab8


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]