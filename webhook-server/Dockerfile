
FROM python:3.12@sha256:154f46575bab3aaa3a77cb2a00cfcc195efd1d396f02c82809d51db4ebbc13e6


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]