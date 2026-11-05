
FROM python:3.15@sha256:61bd1dea5cd008d1d399dbebddd1b441b0229e966867c96e16467a85c6d3da03


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]