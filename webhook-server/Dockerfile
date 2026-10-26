
FROM python:3.15@sha256:8c86ab95a8588291d18194f11a0c7803a1c42f786396b342b0dfde8d87d113de


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]