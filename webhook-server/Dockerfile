
FROM python:3.15@sha256:df3f05ff4d74833f52146edd195c8f7913581b57796a820fb7e373457ace9bad


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]