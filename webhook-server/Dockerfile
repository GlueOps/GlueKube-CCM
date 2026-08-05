
FROM python:3.14@sha256:51570a50616289f78f340811c34ca4384985f9e891819cbe7620f738d6ca8525


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]