
FROM python:3.14@sha256:5c485439db26ba10745100656f6712d662075edb7ec6861dda715bcdfe579b29


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]