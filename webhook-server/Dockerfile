
FROM python:3.15@sha256:20fe87d3bb1974b1c4e14d426b399b0e7209d8bbf3b6cae3ff51e09ddd490e73


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]