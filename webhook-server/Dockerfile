
FROM python:3.15@sha256:67ee282bdbe5ece1e38ea94fa68f1420e969e6176e6c826b9acc931055011bfb


WORKDIR /code


COPY ./requirements.txt /code/requirements.txt


RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt


COPY ./app /code/app


CMD ["fastapi", "run", "app/main.py", "--port", "80"]