FROM python:3.9

WORKDIR /afl_games

COPY /afl_games .

RUN pip install -r requirements.txt

CMD ["python", "run.py"]