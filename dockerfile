
FROM python:3.8

WORKDIR /main

COPY ./data ./data

COPY ./preprocess.ipynb ./preprocess.ipynb

COPY ./requirements.txt ./requirements.txt

COPY ./dataextract.py ./dataextract.py

COPY ./model.py ./model.py

COPY ./main.py ./main.py

CMD ["python3","./main.py"]
