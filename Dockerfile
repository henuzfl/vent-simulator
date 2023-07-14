FROM python:3.8.13-bullseye
RUN mkdir /work
COPY . /work/
RUN  pip install --no-cache-dir -r /work/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple some-package
WORKDIR /work/
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone
ENTRYPOINT python -u ./app.py
