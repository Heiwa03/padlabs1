FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY common/ common/
COPY sender.py receiver.py ./
RUN useradd -r -u 10001 app
USER app
# implicit: receiver; pentru sender se suprascrie command în compose / K8s
ENTRYPOINT ["python"]
CMD ["receiver.py", "--topic", "orders"]
