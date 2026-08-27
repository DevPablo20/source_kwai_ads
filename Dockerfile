FROM docker.io/airbyte/python-connector-base:4.0.2@sha256:9fdb1888c4264cf6fee473649ecb593f56f58e5d0096a87ee0b231777e2e3e73

WORKDIR /airbyte/integration_code

COPY . ./
RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=devpablo20/source-kwai-ads
