FROM prefecthq/prefect:2-python3.11

COPY ./ /tmp/rca-echo-tools

RUN pip install uv
RUN uv pip install --system prefect-aws /tmp/rca-echo-tools