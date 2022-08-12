FROM python:3.10 AS build
ENV POETRY_VIRTUALENVS_CREATE false
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
COPY . /src
WORKDIR /src
RUN $HOME/.poetry/bin/poetry install --no-dev

FROM python:3.10-slim
COPY --from=build /usr/local/lib/python3.10/site-packages/ /usr/local/lib/python3.10/site-packages/
COPY --from=build /src/ /src/
COPY --from=build /usr/local/bin/gcn-email-service /usr/local/bin/
ENTRYPOINT ["gcn-email-service"]
USER nobody:nogroup
