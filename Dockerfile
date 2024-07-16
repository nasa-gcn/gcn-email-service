# Install dependencies separately from the package itself. Assuming that the
# requirements change less frequently than the code, this will result in more
# efficient caching of container layers.

FROM python:3.12 AS requirements
RUN pip install --no-cache-dir poetry poetry-plugin-export
COPY pyproject.toml poetry.lock /
RUN poetry export | pip install --no-cache-dir --ignore-installed --root /destdir -r /dev/stdin

FROM python:3.12-slim
COPY --from=requirements /destdir /
COPY . /src
RUN pip install --no-cache-dir --no-deps --editable /src
ENTRYPOINT ["gcn-email"]
USER nobody:nogroup
