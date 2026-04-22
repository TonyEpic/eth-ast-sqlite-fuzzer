# Base image provided by the course: contains the patched sqlite3-3.39.4,
# the vanilla sqlite3 (3.51.1), the SQLite source at /home/test/sqlite3-src,
# and the seed corpus at /home/test/seeds.
FROM theosotr/sqlite3-test

USER root

# Python runtime + build basics for any wheels we may need later.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
 && rm -rf /var/lib/apt/lists/*

# Copy the project sources into the image.
WORKDIR /opt/test-db
COPY requirements.txt /opt/test-db/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/test-db/requirements.txt

COPY src /opt/test-db/src
COPY tests /opt/test-db/tests
COPY README.md /opt/test-db/README.md

ENV PYTHONPATH=/opt/test-db/src

# Install the tool entrypoint as required by the spec.
RUN printf '#!/bin/sh\nexec python3 -m test_db.main "$@"\n' > /usr/bin/test-db \
 && chmod +x /usr/bin/test-db

# Default command shows help so `docker run <image>` is informative.
CMD ["test-db", "--help"]
