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
        build-essential \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir gcovr

# Build a third sqlite3 binary instrumented with gcov so we can measure how
# much of the SQLite source the fuzzer actually exercises. Re-use the source
# tree that the base image already ships at /home/test/sqlite3-src.
RUN cp -r /home/test/sqlite3-src /opt/sqlite3-coverage \
 && cd /opt/sqlite3-coverage/build \
 && make distclean >/dev/null 2>&1 || true \
 && ../configure --disable-shared --disable-tcl \
        CFLAGS="-O0 -g -fprofile-arcs -ftest-coverage" \
        LDFLAGS="-fprofile-arcs -ftest-coverage" >/dev/null \
 && make -j"$(nproc)" sqlite3 \
 && cp sqlite3 /usr/bin/sqlite3-coverage

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
