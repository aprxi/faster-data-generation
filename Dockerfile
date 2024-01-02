FROM public.ecr.aws/docker/library/rust:slim-bookworm

RUN apt-get update --fix-missing \
    && apt-get -y install --no-install-recommends \
        bash \
        zip \
        python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && pip3 install --break-system-packages \
      faker==21.0.0 \
      polars==0.20.2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./examples /examples
RUN \
  cd /examples/rsfake-v1 && cargo build --release \
  && mkdir -p bin && cp target/release/rsfake bin/rsfake \
  && rm -rf target \
  && cd /examples/rsfake-v2 && cargo build --release \
  && mkdir -p bin && cp target/release/rsfake bin/rsfake \
  && rm -rf target

RUN useradd -u 4000 -ms /bin/bash foo \
  && chown -R foo:foo /examples /usr/local/cargo
WORKDIR /examples
USER foo