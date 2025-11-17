FROM debian:bookworm-slim

# Basic tools + build essentials + libs you need
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      clang \
      clang-tools \
      cmake \
      ninja-build \
      clang-format \
      git \
      ca-certificates \
      build-essential \
      curl \
      ncat \
      vim \
      liburing-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Boost 1.89 headers only into /usr/local/include
ARG BOOST_VERSION=1.89.0
ARG BOOST_DIR=boost_1_89_0

RUN curl -L "https://archives.boost.io/release/${BOOST_VERSION}/source/${BOOST_DIR}.tar.gz" \
        -o "${BOOST_DIR}.tar.gz" \
    && tar -xf "${BOOST_DIR}.tar.gz" \
    && mkdir -p /usr/local/include \
    && cp -r "${BOOST_DIR}/boost" /usr/local/include/ \
    && rm -rf "${BOOST_DIR}" "${BOOST_DIR}.tar.gz"

# Prefer clang as default
ENV CC=clang \
    CXX=clang++ \
    CMAKE_BUILD_PARALLEL_LEVEL=8

# Create non-root user
ARG USERNAME=vscode
RUN useradd -ms /bin/bash ${USERNAME}
USER ${USERNAME}

WORKDIR /workspaces

CMD [ "bash" ]
