# Stage 1: Set permissions in a temporary container
FROM debian:latest as temp-container
COPY . .
COPY ./expected-data /tmp/minio-data
RUN ls ./expected-data -la
RUN chown -R 1001:0 /tmp/minio-data \
    && chmod -R 755 /tmp/minio-data

# Stage 2: Build the final image
FROM docker.io/bitnami/minio as minio-container
COPY --from=temp-container /tmp/minio-data /bitnami/minio/data/

#
#  EndToEndTest - base
#
FROM bash as tests-base

COPY --from=fullstorydev/grpcurl /bin/grpcurl /bin/grpcurl
COPY --from=ghcr.io/jqlang/jq /jq /bin/jq

WORKDIR /work
COPY e2e_tests.sh .

COPY put_request.json .

#
#  EndToEndTest - tests-run
#
FROM tests-base as end-to-end-test
# Default command
CMD ["bash", "./e2e_tests.sh", "dtoserver-actual:10080", "dtoserver-expected:10080"]
