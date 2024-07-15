#
#  EndToEndTest - base
#
FROM debian:latest AS tests-base

# Install curl, grpcurl, and jq
RUN apt-get update && \
    apt-get install -y curl jq && \
    curl -L "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_x86_64.tar.gz" -o grpcurl.tar.gz && \
    tar -xzf grpcurl.tar.gz -C /usr/local/bin && \
    chmod +x /usr/local/bin/grpcurl

WORKDIR /work
COPY ./e2e_tests.sh .
RUN chmod +x ./e2e_tests.sh
COPY ./put_request.json .

#
#  EndToEndTest - generate-data
#
FROM tests-base AS generate-tests-data
# Default command for generate data
CMD ["./e2e_tests.sh", "dtoserver:10080", "--generate-data"]
