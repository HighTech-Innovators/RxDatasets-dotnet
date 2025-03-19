# REFS
# Optional support for using a private 'proxy' registry
ARG REGISTRY_PREFIX=
FROM ${REGISTRY_PREFIX}mcr.microsoft.com/dotnet/sdk:8.0 AS sdk8

FROM sdk8 AS sonarqube-prepare

# Install Java (OpenJDK 17)
RUN apt-get update && \
    apt-get install --yes --no-install-recommends openjdk-17-jre git && \
    rm -rf /var/lib/apt/lists/*

# Install SonarScanner if not already installed
RUN dotnet tool install --global dotnet-sonarscanner

# Add SonarScanner to PATH
ENV PATH="${PATH}:/root/.dotnet/tools"

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               sonarqube                                            ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

FROM sonarqube-prepare AS sonarqube-check
WORKDIR /code

# Set environment variables for SonarQube
ARG SONAR_PROJECT_KEY=""
ARG SONAR_HOST_URL=""
ARG SONAR_TOKEN=""
ARG SONAR_USER_HOME=""
ARG VERSION=""

ARG CI_COMMIT_REF_NAME=""
ARG CI_MERGE_REQUEST_IID=""
ARG CI_MERGE_REQUEST_TARGET_BRANCH_NAME=""
ARG CI_PROJECT_ID=""

ENV SONAR_USER_HOME=${SONAR_USER_HOME}
ENV VERSION=${VERSION}

ARG DOCKER_HOST
ENV DOCKER_HOST=${DOCKER_HOST}

ARG RunExternalProvidersTests=false

# Copy solution file(s)
COPY *.sln ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./

# Copy all project directories
COPY src/ ./src/
COPY testapps/ ./testapps/
COPY tst/ ./tst/

RUN dotnet restore

# This copy is for sonar to determine what issues were fixed or not
COPY .git /code/.git
ENV sonar_coverage_exclusions="**/RxDatasets/Reporting/**/*,**/testapps/**/*,**/tst/**/*"

# handle branch vs merge request contexts 
RUN set -e; \
    SONAR_SCANNER_PARAMS="/k:${SONAR_PROJECT_KEY} \
    /d:sonar.host.url=${SONAR_HOST_URL} \
    /d:sonar.token=${SONAR_TOKEN} \
    /d:sonar.scm.provider=git \
    /d:sonar.coverage.exclusions=${sonar_coverage_exclusions} \
    /d:sonar.cs.opencover.reportsPaths=**/coverage.opencover.xml"; \
    if [ -n "${CI_MERGE_REQUEST_IID}" ] && [ -n "${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}" ]; then \
      SONAR_SCANNER_PARAMS="${SONAR_SCANNER_PARAMS} \
      /d:sonar.pullrequest.provider=gitlab \
      /d:sonar.pullrequest.key=${CI_MERGE_REQUEST_IID} \
      /d:sonar.pullrequest.branch=${CI_COMMIT_REF_NAME} \
      /d:sonar.pullrequest.base=${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}"; \
    fi; \
    dotnet sonarscanner begin ${SONAR_SCANNER_PARAMS}


RUN dotnet build --no-restore

RUN dotnet test --no-build \
    $(if [ "$RunExternalProvidersTests" = "false" ]; then echo '--filter "FullyQualifiedName!~WithCustomTestContainersImplementation"'; fi) \
    --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=opencover

RUN dotnet sonarscanner end /d:sonar.token="${SONAR_TOKEN}"

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               sonarqube-report                                     ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

FROM sdk8 AS sonarqube-report

# Set environment variables for SonarQube
ARG SONAR_PROJECT_KEY
ARG SONAR_HOST_URL

# Add SONAR_TOKEN as ARG
ARG SONAR_TOKEN
ARG CI_COMMIT_BRANCH
ARG CI_MERGE_REQUEST_IID
WORKDIR /output
RUN curl -u "${SONAR_TOKEN}:" "${SONAR_HOST_URL}/api/issues/gitlab_sast_export?projectKey=${SONAR_PROJECT_KEY}&branch=${CI_COMMIT_BRANCH}&pullRequest=${CI_MERGE_REQUEST_IID}" -o gl-sast-sonar-report.json

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               sonarqube-downloader                                     ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################
FROM scratch AS sonar-report-downloader
COPY --from=sonarqube-report /output /

