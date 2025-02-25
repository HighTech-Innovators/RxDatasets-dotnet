# REFS
# Optional support for using a private 'proxy' registry
ARG REGISTRY_PREFIX=
FROM ${REGISTRY_PREFIX}mcr.microsoft.com/dotnet/sdk:8.0 AS sdk8
FROM ${REGISTRY_PREFIX}mcr.microsoft.com/dotnet/aspnet:8.0 AS aspnet8
FROM ${REGISTRY_PREFIX}mcr.microsoft.com/dotnet/runtime:8.0 AS runtime8
FROM ${REGISTRY_PREFIX}ghcr.io/tarampampam/curl:8.12.1 AS curl

#
#  Nuget config image
#
FROM sdk8 AS nuget-config

# Optional support for 1 private package source without auth
ARG PRIVATE_NUGET=""
RUN if [ -n "${PRIVATE_NUGET}" ]; \
  then \
    if echo "${PRIVATE_NUGET}" | grep -E -q "\.json/?$"; then \
      dotnet nuget add source "${PRIVATE_NUGET}" --name private --protocol-version 3; \
    else \
      dotnet nuget add source "${PRIVATE_NUGET}" --name private --protocol-version 2; \
    fi; \
  else \
    echo "PRIVATE_NUGET not provided"; \
  fi

# Optional support for a nuget proxy instead of nuget.org
ARG NUGET_PROXY=""
RUN if [ -n "${NUGET_PROXY}" ]; \
  then \
    if echo "${NUGET_PROXY}" | grep -E -q "\.json/?$"; then \
      dotnet nuget add source "${NUGET_PROXY}" --name proxy --protocol-version 3; \
    else \
      dotnet nuget add source "${NUGET_PROXY}" --name proxy --protocol-version 2; \
    fi; \
    dotnet nuget disable source nuget.org; \
  else \
    echo "NUGET_PROXY not provided, using nuget.org"; \
  fi

#
#  Pre-restore tool image
#
FROM sdk8 AS prepare-restore-tool

COPY --from=nuget-config /root/.nuget/NuGet/NuGet.Config ./nuget.config
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet tool install --global dotnet-subset --version 0.3.2

##
#  Pre-restore image
#
FROM sdk8 AS prepare-restore-files

ENV PATH="${PATH}:/root/.dotnet/tools"
ENV CI=true
COPY --from=prepare-restore-tool /root/.dotnet/tools /root/.dotnet/tools/
WORKDIR /code
COPY src src
COPY tst tst
COPY testapps testapps
COPY *.sln ./
COPY Directory.Build.props ./
COPY Directory.Packages.props ./

COPY --from=nuget-config /root/.nuget/NuGet/NuGet.Config ./nuget.config
RUN dotnet subset restore *.sln --root-directory . --output restore_subset/

#
# RESTORE
#
FROM sdk8 AS restore
WORKDIR /code

COPY --from=prepare-restore-files /code/restore_subset .
ENV CI=true
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet restore

#
# BUILD
#
FROM restore AS build
COPY src src
COPY tst tst
COPY testapps testapps
RUN echo dotnet build -p:VERSION=0.0.0-local --no-restore -c Release
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet build -p:VERSION=0.0.0-local --no-restore -c Release

#
# TEST
#
FROM build AS test
ARG DOCKER_HOST=
ENV DOCKER_HOST=${DOCKER_HOST}

ARG RunExternalProvidersTests=true

RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet test --no-build -c Release \
    $(if [ "$RunExternalProvidersTests" = "false" ]; then echo '--filter "FullyQualifiedName!~WithCustomTestContainersImplementation"'; fi)

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               E2E TEST HOST BUILD                                  ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

#
# PUBLISH HOST
#
# don't use release build here - it's not a publishable target, and unversioned build can be cached way more often
FROM build AS publish-host
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish\
      --no-restore\
      --no-build\
      -o /pub/Host\
      testapps/Host

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               NUGET                                                ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

#
# RELEASE BUILD (w/version)
#
FROM build AS releasebuild
ARG VERSION
RUN echo dotnet build -p:VERSION=${VERSION} --no-restore -c Release
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet build\
      --no-restore\
      -c Release\
      -p:DebugType=embedded\
      -p:DebugSymbols=true\
      -p:Deterministic=true\
      -p:VERSION=${VERSION}\
      -p:ServerGarbageCollection=false\
      -p:InvariantGlobalization=true\
      -p:EmitCompilerGeneratedFiles=true\
      .
#
# PACKAGE
#
FROM releasebuild AS package
ARG VERSION
COPY icon.png .
COPY README.md .
RUN echo dotnet pack -p:VERSION=${VERSION} --no-build --no-restore -c Release -o /nuget/
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet pack -p:VERSION=${VERSION} --no-build --no-restore -c Release -o /nuget/
RUN ls -l /nuget

#
# NUGET PUSH
#
FROM package AS nugetpush
ARG TARGET_NUGET
ARG TARGET_NUGET_APIKEY
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet nuget push /nuget/*.nupkg -s ${TARGET_NUGET} -k ${TARGET_NUGET_APIKEY}

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               PUBLISH RELEASE BUILDS                               ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

#
# PUBLISH TESTCONTAINER
#
FROM releasebuild AS publish-testcontainer
ARG VERSION
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish\
      --no-restore\
      --no-build\
      -c Release\
      -p:DebugType=embedded\
      -p:DebugSymbols=true\
      -p:Deterministic=true\
      -p:VERSION=${VERSION}\
      -p:ServerGarbageCollection=false\
      -p:InvariantGlobalization=true\
      -p:EmitCompilerGeneratedFiles=true\
      -o /pub/Host.Testserver\
      testapps/Testserver

##
## PUBLISH BENCHMARK TOOL
##
FROM releasebuild AS publish-benchmark
ARG VERSION
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish\
      --no-restore\
      --no-build\
      -c Release\
      -p:DebugType=embedded\
      -p:DebugSymbols=true\
      -p:Deterministic=true\
      -p:VERSION=${VERSION}\
      -p:ServerGarbageCollection=false\
      -p:InvariantGlobalization=true\
      -p:EmitCompilerGeneratedFiles=true\
      -o /pub/DtoServerBenchmark\
      testapps/DtoServerBenchmark

##
## PUBLISH BENCHMARK TOOL
##
FROM releasebuild AS publish-reader-benchmark
ARG VERSION
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish\
      --no-restore\
      --no-build\
      -c Release\
      -p:DebugType=embedded\
      -p:DebugSymbols=true\
      -p:Deterministic=true\
      -p:VERSION=${VERSION}\
      -p:ServerGarbageCollection=false\
      -p:InvariantGlobalization=true\
      -p:EmitCompilerGeneratedFiles=true\
      -o /pub/DtoServerReaderBenchmark\
      testapps/DtoServerReaderBenchmark

###########################################################################################################
###########################################################################################################
###                                                                                                    ####
###                                               FINAL RUNTIME IMAGES                                 ####
###                                                                                                    ####
###########################################################################################################
###########################################################################################################

#
#  Final image - shared base
#
FROM aspnet8 AS final-dtoserver-base

# import curl from current repository image
COPY --from=curl /bin/curl /bin/curl

# Docs: <https://docs.docker.com/engine/reference/builder/#healthcheck>
HEALTHCHECK --interval=5s --timeout=2s --retries=2 --start-period=2s CMD [ \
    "curl", "--fail", "http://127.0.0.1:18080/" \
]

ENV COMPlus_EnableDiagnostics=0
ENV TEMP_FILE_DIRECTORY=/temp_file_directory
RUN mkdir -p $TEMP_FILE_DIRECTORY
VOLUME $TEMP_FILE_DIRECTORY

EXPOSE 10080/tcp
EXPOSE 18080/tcp

#
#  Final image - BENCHMARK TOOL
#
FROM aspnet8 AS final-benchmark

ENV COMPlus_EnableDiagnostics=0

COPY --from=publish-benchmark /pub/DtoServerBenchmark/ /bin/DtoServerBenchmark/

ENTRYPOINT ["/bin/DtoServerBenchmark/DtoServerBenchmark"]

#
#  Final image - BENCHMARK TOOL
#
FROM aspnet8 AS final-reader-benchmark

ENV COMPlus_EnableDiagnostics=0

COPY --from=publish-reader-benchmark /pub/DtoServerReaderBenchmark/ /bin/DtoServerReaderBenchmark/

ENTRYPOINT ["/bin/DtoServerReaderBenchmark/DtoServerReaderBenchmark"]

#
#  Final image - Testcontainer
#
FROM final-dtoserver-base AS final-testcontainer

COPY --from=publish-testcontainer /pub/Host.Testserver/ /bin/dtoserver/

VOLUME /data
ENTRYPOINT ["/bin/dtoserver/Host.Testserver"]

#
#  Final image - E2E Test Host
#
FROM final-dtoserver-base AS final-host

COPY --from=publish-host /pub/Host/ /bin/dtoserver/

VOLUME /data

# Define the command to run your main application
ENTRYPOINT ["dotnet", "/bin/dtoserver/Host.dll"]
