# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/Broker/Broker.csproj src/Broker/
RUN dotnet restore src/Broker/Broker.csproj
COPY src/Broker/ src/Broker/
RUN dotnet publish src/Broker/Broker.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS run
WORKDIR /app
COPY --from=build /app .

# utilizator non-root; /data = jurnalul persistent (volum / PVC)
RUN mkdir -p /data && chown -R app:app /data
USER app

ENV BROKER__HOST=0.0.0.0 \
    BROKER__PORT=5000 \
    BROKER__DATADIR=/data \
    BROKER__HEALTHPREFIX=http://*:8080/
EXPOSE 5000 8080
VOLUME /data
ENTRYPOINT ["dotnet", "Broker.dll"]
