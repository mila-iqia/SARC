FROM gcr.io/buildpacks/gcp/run:google-24

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
  curl \
  gnupg2 \
  apt-transport-https \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
  unixodbc \
  unixodbc-dev \
  msodbcsql18 \
  && rm -rf /var/lib/apt/lists/*

USER 33:33
