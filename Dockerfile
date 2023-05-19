FROM apache/beam_python3.9_sdk:2.46.0


RUN apt-get update -y

# Install various dependencies for pyodbc
RUN apt-get install -y gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN ACCEPT_EULA=Y apt-get install -y  msodbcsql17
RUN apt-get install -y unixodbc-dev

RUN pip install -U pyodbc