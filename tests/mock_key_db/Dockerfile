FROM postgres
ENV POSTGRES_PASSWORD=pleaselogmein
ENV POSTGRES_USER=testuser
ENV POSTGRES_DB=keys
COPY ./init.sql /docker-entrypoint-initdb.d/init.sql 
