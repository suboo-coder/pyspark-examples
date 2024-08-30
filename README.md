# Pre-requisites
- Python Version 3.11
- Spark 3.5

# Run Pipelines
## Build image
docker build -t app8 .
## Interact
docker run -it javaapp8:latest bash

## Not required for now
Set SPARK_CONF_DIR to log4j.properties

## Run the job
cd pyspark-examples/<app-dir>
spark-submit <entry_file>.py

# Approach 1 to run the different pipelines (Not Working!)
## Build Docker image
docker compose build --no-cache

## Start services
docker compose up -d

# Approach 2 to run all container once (Recomended!)
## Build Docker image
docker compose build --no-cache

## Run Docker in interactive mode once
docker compose run --rm app /bin/bash

# Postgres
## Connect with DB client
docker exec -it <container id for postgres app> psql -U admin -d coursedb

## DB Queries

```
create schema courseschema;
create table courseschema.course
(
    course_id integer NOT NULL,
    course_name character varying collate "default" NOT NULL,
    author_name character varying collate "default" NOT NULL,
    course_section json NOT NULL,
    creation_date date NOT NULL,
    CONSTRAINT course_pkey PRIMARY KEY (course_id)
);

create table courseschema.people
(
    name character varying collate "default" NOT NULL,
    gender character varying collate "default" NOT NULL,
    birth date NOT NULL,
    rank INT NOT NULL
);
```

```
insert into courseschema.course
(course_id, course_name, author_name, course_section, creation_date)
values (2, 'Hadoop Spark', 'Abc', '{"section": 1, "title": "Hadoop"}', '2024-08-28');

insert into courseschema.course
(course_id, course_name, author_name, course_section, creation_date)
values (2, 'Python', 'Xyz', '{"section": 1, "title": "Python"}', '2024-08-28');
```

```
```

```
```
