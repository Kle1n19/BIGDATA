CREATE KEYSPACE IF NOT EXISTS wiki
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


CREATE TABLE IF NOT EXISTS wiki.domain_page_stats (
    time timestamp,
    domain text,
    created_by_bot boolean,
    PRIMARY KEY ((time, domain), created_by_bot)
);

CREATE TABLE IF NOT EXISTS wiki.user_stat (
    time timestamp,   
    user_id bigint,
    username text,
    page_title text,
    PRIMARY KEY (time, user_id)
);