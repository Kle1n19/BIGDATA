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

CREATE TABLE IF NOT EXISTS wiki.distinct_domains (
    domain TEXT PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS wiki.pages_by_user (
    user_id INT,
    page_title TEXT,
    page_id INT,
    PRIMARY KEY (user_id, page_id)
) WITH CLUSTERING ORDER BY (page_id ASC);


CREATE TABLE IF NOT EXISTS wiki.domain_page_counts (
    domain TEXT,
    page_id INT,
    PRIMARY KEY (domain, page_id)
);


CREATE TABLE IF NOT EXISTS wiki.page_details_by_page_id (
    page_id INT PRIMARY KEY,
    page_title TEXT,
    domain TEXT,
    user_id INT,
    username TEXT,
    created_by_bot boolean,
    time timestamp
);


CREATE TABLE IF NOT EXISTS wiki.page_creations_by_time (
    day date,
    user_id INT,
    username TEXT,
    PRIMARY KEY ((day), user_id)
);