CREATE TABLE IF NOT EXISTS twitter_profile
(
    id            serial primary key,
    following     BOOLEAN               NOT NULL,
    verified      BOOLEAN               NOT NULL,
    name          VARCHAR(500)          NULL,
    description   VARCHAR(500)          NULL,
    screen_name   VARCHAR(500)          NULL,
    followed      BOOLEAN DEFAULT FALSE NOT NULL,
    followed_date date                  NULL
);

CREATE TABLE IF NOT EXISTS twitter_profile_id
(
    id        serial primary key,
    processed date NULL
);

