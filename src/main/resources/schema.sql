-- represents the enriched profiles
CREATE TABLE IF NOT EXISTS PROFILE (
  id          BIGINT       NOT NULL,
  following   BOOLEAN      NOT NULL,
  verified    BOOLEAN      NOT NULL,
  name        VARCHAR(500) NULL,
  description VARCHAR(500) NULL,
  screen_name VARCHAR(500) NULL
);

-- this describes the initial batch of following IDs that we want to process.
-- we'll work through this list, adding entries in the following table as we process them.
-- we should remember to mark this row as 'processed' when we work through it
CREATE TABLE IF NOT EXISTS PROFILE_IDS (
  id        BIGINT   NOT NULL UNIQUE,
  processed DATETIME NULL
);