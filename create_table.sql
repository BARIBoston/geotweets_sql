DROP TABLE IF EXISTS tweets;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS places;

CREATE TABLE users(
	id BIGINT PRIMARY KEY,
	name VARCHAR,
	screen_name VARCHAR,
	description VARCHAR,
	verified BOOLEAN,
	geo_enabled BOOLEAN,
	statuses_count INTEGER,
	followers_count INTEGER,
	friends_count INTEGER,
	time_zone VARCHAR,
	lang VARCHAR,
	location VARCHAR
);

CREATE TABLE places(
	id VARCHAR PRIMARY KEY,
	country VARCHAR,
	full_name VARCHAR,
	place_type VARCHAR
);

CREATE TABLE tweets(
	id VARCHAR PRIMARY KEY,
	user_id BIGINT REFERENCES users(id),
	place_id VARCHAR REFERENCES places(id),
	text VARCHAR,
	created_at TIMESTAMP,
	hashtags VARCHAR[],
	urls VARCHAR[],
	lang VARCHAR,
	mentioned_user_ids BIGINT[],
	in_reply_to_status_id VARCHAR,
	in_reply_to_user_id BIGINT
);
ALTER TABLE tweets ADD COLUMN coordinates geometry(Point, 4326);
