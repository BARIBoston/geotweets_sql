DROP TABLE IF EXISTS geotweets;
DROP TABLE IF EXISTS geotweets_users;
DROP TABLE IF EXISTS geotweets_places;

CREATE TABLE geotweets_users(
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

CREATE TABLE geotweets_places(
	id VARCHAR PRIMARY KEY,
	country VARCHAR,
	full_name VARCHAR,
	place_type VARCHAR
);
ALTER TABLE geotweets_places ADD COLUMN bounding_box geometry(Polygon, 4326);

CREATE TABLE geotweets(
	id BIGINT PRIMARY KEY,
	user_id BIGINT REFERENCES geotweets_users(id),
	place_id VARCHAR REFERENCES geotweets_places(id),
	text VARCHAR,
	created_at TIMESTAMP,
	hashtags VARCHAR[],
	urls VARCHAR[],
	media VARCHAR[],
	lang VARCHAR,
	quoted_status_id BIGINT,
	mentioned_user_ids BIGINT[],
	in_reply_to_status_id VARCHAR,
	in_reply_to_user_id BIGINT
);
ALTER TABLE geotweets ADD COLUMN coordinates geometry(Point, 4326);
