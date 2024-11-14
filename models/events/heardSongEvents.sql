CREATE OR REPLACE VIEW heardSongs AS
WITH TEMP AS (
    SELECT
        a.uid AS distinct_id,
        a.username AS username,
        a.showdate AS time,
        'heard song' AS event,
        a.showid AS show_id,
        a.venueid AS venue_id,
        m.venue.latitude AS latitude,
        m.venue.longitude AS longitude,
        unnest.slug AS track_slug,
        unnest.title AS song_name,
        ROUND(unnest.duration / 60000, 2) AS duration_mins,
        unnest.mp3_url AS url
    FROM
		 (SELECT * FROM attendance LIMIT 10) AS a
        -- attendance a
        JOIN shows s ON a.showid = s.showid
        JOIN metadata m ON a.showdate = m.date,
        UNNEST(m.tracks)
)
SELECT * FROM TEMP;

SELECT * FROM heardSongs LIMIT 10;