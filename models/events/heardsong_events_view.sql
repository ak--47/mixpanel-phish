CREATE OR REPLACE VIEW heardsong_events_view AS
WITH unnested_tracks AS (
    SELECT
        m.date AS show_date,
        m.venue.latitude AS latitude,
        m.venue.longitude AS longitude,
        track.slug AS track_slug,
        track.title AS song_name,
        track.duration AS duration,
        track.mp3_url AS url,
        track.position AS track_position,
		track.set_name AS set_name		
    FROM
        metadata as m,
        UNNEST(m.tracks) AS tracks(track)
),
TEMP AS (
    SELECT
		-- required Mixpanel fields
        'heard song' AS event,
		a.uid AS distinct_id,        		
        a.showdate + INTERVAL '4 minutes' + INTERVAL '20 seconds' AS show_date, -- this gets turned into a time post-explosion
		-- additional joins		   
	    a.showid AS show_id,
        a.venueid AS venue_id,
		CONCAT_WS (
			'-',
			t.show_date,
			t.set_name,
			t.track_position,
			t.track_slug
		) AS performance_id,
		
		-- analysis fields
		a.username AS username,
        t.latitude,
        t.longitude,
        t.track_slug,
        t.song_name,
        ROUND(t.duration / 60000, 2) AS duration_mins,
        t.url,
		CASE
			WHEN a.showdate < '2000-10-07' THEN '1.0'
			WHEN a.showdate < '2004-08-15' THEN '2.0'
			WHEN a.showdate < '2020-02-23' THEN '3.0'
			WHEN a.showdate > '2020-02-23' THEN '4.0'
			ELSE 'unknown'
		END AS era,
        ROW_NUMBER() OVER (PARTITION BY a.uid, a.showid ORDER BY t.track_position) AS song_number
    FROM
        attendance a
        JOIN shows s ON a.showid = s.showid
        JOIN unnested_tracks t ON a.showdate = t.show_date
)
SELECT
    *,
	-- row timestamp defined here
    show_date + (song_number - 1) * INTERVAL '4 minutes' + (song_number - 1) * INTERVAL '20 seconds' AS time,
	-- dedupe
	hash('heard song',  performance_id) AS insert_id
FROM
    TEMP;

SELECT * FROM heardsong_events_view LIMIT 10;