CREATE OR REPLACE VIEW heardsong_events_view AS
WITH TEMP AS (
    SELECT
        a.uid AS distinct_id,
        a.username AS username,
        a.showdate AS show_date,  -- Store the original time
        'heard song' AS event,
        a.showid AS show_id,
        a.venueid AS venue_id,
        m.venue.latitude AS latitude,
        m.venue.longitude AS longitude,
        track.slug AS track_slug,
        track.title AS song_name,
        ROUND(track.duration / 60000, 2) AS duration_mins,
        track.mp3_url AS url,
		ROW_NUMBER() OVER (PARTITION BY a.uid, a.showid ORDER BY track.position) AS song_number  -- Calculate song number for time offset
    FROM
		attendance a
		-- (SELECT * FROM attendance LIMIT 2000) AS a
		--  (SELECT * FROM attendance WHERE uid='104' LIMIT 10) AS a        
        JOIN shows s ON a.showid = s.showid
        JOIN metadata m ON a.showdate = m.date,
        UNNEST(m.tracks) as tracks(track)
)
SELECT 
	*,
    show_date + (song_number - 1) * 4 * INTERVAL '1 minute' + (song_number - 1) * 20 * INTERVAL '1 second' AS time  -- Add the time offset
 FROM TEMP;

-- SELECT * FROM heardsong_events_view LIMIT 1000;