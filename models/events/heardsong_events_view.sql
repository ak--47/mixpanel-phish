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
        track.position AS track_position
    FROM
        metadata m,
        UNNEST(m.tracks) AS tracks(track)
),
TEMP AS (
    SELECT
        a.uid AS distinct_id,
        a.username AS username,
        a.showdate AS show_date,
        'heard song' AS event,
        a.showid AS show_id,
        a.venueid AS venue_id,
        t.latitude,
        t.longitude,
        t.track_slug,
        t.song_name,
        ROUND(t.duration / 60000, 2) AS duration_mins,
        t.url,
        ROW_NUMBER() OVER (PARTITION BY a.uid, a.showid ORDER BY t.track_position) AS song_number
    FROM
        attendance a
        JOIN shows s ON a.showid = s.showid
        JOIN unnested_tracks t ON a.showdate = t.show_date
)
SELECT
    *,
    show_date + (song_number - 1) * INTERVAL '4 minutes' + (song_number - 1) * INTERVAL '20 seconds' AS time
FROM
    TEMP;

SELECT * FROM heardsong_events_view LIMIT 1000;