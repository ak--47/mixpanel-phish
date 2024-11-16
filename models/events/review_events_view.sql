CREATE OR REPLACE VIEW review_events_view AS
WITH TEMP AS (
SELECT
    r.uid AS distinct_id,
    r.username AS username,
    r.showdate AS time,  -- Assuming r.showdate is already in the desired format
    'reviewed show' AS event,
    r.showid AS show_id,
    s.venueid AS venue_id,
    r.reviewid AS review_id,
    r.score AS rating,
    r.review_text AS review,
    m.venue.latitude AS latitude,
    m.venue.longitude AS longitude,
    ROUND(m.duration / 60000, 2) AS duration_mins
FROM
    reviews r
JOIN
    shows s ON r.showid = s.showid
LEFT JOIN  -- Use LEFT JOIN to handle cases where metadata might be missing
    metadata m ON r.showdate = m.date
)
SELECT * FROM TEMP;

SELECT * FROM review_events_view LIMIT 100;