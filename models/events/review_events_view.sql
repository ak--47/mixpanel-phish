CREATE OR REPLACE VIEW review_events_view AS
WITH TEMP AS (
SELECT
	-- required Mixpanel fields
    'reviewed show' AS event,
	r.uid AS distinct_id,
    r.showdate + INTERVAL '24 HOUR' AS time, -- review always comes after the show...	
	hash('reviewed show', r.uid, r.showdate) AS insert_id,
	-- additional joins
    r.showid AS show_id,
    s.venueid AS venue_id,
    r.reviewid AS review_id,
	-- analysis fields
	r.username AS username,    
    r.score AS rating,
    r.review_text AS review,
    m.venue.latitude AS latitude,
    m.venue.longitude AS longitude,
    ROUND(m.duration / 60000, 2) AS duration_mins,
	CASE
		WHEN r.showdate < '2000-10-07' THEN '1.0'
		WHEN r.showdate < '2004-08-15' THEN '2.0'
		WHEN r.showdate < '2020-02-23' THEN '3.0'
		WHEN r.showdate > '2020-02-23' THEN '4.0'
		ELSE 'unknown'
	END AS era
	
FROM
    reviews r
JOIN
    shows s ON r.showid = s.showid
LEFT JOIN  -- Use LEFT JOIN to handle cases where metadata might be missing
    metadata m ON r.showdate = m.date
)
SELECT * FROM TEMP;

SELECT * FROM review_events_view LIMIT 10;