CREATE OR REPLACE VIEW attend_events_view AS
WITH
	TEMP AS (
		SELECT
			a.uid as distinct_id,
			a.username as username,
			a.showdate as time,
			'attended show' as event,
			a.showid as show_id,
			a.venueid as venue_id,
			m.venue.latitude as latitude,
			m.venue.longitude as longitude,
			ROUND(m.duration / 60000, 2) AS duration_mins
		FROM
			attendance a
			JOIN shows s ON a.showid = s.showid
			JOIN metadata m on a.showdate = m.date
	)
SELECT
	*
FROM
	TEMP;

SELECT
	*
FROM
	attend_events_view
LIMIT
	100;