CREATE
OR REPLACE VIEW perfProfiles_view AS
WITH
	TEMP AS (
		SELECT
			-- mixpanel specific
			CONCAT_WS (
				'-',
				m.date,
				unnest.set_name,
				unnest.position,
				unnest.slug
			) AS distinct_id,
			unnest.title || ' (' || m.date || ')' AS name,
			s.venue || ' - ' || s.city || ', ' || s.state AS email,
			m.album_cover_url AS avatar,
			
			

			m.date AS show_date,
			COALESCE(m.tour_name, p.tourname) AS tour_name,
			unnest.slug AS track_slug,
			unnest.title AS track_title,
			unnest.position AS track_position,
			ROUND(unnest.duration / 60000, 2) AS duration_mins,
			unnest.set_name AS set_name,
			unnest.likes_count AS track_likes,
			unnest.mp3_url AS track_url,
			s.venue as venue,
			p.reviews as reviews,
			p.gap AS gap,
			p.songid AS song_id,
			p.showid AS show_id,
			
			-- interesting modeling things
			
			CASE TRIM(p.trans_mark)
				WHEN ',' THEN 'pause (,)'
				WHEN '>' THEN 'segue (>)'
				WHEN '->' THEN 'jam (->)'
				ELSE 'closer (.)' -- This corresponds to the '' case in your switch statement
			END AS transition,
			CASE
				WHEN p.showdate < '2000-10-07' THEN '1.0'
				WHEN p.showdate < '2004-08-15' THEN '2.0'
				WHEN p.showdate < '2020-02-23' THEN '3.0'
				WHEN p.showdate > '2020-02-23' THEN '4.0'
				ELSE 'unknown'
			END AS era
		FROM
			metadata AS m
			JOIN shows AS s ON s.showdate = m.date
			CROSS JOIN UNNEST (m.tracks) -- can't seem to alias this... 
			JOIN performances p ON p.showdate = m.date
			AND p.slug = unnest.slug
	)
SELECT
	*
FROM
	TEMP;

SELECT * FROM perfProfiles_view LIMIT 100;