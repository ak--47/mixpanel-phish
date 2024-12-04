CREATE
OR REPLACE VIEW performance_profiles_view AS
WITH
	TEMP AS (
		SELECT
			-- required Mixpanel fields
			CONCAT_WS (
				'-',
				m.date,
				track.set_name,
				track.position,
				track.slug
			) AS distinct_id,
			track.title || ' (' || m.date || ')' AS name,
			s.venue || ' - ' || s.city || ', ' || s.state AS email,
			-- analysis fields
			m.album_cover_url AS avatar,
			m.date AS show_date,
			COALESCE(m.tour_name, p.tourname) AS tour_name,
			track.slug AS track_slug,
			track.title AS track_title,
			track.position AS track_position,
			ROUND(track.duration / 60000, 2) AS duration_mins,
			track.set_name AS set_name,
			track.likes_count AS track_likes,
			track.mp3_url AS track_url,
			s.venue as venue,
			p.reviews as reviews,
			p.gap AS gap,
			p.songid AS song_id,
			p.showid AS show_id,
			n.jamchart_description AS jam_notes,
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
			CROSS JOIN UNNEST (m.tracks) as tracks (track) -- can't seem to alias this... 
			JOIN performances p ON p.showdate = m.date
			AND p.slug = track.slug			
            LEFT JOIN  notes n ON 
			p.showdate = n.showdate AND p.set = n.set AND p.position = n.position AND p.slug = n.slug

	)
SELECT
	*
FROM
	TEMP;

SELECT
	*
FROM
	performance_profiles_view
WHERE
	jam_notes not null
LIMIT
	10;