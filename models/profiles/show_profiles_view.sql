CREATE
OR REPLACE VIEW show_profiles_view AS
WITH
	TEMP AS (
    SELECT 
		-- required Mixpanel fields
        s.showid as distinct_id,
        COALESCE(s.venue || ' - ' || s.city || ', ' || s.state, 'upcoming') as name,
        STRFTIME(s.showdate, '%d/%m/%Y') as email,
        
		s.city as city,
        s.country as country,
        s.state as region,
        m.album_cover_url as avatar,
        ROUND(m.duration / 60000, 2) as duration_mins,
        json_array_length(CAST(m.tracks AS JSON)) as num_songs,
        m.likes_count as likes,
		n.reviews as reviews,
		n.setlistnotes as setlist_notes,
		n.soundcheck as soundcheck,
		n.footnote as footnote
    FROM
       shows s
    JOIN
       metadata m
    ON
       s.showdate = m.date
	JOIN notes n ON n.showid = s.showid
	)
SELECT
	*
FROM
	TEMP;

SELECT * FROM show_profiles_view LIMIT 10;