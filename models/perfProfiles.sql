CREATE OR REPLACE VIEW perfProfiles AS
SELECT
    s.id AS show_id,
    s.date AS show_date,
    s.tour_name AS tour_name,
	-- why TF is this called 'unnest'
	-- unnest.slug as song_slug
	track.id as song_id,
	track.slug as song_slug,
	track.title as song_title,
	track.position as song_position,
	ROUND(track.duration / 60000, 2) as duration_mins,
	track.set_name as set_name,
	track.likes_count as likes
	track.mp3_url as url
	track.show_album_cover_url as avatar,
	track.venue_name as venue_name

	
FROM
    metadata s,
    UNNEST(s.tracks) AS track;
