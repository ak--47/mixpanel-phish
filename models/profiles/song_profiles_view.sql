CREATE
OR REPLACE VIEW song_profiles_view AS
WITH
	TEMP AS (
    SELECT 
		-- required Mixpanel fields
        s.songid as distinct_id,
        s.song as name,
        s.artist as email,
		
		s.debut as debut,
		s.times_played as times_played,
		s.last_played as last_played,
		s.song as song_name
    FROM
       songs as s
	)
SELECT * FROM TEMP;

SELECT * FROM song_profiles_view LIMIT 10;
