CREATE OR REPLACE VIEW songProfiles AS
    SELECT 
        s.songid as distinct_id,
        s.song as name,
        s.artist as email,
		s.debut as debut,
		s.times_played as times_played,
		s.last_played as last_played        
    FROM
       songs s
    
