CREATE
OR REPLACE VIEW venue_profiles_view AS
WITH
	TEMP AS (
    SELECT 
		-- required Mixpanel fields
        v.venueid as distinct_id,
        v.venuename as name,
		v.city || ', ' || v.state as email,
        
		v.city as city,
		v.country as country,
		v.state as region,
    FROM
       venues v
	)
SELECT
	*
FROM
	TEMP;

SELECT * FROM venue_profiles_view LIMIT 100;