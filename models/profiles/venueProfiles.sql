CREATE
OR REPLACE VIEW venueProfiles AS
WITH
	TEMP AS (
    SELECT 
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

SELECT * FROM venueProfiles LIMIT 100;