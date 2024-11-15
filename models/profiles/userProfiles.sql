CREATE
OR REPLACE VIEW userProfiles_view AS
WITH
	TEMP AS (
    SELECT 
        u.uid as distinct_id,
        u.username as name,
        u.date_joined as created,
		'since ' || CAST(STRFTIME('%Y', u.date_joined) AS TEXT) as email		
    FROM
       users as u
	)
SELECT * FROM TEMP;

SELECT * FROM userProfiles_view LIMIT 100;
