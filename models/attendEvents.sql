CREATE OR REPLACE VIEW attend_events AS
    SELECT 
        a.uid as distinct_id,
        a.username as username,
        a.showdate as time,
        'attended show' as event,
        a.showid as show_id,
        a.venueid as venue_id
FROM
   attendance a
JOIN
   shows s
ON
   a.showid = s.showid