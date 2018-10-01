--(1) Write MySQL query to find IPs that mode more than a certain number of requests for a given time period.
--Ex: Write SQL to find IPs that made more than 100 requests starting from 2017-01-01.13:00:00 to 2017-01-01.14:00:00.
select
        ip as ip,
        connectionCount as connectionCount
    from (
        select
               ip as ip,
               count(ip) as connectionCount
           from log_entries
           where entry_date >= ?
                and entry_date < ?
           group by (ip) ) AS counts
    where
    connectionCount > ? ;

select
        ip as ip,
        connectionCount as connectionCount
    from (
        select
               ip as ip,
               count(ip) as connectionCount
           from log_entries
           where entry_date >= STR_TO_DATE('2017-01-01.13:00:00', '%Y-%m-%d.%H:%i:%s')
                and entry_date < STR_TO_DATE('2017-01-01.14:00:00', '%Y-%m-%d.%H:%i:%s')
           group by (ip) ) AS counts
    where
    connectionCount > 100;

--(2) Write MySQL query to find requests made by a given IP.

select * from log_entries where ip = ? ;

select * from log_entries where ip = '192.168.228.188' ;