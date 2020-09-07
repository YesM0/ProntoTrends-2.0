-- CONNECTION: name=ProntoPro - Production Database
 SELECT
	YEAR(t.status_new_at),
	MONTH(t.status_new_at),
	r.name as 'Region_name',
	t2.name as 'tag_name',
	count(t.id)
FROM
	prontopro.ticket t
LEFT JOIN prontopro.tag t2 on
	t.tag_id = t2.id
LEFT JOIN prontopro.locality l on
	t.locality_id = l.id
LEFT JOIN prontopro.province p on
	l.province_id = p.id
LEFT JOIN prontopro.region r on
	p.region_id = r.id
WHERE
	t.status_new_at > '2018-12-31'
GROUP BY
	1,
	2,
	3,
	4