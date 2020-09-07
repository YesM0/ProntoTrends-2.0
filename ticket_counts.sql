-- CONNECTION: name=ProntoPro - Production Database
 SELECT
	tg.id as 'tag_id',
	tg.name as 'tag_name',
	DATE_FORMAT(t.status_new_at, "%Y-%m-%d") as 'date',
	r.name,
	COUNT(t.id) as 'No_of_tickets'
FROM
	prontopro.ticket t
LEFT JOIN prontopro.tag tg on
	t.tag_id = tg.id
LEFT JOIN prontopro.locality l on
	t.locality_id = l.id
LEFT JOIN prontopro.province p on
	l.province_id = p.id
LEFT JOIN prontopro.region r on
	p.region_id = r.id
WHERE
	t.status_new_at >= '2018-01-01' and tg.name in ('Abito da sposa','Affitto location matrimonio','Allestimenti e decorazioni per matrimonio','Animazione per matrimonio','Catering matrimoni','Dj per matrimoni','Fiorista matrimonio','Fotografo di matrimonio','Musica per matrimonio','Organizzatore di matrimoni','Parrucchiere matrimonio','Torta nuziale','Trucco sposa','Video matrimonio','Wedding planner')
GROUP BY
	1,
	2,
	3,
	4