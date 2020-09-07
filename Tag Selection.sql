-- CONNECTION: name=ProntoPro - Production Database
# SET @kw1 = 'Boda' COLLATE utf8_unicode_ci, @kw2 = 'Matrimonio' COLLATE utf8_unicode_ci, @kw3 = 'Novi' COLLATE utf8_unicode_ci, @kw4 = 'Marid' COLLATE utf8_unicode_ci, @kw5 = 'Novio' COLLATE utf8_unicode_ci;
# SET @kw1 = 'Cérémonie' COLLATE utf8_unicode_ci, @kw2 = 'Marié' COLLATE utf8_unicode_ci, @kw3 = 'Noce' COLLATE utf8_unicode_ci, @kw4 = 'Épouser' COLLATE utf8_unicode_ci, @kw5 = 'Épouser' COLLATE utf8_unicode_ci;
SET @kw1 = 'Wedding' COLLATE utf8_unicode_ci, @kw2 = 'Hochzeit' COLLATE utf8_unicode_ci, @kw3 = 'Br[äa]ut' COLLATE utf8_unicode_ci, @kw4 = 'Trauung' COLLATE utf8_unicode_ci, @kw5 = 'Heirat' COLLATE utf8_unicode_ci;
SELECT
	t.id,
	t.name,
	s.name,
	b.name,
	b.elite_keywords,
	b.top_keywords
FROM
	prontopro_de.tag t
RIGHT OUTER JOIN prontopro_de.service s on
	s.id = t.service_id
LEFT OUTER JOIN prontopro_de.business_service bs on
	s.id = bs.service_id
LEFT OUTER JOIN prontopro_de.business b on
	bs.business_id = b.id
WHERE
	t.name like CONCAT('%', @kw1, '%')
	or t.name like CONCAT('%', @kw2, '%')
	or t.name like CONCAT('%', @kw3, '%')
	or t.name like CONCAT('%', @kw4, '%')
	or t.name like CONCAT('%', @kw5, '%')
	or b.name like CONCAT('%', @kw1, '%')
	or b.name like CONCAT('%', @kw2, '%')
	or b.name like CONCAT('%', @kw3, '%')
	or b.name like CONCAT('%', @kw4, '%')
	or b.name like CONCAT('%', @kw5, '%')
	or b.elite_keywords like CONCAT('%', @kw1, '%')
	or b.elite_keywords like CONCAT('%', @kw2, '%')
	or b.elite_keywords like CONCAT('%', @kw3, '%')
	or b.elite_keywords like CONCAT('%', @kw4, '%')
	or b.elite_keywords like CONCAT('%', @kw5, '%')
	or b.top_keywords like CONCAT('%', @kw1, '%')
	or b.top_keywords like CONCAT('%', @kw2, '%')
	or b.top_keywords like CONCAT('%', @kw3, '%')
	or b.top_keywords like CONCAT('%', @kw4, '%')
	or b.top_keywords like CONCAT('%', @kw5, '%')
	or s.name like CONCAT('%', @kw1, '%')
	or s.name like CONCAT('%', @kw2, '%')
	or s.name like CONCAT('%', @kw3, '%')
	or s.name like CONCAT('%', @kw4, '%')
	or s.name like CONCAT('%', @kw5, '%')