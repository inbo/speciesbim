SELECT * FROM scientificname
WHERE "taxonomyId" is NULL
LIMIT {{ limit }};
