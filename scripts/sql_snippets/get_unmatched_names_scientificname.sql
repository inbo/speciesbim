SELECT * FROM scientificname 
WHERE taxonomyId IS NULL
LIMIT {{ limit }};
