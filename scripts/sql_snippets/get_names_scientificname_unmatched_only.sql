SELECT * FROM scientificname
WHERE "taxonomyId" is NULL
{% if limit %}
LIMIT {{ limit }}
{% endif %};
