SELECT * FROM annexscientificname
WHERE "scientificName" != ''
{% if unmatched_only %}
     AND "scientificNameId" is NULL
{% endif %}
{% if limit %}
LIMIT {{ limit }}
{% endif %};
