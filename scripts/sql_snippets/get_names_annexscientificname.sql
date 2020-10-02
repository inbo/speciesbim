SELECT * FROM annexscientificname
WHERE "scientificName" != ''
{% if unmatched_only %}
     AND "scientificnameId" is NULL
{% endif %}
{% if limit %}
LIMIT {{ limit }}
{% endif %};
