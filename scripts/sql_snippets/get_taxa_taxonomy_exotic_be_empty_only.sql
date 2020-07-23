SELECT * FROM taxonomy
WHERE "exotic_be" is NULL
{% if limit %}
LIMIT {{ limit }}
{% endif %};
