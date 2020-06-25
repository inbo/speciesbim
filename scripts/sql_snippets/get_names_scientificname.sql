SELECT * FROM scientificname
{% if limit %}
LIMIT {{ limit }}
{% endif %};
