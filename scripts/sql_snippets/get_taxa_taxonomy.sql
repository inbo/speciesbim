SELECT * FROM taxonomy
{% if limit %}
LIMIT {{ limit }}
{% endif %};
