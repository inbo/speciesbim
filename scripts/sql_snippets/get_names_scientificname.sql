SELECT * FROM scientificname
{% if demo %}
WHERE "scientificName" IN (
        'Elachista', -- no match to GBIF Backbone will be found
        'Triturus alpestris', -- synonym of Ichthyosaura alpestris
        'Fallopia japonica', -- exotic and synonym of Reynoutria japonica
        'Trentepholia' -- accepted genus
    )
    {% if unmatched_only %}
    AND "taxonomyId" is NULL
    {% endif %}
{% else %}
    {% if unmatched_only %}
    WHERE "taxonomyId" is NULL
    {% endif %}
{% endif %}
{% if limit %}
LIMIT {{ limit }}
{% endif %};
