SELECT * FROM scientificname
{% if demo %}
WHERE "scientificName" IN (
        'Mellitiosporium pteridium', -- no matchc to GBIF Backbone, to be corrected as
        'Mellitiosporium pteridinum', -- correct version
        'Rana ridibunda', -- Synonym of Pelophylax ridibundus
        'Fallopia japonica', -- Exotic and synonym of Reynoutria japonica
        'Fallopia' -- parent of Fallopia synonym of Reynoutria
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
