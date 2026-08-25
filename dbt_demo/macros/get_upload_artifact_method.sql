{#
  Workaround for elementary 0.16.x / dbt 1.8+:
  upload_artifacts_to_table.sql calls get_upload_artifact_method() without the
  `elementary.` package prefix, which fails with
  "'get_upload_artifact_method' is undefined" during on-run-end.
  Defining the macro in the root project makes the unqualified call resolve.
#}
{% macro get_upload_artifact_method(table_relation, metadata_hashes) %}
    {% if not elementary.get_config_var("cache_artifacts") %}
        {% do return({"type": "replace"}) %}
    {% endif %}

    {% set upload_artifacts_method = elementary.get_config_var("upload_artifacts_method") %}
    {% if upload_artifacts_method == 'diff' %}
        {% set metadata_hashes = metadata_hashes if metadata_hashes is not none else elementary.get_artifacts_hashes_for_model(table_relation.name) %}
        {% if metadata_hashes is not none %}
            {% do return({"type": "diff", "metadata_hashes": metadata_hashes}) %}
        {% else %}
            {% do return({"type": "replace"}) %}
        {% endif %}
    {% elif upload_artifacts_method == "replace" %}
        {% do return({"type": "replace"}) %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid var('upload_artifacts_method') provided.") %}
    {% endif %}
{% endmacro %}
