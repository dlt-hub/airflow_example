

{{ config(materialized='table') }}


select *
from {{ source('raw', 'persons') }} as a
