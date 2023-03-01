

{{ config(materialized='table') }}

-- get the first non null phone and email of the person in the table.

SELECT p.*, pe.value as email, pp.value as phone
FROM {{ source('raw', 'persons') }} as p
left join {{ source('raw', 'persons__email') }} as pe
on p._dlt_id=pe._dlt_parent_id and pe._dlt_list_idx = 0 and pe.value!=''
left join {{ source('raw', 'persons__phone') }} as pp
on p._dlt_id=pe._dlt_parent_id and pp._dlt_list_idx = 0 and pp.value!=''


