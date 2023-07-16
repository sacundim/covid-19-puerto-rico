SELECT *
FROM {{ ref('hhs_hospital_history_all') }}
WHERE state = 'PR'