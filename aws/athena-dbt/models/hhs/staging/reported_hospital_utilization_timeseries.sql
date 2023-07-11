-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Filtering to Puerto Rico
--

SELECT *
FROM {{ ref('reported_hospital_utilization_timeseries_all') }}
WHERE state = 'PR'