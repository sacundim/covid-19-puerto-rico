{% docs bioportal_encounters %}

# Encounters and followup analysis

This table takes all the antigen and PCR tests (no serology) and does the following 
cleanup and enrichment:

1. Eliminates duplicate tests for the same patient on the same date. If any of the 
   tests on one day is positive, we classify the patient as a positive on that day.
   We call these "test encounters," a term used by the COVID Tracking Project. See: 
   https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess
2. Flags "followup" tests—tests such that the same patient had a positive test no 
   more than 90 days earlier. We use a three month cutoff following the Council of 
   State and Territorial Epidemiologists (CSTE)'s 2020 Interim Case Definition
   (Interim-20-ID-02, approved August 5, 2020), which recommends this criterion for 
   distinguishing new cases from previous ones for the same patient. See: 
   https://wwwn.cdc.gov/nndss/conditions/coronavirus-disease-2019-covid-19/case-definition/2020/08/05/

{% enddocs %}