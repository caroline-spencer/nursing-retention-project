-- SQL Queries for Registed Nurse Retention and Reengagement Project

-- Project goal:
    -- Using database of RN licensure data (NC Health Professions Data System Version 2), track RNs active status based on 2 year RN license renewal cycle to 
    -- determine retention or exit from the NC RN workforce. Group RNs based on retention or exit at end of 2 year RN license renewal cycle and analyze to 
    -- compare characteristics of those who were retained or exited. Create multiple analysis cohorts, available data covers years 2016 to 2023.

-- STEP 1: Create table for analysis cohort 2021-2023 RNs who had a license issued in 2021 (to track 2 year license renewal cycle)
CREATE TABLE rn_retention_reengagement.analysis_cohort_2021_2023
AS
-- common table expression
(WITH rn_lead_values
	AS
    -- common table expression
	(WITH rn_lead_window
		AS
        -- window function to get RN active status for next 2 years for each record
		(SELECT person_id, year, active_in_state_bool, 
    			LEAD(active_in_state_bool, 1) OVER (partition by person_id order by year) AS active_in_state_next,
    			LEAD(active_in_state_bool, 2) OVER (partition by person_id order by year) AS active_in_state_next_2
			FROM rn_retention_reengagement.rn)
	-- select 2021 records from previous window function result to track active status from 2021-2023
	SELECT person_id, active_in_state_next, active_in_state_next_2
		FROM rn_lead_window
		WHERE year = 2021)
-- merge tracked active status for 2021-2023 with all variables for RN records in years 2021-2023
SELECT rn_retention_reengagement.rn.*, rn_lead_values.active_in_state_next, rn_lead_values.active_in_state_next_2
	FROM rn_retention_reengagement.rn, rn_lead_values
    -- join main RN table with created lead_values table tracking active status 
	WHERE rn_retention_reengagement.rn.person_id = rn_lead_values.person_id 
  		and (rn_retention_reengagement.rn.year = 2021
            or rn_retention_reengagement.rn.year = 2022
            or rn_retention_reengagement.rn.year = 2023)
   		and rn_retention_reengagement.rn.person_id IN
        -- limit records to RNs who had a license issued in 2021
        (SELECT person_id 
			FROM rn_retention_reengagement.rn 
       		WHERE year = 2021 and license_issued_year = 2021 and active_in_state_bool = 1)
ORDER BY person_id, YEAR);

-- Step 2: Extract data from previously created table for analysis and visualization
-- RNs who failed to renew license at end of 2 year cycle (2023 or active_in_state_next_2) became inactive or 'exited' the workforce
SELECT *, 'inactive' as status
FROM rn_retention_reengagement.analysis_cohort_2021_2023
WHERE active_in_state_next_2 = 0

UNION ALL

-- RNs who renewed license at end of 2 year cycle (2023 or active_in_state_next_2) remained active or 'were retained' in the workforce
SELECT *, 'active' as status
FROM rn_retention_reengagement.analysis_cohort_2021_2023
WHERE active_in_state_next_2 = 1

-- Step 3: Use this code as a template to create additional analysis cohort datasets using data from other years
