ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(255);
ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(255);
ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(255);
ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(255);
ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
SELECT * FROM dim_date_times;