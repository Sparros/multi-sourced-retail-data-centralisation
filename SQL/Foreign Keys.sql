-- Create a foreign key constraint in orders_table referencing dim_date_times
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_date_times
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

-- Create a foreign key constraint in orders_table referencing dim_users
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

-- Create a foreign key constraint in orders_table referencing dim_card_details
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);
