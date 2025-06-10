/*USE master;
GO
DROP DATABASE IF EXISTS SUPPORT;
GO;
CREATE DATABASE SUPPORT;
GO;
USE SUPPORT;
GO;
CREATE SCHEMA service AUTHORIZATION dbo;
GO
CREATE TABLE dbo.target_table
(
    [key]   NVARCHAR(MAX),
    [value] NVARCHAR(MAX)
);

  CREATE TABLE order_table (
    order_id NVARCHAR(64),
    customer_id NVARCHAR(64),
    order_date NVARCHAR(32),
    delivery_date NVARCHAR(32),
    status NVARCHAR(32),
    total_amount FLOAT,
    currency NVARCHAR(8),
    item_count INT,
    shipping_address NVARCHAR(MAX),
    billing_address NVARCHAR(MAX),
    shipping_zip NVARCHAR(16),
    billing_zip NVARCHAR(16),
    shipping_city NVARCHAR(64),
    billing_city NVARCHAR(64),
    shipping_country NVARCHAR(64),
    billing_country NVARCHAR(64),
    payment_method NVARCHAR(32),
    card_last_digits NVARCHAR(8),
    card_expiry NVARCHAR(16),
    ip_address NVARCHAR(64),
    user_agent NVARCHAR(MAX),
    campaign_id NVARCHAR(64),
    referrer_url NVARCHAR(MAX),
    device_type NVARCHAR(32),
    browser NVARCHAR(32),
    os NVARCHAR(32),
    coupon_code NVARCHAR(32),
    discount_amount FLOAT,
    loyalty_points_used INT,
    gift_wrap BIT,
    special_instructions NVARCHAR(MAX)
);



  */
