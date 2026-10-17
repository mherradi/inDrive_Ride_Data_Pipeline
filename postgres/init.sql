-- ============================================================================
-- 1. SCHEMA DEFINITIONS
-- ============================================================================

CREATE SCHEMA raw;      -- data as it arrived
CREATE SCHEMA staging;  -- cleaned data
CREATE SCHEMA mart;     -- business tables for analysts

-- ============================================================================
-- 1. RAW LAYER 
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS raw;

-- Stream target
CREATE TABLE IF NOT EXISTS raw.rides_stream (
    ride_id VARCHAR(100) PRIMARY KEY,
    driver_id INT,
    pickup_zone_id INT,
    dropoff_zone_id INT,
    distance_km NUMERIC,
    fare_amount NUMERIC,
    requested_at DATE
);

-- Batch targets
CREATE TABLE IF NOT EXISTS raw.zones (
    zone_id INT PRIMARY KEY,
    zone_name VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.drivers (
    driver_id INT PRIMARY KEY,
    driver_name VARCHAR(100),
    phone VARCHAR(10),
    city VARCHAR(100),
    rating NUMERIC,
    joined_date DATE
);

CREATE TABLE IF NOT EXISTS raw.vehicles (
    vehicle_id INT PRIMARY KEY,
    driver_id INT,
    make VARCHAR(50),
    model VARCHAR(50),
    plate_number VARCHAR(30),
    vehicle_type VARCHAR(30)
);
