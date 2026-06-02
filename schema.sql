-- PM Monitoring Dashboard Database Schema

-- Users table
CREATE TABLE IF NOT EXISTS dust_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Data sources table (MQTT brokers or API endpoints)
CREATE TABLE IF NOT EXISTS dust_data_sources (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(10) NOT NULL CHECK (source_type IN ('mqtt', 'api')),
    broker_url TEXT,
    api_device_id TEXT,
    username TEXT,
    password TEXT,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_type, broker_url, api_device_id)
);

-- Devices table
CREATE TABLE IF NOT EXISTS dust_devices (
    id SERIAL PRIMARY KEY,
    deviceid VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    user_id INTEGER REFERENCES dust_users(id) ON DELETE CASCADE,
    data_source_id INTEGER REFERENCES dust_data_sources(id) ON DELETE CASCADE,
    has_relay BOOLEAN DEFAULT FALSE,
    location VARCHAR(255),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (deviceid, data_source_id)
);

-- Sensor data table
CREATE TABLE IF NOT EXISTS dust_sensor_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    device_id INTEGER REFERENCES dust_devices(id) ON DELETE CASCADE,
    data_source_id INTEGER REFERENCES dust_data_sources(id) ON DELETE CASCADE,
    pm1 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    pm4 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    tsp DOUBLE PRECISION
);

-- Extended sensor data table (for advanced devices)
CREATE TABLE IF NOT EXISTS dust_extended_data (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES dust_devices(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature_c DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    voc_ppb DOUBLE PRECISION,
    no2_ppb DOUBLE PRECISION,
    noise_db DOUBLE PRECISION,
    pm1 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    pm4 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    tsp_um DOUBLE PRECISION,
    gps_lat DOUBLE PRECISION,
    gps_lon DOUBLE PRECISION,
    gps_alt_m DOUBLE PRECISION,
    gps_speed_kmh DOUBLE PRECISION,
    cloud_cover_percent DOUBLE PRECISION,
    raw_payload TEXT
);

-- Thresholds table
CREATE TABLE IF NOT EXISTS dust_thresholds (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES dust_devices(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    pm1 DOUBLE PRECISION DEFAULT 50.0,
    pm2_5 DOUBLE PRECISION DEFAULT 75.0,
    pm4 DOUBLE PRECISION DEFAULT 100.0,
    pm10 DOUBLE PRECISION DEFAULT 150.0,
    tsp DOUBLE PRECISION DEFAULT 200.0,
    averaging_window INTEGER DEFAULT 15
);

-- Device alerts table
CREATE TABLE IF NOT EXISTS dust_device_alerts (
    id SERIAL PRIMARY KEY,
    device_id INTEGER REFERENCES dust_devices(id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    threshold_value DOUBLE PRECISION,
    measured_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sensor_data_device_timestamp ON dust_sensor_data(device_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_data_timestamp ON dust_sensor_data(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_extended_data_device_timestamp ON dust_extended_data(device_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_thresholds_device_timestamp ON dust_thresholds(device_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_device_created ON dust_device_alerts(device_id, created_at DESC);
