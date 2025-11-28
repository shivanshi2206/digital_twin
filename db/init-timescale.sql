
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50),
    timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    occupancy INT
);
SELECT create_hypertable('sensor_data', 'timestamp')
