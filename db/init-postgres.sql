
CREATE TABLE analytics_data (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50),
    date DATE NOT NULL,
    avg_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    occupancy_rate DOUBLE PRECISION
)
