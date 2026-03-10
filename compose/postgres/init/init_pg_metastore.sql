-- Create metastore database
CREATE DATABASE metastore OWNER core;

-- Connect to metastore database
\c metastore

-- Grant necessary privileges
GRANT ALL PRIVILEGES ON SCHEMA public TO core;