-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS archive WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a customer table
CREATE TABLE IF NOT EXISTS archive.customer (
    country TEXT,
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    company TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    phone TEXT,
    fax TEXT,
    email TEXT,
    support_rep TEXT,
    PRIMARY KEY (country, customer_id)
);

-- Create a track table
CREATE TABLE IF NOT EXISTS archive.track (
    album TEXT,
    track_id INT,
    name TEXT,
    media_type TEXT,
    genre TEXT,
    composer TEXT,
    milliseconds INT,
    bytes INT,
    unit_price DECIMAL,
    PRIMARY KEY (album, track_id)
);