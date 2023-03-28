CREATE DATABASE rock;
create table if not exists rock (id bigserial primary key, name varchar(6));
INSERT INTO rock VALUES(1, 'rock');
GRANT ALL PRIVILEGES ON DATABASE new_db TO admin1;
ALTER DATABASE rock OWNER TO admin1;