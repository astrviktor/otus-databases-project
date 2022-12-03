--CREATE DATABASE creator;
--CREATE USER "user" WITH ENCRYPTED PASSWORD 'password';
--GRANT ALL PRIVILEGES ON DATABASE creator TO "user";

CREATE SCHEMA IF NOT EXISTS creator;


CREATE TABLE IF NOT EXISTS creator.clients (
    msisdn int4,
    gender char(1),
    age smallint,
    income decimal(10,2)
);


CREATE TABLE IF NOT EXISTS creator.segments (
    id uuid,
    msisdn int4
);


CREATE OR REPLACE PROCEDURE creator.create_clients(size int)
LANGUAGE plpgsql AS
$$
DECLARE
    msisdn int4;
    gender char(1);
    age smallint;
    income decimal(10,2);
BEGIN
    msisdn := 100000000;

    FOR i IN 1..size LOOP
        msisdn := msisdn + 1;

        CASE (random()*2)::int
            WHEN 0 THEN gender := 'M';
            WHEN 1 THEN gender := 'F';
            ELSE gender := '';
        END CASE;

        age := (random()*82 + 18)::int;
        income := (random()*90000 + 10000)::int;

        INSERT INTO creator.clients (msisdn, gender, age, income)
        VALUES (msisdn, gender, age, income);
    END LOOP;
END;
$$;


CREATE OR REPLACE PROCEDURE creator.delete_clients()
LANGUAGE plpgsql AS
$$
BEGIN
    DELETE FROM clients;
END;
$$;

CREATE OR REPLACE PROCEDURE creator.create_segment(id uuid, size int)
LANGUAGE plpgsql AS
$$
BEGIN
    INSERT INTO creator.segments(id, msisdn)
    SELECT id, msisdn
    FROM creator.clients
    LIMIT size;
END;
$$;



-- CREATE OR REPLACE FUNCTION creator.create_segment(size int)
-- RETURNS uuid
-- LANGUAGE plpgsql AS
-- $$
-- DECLARE
--     id uuid;
-- BEGIN
--     id := gen_random_uuid();
--
--     INSERT INTO creator.segments(id, msisdn)
--     SELECT id, msisdn
--     FROM creator.clients
--     LIMIT size;
--
--     return id;
-- END;
-- $$;