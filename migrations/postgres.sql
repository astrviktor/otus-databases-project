--CREATE DATABASE creator;
--CREATE USER "user" WITH ENCRYPTED PASSWORD 'password';
--GRANT ALL PRIVILEGES ON DATABASE creator TO "user";

CREATE SCHEMA IF NOT EXISTS creator;


CREATE TABLE IF NOT EXISTS creator.clients (
    msisdn bigint primary key,
    gender char(1),
    age smallint,
    income decimal(10,2),
    counter integer
);

CREATE INDEX clients_counter ON creator.clients (counter);

CREATE TABLE IF NOT EXISTS creator.segments (
    id uuid,
    msisdn bigint
);

-- CREATE INDEX segments_id ON creator.segments (id);
-- CREATE INDEX segments_msisdn ON creator.segments (msisdn);
CREATE INDEX segments_id_msisdn ON creator.segments (id, msisdn);

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

        INSERT INTO creator.clients (msisdn, gender, age, income, counter)
        VALUES (msisdn, gender, age, income, 0);
    END LOOP;
END;
$$;

-- CALL creator.create_clients(10000000);

CREATE OR REPLACE PROCEDURE creator.delete_clients()
LANGUAGE plpgsql AS
$$
BEGIN
    DELETE FROM creator.clients;
END;
$$;


CREATE OR REPLACE PROCEDURE creator.create_segment(idx uuid, size int)
LANGUAGE plpgsql AS
$$
BEGIN
    INSERT INTO creator.segments(id, msisdn)
    SELECT idx, msisdn
    FROM creator.clients
    ORDER BY counter
    LIMIT size;

--     UPDATE creator.clients
--     SET counter = counter + 1
--     WHERE msisdn IN (
--         SELECT msisdn
--         FROM creator.segments
--         WHERE id = idx);

END;
$$;


-- CREATE OR REPLACE FUNCTION creator.update_counter_trigger() RETURNS TRIGGER AS $$
-- BEGIN
--     UPDATE creator.clients
--     SET counter = counter + 1
--     WHERE msisdn = new.msisdn;
--
--     RETURN new;
-- END
-- $$ LANGUAGE plpgsql;
--
-- CREATE OR REPLACE TRIGGER counter_trigger BEFORE INSERT ON creator.segments
--     FOR EACH ROW EXECUTE PROCEDURE creator.update_counter_trigger();


-- CALL creator.create_segment('12345678-1234-5678-1234-567812345678',10000)


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

-- SELECT creator.create_segment(10000)