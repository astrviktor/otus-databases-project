CREATE DATABASE IF NOT EXISTS creator;


USE creator;


CREATE TABLE IF NOT EXISTS clients (
    msisdn bigint primary key,
    gender char(1),
    age tinyint,
    income decimal(10,2),
    counter int
) ENGINE = InnoDB;

CREATE INDEX clients_counter ON creator.clients (counter);


CREATE TABLE IF NOT EXISTS segments (
    id binary(16),
    msisdn bigint
) ENGINE = InnoDB;
# PARTITION BY KEY(id)
# PARTITIONS 10;

# CREATE INDEX segments_id ON creator.segments (id);
# CREATE INDEX segments_msisdn ON creator.segments (msisdn);
CREATE INDEX segments_id_msisdn ON creator.segments (id, msisdn);

DELIMITER $$
CREATE DEFINER=`user`@`%` PROCEDURE create_clients(size int)
BEGIN
    DECLARE msisdn int;
    DECLARE gender char(1);
    DECLARE age tinyint;
    DECLARE income decimal(10,2);
    DECLARE i int;

    SET i = 0;
    SET msisdn = 100000000;

    START TRANSACTION;

    loop_label: LOOP
        IF i >= size THEN
            LEAVE loop_label;
        END IF;

        SET msisdn = msisdn + 1;
        SET i = i + 1;

        CASE floor(rand()*3)
            WHEN 0 THEN SET gender = 'M';
            WHEN 1 THEN SET gender = 'F';
            ELSE SET gender = '';
        END CASE;

        SET age = floor(rand()*82 + 18);
        SET income = floor(rand()*90000 + 10000);

        INSERT INTO creator.clients(msisdn, gender, age, income, counter)
        VALUES (msisdn, gender, age, income, 0);
    END LOOP;

    COMMIT;
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER=`user`@`%` PROCEDURE delete_clients()
BEGIN
    START TRANSACTION;

    DELETE FROM creator.clients;

    COMMIT;
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER=`user`@`%` PROCEDURE create_segment(idx binary(16), size int)
BEGIN
    START TRANSACTION;

    INSERT INTO creator.segments(id, msisdn)
    SELECT idx, msisdn
    FROM creator.clients
    ORDER BY counter
    LIMIT size;

    UPDATE creator.clients
    SET counter = counter + 1
    WHERE msisdn IN (
        SELECT msisdn
        FROM creator.segments
        WHERE id = idx);

    COMMIT;
END$$
DELIMITER ;


# DELIMITER $$
# CREATE DEFINER=`user`@`%` TRIGGER update_creator_trigger AFTER INSERT ON creator.segments
# FOR EACH ROW
# BEGIN
#     UPDATE creator.clients
#     SET counter = counter + 1
#     WHERE msisdn = new.msisdn;
# END$$
# DELIMITER ;

# ERROR:  Error 1442 (HY000): Can't update table 'clients' in stored function/trigger because
#     it is already used by statement which invoked this stored function/trigger.



