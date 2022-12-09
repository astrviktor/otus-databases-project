CREATE DATABASE IF NOT EXISTS creator;


USE creator;


CREATE TABLE IF NOT EXISTS clients (
    msisdn int,
    gender char(1),
    age tinyint,
    income decimal(10,2)
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS segments (
    id binary(16),
    msisdn int
) ENGINE=InnoDB;


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

        INSERT INTO creator.clients(msisdn, gender, age, income)
        VALUES (msisdn, gender, age, income);
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
CREATE DEFINER=`user`@`%` PROCEDURE create_segment(id binary(16), size int)
BEGIN
    START TRANSACTION;

    INSERT INTO creator.segments(id, msisdn)
    SELECT id, msisdn
    FROM creator.clients
    LIMIT size;

COMMIT;
END$$
DELIMITER ;

