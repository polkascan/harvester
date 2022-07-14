"""ETL procedures

Revision ID: 861750430061
Revises: 0d18f3ceaace
Create Date: 2022-07-06 19:43:47.592540

"""
from alembic import op
import sqlalchemy as sa
from app import settings

# revision identifiers, used by Alembic.
revision = '861750430061'
down_revision = '0d18f3ceaace'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(f"""
        CREATE DEFINER=`{settings.DB_USERNAME}`@`%` PROCEDURE `etl`(`block_start` INT(11), `block_end` INT(11), `update_status` INT(1))
        BEGIN
            DECLARE `idx` INT;
            SET `idx` = `block_start`;
            SET @update_status = `update_status`;
            label1: 
            WHILE `idx` <= `block_end` DO
                CALL `etl_range`(`idx`,`idx`,`update_status`);
                SET `idx` = `idx` + 1;
            END WHILE label1;
        END
                    """)

    op.execute(f"""
            CREATE DEFINER=`{settings.DB_USERNAME}`@`%` PROCEDURE `etl_range`(`block_start` INT(11), `block_end` INT(11), `update_status` INT(1))
            BEGIN
                ### GLOBAL SETTINGS ###
                SET @block_start = `block_start`;
                SET @block_end = `block_end`;
                SET @update_status = `update_status`;

                ### CALL OTHER STORED PROCEDURES ###
                CALL `etl_codec_block_timestamp`(`block_start`,`block_end`,`update_status`);
                
                # CALL `stored_procedure_02`();
                # CALL `stored_procedure_03`();

                ### UPDATE STATUS TABLE ###
                IF @update_status = 1 THEN
                    INSERT INTO `harvester_status` (`key`,`description`,`value`)(
                        SELECT
                            'PROCESS_ETL' AS	`key`,
                            'Max blocknumber of etl process' AS `description`,
                            CAST(@block_end AS JSON) AS `value`
                        LIMIT 1
                    ) ON DUPLICATE KEY UPDATE
                        `description` = VALUES(`description`),
                        `value` = VALUES(`value`)
                    ;
                END IF;
            END
                        """)

    op.execute(f"""
                CREATE DEFINER=`{settings.DB_USERNAME}`@`%` PROCEDURE `etl_codec_block_timestamp`(`block_start` INT(11), `block_end` INT(11), `update_status` INT(1))
                BEGIN
                    # GLOBAL SETTINGS
                    SET @block_start = `block_start`;
                    SET @block_end = `block_end`;
                    SET @update_status = `update_status`;
            
                    INSERT INTO `codec_block_timestamp` (
                                    `block_number`,
                                    `block_hash`,
                                    `timestamp`,
                                    `datetime`,
                                    `year`,
                                    `quarter`,
                                    `month`,
                                    `week`,
                                    `day`,
                                    `hour`,
                                    `minute`,
                                    `second`,
                                    `full_quarter`,
                                    `full_month`,
                                    `full_week`,
                                    `full_day`,
                                    `full_hour`,
                                    `full_minute`,
                                    `full_second`,
                                    `weekday`,
                                    `weekday_name`,
                                    `month_name`,
                                    `weekend`,
                                    `range10000`,
                                    `range100000`,
                                    `range1000000`,
                                    `complete`
                        )(
                                    SELECT
                                        `nbh`.`block_number` AS `block_number`,
                                        `nbh`.`hash` AS `block_hash`,
                                        FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")) AS `timetamp`,
                                        FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)) AS `datetime`,
                                        YEAR(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `year`,
                                        QUARTER(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `quarter`,
                                        MONTH(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `month`,
                                        (DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%u')+0) AS `week`,
                                        DAY(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `day`,
                                        HOUR(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `hour`,
                                        MINUTE(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `minute`,
                                        SECOND(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `second`,
                                        CONCAT(DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y'),QUARTER(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)))) AS `full_quarter`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%m') AS `full_month`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%u') AS `full_week`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%m%d') AS `full_day`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%m%d%H') AS `full_hour`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%m%d%H%i') AS `full_minute`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%Y%m%d%H%i%s') AS `full_second`,
                                        DAYOFWEEK(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) AS `weekday`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%W') AS `weekday_name`,
                                        DATE_FORMAT(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000)),'%M') AS `month_name`,
                                        CASE DAYOFWEEK(FROM_UNIXTIME(FLOOR(JSON_UNQUOTE(`cbex`.`data`->"$.call.call_args[0].value")/1000))) WHEN (1 OR 7) THEN 1 ELSE 0 END AS `weekend`,
                                        FLOOR(`nbh`.`block_number`/10000) AS `range10000`,
                                        FLOOR(`nbh`.`block_number`/100000) AS `range100000`,
                                        FLOOR(`nbh`.`block_number`/1000000) AS `range1000000`,
                                        1 AS `complete`
                                    FROM `node_block_header` AS `nbh`
                                    INNER JOIN `node_block_runtime` AS `nbr`
                                    ON `nbr`.`hash` = `nbh`.`hash` AND `nbr`.`block_number` >= @block_start AND	`nbr`.`block_number` <= @block_end
                                    INNER JOIN `codec_block_extrinsic` AS `cbex`
                                    ON `cbex`.`block_hash` = `nbh`.`hash` AND `cbex`.`block_number` >= @block_start AND	`cbex`.`block_number` <= @block_end AND `cbex`.`call_module` = 'Timestamp' AND `cbex`.`call_name` = 'set' AND `cbex`.`signed`=0
                                    WHERE `nbh`.`block_number` >= @block_start AND	`nbh`.`block_number` <= @block_end
                                ) ON DUPLICATE KEY UPDATE
                                    `block_hash` = VALUES(`block_hash`),
                                    `timestamp` = VALUES(`timestamp`),
                                    `datetime` = VALUES(`datetime`),
                                    `year` = VALUES(`year`),
                                    `quarter` = VALUES(`quarter`),
                                    `month` = VALUES(`month`),
                                    `week` = VALUES(`week`),
                                    `day` = VALUES(`day`),
                                    `hour` = VALUES(`hour`),
                                    `minute` = VALUES(`minute`),
                                    `second` = VALUES(`second`),
                                    `full_quarter` = VALUES(`full_quarter`),
                                    `full_month` = VALUES(`full_month`),
                                    `full_week` = VALUES(`full_week`),
                                    `full_day` = VALUES(`full_day`),
                                    `full_hour` = VALUES(`full_hour`),
                                    `full_minute` = VALUES(`full_minute`),
                                    `full_second` = VALUES(`full_second`),
                                    `weekday` = VALUES(`weekday`),
                                    `weekday_name` = VALUES(`weekday_name`),
                                    `month_name` = VALUES(`month_name`),
                                    `weekend` = VALUES(`weekend`),
                                    `range10000` = VALUES(`range10000`),
                                    `range100000` = VALUES(`range100000`),
                                    `range1000000` = VALUES(`range1000000`),
                                    `complete` = VALUES(`complete`)
                                ;
                
                                ### UPDATE STATUS TABLE ###
                                IF @update_status = 1 THEN
                                        INSERT INTO `harvester_status` (`key`,`description`,`value`)(
                                                SELECT
                                                        'PROCESS_ETL_DATETIME' AS	`key`,
                                                        'Max blocknumber of etl process' AS `description`,
                                                        CAST(@block_end AS JSON) AS `value`
                                                LIMIT 1
                                        ) ON DUPLICATE KEY UPDATE
                                                `description` = VALUES(`description`),
                                                `value` = VALUES(`value`)
                                        ;
                                END IF;
                
                    END
                            """)


def downgrade():
    op.execute("""DROP PROCEDURE IF EXISTS `etl`;""")
    op.execute("""DROP PROCEDURE IF EXISTS `etl_range`;""")
    op.execute("""DROP PROCEDURE IF EXISTS `etl_codec_block_timestamp`;""")

