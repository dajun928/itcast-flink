
-- 维度数据，用户信息表

CREATE TABLE IF NOT EXISTS db_flink.tbl_users_dim(
     user_id VARCHAR(255) PRIMARY KEY,
     user_name VARCHAR(255),
     id_card VARCHAR(255),
     mobile VARCHAR(255),
     address VARCHAR(2000),
     gender VARCHAR(20)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


INSERT INTO db_flink.tbl_users_dim(user_id, user_name, id_card, mobile, address, gender)
VALUES (?, ?, ?, ?, ?, ?) ;



