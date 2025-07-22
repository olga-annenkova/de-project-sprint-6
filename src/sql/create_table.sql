--основные события
CREATE TABLE STV202507096__STAGING.group_log (
    group_id INT NOT NULL,
    user_id INT NOT NULL,
    user_id_from INT,
    event VARCHAR(10) NOT NULL,
    datetime TIMESTAMP NOT NULL
)
ORDER BY group_id, user_id
SEGMENTED BY HASH(group_id) ALL NODES;

-- отклоненные события
CREATE TABLE STV202507096__STAGING.group_log_rejected (
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(10),
    datetime TIMESTAMP,
    rejection_reason VARCHAR(255)
)
ORDER BY datetime
SEGMENTED BY HASH(datetime) ALL NODES;


--расчет конверсии
CREATE TABLE STV202507096__DWH.groups_conversion (
    group_id INT PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    cnt_added_users INT NOT NULL,
    cnt_users_with_messages INT NOT NULL,
    conversion FLOAT NOT NULL
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES;
