CREATE TABLE hset_fields(
    redis_key_id BIGINT UNSIGNED NOT NULL,
    -- Redis actually supports a max field size of 512MiB,
    -- but we choose not to support that here
    redis_field VARBINARY(3000) NOT NULL,
    -- This is to save space when referencing the field in the value table
    field_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
    -- TODO: Replace with Enum below
    value_data_type INT UNSIGNED NOT NULL,
    -- value_data_type ENUM('string', 'number', 'binary_string'),
    -- Max 512MiB --> 512 * 1,048,576 bytes = 536,870,912 characters
    -- --> To describe the length, one needs at least UINT (4,294,967,295)
    tot_value_len INT UNSIGNED NOT NULL,
    -- Technically implicit
    num_rows INT UNSIGNED NOT NULL,
    value_start VARBINARY(26500) NOT NULL,
    -- Redis supports get/set of seconds/milliseconds
    expiry_date INT UNSIGNED,
    -- Easier to sort and delete keys this way
    KEY expiry_index(expiry_date),
    PRIMARY KEY (redis_key_id, field_key),
    UNIQUE KEY (field_key) USING HASH
) ENGINE NDB -- Each CHAR will use 1 byte
CHARSET = latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8";
