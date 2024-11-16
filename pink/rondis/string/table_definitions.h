#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef STRING_TABLE_DEFINITIONS_H
#define STRING_TABLE_DEFINITIONS_H

/*
    NdbRecords are used for serialization. They map columns of a table to fields in a struct.
    For each table we interact with, we define:
    - one NdbRecord defining the columns to filter the row we want to read
    - one NdbRecord defining the columns we want to fetch
*/

#define MAX_PARALLEL_READ_KEY_OPS 100

#define MAX_VALUES_TO_WRITE 4
#define MAX_KEY_VALUE_LEN 3000
#define STRING_REDIS_KEY_ID 0

/*
    HSET KEY TABLE
*/
#define HSET_KEY_TABLE_NAME "hset_keys"

int init_hset_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_hset_key_record;
extern NdbRecord *entire_hset_key_record;

#define HSET_KEY_TABLE_COL_redis_key "redis_key"
#define HSET_KEY_TABLE_COL_redis_key_id "redis_key_id"

struct hset_key_table
{
    Uint64 redis_key_id;
    char redis_key[MAX_KEY_VALUE_LEN + 2];
};

/*
    KEY AND FIELD TABLE
*/

#define KEY_TABLE_NAME "string_keys"
#define INLINE_VALUE_LEN 4096

int init_key_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_key_record;
extern NdbRecord *entire_key_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the key_table struct.
*/
#define KEY_TABLE_COL_redis_key_id "redis_key_id"
#define KEY_TABLE_COL_redis_key "redis_key"
#define KEY_TABLE_COL_rondb_key "rondb_key"
#define KEY_TABLE_COL_expiry_date "expiry_date"
#define KEY_TABLE_COL_value_data_type "value_data_type"
#define KEY_TABLE_COL_tot_value_len "tot_value_len"
#define KEY_TABLE_COL_num_rows "num_rows"
#define KEY_TABLE_COL_value_start "value_start"

struct key_table
{
    Uint32 null_bits;
    Uint64 redis_key_id;
    Uint64 rondb_key;
    Uint32 expiry_date;
    Uint32 value_data_type;
    Uint32 tot_value_len;
    // Technically implicit
    Uint32 num_rows;
    char redis_key[MAX_KEY_VALUE_LEN + 4];
    char value_start[INLINE_VALUE_LEN + 4];
};

/*
    VALUE TABLE
*/

#define VALUE_TABLE_NAME "string_values"
#define EXTENSION_VALUE_LEN 29500

int init_value_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_value_record;
extern NdbRecord *entire_value_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the value_table struct.
*/
#define VALUE_TABLE_COL_rondb_key "rondb_key"
#define VALUE_TABLE_COL_ordinal "ordinal"
#define VALUE_TABLE_COL_value "value"

struct value_table
{
    Uint64 rondb_key;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN + 2];
};

/*
    SHARED/EXPORT
*/

int init_record(NdbDictionary::Dictionary *dict,
                const NdbDictionary::Table *tab,
                std::map<const NdbDictionary::Column *, std::pair<size_t, int>> column_info_map,
                NdbRecord *&record);

int init_string_records(NdbDictionary::Dictionary *dict);

enum KeyState {
    /* m_read_value_size undefined */
    NotCompleted = 0,
    /* Use m_error_code */
    CompletedFailed = 1,
    /* Use m_read_value_size */
    CompletedSuccess = 2,
    /* Use m_num_rows */
    MultiRow = 3,
    /* Use m_num_rows */
    CompletedMultiRow = 4
};

struct GetControl;
struct KeyStorage {
    struct GetControl *m_get_ctrl;
    NdbTransaction *m_trans;
    const char *m_key_str;
    Uint32 m_key_len;
    char m_header_buf[20];
    Uint32 m_header_len;
    Uint32 m_num_rows;
    union {
        Uint32 m_read_value_size;
        Uint32 m_error_code;
    };
    enum KeyState m_key_state;
    struct key_table m_key_row;
};

class Ndb;
struct GetControl {
    Ndb *m_ndb;
    struct KeyStorage *m_key_store;
    Uint32 m_num_transactions;
    Uint32 m_num_keys_requested;
    Uint32 m_num_keys_outstanding;
    Uint32 m_num_keys_completed_first_pass;
    Uint32 m_num_keys_multi_rows;
    Uint32 m_num_keys_multi_rows_completed;
    Uint32 m_num_keys_failed;
    Uint32 m_error_code;
};
#endif
