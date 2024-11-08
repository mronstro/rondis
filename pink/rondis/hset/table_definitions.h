#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef HSET_TABLE_DEFINITIONS_H
#define HSET_TABLE_DEFINITIONS_H
/*
    NdbRecords are used for serialization. They map columns of a table to fields in a struct.
    For each table we interact with, we define:
    - one NdbRecord defining the columns to filter the row we want to read
    - one NdbRecord defining the columns we want to fetch
*/

/*
    KEY TABLE
*/
#define MAX_HSET_KEY_VALUE_LEN 3000
#define KEY_FIELD_TABLE_NAME "hset_keys"
#define KEY_FIELD_TABLE_COL_redis_key "redis_key"
#define KEY_FIELD_TABLE_COL_redis_key_id "redis_key_id"

extern NdbRecord *pk_hset_key_record;
extern NdbRecord *entire_hset_key_record;

struct hset_key_table
{
    char redis_key[MAX_KEY_VALUE_LEN + 2];
    Uint64 redis_key_id;
};

/*
    FIELD TABLE
*/

#define FIELD_TABLE_NAME "hset_fields"
#define MAX_FIELD_VALUE_LEN 3000
#define INLINE_FIELD_VALUE_LEN 26500

int init_field_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_field_record;
extern NdbRecord *entire_field_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the key_table struct.
*/
#define FIELD_TABLE_COL_redis_key_id "redis_key_id"
#define FIELD_TABLE_COL_redis_field "redis_field"
#define FIELD_TABLE_COL_field_key "field_key"
#define FIELD_TABLE_COL_expiry_date "expiry_date"
#define FIELD_TABLE_COL_value_data_type "value_data_type"
#define FIELD_TABLE_COL_tot_value_len "tot_value_len"
#define FIELD_TABLE_COL_num_rows "num_rows"
#define FIELD_TABLE_COL_value_start "value_start"

struct field_table
{
    Uint32 null_bits;
    Uint64 redis_key_id;
    char redis_key[MAX_KEY_VALUE_LEN + 2];
    Uint64 field_key;
    Uint32 expiry_date;
    Uint32 value_data_type;
    Uint32 tot_value_len;
    // Technically implicit
    Uint32 num_rows;
    char value_start[INLINE_VALUE_LEN + 2];
};

/*
    VALUE TABLE
*/

#define FIELD_VALUE_TABLE_NAME "string_values"
#define FIELD_EXTENSION_VALUE_LEN 29500

int init_hset_value_records(NdbDictionary::Dictionary *dict);

extern NdbRecord *pk_hset_value_record;
extern NdbRecord *entire_hset_value_record;

/*
    Doing this instead of reflection; Keep these the same
    as the field names in the value_table struct.
*/
#define HSET_VALUE_TABLE_COL_field_key "field_key"
#define HSET_VALUE_TABLE_COL_ordinal "ordinal"
#define HSET_VALUE_TABLE_COL_value "value"

struct hset_value_table
{
    Uint64 field_key;
    Uint32 ordinal;
    char value[EXTENSION_VALUE_LEN + 2];
};

/*
    SHARED/EXPORT
*/

int init_hset_record(NdbDictionary::Dictionary *dict,
                     const NdbDictionary::Table *tab,
                     std::map<const NdbDictionary::Column *,
                     std::pair<size_t, int>> column_info_map,
                     NdbRecord *&record);

int init_hset_records(NdbDictionary::Dictionary *dict);
#endif
