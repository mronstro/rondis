#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "../common.h"
#include "table_definitions.h"
#include "commands.h"

/**
 * Create NdbRecord's for all table accesses, they can be reused
 * for all Ndb objects.
 */

int init_hset_key_records(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable(HSET_KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting Ndb table %s\n", HSET_KEY_TABLE_NAME);
        return -1;
    }
    const NdbDictionary::Column *redis_key_col = tab->getColumn(HSET_KEY_TABLE_COL_redis_key);
    const NdbDictionary::Column *redis_key_id_col = tab->getColumn(HSET_KEY_TABLE_COL_redis_key_id);
    if (redis_key_col == nullptr ||
        redis_key_id_col == nullptr)
    {
        printf("Failed getting Ndb columns for table %s\n", HSET_KEY_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> pk_lookup_column_map = {
        {redis_key_col, {offsetof(struct hset_key_table, redis_key), 0}},

    if (init_record(dict, tab, pk_lookup_column_map, pk_hset_key_record) != 0)
    {
        printf("Failed creating pk-lookup record for table %s\n", HSET_KEY_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> read_all_column_map = {
        {redis_key_col, {offsetof(struct hset_key_table, redis_key), 0}},
        {redis_key_id_col, {offsetof(struct hset_key_table, redis_key_id), 0}},

    if (init_record(dict, tab, read_all_column_map, entire_hset_key_record) != 0)
    {
        printf("Failed creating read-all cols record for table %s\n", HSET_KEY_TABLE_NAME);
        return -1;
    }
    return 0;
}

int init_field_records(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable(FIELD_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting Ndb table %s\n", FIELD_TABLE_NAME);
        return -1;
    }
    const NdbDictionary::Column *redis_key_col = tab->getColumn(FIELD_TABLE_COL_redis_key);
    const NdbDictionary::Column *redis_key_id_col = tab->getColumn(FIELD_TABLE_COL_redis_key_id);
    const NdbDictionary::Column *rondb_key_col = tab->getColumn(FIELD_TABLE_COL_rondb_key);
    const NdbDictionary::Column *expiry_date_col = tab->getColumn(FIELD_TABLE_COL_expiry_date);
    const NdbDictionary::Column *value_start_col = tab->getColumn(FIELD_TABLE_COL_value_start);
    const NdbDictionary::Column *tot_value_len_col = tab->getColumn(FIELD_TABLE_COL_tot_value_len);
    const NdbDictionary::Column *num_rows_col = tab->getColumn(FIELD_TABLE_COL_num_rows);
    const NdbDictionary::Column *value_data_type_col = tab->getColumn(FIELD_TABLE_COL_value_data_type);
    if (redis_key_col == nullptr ||
        redis_key_id_col == nullptr ||
        rondb_key_col == nullptr ||
        expiry_date_col == nullptr ||
        value_start_col == nullptr ||
        tot_value_len_col == nullptr ||
        num_rows_col == nullptr ||
        value_data_type_col == nullptr)
    {
        printf("Failed getting Ndb columns for table %s\n", FIELD_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> pk_lookup_column_map = {
        {redis_key_col, {offsetof(struct field_table, redis_key), 0}},
        {redis_key_id_col, {offsetof(struct field_table, redis_key_id), 0}},
    };
    if (init_record(dict, tab, pk_lookup_column_map, pk_field_record) != 0)
    {
        printf("Failed creating pk-lookup record for table %s\n", FIELD_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> read_all_column_map = {
        {redis_key_col, {offsetof(struct field_table, redis_key), 0}},
        {redis_key_id_col, {offsetof(struct field_table, redis_key_id), 0}},
        {rondb_key_col, {offsetof(struct field_table, rondb_key), 0}},
        {expiry_date_col, {offsetof(struct field_table, expiry_date), 1}},
        {value_start_col, {offsetof(struct field_table, value_start), 0}},
        {tot_value_len_col, {offsetof(struct field_table, tot_value_len), 0}},
        {num_rows_col, {offsetof(struct field_table, num_rows), 0}},
        {value_data_type_col, {offsetof(struct field_table, value_data_type), 0}}
    };
    if (init_record(dict, tab, read_all_column_map, entire_field_record) != 0)
    {
        printf("Failed creating read-all cols record for table %s\n", FIELD_TABLE_NAME);
        return -1;
    }
    return 0;
}

int init_hset_value_records(NdbDictionary::Dictionary *dict)
{
    const NdbDictionary::Table *tab = dict->getTable(HSET_VALUE_TABLE_NAME);
    if (tab == nullptr)
    {
        printf("Failed getting Ndb table %s\n", HSET_VALUE_TABLE_NAME);
        return -1;
    }
    const NdbDictionary::Column *field_key_col = tab->getColumn(HSET_VALUE_TABLE_COL_field_key);
    const NdbDictionary::Column *ordinal_col = tab->getColumn(HSET_VALUE_TABLE_COL_ordinal);
    const NdbDictionary::Column *value_col = tab->getColumn(HSET_VALUE_TABLE_COL_value);
    if (field_key_col == nullptr ||
        ordinal_col == nullptr ||
        value_col == nullptr)
    {
        printf("Failed getting Ndb columns for table %s\n", HSET_VALUE_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> pk_lookup_column_map = {
        {field_key_col, {offsetof(struct hset_value_table, field_key), 0}},
        {ordinal_col, {offsetof(struct hset_value_table, ordinal), 0}}};
    if (init_record(dict, tab, pk_lookup_column_map, pk_hset_value_record) != 0)
    {
        printf("Failed creating pk-lookup record for table %s\n", HSET_VALUE_TABLE_NAME);
        return -1;
    }
    std::map<const NdbDictionary::Column *, std::pair<size_t, int>> read_all_column_map = {
        {field_key_col, {offsetof(struct hset_value_table, field_key), 0}},
        {ordinal_col, {offsetof(struct hset_value_table, ordinal), 0}},
        {value_col, {offsetof(struct hset_value_table, value), 0}}};
    if (init_record(dict, tab, read_all_column_map, entire_hset_value_record) != 0)
    {
        printf("Failed creating read-all cols record for table %s\n", HSET_VALUE_TABLE_NAME);
        return -1;
    }
    return 0;
}

int init_record(NdbDictionary::Dictionary *dict,
                const NdbDictionary::Table *tab,
                std::map<const NdbDictionary::Column *, std::pair<size_t, int>> column_info_map,
                NdbRecord *&record)
{
    NdbDictionary::RecordSpecification col_specs[column_info_map.size()];
    int i = 0;
    for (const auto &entry : column_info_map)
    {
        col_specs[i].column = entry.first;
        col_specs[i].offset = entry.second.first;
        col_specs[i].nullbit_byte_offset = 0;
        col_specs[i].nullbit_bit_in_byte = entry.second.second;
        ++i;
    }
    record = dict->createRecord(tab,
                                col_specs,
                                column_info_map.size(),
                                sizeof(col_specs[0]));

    return (record == nullptr) ? -1 : 0;
}

int init_hset_records(NdbDictionary::Dictionary *dict)
{
    int res = init_hset_key_records(dict);
    if (res != 0)
    {
        return res;
    }
    res = init_field_records(dict);
    if (res != 0)
    {
        return res;
    }
    return init_hset_value_records(dict);
}
