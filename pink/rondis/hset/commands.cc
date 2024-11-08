#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "db_operations.h"
#include "commands.h"
#include "../common.h"
#include "table_definitions.h"

bool setup_transaction(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response,
    struct field_table *field_row,
    const char *field_str,
    Uint32 field_len,
    const NdbDictionary::Dictionary **ret_dict,
    const NdbDictionary::Table **ret_tab,
    NdbTransaction **ret_trans)
{
    if (field_len > MAX_KEY_VALUE_LEN)
    {
        assign_generic_err_to_response(response, REDIS_KEY_TOO_LARGE);
        return false;
    }
    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    if (dict == nullptr)
    {
        assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
        return false;
    }
    const NdbDictionary::Table *tab = dict->getTable(FIELD_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response, FAILED_CREATE_TABLE_OBJECT, dict->getNdbError());
        return false;
    }
    memcpy(&field_row->redis_field[2], field_str, field_len);
    set_length((char*)&field_row->redis_field[0], field_len);
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  &field_row->redis_field[0],
                                                  field_len + 2);
    if (trans == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TXN_OBJECT,
                                   ndb->getNdbError());
        return false;
    }
    *ret_tab = tab;
    *ret_trans = trans;
    *ret_dict = dict;
    return true;
}

/*
    A successful GET will return in this format:
        $5
        Hello
    where:
    - $ indicates a Bulk String reply
    - 5 is the length of the value ("Hello" has 5 characters)
    - Hello is the actual value stored at the key
    Special cases:
        $-1
    The key does not exist.
        $0
    The key exists but has no value (empty string).
*/
void rondb_hget_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response)
{
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct field_table field_row;
    const char *field_str = argv[1].c_str();
    Uint32 field_len = argv[1].size();
    if (!setup_transaction(ndb,
                           argv,
                           response,
                           &field_row,
                           field_str,
                           field_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    int ret_code = get_simple_field_row(
        response,
        tab,
        ndb,
        trans,
        &field_row,
        field_len);
    ndb->closeTransaction(trans);
    if ((ret_code != 0) || field_row.num_rows == 0)
    {
        return;
    }
    printf("Getting %d value rows\n", field_row.num_rows);
    {
        /*
            Our value uses value rows, so a more complex read is required.
            We're starting from scratch here since we'll use a shared lock
            on the key table this time we read from it.
        */
        NdbTransaction *trans = ndb->startTransaction(tab,
                                                      &(field_row.redis_field[0]),
                                                      field_len + 2);
        if (trans == nullptr)
        {
            assign_ndb_err_to_response(response,
                                       FAILED_CREATE_TXN_OBJECT,
                                       ndb->getNdbError());
            return;
        }
        get_complex_key_row(response,
                            dict,
                            tab,
                            ndb,
                            trans,
                            &field_row,
                            field_len);
        ndb->closeTransaction(trans);
        return;
    }
}

void rondb_hset_command(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response)
{
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct field_table field_row;
    const char *field_str = argv[1].c_str();
    Uint32 field_len = argv[1].size();
    if (!setup_transaction(ndb,
                           argv,
                           response,
                           &field_row,
                           field_str,
                           field_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    const char *value_str = argv[2].c_str();
    Uint32 value_len = argv[2].size();
    char varsize_param[EXTENSION_VALUE_LEN + 500];
    Uint32 num_value_rows = 0;
    Uint64 field_key = 0;

    if (value_len > INLINE_VALUE_LEN)
    {
        /**
         * The row doesn't fit in one RonDB row, create more rows
         * in the value_tables table.
         *
         * We also use the generated rondb_key which is the foreign
         * key column in the key_table table such that
         * deleting the row in the main table ensures that all
         * value rows are also deleted.
         */
        Uint32 extended_value_len = value_len - INLINE_VALUE_LEN;
        num_value_rows = extended_value_len / EXTENSION_VALUE_LEN;
        if (extended_value_len % EXTENSION_VALUE_LEN != 0)
        {
            num_value_rows++;
        }

        if (rondb_get_field_key(tab, field_key, ndb, response) != 0)
        {
            return;
        }
    }

    int ret_code = 0;
    ret_code = create_field_row(response,
                                ndb,
                                tab,
                                trans,
                                field_key,
                                field_str,
                                field_len,
                                value_str,
                                value_len,
                                num_value_rows,
                                Uint32(0),
                                &varsize_param[0]);
    if (ret_code != 0)
    {
        // Often unnecessary since it already failed to commit
        ndb->closeTransaction(trans);
        if (ret_code != FOREIGN_KEY_RESTRICT_ERROR)
        {
            return;
        }
        else
        {
            /*
                If we are here, we have tried writing a key that already exists.
                This would not be a problem if this key did not have references
                to value rows. Hence we first need to delete all of those - this
                is best done via a cascade delete. We do a delete & insert in
                a single transaction (plus writing the value rows).
            */
            NdbTransaction *trans = ndb->startTransaction(tab,
                                                          &(field_row.redis_field[0]),
                                                          field_len + 2);
            if (trans == nullptr)
            {
                assign_ndb_err_to_response(response, FAILED_CREATE_TXN_OBJECT, ndb->getNdbError());
                return;
            }

            if (delete_and_insert_key_row(response,
                                          ndb,
                                          tab,
                                          trans,
                                          field_key,
                                          field_str,
                                          field_len,
                                          value_str,
                                          value_len,
                                          num_value_rows,
                                          Uint32(0),
                                          &varsize_param[0]) != 0)
            {
                ndb->closeTransaction(trans);
                return;
            }
        }
    }

    if (num_value_rows == 0)
    {
        ndb->closeTransaction(trans);
        response->append("+OK\r\n");
        return;
    }
    printf("Inserting %d value rows\n", num_value_rows);
    create_all_value_rows(response,
                          ndb,
                          dict,
                          trans,
                          field_key,
                          value_str,
                          value_len,
                          num_value_rows,
                          &varsize_param[0]);
    ndb->closeTransaction(trans);
    return;
}

void rondb_hincr_command(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response)
{
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct field_table field_row;
    const char *field_str = argv[1].c_str();
    Uint32 field_len = argv[1].size();
    if (!setup_transaction(ndb,
                           argv,
                           response,
                           &field_row,
                           field_str,
                           field_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    incr_field_row(response,
                   ndb,
                   tab,
                   trans,
                   &field_row);
    ndb->closeTransaction(trans);
    return;
}
