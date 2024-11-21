#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <algorithm>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "db_operations.h"
#include "commands.h"
#include "../common.h"
#include "table_definitions.h"

static
bool setup_metadata(
    Ndb *ndb,
    std::string *response,
    const NdbDictionary::Dictionary **ret_dict,
    const NdbDictionary::Table **ret_tab) {
    const NdbDictionary::Dictionary *dict = ndb->getDictionary();
    if (dict == nullptr)
    {
        assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
        return false;
    }
    const NdbDictionary::Table *tab = dict->getTable(KEY_TABLE_NAME);
    if (tab == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   dict->getNdbError());
        return false;
    }
    *ret_tab = tab;
    *ret_dict = dict;
    return true;
}

static
bool setup_one_transaction(Ndb *ndb,
                           std::string *response,
                           Uint64 redis_key_id,
                           struct key_table *key_row,
                           const char *key_str,
                           Uint32 key_len,
                           const NdbDictionary::Table *tab,
                           NdbTransaction **ret_trans) {
    if (key_len > MAX_KEY_VALUE_LEN)
    {
        assign_generic_err_to_response(response, REDIS_KEY_TOO_LARGE);
        return false;
    }
    key_row->redis_key_id = redis_key_id;
    memcpy(&key_row->redis_key[2], key_str, key_len);
    set_length((char*)&key_row->redis_key[0], key_len);
    NdbTransaction *trans = ndb->startTransaction(tab,
                                                  (const char*)&key_row->redis_key_id,
                                                  key_len + 10);
    if (trans == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TXN_OBJECT,
                                   ndb->getNdbError());
        return false;
    }
    *ret_trans = trans;
    return true;
}

bool setup_transaction(
    Ndb *ndb,
    std::string *response,
    Uint64 redis_key_id,
    struct key_table *key_row,
    const char *key_str,
    Uint32 key_len,
    const NdbDictionary::Dictionary **ret_dict,
    const NdbDictionary::Table **ret_tab,
    NdbTransaction **ret_trans)
{
    if (!setup_metadata(ndb,
                        response,
                        ret_dict,
                        ret_tab)) {
        return false;
    }
    return setup_one_transaction(ndb,
                                 response,
                                 redis_key_id,
                                 key_row,
                                 key_str,
                                 key_len,
                                 *ret_tab,
                                 ret_trans);
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
static
void rondb_get(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               Uint64 redis_key_id)
{
    Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct key_table key_row;
    const char *key_str = argv[arg_index_start].c_str();
    Uint32 key_len = argv[arg_index_start].size();
    if (!setup_transaction(ndb,
                           response,
                           redis_key_id,
                           &key_row,
                           key_str,
                           key_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    int ret_code = get_simple_key_row(
        response,
        tab,
        ndb,
        trans,
        &key_row);
    ndb->closeTransaction(trans);
    if ((ret_code != 0) || key_row.num_rows == 0)
    {
        return;
    }
    {
        /*
            Our value uses value rows, so a more complex read is required.
            We're starting from scratch here since we'll use a shared lock
            on the key table this time we read from it.
        */
        trans = ndb->startTransaction(tab,
                                      (const char*)&key_row.redis_key_id,
                                      key_len + 10);
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
                            &key_row);
        ndb->closeTransaction(trans);
        return;
    }
}

void release_mget(struct GetControl *get_ctrl) {
    struct KeyStorage *key_storage = get_ctrl->m_key_store;
    for (Uint32 i = 0; i < get_ctrl->m_num_keys_requested; i++) {
        if (key_storage[i].m_trans != nullptr) {
            get_ctrl->m_ndb->closeTransaction(key_storage[i].m_trans);
            get_ctrl->m_num_transactions--;
        }
        if (key_storage[i].m_complex_value != nullptr) {
            free(key_storage[i].m_complex_value);
        }
    }
    if (get_ctrl->m_value_rows != nullptr) {
        free(get_ctrl->m_value_rows);
    }
    assert(get_ctrl->m_num_transactions == 0);
    free(get_ctrl);
    free(key_storage);
}

static int get_simple_rows(Ndb *ndb,
                           const NdbDictionary::Table *tab,
                           std::string *response,
                           Uint64 redis_key_id,
                           struct KeyStorage *key_storage,
                           struct GetControl *get_ctrl,
                           Uint32 loop_count,
                           Uint32 current_index) {
    for (Uint32 i = 0; i < loop_count; i++) {
        Uint32 inx = current_index + i;
        if (!setup_one_transaction(ndb,
                                   response,
                                   redis_key_id,
                                   &key_storage[inx].m_key_row,
                                   key_storage[inx].m_key_str,
                                   key_storage[inx].m_key_len,
                                   tab,
                                   &key_storage[inx].m_trans)) {
            return 1;
        }
        get_ctrl->m_num_transactions++;
        if (prepare_get_simple_key_row(response,
                                       tab,
                                       key_storage[inx].m_trans,
                                       &key_storage[inx].m_key_row) != 0) {
            return 1;
        }
        prepare_simple_read_transaction(response,
                                        key_storage[inx].m_trans,
                                        &key_storage[inx]);
    }
    Uint32 current_finished_in_loop = 0;
    Uint32 min_finished = loop_count;
    get_ctrl->m_num_keys_outstanding = loop_count;
    do {
        /**
         * Now send off all prepared and wait for all to complete.
         * Since we are using CommitedRead there is no risk of
         * deadlocks by waiting for all to complete here.
         */
        int finished = ndb->sendPollNdb(3000, (int)min_finished);
        assert(finished >= 0);
        current_finished_in_loop += finished;
    } while (current_finished_in_loop < loop_count);
    return 0;
}

static int send_value_read(std::string *response,
                           struct KeyStorage *key_store,
                           struct GetControl *get_ctrl) {
    if (key_store->m_num_read_rows == 0) {
        /* Before we read the data we need to allocate memory for the row */
        key_store->m_complex_value = (char*)
          malloc(key_store->m_read_value_size);
        if (key_store->m_complex_value == nullptr) {
          assign_generic_err_to_response(response, FAILED_MALLOC);
          return 1;
        }
        key_store->m_first_value_row = get_ctrl->m_next_value_row;
        get_ctrl->m_next_value_row += MAX_PARALLEL_VALUE_READS;
    }
    Uint32 i = 0;
    Uint32 value_row_index = key_store->m_first_value_row;
    do {
        struct value_table *value_row =
          &get_ctrl->m_value_rows[value_row_index + i];
        struct key_table *key_row = &key_store->m_key_row;
        value_row->rondb_key = key_store->m_key_row.rondb_key;
        value_row->ordinal = key_store->m_num_read_rows;
        int ret_code = prepare_get_value_row(response,
                                             key_store->m_trans,
                                             value_row);
        if (ret_code != 0) return 1;
        i++;
        key_store->m_num_read_rows++;
    } while (i < MAX_PARALLEL_VALUE_READS &&
             key_store->m_num_read_rows < key_store->m_num_rows);
    key_store->m_num_current_read_rows = i;
    get_ctrl->m_num_bytes_outstanding += i * sizeof(struct value_table);
    if (key_store->m_num_read_rows == key_store->m_num_rows) {
        commit_read_value_transaction(key_store->m_trans, key_store);
        key_store->m_key_state = KeyState::MultiRowReadAll;
    }
    else
    {
        prepare_read_value_transaction(key_store->m_trans, key_store);
    }
}

static int send_next_read_batch(std::string *response,
                                struct KeyStorage *key_storage,
                                struct GetControl *get_ctrl,
                                Uint32 current_index,
                                Uint32 loop_count,
                                Uint32 &current_finished) {
    if (get_ctrl->m_num_keys_multi_rows == 0) {
        return 0;
    }
    for (Uint32 i = 0; i < loop_count; i++) {
        Uint32 inx = current_index + i;
        if (key_storage[inx].m_key_state == KeyState::MultiRowReadValue) {
            if (get_ctrl->m_num_bytes_outstanding > MAX_OUTSTANDING_BYTES) {
                return 0;
            }
            int ret_code = send_value_read(response,
                                           &key_storage[inx],
                                           get_ctrl);
            if (ret_code != 0) return 1;
            assert(current_finished > 0);
            current_finished--;
        }
    }
    return 0;
}

static int get_complex_rows(Ndb *ndb,
                            const NdbDictionary::Table *tab,
                            std::string *response,
                            Uint64 redis_key_id,
                            struct KeyStorage *key_storage,
                            struct GetControl *get_ctrl,
                            Uint32 loop_count,
                            Uint32 current_index) {
    Uint32 num_complex_reads = 0;
    for (Uint32 i = 0; i < loop_count; i++) {
        Uint32 inx = current_index + i;
        if (key_storage[inx].m_key_state == KeyState::MultiRow) {
            num_complex_reads++;
            if (!setup_one_transaction(ndb,
                                       response,
                                       redis_key_id,
                                       &key_storage[inx].m_key_row,
                                       key_storage[inx].m_key_str,
                                       key_storage[inx].m_key_len,
                                       tab,
                                       &key_storage[inx].m_trans)) {
                return 1;
            }
            get_ctrl->m_num_transactions++;
            if (prepare_get_key_row(response,
                                    key_storage[inx].m_trans,
                                    &key_storage[inx].m_key_row) != 0) {
                return 1;
            }
            prepare_read_transaction(response,
                                     key_storage[inx].m_trans,
                                     &key_storage[inx]);
        }
    }
    get_ctrl->m_value_rows = (struct value_table*)malloc(
      num_complex_reads * MAX_PARALLEL_VALUE_READS *
      sizeof(struct value_table));
    if (get_ctrl->m_value_rows == nullptr) {
        assign_generic_err_to_response(response, FAILED_MALLOC);
        return 1;
    }
    assert(num_complex_reads == get_ctrl->m_num_keys_multi_rows);
    Uint32 current_finished_in_loop = 0;
    get_ctrl->m_num_keys_outstanding = num_complex_reads;
    get_ctrl->m_num_bytes_outstanding = loop_count * sizeof(struct key_table);
    do {
        /**
         * Now send off all prepared and wait for at least one to complete.
         * We cannot wait for multiple ones since we could then run into
         * deadlock issues. The transactions are independent of each other,
         * so if one of them has to wait for a lock, it should not stop
         * other transactions from progressing.
         */
        int finished = ndb->sendPollNdb(3000, (int)1);
        assert(finished >= 0);
        current_finished_in_loop += finished;
        if (get_ctrl->m_num_keys_failed > 0) return 0;
        int ret_code = send_next_read_batch(response,
                                            key_storage,
                                            get_ctrl,
                                            current_index,
                                            loop_count,
                                            current_finished_in_loop);
        if (ret_code != 0) return 1;
    } while (current_finished_in_loop < num_complex_reads);
    return 0;
}

static
void rondb_mget(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               Uint64 redis_key_id)
{
    Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
    Uint32 num_keys = argv.size() - arg_index_start;
    assert(num_keys > 0);
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct KeyStorage *key_storage;
    key_storage = (struct KeyStorage*)malloc(
      sizeof(struct KeyStorage) * num_keys);
    if (key_storage == nullptr) {
        assign_generic_err_to_response(response, FAILED_MALLOC);
        return;
    }
    struct GetControl *get_ctrl = (struct GetControl*)
      malloc(sizeof(struct GetControl));
    if (get_ctrl == nullptr) {
        assign_generic_err_to_response(response, FAILED_MALLOC);
        free(get_ctrl);
        return;
    }
    get_ctrl->m_ndb = ndb;
    get_ctrl->m_key_store = key_storage;
    get_ctrl->m_value_rows = nullptr;
    get_ctrl->m_next_value_row = 0;
    get_ctrl->m_num_transactions = 0;
    get_ctrl->m_num_keys_requested = num_keys;
    get_ctrl->m_num_keys_outstanding = 0;
    get_ctrl->m_num_bytes_outstanding = 0;
    get_ctrl->m_num_keys_completed_first_pass = 0;
    get_ctrl->m_num_keys_multi_rows = 0;
    get_ctrl->m_num_keys_failed = 0;
    get_ctrl->m_error_code = 0;
    for (Uint32 i = 0; i < num_keys; i++) {
        Uint32 arg_index = i + arg_index_start;
        key_storage[i].m_get_ctrl = get_ctrl;
        key_storage[i].m_trans = nullptr;
        key_storage[i].m_key_str = argv[arg_index].c_str();
        key_storage[i].m_key_len = argv[arg_index].size();
        key_storage[i].m_complex_value = nullptr;
        key_storage[i].m_header_len = 0;
        key_storage[i].m_first_value_row = 0;
        key_storage[i].m_current_pos = 0;
        key_storage[i].m_num_rows = 0;
        key_storage[i].m_num_read_rows = 0;
        key_storage[i].m_read_value_size = 0;
        key_storage[i].m_key_state = KeyState::NotCompleted;
    }
    if (!setup_metadata(ndb,
                        response,
                        &dict,
                        &tab)) {
        release_mget(get_ctrl);
        return;
    }
    Uint32 current_index = 0;
    do {
        Uint32 loop_count = std::min(num_keys - current_index,
                                     (Uint32)MAX_PARALLEL_READ_KEY_OPS);
        int ret_code = get_simple_rows(ndb,
                                       tab,
                                       response,
                                       redis_key_id,
                                       key_storage,
                                       get_ctrl,
                                       loop_count,
                                       current_index);
        if (ret_code != 0) {
            release_mget(get_ctrl);
            return;
        }
        /**
         * We have finished the initial round of simple GETs. Now time
         * to handle those that require multi-row GETs. Since we used
         * an optimistic approach we need to start this from scratch
         * again for these new GETs.
         */
        Uint32 num_complex_reads = 0;
        assert(get_ctrl->m_num_transactions == 0);
        assert(get_ctrl->m_num_keys_outstanding == 0);
        if (get_ctrl->m_num_keys_multi_rows > 0 &&
            get_ctrl->m_num_keys_failed == 0) {
            int ret_code = get_complex_rows(ndb,
                                            tab,
                                            response,
                                            redis_key_id,
                                            key_storage,
                                            get_ctrl,
                                            loop_count,
                                            current_index);
            if (ret_code != 0) {
                release_mget(get_ctrl);
                return;
            }
        }
        current_index += loop_count;
    } while (current_index < num_keys && get_ctrl->m_num_keys_failed == 0);
    /**
     * We are done with the reading process, now it is time to report the
     * result based on the KeyStorage array.
     */
    if (get_ctrl->m_num_keys_failed > 0) {
        assign_err_to_response(response,
                               FAILED_EXECUTE_MGET,
                               get_ctrl->m_error_code);
        release_mget(get_ctrl);
        return;
    }
    Uint64 tot_bytes = 0;
    for (Uint32 i = 0; i < num_keys; i++) {
        struct KeyStorage *key_store = &key_storage[i];
        if (key_store->m_key_state == KeyState::CompletedFailed) {
            /* Report found NULL */
            key_store->m_header_len = (Uint32)snprintf(
                key_store->m_header_buf,
                sizeof(key_store->m_header_buf),
                "$-1\r\n");
        } else {
            tot_bytes += key_store->m_read_value_size;
            key_store->m_header_len = (Uint32)snprintf(
                key_store->m_header_buf,
                sizeof(key_store->m_header_buf),
                "$%u\r\n",
                key_store->m_read_value_size);
        }
        tot_bytes += key_store->m_header_len;
        tot_bytes += 2;
    }
    {
        char header_buf[20];
        Uint32 header_len = (Uint32)snprintf(header_buf,
                                             sizeof(header_buf),
                                             "*%u\r\n",
                                             num_keys);
        tot_bytes += (Uint32)header_len;
        response->reserve(tot_bytes);
        response->append((const char*)&header_buf[0], header_len);
    }
    for (Uint32 i = 0; i < num_keys; i++) {
        struct KeyStorage *key_store = &key_storage[i];
        response->append((const char*)&key_store->m_header_buf[0],
                         key_store->m_header_len);
        if (key_store->m_key_state == KeyState::CompletedSuccess) {
            response->append((const char*)&key_store->m_key_row.value_start[2],
                             key_store->m_read_value_size);
        }
        else
        {
            assert(key_store->m_key_state == KeyState::CompletedMultiRow);
            response->append((const char*)&key_store->m_complex_value,
                             key_store->m_read_value_size);
        }
        response->append("\r\n");
    }
    release_mget(get_ctrl);
    return;
}

static
void rondb_set(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response,
    Uint64 redis_key_id)
{
    Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct key_table key_row;
    const char *key_str = argv[arg_index_start].c_str();
    Uint32 key_len = argv[arg_index_start].size();
    if (!setup_transaction(ndb,
                           response,
                           redis_key_id,
                           &key_row,
                           key_str,
                           key_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    const char *value_str = argv[arg_index_start + 1].c_str();
    Uint32 value_len = argv[arg_index_start + 1].size();
    char varsize_param[EXTENSION_VALUE_LEN + 500];
    Uint32 num_value_rows = 0;
    Uint32 prev_num_rows = 0;
    Uint64 rondb_key = 0;

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

        if (rondb_get_rondb_key(tab, rondb_key, ndb, response) != 0)
        {
            ndb->closeTransaction(trans);
            return;
        }
    }

    int ret_code = 0;
    ret_code = create_key_row(response,
                              tab,
                              trans,
                              redis_key_id,
                              rondb_key,
                              key_str,
                              key_len,
                              value_str,
                              value_len,
                              num_value_rows,
                              prev_num_rows,
                              Uint32(0));
    if (ret_code != 0)
    {
        // Often unnecessary since it already failed to commit
        ndb->closeTransaction(trans);
        if (ret_code != RESTRICT_VALUE_ROWS_ERROR)
        {
            return;
        }
        /*
            If we are here, we have tried writing a key that already exists.
            This would not be a problem if this key did not have references
            to value rows. Hence we first need to delete all of those - this
            is best done via a cascade delete. We do a delete & insert in
            a single transaction (plus writing the value rows).
        */
        trans = ndb->startTransaction(tab,
                                      (const char*)&key_row.redis_key_id,
                                      key_len + 10);
        if (trans == nullptr)
        {
            assign_ndb_err_to_response(response, FAILED_CREATE_TXN_OBJECT, ndb->getNdbError());
            return;
        }
        /**
         * We don't know the exact number of value rows, but we know that it is
         * at least one.
         */
        prev_num_rows = 1;
        ret_code = create_key_row(response,
                                  tab,
                                  trans,
                                  redis_key_id,
                                  rondb_key,
                                  key_str,
                                  key_len,
                                  value_str,
                                  value_len,
                                  num_value_rows,
                                  prev_num_rows,
                                  Uint32(0));
        if (ret_code != 0) {
            ndb->closeTransaction(trans);
            return;
        }
    } else if (num_value_rows == 0) {
        ndb->closeTransaction(trans);
        response->append("+OK\r\n");
        return;
    }
    const NdbDictionary::Table *value_tab = dict->getTable(VALUE_TABLE_NAME);
    if (value_tab == nullptr)
    {
        ndb->closeTransaction(trans);
        assign_ndb_err_to_response(response,
                                   FAILED_CREATE_TABLE_OBJECT,
                                   ndb->getNdbError());
        return;
    }
    /**
     * Coming here means that we either have to add new value rows or we have
     * to delete previous value rows or both. Thus the transaction is still
     * open. We start by creating the new value rows. Next we delete the
     * remaining value rows from the previous instantiation of the row.
     */
    if (num_value_rows > 0) {
        ret_code = create_all_value_rows(response,
                                         ndb,
                                         value_tab,
                                         trans,
                                         rondb_key,
                                         value_str,
                                         value_len,
                                         num_value_rows,
                                         &varsize_param[0]);
    }
    if (ret_code != 0) {
        ndb->closeTransaction(trans);
        return;
    }
    ret_code = delete_value_rows(response,
                                 value_tab,
                                 trans,
                                 rondb_key,
                                 num_value_rows,
                                 prev_num_rows);
    if (ret_code != 0) {
        ndb->closeTransaction(trans);
        return;
    }
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) == 0 &&
        trans->getNdbError().code != 0)
    {
        ndb->closeTransaction(trans);
        assign_ndb_err_to_response(response,
                                   FAILED_EXEC_TXN,
                                   trans->getNdbError());
        return;
    }
    ndb->closeTransaction(trans);
    response->append("+OK\r\n");
    return;
}

static
void rondb_mset(Ndb *ndb,
               const pink::RedisCmdArgsType &argv,
               std::string *response,
               Uint64 redis_key_id)
{
    Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct key_table key_row;
    const char *key_str = argv[arg_index_start].c_str();
    Uint32 key_len = argv[arg_index_start].size();
    if (!setup_transaction(ndb,
                           response,
                           redis_key_id,
                           &key_row,
                           key_str,
                           key_len,
                           &dict,
                           &tab,
                           &trans)) {
        return;
    }
    return;
}

static
void rondb_incr(
    Ndb *ndb,
    const pink::RedisCmdArgsType &argv,
    std::string *response,
    Uint64 redis_key_id)
{
    Uint32 arg_index_start = (redis_key_id == STRING_REDIS_KEY_ID) ? 1 : 2;
    const NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tab = nullptr;
    NdbTransaction *trans = nullptr;
    struct key_table key_row;
    const char *key_str = argv[arg_index_start].c_str();
    Uint32 key_len = argv[arg_index_start].size();
    if (!setup_transaction(ndb,
                           response,
                           redis_key_id,
                           &key_row,
                           key_str,
                           key_len,
                           &dict,
                           &tab,
                           &trans))
      return;

    incr_key_row(response,
                 ndb,
                 tab,
                 trans,
                 &key_row);
    ndb->closeTransaction(trans);
    return;
}

void rondb_get_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response)
{
  return rondb_get(ndb, argv, response, STRING_REDIS_KEY_ID);
}

void rondb_mget_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response)
{
  return rondb_mget(ndb, argv, response, STRING_REDIS_KEY_ID);
}

void rondb_set_command(Ndb *ndb,
                       const pink::RedisCmdArgsType &argv,
                       std::string *response)
{
  return rondb_set(ndb, argv, response, STRING_REDIS_KEY_ID);
}

void rondb_mset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response)
{
  return rondb_mset(ndb, argv, response, STRING_REDIS_KEY_ID);
}

void rondb_incr_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response)
{
  return rondb_incr(ndb, argv, response, STRING_REDIS_KEY_ID);
}

void rondb_hget_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response)
{
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response);
  if (ret_code != 0) {
      return;
  }
  return rondb_get(ndb, argv, response, redis_key_id);
}

void rondb_hmget_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response)
{
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response);
  if (ret_code != 0) {
      return;
  }
  return rondb_mget(ndb, argv, response, redis_key_id);
}

void rondb_hset_command(Ndb *ndb,
                        const pink::RedisCmdArgsType &argv,
                        std::string *response)
{
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                        redis_key_id,
                                        argv[1].c_str(),
                                        argv[1].size(),
                                        response);
  if (ret_code != 0) {
      return;
  }
  return rondb_mset(ndb, argv, response, redis_key_id);
}

void rondb_hincr_command(Ndb *ndb,
                         const pink::RedisCmdArgsType &argv,
                         std::string *response)
{
  Uint64 redis_key_id;
  int ret_code = rondb_get_redis_key_id(ndb,
                                       redis_key_id,
                                       argv[1].c_str(),
                                       argv[1].size(),
                                       response);
  if (ret_code != 0) {
      return;
  }
  return rondb_incr(ndb, argv, response, redis_key_id);
}
