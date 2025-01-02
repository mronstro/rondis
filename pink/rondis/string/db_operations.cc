#include <unordered_map>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "../common.h"
#include "db_operations.h"
#include "table_definitions.h"
#include "interpreted_code.h"

#define DEBUG_KS 1
#define DEBUG_CTRL 1
#define DEBUG_HSET_KEY 1
#define DEBUG_MSET 1
#define DEBUG_INCR 1

#ifdef DEBUG_INCR
#define DEB_INCR(arglist) do { printf arglist ; } while (0)
#else
#define DEB_INCR(arglist)
#endif

#ifdef DEBUG_KS
#define DEB_KS(arglist) do { printf arglist ; } while (0)
#else
#define DEB_KS(arglist)
#endif

#ifdef DEBUG_CTRL
#define DEB_CTRL(arglist) do { printf arglist ; } while (0)
#else
#define DEB_CTRL(arglist)
#endif

#ifdef DEBUG_HSET_KEY
#define DEB_HSET_KEY(arglist) do { printf arglist ; } while (0)
#else
#define DEB_HSET_KEY(arglist)
#endif

#ifdef DEBUG_MSET
#define DEB_MSET(arglist) do { printf arglist ; } while (0)
#else
#define DEB_MSET(arglist)
#endif

NdbRecord *pk_hset_key_record = nullptr;
NdbRecord *entire_hset_key_record = nullptr;
NdbRecord *pk_key_record = nullptr;
NdbRecord *entire_key_record = nullptr;
NdbRecord *pk_value_record = nullptr;
NdbRecord *entire_value_record = nullptr;

int write_data_to_key_op(std::string *response,
                         const NdbDictionary::Table *tab,
                         NdbTransaction *trans,
                         Uint64 redis_key_id,
                         Uint64 rondb_key,
                         const char *key_str,
                         Uint32 key_len,
                         const char *value_str,
                         Uint32 tot_value_len,
                         Uint32 num_value_rows,
                         bool commit_flag,
                         Uint32 row_state,
                         NdbRecAttr **recAttr0,
                         NdbRecAttr **recAttr1) {
    struct key_table key_row;
    Uint32 mask = 0xFB;
    key_row.null_bits = 0;
    memcpy(&key_row.redis_key[2], key_str, key_len);
    set_length(&key_row.redis_key[0], key_len);
    key_row.redis_key_id = redis_key_id;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    key_row.tot_value_len = tot_value_len;
    key_row.num_rows = num_value_rows;
    key_row.value_data_type = row_state;
    key_row.expiry_date = 0;

    Uint32 this_value_len = tot_value_len;
    if (this_value_len > INLINE_VALUE_LEN)
    {
        this_value_len = INLINE_VALUE_LEN;
    }
    memcpy(&key_row.value_start[2], value_str, this_value_len);
    set_length(&key_row.value_start[0], this_value_len);

    Uint32 code_buffer[64];
    NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
    int ret_code = 0;
    if (commit_flag) {
        ret_code = write_key_row_commit(response, code, tab);
    }
    else
    {
        ret_code = write_key_row_no_commit(response, code, tab, rondb_key);
    }
    if (ret_code != 0) {
        return ret_code;
    }

    // Prepare the interpreted program to be part of the write
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
    opts.interpretedCode = &code;

    NdbOperation::GetValueSpec getvals[2];
    if (commit_flag) {
        getvals[0].appStorage = nullptr;
        getvals[0].recAttr = nullptr;
        getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
        opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
        opts.numExtraGetFinalValues = 1;
        opts.extraGetFinalValues = getvals;
    }
    else
    {
        getvals[0].appStorage = nullptr;
        getvals[0].recAttr = nullptr;
        getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
        getvals[1].appStorage = nullptr;
        getvals[1].recAttr = nullptr;
        getvals[1].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_1;
        opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
        opts.numExtraGetFinalValues = 2;
        opts.extraGetFinalValues = getvals;
    }

    /* Define the actual operation to be sent to RonDB data node. */
    const NdbOperation *op = trans->writeTuple(
        pk_key_record,
        (const char *)&key_row,
        entire_key_record,
        (char *)&key_row,
        mask_ptr,
        &opts,
        sizeof(opts));
    if (op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create NdbOperation",
                                   trans->getNdbError());
        return -1;
    }
    *recAttr0 = getvals[0].recAttr;
    *recAttr1 = getvals[1].recAttr;
    return 0;
}

int delete_key_row(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbTransaction *trans,
                   Uint64 redis_key_id,
                   const char *key_str,
                   Uint32 key_len,
                   char *buf) {
    NdbOperation *del_op = trans->getNdbOperation(tab);
    if (del_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return -1;
    }
    del_op->deleteTuple();
    memcpy(&buf[2], key_str, key_len);
    set_length(buf, key_len);
    del_op->equal(KEY_TABLE_COL_redis_key, buf);
    del_op->equal(KEY_TABLE_COL_redis_key_id, redis_key_id);

    if (del_op->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_DEFINE_OP,
                                   del_op->getNdbError());
        return -1;
    }

    if (trans->execute(NdbTransaction::NoCommit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_EXEC_TXN,
                                   trans->getNdbError());
        return -1;
    }
    return 0;
}

static void
read_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_store = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_store->m_get_ctrl;
    assert(get_ctrl->m_num_transactions > 0);
    assert(trans == key_store->m_trans);
    assert(key_store->m_key_state == KeyState::MultiRow);
    (void)result;
    int code = trans->getNdbError().code;
    if (code != 0) {
        DEB_KS(("Key %u had error: %d\n", key_store->m_index, code));
        key_store->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_completed_first_pass++;
        if (code != READ_ERROR) {
            get_ctrl->m_num_keys_failed++;
            if (get_ctrl->m_error_code == 0) {
                get_ctrl->m_error_code = code;
            }
        }
        get_ctrl->m_ndb->closeTransaction(trans);
        get_ctrl->m_num_transactions--;
        key_store->m_trans = nullptr;
        assert(get_ctrl->m_num_keys_multi_rows > 0);
        get_ctrl->m_num_keys_multi_rows--;
    } else if (key_store->m_key_row.num_rows > 0) {
        key_store->m_key_state = KeyState::MultiRowRWValue;
        key_store->m_value_size = key_store->m_key_row.tot_value_len;
        key_store->m_num_rows = key_store->m_key_row.num_rows;
        key_store->m_rondb_key = key_store->m_key_row.rondb_key;
        DEB_KS(("LockRead Key %u with size: %u, num_rows: %u"
                ", key_state: %u\n",
          key_store->m_index,
          key_store->m_value_size,
          key_store->m_num_rows,
          key_store->m_key_state));
    } else {
        key_store->m_key_state = KeyState::CompletedMultiRowSuccess;
        Uint32 value_len =
          get_length((char*)&key_store->m_key_row.value_start[0]);
        assert(value_len == key_store->m_key_row.tot_value_len);
        key_store->m_value_size = value_len;
        key_store->m_rondb_key = 0;
        DEB_KS(("LockRead Key %u completed, no value rows\n",
          key_store->m_index));
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    Uint32 sz = (sizeof(struct key_table) - MAX_KEY_VALUE_LEN);
    assert(get_ctrl->m_num_bytes_outstanding >= sz);
    get_ctrl->m_num_bytes_outstanding -= sz;
    DEB_KS(("Key %u, keys %u and bytes %u out\n",
      key_store->m_index,
      get_ctrl->m_num_keys_outstanding,
      get_ctrl->m_num_bytes_outstanding));
}

int prepare_get_value_row(std::string *response,
                          NdbTransaction *trans,
                          struct value_table *value_row) {
    /**
     * Mask and options means simply reading all columns
     * except primary key columns. In this case only the
     * value column is read. We read the ordinal column
     * as well to ensure that we don't rely on order of
     * signals arriving. Normally they should be arriving
     * in order, but it is safer to not rely on that.
     *
     * We use SimpleRead to ensure that DBTC is aborted if
     * something goes wrong with the read, the row should
     * never be locked since we hold a lock on the key row
     * at this point.
     */
    const Uint32 mask = 0x6;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *read_op = trans->readTuple(
        pk_value_record,
        (const char *)value_row,
        entire_value_record,
        (char *)value_row,
        NdbOperation::LM_SimpleRead,
        mask_ptr);
    if (read_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    return 0;
}

static void
value_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_store = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_store->m_get_ctrl;
    assert(get_ctrl->m_num_transactions > 0);
    assert(trans == key_store->m_trans);
    (void)result;
    if (key_store->m_key_state == KeyState::CompletedMultiRowSuccess) {
        /* Only commit of Locked Read performed here */
        get_ctrl->m_ndb->closeTransaction(trans);
        get_ctrl->m_num_transactions--;
        key_store->m_trans = nullptr;
        assert(get_ctrl->m_num_keys_multi_rows > 0);
        get_ctrl->m_num_keys_multi_rows--;
        key_store->m_key_state = KeyState::CompletedSuccess;
        assert(get_ctrl->m_num_keys_outstanding > 0);
        get_ctrl->m_num_keys_outstanding--;
        return;
    }
    assert(key_store->m_key_state == KeyState::MultiRowRWValueSent ||
           key_store->m_key_state == KeyState::MultiRowRWAll);
    int code = trans->getNdbError().code;
    if (code != 0) {
        DEB_KS(("Key %u had error %d reading value\n",
          key_store->m_index, code));
        key_store->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_completed_first_pass++;
        get_ctrl->m_num_keys_failed++;
        if (get_ctrl->m_error_code == 0) {
            get_ctrl->m_error_code = code;
        }
        get_ctrl->m_ndb->closeTransaction(trans);
        assert(get_ctrl->m_num_transactions > 0);
        get_ctrl->m_num_transactions--;
        key_store->m_trans = nullptr;
        assert(get_ctrl->m_num_keys_multi_rows > 0);
        get_ctrl->m_num_keys_multi_rows--;
    } else {
        Uint32 current_pos = key_store->m_current_pos;
        char *complex_value = key_store->m_value_ptr;
        for (Uint32 i = 0; i < key_store->m_num_current_rw_rows; i++) {
          Uint32 inx = key_store->m_first_value_row + i;
          struct value_table *value_row = &get_ctrl->m_value_rows[inx];
          Uint32 value_len = get_length((char*)&value_row->value[0]);
          Uint32 calc_pos = INLINE_VALUE_LEN +
            (value_row->ordinal * EXTENSION_VALUE_LEN);
          assert(calc_pos == current_pos);
          assert(calc_pos + value_len <= key_store->m_value_size);
          memcpy(&complex_value[calc_pos], &value_row->value[2], value_len);
          Uint32 old_pos = current_pos;
          (void)old_pos;
          current_pos += value_len;
          DEB_KS(("Read value of %u bytes, new pos: %u old_pos: %u (%u), key: %u\n",
            value_len, current_pos, old_pos, calc_pos, key_store->m_index));
        }
        key_store->m_current_pos = current_pos;
        if (key_store->m_num_rows == key_store->m_num_rw_rows) {
            key_store->m_key_state = KeyState::CompletedMultiRow;
            assert(get_ctrl->m_num_keys_multi_rows > 0);
            get_ctrl->m_num_keys_multi_rows--;
            get_ctrl->m_ndb->closeTransaction(trans);
            assert(get_ctrl->m_num_transactions > 0);
            get_ctrl->m_num_transactions--;
            key_store->m_trans = nullptr;
        }
        else
        {
            key_store->m_key_state = KeyState::MultiRowRWValue;
        }
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
    Uint32 sz = sizeof(struct value_table) * key_store->m_num_current_rw_rows;
    assert(get_ctrl->m_num_bytes_outstanding >= sz);
    get_ctrl->m_num_bytes_outstanding -= sz;
    key_store->m_num_current_rw_rows = 0;
    DEB_KS(("Read value for key %u, key_state: %u, keys %u and"
            " bytes %u out, num_read_rows: %u\n",
            key_store->m_index,
            key_store->m_key_state,
            get_ctrl->m_num_keys_outstanding,
            get_ctrl->m_num_bytes_outstanding,
            key_store->m_num_rw_rows));
}

void prepare_read_value_transaction(NdbTransaction *trans,
                                    struct KeyStorage *key_store) {
    trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                &value_callback,
                                (void*)key_store);
}

void commit_read_value_transaction(NdbTransaction *trans,
                                   struct KeyStorage *key_store) {
    trans->executeAsynchPrepare(NdbTransaction::Commit,
                                &value_callback,
                                (void*)key_store);
}

int prepare_get_key_row(std::string *response,
                        NdbTransaction *trans,
                        struct key_table *key_row) {
    /**
     * Mask and options means simply reading all columns
     * except primary key columns.
     */

    const Uint32 mask = 0xFC;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *read_op = trans->readTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        NdbOperation::LM_Read,
        mask_ptr);
    if (read_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    return 0;
}

void prepare_read_transaction(std::string *response,
                              NdbTransaction *trans,
                              struct KeyStorage *key_storage) {
    trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                &read_callback,
                                (void*)key_storage);
}

int prepare_get_simple_key_row(std::string *response,
                               const NdbDictionary::Table *tab,
                               NdbTransaction *trans,
                               struct key_table *key_row) {
    /**
     * Mask and options means simply reading all columns
     * except primary key columns.
     */

    const Uint32 mask = 0xFC;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *read_op = trans->readTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        NdbOperation::LM_CommittedRead,
        mask_ptr);
    if (read_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    return 0;
}

static void
simple_read_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_storage->m_get_ctrl;
    (void)result;
    assert(trans == key_storage->m_trans);
    int code = trans->getNdbError().code;
    if (code != 0) {
        key_storage->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_completed_first_pass++;
        if (code == READ_ERROR) {
            DEB_HSET_KEY(("key %u had READ_ERROR\n", key_storage->m_index));
        } else {
            get_ctrl->m_num_keys_failed++;
            DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
            if (get_ctrl->m_error_code == 0) {
                get_ctrl->m_error_code = code;
            }
        }
    } else if (key_storage->m_key_row.num_rows > 0) {
        key_storage->m_key_state = KeyState::MultiRow;
        get_ctrl->m_num_keys_multi_rows++;
        DEB_HSET_KEY(("key %u required multi-row handling: num_rows: %u\n",
          key_storage->m_index, key_storage->m_key_row.num_rows));
    } else {
        key_storage->m_key_state = KeyState::CompletedSuccess;
        Uint32 value_len = get_length((char*)&key_storage->m_key_row.value_start[0]);
        assert(value_len == key_storage->m_key_row.tot_value_len);
        key_storage->m_value_size = value_len;
        get_ctrl->m_num_keys_completed_first_pass++;
        DEB_HSET_KEY(("key %u was read, size: %u\n",
          key_storage->m_index, value_len));
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    assert(get_ctrl->m_num_transactions > 0);
    get_ctrl->m_num_keys_outstanding--;
    get_ctrl->m_num_transactions--;
    get_ctrl->m_ndb->closeTransaction(trans);
    key_storage->m_trans = nullptr;
}

void prepare_simple_read_transaction(std::string *response,
                                    NdbTransaction *trans,
                                    struct KeyStorage *key_storage) {
    trans->executeAsynchPrepare(NdbTransaction::Commit,
                                &simple_read_callback,
                                (void*)key_storage);
}

int prepare_set_value_row(std::string *response,
                          KeyStorage *key_store) {
    struct value_table value_row;
    Uint32 remaining = key_store->m_value_size - key_store->m_current_pos;
    Uint32 len = std::min((Uint32)EXTENSION_VALUE_LEN, remaining);
    memcpy(&value_row.value[2],
           &key_store->m_value_ptr[key_store->m_current_pos],
           len);
    set_length(&value_row.value[0], len);
    value_row.ordinal = key_store->m_num_rw_rows;
    value_row.rondb_key = key_store->m_rondb_key;
    DEB_MSET(("Set value key: %u, rondb_key: %llu, ordinal: %u\n",
      key_store->m_index,
      key_store->m_rondb_key,
      key_store->m_num_rw_rows));
    key_store->m_num_rw_rows++;
    key_store->m_current_pos += len;
    /* Mask means writing all columns. */
    const Uint32 mask = 0x7;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;
    const NdbOperation *write_op = key_store->m_trans->writeTuple(
        pk_value_record,
        (const char *)&value_row,
        entire_value_record,
        (char *)&value_row,
        mask_ptr);
    if (write_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   key_store->m_trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    return 0;
}

int prepare_delete_value_row(std::string *response,
                             struct KeyStorage *key_store,
                             Uint32 ordinal) {
    struct value_table value_row;
    value_row.ordinal = ordinal;
    value_row.rondb_key = key_store->m_rondb_key;
    DEB_MSET(("Delete value row with rondb_key: %llu and ordinal: %u\n",
      key_store->m_rondb_key, ordinal));
    const NdbOperation *delete_op = key_store->m_trans->deleteTuple(
        pk_value_record,
        (const char *)&value_row,
        entire_value_record);
    if (delete_op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   FAILED_GET_OP,
                                   key_store->m_trans->getNdbError());
        return RONDB_INTERNAL_ERROR;
    }
    return 0;
}

static void
write_value_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_storage->m_get_ctrl;
    (void)result;
    assert(trans == key_storage->m_trans);
    assert(key_storage->m_key_state == KeyState::MultiRowRWValueSent);
    int code = trans->getNdbError().code;
    if (code != 0) {
        key_storage->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_failed++;
        DEB_HSET_KEY(("key %u write value had ERROR: %d\n",
          key_storage->m_index, code));
        if (get_ctrl->m_error_code == 0) {
            get_ctrl->m_error_code = code;
        }
        get_ctrl->m_num_transactions--;
        get_ctrl->m_ndb->closeTransaction(trans);
        key_storage->m_trans = nullptr;
    } else {
        key_storage->m_key_state = KeyState::MultiRowRWValue;
        DEB_HSET_KEY(("key %u write value succeeded\n",
          key_storage->m_index));
        assert(get_ctrl->m_num_bytes_outstanding >=
          (key_storage->m_num_current_rw_rows * sizeof(struct value_table)));
        get_ctrl->m_num_bytes_outstanding -=
          (key_storage->m_num_current_rw_rows * sizeof(struct value_table));
        assert(get_ctrl->m_num_transactions > 0);
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    get_ctrl->m_num_keys_outstanding--;
}

void prepare_write_value_transaction(NdbTransaction *trans,
                                     struct KeyStorage *key_store) {
    trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                &write_value_callback,
                                (void*)key_store);
}

static void
write_commit_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_storage->m_get_ctrl;
    (void)result;
    assert(trans == key_storage->m_trans);
    assert(key_storage->m_key_state == KeyState::MultiRowRWAll);
    int code = trans->getNdbError().code;
    if (code != 0) {
        key_storage->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_failed++;
        DEB_HSET_KEY(("key %u write commit had ERROR: %d\n",
          key_storage->m_index, code));
        if (get_ctrl->m_error_code == 0) {
            get_ctrl->m_error_code = code;
        }
    } else {
        key_storage->m_key_state = KeyState::CompletedSuccess;
        assert(get_ctrl->m_num_keys_multi_rows > 0);
        get_ctrl->m_num_keys_multi_rows--;
        DEB_HSET_KEY(("key %u write commit succeeded\n",
          key_storage->m_index));
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    assert(get_ctrl->m_num_transactions > 0);
    get_ctrl->m_num_keys_outstanding--;
    get_ctrl->m_num_transactions--;
    get_ctrl->m_ndb->closeTransaction(trans);
    key_storage->m_trans = nullptr;
}

void commit_write_value_transaction(NdbTransaction *trans,
                                    struct KeyStorage *key_store) {
    trans->executeAsynchPrepare(NdbTransaction::Commit,
                                &write_commit_callback,
                                (void*)key_store);
}

static void
write_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_storage->m_get_ctrl;
    (void)result;
    assert(trans == key_storage->m_trans);
    int code = trans->getNdbError().code;
    if (code != 0) {
        key_storage->m_key_state = KeyState::CompletedFailed;
        get_ctrl->m_num_keys_failed++;
        DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
        if (get_ctrl->m_error_code == 0) {
            get_ctrl->m_error_code = code;
        }
        assert(get_ctrl->m_num_keys_outstanding > 0);
        assert(get_ctrl->m_num_transactions > 0);
        get_ctrl->m_num_keys_outstanding--;
        get_ctrl->m_num_transactions--;
        get_ctrl->m_ndb->closeTransaction(trans);
        key_storage->m_trans = nullptr;
    } else {
        key_storage->m_prev_num_rows =
          key_storage->m_rec_attr_prev_num_rows->u_32_value();
        key_storage->m_rondb_key =
          key_storage->m_rec_attr_rondb_key->u_32_value();
        key_storage->m_current_pos = INLINE_VALUE_LEN;
        key_storage->m_key_state = KeyState::MultiRowRWValue;
        assert(get_ctrl->m_num_transactions > 0);
        assert(get_ctrl->m_num_keys_outstanding > 0);
        get_ctrl->m_num_keys_outstanding--;
        DEB_HSET_KEY(("key %u simple write succeeded, prev_num_rows: %u"
                      ", rondb_key: %llu\n",
          key_storage->m_index,
          key_storage->m_prev_num_rows,
          key_storage->m_rondb_key));
    }
}

void prepare_write_transaction(NdbTransaction *trans,
                               struct KeyStorage *key_store) {
    trans->executeAsynchPrepare(NdbTransaction::NoCommit,
                                &write_callback,
                                (void*)key_store);
}

static void
simple_write_callback(int result, NdbTransaction *trans, void *aObject) {
    struct KeyStorage *key_storage = (struct KeyStorage*)aObject;
    struct GetControl *get_ctrl = key_storage->m_get_ctrl;
    (void)result;
    assert(trans == key_storage->m_trans);
    int code = trans->getNdbError().code;
    if (code != 0) {
        if (code == RESTRICT_VALUE_ROWS_ERROR) {
            key_storage->m_key_state = KeyState::MultiRow;
            get_ctrl->m_num_keys_multi_rows++;
            DEB_HSET_KEY(("key %u had RESTRICT_VALUE_ROWS_ERROR\n",
              key_storage->m_index));
        } else {
            key_storage->m_key_state = KeyState::CompletedFailed;
            get_ctrl->m_num_keys_failed++;
            DEB_HSET_KEY(("key %u had ERROR: %d\n", key_storage->m_index, code));
            if (get_ctrl->m_error_code == 0) {
                get_ctrl->m_error_code = code;
            }
        }
    } else {
        get_ctrl->m_num_keys_completed_first_pass++;
        key_storage->m_key_state = KeyState::CompletedSuccess;
        DEB_HSET_KEY(("key %u simple write succeeded\n",
          key_storage->m_index));
    }
    assert(get_ctrl->m_num_keys_outstanding > 0);
    assert(get_ctrl->m_num_transactions > 0);
    get_ctrl->m_num_keys_outstanding--;
    get_ctrl->m_num_transactions--;
    get_ctrl->m_ndb->closeTransaction(trans);
    key_storage->m_trans = nullptr;
}

void prepare_simple_write_transaction(std::string *response,
                                      NdbTransaction *trans,
                                      struct KeyStorage *key_storage) {
    trans->executeAsynchPrepare(NdbTransaction::Commit,
                                &simple_write_callback,
                                (void*)key_storage);
}

int rondb_get_rondb_key(const NdbDictionary::Table *tab,
                        Uint64 &rondb_key,
                        Ndb *ndb,
                        std::string *response) {
    if (ndb->getAutoIncrementValue(tab, rondb_key, unsigned(1024)) != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to get autoincrement value",
                                   ndb->getNdbError());
        return -1;
    }
    return 0;
}

void incr_decr_key_row(std::string *response,
                       Ndb *ndb,
                       const NdbDictionary::Table *tab,
                       NdbTransaction *trans,
                       struct key_table *key_row,
                       bool incr_flag,
                       Uint64 inc_dec_value) {
    /**
     * The mask specifies which columns is to be updated after the interpreter
     * has finished. The values are set in the key_row.
     * We have 7 columns, we will update tot_value_len in interpreter, same with
     * value_start.
     *
     * The rest, redis_key, rondb_key, value_data_type, num_rows and expiry_date
     * are updated through final update.
     */

    const Uint32 mask = 0xAB;
    const unsigned char *mask_ptr = (const unsigned char *)&mask;

    // redis_key already set as this is the Primary key
    key_row->null_bits = 1; // Set rondb_key to NULL, first NULL column
    key_row->num_rows = 0;
    key_row->value_data_type = 0;
    key_row->expiry_date = 0;

    Uint32 code_buffer[128];
    NdbInterpretedCode code(tab, &code_buffer[0], sizeof(code_buffer));
    if (initNdbCodeIncrDecr(response,
                            &code,
                            tab,
                            incr_flag,
                            inc_dec_value) != 0)
        return;

    // Prepare the interpreted program to be part of the write
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_INTERPRETED_INSERT;
    opts.interpretedCode = &code;

    /**
     * Prepare to get the final value of the Redis row after INCR is finished
     * This is performed by the reading the pseudo column that is reading the
     * output index written in interpreter program.
     */
    NdbOperation::GetValueSpec getvals[1];
    getvals[0].appStorage = nullptr;
    getvals[0].recAttr = nullptr;
    getvals[0].column = NdbDictionary::Column::READ_INTERPRETER_OUTPUT_0;
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_GET_FINAL_VALUE;
    opts.numExtraGetFinalValues = 1;
    opts.extraGetFinalValues = getvals;


    if (1)
        opts.optionsPresent |= NdbOperation::OperationOptions::OO_DIRTY_FLAG;

    /* Define the actual operation to be sent to RonDB data node. */
    const NdbOperation *op = trans->writeTuple(
        pk_key_record,
        (const char *)key_row,
        entire_key_record,
        (char *)key_row,
        mask_ptr,
        &opts,
        sizeof(opts));
    if (op == nullptr)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to create NdbOperation",
                                   trans->getNdbError());
        return;
    }

    /* Send to RonDB and execute the INCR operation */
    if (trans->execute(NdbTransaction::Commit,
                       NdbOperation::AbortOnError) != 0 ||
        trans->getNdbError().code != 0)
    {
        if (trans->getNdbError().code == RONDB_KEY_NOT_NULL_ERROR)
        {
            DEB_INCR(("RONDB_KEY_NOT_NULL_ERROR\n"));
            assign_ndb_err_to_response(response,
                                       FAILED_INCR_KEY_MULTI_ROW,
                                       trans->getNdbError());
            return;
        }
        DEB_INCR(("INCR_DECR_ERROR: %d\n", trans->getNdbError().code));
        assign_ndb_err_to_response(response,
                                   FAILED_INCR_KEY,
                                   trans->getNdbError());
        return;
    }

    /* Retrieve the returned new value as an Int64 value */
    NdbRecAttr *recAttr = getvals[0].recAttr;
    Int64 new_incremented_value = recAttr->int64_value();

    DEB_INCR(("INCR/DECR success, new value: %lld\n", new_incremented_value));
    /* Send the return message to Redis client */
    char header_buf[20];
    int header_len = snprintf(header_buf,
                              sizeof(header_buf),
                              ":%lld\r\n",
                              new_incremented_value);
    response->append(header_buf);
    return;
}

static
int get_unique_redis_key_id(const NdbDictionary::Table *tab,
                            Ndb *ndb,
                            Uint64 &redis_key_id,
                            std::string *response) {

    if (ndb->getAutoIncrementValue(tab, redis_key_id, unsigned(1024)) != 0)
    {
        assign_ndb_err_to_response(response,
                                   "Failed to get autoincrement value",
                                   ndb->getNdbError());
        return -1;
    }
    return 0;
}

std::unordered_map<std::string, Uint64> redis_key_id_hash;
int rondb_get_redis_key_id(Ndb *ndb,
                           Uint64 &redis_key_id,
                           const char *key_str,
                           Uint32 key_len,
                           std::string *response) {
    std::string std_key_str = std::string(key_str, key_len);
    auto it = redis_key_id_hash.find(std_key_str);
    if (it == redis_key_id_hash.end()) {
        /* Found no redis_key_id in local hash */
        const NdbDictionary::Dictionary *dict = ndb->getDictionary();
        if (dict == nullptr)
        {
            assign_ndb_err_to_response(response, FAILED_GET_DICT, ndb->getNdbError());
            return -1;
        }
        const NdbDictionary::Table *tab = dict->getTable(HSET_KEY_TABLE_NAME);
        if (tab == nullptr)
        {
            assign_ndb_err_to_response(response, FAILED_CREATE_TABLE_OBJECT, dict->getNdbError());
            return -1;
        }
        int ret_code = get_unique_redis_key_id(tab,
                                               ndb,
                                               redis_key_id,
                                               response);
        if (ret_code < 0) {
            DEB_HSET_KEY(("Failed get_unique_redis_key_id, err: %d\n", ret_code));
            return -1;
        }
        ret_code = write_hset_key_table(ndb,
                                        tab,
                                        std_key_str,
                                        redis_key_id,
                                        response);
        if (ret_code < 0) {
            DEB_HSET_KEY(("Failed write_hset_key_table, err: %d\n", ret_code));
            return -1;
        }
        redis_key_id_hash[std_key_str] = redis_key_id;
        DEB_HSET_KEY(("Created redis_key_id = %llu for key: %s\n",
          redis_key_id, key_str));
    } else {
        redis_key_id = it->second;
        /* Found local redis_key_id */
        DEB_HSET_KEY(("Found local redis_key_id = %llu for key: %s\n",
          redis_key_id, key_str));
    }
    return 0;
}
