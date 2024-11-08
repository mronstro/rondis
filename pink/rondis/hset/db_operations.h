#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#ifndef HSET_DB_OPERATIONS_H
#define HSET_DB_OPERATIONS_H

const Uint32 ROWS_PER_READ = 2;

int create_field_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Table *tab,
                     NdbTransaction *trans,
                     Uint64 field_key,
                     const char *field_str,
                     Uint32 field_len,
                     const char *value_str,
                     Uint32 tot_value_len,
                     Uint32 num_value_rows,
                     Uint32 row_state,
                     char *buf);

void write_data_to_field_op(NdbOperation *ndb_op,
                            Uint64 field_key,
                            const char *field_str,
                            Uint32 field_len,
                            const char *value_str,
                            Uint32 tot_value_len,
                            Uint32 num_value_rows,
                            Uint32 row_state,
                            char *buf);

int delete_and_insert_field_row(std::string *response,
                                Ndb *ndb,
                                const NdbDictionary::Table *tab,
                                NdbTransaction *trans,
                                Uint64 field_key,
                                const char *field_str,
                                Uint32 field_len,
                                const char *value_str,
                                Uint32 tot_value_len,
                                Uint32 num_value_rows,
                                Uint32 row_state,
                                char *buf);

int delete_field_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Table *tab,
                     NdbTransaction *trans,
                     const char *field_str,
                     Uint32 field_len,
                     char *buf);

int insert_field_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Table *tab,
                     NdbTransaction *trans,
                     Uint64 field_key,
                     const char *field_str,
                     Uint32 field_len,
                     const char *value_str,
                     Uint32 tot_value_len,
                     Uint32 num_value_rows,
                     Uint32 row_state,
                     char *buf);

int create_value_row(std::string *response,
                     Ndb *ndb,
                     const NdbDictionary::Dictionary *dict,
                     NdbTransaction *trans,
                     const char *start_value_ptr,
                     Uint64 field_key,
                     Uint32 this_value_len,
                     Uint32 ordinal,
                     char *buf);

int create_all_value_rows(std::string *response,
                          Ndb *ndb,
                          const NdbDictionary::Dictionary *dict,
                          NdbTransaction *trans,
                          Uint64 field_key,
                          const char *value_str,
                          Uint32 value_len,
                          Uint32 num_value_rows,
                          char *buf);

/*
    Since the beginning of the value is saved within the key table, it
    can suffice to read the key table to get the value. If the value is
*/
int get_simple_field_row(std::string *response,
                         const NdbDictionary::Table *tab,
                         Ndb *ndb,
                         NdbTransaction *trans,
                         struct field_table *field_row,
                         Uint32 field_len);

int get_complex_field_row(std::string *response,
                          const NdbDictionary::Dictionary *dict,
                          const NdbDictionary::Table *tab,
                          Ndb *ndb,
                          NdbTransaction *trans,
                          struct field_table *row,
                          Uint32 field_len);

int get_value_rows(std::string *response,
                   Ndb *ndb,
                   const NdbDictionary::Dictionary *dict,
                   NdbTransaction *trans,
                   const Uint32 num_rows,
                   const Uint64 field_key,
                   const Uint32 tot_value_len);

int read_batched_value_rows(std::string *response,
                            NdbTransaction *trans,
                            const Uint64 field_key,
                            const Uint32 num_rows_to_read,
                            const Uint32 start_ordinal,
                            const NdbTransaction::ExecType commit_type);

int rondb_get_field_key(const NdbDictionary::Table *tab,
                        Uint64 &field_key,
                        Ndb *ndb,
                        std::string *response);

void incr_field_row(std::string *response,
                    Ndb *ndb,
                    const NdbDictionary::Table *tab,
                    NdbTransaction *trans,
                    struct field_table *field_row);
#endif
