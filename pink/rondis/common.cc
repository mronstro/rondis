#include <stdarg.h>
#include "pink/include/redis_conn.h"
#include <ndbapi/NdbApi.hpp>
#include <ndbapi/Ndb.hpp>

#include "common.h"

void set_length(char *buf, Uint32 key_len)
{
    Uint8 *ptr = (Uint8 *)buf;
    ptr[0] = (Uint8)(key_len & 255);
    ptr[1] = (Uint8)(key_len >> 8);
}

Uint32 get_length(char *buf)
{
    Uint8 *ptr = (Uint8 *)buf;
    Uint8 low = ptr[0];
    Uint8 high = ptr[1];
    Uint32 len32 = Uint32(low) + Uint32(256) * Uint32(high);
    return len32;
}

void assign_ndb_err_to_response(
    std::string *response,
    const char *app_str,
    NdbError error)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s; NDB(%u) %s\r\n", app_str, error.code, error.message);
    std::cout << buf;
    response->assign(buf);
}

void assign_generic_err_to_response(
    std::string *response,
    const char *app_str)
{
    char buf[512];
    snprintf(buf, sizeof(buf), "-ERR %s\r\n", app_str);
    std::cout << buf;
    response->assign(buf);
}
