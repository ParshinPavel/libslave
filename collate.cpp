#include "nanomysql.h"
#include "collate.h"

#include <map>
#include <mysql.h>
#include <stdexcept>
#include <string>

using namespace slave;

collate_map_t slave::readCollateMap(nanomysql::Connection& conn)
{
    collate_map_t res;
    nanomysql::Connection::result_t nanores;

    typedef std::map<std::string, int> charset_maxlen_t;
    charset_maxlen_t cm;

    conn.query("SHOW CHARACTER SET");
    conn.store(nanores);

    for (const auto& nanore : nanores)
    {
        auto z = nanore.find("Charset");
        if (z == nanore.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW CHARACTER SET query did not return 'Charset'");
        const std::string name = z->second.data;

        z = nanore.find("Maxlen");
        if (z == nanore.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW CHARACTER SET query did not return 'Maxlen'");

        const int maxlen = atoi(z->second.data.c_str());

        cm[name] = maxlen;
    }

    nanores.clear();
    conn.query("SHOW COLLATION");
    conn.store(nanores);

    for (const auto& nanore : nanores)
    {
        collate_info ci;

        auto z = nanore.find("Collation");
        if (z == nanore.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION query did not return 'Collation'");
        ci.name = z->second.data;

        z = nanore.find("Charset");
        if (z == nanore.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION query did not return 'Charset'");
        ci.charset = z->second.data;

        charset_maxlen_t::const_iterator j = cm.find(ci.charset);
        if (j == cm.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION returns charset not shown in SHOW CHARACTER SET"
                    " (collation '" + ci.name + "', charset '" + ci.charset + "')");

        ci.maxlen = j->second;

        res[ci.name] = ci;
    }

    return res;
}
