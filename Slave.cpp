/* Copyright 2011 ZAO "Begun".
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
*/


#include <regex>
#include <unistd.h>

#include "Slave.h"
#include "SlaveStats.h"
#include "Logging.h"
#include "nanomysql.h"

#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#include <mysql/my_global.h>
#include <mysql/m_ctype.h>
#include <mysql/sql_common.h>

#define packet_end_data 1

#define ER_NET_PACKET_TOO_LARGE 1153
#define ER_MASTER_FATAL_ERROR_READING_BINLOG 1236
#define BIN_LOG_HEADER_SIZE 4


namespace
{
unsigned char *net_store_length_fast(unsigned char *pkg, unsigned int length)
{
    unsigned char *packet=(unsigned char*) pkg;
    if (length < 251)
    {
        *packet=(unsigned char) length;
        return packet+1;
    }
    *packet++=252;
    int2store(packet,(unsigned int) length);
    return packet+2;
}

unsigned char *net_store_data(unsigned char *to, const unsigned char *from, unsigned int length)
{
    to = net_store_length_fast(to,length);
    ::memcpy(to,from,length);
    return to+length;
}

std::string get_hostname()
{
    char buf[256];
    if (::gethostname(buf, 255) == -1)
    {
        LOG_ERROR(log, "Failed to invoke gethostname()");
        return "0.0.0.0";
    }
    return std::string(buf);
}

const char* binlog_checksum_type_names[] =
{
    "NONE",
    "CRC32",
    nullptr
};

unsigned int binlog_checksum_type_length[] =
{
    sizeof("NONE") - 1,
    sizeof("CRC32") - 1,
    0
};

TYPELIB binlog_checksum_typelib =
{
    array_elements(binlog_checksum_type_names) - 1, "",
    binlog_checksum_type_names,
    binlog_checksum_type_length
};

}// anonymous-namespace


using namespace slave;


void Slave::init()
{

    LOG_TRACE(log, "Initializing libslave...");

    check_master_version();

    check_master_binlog_format();

    ext_state.loadMasterInfo( m_master_info.master_log_name, m_master_info.master_log_pos);

    LOG_TRACE(log, "Libslave initialized OK");
}


void Slave::close_connection()
{
    ::shutdown(mysql.net.fd, SHUT_RDWR);
    ::close(mysql.net.fd);
}


void Slave::createDatabaseStructure_(table_order_t& tabs, RelayLogInfo& rli) const
{
    LOG_TRACE(log, "enter: createDatabaseStructure");

    nanomysql::Connection conn(m_master_info.conn_options);
    const collate_map_t collate_map = readCollateMap(conn);


    for (table_order_t::const_iterator it = tabs.begin(); it != tabs.end(); ++ it) {

        LOG_INFO( log, "Creating database structure for: " << it->first << ", Creating table for: " << it->second );
        createTable(rli, it->first, it->second, collate_map, conn);
    }

    LOG_TRACE(log, "exit: createDatabaseStructure");
}



void Slave::createTable(RelayLogInfo& rli,
                        const std::string& db_name, const std::string& tbl_name,
                        const collate_map_t& collate_map, nanomysql::Connection& conn) const
{
    LOG_TRACE(log, "enter: createTable " << db_name << " " << tbl_name);

    conn.select_db(db_name);

    nanomysql::fields_t fields;
    conn.get_fields(tbl_name, fields);

    nanomysql::Connection::result_t res;
    conn.query("SHOW FULL COLUMNS FROM " + tbl_name);
    conn.store(res);

    std::shared_ptr<Table> table(new Table(db_name, tbl_name));


    LOG_DEBUG(log, "Created new Table object: database:" << db_name << " table: " << tbl_name );

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        //row.at(0) - field name
        //row.at(1) - field type
        //row.at(2) - collation
        //row.at(3) - can be null

        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Field");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Field'");

        std::string name = z->second.data;

        z = i->find("Type");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Type'");

        std::string stype = z->second.data;

        z = i->find("Null");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Null'");

        const auto fi = fields.find(name);
        if (fi == fields.end()) {
            throw std::runtime_error("Slave::create_table(): no field record for '" + name + "'");
        }

        const auto& field = fi->second;
        collate_info ci;
        if (field.type == MYSQL_TYPE_VARCHAR || field.type == MYSQL_TYPE_VAR_STRING || field.type == MYSQL_TYPE_STRING)
        {
            z = i->find("Collation");
            if (z == i->end())
                throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Collation' for field '" + name + "'");
            const std::string collate = z->second.data;
            collate_map_t::const_iterator it = collate_map.find(collate);
            if (collate_map.end() == it)
                throw std::runtime_error("Slave::create_table(): cannot find collate '" + collate + "' from field "
                                         + name + " type " + stype + " in collate info map");
            ci = it->second;
            LOG_DEBUG(log, "Created column: name-type: " << name << " - " << stype
                      << " Field type: " << field.type << " Length: " << field.length << " Collation: " << ci.name);
        }
        else
            LOG_DEBUG(log, "Created column: name-type: " << name << " - " << stype
                      << " Field type: " << field.type << " Length: " << field.length );

        PtrField pfield;

        switch (field.type) {
         // case MYSQL_TYPE_DECIMAL:
         // case MYSQL_TYPE_NEWDECIMAL:
         //     pfield = PtrField(new Field_decimal(name, field.length, field.decimals));
         //     break;
            case MYSQL_TYPE_TINY:
                pfield = field.flags & UNSIGNED_FLAG ? PtrField(new Field_num<uint16, 1>(name)) : PtrField(new Field_num<int16, 1>(name));
                break;
            case MYSQL_TYPE_SHORT:
                pfield = field.flags & UNSIGNED_FLAG ? PtrField(new Field_num<uint16>(name)) : PtrField(new Field_num<int16>(name));
                break;
            case MYSQL_TYPE_INT24:
                pfield = field.flags & UNSIGNED_FLAG ? PtrField(new Field_num<uint32, 3>(name)) : PtrField(new Field_num<int32, 3>(name));
                break;
            case MYSQL_TYPE_LONG:
                pfield = field.flags & UNSIGNED_FLAG ? PtrField(new Field_num<uint32>(name)) : PtrField(new Field_num<int32>(name));
                break;
            case MYSQL_TYPE_LONGLONG:
                pfield = field.flags & UNSIGNED_FLAG ? PtrField(new Field_num<ulonglong>(name)) : PtrField(new Field_num<longlong>(name));
                break;
            case MYSQL_TYPE_FLOAT:
                pfield = PtrField(new Field_num<float>(name));
                break;
            case MYSQL_TYPE_DOUBLE:
                pfield = PtrField(new Field_num<double>(name));
                break;
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
                pfield = PtrField(new Field_timestamp(name, field.decimals, m_master_info.is_old_storage));
                break;
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_TIME2:
                pfield = PtrField(new Field_time(name, field.decimals, m_master_info.is_old_storage));
                break;
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
                pfield = PtrField(new Field_datetime(name, field.decimals, m_master_info.is_old_storage));
                break;
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                pfield = PtrField(new Field_date(name));
                break;
            case MYSQL_TYPE_YEAR:
                pfield = PtrField(new Field_year(name));
                break;
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_VAR_STRING:
                pfield = PtrField(new Field_string(name, field.length, ci));
                break;
         // case MYSQL_TYPE_ENUM:
         // case MYSQL_TYPE_SET:
            case MYSQL_TYPE_STRING:
                if (field.flags & ENUM_FLAG) {
                    pfield = PtrField(new Field_enum(name, stype));
                    break;
                } else if (field.flags & SET_FLAG) {
                    pfield = PtrField(new Field_set(name, stype));
                    break;
                } else {
                    pfield = PtrField(new Field_string(name, field.length, ci));
                    break;
                }
            case MYSQL_TYPE_BIT:
                pfield = PtrField(new Field_bit(name, field.length));
                break;
         // case MYSQL_TYPE_TINY_BLOB:
         // case MYSQL_TYPE_MEDIUM_BLOB:
         // case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                pfield = PtrField(new Field_blob(name, field.length));
                break;
            default:
                LOG_ERROR(log, "Slave::create_table(): class name don't exist for type: " << field.type );
                throw std::runtime_error("Slave::create_table(): error in field '" + name + "'");
        }

        table->fields.push_back(pfield);

    }


    rli.setTable(tbl_name, db_name, table);

}

namespace
{
struct raii_mysql_connector
{
    MYSQL* mysql;
    MasterInfo& m_master_info;
    ExtStateIface &ext_state;

    raii_mysql_connector(MYSQL* m, MasterInfo& mmi, ExtStateIface &state) : mysql(m), m_master_info(mmi), ext_state(state) {

        connect(false);
    }

    ~raii_mysql_connector() {

        end_server(mysql);
        mysql_close(mysql);
    }

    void connect(bool reconnect) {

        LOG_TRACE(log, "enter: connect_to_master");

        ext_state.setConnecting();

        if (reconnect) {
            end_server(mysql);
            mysql_close(mysql);
        }

        if (!(mysql_init(mysql))) {

            throw std::runtime_error("Slave::reconnect() : mysql_init() : could not initialize mysql structure");
        }

        bool was_error = reconnect;
        const auto& sConnOptions = m_master_info.conn_options;
        nanomysql::Connection::setOptions(mysql, sConnOptions);

        while (mysql_real_connect(mysql,
                                  sConnOptions.mysql_host.c_str(),
                                  sConnOptions.mysql_user.c_str(),
                                  sConnOptions.mysql_pass.c_str(), 0, sConnOptions.mysql_port, 0, CLIENT_REMEMBER_OPTIONS)
               == 0) {


            ext_state.setConnecting();
            if(!was_error) {
                LOG_ERROR(log, "Couldn't connect to mysql master " << sConnOptions.mysql_host << ":" << sConnOptions.mysql_port);
                was_error = true;
            }

            LOG_TRACE(log, "try connect to master");
            LOG_TRACE(log, "connect_retry = " << m_master_info.connect_retry << ", reconnect = " << reconnect);

            //
            ::sleep(m_master_info.connect_retry);
        }

        if(was_error)
            LOG_INFO(log, "Successfully connected to " << m_master_info.conn_options.mysql_host << ":" << m_master_info.conn_options.mysql_port);


        mysql->reconnect = 1;

        LOG_TRACE(log, "exit: connect_to_master");
    }
};
}// anonymous-namespace


void Slave::get_remote_binlog(const std::function<bool()>& _interruptFlag)
{
    int count_packet = 0;

    generateSlaveId();

    // Moved to Slave member
    // MYSQL mysql;

    raii_mysql_connector __conn(&mysql, m_master_info, ext_state);

    //connect_to_master(false, &mysql);

    register_slave_on_master(&mysql);

connected:
    do_checksum_handshake(&mysql);

    // Get binlog position saved in ext_state before, or load it
    // from persistent storage. Get false if failed to get binlog position.
    if( !ext_state.getMasterInfo(
                m_master_info.master_log_name,
                m_master_info.master_log_pos) ) {
        // If there is not binlog position saved before,
        // get last binlog name and last binlog position.
        std::pair<std::string,unsigned int> row = getLastBinlog();

        m_master_info.master_log_name = row.first;
        m_master_info.master_log_pos = row.second;

        ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);
        ext_state.saveMasterInfo();
    }

    LOG_INFO(log, "Starting from binlog_name:binlog_pos : " << m_master_info.master_log_name
             << ":" << m_master_info.master_log_pos );


    request_dump(m_master_info.master_log_name, m_master_info.master_log_pos, &mysql);

    while (!_interruptFlag()) {

        try {

            LOG_TRACE(log, "-- reading event --");

            unsigned long len = read_event(&mysql);

            ext_state.setStateProcessing(true);

            count_packet++;
            LOG_TRACE(log, "Got event with length: " << len << " Packet number: " << count_packet );

            // end of data

            if (len == packet_error || len == packet_end_data) {

                uint mysql_error_number = mysql_errno(&mysql);

                switch(mysql_error_number) {
                    case ER_NET_PACKET_TOO_LARGE:
                        LOG_ERROR(log, "Myslave: Log entry on master is longer than max_allowed_packet on "
                                  "slave. If the entry is correct, restart the server with a higher value of "
                                  "max_allowed_packet. max_allowed_packet=" << mysql_error(&mysql) );
                        break;
                    case ER_MASTER_FATAL_ERROR_READING_BINLOG: // Error -- unknown binlog file.
                        LOG_ERROR(log, "Myslave: fatal error reading binlog. " <<  mysql_error(&mysql) );
                        break;
                    case 2013: // Processing error 'Lost connection to MySQL'
                        LOG_WARNING(log, "Myslave: Error from MySQL: " << mysql_error(&mysql) );
                        // Check if connection closed by user for exiting from the loop
                        if (_interruptFlag())
                        {
                            LOG_INFO(log, "Interrupt flag is true, breaking loop");
                            continue;
                        }
                        break;
                    default:
                        LOG_ERROR(log, "Myslave: Error reading packet from server: " << mysql_error(&mysql)
                                << "; mysql_error: " << mysql_errno(&mysql));
                        break;
                }

                __conn.connect(true);

                goto connected;
            } // len == packet_error

            // Ok event

            if (len == packet_end_data) {
                continue;
            }

            slave::Basic_event_info event;

            if (!slave::read_log_event((const char*) mysql.net.read_pos + 1,
                                       len - 1,
                                       event,
                                       event_stat,
                                       masterGe56(),
                                       m_master_info)) {

                LOG_TRACE(log, "Skipping unknown event.");
                continue;
            }

            //

            LOG_TRACE(log, "Event log position: " << event.log_pos );

            if (event.log_pos != 0) {
                m_master_info.master_log_pos = event.log_pos;
                ext_state.setLastEventTimePos(event.when, event.log_pos);
            }

            LOG_TRACE(log, "seconds_behind_master: " << (::time(NULL) - event.when) );


            // MySQL5.1.23 binlogs can be read only starting from a XID_EVENT
            // MySQL5.1.23 ev->log_pos -- the binlog offset

            if (event.type == XID_EVENT) {

                ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);

                LOG_TRACE(log, "Got XID event. Using binlog name:pos: "
                          << m_master_info.master_log_name << ":" << m_master_info.master_log_pos);


                if (m_xid_callback)
                    m_xid_callback(event.server_id);

            } else  if (event.type == ROTATE_EVENT) {

                slave::Rotate_event_info rei(event.buf, event.event_len);

                /*
                 * new_log_ident - new binlog name
                 * pos - position of the starting event
                 */

                LOG_INFO(log, "Got rotate event.");

                /* WTF
                 */

                if (event.when == 0) {

                    //LOG_TRACE(log, "ROTATE_FAKE");
                }

                m_master_info.master_log_name = rei.new_log_ident;
                m_master_info.master_log_pos = rei.pos; // this will always be equal to 4

                ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);

                LOG_TRACE(log, "new position is " << m_master_info.master_log_name << ":" << m_master_info.master_log_pos);
                LOG_TRACE(log, "ROTATE_EVENT processed OK.");
            }


            if (process_event(event, m_rli, m_master_info.master_log_pos)) {

                LOG_TRACE(log, "Error in processing event.");
            }



        } catch (const std::exception& _ex ) {

            LOG_ERROR(log, "Met exception in get_remote_binlog cycle. Message: " << _ex.what() );
            if (event_stat)
                event_stat->tickError();
            usleep(1000*1000);
            continue;

        }

    } //while

    LOG_WARNING(log, "Binlog monitor was stopped. Binlog events are not listened.");

    deregister_slave_on_master(&mysql);
}

std::map<std::string,std::string> Slave::getRowType(const std::string& db_name,
                                                    const std::set<std::string>& tbl_names) const
{
    nanomysql::Connection conn(m_master_info.conn_options);
    nanomysql::Connection::result_t res;

    conn.query("SHOW TABLE STATUS FROM " + db_name);
    conn.store(res);

    std::map<std::string,std::string> ret;

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        if (i->size() <= 3) {
            LOG_ERROR(log, "WARNING: Broken SHOW TABLE STATUS FROM " << db_name);
            continue;
        }

        //row[0] - the table name
        //row[3] - row_format

        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Name");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): SHOW TABLE STATUS query did not return 'Name'");

        std::string name = z->second.data;

        z = i->find("Row_format");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): SHOW TABLE STATUS query did not return 'Row_format'");

        std::string format = z->second.data;

        if (tbl_names.count(name) != 0) {

            ret[name] = format;

            LOG_DEBUG(log, name << " row_type = " << format);
        }
    }

    return ret;
}

void Slave::register_slave_on_master(MYSQL* mysql)
{
    uchar buf[1024], *pos= buf;

    unsigned int report_user_len=0, report_password_len=0;

    const std::string report_host = get_hostname();

    const char* report_user = "begun_slave";
    const char* report_password = "begun_slave";
    unsigned int report_port = 0;
    unsigned long rpl_recovery_rank = 0;

    report_user_len= strlen(report_user);
    report_password_len= strlen(report_password);

    LOG_DEBUG(log, "Registering slave on master: m_server_id = " << m_server_id << "...");

    int4store(pos, m_server_id);
    pos+= 4;
    pos= net_store_data(pos, (uchar*)report_host.c_str(), report_host.size());
    pos= net_store_data(pos, (uchar*)report_user, report_user_len);
    pos= net_store_data(pos, (uchar*)report_password, report_password_len);
    int2store(pos, (unsigned short) report_port);
    pos+= 2;
    int4store(pos, rpl_recovery_rank);
    pos+= 4;

    /* The master will fill in master_id */
    int4store(pos, 0);
    pos+= 4;

    if (simple_command(mysql, COM_REGISTER_SLAVE, buf, (size_t) (pos-buf), 0)) {

        LOG_ERROR(log, "Unable to register slave.");
        throw std::runtime_error("Slave::register_slave_on_master(): Error registring on slave: " +
                                 std::string(mysql_error(mysql)));
    }

    LOG_TRACE(log, "Success registering slave on master");
}

void Slave::deregister_slave_on_master(MYSQL* mysql)
{
    LOG_DEBUG(log, "Deregistering slave on master: m_server_id = " << m_server_id << "...");
    // Last '1' means 'no checking', otherwise command can hung
    simple_command(mysql, COM_QUIT, 0, 0, 1);
}

void Slave::check_master_version()
{
    nanomysql::Connection conn(m_master_info.conn_options);
    nanomysql::Connection::result_t res;

    conn.query("SELECT VERSION()");
    conn.store(res);

    if (res.size() == 1 && res[0].size() == 1)
    {
        std::string tmp = res[0].begin()->second.data;
        int major, minor, patch;
        if (3 == sscanf(tmp.c_str(), "%d.%d.%d", &major, &minor, &patch))
        {
            m_master_version = major * 10000 + minor * 100 + patch;
            // since 5.6.4 storage for temporal types has changed
            m_master_info.is_old_storage = m_master_version < 50604;
            static const int min_version = 50123;   // 5.1.23
            if (m_master_version >= min_version)
                return;
        }
        throw std::runtime_error("Slave::check_master_version(): got invalid version: " + tmp);
    }

    throw std::runtime_error("Slave::check_master_version(): could not SELECT VERSION()");
}

void Slave::check_master_binlog_format()
{
    nanomysql::Connection conn(m_master_info.conn_options);
    nanomysql::Connection::result_t res;

    conn.query("SHOW GLOBAL VARIABLES LIKE 'binlog_format'");
    conn.store(res);

    if (res.size() == 1 && res[0].size() == 2) {

        std::map<std::string,nanomysql::field>::const_iterator z = res[0].find("Value");

        if (z == res[0].end())
            throw std::runtime_error("Slave::check_binlog_format(): SHOW GLOBAL VARIABLES query did not return 'Value'");

        std::string tmp = z->second.data;

        if (tmp == "ROW") {
            return;

        } else {
            throw std::runtime_error("Slave::check_binlog_format(): got invalid binlog format: " + tmp);
        }
    }


    throw std::runtime_error("Slave::check_binlog_format(): Could not SHOW GLOBAL VARIABLES LIKE 'binlog_format'");
}

void Slave::do_checksum_handshake(MYSQL* mysql)
{
    const char query[] = "SET @master_binlog_checksum= @@global.binlog_checksum";

    if (mysql_real_query(mysql, query, static_cast<ulong>(strlen(query))))
    {
        if (mysql_errno(mysql) != ER_UNKNOWN_SYSTEM_VARIABLE)
        {
            mysql_free_result(mysql_store_result(mysql));
            throw std::runtime_error("Slave::do_checksum_handshake(MYSQL* mysql): query 'SET @master_binlog_checksum= @@global.binlog_checksum' failed");
        }
        mysql_free_result(mysql_store_result(mysql));
    }
    else
    {
        mysql_free_result(mysql_store_result(mysql));
        MYSQL_RES* master_res = nullptr;
        MYSQL_ROW master_row = nullptr;
        const char select_query[] = "SELECT @master_binlog_checksum";

        if (!mysql_real_query(mysql, select_query, static_cast<ulong>(strlen(select_query))) &&
            (master_res = mysql_store_result(mysql)) &&
            (master_row = mysql_fetch_row(master_res)) &&
            (master_row[0] != NULL))
        {
            m_master_info.checksum_alg = static_cast<enum_binlog_checksum_alg>(find_type(master_row[0], &binlog_checksum_typelib, 1) - 1);
        }

        if (master_res)
            mysql_free_result(master_res);

        if (m_master_info.checksum_alg != BINLOG_CHECKSUM_ALG_OFF && m_master_info.checksum_alg != BINLOG_CHECKSUM_ALG_CRC32)
            throw std::runtime_error("Slave::do_checksum_handshake(MYSQL* mysql): unknown checksum algorithm");
    }

    LOG_TRACE(log, "Success doing checksum handshake");
}



namespace
{
std::string checkAlterOrCreateQuery(const std::string& str)
{
    static const std::regex query_regex(R"(\s*(?:alter\s+table|create\s+table(?:\s+if\s+not\s+exists)?)\s+(?:\w+\.)?(\w+)(?:[^\w\.].*$|$))",
                                        std::regex_constants::optimize | std::regex_constants::icase);
    std::smatch sm;
    if (std::regex_match(str, sm, query_regex))
        return sm[1];
    return "";
}
}// anonymouos-namespace



int Slave::process_event(const slave::Basic_event_info& bei, RelayLogInfo &m_rli, unsigned long long pos)
{


    if (bei.when < 0 &&
        bei.type != FORMAT_DESCRIPTION_EVENT)
        return 0;

    switch (bei.type) {

    case QUERY_EVENT:
    {
        // Check for ALTER TABLE or CREATE TABLE

        slave::Query_event_info qei(bei.buf, bei.event_len);

        LOG_TRACE(log, "Received QUERY_EVENT: " << qei.query);

        const auto tbl_name = checkAlterOrCreateQuery(qei.query);
        if (!tbl_name.empty())
        {
            const auto key = std::make_pair(qei.db_name, tbl_name);
            if (m_table_order.count(key) == 1)
            {
                LOG_DEBUG(log, "Rebuilding database structure.");
                table_order_t order {key};
                createDatabaseStructure_(order, m_rli);
                auto it = m_rli.m_table_map.find(key);
                if (it != m_rli.m_table_map.end())
                {
                    it->second->m_callback = m_callbacks[key];
                    it->second->m_filter = m_filters[key];
                }
            }
        }
        break;
    }

    case TABLE_MAP_EVENT:
    {
        LOG_TRACE(log, "Got TABLE_MAP_EVENT.");

        slave::Table_map_event_info tmi(bei.buf, bei.event_len);

        m_rli.setTableName(tmi.m_table_id, tmi.m_tblnam, tmi.m_dbnam);

        if (m_master_version >= 50604)
        {
            auto table = m_rli.getTable(std::make_pair(tmi.m_dbnam, tmi.m_tblnam));
            if (table && tmi.m_cols_types.size() == table->fields.size())
            {
                int i = 0;
                for (const auto& x : tmi.m_cols_types)
                {
                    switch (x)
                    {
                    case MYSQL_TYPE_TIMESTAMP:
                    case MYSQL_TYPE_DATETIME:
                    case MYSQL_TYPE_TIME:
                        static_cast<Field_temporal*>(table->fields[i].get())->reset(true);
                        break;
                    case MYSQL_TYPE_TIMESTAMP2:
                    case MYSQL_TYPE_DATETIME2:
                    case MYSQL_TYPE_TIME2:
                        static_cast<Field_temporal*>(table->fields[i].get())->reset(false);
                        break;
                    default:
                        break;
                    }
                    i++;
                }
            }
        }

        if (event_stat)
            event_stat->processTableMap(tmi.m_table_id, tmi.m_tblnam, tmi.m_dbnam);

        break;
    }

    case WRITE_ROWS_EVENT_V1:
    case UPDATE_ROWS_EVENT_V1:
    case DELETE_ROWS_EVENT_V1:
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
    {
        LOG_TRACE(log, "Got " << (bei.type == WRITE_ROWS_EVENT_V1 || bei.type == WRITE_ROWS_EVENT ? "WRITE" :
                                  bei.type == DELETE_ROWS_EVENT_V1 || bei.type == DELETE_ROWS_EVENT ? "DELETE" :
                                  "UPDATE") << "_ROWS_EVENT");

        Row_event_info roi(bei.buf, bei.event_len, (bei.type == UPDATE_ROWS_EVENT_V1 || bei.type == UPDATE_ROWS_EVENT), masterGe56());

        apply_row_event(m_rli, bei, roi, ext_state, event_stat);

        break;
    }

    default:
        break;
    }

    return 0;
}

void Slave::request_dump(const std::string& logname, unsigned long start_position, MYSQL* mysql)
{
    uchar buf[128];

    /*
    COM_BINLOG_DUMP accepts only 4 bytes for the position, so we are forced to
    cast to uint32.
    */

    //
    //start_position = 4;

    int binlog_flags = 0;
    int4store(buf, (uint32)start_position);
    int2store(buf + BIN_LOG_HEADER_SIZE, binlog_flags);

    uint logname_len = logname.size();
    int4store(buf + 6, m_server_id);

    memcpy(buf + 10, logname.data(), logname_len);

    if (simple_command(mysql, COM_BINLOG_DUMP, buf, logname_len + 10, 1)) {

        LOG_ERROR(log, "Error sending COM_BINLOG_DUMP");
        throw std::runtime_error("Error in sending COM_BINLOG_DUMP");
    }
}

ulong Slave::read_event(MYSQL* mysql)
{

    ulong len;
    ext_state.setStateProcessing(false);

#if MYSQL_VERSION_ID < 50705
    len = cli_safe_read(mysql);
#else
    len = cli_safe_read(mysql, nullptr);
#endif

    if (len == packet_error) {
        LOG_ERROR(log, "Myslave: Error reading packet from server: " << mysql_error(mysql)
                  << "; mysql_error: " << mysql_errno(mysql));

        return packet_error;
    }

    // check for end-of-data
    if (len < 8 && mysql->net.read_pos[0] == 254) {

        LOG_ERROR(log, "read_event(): end of data\n");
        return packet_end_data;
    }

    return len;
}

void Slave::generateSlaveId()
{

    std::set<unsigned int> server_ids;

    nanomysql::Connection conn(m_master_info.conn_options);
    nanomysql::Connection::result_t res;

    conn.query("SHOW SLAVE HOSTS");
    conn.store(res);

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        //row[0] - server_id

        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Server_id");

        if (z == i->end())
            throw std::runtime_error("Slave::generateSlaveId(): SHOW SLAVE HOSTS query did not return 'Server_id'");

        server_ids.insert(::strtoul(z->second.data.c_str(), NULL, 10));
    }

    unsigned int serveroid = ::time(NULL);
    serveroid ^= (::getpid() << 16);

    while (1) {

        if (server_ids.count(serveroid) != 0) {
            serveroid++;
        } else {
            break;
        }
    }

    m_server_id = serveroid;

    LOG_DEBUG(log, "Generated m_server_id = " << m_server_id);
}

Slave::binlog_pos_t Slave::getLastBinlog() const
{
    nanomysql::Connection conn(m_master_info.conn_options);
    nanomysql::Connection::result_t res;

    static const std::string query = "SHOW MASTER STATUS";
    conn.query(query);
    conn.store(res);

    if (res.size() == 1) {

        std::map<std::string,nanomysql::field>::const_iterator z = res[0].find("File");

        if (z == res[0].end())
            throw std::runtime_error("Slave::getLastBinlog(): " + query + " query did not return 'File'");

        std::string file = z->second.data;

        z = res[0].find("Position");

        if (z == res[0].end())
            throw std::runtime_error("Slave::getLastBinlog(): " + query + " query did not return 'Position'");

        std::string pos = z->second.data;

        return std::make_pair(file, ::strtoul(pos.c_str(), NULL, 10));
    }

    throw std::runtime_error("Slave::getLastBinLog(): Could not " + query);
}
// vim: et ts=4
