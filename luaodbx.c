/**
 * luaodbx - OpenDBX driver
 * (c) 2009-19 Alacner zhang <alacner@gmail.com>
 * This content is released under the MIT License.
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define LUA_ODBX_VERSION "1.0.0"

#include <odbx.h>

#include "lua.h"
#include "lauxlib.h"
#if ! defined (LUA_VERSION_NUM) || LUA_VERSION_NUM < 501
#include "compat-5.1.h"
#endif

/* Compatibility between Lua 5.1+ and Lua 5.0 */
#ifndef LUA_VERSION_NUM
#define LUA_VERSION_NUM 0
#endif
#if LUA_VERSION_NUM < 501
#define luaL_register(a, b, c) luaL_openlib((a), (b), (c), 0)
#endif

#define LUA_ODBX_CONN "OpenDBX connection"
#define LUA_ODBX_RES "OpenDBX result"
#define LUA_ODBX_LO "OpenDBX large object"
#define LUA_ODBX_TABLENAME "odbx"

#define safe_emalloc(nmemb, size, offset)  malloc((nmemb) * (size) + (offset)) 

typedef struct {
	short   closed;
} pseudo_data;

typedef struct {
	short   closed;
	int     env;
	odbx_t  *conn;
} lua_odbx_conn;

typedef struct {
	short   closed;
	int     conn;               /* reference to connection */
	odbx_result_t *res;
} lua_odbx_res;

typedef struct {
	short      closed;
	int        conn;               /* reference to connection */
	odbx_lo_t *lo;
} lua_odbx_lo;

void luaM_setmeta (lua_State *L, const char *name);
int luaM_register (lua_State *L, const char *name, const luaL_reg *methods);
int luaopen_odbx (lua_State *L);
void luaM_regconst(lua_State *L, const char *name, long value);

/* set const */
static int luaM_const (lua_State *L, const char *defined) {

	lua_newtable(L);

	luaM_regconst(L, "ODBX_ERR_SUCCESS", ODBX_ERR_SUCCESS);
	luaM_regconst(L, "-ODBX_ERR_BACKEND", -ODBX_ERR_BACKEND);
	luaM_regconst(L, "-ODBX_ERR_NOCAP", -ODBX_ERR_NOCAP);
	luaM_regconst(L, "-ODBX_ERR_PARAM", -ODBX_ERR_PARAM);
	luaM_regconst(L, "-ODBX_ERR_NOMEM", -ODBX_ERR_NOMEM);
	luaM_regconst(L, "-ODBX_ERR_SIZE", -ODBX_ERR_SIZE);
	luaM_regconst(L, "-ODBX_ERR_NOTEXIST", -ODBX_ERR_NOTEXIST);
	luaM_regconst(L, "-ODBX_ERR_NOOP", -ODBX_ERR_NOOP);
	luaM_regconst(L, "-ODBX_ERR_OPTION ", -ODBX_ERR_OPTION);
	luaM_regconst(L, "-ODBX_ERR_OPTRO", -ODBX_ERR_OPTRO);
	luaM_regconst(L, "-ODBX_ERR_OPTWR", -ODBX_ERR_OPTWR);
	luaM_regconst(L, "-ODBX_ERR_RESULT", -ODBX_ERR_RESULT);
	luaM_regconst(L, "-ODBX_ERR_NOTSUP ", -ODBX_ERR_NOTSUP);
	luaM_regconst(L, "-ODBX_ERR_HANDLE", -ODBX_ERR_HANDLE);

	luaM_regconst(L, "ODBX_CAP_BASIC", ODBX_CAP_BASIC);
	luaM_regconst(L, "ODBX_CAP_LO", ODBX_CAP_LO);
	
	// for odbx_get_option();
	luaM_regconst(L, "ODBX_OPT_API_VERSION", ODBX_OPT_API_VERSION);
	luaM_regconst(L, "ODBX_OPT_THREAD_SAFE", ODBX_OPT_THREAD_SAFE);
	luaM_regconst(L, "ODBX_OPT_TLS", ODBX_OPT_TLS);
	luaM_regconst(L, "ODBX_OPT_MULTI_STATEMENTS", ODBX_OPT_MULTI_STATEMENTS);
	luaM_regconst(L, "ODBX_OPT_PAGED_RESULTS", ODBX_OPT_PAGED_RESULTS);
	luaM_regconst(L, "ODBX_OPT_COMPRESS", ODBX_OPT_COMPRESS);
	luaM_regconst(L, "ODBX_OPT_MODE", ODBX_OPT_MODE);
	// for odbx_set_option();
	luaM_regconst(L, "ODBX_TLS_NEVER", ODBX_TLS_NEVER);
	luaM_regconst(L, "ODBX_TLS_ALWAYS", ODBX_TLS_ALWAYS);
	luaM_regconst(L, "ODBX_TLS_TRY", ODBX_TLS_TRY);

	//Exact numeric values: 
	luaM_regconst(L, "ODBX_TYPE_BOOLEAN", ODBX_TYPE_BOOLEAN);
	luaM_regconst(L, "ODBX_TYPE_SMALLINT", ODBX_TYPE_SMALLINT);
	luaM_regconst(L, "ODBX_TYPE_INTEGER", ODBX_TYPE_INTEGER);
	luaM_regconst(L, "ODBX_TYPE_BIGINT", ODBX_TYPE_BIGINT);
	luaM_regconst(L, "ODBX_TYPE_DECIMAL", ODBX_TYPE_DECIMAL);
	//Approximate numeric values: 
	luaM_regconst(L, "ODBX_TYPE_REAL", ODBX_TYPE_REAL);
	luaM_regconst(L, "ODBX_TYPE_DOUBLE", ODBX_TYPE_DOUBLE);
	luaM_regconst(L, "ODBX_TYPE_FLOAT", ODBX_TYPE_FLOAT);
	//String values: 
	luaM_regconst(L, "ODBX_TYPE_CHAR", ODBX_TYPE_CHAR);
	luaM_regconst(L, "ODBX_TYPE_NCHAR", ODBX_TYPE_NCHAR);
	luaM_regconst(L, "ODBX_TYPE_VARCHAR", ODBX_TYPE_VARCHAR);
	luaM_regconst(L, "ODBX_TYPE_NVARCHAR", ODBX_TYPE_NVARCHAR);
	//Large objects: 
	luaM_regconst(L, "ODBX_TYPE_CLOB", ODBX_TYPE_CLOB);
	luaM_regconst(L, "ODBX_TYPE_NCLOB", ODBX_TYPE_NCLOB);
	luaM_regconst(L, "ODBX_TYPE_XML", ODBX_TYPE_XML);
	luaM_regconst(L, "ODBX_TYPE_BLOB", ODBX_TYPE_BLOB);
	//Date and time values: 
	luaM_regconst(L, "ODBX_TYPE_TIME", ODBX_TYPE_TIME);
	luaM_regconst(L, "ODBX_TYPE_TIMETZ", ODBX_TYPE_TIMETZ);
	luaM_regconst(L, "ODBX_TYPE_TIMESTAMP", ODBX_TYPE_TIMESTAMP);
	luaM_regconst(L, "ODBX_TYPE_TIMESTAMPTZ", ODBX_TYPE_TIMESTAMPTZ);
	luaM_regconst(L, "ODBX_TYPE_DATE", ODBX_TYPE_DATE);
	luaM_regconst(L, "ODBX_TYPE_INTERVAL", ODBX_TYPE_INTERVAL);
	//Arrays and sets: 
	luaM_regconst(L, "ODBX_TYPE_ARRAY", ODBX_TYPE_ARRAY);
	luaM_regconst(L, "ODBX_TYPE_MULTISET", ODBX_TYPE_MULTISET);
	//External links: 
	luaM_regconst(L, "ODBX_TYPE_DATALINK", ODBX_TYPE_DATALINK);
	//Vendor specific: 
	luaM_regconst(L, "ODBX_TYPE_UNKNOWN", ODBX_TYPE_UNKNOWN);

	luaM_regconst(L, "ODBX_BIND_SIMPLE", ODBX_BIND_SIMPLE);

	luaM_regconst(L, "ODBX_ENABLE", ODBX_ENABLE);
	luaM_regconst(L, "ODBX_DISABLE", ODBX_DISABLE);

	lua_pushstring(L, defined);	
	lua_gettable(L, -2); 
	
	int ct = lua_tonumber(L, -1);
	lua_pop(L, 1);

	return ct;
}

/**                   
* Return the name of the object's metatable.
* This function is used by `tostring'.     
*/                            
static int luaM_tostring (lua_State *L) {                
	char buff[100];             
	pseudo_data *obj = (pseudo_data *)lua_touserdata (L, 1);     
	if (obj->closed)                          
		strcpy (buff, "closed");
	else
		sprintf (buff, "%p", (void *)obj);
	lua_pushfstring (L, "%s (%s)", lua_tostring(L,lua_upvalueindex(1)), buff);
	return 1;                            
}       

void luaM_regconst(lua_State *L, const char *name, long value) {
	lua_pushstring(L, name);
	lua_pushnumber(L, value);
	lua_rawset(L, -3);
}

/**
* Define the metatable for the object on top of the stack
*/
void luaM_setmeta (lua_State *L, const char *name) {
	luaL_getmetatable (L, name);
	lua_setmetatable (L, -2);
}     

/**
* Create a metatable and leave it on top of the stack.
*/
int luaM_register (lua_State *L, const char *name, const luaL_reg *methods) {
	if (!luaL_newmetatable (L, name))
		return 0;

	/* define methods */
	luaL_register (L, NULL, methods);

	/* define metamethods */
	lua_pushliteral (L, "__gc");
	lua_pushcfunction (L, methods->func);
	lua_settable (L, -3);

	lua_pushliteral (L, "__index");
	lua_pushvalue (L, -2);
	lua_settable (L, -3);

	lua_pushliteral (L, "__tostring");
	lua_pushstring (L, name);
	lua_pushcclosure (L, luaM_tostring, 1);
	lua_settable (L, -3);

	lua_pushliteral (L, "__metatable");
	lua_pushliteral (L, "you're not allowed to get this metatable");
	lua_settable (L, -3);

	return 1;
}

/**
* message
*/

static void luaM_msg(lua_State *L, const int n, const char *m) {
	if (n) {
		lua_pushnumber(L, n);
	} else {
		lua_pushnil(L);
	}
	lua_pushstring(L, m);
}
#if 0
/**
* Push the value of #i field of #tuple row.
*/
static void luaM_pushvalue (lua_State *L, void *row, long int len) {
    if (row == NULL)
        lua_pushnil (L);
    else
        lua_pushlstring (L, row, len);
}
#endif
/**
* Handle Part
*/

/**
* Check for valid connection.
*/
static lua_odbx_conn *Mget_conn (lua_State *L) {
	lua_odbx_conn *my_conn = (lua_odbx_conn *)luaL_checkudata (L, 1, LUA_ODBX_CONN);
	luaL_argcheck (L, my_conn != NULL, 1, "connection expected");
	luaL_argcheck (L, !my_conn->closed, 1, "connection is closed");
	return my_conn;
}

/**
* Check for valid result.
*/
static lua_odbx_res *Mget_res (lua_State *L) {
	lua_odbx_res *my_res = (lua_odbx_res *)luaL_checkudata (L, 1, LUA_ODBX_RES);
	luaL_argcheck (L, my_res != NULL, 1, "result expected");
	luaL_argcheck (L, !my_res->closed, 1, "result is closed");
	return my_res;
}

/**
* Check for valid lo.
*/
static lua_odbx_lo *Mget_lo (lua_State *L) {
	lua_odbx_lo *my_lo = (lua_odbx_lo *)luaL_checkudata (L, 1, LUA_ODBX_LO);
	luaL_argcheck (L, my_lo != NULL, 1, "lo expected");
	luaL_argcheck (L, !my_lo->closed, 1, "lo is closed");
	return my_lo;
}

/**
* OpenDBX operate functions
*/

/**
* Open a connection to a PgSQL Server
*/
static int Lodbx_init (lua_State *L) {
	int err;
	const char *backend = luaL_checkstring (L, 1);
	const char *host = luaL_optstring(L, 2, "127.0.0.1");
	const char *port = luaL_optstring(L, 3, "");

	odbx_t* conn;

	if ((err = odbx_init( &conn, backend, host, port )) < 0) {
		luaM_msg (L, err, odbx_error(conn, err));
		return 2;
	}

	lua_odbx_conn *my_conn = (lua_odbx_conn *)lua_newuserdata(L, sizeof(lua_odbx_conn));
	luaM_setmeta (L, LUA_ODBX_CONN);

	/* fill in structure */
	my_conn->closed = 0;
	my_conn->env = LUA_NOREF;
	my_conn->conn = conn;

	return 1;
}

static int Lodbx_error (lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	lua_Number error = luaL_checknumber(L, 2);
	lua_pushstring(L, odbx_error(my_conn->conn, error));
	return 1;
}

static int Lodbx_error_type (lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	lua_Number error = luaL_checknumber(L, 2);
	lua_pushnumber(L, odbx_error_type(my_conn->conn, error));
	return 1;
}

static int Lodbx_bind(lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *database = luaL_optstring(L, 2, NULL);
	const char *who = luaL_optstring(L, 3, NULL);
	const char *cred = luaL_optstring(L, 4, NULL);
	const char *method_str = luaL_optstring (L, 5, "ODBX_BIND_SIMPLE");

	lua_Number method = luaM_const(L, method_str);

	lua_pushnumber (L, odbx_bind(my_conn->conn, database, who, cred, method));
	return 1;
}

static int Lodbx_bind_simple(lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *database = luaL_optstring(L, 2, NULL);
	const char *who = luaL_optstring(L, 3, NULL);
	const char *cred = luaL_optstring(L, 4, NULL);

	lua_pushnumber (L, odbx_bind_simple(my_conn->conn, database, who, cred));
	return 1;
}

static int Lodbx_unbind(lua_State *L) {
	lua_pushnumber(L, odbx_unbind(Mget_conn(L)->conn));
	return 1;
}

static int Lodbx_capabilities(lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *cap_str = luaL_optstring (L, 2, "-ODBX_CAP_BASIC");

	lua_Number cap = luaM_const(L, cap_str);

	lua_pushnumber (L, odbx_capabilities(my_conn->conn, cap));
	return 1;
}

static int Lodbx_escape(lua_State *L) {
	int err;

	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *from = luaL_checkstring(L, 2);
	unsigned long fromlen = strlen(from);
	// To be precise, the output buffer must be 2 * size of input + 1 bytes long. 
	char to[2*(fromlen+1)];
	unsigned long tolen = sizeof(to);

	if ((err = odbx_escape(my_conn->conn, from, fromlen, to, &tolen)) < 0) {
		luaM_msg (L, err, odbx_error(my_conn->conn, err));
		return 2;
	}

	lua_pushstring (L, to);
	return 1;
}

static int Lodbx_set_option(lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *option_str = luaL_checkstring (L, 2);
	void *value = (void *)luaL_checkstring (L, 3);

	lua_Number option = luaM_const(L, option_str);

	lua_pushnumber (L, odbx_set_option(my_conn->conn, option, value));
	return 1;
}

static int Lodbx_get_option(lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *option_str = luaL_checkstring (L, 2);
	void *value = (void *)luaL_checkstring (L, 3);

	lua_Number option = luaM_const(L, option_str);

	lua_pushnumber (L, odbx_get_option(my_conn->conn, option, value));
	return 1;
}

static int Lodbx_query(lua_State *L) {
	int err;
	odbx_result_t* res;

	lua_odbx_conn *my_conn = Mget_conn (L);
	const char *stmt = luaL_checkstring (L, 2);
	unsigned long len = (unsigned long) strlen(stmt);

	if( ( err = odbx_query(my_conn->conn, stmt, len ) ) < 0 ) {
		luaM_msg (L, err, odbx_error(my_conn->conn, err));
		return 2;
	}

	if( ( err = odbx_result(my_conn->conn, &res, NULL, 0 ) ) < 0 ) {
		luaM_msg (L, err, odbx_error(my_conn->conn, err));
		return 2;
	}

	lua_odbx_res *my_res = (lua_odbx_res *)lua_newuserdata(L, sizeof(lua_odbx_res));
	luaM_setmeta (L, LUA_ODBX_RES);

	/* fill in structure */
	my_res->closed = 0;
	my_res->conn = LUA_NOREF;
	my_res->res = res;

	lua_pushvalue(L, 1);

	my_res->conn = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
}

static int Lodbx_result (lua_State *L) {
	luaM_msg (L, 0, "Use db:query first return value if it not false or nil.");
	return 2;
#if 0
	/*
	   i drop this, Use query first return value, if it not false,it will be the result handle.
	 */
	int err;
	odbx_result_t* res;

	lua_odbx_conn *my_conn = Mget_conn (L);

	if( ( err = odbx_result(my_conn->conn, &res, NULL, 0 ) ) < 0 ) {
		luaM_msg (L, err, odbx_error(my_conn->conn, err));
		return 2;
	}

	lua_odbx_res *my_res = (lua_odbx_res *)lua_newuserdata(L, sizeof(lua_odbx_res));
	luaM_setmeta (L, LUA_ODBX_RES);

	/* fill in structure */
	my_res->closed = 0;
	my_res->conn = LUA_NOREF;
	my_res->res = res;

	lua_pushvalue(L, 1);

	my_res->conn = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
#endif
}

static int Lodbx_result_finish (lua_State *L) {
	lua_pushboolean(L, 1);
	return 1;
#if 0
	/*
	   i drop it, lua can auto free the result memory.
	 */
	lua_odbx_res *my_res = Mget_res (L);
	luaL_argcheck (L, my_res != NULL, 1, "connection expected");
	if (my_res->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	my_res->closed = 1;
	luaL_unref (L, LUA_REGISTRYINDEX, my_res->conn);
	lua_pushnumber(L, odbx_result_finish(my_res->res));
	return 1;
#endif
}

static int Lodbx_result_free (lua_State *L) {
	return 0;
#if 0
	/*
	   i drop it, lua can auto free the result memory.
	 */
	lua_odbx_res *my_res = Mget_res (L);
	luaL_argcheck (L, my_res != NULL, 1, "connection expected");
	if (my_res->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	my_res->closed = 1;
	luaL_unref (L, LUA_REGISTRYINDEX, my_res->conn);
	odbx_result_free(my_res->res);

	return 0;
#endif
}

static int Lodbx_column_name (lua_State *L) {
	lua_odbx_res *my_res = Mget_res (L);
	lua_Number pos = luaL_optnumber (L, 2, 0);

	lua_pushstring(L, odbx_column_name(my_res->res, pos));
	return 1;
}

static int Lodbx_column_type (lua_State *L) {
	lua_odbx_res *my_res = Mget_res (L);
	lua_Number pos = luaL_optnumber (L, 2, 0);

	lua_pushnumber(L, odbx_column_type(my_res->res, pos));
	return 1;
}

static int Lodbx_column_count (lua_State *L) {
	lua_pushnumber(L, odbx_column_count(Mget_res (L)->res));
	return 1;
}

static int Lodbx_field_length (lua_State *L) {
	lua_odbx_res *my_res = Mget_res (L);
	lua_Number pos = luaL_optnumber (L, 2, 0);

	lua_pushnumber(L,  odbx_field_length(my_res->res, pos));
	return 1;
}

static int Lodbx_field_value (lua_State *L) {
	lua_odbx_res *my_res = Mget_res (L);
	lua_Number pos = luaL_optnumber (L, 2, 0);

	lua_pushstring(L,  odbx_field_value(my_res->res, pos));
	return 1;
}

static int Lodbx_row_fetch(lua_State *L) {
	int err = odbx_row_fetch(Mget_res (L)->res);
	lua_pushboolean(L, err > 0 ? 1 : 0);
	lua_pushnumber(L, err);
	return 2;
}

static int Lodbx_rows_affected(lua_State *L) {
	lua_pushnumber(L, odbx_rows_affected(Mget_res (L)->res));
	return 1;
}

static int Lodbx_lo_open(lua_State *L) {
	int err;
	odbx_lo_t* lo;

	lua_odbx_res *my_res = Mget_res (L);

	const char *value = luaL_checkstring (L, 2);

	if( ( err = odbx_lo_open(my_res->res, &lo, value ) ) < 0 ) {
		lua_pushnumber(L, err);
		return 1;
	}
	lua_odbx_lo *my_lo = (lua_odbx_lo *)lua_newuserdata(L, sizeof(lua_odbx_lo));
	luaM_setmeta (L, LUA_ODBX_LO);

	/* fill in structure */
	my_lo->closed = 0;
	my_lo->conn = LUA_NOREF;
	my_lo->lo = lo;

	lua_pushvalue(L, 1);

	my_lo->conn = luaL_ref (L, LUA_REGISTRYINDEX);

	return 1;
}

static int Lodbx_lo_write (lua_State *L) {
	lua_odbx_lo *my_lo = Mget_lo (L);
	void *buffer = (void *)luaL_checkstring (L, 2);
	lua_Number buflen = luaL_checknumber (L, 3);

	lua_pushnumber(L,  odbx_lo_write(my_lo->lo, buffer, buflen));
	return 1;
}

static int Lodbx_lo_read (lua_State *L) {
	lua_odbx_lo *my_lo = Mget_lo (L);
	void *buffer = (void *)luaL_checkstring (L, 2);
	lua_Number buflen = luaL_checknumber (L, 3);

	lua_pushnumber(L,  odbx_lo_read(my_lo->lo, buffer, buflen));
	return 1;
}

static int Lodbx_lo_close(lua_State *L) {
	lua_pushnumber(L, odbx_lo_close(Mget_lo (L)->lo));
	return 1;
}

/**
* Frees connection resources
*/
static int Lodbx_finish (lua_State *L) {
	lua_odbx_conn *my_conn = Mget_conn (L);
	luaL_argcheck (L, my_conn != NULL, 1, "connection expected");
	if (my_conn->closed) {
		lua_pushboolean (L, 0);
		return 1;
	}

	my_conn->closed = 1;
	luaL_unref (L, LUA_REGISTRYINDEX, my_conn->env);
	odbx_finish(my_conn->conn);
	lua_pushboolean (L, 1);
	return 1;
}

/**
* version info
*/
static int Lversion (lua_State *L) {
	lua_pushfstring(L, "luaodbx (%s) - OpenDBX driver\n", LUA_ODBX_VERSION);
	lua_pushstring(L, "(c) 2009-19 Alacner zhang <alacner@gmail.com>\n");
	lua_pushstring(L, "This content is released under the MIT License.\n");

	lua_concat (L, 3);
	return 1;
}
/**
* Creates the metatables for the objects and registers the
* driver open method.
*/
int luaopen_odbx (lua_State *L) {
	struct luaL_reg driver[] = {
		{ "init",   Lodbx_init },
		{ "version",   Lversion },
		{ NULL, NULL },
	};

	struct luaL_reg lo_methods[] = {
		{ "lo_read",   Lodbx_lo_read },
		{ "lo_write",   Lodbx_lo_write },
		{ "lo_close",   Lodbx_lo_close },
		{ NULL, NULL }
	};

	struct luaL_reg result_methods[] = {
		{ "finish",   Lodbx_result_finish },
		{ "free",   Lodbx_result_free },
		{ "column_name",   Lodbx_column_name },
		{ "column_type",   Lodbx_column_type },
		{ "column_count",   Lodbx_column_count },
		{ "row_fetch",   Lodbx_row_fetch },
		{ "rows_affected",   Lodbx_rows_affected },
		{ "field_length",   Lodbx_field_length },
		{ "field_value",   Lodbx_field_value },
		{ "lo_open",   Lodbx_lo_open },
		{ NULL, NULL }
	};

	struct luaL_reg connection_methods[] = {
		{ "bind",   Lodbx_bind },
		{ "bind_simple",   Lodbx_bind_simple },
		{ "unbind",   Lodbx_unbind },
		{ "capabilities",   Lodbx_capabilities },
		{ "escape",   Lodbx_escape },
		{ "set_option",   Lodbx_set_option },
		{ "get_option",   Lodbx_get_option },
		{ "query",   Lodbx_query },
		{ "result",   Lodbx_result},
		{ "error",   Lodbx_error },
		{ "error_type",   Lodbx_error_type },
		{ "finish",   Lodbx_finish },
		{ NULL, NULL }
	};

	luaM_register (L, LUA_ODBX_CONN, connection_methods);
	luaM_register (L, LUA_ODBX_RES, result_methods);
	luaM_register (L, LUA_ODBX_LO, lo_methods);
	lua_pop (L, 2);
	lua_pop (L, 2);
	luaL_register (L, LUA_ODBX_TABLENAME, driver);

	return 1;
}
