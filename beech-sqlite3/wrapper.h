typedef void (*sqlite3_destructor_type)(void*);
extern sqlite3_destructor_type SHIM_SQLITE_STATIC;
extern sqlite3_destructor_type SHIM_SQLITE_TRANSIENT;



#include <sqlite3ext.h>
