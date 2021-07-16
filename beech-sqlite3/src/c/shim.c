#include "wrapper.h"

sqlite3_destructor_type SHIM_SQLITE_STATIC = ((sqlite3_destructor_type)0);
sqlite3_destructor_type SHIM_SQLITE_TRANSIENT = ((sqlite3_destructor_type)-1);

