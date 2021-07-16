#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use beech::*;
use libc::*;
use std::ffi::{CStr, CString};

use avro_rs::types::Value;
use beech::query::{Constraint, IndexUsage};
use beech::source::{HttpFetcher, CachedStore, Store, FileFetcher};
use beech::{BeechError, Result};
use std::collections::HashMap;
use const_cstr::const_cstr;
use log::{error, debug, info};
use std::time::Duration;
use std::convert::TryFrom;

const_cstr! {
    BEECH = "beech";
}

fn as_sqlite_errno<T>(r: &Result<T>) -> c_int {
    (match &r {
        Ok(_) => 0,
        Err(e) => {
            match e.as_fail().downcast_ref() {
                Some(beech::BeechError::Args) => SQLITE_MISUSE,
                Some(beech::BeechError::Done) => SQLITE_DONE,
                _ => SQLITE_INTERNAL,
            }
        }
    }) as c_int
}

fn log_if_error<T>(context: &str, r: &Result<T>) {
    let _ = r.as_ref().map_err(|e| error!("{}: {}", context, e));
}

static mut SAPI: *const sqlite3_api_routines = std::ptr::null_mut();

macro_rules! sqlite3_fn{
	($func_name:ident, $( $x:expr ),*)=>(
		(*SAPI).$func_name.unwrap()($($x),*)
	)
}



#[repr(C)]
struct beech_vtab_t {
    module: *const c_void,
    n_ref: c_int,
    err_msg: *const c_char,
    url: String,
    local_table_name: String,
    remote_table_name: String,

    source: Box<dyn Store>
}

#[repr(C)]
struct beech_cursor_t {
    vtab: *mut beech_vtab_t,
    cursor: beech::query::Cursor,
}


unsafe fn args_to_vec<'a>(argc: c_int, argv: *const *const c_char) -> Result<Vec<&'a str>> {
    let mut vec = Vec::with_capacity(argc as usize);
    for i in 0..argc as isize {
        let maybe_url = CStr::from_ptr(*argv.offset(i)).to_str()?;
        vec.push(maybe_url)
    }
    Ok(vec)
}

fn to_column_spec(c: &Column) -> Option<String> {
    if c.name == "rowid" {
        None
    } else {
        let col_type = {
            match c.typ.as_ref() {
                "long" | "int" => "integer",
                "float" | "double" => "real",
                "boolean" => "boolean",
                _ => "varchar",
            }
        };
        Some(format!("{} {}", c.name, col_type))
    }
}

#[allow(dead_code)]
fn log_error<T>(r: Result<T>) -> Result<T> {
    if let Err(ref e) = r {
        println!("ERROR {}", e);
    }
    r
}

fn parse_options(args: &[&str]) -> HashMap<String, String> {
    let mut hm : HashMap<String, String> = Default::default();
    for a in args.iter() {
        let pieces :Vec<&str>= a.splitn(2, "=").collect();
        if pieces.len() == 2 {
            hm.insert(pieces[0].to_string(), pieces[1].to_string());
        } else {
            hm.insert(pieces[0].to_string(), "".to_string());
        }
    }
    hm
}

unsafe fn do_beech_create(
    db: *mut sqlite3,
    _aux: *mut std::os::raw::c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    _p_err: *mut *mut c_char,
) -> Result<()> {
    if argc < 4 {
        return Err(beech::BeechError::Args.into());
    }
    let args = args_to_vec(argc, argv)?;
    println!("args {:?} (xxx, yyy, source, remote_table, options...)", args);
    
    let remote_table_name = args[4].to_string();
    let source_arg = args[3].trim_matches('\'').trim_matches('"');
    let options = parse_options(&args[4..]);
    let cache_size = match options.get("cache_size") {
        Some(x) => str::parse::<usize>(x)?,
        None => 16*1024*1024
    };
    let maybe_update_duration = match options.get("update") {
        Some(ref x) =>
            if x.as_str() == "never" {
                None
            } else {
                Some(Duration::from_secs(str::parse::<u64>(&x)?))
            },
        None => Some(Duration::from_secs(5)),
    };
    debug!("using cache size {}", cache_size);

    let mut source : Box<dyn Store> = if let Ok(fetcher) = HttpFetcher::try_from(source_arg) {
	Box::new(CachedStore::new(cache_size, maybe_update_duration, fetcher)?)
    } else {
	let fetcher = FileFetcher::try_from(source_arg)?;
	Box::new(CachedStore::new(cache_size, maybe_update_duration, fetcher)?)
    };

    let column_spec = {
        let tab = source.get_table(&remote_table_name)?;
        debug!("fetched table '{}'", remote_table_name);

        let columns: Vec<String> = tab
            .columns()
            .into_iter()
            .filter_map(to_column_spec)
            .collect();
        columns.join(", ")
    };

    let vtab = beech_vtab_t {
        module: std::ptr::null(),
        n_ref: 0,
        err_msg: std::ptr::null(),
        url: args[3].trim_matches('\'').to_string(),
        local_table_name: args[2].to_string(),
        remote_table_name: remote_table_name.to_string(),
        source,
    };
    let create_sql = format!("CREATE TABLE {} ({});", &vtab.local_table_name, column_spec);
    let boxed = Box::new(vtab);
    *pp_vtab = Box::into_raw(boxed) as *mut sqlite3_vtab;
    let rc = sqlite3_fn!(declare_vtab, db, create_sql.as_ptr() as *const i8);
    if 0 != rc {
        return Err(beech::BeechError::Sqlite3 { code: rc }.into());
    }
    debug!("created table with {:?}", create_sql);
    Ok(())
}

pub unsafe extern "C" fn beech_create(
    db: *mut sqlite3,
    aux: *mut std::os::raw::c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    p_err: *mut *mut c_char,
) -> c_int {
    debug!("beech_create()");
    let result = &do_beech_create(db, aux, argc, argv, pp_vtab, p_err);
    log_if_error("beech_create()", result);
    as_sqlite_errno(result)
}

fn from_sqlite_op(op: c_uchar) -> beech::query::ConstraintOp {
    match u32::from(op) {
        SQLITE_INDEX_CONSTRAINT_EQ => beech::query::ConstraintOp::Eq,
        SQLITE_INDEX_CONSTRAINT_GT => beech::query::ConstraintOp::Gt,
        SQLITE_INDEX_CONSTRAINT_LE => beech::query::ConstraintOp::Le,
        SQLITE_INDEX_CONSTRAINT_LT => beech::query::ConstraintOp::Lt,
        SQLITE_INDEX_CONSTRAINT_GE => beech::query::ConstraintOp::Ge,
        _ => beech::query::ConstraintOp::Unknown,
    }
}

unsafe fn do_beech_best_index(
    vtab: *mut sqlite3_vtab,
    index_info: *mut sqlite3_index_info,
) -> Result<()> {
    let tab = vtab as *mut beech_vtab_t;

    let mut constraints = Vec::new();

    for i in 0..(*index_info).nConstraint as isize {
        let c = *(*index_info).aConstraint.offset(i);
        let cu = (*index_info).aConstraintUsage.offset(i as isize);
        if c.usable != 0 {
            constraints.push(Constraint::new(c.iColumn as usize, from_sqlite_op(c.op)));
            (*cu).argvIndex = constraints.len() as i32;
        }
    }
    let mut order_bys: Vec<beech::query::OrderBy> = Vec::new();
    for i in 0..(*index_info).nOrderBy as isize {
        let o = *(*index_info).aOrderBy.offset(i);
        order_bys.push(beech::query::OrderBy {
            column: o.iColumn as usize,
            desc: o.desc != 0,
        });
    }
    let table = (*tab).source.get_table(&(*tab).remote_table_name)?;
    debug!("beech_best_index() = {:?}", table);
    let index_usage = beech::query::best_index(table, constraints, order_bys);

    (*index_info).estimatedCost = index_usage.estimated_cost;

    let json_usage = serde_json::to_string(&index_usage)?;
    debug!("beech_best_index() = {}", json_usage);
    let bytes_usage = json_usage.as_bytes();
    let bytes_len = bytes_usage.len();
    let buf = sqlite3_fn!(malloc, (bytes_len + 1) as i32) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes_usage.as_ptr(), buf, bytes_len);
    (*buf.offset(bytes_len as isize)) = 0;

    (*index_info).idxStr = buf as *mut i8;
    (*index_info).needToFreeIdxStr = 1;

    Ok(())
}

pub unsafe extern "C" fn beech_best_index(
    vtab: *mut sqlite3_vtab,
    index_info: *mut sqlite3_index_info,
) -> c_int {
    debug!("beech_best_index()");
    let res = &do_beech_best_index(vtab, index_info);
    log_if_error("beech_best_index()", res);
    as_sqlite_errno(res)
}

unsafe fn do_beech_open(
    vtab: *mut sqlite3_vtab,
    cursor_ptr: *mut *mut sqlite3_vtab_cursor,
) -> Result<()> {
    let tab = vtab as *mut beech_vtab_t;
    let desc = (*tab).source.get_table(&(*tab).remote_table_name)?;

    let cursor = beech_cursor_t {
        vtab: tab,
        cursor: beech::query::Cursor::new(desc),
    };
    let boxed = Box::new(cursor);
    *cursor_ptr = Box::into_raw(boxed) as *mut sqlite3_vtab_cursor;

    Ok(())
}

pub unsafe extern "C" fn beech_open(
    vtab: *mut sqlite3_vtab,
    cursor_ptr: *mut *mut sqlite3_vtab_cursor,
) -> c_int {
    debug!("beech_open()");
    let res = &do_beech_open(vtab, cursor_ptr);
    log_if_error("beech_open()", res);
    as_sqlite_errno(res)
}

pub unsafe extern "C" fn beech_close(cursor: *mut sqlite3_vtab_cursor) -> c_int {
    debug!("beech_close({:p})", cursor);
    Box::from_raw(cursor);
    0
}

unsafe fn do_beech_filter(
    cursor: *mut sqlite3_vtab_cursor,
    _idx: c_int,
    idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> Result<()> {
    let c = &mut *(cursor as *mut beech_cursor_t);
    let json_usage = CStr::from_ptr(idx_str as *mut c_void as *const c_char).to_str()?;
    debug!("{}", json_usage);
    let usage: IndexUsage = serde_json::from_str(&json_usage)?;

    // TODO: check new scheme with old for compatibility
    if usage.table_id != c.cursor.table.id {
        return Err(BeechError::SchemaMismatch.into());
    }
    debug!("beech_filter(): schema matches");
    let mut values = vec![];
    for i in 0..argc as usize {
        let arg = *argv.offset(i as isize);
        let varg = avro_rs_value_from_sqlite(arg)?;
        values.push(varg);
    }
    c.cursor.init(usage, values);
    c.cursor.advance_to_left(&mut *(*c.vtab).source)
}
pub unsafe extern "C" fn beech_filter(
    cursor: *mut sqlite3_vtab_cursor,
    idx: c_int,
    idx_str: *const c_char,
    argc: c_int,
    argv: *mut *mut sqlite3_value,
) -> c_int {
    debug!("beech_filter({:p})", cursor);
    let res = &do_beech_filter(cursor, idx, idx_str, argc, argv);
    log_if_error("beech_filter()", res);
    as_sqlite_errno(res)
}

unsafe fn do_beech_next(c: &mut beech_cursor_t) -> Result<()> {
    c.cursor.next(&mut *(*c.vtab).source)
}

pub unsafe extern "C" fn beech_next(c: *mut sqlite3_vtab_cursor) -> c_int {
    debug!("beech_next({:p})", c);
    as_sqlite_errno(&do_beech_next(&mut *(c as *mut beech_cursor_t)))
}

pub unsafe extern "C" fn beech_eof(c: *mut sqlite3_vtab_cursor) -> c_int {
    let is_eof = (*(c as *mut beech_cursor_t)).cursor.eof();
    debug!("beech_eof({:p}) = {:?}", c, is_eof);
    if is_eof {
        1
    } else {
        0
    }
}

unsafe fn avro_rs_value_from_sqlite(sv: *mut sqlite3_value) -> Result<Value> {
    match sqlite3_fn!(value_type, sv) as u32 {
        SQLITE_INTEGER => Ok(Value::Long(sqlite3_fn!(value_int64, sv))),
        SQLITE_FLOAT => Ok(Value::Double(sqlite3_fn!(value_double, sv))),
        SQLITE_NULL => Ok(Value::Null),
        SQLITE_TEXT => {
            let strv = sqlite3_fn!(value_text, sv) as *const i8;
            Ok(Value::String(CStr::from_ptr(strv).to_str()?.to_string()))
        }
        typ => {
            error!("converting sqlite value of type {} to avro value", typ);

            Err(BeechError::Corrupt.into())
        }
    }
}

unsafe fn output_column(ctx: *mut sqlite3_context, c: Option<&Value>) -> Result<()> {
    match c {
        Some(&Value::Boolean(b)) => {
            sqlite3_fn!(result_int, ctx, if b { 1 } else { 0 } as c_int);
            Ok(())
        }
        Some(&Value::Int(i)) => {
            sqlite3_fn!(result_int, ctx, i as c_int);
            Ok(())
        }
        Some(&Value::Long(i)) => {
            sqlite3_fn!(result_int64, ctx, i as i64);
            Ok(())
        }
        Some(&Value::Float(f)) => {
            sqlite3_fn!(result_double, ctx, f64::from(f));
            Ok(())
        }
        Some(&Value::Double(f)) => {
            sqlite3_fn!(result_double, ctx, f);
            Ok(())
        }
        Some(&Value::String(ref s)) => {
            let len = s.len() as c_int;
            let cstr = CString::new(s.to_string())?;

            sqlite3_fn!(result_text, ctx, cstr.as_ptr(), len, SHIM_SQLITE_TRANSIENT);
            Ok(())
        }
        Some(&Value::Null) => {
            sqlite3_fn!(result_null, ctx);
            Ok(())
        }
        Some(&Value::Bytes(ref buf)) => {
            sqlite3_fn!(
                result_blob,
                ctx,
                buf.as_ptr() as *const std::os::raw::c_void,
                buf.len() as c_int,
                SHIM_SQLITE_TRANSIENT
            );
            Ok(())
        }
        Some(&Value::Union(ref bv)) => output_column(ctx, Some(&**bv)),
        _ => Err(beech::BeechError::Corrupt.into()),
    }
}

unsafe fn do_beech_column(c: &mut beech_cursor_t, ctx: *mut sqlite3_context, i: c_int) -> Result<()> {
    match &c.cursor.current() {
        None => Err(beech::BeechError::Done.into()),
        Some((id, child)) => {
            let p = (*c.vtab).source.get_page(&(*c).cursor.table, &id)?;
            let c = p.column(*child, i as usize);
            output_column(ctx, c)
        }
    }
}

pub unsafe extern "C" fn beech_column(
    c: *mut sqlite3_vtab_cursor,
    ctx: *mut sqlite3_context,
    i: c_int,
) -> c_int {
    debug!("beech_column()");
    let res = &do_beech_column(&mut *(c as *mut beech_cursor_t), ctx, i);
    log_if_error("beech_column()", res);
    as_sqlite_errno(res)
}

unsafe fn do_beech_rowid(c: &mut beech_cursor_t, rowidp: *mut sqlite_int64) -> Result<()> {
    match &c.cursor.current() {
        None => Err(beech::BeechError::Done.into()),
        Some((id, child)) => {
            let p = (*c.vtab).source.get_page(&(*c).cursor.table, &id)?;
            let rowid = p.rowid(*child).ok_or(beech::BeechError::Corrupt)?;
            *rowidp = rowid as sqlite3_int64;
            Ok(())
        }
    }
}

pub unsafe extern "C" fn beech_rowid(
    c: *mut sqlite3_vtab_cursor,
    rowidp: *mut sqlite_int64,
) -> c_int {
    debug!("beech_rowid()");
    let res = &do_beech_rowid(&mut *(c as *mut beech_cursor_t), rowidp);
    log_if_error("beech_rowid()", res);
    as_sqlite_errno(res)
}

pub unsafe extern "C" fn beech_connect(
    db: *mut sqlite3,
    aux: *mut std::os::raw::c_void,
    argc: c_int,
    argv: *const *const c_char,
    pp_vtab: *mut *mut sqlite3_vtab,
    p_err: *mut *mut c_char,
) -> c_int {
    //TODO: need to check schema compatibility with the already created local table
    debug!("beech_connect()");
    let result = &do_beech_create(db, aux, argc, argv, pp_vtab, p_err);
    log_if_error("beech_connect()", result);
    as_sqlite_errno(result)
}

pub unsafe extern "C" fn beech_destroy(vtab: *mut sqlite3_vtab) -> c_int {
    Box::from_raw(vtab);
    0
}

pub unsafe extern "C" fn beech_disconnect(vtab: *mut sqlite3_vtab) -> c_int {
    Box::from_raw(vtab);
    0
}

pub unsafe extern "C" fn beech_update(
    _vtab: *mut sqlite3_vtab,
    _n: c_int,
    _v: *mut *mut sqlite3_value,
    _x: *mut sqlite3_int64,
) -> c_int {
    8 //SQLITE_READONLY
}

const BEECH_TABLE: sqlite3_module = sqlite3_module {
    iVersion: 0,                        /* iVersion */
    xCreate: Some(beech_create),         /* xCreate - handle CREATE VIRTUAL TABLE */
    xConnect: Some(beech_connect),       /* xConnect - same as xCreate "eponymous" virtual table */
    xBestIndex: Some(beech_best_index),  /* xBestIndex - figure out how to do a query */
    xDisconnect: Some(beech_disconnect), /* xDisconnect - close a connection */
    xDestroy: Some(beech_destroy),       /* xDestroy - handle DROP TABLE */
    xOpen: Some(beech_open),             /* xOpen - open a cursor */
    xClose: Some(beech_close),           /* xClose - close a cursor */
    xFilter: Some(beech_filter),         /* xFilter - configure scan constraints */
    xNext: Some(beech_next),             /* xNext - advance a cursor */
    xEof: Some(beech_eof),               /* xEof - check for end of scan */
    xColumn: Some(beech_column),         /* xColumn - read data */
    xRowid: Some(beech_rowid),           /* xRowid - read data */
    xUpdate: Some(beech_update),         /* xUpdate */
    xBegin: None,                       /* xBegin */
    xSync: None,                        /* xSync */
    xCommit: None,                      /* xCommit */
    xRollback: None,                    /* xRollback */
    xFindFunction: None,                /* xFindFunction */
    xRename: None,                      //Some(beech_rename),         /* xRename */
    xSavepoint: None,                   /* xSavepoint */
    xRelease: None,                     /* xRelease */
    xRollbackTo: None,                  /* xRollbackTo */
};

#[no_mangle]
pub extern "C" fn sqlite_extension_fini(_arg1: *mut std::os::raw::c_void) {}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut sqlite3,
    _p_err_msg: &mut *const c_char,
    p_api: *const sqlite3_api_routines,
) -> c_int {
    let _ = env_logger::try_init().map_err(|e| info!("{}", e));
    debug!("sqlite3_extension_init()");
    SAPI = p_api;
    sqlite3_fn!(
        create_module_v2,
        db,
        BEECH.as_ptr(),
        &BEECH_TABLE,
        std::ptr::null_mut(),
        Some(sqlite_extension_fini)
    )
}
