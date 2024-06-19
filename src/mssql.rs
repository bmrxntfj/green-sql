use std::borrow::Cow;
use std::io::Write;

use crate::metadata::{DbType, IndexType, JoinType, ObjectSchema, TableSchema, ViewSchema};
use crate::util;
use tiberius::numeric::Numeric;
use tiberius::{IntoSql, Query};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::dbsql::{
    ColumnExpression, Deletable, DeleteStatement, InsertStatement, Insertable, JoinClosure,
    Predicate, Queryable, SelectColumn, SelectOrderColumn, SqlParameter, SqlStatement, Updatable,
    UpdateStatement,
};

pub struct MssqlDbConnection<'a> {
    conn_str: &'a str,
    client: tiberius::Client<Compat<TcpStream>>,
}

impl<'a> MssqlDbConnection<'a> {
    pub async fn new(conn_str: &'a str) -> anyhow::Result<Self> {
        let config = tiberius::Config::from_ado_string(&conn_str)?;
        let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        let client = tiberius::Client::connect(config, tcp.compat_write()).await?;
        Ok(Self { conn_str, client })
    }
    pub fn get_conn_str(&self) -> &str {
        &self.conn_str
    }

    pub async fn execute(&mut self, statement: SqlStatement<'a>) -> anyhow::Result<u64> {
        println!("execute:>{}", &statement.sql);
        match &statement.parameters {
            Some(params) => {
                let mut select = Query::new(&statement.sql);
                params.iter().for_each(|param| select.bind(param));
                Ok(select.execute(&mut self.client).await?.total())
            }
            _ => Ok(self.client.execute(&statement.sql, &[]).await?.total()),
        }
    }
    pub async fn get_value_i16(&mut self, statement: SqlStatement<'a>) -> anyhow::Result<i16> {
        let row = self.get_row(statement).await?;
        match row {
            Some(row) => match row.try_get::<i16, _>(0)? {
                Some(v) => Ok(v),
                None => Err(anyhow::Error::msg(format!("can't get value at index 0."))),
            },
            None => Err(anyhow::Error::msg(format!("this sql can't get row.",))),
        }
    }
    pub async fn get_value_i32(&mut self, statement: SqlStatement<'a>) -> anyhow::Result<i32> {
        let row = self.get_row(statement).await?;
        match row {
            Some(row) => match row.try_get::<i32, _>(0)? {
                Some(v) => Ok(v),
                None => Err(anyhow::Error::msg(format!("can't get value at index 0."))),
            },
            None => Err(anyhow::Error::msg(format!("this sql can't get row.",))),
        }
    }

    pub async fn get_value_numeric(
        &mut self,
        statement: SqlStatement<'a>,
    ) -> anyhow::Result<Numeric> {
        let row = self.get_row(statement).await?;
        match row {
            Some(row) => match row.try_get::<Numeric, _>(0)? {
                Some(v) => Ok(v),
                None => Err(anyhow::Error::msg(format!("can't get value at index 0."))),
            },
            None => Err(anyhow::Error::msg(format!("this sql can't get row.",))),
        }
    }

    pub async fn get_row(
        &mut self,
        statement: SqlStatement<'a>,
    ) -> anyhow::Result<Option<tiberius::Row>> {
        println!("get_row:>{}", &statement.sql);
        match &statement.parameters {
            Some(params) => {
                let mut select = Query::new(&statement.sql);
                params.iter().for_each(|param| select.bind(param));
                let row = select.query(&mut self.client).await?.into_row().await?;
                Ok(row)
            }
            _ => {
                let row = self
                    .client
                    .simple_query(&statement.sql)
                    .await?
                    .into_row()
                    .await?;
                Ok(row)
            }
        }
    }

    pub async fn get_rows(
        &mut self,
        statement: SqlStatement<'a>,
    ) -> anyhow::Result<Vec<tiberius::Row>> {
        println!("get_rows:>{}", &statement.sql);
        match &statement.parameters {
            Some(params) => {
                let mut select = Query::new(&statement.sql);
                params.iter().for_each(|param| select.bind(param));
                let row = select
                    .query(&mut self.client)
                    .await?
                    .into_first_result()
                    .await?;
                Ok(row)
            }
            _ => {
                let row = self
                    .client
                    .simple_query(&statement.sql)
                    .await?
                    .into_first_result()
                    .await?;
                Ok(row)
            }
        }
    }
    // pub async fn tran<T, F>(&mut self, f: F) ->F::Output
    // where
    //     F: std::future::Future<Output = anyhow::Result<i32,anyhow::Error>>,
    // {
    //     self.execute(SqlStatement::from("BEGIN TRANSTRACTION".to_string())).await?;
    //     let r= f.await;
    //     match r {
    //         Ok(_)=>{
    //             self.execute(SqlStatement::from("COMMIT TRANSTRACTION".to_string())).await?;
    //         }
    //         Err(_)=>{
    //             self.execute(SqlStatement::from("ROLLBACK TRANSTRACTION".to_string())).await?;
    //         }
    //     }
    //     r
    // }
}

pub struct MssqlDbTransaction {
    
}

impl MssqlDbTransaction{

    pub async fn begin<'a>(&self,conn:&mut MssqlDbConnection<'a>)->anyhow::Result<(),anyhow::Error>{
        conn.execute(SqlStatement::from("if @@TRANCOUNT=0 BEGIN TRAN t0 else save TRAN t0".to_string())).await?;
        anyhow::Ok(())
    }
    pub async fn commit<'a>(&self,conn:&mut MssqlDbConnection<'a>)->anyhow::Result<(),anyhow::Error>{
        conn.execute(SqlStatement::from("if @@TRANCOUNT = 0 commit TRAN t0".to_string())).await?;
        anyhow::Ok(())
    }
    pub async fn rollback<'a>(&self,conn:&mut MssqlDbConnection<'a>)->anyhow::Result<(),anyhow::Error>{
        conn.execute(SqlStatement::from("if @@TRANCOUNT = 0 rollback TRAN t0".to_string())).await?;
        anyhow::Ok(())
    }
}

impl<'a> IntoSql<'a> for SqlParameter<'a> {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        IntoSql::into_sql(&self)
    }
}
impl<'a> IntoSql<'a> for &SqlParameter<'a> {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        match &self {
            &SqlParameter::I8(x) => tiberius::ColumnData::I16(Some(*x as i16)),
            &SqlParameter::U8(x) => tiberius::ColumnData::U8(Some(*x)),
            &SqlParameter::I16(x) => tiberius::ColumnData::I16(Some(*x)),
            &SqlParameter::U16(x) => tiberius::ColumnData::I32(Some(*x as i32)),
            &SqlParameter::I32(x) => tiberius::ColumnData::I32(Some(*x)),
            &SqlParameter::U32(x) => tiberius::ColumnData::I64(Some(*x as i64)),
            &SqlParameter::I64(x) => tiberius::ColumnData::I64(Some(*x)),
            &SqlParameter::U64(x) => tiberius::ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(*x as i128, 0),
            )),
            &SqlParameter::I128(x) => tiberius::ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(*x as i128, 0),
            )),
            &SqlParameter::F32(x) => tiberius::ColumnData::F32(Some(*x)),
            &SqlParameter::F64(x) => tiberius::ColumnData::F64(Some(*x)),
            &SqlParameter::U8Array(x) => tiberius::ColumnData::Binary(Some(x.clone())),
            &SqlParameter::String(x) => tiberius::ColumnData::String(Some(x.clone())),
            &SqlParameter::U128(_) => panic!("U128 is not supported now."),
            &SqlParameter::NULL => panic!("NULL is replacement for Default trait."), //_=>panic!("")
        }
    }
}

pub enum DbDataType {
    Bigint,
    Numeric(u8, u8),
    Bit,
    Smallint,
    Decimal(u8, u8),
    Smallmoney,
    Int,
    Tinyint,
    Money,
    Float,
    Real,
    Date,
    Datetimeoffset,
    Datetime2,
    Smalldatetime,
    Datetime,
    Time,
    Char(u32),
    Varchar(u32),
    Text(u32),
    Nchar(u32),
    Nvarchar(u32),
    Ntext(u32),
    Binary(u32),
    Varbinary(u32),
    Image,
    Cursor,
    Rowversion,
    Hierarchyid,
    Uniqueidentifier,
    SqlVariant,
    Xml,
    Geometry,
    Geography,
}

impl From<DbDataType> for String {
    fn from(value: DbDataType) -> Self {
        From::<&DbDataType>::from(&value)
    }
}
impl From<&DbDataType> for String {
    fn from(value: &DbDataType) -> Self {
        match value {
            &DbDataType::Bigint => "bigint".to_string(),
            &DbDataType::Numeric(p, s) => format!("numeric({p},{s})"),
            &DbDataType::Bit => "bit".to_string(),
            &DbDataType::Smallint => "smallint".to_string(),
            &DbDataType::Decimal(p, s) => format!("decimal({p},{s})"),
            &DbDataType::Smallmoney => "smallmoney".to_string(),
            &DbDataType::Int => "int".to_string(),
            &DbDataType::Tinyint => "tinyint".to_string(),
            &DbDataType::Money => "money".to_string(),
            &DbDataType::Float => "float".to_string(),
            &DbDataType::Real => "real".to_string(),
            &DbDataType::Date => "date".to_string(),
            &DbDataType::Datetimeoffset => "datetimeoffset".to_string(),
            &DbDataType::Datetime2 => "datetime2".to_string(),
            &DbDataType::Smalldatetime => "smalldatetime".to_string(),
            &DbDataType::Datetime => "datetime".to_string(),
            &DbDataType::Time => "time".to_string(),
            &DbDataType::Char(len) => format!("char({len})"),
            &DbDataType::Varchar(len) => format!("varchar({len})"),
            &DbDataType::Text(len) => format!("text({len})"),
            &DbDataType::Nchar(len) => format!("nchar({len})"),
            &DbDataType::Nvarchar(len) => format!("nvarchar({len})"),
            &DbDataType::Ntext(len) => format!("ntext({len})"),
            &DbDataType::Binary(len) => format!("binary({len})"),
            &DbDataType::Varbinary(len) => format!("varbinary({len})"),
            &DbDataType::Image => "image".to_string(),
            &DbDataType::Cursor => "cursor".to_string(),
            &DbDataType::Rowversion => "rowversion".to_string(),
            &DbDataType::Hierarchyid => "hierarchyid".to_string(),
            &DbDataType::Uniqueidentifier => "uniqueidentifier".to_string(),
            &DbDataType::SqlVariant => "sqlVariant".to_string(),
            &DbDataType::Xml => "xml".to_string(),
            &DbDataType::Geometry => "geometry".to_string(),
            &DbDataType::Geography => "geography".to_string(),
        }
    }
}

pub enum TableHint {
    Index(String),
    ForceSeek(String),
    ForceScan,
    HoldLock,
    NoLock,
    NoWait,
    PagLock,
    ReadCommited,
    ReadCommitedLock,
    ReadPast,
    ReadUnCommited,
    RepeatableRead,
    RowLock,
    Serializable,
    SnapShot,
    SpatialWindowMaxCells(u32),
    TabLock,
    TabLockX,
    UpDLock,
    XLock,
}

impl From<TableHint> for String {
    fn from(value: TableHint) -> Self {
        From::<&TableHint>::from(&value)
    }
}
impl From<&TableHint> for String {
    fn from(value: &TableHint) -> Self {
        match &value {
            &TableHint::Index(index) => format!("index({index})"),
            &TableHint::ForceSeek(index) => format!("ForceSeek({index})"),
            &TableHint::ForceScan => "ForceScan".to_string(),
            &TableHint::HoldLock => "HoldLock".to_string(),
            &TableHint::NoLock => "NoLock".to_string(),
            &TableHint::NoWait => "NoWait".to_string(),
            &TableHint::PagLock => "PagLock".to_string(),
            &TableHint::ReadCommited => "ReadCommited".to_string(),
            &TableHint::ReadCommitedLock => "ReadCommitedLock".to_string(),
            &TableHint::ReadPast => "ReadPast".to_string(),
            &TableHint::ReadUnCommited => "ReadUnCommited".to_string(),
            &TableHint::RepeatableRead => "RepeatableRead".to_string(),
            &TableHint::RowLock => "RowLock".to_string(),
            &TableHint::Serializable => "Serializable".to_string(),
            &TableHint::SnapShot => "SnapShot".to_string(),
            &TableHint::SpatialWindowMaxCells(value) => format!("SpatialWindowMaxCells({value})"),
            &TableHint::TabLock => "TabLock".to_string(),
            &TableHint::TabLockX => "TabLockX".to_string(),
            &TableHint::UpDLock => "UpDLock".to_string(),
            &TableHint::XLock => "XLock".to_string(),
        }
    }
}

pub trait MssqlQueryable<'a, T>: Queryable<'a, T> {
    fn with(&mut self, table_hint: TableHint) -> &mut Self;
    fn with_s(&mut self, table_hint: Cow<'a, str>) -> &mut Self;
}

#[derive(Debug, Default, Clone)]
pub struct MssqlSelectStatement<'a> {
    pub table_id: u16,
    pub schema: &'a str,
    pub table: &'a str,
    pub distinct: Option<()>,
    pub exist: Option<()>,
    pub limit: u32,
    pub offset: u32,
    pub select_columns: Option<Vec<SelectColumn<'a>>>,
    pub order_columns: Option<Vec<SelectOrderColumn<'a>>>,
    pub group_columns: Option<Vec<SelectColumn<'a>>>,
    pub wheres: Option<Vec<Predicate<'a>>>,
    pub joins: Option<Vec<JoinClosure<'a, Self>>>,
    //ms ext
    pub table_hint: Option<Cow<'a, str>>,
    pub was_gonna_inited: Option<bool>,
    pub sub_query: Option<Box<MssqlSelectStatement<'a>>>,
}

impl<'a> MssqlSelectStatement<'a> {
    pub fn new(table_id: u16, schema: &'a str, table: &'a str) -> Self {
        Self {
            table_id,
            schema,
            table,
            ..Default::default()
        }
    }

    pub fn into_clause(self) -> Self {
        Self {
            table_id: self.table_id,
            schema: self.schema,
            table: self.table,
            sub_query: Some(Box::new(self)),
            ..Default::default()
        }
    }

    pub fn from(&mut self, table: &'a str) -> &mut Self {
        self.table = table;
        self
    }

    pub fn offset(&mut self, offset: u32) -> &mut Self {
        self.offset = offset;
        self
    }
    pub fn limit(&mut self, limit: u32) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn distinct(&mut self) -> &mut Self {
        self.distinct = Some(());
        self
    }

    pub fn exist(&mut self) -> &mut Self {
        self.exist = Some(());
        self
    }

    pub fn order(&mut self, column: Vec<SelectOrderColumn<'a>>) -> &mut Self {
        self.order_columns = Some(column);
        self
    }

    pub fn group(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
        self.group_columns = Some(columns);
        self
    }

    pub fn select(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
        self.select_columns = Some(columns);
        self
    }

    pub fn when(&mut self, predicate: Predicate<'a>) -> &mut Self {
        self.wheres.get_or_insert(Vec::new()).push(predicate);
        self
    }

    pub fn join(&mut self, join: JoinType, table: Self, on: Predicate<'a>) -> &mut Self {
        self.join_s(join.into(), table, on)
    }

    pub fn join_s(&mut self, join: &'a str, table: Self, on: Predicate<'a>) -> &mut Self {
        self.joins.get_or_insert(Vec::new()).push(JoinClosure {
            join_table: table,
            join_type: join,
            join_on: on,
        });
        self
    }

    pub fn with(&mut self, table_hint: TableHint) -> &mut Self {
        self.with_s(Cow::Owned(table_hint.into()))
    }
    pub fn with_s(&mut self, table_hint: Cow<'a, str>) -> &mut Self {
        self.table_hint = Some(table_hint);
        self
    }
}

#[derive(Debug)]
pub struct MssqlRow {
    index: usize,
    row: tiberius::Row,
    pub use_index: bool,
}

impl MssqlRow {
    pub fn new(row: tiberius::Row, use_index: bool) -> Self {
        Self {
            index: 0,
            row,
            use_index,
        }
    }

    pub fn get<'a, R: tiberius::FromSql<'a>>(&'a self, name: &'a str) -> Option<R> {
        self.row.get::<R, &str>(name)
    }

    pub fn get_next<'a, R: tiberius::FromSql<'a>>(&'a mut self) -> Option<R> {
        let r = self.row.get::<R, usize>(self.index);
        self.index += 1;
        r
    }
}

pub async fn get_conn(conn_str: &str) -> anyhow::Result<MssqlDbConnection<'_>> {
    MssqlDbConnection::new(conn_str).await
}
// pub fn use_database<'a>(&'a self, conn: &'a mut MssqlDbConnection, db_mame: &str) -> &str {
//     let conn_str = conn.get_conn_str();
//     let new_conn_str = conn_str
//         .split(';')
//         .map(|x| {
//             let mut s = x.split('=');
//             let k = s.next();

//             if k == Some("DATABASE") || k == Some("INITIAL CATALOG") {
//                 format!("{:?}={}", k, db_mame)
//             } else {
//                 x.to_owned()
//             }
//         })
//         .collect::<Vec<_>>()
//         .join(";");
//     conn.set_conn_str(new_conn_str);
//     conn_str
// }
pub async fn has_db(conn: &mut MssqlDbConnection<'_>, db_mame: &str) -> anyhow::Result<bool> {
    //self.use_db(conn, "MASTER");
    let value = conn
        .get_value_i16(SqlStatement::from(format!(
            r"USE [MASTER];
        SELECT ISNULL(DB_ID('{db_mame}'),0)"
        )))
        .await?;
    Ok(value > 0)
}
pub async fn close_db(conn: &mut MssqlDbConnection<'_>, db_mame: &str) -> anyhow::Result<u64> {
    //self.use_db(conn, "MASTER");
    let sql = format!(
        r"USE [MASTER];
        declare @sql nvarchar(500);
        declare @temp varchar(1000);
        declare @spid int 
        set @sql='declare getspid cursor for select spid from sysprocesses where dbid=db_id(''{db_mame}'')';
        exec(@sql);
        open getspid 
        fetch next from getspid into @spid 
            while @@fetch_status<>-1 
            begin 
                set @temp='kill '+rtrim(@spid) 
                exec(@temp) 
                fetch next from getspid into @spid 
            end
        close getspid
        deallocate getspid"
    );
    conn.execute(SqlStatement::from(sql)).await
}
pub async fn create_db(conn: &mut MssqlDbConnection<'_>, db_mame: &str) -> anyhow::Result<u64> {
    //let conn_str = self.use_db(conn, "MASTER");
    let sql = format!(
        r"USE [MASTER];
        if db_id('{db_mame}') is null begin create database [{db_mame}] end"
    );
    let count = conn.execute(SqlStatement::from(sql)).await;
    //conn.set_conn_str(conn_str);
    count
}
pub async fn delete_db(conn: &mut MssqlDbConnection<'_>, db_mame: &str) -> anyhow::Result<u64> {
    //let conn_str = self.use_db(conn, "MASTER");
    let sql = format!(
        r"USE [MASTER];
        if db_id('{db_mame}') is not null begin drop database [{db_mame}] end"
    );
    let count = conn.execute(SqlStatement::from(sql)).await;
    //conn.set_conn_str(conn_str);
    count
}
pub fn current_db(conn: &MssqlDbConnection<'_>) -> Option<String> {
    let conn_str = conn.get_conn_str();
    let db_name = conn_str.split(';').find_map(|x| {
        let mut s = x.split('=');
        let k = s.next();

        if k == Some("DATABASE") || k == Some("INITIAL CATALOG") {
            s.next().map(|r| r.to_owned())
        } else {
            None
        }
    });
    db_name
}
pub async fn has_schema(
    conn: &mut MssqlDbConnection<'_>,
    object_schema: &ObjectSchema<'_>,
) -> anyhow::Result<bool> {
    match object_schema {
        ObjectSchema::Table(table) => has_table(conn, table).await,
        ObjectSchema::View(view) => has_view(conn, view).await,
    }
}
async fn has_table(
    conn: &mut MssqlDbConnection<'_>,
    table: &TableSchema<'_>,
) -> anyhow::Result<bool> {
    let sql = format!(
        r"IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{table_schema}].[{table_name}]') AND type in (N'U'))
        BEGIN
            SELECT 0
        END
        ELSE
        BEGIN
            SELECT 1
        END",
        table_schema = table.schema,
        table_name = table.name
    );
    let value = conn.get_value_i32(SqlStatement::from(sql)).await?;
    Ok(value > 0)
}
async fn has_view(
    conn: &mut MssqlDbConnection<'_>,
    view: &ViewSchema<'_>,
) -> anyhow::Result<bool> {
    let sql = format!(
        r"IF  NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{view_schema}].[{view_name}]') AND type in (N'V'))
        BEGIN
            SELECT 0
        END
        ELSE
        BEGIN
            SELECT 1
        END",
        view_schema = view.schema,
        view_name = view.name
    );
    let value = conn.get_value_i32(SqlStatement::from(sql)).await?;
    Ok(value > 0)
}

pub async fn create_schema(
    conn: &mut MssqlDbConnection<'_>,
    object_schema: &ObjectSchema<'_>,
) -> anyhow::Result<u64> {
    match object_schema {
        ObjectSchema::Table(table) => {
            if has_table(conn, table).await? {
                Ok(0)
            } else {
                create_table(conn, table).await
            }
        }
        ObjectSchema::View(view) => {
            if has_view(conn, view).await? {
                Ok(0)
            } else {
                create_view(conn, view).await
            }
        }
    }
}

async fn create_table(
    conn: &mut MssqlDbConnection<'_>,
    table: &TableSchema<'_>,
) -> anyhow::Result<u64> {
    let column_part = table
        .columns
        .iter()
        .map(|x| {
            let raw_db_type = match x.db_type {
                DbType::Binary => "image".to_owned(),
                DbType::Boolean => "bit".to_owned(),
                DbType::U8 => "tinyint".to_owned(),
                DbType::Date => "date".to_owned(),
                DbType::DateTime => "datetime".to_owned(),
                DbType::DateTimeOffset => "datetimeoffset(7)".to_owned(),
                DbType::Decimal => {
                    format!("decimal(18, {scale})", scale = x.scale.unwrap_or_default())
                }
                DbType::F64 => "float".to_owned(),
                DbType::Guid => "uniqueidentifier".to_owned(),
                DbType::I16 => "smallint".to_owned(),
                DbType::I32 => "int".to_owned(),
                DbType::I64 => "bigint".to_owned(),
                DbType::I8 => "tinyint".to_owned(),
                DbType::F32 => "real".to_owned(),
                DbType::Time => "time(7)".to_owned(),
                DbType::U16 => "smallint".to_owned(),
                DbType::U32 => "int".to_owned(),
                DbType::U64 => "bigint".to_owned(),
                DbType::Xml => "xml".to_owned(),
                DbType::Currency => "money".to_owned(),
                DbType::String => util::if_or(
                    x.max_length == Some(u32::MAX),
                    "nvarchar(MAX)".to_owned(),
                    format!("nvarchar({len})", len = x.max_length.unwrap_or_default()),
                ),
                DbType::AnsiString => util::if_or(
                    x.max_length == Some(u32::MAX),
                    "nvarchar(MAX)".to_owned(),
                    format!("nvarchar({len})", len = x.max_length.unwrap_or_default()),
                ),
                DbType::Json => util::if_or(
                    x.max_length == Some(u32::MAX),
                    "nvarchar(MAX)".to_owned(),
                    format!("nvarchar({len})", len = x.max_length.unwrap_or_default()),
                ),
            };
            format!(
                "[{column_name}] {db_type} {primary_key} {identity} {nullable}",
                column_name = x.name,
                db_type = raw_db_type,
                primary_key = util::if_or_default(x.primary_key, "PRIMARY KEY"),
                identity = util::if_or_default(x.identity, "IDENTITY(1,1)"),
                nullable = util::if_or(x.nullable, "NULL", "NOT NULL")
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    let index_type_part = &table.indexs.as_ref().map(|v| {
        v.into_iter()
            .map(|x| {
                let index_type = match x.index_type {
                    IndexType::PrimaryXML => util::if_or_default(x.primary_key, "Primary XML"),
                    IndexType::Clustered => "CLUSTERED",
                    IndexType::Nonclustered => "NONCLUSTERED",
                    _ => panic!("{:?} unimplement", x.index_type),
                };
                format!(
                    " CREATE {unique} {index_type} INDEX [{index_name}] ON [{schema}].[{table}] ({clumns}) {includes}",
                    unique = util::if_or_default(x.unique, "Unique"),
                    index_name=x.name,
                    schema=table.schema,
                    table=table.name,
                    clumns=x.clumns.join(","),
                    includes=x.includes.as_ref().map_or("".to_owned(),|s| format!("INCLUDE({})",s.join(",")))
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    });
    let sql = match (&table.schema, &table.name, column_part, index_type_part) {
        (schema, table, clumns, Some(indexs)) => {
            format!("CREATE TABLE [{schema}].[{table}]({clumns});{indexs}")
        }
        (schema, table, clumns, None) => {
            format!("CREATE TABLE [{schema}].[{table}]({clumns});")
        }
    };
    conn.execute(SqlStatement::from(sql)).await
}
async fn create_view(
    conn: &mut MssqlDbConnection<'_>,
    view: &ViewSchema<'_>,
) -> anyhow::Result<u64> {
    let columns_part = view
        .columns
        .iter()
        .map(|x| {
            format!(
                "[{table_alias}].[{column}] {column_alias}",
                table_alias = x.table_alias,
                column = x.name,
                column_alias = x.alias.as_ref().map_or("", |s| &s)
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    let table_join_part=view.joins.iter().map(|x|{
        let join=match x.join_type {
            JoinType::Inner=>"INNER JOIN",
            JoinType::Outer=>"OUTER JOIN",
            JoinType::Left=>"LEFT JOIN",
            JoinType::Right=>"RIGHT JOIN",
            JoinType::LeftOuter=>"LEFT OUTER JOIN",
            JoinType::RightOuter=>"RIGHT OUTER JOIN",
            JoinType::Full=>"FULL JOIN",
            JoinType::Cross=>"CROSS JOIN",
            JoinType::None=>"JOIN JOIN",
            _=>panic!("JoinType {:?} is not supported.",x.join_type)
        };

        format!(
            "[{left_schema}].[{left_name}] AS [{left_alias}] {join} [{right_schema}].[{right_name}] AS [{right_alias}] ON [{left_alias}].[{on_left}]=[{right_alias}].[{on_right}]",
            left_schema = x.left_schema,
            left_name = x.left_name,
            left_alias=x.left_alias,
            right_schema = x.right_schema,
            right_name = x.right_name,
            right_alias=x.right_alias,
            on_left = x.on_left,
            on_right = x.on_right
        )
    }).collect::<Vec<_>>().join(" ");

    let index_type_part = &view.indexs.as_ref().map(|v| {
        v.iter()
            .map(|x| {
                let index_type = match x.index_type {
                    IndexType::PrimaryXML => util::if_or_default(x.primary_key, "Primary XML"),
                    IndexType::Clustered => "CLUSTERED",
                    IndexType::Nonclustered => "NONCLUSTERED",
                    _ => panic!("{:?} unimplement", x.index_type),
                };
                format!(
                    " CREATE {unique} {index_type} INDEX [{index_name}] ON [{schema}].[{table}] ({clumns}) {includes}",
                    unique = util::if_or_default(x.unique, "Unique"),
                    index_name=x.name,
                    schema=view.schema,
                    table=view.name,
                    clumns=x.clumns.join(","),
                    includes=x.includes.as_ref().map_or("".to_owned(),|s| format!("INCLUDE({})",s.join(",")))
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    });
    let sql = match (
        &view.schema,
        &view.name,
        columns_part,
        table_join_part,
        index_type_part,
    ) {
        (schema, view, columns, table_joins, Some(indexs)) => format!(
            "CREATE VIEW [{schema}].[{view}] AS (SELECT {columns} FROM {table_joins});{indexs}"
        ),
        (schema, view, columns, table_joins, None) => {
            format!("CREATE VIEW [{schema}].[{view}] AS (SELECT {columns} FROM {table_joins})")
        }
        _ => panic!("{} failed to generate sql of creating view.", view.name),
    };
    conn.execute(SqlStatement::from(sql)).await
}

pub async fn update_schema(
    conn: &mut MssqlDbConnection<'_>,
    object_schema: &ObjectSchema<'_>,
) -> anyhow::Result<u64> {
    match object_schema {
        ObjectSchema::Table(table) => {
            if !has_table(conn, table).await? {
                Ok(0)
            } else {
                let _ = drop_table(conn, table).await;
                create_table(conn, table).await
            }
        }
        ObjectSchema::View(view) => {
            if !has_view(conn, view).await? {
                Ok(0)
            } else {
                let _ = drop_view(conn, view).await;
                create_view(conn, view).await
            }
        }
    }
}
pub async fn delete_schema(
    conn: &mut MssqlDbConnection<'_>,
    object_schema: &ObjectSchema<'_>,
) -> anyhow::Result<u64> {
    match object_schema {
        ObjectSchema::Table(table) => {
            if has_table(conn, table).await? {
                Ok(0)
            } else {
                drop_table(conn, table).await
            }
        }
        ObjectSchema::View(view) => {
            if has_view(conn, view).await? {
                Ok(0)
            } else {
                drop_view(conn, view).await
            }
        }
    }
}
async fn drop_table(
    conn: &mut MssqlDbConnection<'_>,
    table: &TableSchema<'_>,
) -> anyhow::Result<u64> {
    let sql = format!(
        "DROP TABLE [{SCHEMA}].[{TABLE}]",
        SCHEMA = table.schema,
        TABLE = table.name
    );
    conn.execute(SqlStatement::from(sql)).await
}
async fn drop_view(
    conn: &mut MssqlDbConnection<'_>,
    view: &ViewSchema<'_>,
) -> anyhow::Result<u64> {
    let sql = format!(
        "DROP VIEW [{SCHEMA}].[{VIEW}]",
        SCHEMA = view.schema,
        VIEW = view.name
    );
    conn.execute(SqlStatement::from(sql)).await
}

pub async fn save_schema(
    conn: &mut MssqlDbConnection<'_>,
    object_schema: &ObjectSchema<'_>,
) -> anyhow::Result<u64> {
    match object_schema {
        ObjectSchema::Table(table) => {
            if has_table(conn, table).await? {
                let _ = drop_table(conn, table).await;
                create_table(conn, table).await
            } else {
                create_table(conn, table).await
            }
        }
        ObjectSchema::View(view) => {
            if !has_view(conn, view).await? {
                let _ = drop_view(conn, view).await;
                create_view(conn, view).await
            } else {
                create_view(conn, view).await
            }
        }
    }
}
pub async fn truncate_table(
    conn: &mut MssqlDbConnection<'_>,
    table: &TableSchema<'_>,
) -> anyhow::Result<u64> {
    let sql = format!(
        "TRUNCATE TABLE [{SCHEMA}].[{TABLE}]",
        SCHEMA = table.schema,
        TABLE = table.name
    );
    conn.execute(SqlStatement::from(sql)).await
}

pub fn build_query<'a>(db_sql: &MssqlSelectStatement<'a>) -> anyhow::Result<SqlStatement<'a>> {
    let mut ctx = SqlStatement::new();
    let sql = build_query_with_sub(db_sql, &mut ctx);
    ctx.set_sql(sql);
    Ok(ctx)
}

fn build_query_with_sub<'a>(
    db_sql: &MssqlSelectStatement<'a>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    match &db_sql.sub_query {
        Some(sub_query) => build_query_with_ctx(
            db_sql,
            Some(build_query_with_ctx(&sub_query, None, ctx)),
            ctx,
        ),
        None => build_query_with_ctx(db_sql, None, ctx),
    }
}

fn build_query_with_ctx<'a>(
    select_statement: &MssqlSelectStatement<'a>,
    sub_query: Option<String>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    build_exists_sql(
        select_statement,
        build_page_sql(
            select_statement,
            build_query_sql(select_statement, sub_query, ctx),
            ctx,
        ),
    )
}

fn build_query_sql<'a>(
    select_statement: &MssqlSelectStatement<'a>,
    sub_query: Option<String>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    let mut builder = Vec::new();
    let _ = builder.write_all("SELECT ".as_bytes());
    if select_statement.limit > 0 && select_statement.offset == 0 {
        let _ = builder.write_all(format!("TOP {} ", select_statement.limit).as_bytes());
    } else {
        let _ = builder.write_all("TOP (100) PERCENT ".as_bytes());
    }

    if let Some(_) = select_statement.distinct {
        let _ = builder.write_all("DISTINCT ".as_bytes());
    }

    match &select_statement.select_columns {
        Some(select_columns) => {
            let _ = builder.write_all(
                select_columns
                    .iter()
                    .map(|x| build_column(x, ctx))
                    .collect::<Vec<_>>()
                    .join(",")
                    .as_bytes(),
            );
        }
        _ => {
            let _ = builder.write_all(format!("t{}.*", select_statement.table_id).as_bytes());
        }
    }

    if let Some(joins) = &select_statement.joins {
        let _ = builder.write_all(",".as_bytes());
        joins.into_iter().for_each(|join| {
            let join_table = &join.join_table;
            match &join_table.select_columns {
                Some(select_columns) => {
                    let _ = builder.write_all(
                        select_columns
                            .iter()
                            .map(|x| build_column(x, ctx))
                            .collect::<Vec<_>>()
                            .join(",")
                            .as_bytes(),
                    );
                }
                _ => {
                    let _ = builder.write_all(format!("t{}.*", join_table.table_id).as_bytes());
                }
            }
        });
    }

    match (&select_statement.order_columns, select_statement.offset) {
        (Some(order_columns), 1..) => {
            let _ = builder.write_all(
                format!(
                    ",ROW_NUMBER() OVER (ORDER BY {}) AS [ROW_NUMBER]",
                    order_columns
                        .iter()
                        .map(|x| build_order(x, ctx))
                        .collect::<Vec<_>>()
                        .join(",")
                )
                .as_bytes(),
            );
        }
        (_, _) => {}
    }

    let _ = builder.write_all(" FROM ".as_bytes());

    match sub_query {
        Some(sub_query) => {
            let _ = builder.write_all(
                format!(" ({sub_query}) AS [t{}]", select_statement.table_id).as_bytes(),
            );
        }
        None => match &select_statement.table_hint {
            Some(hint) => {
                let _ = builder.write_all(
                    format!(
                        "[{}].[{}] AS [t{}] WITH({hint})",
                        select_statement.schema,
                        select_statement.table,
                        select_statement.table_id
                    )
                    .as_bytes(),
                );
            }
            _ => {
                let _ = builder.write_all(
                    format!(
                        "[{}].[{}] AS [t{}] ",
                        select_statement.schema,
                        select_statement.table,
                        select_statement.table_id
                    )
                    .as_bytes(),
                );
            }
        },
    }

    if let Some(joins) = &select_statement.joins {
        joins.into_iter().for_each(|join| {
            let join_table = &join.join_table;
            let join_table_sql = if join_table.was_gonna_inited.is_none() {
                format!(
                    "[{}].[{}] AS t{}",
                    join_table.schema, join_table.table, join_table.table_id
                )
            } else {
                format!(
                    "({}) AS t{}",
                    build_query_with_sub(join_table, ctx),
                    join_table.table_id
                )
            };
            let _ = builder.write_all(
                format!(
                    "{join_type} {join_table_sql} ON {join_on}",
                    join_type = join.join_type,
                    join_on = build_condition(&join.join_on, ctx)
                )
                .as_bytes(),
            );
        });
    }

    if let Some(wheres) = &select_statement.wheres {
        let _ = builder.write_all(
            format!(
                " WHERE {}",
                wheres
                    .iter()
                    .map(|x| { build_where(x, ctx) })
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )
            .as_bytes(),
        );
    }

    if let Some(groups) = &select_statement.group_columns {
        let _ = builder.write_all(
            format!(
                " GROUP BY {}",
                groups
                    .iter()
                    .map(|x| { build_group(x, ctx) })
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .as_bytes(),
        );
    }

    if select_statement.offset == 0 {
        if let Some(orders) = &select_statement.order_columns {
            let _ = builder.write_all(
                format!(
                    " ORDER BY {}",
                    orders
                        .iter()
                        .map(|x| { build_order(x, ctx) })
                        .collect::<Vec<_>>()
                        .join(",")
                )
                .as_bytes(),
            );
        }
    }

    String::from_utf8(builder).unwrap()
}

fn build_page_sql<'a>(
    db_sql: &MssqlSelectStatement<'a>,
    child_sql: String,
    ctx: &mut SqlStatement<'a>,
) -> String {
    if db_sql.offset == 0 {
        return child_sql;
    }
    let new_table = format!("t{}", db_sql.table_id);
    let page_sql = format!(
        r"SELECT {columns} FROM ({child_sql}) AS [{new_table}] WHERE ([{new_table}].[ROW_NUMBER]) BETWEEN {start} AND {end}",
        columns = match &db_sql.select_columns {
            Some(select_columns) => select_columns
                .iter()
                .map(|x| { build_outer_column(x, ctx) })
                .collect::<Vec<_>>()
                .join(","),
            _ => format!("{new_table}.*"),
        },
        start = db_sql.offset + 1,
        end = db_sql.offset + db_sql.limit
    );
    page_sql
}

fn build_exists_sql<'a>(db_sql: &MssqlSelectStatement<'a>, child_sql: String) -> String {
    if db_sql.exist.is_none() {
        return child_sql;
    }
    format!("SELECT (CASE WHEN EXISTS(SELECT NULL AS [EMPTY] FROM({}) AS EMPTYTABLE) THEN 1 ELSE 0 END) AS [VALUE]", child_sql)
}

fn build_where<'a>(condition: &Predicate<'a>, ctx: &mut SqlStatement<'a>) -> String {
    build_condition(condition, ctx)
}

fn build_outer_column<'a>(
    db_sql_column: &SelectColumn<'a>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    match (&db_sql_column.expression, &db_sql_column.alias) {
        (ColumnExpression::COLUMN(Some(table_id), column), None) => {
            format!("[t{table_id}].[{column}]")
        }
        (ColumnExpression::NULL, _) => "NULL".to_string(),
        (_, Some(alias)) => format!("[t{}].[{}]", db_sql_column.table_id, alias.to_string()),
        (_, _) => panic!("complex column must has an alias."),
    }
}

fn build_column<'a>(db_sql_column: &SelectColumn<'a>, ctx: &mut SqlStatement<'a>) -> String {
    let column = build_column_expression(&db_sql_column.expression, ctx);
    match &db_sql_column.alias {
        Some(alias) => format!("{column} AS [{alias}]"),
        None => column,
    }
}

fn build_column_expression<'a>(
    expression: &ColumnExpression<'a>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    match expression {
        ColumnExpression::COLUMN(None, column) => {
            format!("[{column}]")
        }
        ColumnExpression::COLUMN(Some(table_id), column) => {
            format!("[t{table}].[{column}]", table = table_id)
        }
        ColumnExpression::CONSTANT(parameter_value) => {
            ctx.push_param(parameter_value.clone())
        }
        ColumnExpression::SUM(one) => {
            format!("SUM({})", build_column_expression(one.as_ref(), ctx))
        }
        ColumnExpression::MAX(one) => {
            format!("MAX({})", build_column_expression(one.as_ref(), ctx))
        }
        ColumnExpression::MIN(one) => {
            format!("MIN({})", build_column_expression(one.as_ref(), ctx))
        }
        ColumnExpression::AVG(one) => {
            format!("AVG({})", build_column_expression(one.as_ref(), ctx))
        }
        ColumnExpression::ADD(one, other) => format!(
            "{}+{}",
            build_column_expression(one.as_ref(), ctx),
            build_column_expression(other.as_ref(), ctx)
        ),
        ColumnExpression::Subtract(one, other) => format!(
            "{}-{}",
            build_column_expression(one.as_ref(), ctx),
            build_column_expression(other.as_ref(), ctx)
        ),
        ColumnExpression::Multiply(one, other) => format!(
            "{}*{}",
            build_column_expression(one.as_ref(), ctx),
            build_column_expression(other.as_ref(), ctx)
        ),
        ColumnExpression::Divide(one, other) => format!(
            "{}/{}",
            build_column_expression(one.as_ref(), ctx),
            build_column_expression(other.as_ref(), ctx)
        ),
        ColumnExpression::Modulo(one, other) => format!(
            "{}%{}",
            build_column_expression(one.as_ref(), ctx),
            build_column_expression(other.as_ref(), ctx)
        ),
        ColumnExpression::COUNT(one) => format!(
            "COUNT({})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::NULL => "NULL".to_string(),
        ColumnExpression::Len(one) => {
            format!("LEN({})", build_column_expression(one.as_ref(), ctx))
        }
        ColumnExpression::Convert(one, data_type) => format!(
            "CAST({}] AS {data_type})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::DatePart(one, date_part) => format!(
            "DATEPART({datepart},{})",
            build_column_expression(one.as_ref(), ctx),
            datepart = Into::<&str>::into(date_part)
        ),
        ColumnExpression::DateAdd(one, date_part, value) => {
            let date_part_param =
                ctx.push_param(SqlParameter::<'a>::String(Cow::Borrowed(date_part.into())));
            format!(
                "DATEADD({date_part_param},{value},{})",
                build_column_expression(one.as_ref(), ctx)
            )
        }
        ColumnExpression::LTrim(one, None) => format!(
            "LTRIM({})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::LTrim(one, Some(value)) => {
            let value_param = ctx.push_param(SqlParameter::String(Cow::Borrowed(value)));
            format!(
                "LTRIM({},{value_param})",
                build_column_expression(one.as_ref(), ctx)
            )
        }
        ColumnExpression::RTrim(one, None) => format!(
            "RTRIM({})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::RTrim(one, Some(value)) => {
            let value_param = ctx.push_param(SqlParameter::String(Cow::Borrowed(value)));
            format!(
                "RTRIM({},@p{value_param})",
                build_column_expression(one.as_ref(), ctx)
            )
        }
        ColumnExpression::Trim(one, None) => format!(
            "LTRIM(RTRIM({}))",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::Trim(one, Some(value)) => {
            let value_param = ctx.push_param(SqlParameter::String(Cow::Borrowed(value)));
            format!(
                "LTRIM(RTRIM({},{value_param}),{value_param})",
                build_column_expression(one.as_ref(), ctx)
            )
        }
        ColumnExpression::Replace(one, pattern, replacement) => {
            let pattern_param =
                ctx.push_param(SqlParameter::String(Cow::Borrowed(pattern)));
            let replacement_param =
                ctx.push_param(SqlParameter::String(Cow::Borrowed(replacement)));
            format!(
                "REPLACE({},{pattern_param},{replacement_param})",
                build_column_expression(one.as_ref(), ctx)
            )
        }
        ColumnExpression::Substring(one, start, len) => format!(
            "SUBSTRING({},{start},{len})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::Lower(one) => format!(
            "LOWER({})",
            build_column_expression(one.as_ref(), ctx)
        ),
        ColumnExpression::Upper(one) => format!(
            "UPPER({})",
            build_column_expression(one.as_ref(), ctx)
        ),
        //_=>panic!("no impl")
    }
}

fn build_group<'a>(db_sql_column: &SelectColumn<'a>, ctx: &mut SqlStatement<'a>) -> String {
    build_column_expression(&db_sql_column.expression, ctx)
}
fn build_order<'a>(
    db_sql_column: &SelectOrderColumn<'a>,
    ctx: &mut SqlStatement<'a>,
) -> String {
    let column = build_column_expression(&db_sql_column.column_expression, ctx);
    format!(
        "{column} {direction}",
        direction = Into::<&str>::into(&db_sql_column.direction)
    )
}

fn build_condition<'a>(condition: &Predicate<'a>, ctx: &mut SqlStatement<'a>) -> String {
    match condition {
        Predicate::Column(expression) => build_column_expression(expression, ctx),
        Predicate::LT(left, right) => format!(
            "{}<{}",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::LTE(left, right) => format!(
            "{}<={}",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::GT(left, right) => format!(
            "{}>{}",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::GTE(left, right) => format!(
            "{}>={}",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::EQ(left, right) => {
            let right_value = build_condition(right, ctx);
            match right_value.as_str() {
                "NULL" => format!("{} IS NULL", build_condition(left, ctx)),
                _ => format!("{}={}", build_condition(left, ctx), right_value),
            }
        }
        Predicate::NEQ(left, right) => {
            let right_value = build_condition(right, ctx);
            match right_value.as_str() {
                "NULL" => format!("{} IS NOT NULL", build_condition(left, ctx)),
                _ => format!("{}<>{}", build_condition(left, ctx), right_value),
            }
        }
        Predicate::CONTAINS(left, right) => format!(
            "{} IN ({})",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::NCONTAINS(left, right) => format!(
            "{} NOT IN ({})",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::LIKE(left, right) => format!(
            "{} LIKE '%'+{}+'%'",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::LLIKE(left, right) => format!(
            "{} LIKE {}+'%'",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::RLIKE(left, right) => format!(
            "{} LIKE '%'+{}",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::AND(left, right) => format!(
            "({} AND {})",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::OR(left, right) => format!(
            "({} OR {})",
            build_condition(left, ctx),
            build_condition(right, ctx)
        ),
        Predicate::NOT(left) => format!("NOT ({})", build_condition(left, ctx)),
    }
}

pub fn build_update<'a>(
    update_statement: &UpdateStatement<'a>,
) -> anyhow::Result<SqlStatement<'a>> {
    let mut ctx = SqlStatement::new();
    let mut builder = Vec::new();

    let _ = builder.write_all(
        format!(
            "UPDATE [{table_schema}].[{table_name}] SET {set_values}",
            table_schema = update_statement.schema,
            table_name = update_statement.table,
            set_values = update_statement
                .set_values
                .iter()
                .map(|x| {
                    let p = ctx.push_param(x.1.clone());
                    format!("{}={p}", x.0)
                })
                .collect::<Vec<_>>()
                .join(",")
        )
        .as_bytes(),
    );
    if let Some(wheres) = &update_statement.wheres {
        let _ = builder.write_all(" WHERE ".as_bytes());
        let _ = builder.write_all(
            wheres
                .iter()
                .map(|x| build_where(x, &mut ctx))
                .collect::<Vec<_>>()
                .join(" AND ")
                .as_bytes(),
        );
    }
    let sql = String::from_utf8(builder).unwrap();
    ctx.set_sql(sql);
    Ok(ctx)
}

pub fn build_delete<'a>(
    delete_statement: &DeleteStatement<'a>,
) -> anyhow::Result<SqlStatement<'a>> {
    let mut ctx = SqlStatement::new();
    let mut builder = Vec::new();
    let _ = builder.write_all(
        format!(
            "DELETE [{table_schema}].[{table_name}]",
            table_schema = delete_statement.schema,
            table_name = delete_statement.table
        )
        .as_bytes(),
    );
    if let Some(wheres) = &delete_statement.wheres {
        let _ = builder.write_all("WHERE ".as_bytes());
        let _ = builder.write_all(
            wheres
                .iter()
                .map(|x| build_where(x, &mut ctx))
                .collect::<Vec<_>>()
                .join(" AND ")
                .as_bytes(),
        );
    }
    let sql = String::from_utf8(builder).unwrap();
    ctx.set_sql(sql);
    Ok(ctx)
}

pub fn build_insert<'a>(
    insert_statement: &InsertStatement<'a>,
) -> anyhow::Result<SqlStatement<'a>> {
    let mut sql_statement = SqlStatement::new();
    let mut builder = Vec::new();
    if insert_statement.has_inerst_increment {
        let _ = builder.write_all(
            format!(
                "SET IDENTITY_INSERT [{table_schema}].[{table_name}] ON;",
                table_schema = insert_statement.schema,
                table_name = insert_statement.table,
            )
            .as_bytes(),
        );
    }
    let _ = builder.write_all(
        format!(
            "INSERT INTO [{table_schema}].[{table_name}]({columns}) values({values})",
            table_schema = insert_statement.schema,
            table_name = insert_statement.table,
            columns = insert_statement
                .columns
                .iter()
                .map(|x| format!("[{x}]"))
                .collect::<Vec<_>>()
                .join(","),
            values = insert_statement
                .values
                .iter()
                .map(|x| { sql_statement.push_param(x.clone()) })
                .collect::<Vec<_>>()
                .join(",")
        )
        .as_bytes(),
    );
    if insert_statement.has_increment {
        let _ = builder.write_all(";SELECT SCOPE_IDENTITY();".as_bytes());
    }
    if insert_statement.has_inerst_increment {
        let _ = builder.write_all(
            format!(
                "SET IDENTITY_INSERT [{table_schema}].[{table_name}] OFF;SELECT 0;",
                table_schema = insert_statement.schema,
                table_name = insert_statement.table,
            )
            .as_bytes(),
        );
    }
    let sql = String::from_utf8(builder).unwrap();
    sql_statement.set_sql(sql);
    Ok(sql_statement)
}

// pub async fn insert_s<'a>(
//     conn: &mut MssqlDbConnection<'a>,
//     sql_statement: SqlStatement<'a>,
// ) -> anyhow::Result<i32> {
//     let id = conn.get_value_numeric(sql_statement).await?;
//     Ok(id.value() as i32)
// }

// pub async fn insert<'a,T: Insertable>(
//     conn: &mut MssqlDbConnection<'a>,
//     t: &'a mut T,
// ) -> anyhow::Result<i32> {
//     let insert_statement =t.build_statement();
//     let sql_statement = build_insert(&insert_statement)?;
//     let id = conn.get_value_numeric(sql_statement).await?;
//     let id = id.value() as i32;
//     //t.write_back_id(id);
//     Ok(id)
// }

// pub async fn update_s<'a>(
//     conn: &mut MssqlDbConnection<'a>,
//     sql_statement: SqlStatement<'a>,
// ) -> anyhow::Result<bool> {
//     Ok(conn.execute(sql_statement).await? > 0)
// }

// pub async fn update<'a, T: Updatable>(
//     conn: &mut MssqlDbConnection<'a>,
//     t: &'a T,
// ) -> anyhow::Result<bool> {
//     let update_statement = t.build_statement();
//     let sql_statement = build_update(&update_statement)?;
//     Ok(conn.execute(sql_statement).await? > 0)
// }

// pub async fn delete_s<'a>(
//     conn: &mut MssqlDbConnection<'a>,
//     sql_statement: SqlStatement<'a>,
// ) -> anyhow::Result<bool> {
//     Ok(conn.execute(sql_statement).await? > 0)
// }

// pub async fn delete<'a, T: Deletable>(
//     conn: &mut MssqlDbConnection<'a>,
//     t: &'a T,
// ) -> anyhow::Result<bool> {
//     let delete_statement = t.build_statement();
//     let sql_statement = build_delete(&delete_statement)?;
//     Ok(conn.execute(sql_statement).await? > 0)
// }

pub async fn first<'a, T: From<MssqlRow>>(
    conn: &mut MssqlDbConnection<'a>,
    select_statement: &'a MssqlSelectStatement<'a>,
) -> anyhow::Result<Option<T>> {
    let sql_statement = build_query(select_statement)?;
    first_s(conn, sql_statement).await
}

pub async fn first_s<'a, T: From<MssqlRow>>(
    conn: &mut MssqlDbConnection<'a>,
    statement: SqlStatement<'a>,
) -> anyhow::Result<Option<T>> {
    let row = conn.get_row(statement).await?;
    match row {
        Some(row) => Ok(Some(T::from(MssqlRow::new(row, false)))),
        None => Ok(None),
    }
}

pub async fn to_vec<'a, T: From<MssqlRow>>(
    conn: &mut MssqlDbConnection<'a>,
    select_statement: &'a MssqlSelectStatement<'a>,
) -> anyhow::Result<Vec<T>> {
    let sql_statement = build_query(select_statement)?;
    to_vec_s(conn, sql_statement).await
}

pub async fn to_vec_s<'a, T: From<MssqlRow>>(
    conn: &mut MssqlDbConnection<'a>,
    statement: SqlStatement<'a>,
) -> anyhow::Result<Vec<T>> {
    let row = conn.get_rows(statement).await?;
    Ok(row
        .into_iter()
        .map(|row| T::from(MssqlRow::new(row, false)))
        .collect::<Vec<_>>())
}
