use std::{collections::HashMap, borrow::Cow,future::Future};

use crate::{metadata::{JoinType,TableSchema, ObjectSchema, ViewSchema}, util};

#[derive(Debug, Default)]
pub struct SqlStatement<'a> {
    pub sql: String,
    parameter_index: u8,
    pub parameters: Option<Vec<SqlParameter<'a>>>,
}
impl<'a> SqlStatement<'a> {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn from(sql: String) -> Self {
        Self {
            sql,
            ..Default::default()
        }
    }

    pub(crate) fn push_param(&mut self, value: SqlParameter<'a>) -> String {
        self.parameter_index += 1;
        self.parameters.get_or_insert(Vec::new()).push(value);
        format!("@p{}", self.parameter_index)
    }

    pub(crate) fn set_sql(&mut self, sql: String) {
        self.sql = sql;
    }
}

pub trait Queryable<'a, T> {
    fn from(&mut self, table: &'a str) -> &mut Self;
    fn offset(&mut self, offset: u32) -> &mut Self;
    fn limit(&mut self, limit: u32) -> &mut Self;
    fn distinct(&mut self) -> &mut Self;
    fn exist(&mut self) -> &mut Self;
    fn order(&mut self, column: Vec<SelectOrderColumn<'a>>) -> &mut Self;
    fn group(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self;
    fn select(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self;
    fn when(&mut self, condition: Predicate<'a>) -> &mut Self;
    fn as_clause(&self) -> Self;
    fn into_clause(self)->Self;
}

pub trait InnerQueryable<'a, T>:Queryable<'a, T> {
    type ImplSelectStatement;
    fn into_inner(self) -> Self::ImplSelectStatement;
    fn inner(&self) -> &Self::ImplSelectStatement;
}

pub trait QueryJoinable<'a, T>:InnerQueryable<'a,T>{
    fn join<K, R: InnerQueryable<'a, K,ImplSelectStatement = Self::ImplSelectStatement>>(
        &mut self,
        join: JoinType,
        table: R,
        on: Predicate<'a>,
    ) -> &mut Self;
    fn join_s<K, R: InnerQueryable<'a, K,ImplSelectStatement = Self::ImplSelectStatement>>(
        &mut self,
        join: &'a str,
        table: R,
        on: Predicate<'a>,
    ) -> &mut Self;
}

#[derive(Debug,Clone)]
pub struct JoinClosure<'a,T> {
    pub join_table: T,//SelectStatement<'a>,
    pub join_type: &'a str,
    pub join_on: Predicate<'a>,
}

// #[derive(Debug, Default)]
// pub struct SelectStatement<'a> {
//     pub joins: Option<Vec<JoinClosure<'a,Self>>>,

//     pub table_id: u16,
//     pub schema: &'a str,
//     pub table: &'a str,
//     pub distinct: Option<bool>,
//     pub exist: Option<bool>,
//     pub take: u32,
//     pub skip: u32,
//     pub select_columns: Option<Vec<SelectColumn<'a>>>,
//     pub order_columns: Option<Vec<SelectOrderColumn<'a>>>,
//     pub group_columns: Option<Vec<SelectColumn<'a>>>,
//     pub wheres: Option<Vec<Predicate<'a>>>,
//     pub sub_query:Option<Box<SelectStatement<'a>>>,

//     #[cfg(feature = "mssql")]
//     #[cfg_attr(feature = "docs", doc(cfg(feature = "mssql")))]
//     pub table_hint: Option<Cow<'a, str>>,
// }

// impl<'a> SelectStatement<'a> {
//     pub fn new(
//         table_id: u16,
//         schema: &'a str,
//         table: &'a str,
//     ) -> Self {
//         Self {
//             table_id,
//             schema,
//             table,
//             ..Default::default()
//         }
//     }

//     pub fn into_clause(self)->Self{
//         Self{
//             table_id:self.table_id,
//             schema:self.schema,
//             table:self.table,
//             sub_query:Some(Box::new(self)),
//             ..Default::default()
//         }
//     }

//     pub fn from(&mut self, table: &'a str) -> &mut Self {
//         self.table = table;
//         self
//     }

//     pub fn skip(&mut self, skip: u32) -> &mut Self {
//         self.skip = skip;
//         self
//     }
//     pub fn take(&mut self, take: u32) -> &mut Self {
//         self.take = take;
//         self
//     }

//     pub fn distinct(&mut self) -> &mut Self {
//         self.distinct = Some(true);
//         self
//     }

//     pub fn exist(&mut self) -> &mut Self {
//         self.exist = Some(true);
//         self
//     }

//     pub fn first(&mut self) -> &mut Self {
//         self.take(1);
//         self
//     }

//     pub fn order(&mut self, column: Vec<SelectOrderColumn<'a>>) -> &mut Self {
//         self.order_columns = Some(column);
//         self
//     }

//     pub fn group(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
//         self.group_columns = Some(columns);
//         self
//     }

//     pub fn select(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
//         self.select_columns = Some(columns);
//         self
//     }

//     pub fn when(&mut self, predicate: Predicate<'a>) -> &mut Self {
//         self.wheres.get_or_insert(Vec::new()).push(predicate);
//         self
//     }

//     pub fn join(&mut self, join: JoinType, table: Self, on: Predicate<'a>) -> &mut Self {
//         self.join_s(join.into(), table, on)
//     }

//     pub fn join_s(&mut self, join: &'a str, table: Self, on: Predicate<'a>) -> &mut Self {
//         self.joins.get_or_insert(Vec::new()).push(JoinClosure {
//             join_table: table,
//             join_type: join,
//             join_on: on,
//         });
//         self
//     }

//     #[cfg(feature = "mssql")]
//     #[cfg_attr(feature = "docs", doc(cfg(feature = "mssql")))]
//     pub fn with(&mut self, table_hint: crate::mssql::TableHint) -> &mut Self {
//         self.with_s(Cow::Owned(table_hint.into()))
//     }

//     #[cfg(feature = "mssql")]
//     #[cfg_attr(feature = "docs", doc(cfg(feature = "mssql")))]
//     pub fn with_s(&mut self, table_hint: Cow<'a, str>) -> &mut Self {
//         self.table_hint = Some(table_hint);
//         self
//     }
// }

#[derive(Debug, Default,Clone)]
pub enum SqlParameter<'a> {
    #[default]
    NULL,
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    F32(f32),
    I64(i64),
    U64(u64),
    F64(f64),
    I128(i128),
    U128(u128),
    String(Cow<'a,str>),
    U8Array(Cow<'a,[u8]>),
}

impl<'a> Default for &'a SqlParameter<'a> {
    fn default() -> Self {
        &SqlParameter::NULL
    }
}

// #[derive(Debug)]
// pub enum CowBox<'a,T> {
//     Value(T),
//     Ref(&'a T),
// }

impl<'a> Into<String> for &SqlParameter<'a> {
    fn into(self) -> String {
        match self {
            SqlParameter::NULL => "NULL".to_owned(),
            SqlParameter::I8(x) => format!("{x}"),
            SqlParameter::U8(x) => format!("{x}"),
            SqlParameter::I16(x) => format!("{x}"),
            SqlParameter::U16(x) => format!("{x}"),
            SqlParameter::I32(x) => format!("{x}"),
            SqlParameter::U32(x) => format!("{x}"),
            SqlParameter::F32(x) => format!("{x}"),
            SqlParameter::I64(x) => format!("{x}"),
            SqlParameter::U64(x) => format!("{x}"),
            SqlParameter::F64(x) => format!("{x}"),
            SqlParameter::I128(x) => format!("{x}"),
            SqlParameter::U128(x) => format!("{x}"),
            SqlParameter::String(x) => x.to_string(),
            _ => panic!("no impl"),
        }
    }
}

impl<'a> From<i8> for SqlParameter<'a> {
    fn from(value: i8) -> Self {
        Self::I8(value)
    }
}
impl<'a> From<u8> for SqlParameter<'a> {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}
impl<'a> From<i16> for SqlParameter<'a> {
    fn from(value: i16) -> Self {
        Self::I16(value)
    }
}
impl<'a> From<u16> for SqlParameter<'a> {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}
impl<'a> From<i32> for SqlParameter<'a> {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}
impl<'a> From<u32> for SqlParameter<'a> {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}
impl<'a> From<f32> for SqlParameter<'a> {
    fn from(value: f32) -> Self {
        Self::F32(value)
    }
}
impl<'a> From<i64> for SqlParameter<'a> {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}
impl<'a> From<u64> for SqlParameter<'a> {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}
impl<'a> From<f64> for SqlParameter<'a> {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}
impl<'a> From<i128> for SqlParameter<'a> {
    fn from(value: i128) -> Self {
        Self::I128(value)
    }
}
impl<'a> From<u128> for SqlParameter<'a> {
    fn from(value: u128) -> Self {
        Self::U128(value)
    }
}
impl<'a> From<bool> for SqlParameter<'a> {
    fn from(value: bool) -> Self {
        Self::U8(util::if_or(value, 1, 0))
    }
}

impl<'a> From<&'a str> for SqlParameter<'a> {
    fn from(value: &'a str) -> Self {
        Self::String(Cow::Borrowed( value))
    }
}

impl<'a> From<String> for SqlParameter<'a> {
    fn from(value: String) -> Self {
        Self::String(Cow::Owned(value))
    }
}

impl<'a> From<&'a [u8]> for SqlParameter<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::U8Array(Cow::Borrowed(value))
    }
}

impl<'a> From<Vec<u8>> for SqlParameter<'a> {
    fn from(value: Vec<u8>) -> Self {
        Self::U8Array(Cow::Owned(value))
    }
}

impl<'a> From<DatePart> for SqlParameter<'a> {
    fn from(value: DatePart) -> Self {
        Self::String(Cow::Borrowed(value.into()))
    }
}

#[derive(Debug,Clone)]
pub enum DatePart {
    //yy, yyyy
    Year,
    //qq, q
    Quarter,
    //mm, m
    Month,
    //dy, y
    Dayofyear,
    //dd, d
    Day,
    //wk, ww
    Week,
    //dw, w
    Weekday,
    //hh
    Hour,
    //mi, n
    Minute,
    //ss, s
    Second,
    //ms
    Millisecond,
    //mcs
    Microsecond,
    //ns
    Nanosecond,
}
impl From<DatePart> for &str {
    fn from(value: DatePart) -> Self {
        From::<&DatePart>::from(&value)
    }
}
impl From<&DatePart> for &str {
    fn from(value: &DatePart) -> Self {
        match value {
            &DatePart::Year => "year",
            &DatePart::Quarter => "quarter",
            &DatePart::Month => "month",
            &DatePart::Dayofyear => "dayofyear",
            &DatePart::Day => "day",
            &DatePart::Week => "week",
            &DatePart::Weekday => "weekday",
            &DatePart::Hour => "hour",
            &DatePart::Minute => "minute",
            &DatePart::Second => "Second",
            &DatePart::Millisecond => "millisecond",
            &DatePart::Microsecond => "microsecond",
            &DatePart::Nanosecond => "nanosecond",
        }
    }
}

#[derive(Debug, Default,Clone)]
pub struct SelectColumn<'a> {
    pub table_id: u16,
    pub expression: ColumnExpression<'a>,
    pub alias: Option<std::borrow::Cow<'a, str>>,
}
static INDEX: once_cell::sync::Lazy<std::sync::Arc<std::sync::atomic::AtomicU16>> =
    once_cell::sync::Lazy::new(|| std::sync::Arc::new(std::sync::atomic::AtomicU16::new(0)));
fn next_index() -> u16 {
    INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    INDEX.load(std::sync::atomic::Ordering::Relaxed)
}

impl<'a> SelectColumn<'a> {
    pub fn new(table_id: u16, column_expression: ColumnExpression<'a>) -> Self {
        Self {
            table_id,
            expression: column_expression,
            alias: None,
        }
    }
    fn wrap(
        table_id: u16,
        column_expression: ColumnExpression<'a>,
        alias: std::borrow::Cow<'a, str>,
    ) -> Self {
        Self {
            table_id,
            expression: column_expression,
            alias: Some(alias),
        }
    }
    
    pub fn alias(self, alias: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            self.expression,
            std::borrow::Cow::Borrowed(alias),
        )
    }

    pub fn alias_s(self, alias: String) -> Self {
        Self::wrap(
            self.table_id,
            self.expression,
            std::borrow::Cow::Owned(alias),
        )
    }

    pub fn sum(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::SUM(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn avg(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::AVG(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }

    pub fn max(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::MAX(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }

    pub fn min(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::MIN(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }

    pub fn add(self, column: SelectColumn<'a>) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::ADD(Box::new(self.expression), Box::new(column.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn sub(self, column: SelectColumn<'a>) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Subtract(Box::new(self.expression), Box::new(column.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn mul(self, column: SelectColumn<'a>) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Multiply(Box::new(self.expression), Box::new(column.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn div(self, column: SelectColumn<'a>) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Divide(Box::new(self.expression), Box::new(column.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn modulo(self, column: SelectColumn<'a>) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Modulo(Box::new(self.expression), Box::new(column.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }

    pub fn len(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Len(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn convert(self, data_type: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Convert(Box::new(self.expression), data_type),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn date_part(self, part: DatePart) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::DatePart(Box::new(self.expression), part),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn date_add(self, part: DatePart, value: u16) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::DateAdd(Box::new(self.expression), part, value),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn ltrim(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::LTrim(Box::new(self.expression), None),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn ltrim_s(self, value: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::LTrim(Box::new(self.expression), Some(value)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn rtrim(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::RTrim(Box::new(self.expression), None),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn rtrim_s(self, value: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::RTrim(Box::new(self.expression), Some(value)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn trim(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Trim(Box::new(self.expression), None),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn trim_s(self, value: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Trim(Box::new(self.expression), Some(value)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn replace(self, pattern: &'a str, replacement: &'a str) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Replace(Box::new(self.expression), pattern, replacement),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn substring(self, start: u32, len: u32) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Substring(Box::new(self.expression), start, len),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn lower(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Lower(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }
    pub fn upper(self) -> Self {
        Self::wrap(
            self.table_id,
            ColumnExpression::Upper(Box::new(self.expression)),
            std::borrow::Cow::Owned(format!("c{}", next_index())),
        )
    }

    pub fn asc(self)->SelectOrderColumn<'a>{
        SelectOrderColumn::new(self.table_id, self.expression)
    }

    pub fn desc(self)->SelectOrderColumn<'a>{
        SelectOrderColumn::with(self.table_id, self.expression,SelectOrderDirection::DESC)
    }

    pub fn eqc(self, next: SelectColumn<'a>) -> Predicate<'a> {
        Predicate::eqc(Predicate::new(self.expression), Predicate::new(next.expression))
    }

    pub fn lt<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::lt(
            Predicate::new(self.expression),
            value,
        )
    }

    pub fn lte<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::lte(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn gt<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::gt(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn gte<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::gte(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn eq<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::eq(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn neq<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::neq(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn contains<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::contains(
            Predicate::new(self.expression),
            value,
        )
    }
    pub fn ncontains<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::ncontains(
            Predicate::new(self.expression),
            value,
        )
    }

    pub fn like<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::like(
            Predicate::new(self.expression),
            value,
        )
    }

    pub fn llike<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::llike(
            Predicate::new(self.expression),
            value,
        )
    }

    pub fn rlike<P: Into<SqlParameter<'a>>>(self, value: P) -> Predicate<'a> {
        Predicate::rlike(
            Predicate::new(self.expression),
            value,
        )
    }
    
}

impl<'a> From<u8> for SelectColumn<'a> {
    fn from(value: u8) -> Self {
        Self::new(0, ColumnExpression::CONSTANT(value.into()))
    }
}

#[derive(Debug, Default,Clone)]
pub enum ColumnExpression<'a> {
    #[default]
    NULL,
    COLUMN(Option<u16>,&'a str),
    CONSTANT(SqlParameter<'a>),
    SUM(Box<ColumnExpression<'a>>),
    AVG(Box<ColumnExpression<'a>>),
    MAX(Box<ColumnExpression<'a>>),
    MIN(Box<ColumnExpression<'a>>),
    COUNT(Box<ColumnExpression<'a>>),
    ADD(
        Box<ColumnExpression<'a>>,
        Box<ColumnExpression<'a>>,
    ),
    Subtract(
        Box<ColumnExpression<'a>>,
        Box<ColumnExpression<'a>>,
    ),
    Multiply(
        Box<ColumnExpression<'a>>,
        Box<ColumnExpression<'a>>,
    ),
    Divide(
        Box<ColumnExpression<'a>>,
        Box<ColumnExpression<'a>>,
    ),
    Modulo(
        Box<ColumnExpression<'a>>,
        Box<ColumnExpression<'a>>,
    ),
    Len(Box<ColumnExpression<'a>>),
    Convert(Box<ColumnExpression<'a>>, &'a str),
    DatePart(Box<ColumnExpression<'a>>, DatePart),
    DateAdd(Box<ColumnExpression<'a>>, DatePart, u16),
    LTrim(Box<ColumnExpression<'a>>, Option<&'a str>),
    RTrim(Box<ColumnExpression<'a>>, Option<&'a str>),
    Trim(Box<ColumnExpression<'a>>, Option<&'a str>),
    Replace(Box<ColumnExpression<'a>>, &'a str, &'a str),
    Substring(Box<ColumnExpression<'a>>, u32, u32),
    Lower(Box<ColumnExpression<'a>>),
    Upper(Box<ColumnExpression<'a>>),
}

#[derive(Debug, Default,Clone)]
pub enum SelectOrderDirection {
    #[default]
    ASC,
    DESC,
}
impl From<SelectOrderDirection> for &str {
    fn from(value: SelectOrderDirection) -> Self {
        From::<&SelectOrderDirection>::from(&value)
    }
}
impl From<&SelectOrderDirection> for &str {
    fn from(value: &SelectOrderDirection) -> Self {
        match value {
            &SelectOrderDirection::ASC => "ASC",
            &SelectOrderDirection::DESC => "DESC",
        }
    }
}
#[derive(Debug, Default,Clone)]
pub struct SelectOrderColumn<'a> {
    pub table_id: u16,
    pub column_expression: ColumnExpression<'a>,
    pub direction: SelectOrderDirection,
}

impl<'a> SelectOrderColumn<'a> {
    pub fn new(table_id: u16, column_expression: ColumnExpression<'a>) -> Self {
        Self {
            table_id,
            column_expression,
            direction: SelectOrderDirection::ASC,
        }
    }

    pub fn with(table_id: u16, column_expression: ColumnExpression<'a>,direction:SelectOrderDirection)->Self{
        Self {
            table_id,
            column_expression,
            direction,
        }
    }

    pub fn asc(&mut self) -> &mut Self {
        self.direction = SelectOrderDirection::ASC;
        self
    }

    pub fn desc(&mut self) -> &mut Self {
        self.direction = SelectOrderDirection::DESC;
        self
    }

    pub fn add(self, column_expression: ColumnExpression<'a>) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::ADD(
                Box::new(self.column_expression),
                Box::new(column_expression),
            ),
        )
    }
    pub fn sub(self, column_expression: ColumnExpression<'a>) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Subtract(
                Box::new(self.column_expression),
                Box::new(column_expression),
            ),
        )
    }
    pub fn mul(self, column_expression: ColumnExpression<'a>) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Multiply(
                Box::new(self.column_expression),
                Box::new(column_expression),
            ),
        )
    }
    pub fn div(self, column_expression: ColumnExpression<'a>) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Divide(
                Box::new(self.column_expression),
                Box::new(column_expression),
            ),
        )
    }
    pub fn modulo(self, column_expression: ColumnExpression<'a>) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Modulo(
                Box::new(self.column_expression),
                Box::new(column_expression),
            ),
        )
    }

    pub fn len(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Len(Box::new(self.column_expression)),
        )
    }
    pub fn convert(self, data_type: &'a str) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Convert(Box::new(self.column_expression), data_type),
        )
    }
    pub fn date_part(self, part: DatePart) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::DatePart(Box::new(self.column_expression), part),
        )
    }
    pub fn date_add(self, part: DatePart, value: u16) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::DateAdd(Box::new(self.column_expression), part, value),
        )
    }
    pub fn ltrim(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::LTrim(Box::new(self.column_expression), None),
        )
    }
    pub fn ltrim_s(self, value: &'a str) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::LTrim(Box::new(self.column_expression), Some(value)),
        )
    }
    pub fn rtrim(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::RTrim(Box::new(self.column_expression), None),
        )
    }
    pub fn rtrim_s(self, value: &'a str) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::RTrim(Box::new(self.column_expression), Some(value)),
        )
    }
    pub fn trim(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Trim(Box::new(self.column_expression), None),
        )
    }
    pub fn trim_s(self, value: &'a str) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Trim(Box::new(self.column_expression), Some(value)),
        )
    }
    pub fn replace(self, pattern: &'a str, replacement: &'a str) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Replace(Box::new(self.column_expression), pattern, replacement),
        )
    }
    pub fn substring(self, start: u32, len: u32) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Substring(Box::new(self.column_expression), start, len),
        )
    }
    pub fn lower(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Lower(Box::new(self.column_expression)),
        )
    }
    pub fn upper(self) -> Self {
        Self::new(
            self.table_id,
            ColumnExpression::Upper(Box::new(self.column_expression)),
        )
    }
}

#[derive(Debug,Clone)]
pub enum Predicate<'a> {
    Column(ColumnExpression<'a>),
    LT(Box<Predicate<'a>>, Box<Predicate<'a>>),
    LTE(Box<Predicate<'a>>, Box<Predicate<'a>>),
    GT(Box<Predicate<'a>>, Box<Predicate<'a>>),
    GTE(Box<Predicate<'a>>, Box<Predicate<'a>>),
    EQ(Box<Predicate<'a>>, Box<Predicate<'a>>),
    NEQ(Box<Predicate<'a>>, Box<Predicate<'a>>),
    CONTAINS(Box<Predicate<'a>>, Box<Predicate<'a>>),
    NCONTAINS(Box<Predicate<'a>>, Box<Predicate<'a>>),
    LIKE(Box<Predicate<'a>>, Box<Predicate<'a>>),
    LLIKE(Box<Predicate<'a>>, Box<Predicate<'a>>),
    RLIKE(Box<Predicate<'a>>, Box<Predicate<'a>>),
    AND(Box<Predicate<'a>>, Box<Predicate<'a>>),
    OR(Box<Predicate<'a>>, Box<Predicate<'a>>),
    NOT(Box<Predicate<'a>>),
}

impl<'a> Predicate<'a> {
    pub fn new(expression: ColumnExpression<'a>) -> Self {
        Self::Column(expression)
    }

    pub fn and(self, next: Predicate<'a>) -> Self {
        Self::AND(Box::new(self), Box::new(next))
    }
    pub fn or(self, next: Predicate<'a>) -> Self {
        Self::OR(Box::new(self), Box::new(next))
    }
    pub fn not(self) -> Self {
        Self::NOT(Box::new(self))
    }

    pub fn eqc(self, next: Predicate<'a>) -> Self {
        Self::EQ(Box::new(self), Box::new(next))
    }

    pub fn lt<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::LT(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn lte<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::LTE(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn gt<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::GT(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn gte<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::GTE(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn eq<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::EQ(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn neq<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::NEQ(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn contains<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::CONTAINS(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
    pub fn ncontains<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::NCONTAINS(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }

    pub fn like<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::LIKE(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }

    pub fn llike<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::LLIKE(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }

    pub fn rlike<P: Into<SqlParameter<'a>>>(self, value: P) -> Self {
        Self::RLIKE(
            Box::new(self),
            Box::new(Self::Column(
                ColumnExpression::CONSTANT(value.into()),
            )),
        )
    }
}

pub trait Insertable {
    fn build_statement(&self)->InsertStatement<'_>;
}
#[derive(Debug,Default)]
pub struct InsertStatement<'a>{
    pub schema: &'a str,
    pub table: &'a str,
    pub has_increment:bool,
    pub has_inerst_increment:bool,
    pub columns:Vec<&'a str>,
    pub values:Vec<SqlParameter<'a>>
}
impl<'a> InsertStatement<'a> {
    pub fn new(schema: &'a str,table: &'a str,has_increment:bool)->Self{
        Self{schema,table,has_increment,..Default::default()}
    }

    pub fn insert_increment_value(&mut self, column:&'a str,value:SqlParameter<'a>)->&mut Self{
        self.has_inerst_increment=true;
        self.insert_value(column,value)
    }

    pub fn insert_value(&mut self, column:&'a str,value:SqlParameter<'a>)->&mut Self{
        self.columns.push(column);
        self.values.push(value);
        self
    }
}

#[derive(Debug,Default)]
pub struct BulkInsertStatement<'a>{
    pub schema: &'a str,
    pub table: &'a str,
    pub columns:Vec<&'a str>,
    pub row_values:Vec<Vec<SqlParameter<'a>>>
}
impl<'a> BulkInsertStatement<'a> {
    pub fn new(schema: &'a str,table: &'a str)->Self{
        Self{schema,table,..Default::default()}
    }

    pub fn add_column(&mut self, column:&'a str)->&mut Self{
        self.columns.push(column);
        self
    }

    pub fn insert_row_values(&mut self,values:Vec<SqlParameter<'a>>)->&mut Self{
        self.row_values.push(values);
        self
    }
}

pub trait Updatable {
    fn build_statement<'a>(&'a self)->UpdateStatement<'a>;
}

#[derive(Debug,Default)]
pub struct UpdateStatement<'a>{
    pub schema: &'a str,
    pub table: &'a str,
    pub wheres: Option<Vec<Predicate<'a>>>,
    pub set_values:HashMap<&'a str,SqlParameter<'a>>
}

impl<'a> UpdateStatement<'a> {
    pub fn new(schema: &'a str,table: &'a str)->Self{
        Self{schema,table,..Default::default()}
    }

    pub fn when(&mut self, predicate: Predicate<'a>) -> &mut Self {
        self.wheres.get_or_insert(Vec::new()).push(predicate);
        self
    }

    pub fn set_value(&mut self, column:&'a str,value:SqlParameter<'a>)->&mut Self{
        self.set_values.insert(column, value);
        self
    }
}

pub trait Deletable {
    fn build_statement<'a>(&'a self)->DeleteStatement<'a>;
}

#[derive(Debug,Default)]
pub struct DeleteStatement<'a>{
    pub schema: &'a str,
    pub table: &'a str,
    pub wheres: Option<Vec<Predicate<'a>>>,
}

impl<'a> DeleteStatement<'a> {
    pub fn new(schema: &'a str,table: &'a str)->Self{
        Self{schema,table,..Default::default()}
    }

    pub fn when(&mut self, predicate: Predicate<'a>) -> &mut Self {
        self.wheres.get_or_insert(Vec::new()).push(predicate);
        self
    }
}