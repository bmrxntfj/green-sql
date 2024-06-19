#[derive(Debug)]
pub enum ObjectSchema<'a> {
    Table(TableSchema<'a>),
    View(ViewSchema<'a>),
}

#[derive(Debug)]
pub struct ViewSchema<'a> {
    pub schema: &'a str,
    pub name: &'a str,
    pub indexs: Option<&'a [IndexSchema<'a>]>,
    pub columns: &'a [ViewColumnSchema<'a>],
    pub joins: Vec<JoinSchema<'a>>,
}


#[derive(Debug)]
pub struct TableSchema<'a> {
    pub schema: &'a str,
    pub name: &'a str,
    pub indexs: Option<&'a [IndexSchema<'a>]>,
    pub columns: &'a [TableColumnSchema<'a>],
}

#[derive(Debug)]
pub enum IndexType {
    Clustered,
    Nonclustered,
    PrimaryXML,
    Spatial,
}

#[derive(Debug)]
pub struct IndexSchema<'a> {
    pub name: &'a str,
    pub clumns: &'a [&'a str],
    pub includes: Option<&'a [&'a str]>,
    pub index_type: IndexType,
    pub unique: bool,
    pub primary_key: bool,
}

#[derive(Debug)]
pub enum DbType {
    //
    // Summary:
    //     A variable-length stream of binary data ranging between 1 and 8,000 bytes.
    Binary,
    //
    // Summary:
    //     A simple type representing Boolean values of true or false.
    Boolean,
    //
    // Summary:
    //     A currency value ranging from -2 63 (or -922,337,203,685,477.5808) to 2 63 -1
    //     (or +922,337,203,685,477.5807) with an accuracy to a ten-thousandth of a currency
    //     unit.
    Currency,
    //
    // Summary:
    //     A type representing a date value.
    Date,
    //
    // Summary:
    //     A type representing a SQL Server DateTime value. If you want to use a SQL Server
    //     time value, use System.Data.SqlDbType.Time.
    Time,
    //
    // Summary:
    //     A type representing a date and time value.
    DateTime,
    //
    // Summary:
    //     Date and time data with time zone awareness. Date value range is from January
    //     1,1 AD through December 31, 9999 AD. Time value range is 00:00:00 through 23:59:59.9999999
    //     with an accuracy of 100 nanoseconds. Time zone value range is -14:00 through
    //     +14:00.
    DateTimeOffset,
    //
    // Summary:
    //     A simple type representing values ranging from 1.0 x 10 -28 to approximately
    //     7.9 x 10 28 with 28-29 significant digits.
    Decimal,
    //
    // Summary:
    //     A globally unique identifier (or GUID).
    Guid,
    //
    // Summary:
    //     An 8-bit unsigned integer ranging in value from 0 to 255.
    U8,
    //
    // Summary:
    //     An integral type representing signed 8-bit integers with values between -128
    //     and 127.
    I8,
    //
    // Summary:
    //     An integral type representing signed 16-bit integers with values between -32768
    //     and 32767.
    I16,
    //
    // Summary:
    //     An integral type representing unsigned 16-bit integers with values between 0
    //     and 65535.
    U16,
    //
    // Summary:
    //     An integral type representing signed 32-bit integers with values between -2147483648
    //     and 2147483647.
    I32,
    //
    // Summary:
    //     An integral type representing unsigned 32-bit integers with values between 0
    //     and 4294967295.
    U32,
    //
    // Summary:
    //     An integral type representing signed 64-bit integers with values between -9223372036854775808
    //     and 9223372036854775807.
    I64,
    //
    // Summary:
    //     An integral type representing unsigned 64-bit integers with values between 0
    //     and 18446744073709551615.
    U64,
    //
    // Summary:
    //     A floating point type representing values ranging from approximately 1.5 x 10
    //     -45 to 3.4 x 10 38 with a precision of 7 digits.
    F32,
    //
    // Summary:
    //     A floating point type representing values ranging from approximately 5.0 x 10
    //     -324 to 1.7 x 10 308 with a precision of 15-16 digits.
    F64,
    //
    // Summary:
    //     A type representing Unicode character strings.
    String,
    //
    // Summary:
    //     A variable-length stream of non-Unicode characters ranging between 1 and 8,000
    //     characters.
    AnsiString,
    Xml,
    Json,
}

#[derive(Debug)]
pub struct TableColumnSchema<'a> {
    pub name: &'a str,
    pub db_type: DbType,
    pub primary_key: bool,
    pub identity: bool,
    pub max_length: Option<u32>,
    pub scale: Option<u16>,
    pub nullable: bool,
    //pub default_value:Option<>,
    pub desc: Option<&'a str>,
    pub alias: Option<&'a str>,
}

#[derive(Debug)]
pub struct ViewColumnSchema<'a> {
    pub name: &'a str,
    pub table_alias: &'a str,
    pub alias: Option<&'a str>,
}

#[derive(Debug)]
pub enum JoinType {
    None,
    Inner,
    Outer,
    Left,
    Right,
    LeftOuter,
    RightOuter,
    Full,
    Cross,
}
impl From<JoinType> for &str {
    fn from(value: JoinType) -> Self {
        From::<&JoinType>::from(&value)
    }
}
impl From<&JoinType> for &str {
    fn from(value: &JoinType) -> Self {
        match value {
            &JoinType::None => "",
            &JoinType::Inner => "INNER JOIN",
            &JoinType::Outer => "OUTER JOIN",
            &JoinType::Left => "LEFT JOIN",
            &JoinType::Right => "RIGHT JOIN",
            &JoinType::LeftOuter => "LEFTOUTER JOIN",
            &JoinType::RightOuter => "RIGHTOUTER JOIN",
            &JoinType::Full => "FULL JOIN",
            &JoinType::Cross => "CROSS JOIN",
        }
    }
}

#[derive(Debug)]
pub struct JoinSchema<'a> {
    pub join_type: JoinType,
    pub left_schema: &'a str,
    pub left_name: &'a str,
    pub left_alias: &'a str,
    pub right_schema: &'a str,
    pub right_name: &'a str,
    pub right_alias: &'a str,
    pub on_left: &'a str,
    pub on_right: &'a str,
}
