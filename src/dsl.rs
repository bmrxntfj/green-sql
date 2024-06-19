pub mod book {
    use crate::{
        dbsql::{
            ColumnExpression, Deletable, DeleteStatement, InnerQueryable, InsertStatement,
            Insertable, Predicate, QueryJoinable, Queryable, SelectColumn, SelectOrderColumn,
            SqlParameter, Updatable, UpdateStatement,
        },
        metadata::{
            DbType, IndexSchema, IndexType, JoinType, ObjectSchema, TableColumnSchema, TableSchema,
        },
        mssql::{
            MssqlDbConnection, MssqlQueryable, MssqlRow, MssqlSelectStatement,
            TableHint, self,
        },
        util::Observer,
    };
    use std::borrow::Cow;

    use super::author;

    #[derive(Debug, Default, PartialEq,Clone)]
    pub struct Book {
        pub id: i32,
        pub name: String,
        pub published_date: String,
        pub pages: i32,
        pub author_id: i32,
        pub author: Option<author::Author>,
        pub authors: Option<Vec<author::Author>>,
    }

    impl Book {
        pub fn new() -> Self {
            Self {
                ..Default::default()
            }
        }

        pub async fn load_authors<'a>(
            &mut self,
            conn: &mut MssqlDbConnection<'a>
        ) -> anyhow::Result<&Option<Vec<author::Author>>> {
            let vec = author::new_query()
                .select(author::default_columns())
                .when(author::id().eq(self.author_id))
                .to_vec(conn)
                .await?;
            self.authors = vec;
            Ok(&self.authors)
        }

        pub async fn find<'a>(
            conn: &mut MssqlDbConnection<'a>,
            id:i32
        ) -> anyhow::Result<Option<Self>> {
            let book = self::new_query()
                .select(self::default_columns())
                .when(self::id().eq(id))
                .limit(1)
                .first(conn).await?;
            Ok(book)
        }

        pub async fn insert<'a>(&'a mut self,conn: &mut MssqlDbConnection<'a>)->anyhow::Result<i32>{
            let mut insert_statement = InsertStatement::new(SCHEMA_NAME, TABLE_NAME, true);
            insert_statement.insert_value(self::NAME, SqlParameter::String(Cow::Borrowed(&self.name)));
            insert_statement.insert_value(
                self::PUBLISHED_DATE,
                SqlParameter::String(Cow::Borrowed(&self.published_date)),
            );
            insert_statement.insert_value(self::PAGES, SqlParameter::I32(self.pages));
            insert_statement.insert_value(self::AUTHORID, SqlParameter::I32(self.author_id));
            let sql_statement = mssql::build_insert(&insert_statement)?;
            let id = conn.get_value_numeric(sql_statement).await?;
            self.id = id.value() as i32;
            Ok(self.id)
        }

        pub async fn delete<'a>(
            &self,
            conn: &mut MssqlDbConnection<'a>
        ) -> anyhow::Result<bool> {
            let mut delete_statement = DeleteStatement::new(SCHEMA_NAME, TABLE_NAME);
            delete_statement.when(Predicate::new(ColumnExpression::COLUMN(None, self::ID)).eq(self.id));
            let sql_statement = mssql::build_delete(&delete_statement)?;
            Ok(conn.execute(sql_statement).await? > 0)
        }
    }

    // impl Insertable for Book {
    //     fn build_statement(&self) -> InsertStatement<'_> {
    //         let mut statement = InsertStatement::new(SCHEMA_NAME, TABLE_NAME, true);
    //         //statement.insert_value(self::ID, SqlParameter::I32(self.id));
    //         statement.insert_value(self::NAME, SqlParameter::String(Cow::Borrowed(&self.name)));
    //         statement.insert_value(
    //             self::PUBLISHED_DATE,
    //             SqlParameter::String(Cow::Borrowed(&self.published_date)),
    //         );
    //         statement.insert_value(self::PAGES, SqlParameter::I32(self.pages));
    //         statement.insert_value(self::AUTHORID, SqlParameter::I32(self.author_id));
    //         statement
    //     }
    // }

    // impl Deletable for Book {
    //     fn build_statement<'a>(&'a self) -> DeleteStatement<'a> {
    //         let mut statement = DeleteStatement::new(SCHEMA_NAME, TABLE_NAME);
    //         statement.when(Predicate::new(ColumnExpression::COLUMN(None, self::ID)).eq(self.id));
    //         statement
    //     }
    // }

    impl From<MssqlRow> for Book {
        fn from(value: MssqlRow) -> Self {
            Self::from(&value)
        }
    }

    impl From<&MssqlRow> for Book {
        fn from(value: &MssqlRow) -> Self {
            Self {
                id: value.get(self::ID).unwrap_or_default(),
                name: value
                    .get::<&str>(self::NAME)
                    .unwrap_or_default()
                    .to_string(),
                published_date: value
                    .get::<&str>(self::PUBLISHED_DATE)
                    .unwrap_or_default()
                    .to_string(),
                pages: value.get(self::PAGES).unwrap_or_default(),
                author_id: value.get(self::AUTHORID).unwrap_or_default(),
                ..Default::default()
            }
        }
    }

    impl From<&mut MssqlRow> for Book {
        fn from(value: &mut MssqlRow) -> Self {
            if value.use_index {
                Self {
                    id: value.get_next().unwrap_or_default(),
                    name: value.get_next::<&str>().unwrap_or_default().to_string(),
                    published_date: value.get_next::<&str>().unwrap_or_default().to_string(),
                    pages: value.get_next().unwrap_or_default(),
                    author_id: value.get_next().unwrap_or_default(),
                    ..Default::default()
                }
            } else {
                Book::from(&*value)
            }
        }
    }

    #[derive(Debug)]
    pub struct BookObserver {
        pub id: Observer<i32>,
        pub name: Observer<String>,
        pub published_date: Observer<String>,
        pub pages: Observer<i32>,
        pub author_id: Observer<i32>,
        pub author:Option<author::AuthorObserver>,
        book: Book,
    }
    impl BookObserver {
        pub fn new(book: Book) -> Self {
            Self {
                id: Observer::new(book.id),
                name: Observer::new(book.name.clone()),
                published_date: Observer::new(book.published_date.clone()),
                pages: Observer::new(book.pages),
                author_id: Observer::new(book.author_id),
                author:Some(author::AuthorObserver::new(book.author.clone().unwrap())),
                book,
            }
        }

        pub fn write_change(&mut self) {
            if self.id.neq() {
                self.book.id = self.id.0;
            }
            if self.name.neq() {
                self.book.name = self.name.0.clone();
            }
            if self.published_date.neq() {
                self.book.published_date = self.published_date.0.clone();
            }
            if self.pages.neq() {
                self.book.pages = self.pages.0;
            }
            if self.author_id.neq() {
                self.book.author_id = self.author_id.0;
            }
        }

        pub fn into_inner(self) -> Book {
            self.book
        }

        pub async fn update<'a>(
            &'a mut self,
            conn: &mut MssqlDbConnection<'a>,
        ) -> anyhow::Result<bool> {

            // let update_statement = self.build_statement();
            let mut update_statement = UpdateStatement::new(SCHEMA_NAME, TABLE_NAME);
            if self.id.neq() {
                update_statement.set_value(self::ID, SqlParameter::I32(self.id.0));
            }
            if self.name.neq() {
                update_statement.set_value(
                    self::NAME,
                    SqlParameter::String(Cow::Borrowed(&self.name.0)),
                );
            }
            if self.published_date.neq() {
                update_statement.set_value(
                    self::PUBLISHED_DATE,
                    SqlParameter::String(Cow::Borrowed(&self.published_date.0)),
                );
            }
            if self.pages.neq() {
                update_statement.set_value(self::PAGES, SqlParameter::I32(self.pages.0));
            }
            if self.author_id.neq() {
                update_statement.set_value(self::AUTHORID, SqlParameter::I32(self.author_id.0));
            }
            update_statement.when(Predicate::new(ColumnExpression::COLUMN(None, self::ID)).eq(self.id.1));
            //self.author.as_mut().unwrap().build_statement();
            // let mut author=&self.author;
            if let Some(author)= self.author.as_mut(){
                author.build_statement();
            }
            let sql_statement = mssql::build_update(&update_statement)?;
            Ok(conn.execute(sql_statement).await? > 0)
        }
    }

    // impl Updatable for BookObserver {
    //     fn build_statement<'a>(&'a self) -> UpdateStatement<'a> {
    //         let mut statement = UpdateStatement::new(SCHEMA_NAME, TABLE_NAME);
    //         if self.id.neq() {
    //             statement.set_value(self::ID, SqlParameter::I32(self.id.0));
    //         }
    //         if self.name.neq() {
    //             statement.set_value(
    //                 self::NAME,
    //                 SqlParameter::String(Cow::Borrowed(&self.name.0)),
    //             );
    //         }
    //         if self.published_date.neq() {
    //             statement.set_value(
    //                 self::PUBLISHED_DATE,
    //                 SqlParameter::String(Cow::Borrowed(&self.published_date.0)),
    //             );
    //         }
    //         if self.pages.neq() {
    //             statement.set_value(self::PAGES, SqlParameter::I32(self.pages.0));
    //         }
    //         if self.author_id.neq() {
    //             statement.set_value(self::AUTHORID, SqlParameter::I32(self.author_id.0));
    //         }
    //         statement.when(Predicate::new(ColumnExpression::COLUMN(None, self::ID)).eq(self.id.1));
    //         statement
    //     }
    // }

    #[derive(Debug, Clone)]
    pub struct BookQueryable<'a> {
        sql: MssqlSelectStatement<'a>,
        includes: Option<Vec<&'a str>>,
    }

    impl<'a> BookQueryable<'a> {
        pub(self) fn new(sql: MssqlSelectStatement<'a>) -> Self {
            Self {
                sql,
                includes: None,
            }
        }

        pub fn join_author(&mut self) -> &mut Self {
            self.join(
                JoinType::Inner,
                author::new_query(),
                self::author_id().eqc(author::id()),
            )
            .select(
                vec![
                    self::default_columns(),
                    author::default_columns(),
                ]
                .concat(),
            )
            .include_author()
        }

        fn include_author(&mut self) -> &mut Self {
            self.includes
                .get_or_insert(Vec::new())
                .push(author::TABLE_NAME);
            self
        }

        pub async fn first(
            &mut self,
            conn: &mut MssqlDbConnection<'a>,
        ) -> anyhow::Result<Option<Book>> {
            self.limit(1);
            let select_statement = self.inner();
            let sql_statement = mssql::build_query(select_statement)?;
            let row = conn.get_row(sql_statement).await?;
            match row {
                Some(row) => {
                    let mut msrow = MssqlRow::new(row, self.includes.is_some());
                    let mut book = Book::from(&mut msrow);
                    match &self.includes {
                        Some(include) if include.contains(&author::TABLE_NAME) => {
                            book.author = Some(author::Author::from(&mut msrow));
                        }
                        _ => {}
                    }
                    Ok(Some(book))
                }
                None => Ok(None),
            }
        }

        pub async fn to_vec(
            &mut self,
            conn: &mut MssqlDbConnection<'a>,
        ) -> anyhow::Result<Vec<Book>> {
            let select_statement = self.inner();
            let sql_statement = mssql::build_query(select_statement)?;
            let rows = conn.get_rows(sql_statement).await?;
            let vec=rows.into_iter().map(|row|{
                let mut msrow = MssqlRow::new(row, self.includes.is_some());
                let mut book = Book::from(&mut msrow);
                match &self.includes {
                    Some(include) if include.contains(&author::TABLE_NAME) => {
                        book.author = Some(author::Author::from(&mut msrow));
                    }
                    _ => {}
                }
                book
            }).collect::<Vec<_>>();
            Ok(vec)
        }
    }

    impl<'a> Queryable<'a, Book> for BookQueryable<'a> {
        fn from(&mut self, table: &'a str) -> &mut Self {
            self.sql.from(table);
            self
        }

        fn offset(&mut self, skip: u32) -> &mut Self {
            self.sql.offset(skip);
            self
        }
        fn limit(&mut self, take: u32) -> &mut Self {
            self.sql.limit(take);
            self
        }

        fn distinct(&mut self) -> &mut Self {
            self.sql.distinct();
            self
        }

        fn exist(&mut self) -> &mut Self {
            self.sql.exist();
            self
        }

        fn order(&mut self, column: Vec<SelectOrderColumn<'a>>) -> &mut Self {
            self.sql.order(column);
            self
        }

        fn group(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
            self.sql.group(columns);
            self
        }

        fn select(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
            self.sql.select(columns);
            self
        }

        fn when(&mut self, condition: Predicate<'a>) -> &mut Self {
            self.sql.when(condition);
            self
        }

        fn as_clause(&self) -> Self {
            self.clone().into_clause()
        }

        fn into_clause(self) -> Self {
            Self {
                sql: self.sql.into_clause(),
                includes: self.includes,
            }
        }
    }

    impl<'a> InnerQueryable<'a, Book> for BookQueryable<'a> {
        type ImplSelectStatement = MssqlSelectStatement<'a>;

        fn into_inner(self) -> MssqlSelectStatement<'a> {
            self.sql
        }

        fn inner(&self) -> &MssqlSelectStatement<'a> {
            &self.sql
        }
    }

    impl<'a> QueryJoinable<'a, Book> for BookQueryable<'a> {
        fn join<K, R: InnerQueryable<'a, K, ImplSelectStatement = MssqlSelectStatement<'a>>>(
            &mut self,
            join: JoinType,
            table: R,
            on: Predicate<'a>,
        ) -> &mut Self {
            self.sql.join(join, table.into_inner(), on);
            self
        }

        fn join_s<K, R: InnerQueryable<'a, K, ImplSelectStatement = MssqlSelectStatement<'a>>>(
            &mut self,
            join: &'a str,
            table: R,
            on: Predicate<'a>,
        ) -> &mut Self {
            self.sql.join_s(join, table.into_inner(), on);
            self
        }
    }

    impl<'a> MssqlQueryable<'a, Book> for BookQueryable<'a> {
        fn with(&mut self, table_hint: TableHint) -> &mut Self {
            self.sql.with(table_hint);
            self
        }

        fn with_s(&mut self, table_hint: std::borrow::Cow<'a, str>) -> &mut Self {
            self.sql.with_s(table_hint);
            self
        }
    }

    pub fn new_query<'a>() -> BookQueryable<'a> {
        BookQueryable::new(MssqlSelectStatement::new(
            self::TABLE_ID,
            SCHEMA_NAME,
            TABLE_NAME,
        ))
    }

    pub fn new_update_statement<'a>() -> UpdateStatement<'a> {
        UpdateStatement::new(SCHEMA_NAME, TABLE_NAME)
    }

    pub fn new_delete_statement<'a>() -> DeleteStatement<'a> {
        DeleteStatement::new(SCHEMA_NAME, TABLE_NAME)
    }
    
    pub fn default_columns<'a>() -> Vec<SelectColumn<'a>> {
        vec![
            id(),
            name(),
            published_date(),
            pages(),
            author_id(),
        ]
    }

    pub fn alias<'a>(alias: &'a str) -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), alias),
        )
    }

    pub fn id<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::ID),
        )
    }

    pub fn name<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::NAME),
        )
    }

    pub fn published_date<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::PUBLISHED_DATE),
        )
    }

    pub fn pages<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::PAGES),
        )
    }

    pub fn author_id<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::AUTHORID),
        )
    }

    pub const ID: &str = "Id";
    pub const NAME: &str = "Name";
    pub const PUBLISHED_DATE: &str = "PublishedDate";
    pub const PAGES: &str = "Pages";
    pub const AUTHORID: &str = "AuthorId";

    pub const SCHEMA_NAME: &str = "dbo";
    pub const TABLE_NAME: &str = "Book";
    pub const TABLE_ID:u16=1;

    pub const OBJECT_SCHEMA: ObjectSchema = ObjectSchema::Table(TableSchema {
        schema: SCHEMA_NAME,
        name: TABLE_NAME,
        columns: &[
            TableColumnSchema {
                name: ID,
                db_type: DbType::I32,
                primary_key: true,
                identity: true,
                max_length: None,
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: NAME,
                db_type: DbType::String,
                primary_key: false,
                identity: false,
                max_length: Some(50),
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: PUBLISHED_DATE,
                db_type: DbType::String,
                primary_key: false,
                identity: false,
                max_length: Some(12),
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: PAGES,
                db_type: DbType::I32,
                primary_key: false,
                identity: false,
                max_length: None,
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: AUTHORID,
                db_type: DbType::I32,
                primary_key: false,
                identity: false,
                max_length: None,
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
        ],
        indexs: Some(&[IndexSchema {
            name: "ix_1",
            index_type: IndexType::Nonclustered,
            clumns: &[NAME, AUTHORID],
            includes: Some(&[PUBLISHED_DATE]),
            unique: false,
            primary_key: false,
        }]),
    });
}

pub mod author {
    use crate::{
        dbsql::{
            ColumnExpression, DeleteStatement, InnerQueryable, Predicate, QueryJoinable,
            Queryable, SelectColumn, SelectOrderColumn, SqlParameter, UpdateStatement,
        },
        metadata::{
            DbType, IndexSchema, IndexType, JoinType, ObjectSchema, TableColumnSchema, TableSchema,
        },
        mssql::{
            MssqlDbConnection, MssqlQueryable, MssqlRow, MssqlSelectStatement,
            TableHint, self,
        },
        util::Observer,
    };
    use std::borrow::Cow;

    #[derive(Debug, Default, PartialEq,Clone)]
    pub struct Author {
        id: i32,
        name: String,
        phone: String,
    }

    impl From<MssqlRow> for Author {
        fn from(value: MssqlRow) -> Self {
            Self::from(&value)
        }
    }

    impl From<&MssqlRow> for Author {
        fn from(value: &MssqlRow) -> Self {
            Self {
                id: value.get(self::ID).unwrap_or_default(),
                name: value
                    .get::<&str>(self::NAME)
                    .unwrap_or_default()
                    .to_string(),
                phone: value
                    .get::<&str>(self::PHONE)
                    .unwrap_or_default()
                    .to_string(),
                ..Default::default()
            }
        }
    }

    impl From<&mut MssqlRow> for Author {
        fn from(value: &mut MssqlRow) -> Self {
            if value.use_index {
                Self {
                    id: value.get_next().unwrap_or_default(),
                    name: value.get_next::<&str>().unwrap_or_default().to_string(),
                    phone: value.get_next::<&str>().unwrap_or_default().to_string(),
                    ..Default::default()
                }
            } else {
                Self::from(&*value)
            }
        }
    }

    #[derive(Debug)]
    pub struct AuthorObserver {
        pub id: Observer<i32>,
        pub name: Observer<String>,
        pub phone: Observer<String>,
        book: Author,
    }
    impl AuthorObserver {
        pub fn new(book: Author) -> Self {
            Self {
                id: Observer::new(book.id),
                name: Observer::new(book.name.clone()),
                phone: Observer::new(book.phone.clone()),
                book,
            }
        }

        pub fn build_statement<'a>(&'a mut self) -> UpdateStatement<'a> {
            let mut statement = UpdateStatement::new(SCHEMA_NAME, TABLE_NAME);
            if self.id.neq() {
                statement.set_value(self::ID, SqlParameter::I32(self.id.0));
            }
            if self.name.neq() {
                statement.set_value(
                    self::NAME,
                    SqlParameter::String(std::borrow::Cow::Borrowed(&self.name.0)),
                );
            }
            if self.phone.neq() {
                statement.set_value(
                    self::PHONE,
                    SqlParameter::String(std::borrow::Cow::Borrowed(&self.phone.0)),
                );
            }
            statement
        }

        pub fn write_change(&mut self) {
            if self.id.neq() {
                self.book.id = self.id.0;
            }
            if self.name.neq() {
                self.book.name = self.name.0.clone();
            }
            if self.phone.neq() {
                self.book.phone = self.phone.0.clone();
            }
        }

        pub fn into_inner(self) -> Author {
            self.book
        }
    }

    #[derive(Debug,Clone)]
    pub struct AuthorQueryable<'a> {
        sql: MssqlSelectStatement<'a>,
        includes: Option<Vec<&'a str>>,
    }

    impl<'a> AuthorQueryable<'a> {
        pub fn new(sql: MssqlSelectStatement<'a>) -> Self {
            Self {
                sql,
                includes: None,
            }
        }

        pub async fn to_vec(
            &mut self,
            conn: &mut MssqlDbConnection<'a>,
        ) -> anyhow::Result<Option<Vec<Author>>> {
            let select_statement = self.inner();
            let sql_statement = mssql::build_query(select_statement)?;
            let rows = conn.get_rows(sql_statement).await?;
            let vec = rows
                .into_iter()
                .map(|row| {
                    let mut msrow = MssqlRow::new(row, self.includes.is_some());
                    Author::from(&mut msrow)
                })
                .collect::<Vec<_>>();
            Ok(Some(vec))
        }
    }

    impl<'a> Queryable<'a, Author> for AuthorQueryable<'a> {

        fn from(&mut self, table: &'a str) -> &mut Self {
            self.sql.from(table);
            self
        }

        fn offset(&mut self, skip: u32) -> &mut Self {
            self.sql.offset(skip);
            self
        }
        fn limit(&mut self, take: u32) -> &mut Self {
            self.sql.limit(take);
            self
        }

        fn distinct(&mut self) -> &mut Self {
            self.sql.distinct();
            self
        }

        fn exist(&mut self) -> &mut Self {
            self.sql.exist();
            self
        }

        fn order(&mut self, column: Vec<SelectOrderColumn<'a>>) -> &mut Self {
            self.sql.order(column);
            self
        }

        fn group(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
            self.sql.group(columns);
            self
        }

        fn select(&mut self, columns: Vec<SelectColumn<'a>>) -> &mut Self {
            self.sql.select(columns);
            self
        }

        fn when(&mut self, condition: Predicate<'a>) -> &mut Self {
            self.sql.when(condition);
            self
        }

        fn as_clause(&self) -> Self {
            self.clone().into_clause()
        }
        
        fn into_clause(self) -> Self {
            Self::new(self.sql.into_clause())
        }
    }

    impl<'a> InnerQueryable<'a, Author> for AuthorQueryable<'a> {
        type ImplSelectStatement = MssqlSelectStatement<'a>;

        fn into_inner(self) -> MssqlSelectStatement<'a> {
            self.sql
        }

        fn inner(&self) -> &MssqlSelectStatement<'a> {
            &self.sql
        }
    }

    impl<'a> QueryJoinable<'a, Author> for AuthorQueryable<'a> {
        fn join<K, R: InnerQueryable<'a, K, ImplSelectStatement = MssqlSelectStatement<'a>>>(
            &mut self,
            join: JoinType,
            table: R,
            on: Predicate<'a>,
        ) -> &mut Self {
            self.sql.join(join, table.into_inner(), on);
            self
        }

        fn join_s<K, R: InnerQueryable<'a, K, ImplSelectStatement = MssqlSelectStatement<'a>>>(
            &mut self,
            join: &'a str,
            table: R,
            on: Predicate<'a>,
        ) -> &mut Self {
            self.sql.join_s(join, table.into_inner(), on);
            self
        }
    }

    impl<'a> MssqlQueryable<'a, Author> for AuthorQueryable<'a> {
        fn with(&mut self, table_hint: TableHint) -> &mut Self {
            self.sql.with(table_hint);
            self
        }

        fn with_s(&mut self, table_hint: std::borrow::Cow<'a, str>) -> &mut Self {
            self.sql.with_s(table_hint);
            self
        }
    }

    pub fn new_query<'a>() -> AuthorQueryable<'a> {
        AuthorQueryable::new(MssqlSelectStatement::new(
            self::TABLE_ID,
            SCHEMA_NAME,
            TABLE_NAME,
        ))
    }

    pub fn new_update_statement<'a>() -> UpdateStatement<'a> {
        UpdateStatement::new(SCHEMA_NAME, TABLE_NAME)
    }

    pub fn new_delete_statement<'a>() -> DeleteStatement<'a> {
        DeleteStatement::new(SCHEMA_NAME, TABLE_NAME)
    }

    pub fn default_columns<'a>() -> Vec<SelectColumn<'a>> {
        vec![id(), name(), phone()]
    }

    pub fn alias<'a>(alias: &'a str) -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), alias),
        )
    }

    pub fn id<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::ID),
        )
    }

    pub fn name<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::NAME),
        )
    }

    pub fn phone<'a>() -> SelectColumn<'a> {
        SelectColumn::new(
            self::TABLE_ID,
            ColumnExpression::COLUMN(Some(self::TABLE_ID), self::PHONE),
        )
    }

    pub const ID: &str = "Id";
    pub const NAME: &str = "Name";
    pub const PHONE: &str = "Phone";

    pub const SCHEMA_NAME: &str = "dbo";
    pub const TABLE_NAME: &str = "Author";
    pub const TABLE_ID:u16=0;

    pub const OBJECT_SCHEMA: ObjectSchema = ObjectSchema::Table(TableSchema {
        schema: SCHEMA_NAME,
        name: TABLE_NAME,
        columns: &[
            TableColumnSchema {
                name: ID,
                db_type: DbType::I32,
                primary_key: true,
                identity: true,
                max_length: None,
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: NAME,
                db_type: DbType::String,
                primary_key: false,
                identity: false,
                max_length: Some(50),
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
            TableColumnSchema {
                name: PHONE,
                db_type: DbType::String,
                primary_key: false,
                identity: false,
                max_length: Some(12),
                scale: None,
                nullable: false,
                desc: None,
                alias: None,
            },
        ],
        indexs: Some(&[IndexSchema {
            name: "ix_1",
            index_type: IndexType::Nonclustered,
            clumns: &[NAME],
            includes: None,
            unique: false,
            primary_key: false,
        }]),
    });
}
