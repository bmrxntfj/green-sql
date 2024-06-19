use green_sql::{
    dbsql::{InnerQueryable, Insertable, QueryJoinable, Queryable, Updatable},
    dsl::{author, book},
    metadata::JoinType,
    mssql::{self,MssqlQueryable, MssqlRow, TableHint},
};
const CONN_STR:&str="connection string";
const DB_NAME: &str = "ZZKKK";

fn block_run_async<F>(future: F)
where
    F: std::future::Future<Output = Result<(), anyhow::Error>>,
{
    let result = tokio_test::block_on(future);
    match result {
        Err(err) => {
            eprintln!("{:?}", err);
            assert!(false);
        }
        _ => {}
    }
}

#[test]
fn database_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        match mssql::has_db(&mut conn, DB_NAME).await? {
            false => match mssql::create_db(&mut conn, DB_NAME).await? {
                s1 => assert_eq!(s1, 0),
            },
            true => assert!(true),
        }
        anyhow::Ok(())
    })());
}

#[test]
fn table_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        let object_schemas = [book::OBJECT_SCHEMA, author::OBJECT_SCHEMA];
        for object_schema in object_schemas {
            match mssql::has_schema(&mut conn, &object_schema).await? {
                false => {
                    let s1 = mssql::create_schema(&mut conn, &object_schema).await?;
                    assert_eq!(s1, 0);
                }
                true => {
                    let s1 = mssql::update_schema(&mut conn, &object_schema).await?;
                    assert_eq!(s1, 0);
                }
            }
        }
        anyhow::Ok(())
    })());
}

#[test]
fn single_table_first_sql_test() -> anyhow::Result<()> {
    let mut book_table = book::new_query();
    let select_statment = book_table
        .with(TableHint::NoLock)
        .select(book::default_columns())
        .when(
            book::id()
                .gt(100)
                .and(book::author_id().eq(30))
                .and(book::name().llike("Tim").or(book::name().rlike("Cook")))
                .and(book::alias("len").lt(10)),
        )
        .limit(1)
        .inner();
    let sql_statement = mssql::build_query(select_statment)?;
    assert_eq!("", sql_statement.sql);
    anyhow::Ok(())
}

#[test]
fn single_table_first_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        let mut book_table = book::new_query();
        let book = book_table
            .with(TableHint::NoLock)
            .select(book::default_columns())
            .when(
                book::id()
                    .gt(2)
                    .and(book::author_id().eq(5))
                    .and(book::name().llike("Tim").or(book::name().rlike("Cook"))),
            )
            .limit(1)
            .first(&mut conn).await?;
        // let mut conn = mssql::get_conn(CONN_STR).await?;
        // let book = mssql::first::<book::Book>(&mut conn, select_statement).await?;
        match book {
            Some(book) => assert_eq!("Tim Cook", book.name),
            None => assert!(true),
        }
        anyhow::Ok(())
    })());
}

#[test]
fn single_table_query_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        let mut book_table = book::new_query();
        let books = book_table
            .with(TableHint::NoLock)
            .select(book::default_columns())
            .when(
                book::id()
                    .gt(2)
                    .and(book::author_id().eq(5))
                    .and(book::name().llike("Tim").or(book::name().rlike("Cook"))),
            )
            .to_vec(&mut conn).await?;
        // let mut conn = mssql::get_conn(CONN_STR).await?;
        // let books = mssql::to_vec::<book::Book>(&mut conn, select_statement)
        //     .await?;
        if books.len() == 0 {
            assert!(false);
        } else {
            eprintln!("query books->{:?}", books);
            assert!(true);
        }
        anyhow::Ok(())
    })());
}

#[test]
fn single_table_query_complex_test() {
    #[derive(Debug)]
    struct SubBook {
        pub id: i32,
        pub name: String,
        pub double_pages: i32,
        pub name_length: i32,
    }

    impl From<MssqlRow> for SubBook {
        fn from(value: MssqlRow) -> Self {
            eprintln!("{:?}", value);
            let name: Option<&str> = value.get(book::NAME);
            SubBook {
                id: value.get(book::ID).unwrap_or_default(),
                name: name.unwrap_or_default().to_string(),
                double_pages: value.get("double_pages").unwrap_or_default(),
                name_length: value.get("name_length").unwrap_or_default(),
            }
        }
    }

    block_run_async((|| async {
        let mut sub_query = book::new_query()
            .from("Book")
            .with(TableHint::NoLock)
            .select(vec![
                book::id(),
                book::name(),
                book::pages().add(book::pages()).alias("double_pages"),
                book::name().len().alias("name_length"),
            ])
            .when(
                book::id()
                    .gt(2)
                    .and(book::author_id().eq(5))
                    .and(book::name().llike("Tim").or(book::name().rlike("Cook"))),
            )
            .as_clause();
        let query = sub_query
            .when(book::alias("name_length").lt(10))
            .order(vec![book::id().desc()])
            //.offset(1)
            .limit(20)
            .inner();

        let mut conn = mssql::get_conn(CONN_STR).await?;
        let sub_books = mssql::to_vec::<SubBook>(&mut conn,query).await?;
        eprintln!("{:?}", sub_books);
        assert!(true);
        anyhow::Ok(())
    })());
}

#[test]
fn table_join_first_test() {
    block_run_async((|| async {
        let mut book_table = book::new_query();
        let mut conn = mssql::get_conn(CONN_STR).await?;

        let book = book_table
            .join_author()
            .when(
                book::id()
                    .gt(2)
                    .and(book::author_id().eq(5))
                    .and(book::name().llike("Tim").or(author::name().rlike("Cook")))
                    .and(author::id().eq(5)),
            )
            .order(vec![book::id().asc()])
            .first(&mut conn)
            .await?;
        eprintln!("{:?}", book);
        match book {
            Some(mut book) => {
                let authors = book.load_authors(&mut conn).await?;
                eprintln!("{:?}", authors);
            }
            None => {}
        }
        assert!(false);
        anyhow::Ok(())
    })());
}

#[test]
fn table_join_query_test() {
    let mut book_table = book::new_query();
    let author_table = author::new_query();
    let db_sql = book_table
        .join(
            JoinType::Inner,
            author_table,
            book::author_id().eqc(author::id()),
        )
        .select(vec![
            book::id(),
            book::name(),
            book::id().add(author::id()).sum(),
            book::name().len().alias("len"),
            book::id().modulo(2.into()),
            book::name().substring(0, 3),
            author::name(),
        ])
        .when(
            book::id()
                .gt(100)
                .and(book::author_id().eq(30))
                .and(book::name().llike("Tim").or(author::name().rlike("Cook")))
                .and(book::alias("len").lt(10))
                .and(author::id().eq(100)),
        )
        .order(vec![book::id().desc()])
        .offset(10)
        .limit(20)
        .inner();
    let sql_statement = mssql::build_query(db_sql);
    match sql_statement {
        Ok(sql_statement) => assert_eq!("", sql_statement.sql),
        Err(err) => panic!("{err}"),
    }
}

#[test]
fn insert_test() {
    block_run_async((|| async {
        let mut book = book::Book::new();
        book.name = "Tim Cook".to_string();
        book.published_date = "2024-01-10".to_string();
        book.pages = 100;
        book.author_id = 5;

        let mut conn = mssql::get_conn(CONN_STR).await?;
        // let id = mssql::insert(&mut conn, &mut book).await?;
        // book.write_back_id(id);
        book.insert(&mut conn).await?;
        println!("successd to insert book, id:{}", book.id);
        anyhow::Ok(())
    })());
}

#[test]
fn update_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        let book = book::Book::find(&mut conn, 2).await?;
        match book {
            Some(book) => {
                let mut book = book::BookObserver::new(book);
                *book.author_id = 5;
                *book.pages = 500;
                eprintln!("BookObserver->{:?}", book);
                // let success = mssql::update(&mut conn, &book).await?;
                let success = book.update(&mut conn).await?;
                assert!(success);

                // book.write_change();
                // let mut book=book.into_inner();
                // book.author_id=10;
            }
            None => assert!(true),
        }
        anyhow::Ok(())
    })());
}

#[test]
fn delete_test() {
    block_run_async((|| async {
        let mut conn = mssql::get_conn(CONN_STR).await?;
        let book = book::Book::find(&mut conn, 2).await?;
        match book {
            Some(book) => {
                let success = book.delete(&mut conn).await?;
                assert!(success);
            }
            None => assert!(true),
        }
        anyhow::Ok(())
    })());
}

#[test]
fn transaction_test() {
    block_run_async((|| async {
        let mut book = book::Book::new();
        book.name = "Tim Cook".to_string();
        book.published_date = "2024-01-10".to_string();
        book.pages = 100;
        book.author_id = 5;

        let mut conn = mssql::get_conn(CONN_STR).await?;
        let tran=green_sql::mssql::MssqlDbTransaction{};
        tran.begin(&mut conn).await?;
        let r=book.insert(&mut conn).await;
        match r {
            Ok(r)=>{
                tran.commit(&mut conn).await;
                println!("successd to insert book, id:{}", r);
                
            }
            Err(err)=>{
                tran.rollback(&mut conn).await;
                eprintln!("{err}");
            }
        }
        
        anyhow::Ok(())
    })());
}