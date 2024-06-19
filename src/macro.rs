macro_rules! table {
    ($table_name:ident $table_schema{
        columns{ $($column_name:ident $column_type:ty $primary_key:ident $identity:ident),*},
        indexs{}
    }) => {
        use std::panic::{ catch_unwind };

        #[allow(dead_code)]
        #[derive(Debug)]
        pub struct $table_name<'a>{ $(pub $field_name:$type),*}

        impl<'a> $struct_name<'a> {
            fn from_row(row: &'a tiberius::Row) -> Option<Self> {
                let output=catch_unwind(||{
                    $struct_name::<'a> {
                        $($field_name: row.get(stringify!($field_name))),*
                    }
                });
                output.ok()
            }
        }
    };
}

table! {ApiLog dbo{
    columns{
        Id i32 primary_key identity ,
        Successed bool,
        RealSuccessed bool,
        CreatedDate date,
        CreatedTime datetime,
        ApiUserId i32,
        ApiUsername string(20),
        ApiType string(20),
        RequestKey string(20),
        ResponseKey string(20),
        CommandQty i32,
        ElapsedMilliSeconds i32,
        Request string(max),
        Response string(max) nullable
    },
    indexs{
        ix_1 Nonclustered primary_key unique "Successed,CreatedDate" "CreatedTime"
    }
}}

fn is_option(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(typepath) if typepath.qself.is_none() => {
            let idents_of_path = typepath
                .path
                .segments
                .iter()
                .fold(String::new(), |mut acc, v| {
                    acc.push_str(&v.ident.to_string());
                    acc.push(':');
                    acc
                });
            vec!["Option:", "std:option:Option:", "core:option:Option:"]
                .into_iter()
                .find(|s| idents_of_path == *s)
                .and_then(|_| typepath.path.segments.last())
                .is_some()
        }
        _ => false,
    }
}
