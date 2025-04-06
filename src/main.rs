use csv::StringRecord;
use sqlparser::ast::{
    BinaryOperator, Expr, ProjectionSelect, SelectItem, SetExpr, Statement, TableWithJoins, Value,
    ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Clone, Debug)]
enum Literal {
    Int(i64),
    Str(String),
    Bool(bool),
}

struct CsvRow<'a> {
    headers: &'a StringRecord,
    row: &'a StringRecord,
}

impl<'a> CsvRow<'a> {
    fn get(&self, column: &str) -> Option<Literal> {
        let idx = self.headers.iter().position(|col| col == column)?;
        let val = self.row.get(idx)?;
        if let Ok(num) = val.parse::<i64>() {
            Some(Literal::Int(num))
        } else {
            Some(Literal::Str(val.to_string()))
            //     add bool datetime support
        }
    }
}

fn convert_sql_value(value: &ValueWithSpan) -> Literal {
    match &value.value {
        Value::Number(s, _) => Literal::Int(s.parse::<i64>().unwrap_or(0)),
        Value::SingleQuotedString(s) => Literal::Str(s.clone()), // clone needed?
        Value::Boolean(b) => Literal::Bool(*b),
        _ => todo!(),
    }
}

fn evaluate_binary_op(left: Literal, op: &BinaryOperator, right: Literal) -> Option<Literal> {
    match (&left, &op, &right) {
        (Literal::Int(i1), BinaryOperator::Eq, Literal::Int(i2)) => Some(Literal::Bool(i1 == i2)),
        (Literal::Bool(b1), BinaryOperator::Eq, Literal::Bool(b2)) => Some(Literal::Bool(b1 == b2)),
        (Literal::Int(i1), BinaryOperator::Gt, Literal::Int(i2)) => Some(Literal::Bool(i1 > i2)),
        (Literal::Int(i1), BinaryOperator::GtEq, Literal::Int(i2)) => Some(Literal::Bool(i1 >= i2)),
        (Literal::Int(i1), BinaryOperator::Lt, Literal::Int(i2)) => Some(Literal::Bool(i1 < i2)),
        (Literal::Int(i1), BinaryOperator::LtEq, Literal::Int(i2)) => Some(Literal::Bool(i1 <= i2)),
        (Literal::Int(i1), BinaryOperator::Plus, Literal::Int(i2)) => Some(Literal::Int(i1 + i2)),
        (Literal::Int(i1), BinaryOperator::Minus, Literal::Int(i2)) => Some(Literal::Int(i1 - i2)),
        (Literal::Int(i1), BinaryOperator::Multiply, Literal::Int(i2)) => {
            Some(Literal::Int(i1 * i2))
        }
        (Literal::Int(i1), BinaryOperator::Divide, Literal::Int(i2)) => Some(Literal::Int(i1 / i2)),
        (Literal::Int(i1), BinaryOperator::Modulo, Literal::Int(i2)) => Some(Literal::Int(i1 % i2)),
        (Literal::Str(s1), BinaryOperator::Eq, Literal::Str(s2)) => Some(Literal::Bool(s1 == s2)),
        (Literal::Str(s1), BinaryOperator::NotEq, Literal::Str(s2)) => {
            Some(Literal::Bool(s1 != s2))
        }
        (Literal::Bool(b1), BinaryOperator::And, Literal::Bool(b2)) => {
            Some(Literal::Bool(*b1 && *b2))
        }
        (Literal::Bool(b1), BinaryOperator::Or, Literal::Bool(b2)) => {
            Some(Literal::Bool(*b1 || *b2))
        }
        (Literal::Bool(b1), BinaryOperator::Xor, Literal::Bool(b2)) => Some(Literal::Bool(b1 ^ b2)),
        _ => {
            println!("{:?}", (&left, op, &right));
            todo!()
        }
    }
}

fn evaluate_expr(expr: &Expr, row: &CsvRow) -> Option<Literal> {
    match expr {
        Expr::Value(v) => Some(convert_sql_value(v)),
        Expr::Identifier(ident) => row.get(ident.value.as_str()),
        Expr::BinaryOp { left, op, right } => {
            let l = evaluate_expr(left, row)?;
            let r = evaluate_expr(right, row)?;
            evaluate_binary_op(l, op, r)
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            todo!("we can use `like` lib")
        }
        _ => todo!(),
    }
}

fn evaluate_query(query: &str) -> Vec<StringRecord> {
    let mut results: Vec<StringRecord> = Vec::with_capacity(4);

    let ast = Parser::parse_sql(&GenericDialect, query).expect("Query is not parsable");

    if let Statement::Query(query) = &ast[0] {
        if let SetExpr::Select(select) = &*query.as_ref().body {
            assert_eq!(select.from.len(), 1, "Only single file queries are allowed");

            let main_table: TableWithJoins = select.from[0].clone();

            let mut csv =
                csv::Reader::from_path(main_table.to_string()).expect("file should be available");

            let headers: StringRecord = csv.headers().expect("Headers are necessary").clone();

            if let Some(expr) = &select.selection {
                for rec in csv.records() {
                    let rec = rec.unwrap();
                    let csv_row = CsvRow {
                        headers: &headers,
                        row: &rec,
                    };
                    let result = evaluate_expr(expr, &csv_row);
                    if let Some(Literal::Bool(true)) = result {
                        results.push(rec);
                    }
                }
            } else {
                for rec in csv.records() {
                    results.push(rec.unwrap())
                }
            }
        }
        if let Some(Expr::Value(value)) = &query.limit {
            if let Literal::Int(i) = convert_sql_value(&value) {
                results.truncate(i as usize);
            }
        }
    }
    results
}

fn main() {
    let query = "SELECT * FROM example.csv WHERE years + 1 > 2 OR animal = 'dog' limit 1";

    let results = evaluate_query(query);
    for rec in results {
        println!("{:?}", &rec.iter().collect::<Vec<_>>().join(","));
    }
}
