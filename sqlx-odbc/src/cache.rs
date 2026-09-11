use crate::{error::map_error, worker::NativeConnection, OdbcArguments};
use odbc_api::{
    handles::{AsStatementRef, Statement, StatementConnection},
    parameter::InputParameter,
    ConnectionTransitions, Prepared, SharedConnection,
};
use sqlx_core::error::Error;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

type NativeStatement = Prepared<StatementConnection<SharedConnection<'static>>>;
struct Entry {
    statement: NativeStatement,
    parameters: Vec<Box<dyn InputParameter>>,
}
pub(crate) struct CachedConnection {
    entries: VecDeque<(String, Entry)>,
    persistent: bool,
    capacity: usize,
    buffered: bool,
    connection: SharedConnection<'static>,
}
impl CachedConnection {
    pub(crate) fn new(
        connection: odbc_api::Connection<'static>,
        capacity: usize,
        buffered: bool,
    ) -> Self {
        Self {
            persistent: true,
            capacity,
            buffered,
            entries: VecDeque::new(),
            connection: Arc::new(Mutex::new(connection)),
        }
    }
}
impl NativeConnection for CachedConnection {
    fn cache_size(&self) -> usize {
        self.entries.len()
    }
    fn clear_cache(&mut self) {
        self.entries.clear();
    }
    fn set_persistent(&mut self, persistent: bool) {
        self.persistent = persistent;
    }
    fn describe(&mut self, sql: &str) -> Result<sqlx_core::describe::Describe<crate::Odbc>, Error> {
        crate::execution::describe(&self.connection.lock().unwrap(), sql)
    }
    fn begin(&mut self) -> Result<(), Error> {
        self.connection
            .lock()
            .unwrap()
            .set_autocommit(false)
            .map_err(map_error)
    }
    fn commit(&mut self) -> Result<(), Error> {
        let connection = self.connection.lock().unwrap();
        connection.commit().map_err(map_error)?;
        connection.set_autocommit(true).map_err(map_error)
    }
    fn rollback(&mut self) -> Result<(), Error> {
        let connection = self.connection.lock().unwrap();
        connection.rollback().map_err(map_error)?;
        connection.set_autocommit(true).map_err(map_error)
    }
    fn ping(&mut self) -> Result<(), Error> {
        if self
            .connection
            .lock()
            .unwrap()
            .is_dead()
            .map_err(map_error)?
        {
            Err(Error::Protocol("ODBC connection is dead".into()))
        } else {
            Ok(())
        }
    }
    fn execute(
        &mut self,
        sql: &str,
        arguments: OdbcArguments,
        output: &flume::Sender<crate::execution::Output>,
    ) -> Result<(), Error> {
        let parameters = arguments.to_native_parameters().map_err(Error::Encode)?;
        let mut entry = if let Some(index) = self.entries.iter().position(|(key, _)| key == sql) {
            self.entries.remove(index).unwrap().1
        } else {
            Entry {
                statement: self
                    .connection
                    .clone()
                    .into_prepared(sql)
                    .map_err(map_error)?,
                parameters: Vec::new(),
            }
        };
        {
            let mut handle = entry.statement.as_stmt_ref();
            handle
                .reset_parameters()
                .into_result(&handle)
                .map_err(map_error)?;
        }
        entry.parameters = parameters;
        crate::execution::execute(
            &mut entry.statement,
            &entry.parameters,
            output,
            self.buffered,
        )?;
        {
            let mut handle = entry.statement.as_stmt_ref();
            handle
                .reset_parameters()
                .into_result(&handle)
                .map_err(map_error)?;
        }
        entry.parameters.clear();
        if !output.is_disconnected() && self.persistent && self.capacity != 0 {
            if self.entries.len() == self.capacity {
                self.entries.pop_front();
            }
            self.entries.push_back((sql.to_owned(), entry));
        }
        Ok(())
    }
}
