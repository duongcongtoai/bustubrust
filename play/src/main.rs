use async_stream::{stream, try_stream, AsyncStream};
use comfy_table::{Cell, Table, TableComponent};
use datafusion::{
    arrow::{error::Result, record_batch::RecordBatch, util::display::array_value_to_string},
    error::Result,
};
// use core::task::{Context, Poll};
use futures::stream::StreamExt;
use futures_core::Stream;
use proc_macro::TokenStream;
use std::{iter::IntoIterator, pin::Pin};
use strum::IntoEnumIterator;

#[proc_macro]
pub fn do_something(input: TokenStream) -> TokenStream {}
