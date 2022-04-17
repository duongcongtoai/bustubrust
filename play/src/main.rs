use async_stream::{stream, try_stream, AsyncStream};
// use core::task::{Context, Poll};
use futures::stream::{iter, StreamExt};
use futures_core::Stream;
use std::pin::Pin;

#[derive(Debug)]
struct Foo;

#[tokio::main]
async fn main() {}

async fn st() -> Pin<Box<dyn DataBlockStream + Sync + Send>> {
    let fut = Box::pin(stream! {
        let mut st = gen_ret_stream().await?;
        while let Some(st2) = st.next().await{
            println!("{:?}",st2);
            yield Ok(0);
            // yield st2;
        }
    });
    SchemaStream::new(fut)
}

pub trait DataBlockStream: Stream<Item = Result<i64, ()>> {}

async fn gen_ret_stream() -> Result<Pin<Box<dyn Stream<Item = i64> + Sync + Send>>, ()> {
    Ok(Box::pin(iter(vec![17, 19])))
}

pub struct SchemaStream {
    inner: Pin<Box<dyn Stream<Item = Result<i64, ()>> + Send + Sync>>,
}

impl SchemaStream {
    pub fn new(
        inner: Pin<Box<dyn Stream<Item = Result<i64, ()>> + Send + Sync>>,
    ) -> Pin<Box<Self>> {
        Box::pin(SchemaStream { inner })
    }
}
impl DataBlockStream for SchemaStream {}
impl Stream for SchemaStream {
    type Item = Result<i64, ()>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/* #[async_trait]
pub trait Operator: Debug + Send + Sync {
    async fn execute(&mut self, ctx: ExecutionContext) -> SqlResult<SendableDataBlockStream>;
} */
