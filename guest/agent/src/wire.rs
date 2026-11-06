use anyhow::{Result, bail};
use prost::Message;
use prost_types::{Struct, Value, value::Kind};
use serde_json::Value as Json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

include!(concat!(env!("OUT_DIR"), "/sandbox.v1.rs"));

pub const MAX_FRAME: usize = 8 * 1024 * 1024;
pub const CHUNK: usize = 64 * 1024;

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Frame>> {
    let mut prefix = [0u8; 4];
    if r.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    r.read_exact(&mut prefix[1..]).await?;
    let n = u32::from_be_bytes(prefix) as usize;
    if n == 0 || n > MAX_FRAME {
        bail!("invalid frame length {n}");
    }
    let mut data = vec![0; n];
    r.read_exact(&mut data).await?;
    Ok(Some(Frame::decode(data.as_slice())?))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, frame: &Frame) -> Result<()> {
    let n = frame.encoded_len();
    if n == 0 || n > MAX_FRAME {
        bail!("invalid frame length {n}");
    }
    w.write_all(&(n as u32).to_be_bytes()).await?;
    w.write_all(&frame.encode_to_vec()).await?;
    Ok(())
}

fn to_value(v: &Json) -> Value {
    Value {
        kind: Some(match v {
            Json::Null => Kind::NullValue(0),
            Json::Bool(v) => Kind::BoolValue(*v),
            Json::Number(v) => Kind::NumberValue(v.as_f64().unwrap()),
            Json::String(v) => Kind::StringValue(v.clone()),
            Json::Array(v) => Kind::ListValue(prost_types::ListValue {
                values: v.iter().map(to_value).collect(),
            }),
            Json::Object(v) => Kind::StructValue(Struct {
                fields: v.iter().map(|(k, v)| (k.clone(), to_value(v))).collect(),
            }),
        }),
    }
}
fn from_value(v: &Value) -> Json {
    match &v.kind {
        Some(Kind::BoolValue(v)) => Json::Bool(*v),
        Some(Kind::NumberValue(v)) => serde_json::json!(v),
        Some(Kind::StringValue(v)) => Json::String(v.clone()),
        Some(Kind::ListValue(v)) => Json::Array(v.values.iter().map(from_value).collect()),
        Some(Kind::StructValue(v)) => Json::Object(
            v.fields
                .iter()
                .map(|(k, v)| (k.clone(), from_value(v)))
                .collect(),
        ),
        _ => Json::Null,
    }
}
impl Frame {
    pub fn params_json(&self) -> Json {
        self.params
            .as_ref()
            .map(|s| {
                from_value(&Value {
                    kind: Some(Kind::StructValue(s.clone())),
                })
            })
            .unwrap_or_else(|| serde_json::json!({}))
    }
    pub fn reply(&self, params: Json, data: Vec<u8>, eof: bool) -> Frame {
        let params = match to_value(&params).kind {
            Some(Kind::StructValue(s)) => Some(s),
            _ => None,
        };
        Frame {
            id: self.id.clone(),
            method: self.method.clone(),
            params,
            data,
            eof,
            error: String::new(),
        }
    }
    pub fn failure(&self, error: impl ToString) -> Frame {
        let mut f = self.reply(serde_json::json!({}), vec![], true);
        f.error = error.to_string();
        f
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn framing_back_to_back_and_bounds() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let base = Frame {
            id: "x".into(),
            method: "info".into(),
            ..Default::default()
        };
        let f = base.reply(
            serde_json::json!({"n":42,"env":{"X":"y"}}),
            vec![1, 2],
            true,
        );
        write_frame(&mut a, &f).await.unwrap();
        write_frame(&mut a, &f).await.unwrap();
        for _ in 0..2 {
            assert_eq!(read_frame(&mut b).await.unwrap().unwrap(), f);
        }
        a.write_all(&((MAX_FRAME + 1) as u32).to_be_bytes())
            .await
            .unwrap();
        assert!(read_frame(&mut b).await.is_err());
    }
    #[tokio::test]
    async fn golden_wire_matches_go() {
        let frame = Frame {
            id: "a".into(),
            method: "ping".into(),
            data: vec![0, 1],
            eof: true,
            ..Default::default()
        };
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &frame).await.unwrap();
        assert_eq!(
            encoded,
            vec![
                0, 0, 0, 15, 10, 1, 97, 18, 4, 112, 105, 110, 103, 34, 2, 0, 1, 40, 1
            ]
        );
    }
    #[tokio::test]
    async fn truncated_prefix_is_error() {
        let (mut a, mut b) = tokio::io::duplex(16);
        a.write_all(&[0, 0]).await.unwrap();
        drop(a);
        assert!(read_frame(&mut b).await.is_err());
    }
}
