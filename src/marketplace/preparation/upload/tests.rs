use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn waiting(slot: &Slot) -> Result<UploadView> {
    for _ in 0..100 {
        if let Some(view) = slot.view() {
            return Ok(view);
        }
        tokio::task::yield_now().await;
    }
    bail!("original input did not become ready")
}
#[tokio::test]
async fn upload_is_one_exact_bounded_claim_and_only_its_reader_can_acknowledge() -> Result<()> {
    let slot = Slot::default();
    let receive = slot.clone();
    let bytes = vec![7u8; BUFFER * 2 + 1];
    let blob = Blob {
        sha256: Digest::of(&bytes),
        size: bytes.len() as u64,
    };
    let expected = blob.clone();
    let task =
        tokio::spawn(async move { receive.receive(QuarantinePurpose::Package, expected).await });
    let view = waiting(&slot).await?;
    assert!(slot.claim(Uuid::new_v4(), blob.size).is_err());
    assert!(slot.claim(view.upload_id, blob.size + 1).is_err());
    let mut claim = slot.claim(view.upload_id, blob.size)?;
    assert!(slot.claim(view.upload_id, blob.size).is_err());
    let mut input = task.await??;
    let copy = tokio::spawn(async move {
        claim.writer.write_all(&bytes).await?;
        claim.writer.shutdown().await?;
        Ok::<_, anyhow::Error>(claim.completion.await?)
    });
    tokio::task::yield_now().await;
    assert!(
        !copy.is_finished(),
        "writer must backpressure, not buffer the full input"
    );
    let mut received = vec![];
    input.stream.read_to_end(&mut received).await?;
    assert_eq!(Digest::of(&received), blob.sha256);
    slot.complete(input, true)?;
    assert!(copy.await??);
    assert!(slot.view().is_none());
    // New files are separate claims under this SAME owner, not replay of the
    // previous HTTP/native acknowledgement or a fresh quarantine allocation.
    let receiver = slot.clone();
    let next = tokio::spawn(async move { receiver.receive(QuarantinePurpose::Proof, blob).await });
    let next_view = waiting(&slot).await?;
    assert_ne!(next_view.upload_id, view.upload_id);
    slot.close();
    assert!(next.await?.is_err());
    Ok(())
}
#[tokio::test]
async fn disconnected_or_failed_input_never_rearms_a_claim() -> Result<()> {
    let slot = Slot::default();
    let receive = slot.clone();
    let bytes = b"exact upload";
    let blob = Blob {
        sha256: Digest::of(bytes),
        size: bytes.len() as u64,
    };
    let task = tokio::spawn(async move { receive.receive(QuarantinePurpose::Package, blob).await });
    let view = waiting(&slot).await?;
    let claim = slot.claim(view.upload_id, bytes.len() as u64)?;
    let mut input = task.await??;
    drop(claim);
    assert_eq!(input.stream.read(&mut [0u8; 1]).await?, 0);
    assert!(slot.claim(view.upload_id, bytes.len() as u64).is_err());
    slot.complete(input, false)?;
    assert!(slot.view().is_none());
    assert!(slot.claim(view.upload_id, bytes.len() as u64).is_err());
    Ok(())
}
#[tokio::test(start_paused = true)]
async fn an_abandoned_publisher_has_a_finite_start_deadline() -> Result<()> {
    let slot = Slot::default();
    let receive = slot.clone();
    let task = tokio::spawn(async move {
        receive
            .receive(
                QuarantinePurpose::Proof,
                Blob {
                    sha256: Digest::of(b"proof"),
                    size: 5,
                },
            )
            .await
    });
    let _ = waiting(&slot).await?;
    tokio::time::advance(Duration::from_secs(301)).await;
    assert!(task.await?.is_err());
    assert!(slot.view().is_none());
    Ok(())
}
