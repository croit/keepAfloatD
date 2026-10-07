//! Characterization of frame boundaries and allocation lifetimes.

use super::wire::{read_framed_bounded, read_framed_bounded_with_timeout_and_budget};
use crate::connection_admission::FrameByteBudget;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn frame_readers_preserve_eof_at_every_prefix_and_body_boundary() {
    let frame = [0, 0, 0, 3, b'a', b'b', b'c'];
    for end in 0..frame.len() {
        let budget = FrameByteBudget::new(3);
        let plain = read_framed_bounded(&mut &frame[..end], 3)
            .await
            .unwrap_err();
        let reserved = read_framed_bounded_with_timeout_and_budget(
            &mut &frame[..end],
            3,
            Duration::from_secs(1),
            &budget,
        )
        .await
        .unwrap_err();
        assert_eq!(plain.kind(), ErrorKind::UnexpectedEof, "prefix {end}");
        assert_eq!(reserved.kind(), plain.kind(), "prefix {end}");
        assert_eq!(budget.available_bytes(), 3);
    }
}

#[tokio::test]
async fn frame_readers_accept_zero_and_exact_cap_without_consuming_next_frame() {
    for payload in [b"".as_slice(), b"abc".as_slice()] {
        let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(b"next");
        let mut plain = bytes.as_slice();
        assert_eq!(read_framed_bounded(&mut plain, 3).await.unwrap(), payload);
        assert_eq!(plain, b"next");
        let budget = FrameByteBudget::new(3);
        let mut reserved = bytes.as_slice();
        let frame = read_framed_bounded_with_timeout_and_budget(
            &mut reserved,
            3,
            Duration::from_secs(1),
            &budget,
        )
        .await
        .unwrap();
        assert_eq!(frame.as_ref(), payload);
        assert_eq!(reserved, b"next");
        assert_eq!(budget.available_bytes(), 3 - payload.len());
        drop(frame);
        assert_eq!(budget.available_bytes(), 3);
    }
}

#[tokio::test]
async fn frame_cap_precedes_budget_reservation_and_body_read() {
    for size in [4, u32::MAX] {
        let prefix = size.to_be_bytes();
        let budget = FrameByteBudget::new(0);
        let plain = read_framed_bounded(&mut &prefix[..], 3).await.unwrap_err();
        let reserved = read_framed_bounded_with_timeout_and_budget(
            &mut &prefix[..],
            3,
            Duration::from_secs(1),
            &budget,
        )
        .await
        .unwrap_err();
        assert_eq!(plain.kind(), ErrorKind::InvalidData);
        assert_eq!(reserved.kind(), plain.kind());
        assert_eq!(reserved.to_string(), plain.to_string());
        assert_eq!(budget.available_bytes(), 0);
    }
}

#[tokio::test]
async fn insufficient_frame_budget_does_not_read_the_body() {
    let mut bytes = &[0, 0, 0, 3, b'a', b'b', b'c'][..];
    let budget = FrameByteBudget::new(2);
    let error =
        read_framed_bounded_with_timeout_and_budget(&mut bytes, 3, Duration::from_secs(1), &budget)
            .await
            .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::OutOfMemory);
    assert_eq!(bytes, b"abc");
    assert_eq!(budget.available_bytes(), 2);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_partial_frame_releases_only_its_own_reservation() {
    let budget = FrameByteBudget::new(5);
    let held = budget.try_reserve(2).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(16);
    writer.write_all(&[0, 0, 0, 3, b'a']).await.unwrap();
    {
        let read = read_framed_bounded_with_timeout_and_budget(
            &mut reader,
            3,
            Duration::from_secs(1),
            &budget,
        );
        tokio::pin!(read);
        assert!(futures::poll!(&mut read).is_pending());
        assert_eq!(budget.available_bytes(), 0);
    }
    assert_eq!(budget.available_bytes(), 3);
    drop(held);
    assert_eq!(budget.available_bytes(), 5);
}
