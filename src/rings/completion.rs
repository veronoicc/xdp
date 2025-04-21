//! The [`CompletionRing`] is a consumer ring that userspace can dequeue packets
//! that have been sent on the NIC queue the ring is bound to

use crate::{libc::{self, rings}, slab::Slab, Umem};

/// The ring used to dequeue buffers that the kernel has finished sending
pub struct CompletionRing {
    #[allow(missing_docs)]
    pub ring: super::XskConsumer<libc::xdp::xdp_desc>,
    _mmap: crate::mmap::Mmap,
}

impl CompletionRing {
    pub(crate) fn new(
        socket: std::os::fd::RawFd,
        cfg: &super::RingConfig,
        offsets: &rings::xdp_mmap_offsets,
    ) -> Result<Self, crate::socket::SocketError> {
        let (_mmap, mut ring) = super::map_ring(
            socket,
            cfg.completion_count,
            rings::RingPageOffsets::Completion,
            &offsets.completion,
        )
        .map_err(|inner| crate::socket::SocketError::RingMap {
            inner,
            ring: super::Ring::Completion,
        })?;

        ring.cached_consumed = 0;
        ring.cached_produced = 0;

        Ok(Self {
            ring: super::XskConsumer(ring),
            _mmap,
        })
    }

    /// Dequeues up to `num_packets` and makes them available for use again
    ///
    /// # Returns
    ///
    /// The number of packets that were actually dequeued.
    pub fn dequeue<S: Slab>(&mut self, umem: &mut Umem, packets: &mut S) -> usize {
        let nb = packets.available();
        if nb == 0 {
            return 0;
        }

        let (actual, idx) = self.ring.peek(nb as _);

        if actual > 0 {
            for i in idx..idx + actual {
                let desc = self.ring.get(i);
                packets.push_front(
                    // SAFETY: The user is responsible for the lifetime of the
                    // packets we are returning
                    unsafe { umem.packet(desc) },
                );
                umem.free_addr(desc.addr);
            }

            self.ring.release(actual as _);
        }

        actual
    }

    /// The same as [`Self::dequeue`], except the timestamp each packet was
    /// transmitted is written to the provided slice.
    ///
    /// Note this requires that [`crate::Packet::set_tx_metadata`] was called
    pub fn dequeue_with_timestamps<S: Slab>(&mut self, umem: &mut Umem, packets: &mut S, timestamps: &mut [u64]) -> usize {
        let nb = packets.available();
        if nb == 0 {
            return 0;
        }

        let (actual, idx) = self.ring.peek(nb as _);

        if actual > 0 {
            for (ts, i) in timestamps.iter_mut().zip(idx..idx + actual) {
                let desc = self.ring.get(i);
                packets.push_front(
                    // SAFETY: The user is responsible for the lifetime of the
                    // packets we are returning
                    unsafe { umem.packet(desc) },
                );
                *ts = umem.free_get_timestamp(desc.addr);
            }

            self.ring.release(actual as _);
        }

        actual
    }
}
