//! The [`FillRing`] is a producer ring that userspace can enqueue packets to be
//! filled with data received on the NIC queue the ring is bound to

use crate::{
    libc::{self, rings}, slab::Slab
};

/// The ring used to enqueue buffers for the kernel to fill in with packets
/// received from a NIC
pub struct FillRing {
    #[allow(missing_docs)]
    pub ring: super::XskProducer<libc::xdp::xdp_desc>,
    _mmap: crate::mmap::Mmap,
}

impl FillRing {
    pub(crate) fn new(
        socket: std::os::fd::RawFd,
        cfg: &super::RingConfig,
        offsets: &rings::xdp_mmap_offsets,
    ) -> Result<Self, crate::socket::SocketError> {
        let (_mmap, mut ring) = super::map_ring(
            socket,
            cfg.fill_count,
            rings::RingPageOffsets::Fill,
            &offsets.fill,
        )
        .map_err(|inner| crate::socket::SocketError::RingMap {
            inner,
            ring: super::Ring::Fill,
        })?;

        ring.cached_consumed = cfg.fill_count;
        ring.cached_produced = 0;

        Ok(Self {
            ring: super::XskProducer(ring),
            _mmap,
        })
    }

    /// Enqueues `packets` to be received and filled by the kernel
    ///
    /// # Safety
    ///
    /// The [`Umem`] must outlive the `AF_XDP` socket
    ///
    /// # Returns
    ///
    /// The number of packets that were actually enqueued.
    pub unsafe fn enqueue<S: Slab>(&mut self, packets: &mut S) -> usize {
        let requested = packets.len();
        if requested == 0 {
            return 0;
        }

        let (actual, idx) = self.ring.reserve(requested as _);

        if actual > 0 {
            for i in idx..idx + actual {
                let Some(packet) = packets.pop_back() else {
                    unreachable!()
                };

                self.ring.set(i, packet.into());
            }

            self.ring.submit(actual as _);
        }

        actual
    }
}

/// The wakable version of [`FillRing`], which requires that we notify the kernel
/// when there are new buffers available to receive packets
pub struct WakableFillRing {
    inner: FillRing,
    socket: std::os::fd::RawFd,
}

impl WakableFillRing {
    pub(crate) fn new(
        socket: std::os::fd::RawFd,
        cfg: &super::RingConfig,
        offsets: &rings::xdp_mmap_offsets,
    ) -> Result<Self, crate::socket::SocketError> {
        let inner = FillRing::new(socket, cfg, offsets)?;

        Ok(Self { inner, socket })
    }

    /// The same as [`FillRing::enqueue`], except the additional `wakeup` parameter
    /// determines if the kernel is actually informed of the new buffer(s) available
    /// to fill with data
    ///
    /// # Safety
    ///
    /// The [`Umem`] must outlive the `AF_XDP` socket
    #[inline]
    pub unsafe fn enqueue<S: Slab>(
        &mut self,
        packets: &mut S,
        wakeup: bool,
    ) -> std::io::Result<usize> {
        // SAFETY: FillRing::enqueue is unsafe
        let queued = unsafe { self.inner.enqueue(packets) };

        if queued > 0 && wakeup {
            // SAFETY: This is safe, even if the socket descriptor is invalid.
            let ret = unsafe {
                libc::socket::recvfrom(
                    self.socket,
                    std::ptr::null_mut(),
                    0,
                    libc::socket::MsgFlags::DONTWAIT,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };

            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() != std::io::ErrorKind::Interrupted {
                    return Err(err);
                }
            }
        }

        Ok(queued)
    }
}
