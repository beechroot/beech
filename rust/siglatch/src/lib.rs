extern crate libc;
extern crate nix;

pub use nix::sys::signal::Signal;
use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler, SigSet};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

#[derive(Debug)]
pub struct Latch(Option<Signal>, AtomicBool);

struct Latches(Vec<Latch>);

impl Latches {
    fn latches() -> &'static Self {
        static mut SINGLETON: *const Latches = 0 as *const Latches;
        static ONCE: Once = Once::new();

        unsafe {
            ONCE.call_once(|| {
                let max_sig_num = Signal::iterator().map(|s| s as usize).max().unwrap_or(0);
                let latchvec: Vec<Latch> = (0 as i32..=max_sig_num as i32)
                    .map(|i| Latch(Signal::from_c_int(i).ok(), AtomicBool::new(false)))
                    .collect();
                let singleton = Latches(latchvec);
                // Put it in the heap so it can outlive this call
                SINGLETON = mem::transmute(Box::new(singleton));
            });

            &(*SINGLETON)
        }
    }
}

extern "C" fn setter_handler(sig: libc::c_int) {
    if let Ok(s) = Signal::from_c_int(sig) {
        let latch = Latches::latches().0.get(s as usize);
        if let Some(l) = latch {
            l.1.store(true, Ordering::Relaxed);
        }
    }
}

impl Latch {
    pub fn get(s: Signal) -> nix::Result<&'static Latch> {
        unsafe {
            let latch = Latches::latches().0.get(s as usize)
                .ok_or(nix::Error::UnsupportedOperation)?;
            let mut sigset = SigSet::empty();
            sigset.add(s);
            let saflags = SaFlags::empty();

            sigaction(
                s,
                &SigAction::new(SigHandler::Handler(setter_handler), saflags, sigset),
            )?;
            Ok(latch)
        }
    }
    pub fn triggered(&self) -> bool {
        self.1.load(Ordering::Relaxed)
    }
    pub fn reset(&self) {
        self.1.store(false, Ordering::Relaxed)
    }
}
