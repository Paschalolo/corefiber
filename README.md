# corefiber 🚀
> A high-performance, header-only userspace cooperative fiber runtime engineered with manual x86_64/ARM64 assembly context switching and an MPSC work-stealing scheduler.

---

## Architectural Overview

`corefiber` bypasses operating system kernel scheduling overhead to execute millions of concurrent asynchronous tasks across a fixed pool of pinned OS worker threads. Designed around **hardware-first mechanical sympathy**, it achieves single-digit nanosecond context switching with zero syscalls in the hot path.

### Key Engineering Features
* **Manual Assembly Context Switching:** Conforms strictly to the **System V AMD64 / ARM64 ABIs**, saving and restoring callee-saved registers (`rbx`, `rbp`, `r12`-`r15`) ,  (`x29` , `x30`<LR> , `x19` - `x28`) and SIMD states directly on private task stacks.
* **M:N Work-Stealing Runtime:** Combines localized lock-free MPSC queues with a randomized work-stealing protocol to maximize CPU core saturation and eliminate thread starvation.
* **Core Pinning & Cache Locality:** Binds worker OS threads directly to physical CPU cores via `pthread_setaffinity_np` to prevent thread migration and cache thrashing.
* **Cache-Line Aligned Structures:** Uses explicit 64-byte padding (`alignas(128)`) on atomic control blocks to completely eliminate false sharing across CPU cache coherence domains . NOTE : We use twice the Block size because modern processor have agresive prefetchers. 
* **Zero-Dependency Core:** Written in modern C and raw assembly with no external runtime dependencies or heavy frameworks. 
---

## Core Performance Characteristics

| Operation | OS Thread (`pthread`) | `corefiber` (Userspace Fiber) |
| :--- | :--- | :--- |
| **Context Switch Overhead** | ~1,000 – 5,000+ cycles (Ring 0 trap) | ~15 – 30 cycles (Ring 3 assembly) |
| **Memory Footprint / Stack** | 2MB – 8MB virtual (Guard pages) | Configurable (Default 64KB `mmap`) |
| **Syscall Dependency** | High (Futex, sched_yield) | Zero in hot-path scheduling loop |

---

## Low-Level Mechanics

1. **Stack Crafting & Trampolines:** Brand new fibers are initialized by manually crafting a stack layout that injects a jump target into `fiber_trampoline`, preventing illegal instruction faults on first execution.
2. **Lock-Free Synchronization:** Employs explicit memory ordering semantics (`std::memory_order_acquire` / `release`) on atomic head/tail pointers to guarantee safe concurrent pushing and stealing without mutex contention.
