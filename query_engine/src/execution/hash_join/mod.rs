pub mod hash_table;
pub mod operators;

use crc::{CRC_32_ISO_HDLC, Crc};

fn crc32(key: usize, seed: u32) -> u32 {
    let crc = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let mut digest = crc.digest_with_initial(seed);
    digest.update(&key.to_le_bytes());
    digest.finalize()
}

pub fn hash32(key: u32, seed: u32) -> u64 {
    let mix: u64 = 0x8648DBDB;
    let v = crc32(key as usize, seed);
    let multiplier = (mix << 32).wrapping_add(1);
    (v as u64).wrapping_mul(multiplier)
}

pub fn hash64(key: u64) -> u64 {
    let mix: u64 = 0x2545F4914F6CDD1D;
    let lower = crc32(key as usize, 0x243F6A88) as u64;
    let upper = (crc32(key as usize, 0x85A308D3) as u64) << 32;
    (lower | upper).wrapping_mul(mix)
}

pub fn hash_combine(seed: u64, value: u64) -> u64 {
    // it is 64-bit golden ratio constant (2^64 / Phi)
    seed ^ (value
        .wrapping_add(0x9e3779b97f4a7c15)
        .wrapping_add(seed << 6)
        .wrapping_add(seed >> 2))
}
