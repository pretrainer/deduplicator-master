use cityhasher::hash;
use lazy_static::lazy_static;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use regex::Regex;
use std::cmp;
use std::iter::zip;

lazy_static! {
    static ref NUM_PERM: usize = 256;
    static ref PERMUTATIONS: (Vec<u32>, Vec<u32>) = {
        let mut gen = ChaCha8Rng::seed_from_u64(1);
        let mut permutations = (Vec::new(), Vec::new());
        for _ in 0..*NUM_PERM {
            permutations.0.push(gen.gen_range(1..u32::MAX));
            permutations.0.push(gen.gen_range(0..u32::MAX));
        }
        permutations
    };
    static ref TEXT_SPLITTER: Regex = Regex::new("[^А-Яа-яёЁA-Za-z_0-9]+").unwrap();
}

pub struct MinHash {
    values: Vec<u32>,
}

impl<Idx> std::ops::Index<Idx> for MinHash
where
    Idx: std::slice::SliceIndex<[u32]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.values[index]
    }
}

struct MinHashBuilder {
    buffer: Vec<u32>,
    values: Vec<u32>,
}

impl MinHashBuilder {
    pub fn new() -> Self {
        Self {
            buffer: vec![0; *NUM_PERM],
            values: vec![u32::MAX; *NUM_PERM],
        }
    }

    pub fn update(&mut self, value: &str) {
        let hash = hash::<u32>(&value);
        for (x, y) in zip(&mut self.buffer, &PERMUTATIONS.0) {
            *x = y.wrapping_mul(hash);
        }
        for (x, y) in zip(&mut self.buffer, &PERMUTATIONS.1) {
            *x = x.wrapping_add(*y);
        }
        for (h, x) in zip(&mut self.values, &self.buffer) {
            *h = cmp::min(*h, *x);
        }
    }

    pub fn build(self) -> MinHash {
        MinHash {
            values: self.values,
        }
    }
}

pub fn hash_text(text: &str) -> MinHash {
    let mut builder = MinHashBuilder::new();
    let lowercase = text.to_lowercase();

    for token in TEXT_SPLITTER.split(&lowercase) {
        if token.is_empty() {
            continue;
        }
        builder.update(token);
    }

    builder.build()
}
