use rand::RngCore;

pub trait ValueGenerator {
    type Config: Default;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self;
}

#[derive(Debug, Clone)]
pub struct StringConfig {
    pub min_length: usize,
    pub max_length: usize,
    pub alphabet: Vec<char>,
}

impl Default for StringConfig {
    fn default() -> Self {
        Self {
            min_length: 0,
            max_length: 10,
            alphabet: ('a'..='z').chain('A'..='Z').chain('0'..='9').collect(),
        }
    }
}

impl StringConfig {
    fn validate(&self) -> Result<(), &'static str> {
        if self.min_length > self.max_length {
            return Err("min_length must be <= max_length");
        }
        if self.alphabet.is_empty() {
            return Err("alphabet cannot be empty");
        }
        Ok(())
    }
}

impl ValueGenerator for String {
    type Config = StringConfig;

    fn generate(rng: &mut impl RngCore, cfg: &Self::Config) -> Self {
        cfg.validate().expect("Invalid StringConfig");

        let len = usize::generate(
            rng,
            &NumberConfig {
                min: cfg.min_length,
                max: cfg.max_length,
            },
        );

        (0..len)
            .map(|_| {
                let idx = (rng.next_u32() as usize) % cfg.alphabet.len();
                cfg.alphabet[idx]
            })
            .collect()
    }
}

impl ValueGenerator for bool {
    type Config = ();

    fn generate(rng: &mut impl RngCore, _config: &Self::Config) -> Self {
        rng.next_u32() & 1 == 1
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NumberConfig {
    pub min: usize,
    pub max: usize,
}

impl NumberConfig {
    pub fn new(min: usize, max: usize) -> Result<Self, &'static str> {
        if min > max {
            return Err("min must be <= max");
        }
        Ok(Self { min, max })
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.min > self.max {
            return Err("min must be <= max");
        }
        Ok(())
    }

    fn range_size(&self) -> usize {
        self.max.saturating_sub(self.min).saturating_add(1)
    }
}

impl Default for NumberConfig {
    fn default() -> Self {
        Self { min: 0, max: 100 }
    }
}

impl ValueGenerator for f64 {
    type Config = NumberConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid NumberConfig");

        let range = config.range_size();
        let random_offset = (rng.next_u64() as usize) % range;
        (config.min + random_offset) as f64
    }
}

impl ValueGenerator for u32 {
    type Config = NumberConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid NumberConfig");

        let range = config.range_size();
        let random_offset = rng.next_u32() % (range as u32);
        (config.min as u32) + random_offset
    }
}

impl ValueGenerator for usize {
    type Config = NumberConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid NumberConfig");

        let range = config.range_size();
        let random_offset = (rng.next_u32() as usize) % range;
        config.min + random_offset
    }
}

impl ValueGenerator for i32 {
    type Config = NumberConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid NumberConfig");

        let range = config.range_size();
        let random_offset = (rng.next_u32() as usize) % range;
        (config.min + random_offset) as i32
    }
}

#[derive(Debug, Clone)]
pub struct CharConfig {
    pub alphabet: Vec<char>,
}

impl Default for CharConfig {
    fn default() -> Self {
        Self {
            alphabet: ('a'..='z').chain('A'..='Z').chain('0'..='9').collect(),
        }
    }
}

impl CharConfig {
    fn validate(&self) -> Result<(), &'static str> {
        if self.alphabet.is_empty() {
            return Err("alphabet cannot be empty");
        }
        Ok(())
    }
}

impl ValueGenerator for char {
    type Config = CharConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid CharConfig");

        let idx = (rng.next_u32() as usize) % config.alphabet.len();
        config.alphabet[idx]
    }
}

impl ValueGenerator for isize {
    type Config = NumberConfig;

    fn generate(rng: &mut impl RngCore, config: &Self::Config) -> Self {
        config.validate().expect("Invalid NumberConfig");

        let range = config.range_size();
        let random_offset = (rng.next_u32() as usize) % range;
        (config.min + random_offset) as isize
    }
}
