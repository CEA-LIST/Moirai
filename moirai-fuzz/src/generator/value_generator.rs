use std::fmt::Debug;

use rand::{
    Rng, RngExt,
    distr::{Distribution, Uniform},
};

pub trait ValueGenerator {
    type Config: Default;

    fn generate(rng: &mut impl Rng, config: &Self::Config) -> Self;
}

//* STRING */
/// Validated string lengths and alphabet, with reusable unbiased samplers.
#[derive(Debug, Clone)]
pub struct StringConfig {
    length: Uniform<usize>,
    alphabet: Vec<char>,
    alphabet_index: Uniform<usize>,
}

impl StringConfig {
    pub fn new(
        min_length: usize,
        max_length: usize,
        alphabet: Vec<char>,
    ) -> Result<Self, &'static str> {
        let length = Uniform::new_inclusive(min_length, max_length)
            .map_err(|_| "min_length must be <= max_length")?;
        let alphabet_index =
            Uniform::new(0, alphabet.len()).map_err(|_| "alphabet cannot be empty")?;

        Ok(Self {
            length,
            alphabet,
            alphabet_index,
        })
    }
}

impl Default for StringConfig {
    fn default() -> Self {
        Self::new(2, 4, ('a'..='z').collect()).expect("valid default string config")
    }
}

impl ValueGenerator for String {
    type Config = StringConfig;

    fn generate(rng: &mut impl Rng, config: &Self::Config) -> Self {
        let length = config.length.sample(rng);

        (0..length)
            .map(|_| config.alphabet[config.alphabet_index.sample(rng)])
            .collect()
    }
}

//* BOOLEAN */
impl ValueGenerator for bool {
    type Config = ();

    fn generate(rng: &mut impl Rng, _config: &Self::Config) -> Self {
        rng.random()
    }
}

//* INTEGER */
mod integer {
    use super::*;

    pub trait Sampler: Debug + Copy {
        type Distribution: Debug + Copy;

        fn distribution(min: Self, max: Self) -> Result<Self::Distribution, &'static str>;
        fn full_distribution() -> Self::Distribution;
        fn sample(distribution: &Self::Distribution, rng: &mut impl Rng) -> Self;
    }
}

/// Integer types supported by [`IntDistribution`].
///
/// Implementations are provided for all primitive integer types.
pub trait Integer: integer::Sampler {}

/// An inclusive integer range, or the integer type's full domain.
///
/// Bounds have the same type as the generated value. The sampler is validated
/// and constructed once, including for ranges spanning the entire domain.
#[derive(Debug, Clone, Copy)]
pub struct IntDistribution<T: Integer> {
    range: T::Distribution,
}

impl<T: Integer> IntDistribution<T> {
    pub fn new(min: T, max: T) -> Result<Self, &'static str> {
        Ok(Self {
            range: T::distribution(min, max)?,
        })
    }

    pub fn full() -> Self {
        Self {
            range: T::full_distribution(),
        }
    }
}

macro_rules! integer_generators {
    ($($integer:ty),+ $(,)?) => {
        $(
            impl Default for IntDistribution<$integer> {
                fn default() -> Self {
                    Self::new(0, 100).expect("valid default integer range")
                }
            }

            impl ValueGenerator for $integer {
                type Config = IntDistribution<Self>;

                fn generate(rng: &mut impl Rng, config: &Self::Config) -> Self {
                    <Self as integer::Sampler>::sample(&config.range, rng)
                }
            }
        )+
    };
}

macro_rules! integer_samplers {
    ($($integer:ty),+ $(,)?) => {
        $(
            impl Integer for $integer {}

            impl integer::Sampler for $integer {
                type Distribution = Uniform<Self>;

                fn distribution(min: Self, max: Self) -> Result<Self::Distribution, &'static str> {
                    Uniform::new_inclusive(min, max).map_err(|_| "min must be <= max")
                }

                fn full_distribution() -> Self::Distribution {
                    Uniform::new_inclusive(Self::MIN, Self::MAX)
                        .expect("valid full integer domain")
                }

                fn sample(distribution: &Self::Distribution, rng: &mut impl Rng) -> Self {
                    distribution.sample(rng)
                }
            }
        )+
    };
}

integer_samplers!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128);

// rand does not implement integer sampling for isize. Its values fit in i64
// on supported 32- and 64-bit platforms, and the validated bounds ensure the
// sampled value can always be converted back without truncation.
impl Integer for isize {}

impl integer::Sampler for isize {
    type Distribution = Uniform<i64>;

    fn distribution(min: Self, max: Self) -> Result<Self::Distribution, &'static str> {
        Uniform::new_inclusive(min as i64, max as i64).map_err(|_| "min must be <= max")
    }

    fn full_distribution() -> Self::Distribution {
        Self::distribution(Self::MIN, Self::MAX).expect("valid full isize domain")
    }

    fn sample(distribution: &Self::Distribution, rng: &mut impl Rng) -> Self {
        Self::try_from(distribution.sample(rng)).expect("sample lies within validated isize bounds")
    }
}

integer_generators!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

/// A finite floating-point interval `[min, max)` with a finite width.
#[derive(Debug, Clone, Copy)]
pub struct FloatDistribution {
    range: Uniform<f64>,
    max: f64,
}

impl FloatDistribution {
    pub fn new(min: f64, max: f64) -> Result<Self, &'static str> {
        if !min.is_finite() || !max.is_finite() {
            return Err("float bounds must be finite");
        }
        if min >= max {
            return Err("min must be < max");
        }

        let range = Uniform::new(min, max).map_err(|_| "float range width must be finite")?;
        Ok(Self { range, max })
    }
}

impl Default for FloatDistribution {
    fn default() -> Self {
        Self::new(0.0, 100.0).expect("valid default float range")
    }
}

impl ValueGenerator for f64 {
    type Config = FloatDistribution;

    fn generate(rng: &mut impl Rng, config: &Self::Config) -> Self {
        loop {
            let value = config.range.sample(rng);
            // Floating-point rounding may produce the exclusive upper bound.
            if value < config.max {
                return value;
            }
        }
    }
}

//* CHARACTER */
/// A nonempty alphabet sampled uniformly by character position.
#[derive(Debug, Clone)]
pub struct CharConfig {
    alphabet: Vec<char>,
    alphabet_index: Uniform<usize>,
}

impl CharConfig {
    pub fn new(alphabet: Vec<char>) -> Result<Self, &'static str> {
        let alphabet_index =
            Uniform::new(0, alphabet.len()).map_err(|_| "alphabet cannot be empty")?;

        Ok(Self {
            alphabet,
            alphabet_index,
        })
    }
}

impl Default for CharConfig {
    fn default() -> Self {
        Self::new(('a'..='z').chain('A'..='Z').chain('0'..='9').collect())
            .expect("valid default character config")
    }
}

impl ValueGenerator for char {
    type Config = CharConfig;

    fn generate(rng: &mut impl Rng, config: &Self::Config) -> Self {
        config.alphabet[config.alphabet_index.sample(rng)]
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;

    #[test]
    fn integer_ranges_support_endpoints_and_full_domains() {
        let mut rng = ChaCha8Rng::from_seed([1; 32]);

        macro_rules! check_integer {
            ($integer:ty) => {
                let min = IntDistribution::new(<$integer>::MIN, <$integer>::MIN).unwrap();
                let max = IntDistribution::new(<$integer>::MAX, <$integer>::MAX).unwrap();
                let inclusive = IntDistribution::new(<$integer>::MIN, <$integer>::MAX).unwrap();
                let full = IntDistribution::full();
                for _ in 0..32 {
                    assert_eq!(<$integer>::generate(&mut rng, &min), <$integer>::MIN);
                    assert_eq!(<$integer>::generate(&mut rng, &max), <$integer>::MAX);
                    let _ = <$integer>::generate(&mut rng, &inclusive);
                    let _ = <$integer>::generate(&mut rng, &full);
                }
                assert!(IntDistribution::<$integer>::new(2, 1).is_err());
            };
        }

        check_integer!(u8);
        check_integer!(u16);
        check_integer!(u32);
        check_integer!(u64);
        check_integer!(u128);
        check_integer!(usize);
        check_integer!(i8);
        check_integer!(i16);
        check_integer!(i32);
        check_integer!(i64);
        check_integer!(i128);
        check_integer!(isize);
    }

    #[test]
    fn signed_ranges_generate_negative_values() {
        let mut rng = ChaCha8Rng::from_seed([2; 32]);
        let config = IntDistribution::new(-20_i32, -1).unwrap();
        for _ in 0..256 {
            assert!((-20..=-1).contains(&i32::generate(&mut rng, &config)));
        }
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn usize_ranges_use_more_than_32_random_bits() {
        let mut rng = ChaCha8Rng::from_seed([3; 32]);
        let config = IntDistribution::new(0, usize::MAX).unwrap();
        assert!((0..32).any(|_| usize::generate(&mut rng, &config) > u32::MAX as usize));
    }

    #[test]
    fn floats_are_fractional_and_support_negative_ranges() {
        let mut rng = ChaCha8Rng::from_seed([4; 32]);
        let config = FloatDistribution::new(-2.5, -0.5).unwrap();
        let values: Vec<_> = (0..256).map(|_| f64::generate(&mut rng, &config)).collect();
        assert!(values.iter().all(|value| (-2.5..-0.5).contains(value)));
        assert!(values.iter().any(|value| value.fract() != 0.0));

        let adjacent = FloatDistribution::new(1.0, 1.0_f64.next_up()).unwrap();
        for _ in 0..32 {
            assert_eq!(f64::generate(&mut rng, &adjacent), 1.0);
        }
    }

    #[test]
    fn float_ranges_reject_invalid_bounds_and_overflowing_widths() {
        for (min, max) in [
            (1.0, 1.0),
            (2.0, 1.0),
            (f64::NAN, 1.0),
            (0.0, f64::NAN),
            (f64::NEG_INFINITY, 1.0),
            (0.0, f64::INFINITY),
            (-f64::MAX, f64::MAX),
        ] {
            assert!(FloatDistribution::new(min, max).is_err());
        }
    }

    #[test]
    fn alphabets_and_string_lengths_are_validated_at_construction() {
        assert!(CharConfig::new(Vec::new()).is_err());
        assert!(StringConfig::new(0, 1, Vec::new()).is_err());
        assert!(StringConfig::new(2, 1, vec!['a']).is_err());

        let mut rng = ChaCha8Rng::from_seed([5; 32]);
        let chars = CharConfig::new(vec!['é']).unwrap();
        let strings = StringConfig::new(3, 3, vec!['é']).unwrap();
        assert_eq!(char::generate(&mut rng, &chars), 'é');
        assert_eq!(String::generate(&mut rng, &strings), "ééé");
        let empty = StringConfig::new(0, 0, vec!['a']).unwrap();
        assert_eq!(String::generate(&mut rng, &empty), "");
    }
}
